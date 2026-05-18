using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;
using PageRange = Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal.PageRange;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>
/// Filesystem-backed drop-in replacement for <see cref="PageBlobClient"/>.
/// Stores pages as a pre-allocated file with 512-byte page alignment.
/// </summary>
public class FilePageBlobClient : PageBlobClient
{
    internal readonly BlobStore _store;
    internal readonly string _blobName;
    internal readonly FileBlobServiceClient _serviceClient;

    internal FilePageBlobClient(FileBlobServiceClient serviceClient, string containerName, string blobName) : base()
    {
        _serviceClient = serviceClient;
        _store = new BlobStore(serviceClient.BlobsRootPath, containerName, serviceClient.Options);
        _blobName = blobName;
    }

    // ── Properties ──────────────────────────────────────────────────────

    public override string AccountName => _serviceClient.AccountName;
    public override string BlobContainerName => _store.ContainerName;
    public override string Name => _blobName;
    public override Uri Uri => new($"{_serviceClient.Uri}{_store.ContainerName}/{_blobName}");

    // ── Create ──────────────────────────────────────────────────────────

    public override async Task<Response<BlobContentInfo>> CreateAsync(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
    {
        if (size % 512 != 0)
            throw new RequestFailedException(400, "Page blob size must be a multiple of 512.", "InvalidHeaderValue", null);

        var blobPath = _store.BlobPath(_blobName);
        var dir = Path.GetDirectoryName(blobPath);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        await using (var fs = new FileStream(blobPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(size);
        }

        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(size, now, null).ToString();
        var sidecar = new BlobSidecar
        {
            BlobType = BlobKind.Page,
            Length = size,
            ETag = etag,
            CreatedOnUtc = now,
            LastModifiedUtc = now,
            SequenceNumber = options?.SequenceNumber ?? 0,
            ContentType = options?.HttpHeaders?.ContentType,
            ContentEncoding = options?.HttpHeaders?.ContentEncoding,
            ContentLanguage = options?.HttpHeaders?.ContentLanguage,
            ContentDisposition = options?.HttpHeaders?.ContentDisposition,
            CacheControl = options?.HttpHeaders?.CacheControl,
        };
        if (options?.Metadata != null)
            sidecar.Metadata = new Dictionary<string, string>(options.Metadata, StringComparer.Ordinal);
        if (options?.Tags != null)
            sidecar.Tags = new Dictionary<string, string>(options.Tags, StringComparer.Ordinal);

        await _store.WriteSidecarAsync(_blobName, sidecar, ct).ConfigureAwait(false);

        var info = BlobsModelFactory.BlobContentInfo(
            eTag: new ETag(etag), lastModified: now,
            contentHash: null, versionId: null, encryptionKeySha256: null, encryptionScope: null, blobSequenceNumber: sidecar.SequenceNumber);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<BlobContentInfo> Create(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
        => CreateAsync(size, options, ct).GetAwaiter().GetResult();

    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
    {
        if (_store.Exists(_blobName))
        {
            var existing = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false);
            var info = BlobsModelFactory.BlobContentInfo(
                eTag: new ETag(existing?.ETag ?? "\"0x0\""), lastModified: existing?.LastModifiedUtc ?? DateTimeOffset.UtcNow,
                contentHash: null, versionId: null, encryptionKeySha256: null, encryptionScope: null, blobSequenceNumber: existing?.SequenceNumber ?? 0);
            return Response.FromValue(info, StubResponse.Ok());
        }
        return await CreateAsync(size, options, ct).ConfigureAwait(false);
    }

    public override Response<BlobContentInfo> CreateIfNotExists(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
        => CreateIfNotExistsAsync(size, options, ct).GetAwaiter().GetResult();

    // ── UploadPages ─────────────────────────────────────────────────────

    public override async Task<Response<PageInfo>> UploadPagesAsync(Stream content, long offset, PageBlobUploadPagesOptions? options = null, CancellationToken ct = default)
    {
        if (offset % 512 != 0)
            throw new RequestFailedException(400, "Page offset must be a multiple of 512.", "InvalidHeaderValue", null);

        var sidecar = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var blobPath = _store.BlobPath(_blobName);
        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, ct).ConfigureAwait(false);
        var data = ms.ToArray();

        if (data.Length % 512 != 0)
            throw new RequestFailedException(400, "Page data length must be a multiple of 512.", "InvalidHeaderValue", null);

        await using (var fs = new FileStream(blobPath, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            await fs.WriteAsync(data, ct).ConfigureAwait(false);
        }

        MergePageRange(sidecar, offset, data.Length);

        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;
        sidecar.SequenceNumber++;
        sidecar.ETag = ETagCalculator.Compute(sidecar.Length, sidecar.LastModifiedUtc, null).ToString();
        await _store.WriteSidecarAsync(_blobName, sidecar, ct).ConfigureAwait(false);

        var info = BlobsModelFactory.PageInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc, null, null, sidecar.SequenceNumber, null);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<PageInfo> UploadPages(Stream content, long offset, PageBlobUploadPagesOptions? options = null, CancellationToken ct = default)
        => UploadPagesAsync(content, offset, options, ct).GetAwaiter().GetResult();

    public override async Task<Response<PageInfo>> UploadPagesAsync(Stream content, long offset, byte[]? transactionalContentHash = null, PageBlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, CancellationToken ct = default)
        => await UploadPagesAsync(content, offset, (PageBlobUploadPagesOptions?)null, ct).ConfigureAwait(false);

    public override Response<PageInfo> UploadPages(Stream content, long offset, byte[]? transactionalContentHash = null, PageBlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, CancellationToken ct = default)
        => UploadPagesAsync(content, offset, (PageBlobUploadPagesOptions?)null, ct).GetAwaiter().GetResult();

    // ── ClearPages ──────────────────────────────────────────────────────

    public override async Task<Response<PageInfo>> ClearPagesAsync(HttpRange range, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var offset = range.Offset;
        var length = range.Length ?? (sidecar.Length - offset);

        var blobPath = _store.BlobPath(_blobName);
        var zeros = new byte[length];
        await using (var fs = new FileStream(blobPath, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            await fs.WriteAsync(zeros, ct).ConfigureAwait(false);
        }

        RemovePageRange(sidecar, offset, length);

        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;
        sidecar.SequenceNumber++;
        sidecar.ETag = ETagCalculator.Compute(sidecar.Length, sidecar.LastModifiedUtc, null).ToString();
        await _store.WriteSidecarAsync(_blobName, sidecar, ct).ConfigureAwait(false);

        var info = BlobsModelFactory.PageInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc, null, null, sidecar.SequenceNumber, null);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageInfo> ClearPages(HttpRange range, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => ClearPagesAsync(range, conditions, ct).GetAwaiter().GetResult();

    // ── GetPageRanges ───────────────────────────────────────────────────

    public override async Task<Response<PageRangesInfo>> GetPageRangesAsync(HttpRange? range = null, string? snapshot = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var pageRanges = sidecar.PageRanges
            .Select(r => new HttpRange(r.Offset, r.Length))
            .ToList();

        var info = BlobsModelFactory.PageRangesInfo(sidecar.LastModifiedUtc, new ETag(sidecar.ETag), sidecar.Length, pageRanges, Array.Empty<HttpRange>());
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageRangesInfo> GetPageRanges(HttpRange? range = null, string? snapshot = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => GetPageRangesAsync(range, snapshot, conditions, ct).GetAwaiter().GetResult();

    // ── Resize ──────────────────────────────────────────────────────────

    public override async Task<Response<PageBlobInfo>> ResizeAsync(long size, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        if (size % 512 != 0)
            throw new RequestFailedException(400, "Page blob size must be a multiple of 512.", "InvalidHeaderValue", null);

        var sidecar = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var blobPath = _store.BlobPath(_blobName);
        await using (var fs = new FileStream(blobPath, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(size);
        }

        sidecar.Length = size;
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;
        sidecar.SequenceNumber++;
        sidecar.ETag = ETagCalculator.Compute(size, sidecar.LastModifiedUtc, null).ToString();
        sidecar.PageRanges.RemoveAll(r => r.Offset >= size);
        await _store.WriteSidecarAsync(_blobName, sidecar, ct).ConfigureAwait(false);

        var info = BlobsModelFactory.PageBlobInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc, sidecar.SequenceNumber);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageBlobInfo> Resize(long size, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => ResizeAsync(size, conditions, ct).GetAwaiter().GetResult();

    // ── GetProperties ───────────────────────────────────────────────────

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken ct = default)
    {
        var blobClient = new FileBlobClient(_serviceClient, _store.ContainerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, ct).ConfigureAwait(false);
    }

    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken ct = default)
        => GetPropertiesAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Helpers ──────────────────────────────────────────────────────────

    private static void MergePageRange(BlobSidecar sidecar, long offset, long length)
    {
        sidecar.PageRanges.Add(new PageRange { Offset = offset, Length = length });
        sidecar.PageRanges = sidecar.PageRanges.OrderBy(r => r.Offset).ToList();
        var merged = new List<PageRange>();
        foreach (var range in sidecar.PageRanges)
        {
            if (merged.Count > 0 && merged[^1].Offset + merged[^1].Length >= range.Offset)
            {
                var last = merged[^1];
                var end = Math.Max(last.Offset + last.Length, range.Offset + range.Length);
                last.Length = end - last.Offset;
            }
            else
            {
                merged.Add(new PageRange { Offset = range.Offset, Length = range.Length });
            }
        }
        sidecar.PageRanges = merged;
    }

    private static void RemovePageRange(BlobSidecar sidecar, long offset, long length)
    {
        var clearEnd = offset + length;
        var result = new List<PageRange>();
        foreach (var r in sidecar.PageRanges)
        {
            var rEnd = r.Offset + r.Length;
            if (rEnd <= offset || r.Offset >= clearEnd)
            {
                result.Add(r);
            }
            else
            {
                if (r.Offset < offset)
                    result.Add(new PageRange { Offset = r.Offset, Length = offset - r.Offset });
                if (rEnd > clearEnd)
                    result.Add(new PageRange { Offset = clearEnd, Length = rEnd - clearEnd });
            }
        }
        sidecar.PageRanges = result;
    }
}
