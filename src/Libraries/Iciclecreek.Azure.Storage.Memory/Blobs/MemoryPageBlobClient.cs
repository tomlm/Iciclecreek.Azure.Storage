using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>In-memory drop-in replacement for <see cref="PageBlobClient"/>.</summary>
public class MemoryPageBlobClient : PageBlobClient
{
    internal readonly MemoryBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    // Track page ranges in memory
    private readonly List<(long Offset, long Length)> _pageRanges = new();
    private readonly object _pageRangesLock = new();

    internal MemoryPageBlobClient(MemoryBlobServiceClient serviceClient, string containerName, string blobName) : base()
    {
        _serviceClient = serviceClient;
        _containerName = containerName;
        _blobName = blobName;
    }

    // -- Properties --

    public override string AccountName => _serviceClient.AccountName;
    public override string BlobContainerName => _containerName;
    public override string Name => _blobName;
    public override Uri Uri => new($"{_serviceClient.Uri}{_containerName}/{System.Uri.EscapeDataString(_blobName)}");

    // -- Create --

    public override async Task<Response<BlobContentInfo>> CreateAsync(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
    {
        if (size % 512 != 0)
            throw new RequestFailedException(400, "Page blob size must be a multiple of 512.", "InvalidHeaderValue", null);

        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var now = DateTimeOffset.UtcNow;
        var etag = BlobEntry.NewETag();
        var sequenceNumber = options?.SequenceNumber ?? 0;

        var entry = new BlobEntry
        {
            Content = new byte[size],
            BlobType = "Page",
            ContentType = options?.HttpHeaders?.ContentType ?? "application/octet-stream",
            ContentEncoding = options?.HttpHeaders?.ContentEncoding,
            ContentLanguage = options?.HttpHeaders?.ContentLanguage,
            ContentDisposition = options?.HttpHeaders?.ContentDisposition,
            CacheControl = options?.HttpHeaders?.CacheControl,
            ETag = etag,
            CreatedOn = now,
            LastModified = now,
            SequenceNumber = sequenceNumber,
            Metadata = options?.Metadata is not null ? new Dictionary<string, string>(options.Metadata) : null,
            Tags = options?.Tags is not null ? new Dictionary<string, string>(options.Tags) : null,
        };

        store.Blobs[_blobName] = entry;

        lock (_pageRangesLock) { _pageRanges.Clear(); }

        var info = BlobsModelFactory.BlobContentInfo(
            eTag: new ETag(etag), lastModified: now,
            contentHash: null, versionId: null, encryptionKeySha256: null, encryptionScope: null, blobSequenceNumber: sequenceNumber);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<BlobContentInfo> Create(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
        => CreateAsync(size, options, ct).GetAwaiter().GetResult();

    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        if (store.Blobs.TryGetValue(_blobName, out var existing))
        {
            var info = BlobsModelFactory.BlobContentInfo(
                eTag: new ETag(existing.ETag), lastModified: existing.LastModified,
                contentHash: null, versionId: null, encryptionKeySha256: null, encryptionScope: null, blobSequenceNumber: existing.SequenceNumber);
            return Response.FromValue(info, StubResponse.Ok());
        }

        return await CreateAsync(size, options, ct).ConfigureAwait(false);
    }

    public override Response<BlobContentInfo> CreateIfNotExists(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
        => CreateIfNotExistsAsync(size, options, ct).GetAwaiter().GetResult();

    // -- UploadPages --

    public override async Task<Response<PageInfo>> UploadPagesAsync(Stream content, long offset, PageBlobUploadPagesOptions? options = null, CancellationToken ct = default)
    {
        if (offset % 512 != 0)
            throw new RequestFailedException(400, "Page offset must be a multiple of 512.", "InvalidHeaderValue", null);

        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, ct).ConfigureAwait(false);
        var data = ms.ToArray();

        if (data.Length % 512 != 0)
            throw new RequestFailedException(400, "Page data length must be a multiple of 512.", "InvalidHeaderValue", null);

        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        long seqNum;
        string etag;
        DateTimeOffset now;

        lock (entry.Lock)
        {
            // Write pages at offset
            Array.Copy(data, 0, entry.Content, offset, data.Length);
            entry.SequenceNumber++;
            entry.Touch();

            seqNum = entry.SequenceNumber;
            etag = entry.ETag;
            now = entry.LastModified;
        }

        // Track page range
        lock (_pageRangesLock)
        {
            _pageRanges.Add((offset, data.Length));
            MergePageRangesInPlace();
        }

        var info = BlobsModelFactory.PageInfo(new ETag(etag), now, null, null, seqNum, null);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<PageInfo> UploadPages(Stream content, long offset, PageBlobUploadPagesOptions? options = null, CancellationToken ct = default)
        => UploadPagesAsync(content, offset, options, ct).GetAwaiter().GetResult();

    public override async Task<Response<PageInfo>> UploadPagesAsync(Stream content, long offset, byte[]? transactionalContentHash = null, PageBlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, CancellationToken ct = default)
        => await UploadPagesAsync(content, offset, (PageBlobUploadPagesOptions?)null, ct).ConfigureAwait(false);

    public override Response<PageInfo> UploadPages(Stream content, long offset, byte[]? transactionalContentHash = null, PageBlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, CancellationToken ct = default)
        => UploadPagesAsync(content, offset, (PageBlobUploadPagesOptions?)null, ct).GetAwaiter().GetResult();

    // -- ClearPages --

    public override async Task<Response<PageInfo>> ClearPagesAsync(HttpRange range, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var offset = range.Offset;
        var length = range.Length ?? (entry.Content.Length - offset);
        long seqNum;
        string etag;
        DateTimeOffset now;

        lock (entry.Lock)
        {
            Array.Clear(entry.Content, (int)offset, (int)length);
            entry.SequenceNumber++;
            entry.Touch();

            seqNum = entry.SequenceNumber;
            etag = entry.ETag;
            now = entry.LastModified;
        }

        lock (_pageRangesLock)
        {
            RemovePageRangeInPlace(offset, length);
        }

        var info = BlobsModelFactory.PageInfo(new ETag(etag), now, null, null, seqNum, null);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageInfo> ClearPages(HttpRange range, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => ClearPagesAsync(range, conditions, ct).GetAwaiter().GetResult();

    // -- GetPageRanges --

    public override async Task<Response<PageRangesInfo>> GetPageRangesAsync(HttpRange? range = null, string? snapshot = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        List<HttpRange> pageRanges;
        lock (_pageRangesLock)
        {
            pageRanges = _pageRanges.Select(r => new HttpRange(r.Offset, r.Length)).ToList();
        }

        var info = BlobsModelFactory.PageRangesInfo(entry.LastModified, new ETag(entry.ETag), entry.Content.Length, pageRanges, Array.Empty<HttpRange>());
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageRangesInfo> GetPageRanges(HttpRange? range = null, string? snapshot = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => GetPageRangesAsync(range, snapshot, conditions, ct).GetAwaiter().GetResult();

    // -- Resize --

    public override async Task<Response<PageBlobInfo>> ResizeAsync(long size, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        if (size % 512 != 0)
            throw new RequestFailedException(400, "Page blob size must be a multiple of 512.", "InvalidHeaderValue", null);

        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        long seqNum;
        string etag;
        DateTimeOffset now;

        lock (entry.Lock)
        {
            var newContent = new byte[size];
            Array.Copy(entry.Content, newContent, Math.Min(entry.Content.Length, (int)size));
            entry.Content = newContent;
            entry.SequenceNumber++;
            entry.Touch();

            seqNum = entry.SequenceNumber;
            etag = entry.ETag;
            now = entry.LastModified;
        }

        // Remove page ranges beyond new size
        lock (_pageRangesLock)
        {
            _pageRanges.RemoveAll(r => r.Offset >= size);
            // Truncate ranges that extend past new size
            for (int i = 0; i < _pageRanges.Count; i++)
            {
                var r = _pageRanges[i];
                if (r.Offset + r.Length > size)
                    _pageRanges[i] = (r.Offset, size - r.Offset);
            }
        }

        var info = BlobsModelFactory.PageBlobInfo(new ETag(etag), now, seqNum);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageBlobInfo> Resize(long size, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => ResizeAsync(size, conditions, ct).GetAwaiter().GetResult();

    // -- UpdateSequenceNumber --

    public override async Task<Response<PageBlobInfo>> UpdateSequenceNumberAsync(SequenceNumberAction action, long? sequenceNumber = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        long seqNum;
        string etag;
        DateTimeOffset now;

        lock (entry.Lock)
        {
            switch (action)
            {
                case SequenceNumberAction.Max:
                    if (sequenceNumber.HasValue && sequenceNumber.Value > entry.SequenceNumber)
                        entry.SequenceNumber = sequenceNumber.Value;
                    break;
                case SequenceNumberAction.Update:
                    entry.SequenceNumber = sequenceNumber ?? 0;
                    break;
                case SequenceNumberAction.Increment:
                    entry.SequenceNumber++;
                    break;
            }
            entry.Touch();

            seqNum = entry.SequenceNumber;
            etag = entry.ETag;
            now = entry.LastModified;
        }

        var info = BlobsModelFactory.PageBlobInfo(new ETag(etag), now, seqNum);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageBlobInfo> UpdateSequenceNumber(SequenceNumberAction action, long? sequenceNumber = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => UpdateSequenceNumberAsync(action, sequenceNumber, conditions, ct).GetAwaiter().GetResult();

    // -- GetProperties --

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken ct = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, ct).ConfigureAwait(false);
    }

    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken ct = default)
        => GetPropertiesAsync(conditions, ct).GetAwaiter().GetResult();

    // -- Helpers --

    private void MergePageRangesInPlace()
    {
        if (_pageRanges.Count <= 1) return;
        _pageRanges.Sort((a, b) => a.Offset.CompareTo(b.Offset));
        var merged = new List<(long Offset, long Length)> { _pageRanges[0] };
        for (int i = 1; i < _pageRanges.Count; i++)
        {
            var last = merged[^1];
            var lastEnd = last.Offset + last.Length;
            if (lastEnd >= _pageRanges[i].Offset)
            {
                var newEnd = Math.Max(lastEnd, _pageRanges[i].Offset + _pageRanges[i].Length);
                merged[^1] = (last.Offset, newEnd - last.Offset);
            }
            else
            {
                merged.Add(_pageRanges[i]);
            }
        }
        _pageRanges.Clear();
        _pageRanges.AddRange(merged);
    }

    private void RemovePageRangeInPlace(long offset, long length)
    {
        var clearEnd = offset + length;
        var result = new List<(long Offset, long Length)>();

        foreach (var r in _pageRanges)
        {
            var rEnd = r.Offset + r.Length;
            if (rEnd <= offset || r.Offset >= clearEnd)
            {
                result.Add(r);
            }
            else
            {
                if (r.Offset < offset)
                    result.Add((r.Offset, offset - r.Offset));
                if (rEnd > clearEnd)
                    result.Add((clearEnd, rEnd - clearEnd));
            }
        }
        _pageRanges.Clear();
        _pageRanges.AddRange(result);
    }
}
