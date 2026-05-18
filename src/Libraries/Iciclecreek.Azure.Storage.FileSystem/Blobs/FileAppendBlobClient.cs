using System.Security.Cryptography;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>Filesystem-backed drop-in replacement for <see cref="Azure.Storage.Blobs.Specialized.AppendBlobClient"/>. Supports Create and AppendBlock using filesystem append mode.</summary>
public class FileAppendBlobClient : AppendBlobClient
{
    internal readonly BlobStore _store;
    internal readonly string _blobName;
    internal readonly FileBlobServiceClient _serviceClient;

    internal FileAppendBlobClient(FileBlobServiceClient serviceClient, string containerName, string blobName) : base()
    {
        _serviceClient = serviceClient;
        _store = new BlobStore(serviceClient.BlobsRootPath, containerName, serviceClient.Options);
        _blobName = blobName;
    }

    /// <inheritdoc/>
    public override string Name => _blobName;
    /// <inheritdoc/>
    public override string BlobContainerName => _store.ContainerName;
    /// <inheritdoc/>
    public override string AccountName => _serviceClient.AccountName;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_serviceClient.Uri}{_store.ContainerName}/{System.Uri.EscapeDataString(_blobName)}");

    // ---- Create (async = primary) ----

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        var conditions = options?.Conditions;
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        if (conditions is not null)
            _store.CheckConditions(sidecar, conditions.IfMatch, conditions.IfNoneMatch, mustExist: false, nameof(Create));

        var path = _store.BlobPath(_blobName);
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        await File.WriteAllBytesAsync(path, [], cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(0, now, ReadOnlySpan<byte>.Empty);
        var newSidecar = new BlobSidecar
        {
            BlobType = BlobKind.Append,
            Length = 0,
            ETag = etag.ToString()!,
            CreatedOnUtc = now,
            LastModifiedUtc = now,
            ContentType = options?.HttpHeaders?.ContentType ?? "application/octet-stream",
            ContentEncoding = options?.HttpHeaders?.ContentEncoding,
        };
        if (options?.Metadata is { } meta)
            newSidecar.Metadata = new Dictionary<string, string>(meta, StringComparer.Ordinal);
        await _store.WriteSidecarAsync(_blobName, newSidecar, cancellationToken).ConfigureAwait(false);

        var info = BlobsModelFactory.BlobContentInfo(etag, now, null, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => await CreateAsync(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        if (_store.Exists(_blobName))
            return Response.FromValue<BlobContentInfo>(null!, StubResponse.Ok());
        return await CreateAsync(options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
        => await CreateIfNotExistsAsync(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobContentInfo> Create(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
        => CreateAsync(options, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Response<BlobContentInfo> Create(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => CreateAsync(httpHeaders, metadata, conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Response<BlobContentInfo> CreateIfNotExists(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
        => CreateIfNotExistsAsync(options, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Response<BlobContentInfo> CreateIfNotExists(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
        => CreateIfNotExistsAsync(httpHeaders, metadata, cancellationToken).GetAwaiter().GetResult();

    // ---- AppendBlock (async = primary) ----

    /// <inheritdoc/>
    public override async Task<Response<BlobAppendInfo>> AppendBlockAsync(Stream content, AppendBlobAppendBlockOptions options = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Append blob not found. Call Create first.", "BlobNotFound", null);

        if (options?.Conditions is { } conditions)
            _store.CheckConditions(sidecar, conditions.IfMatch, conditions.IfNoneMatch, mustExist: true, nameof(AppendBlock));

        var blobPath = _store.BlobPath(_blobName);
        long newLength;

        await using (var fs = new FileStream(blobPath, FileMode.Append, FileAccess.Write, FileShare.None, 81920, useAsync: true))
        {
            if (options?.Conditions?.IfAppendPositionEqual is { } expectedPos && fs.Position != expectedPos)
                throw new RequestFailedException(412, $"AppendPositionOffset mismatch. Expected {expectedPos}, actual {fs.Position}.", "AppendPositionConditionNotMet", null);

            await content.CopyToAsync(fs, cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
            newLength = fs.Length;
        }

        if (options?.Conditions?.IfMaxSizeLessThanOrEqual is { } maxSize && newLength > maxSize)
            throw new RequestFailedException(412, $"Blob size {newLength} exceeds MaxSize {maxSize}.", "MaxBlobSizeConditionNotMet", null);

        byte[] md5;
        using (var _md5 = MD5.Create())
            md5 = _md5.ComputeHash(await File.ReadAllBytesAsync(blobPath, cancellationToken).ConfigureAwait(false));
        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(newLength, now, md5);

        sidecar.Length = newLength;
        sidecar.ContentHashBase64 = Convert.ToBase64String(md5);
        sidecar.ETag = etag.ToString()!;
        sidecar.LastModifiedUtc = now;
        await _store.WriteSidecarAsync(_blobName, sidecar, cancellationToken).ConfigureAwait(false);

        var info = BlobsModelFactory.BlobAppendInfo(etag, now, md5, null, newLength.ToString(), 0, false, null!, null!);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobAppendInfo>> AppendBlockAsync(Stream content, byte[] transactionalContentHash, AppendBlobRequestConditions conditions, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => await AppendBlockAsync(content, new AppendBlobAppendBlockOptions { Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobAppendInfo> AppendBlock(Stream content, AppendBlobAppendBlockOptions options = default!, CancellationToken cancellationToken = default)
        => AppendBlockAsync(content, options, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Response<BlobAppendInfo> AppendBlock(Stream content, byte[] transactionalContentHash, AppendBlobRequestConditions conditions, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => AppendBlockAsync(content, transactionalContentHash, conditions, progressHandler, cancellationToken).GetAwaiter().GetResult();

    // ---- Shared blob operations (async = primary) ----

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.Exists(_blobName), StubResponse.Ok());

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => ExistsAsync(cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(Delete));
        _store.Delete(_blobName);
        return StubResponse.Accepted();
    }

    /// <inheritdoc/>
    public override Response Delete(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => DeleteAsync(snapshotsOption, conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new FileBlobClient(_serviceClient, _store.ContainerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetPropertiesAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
    {
        var blobClient = new FileBlobClient(_serviceClient, _store.ContainerName, _blobName);
        return await blobClient.DownloadContentAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(CancellationToken cancellationToken = default)
        => DownloadContentAsync(cancellationToken).GetAwaiter().GetResult();

    // ---- GenerateSasUri ----
    /// <inheritdoc/>
    public override Uri GenerateSasUri(global::Azure.Storage.Sas.BlobSasBuilder builder) => Uri;
    /// <inheritdoc/>
    public override Uri GenerateSasUri(global::Azure.Storage.Sas.BlobSasPermissions permissions, DateTimeOffset expiresOn) => Uri;

    // ---- OpenWrite ----
    /// <inheritdoc/>
    public override async Task<Stream> OpenWriteAsync(bool overwrite, AppendBlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        var blobClient = new FileBlobClient(_serviceClient, _store.ContainerName, _blobName);
        return await blobClient.OpenWriteAsync(overwrite, null, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Stream OpenWrite(bool overwrite, AppendBlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
        => OpenWriteAsync(overwrite, options, cancellationToken).GetAwaiter().GetResult();

    // ---- AppendBlockFromUri — downloads source and appends locally ----
    /// <summary>Appends a block by downloading content from the source URI.</summary>
    public new async Task<Response<BlobAppendInfo>> AppendBlockFromUriAsync(Uri sourceUri, AppendBlobAppendBlockFromUriOptions options = null!, CancellationToken cancellationToken = default)
    {
        await using var sourceStream = await ResolveUriToStreamAsync(sourceUri, cancellationToken).ConfigureAwait(false);
        return await AppendBlockAsync(sourceStream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
    /// <summary>Appends a block by downloading content from the source URI.</summary>
    public new Response<BlobAppendInfo> AppendBlockFromUri(Uri sourceUri, AppendBlobAppendBlockFromUriOptions options = null!, CancellationToken cancellationToken = default)
        => AppendBlockFromUriAsync(sourceUri, options, cancellationToken).GetAwaiter().GetResult();

    // ---- Seal (not virtual — shadow with new) ----
    /// <inheritdoc/>
    public new async Task<Response<BlobInfo>> SealAsync(AppendBlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Append blob not found.", "BlobNotFound", null);

        if (conditions is not null)
            _store.CheckConditions(sidecar, conditions.IfMatch, conditions.IfNoneMatch, mustExist: true, nameof(Seal));

        sidecar.IsSealed = true;
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;
        sidecar.ETag = ETagCalculator.Compute(sidecar.Length, sidecar.LastModifiedUtc, ReadOnlySpan<byte>.Empty).ToString()!;
        await _store.WriteSidecarAsync(_blobName, sidecar, cancellationToken).ConfigureAwait(false);

        var info = BlobsModelFactory.BlobInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc);
        return Response.FromValue(info, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public new Response<BlobInfo> Seal(AppendBlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
        => SealAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    private async Task<Stream> ResolveUriToStreamAsync(Uri uri, CancellationToken ct)
    {
        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length >= 2)
        {
            var srcContainer = segments[0];
            var srcBlob = Uri.UnescapeDataString(string.Join("/", segments.Skip(1)));
            var srcStore = new Internal.BlobStore(_serviceClient.BlobsRootPath, srcContainer, _serviceClient.Options);
            var srcPath = srcStore.BlobPath(srcBlob);
            if (File.Exists(srcPath))
                return new MemoryStream(await File.ReadAllBytesAsync(srcPath, ct).ConfigureAwait(false));
        }
        using var http = new HttpClient();
        var bytes = await http.GetByteArrayAsync(uri.ToString()).ConfigureAwait(false);
        return new MemoryStream(bytes);
    }
}
