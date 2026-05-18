using System.Security.Cryptography;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>In-memory drop-in replacement for <see cref="BlobClient"/>.</summary>
public class MemoryBlobClient : BlobClient
{
    internal readonly MemoryBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    internal MemoryBlobClient(MemoryBlobServiceClient serviceClient, string containerName, string blobName) : base()
    {
        _serviceClient = serviceClient;
        _containerName = containerName;
        _blobName = blobName;
    }

    /// <inheritdoc/>
    public override string Name => _blobName;
    /// <inheritdoc/>
    public override string BlobContainerName => _containerName;
    /// <inheritdoc/>
    public override string AccountName => _serviceClient.AccountName;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_serviceClient.Uri}{_containerName}/{System.Uri.EscapeDataString(_blobName)}");

    // ==== Async Upload (primary) ====

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, options, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), options, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, bool overwrite, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, null, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, bool overwrite, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);
    }

    // ==== Sync Upload (delegates) ====

    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(content, options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, CancellationToken ct = default) => UploadAsync(content, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, bool overwrite, CancellationToken ct = default) => UploadAsync(content, overwrite, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(BinaryData content, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(content, options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(BinaryData content, CancellationToken ct = default) => UploadAsync(content, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(BinaryData content, bool overwrite, CancellationToken ct = default) => UploadAsync(content, overwrite, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(path, options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, CancellationToken ct = default) => UploadAsync(path, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, bool overwrite, CancellationToken ct = default) => UploadAsync(path, overwrite, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders h, IDictionary<string, string> m, BlobRequestConditions c, IProgress<long>? p, AccessTier? a, StorageTransferOptions t, CancellationToken ct = default) => UploadAsync(content, h, m, c, p, a, t, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, BlobHttpHeaders h, IDictionary<string, string> m, BlobRequestConditions c, IProgress<long>? p, AccessTier? a, StorageTransferOptions t, CancellationToken ct = default) => UploadAsync(path, h, m, c, p, a, t, ct).GetAwaiter().GetResult();

    // ==== Async Download (primary) ====

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobDownloadOptions options, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(BlobDownloadOptions options = default!, CancellationToken cancellationToken = default)
        => await DownloadStreamingCoreAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);

    // ==== Sync Download (delegates) ====

    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(CancellationToken ct = default) => DownloadContentAsync(ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, CancellationToken ct = default) => DownloadContentAsync(conditions, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(BlobDownloadOptions options, CancellationToken ct = default) => DownloadContentAsync(options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadStreamingResult> DownloadStreaming(BlobDownloadOptions options = default!, CancellationToken ct = default) => DownloadStreamingAsync(options, ct).GetAwaiter().GetResult();

    // ==== Exists / Delete / GetProperties / SetMetadata / SetHttpHeaders ====

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            return Response.FromValue(false, StubResponse.Ok());
        return Response.FromValue(store.Blobs.ContainsKey(_blobName), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        lock (entry.Lock)
        {
            // Check IfMatch condition
            if (conditions?.IfMatch is not null && conditions.IfMatch != ETag.All)
            {
                if (conditions.IfMatch.ToString() != entry.ETag)
                    throw new RequestFailedException(412, "Condition not met.", "ConditionNotMet", null);
            }

            if (!store.Blobs.TryRemove(_blobName, out _))
                throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        }

        return StubResponse.Accepted();
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            return Response.FromValue(false, StubResponse.Ok());
        var removed = store.Blobs.TryRemove(_blobName, out _);
        return Response.FromValue(removed, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var blobType = entry.BlobType switch
        {
            "Append" => BlobType.Append,
            "Page" => BlobType.Page,
            _ => BlobType.Block
        };

        var props = BlobsModelFactory.BlobProperties(
            lastModified: entry.LastModified, createdOn: entry.CreatedOn,
            metadata: entry.CloneMetadata(),
            blobType: blobType,
            contentLength: entry.Content.Length,
            contentType: entry.ContentType,
            eTag: new ETag(entry.ETag), contentHash: entry.ContentHash,
            contentEncoding: entry.ContentEncoding, contentDisposition: entry.ContentDisposition,
            contentLanguage: entry.ContentLanguage, cacheControl: entry.CacheControl,
            accessTier: entry.AccessTier, blobSequenceNumber: entry.SequenceNumber);
        return Response.FromValue(props, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobInfo>> SetMetadataAsync(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        lock (entry.Lock)
        {
            if (conditions?.IfMatch is not null && conditions.IfMatch != ETag.All)
            {
                if (conditions.IfMatch.ToString() != entry.ETag)
                    throw new RequestFailedException(412, "Condition not met.", "ConditionNotMet", null);
            }

            entry.Metadata = new Dictionary<string, string>(metadata);
            entry.Touch();
        }

        return Response.FromValue(BlobsModelFactory.BlobInfo(new ETag(entry.ETag), entry.LastModified), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobInfo>> SetHttpHeadersAsync(BlobHttpHeaders httpHeaders, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        lock (entry.Lock)
        {
            if (httpHeaders.ContentType is not null) entry.ContentType = httpHeaders.ContentType;
            if (httpHeaders.ContentEncoding is not null) entry.ContentEncoding = httpHeaders.ContentEncoding;
            if (httpHeaders.ContentLanguage is not null) entry.ContentLanguage = httpHeaders.ContentLanguage;
            if (httpHeaders.ContentDisposition is not null) entry.ContentDisposition = httpHeaders.ContentDisposition;
            if (httpHeaders.CacheControl is not null) entry.CacheControl = httpHeaders.CacheControl;
            entry.Touch();
        }

        return Response.FromValue(BlobsModelFactory.BlobInfo(new ETag(entry.ETag), entry.LastModified), StubResponse.Ok());
    }

    // ==== Sync delegates ====

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken ct = default) => ExistsAsync(ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response Delete(DeleteSnapshotsOption s = default, BlobRequestConditions c = default!, CancellationToken ct = default) => DeleteAsync(s, c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(DeleteSnapshotsOption s = default, BlobRequestConditions c = default!, CancellationToken ct = default) => DeleteIfExistsAsync(s, c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobProperties> GetProperties(BlobRequestConditions c = default!, CancellationToken ct = default) => GetPropertiesAsync(c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobInfo> SetMetadata(IDictionary<string, string> m, BlobRequestConditions c = default!, CancellationToken ct = default) => SetMetadataAsync(m, c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobInfo> SetHttpHeaders(BlobHttpHeaders h, BlobRequestConditions c = default!, CancellationToken ct = default) => SetHttpHeadersAsync(h, c, ct).GetAwaiter().GetResult();

    // ==== Core async helpers ====

    internal async Task<Response<BlobContentInfo>> UploadCoreAsync(Stream content, BlobUploadOptions? options, CancellationToken ct = default)
    {
        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, ct).ConfigureAwait(false);
        var data = ms.ToArray();
        byte[] md5;
        using (var hasher = MD5.Create()) { md5 = hasher.ComputeHash(data); }
        var now = DateTimeOffset.UtcNow;
        var etag = BlobEntry.NewETag();

        var conditions = options?.Conditions;

        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        // Try to get existing entry for conditional checks
        store.Blobs.TryGetValue(_blobName, out var existing);

        if (existing is not null)
        {
            lock (existing.Lock)
            {
                // IfNoneMatch = * means blob must not exist
                if (conditions?.IfNoneMatch == ETag.All)
                    throw new RequestFailedException(409, "Blob already exists.", "BlobAlreadyExists", null);

                // IfMatch: ETag must match current
                if (conditions?.IfMatch is not null && conditions.IfMatch != ETag.All)
                {
                    if (conditions.IfMatch.ToString() != existing.ETag)
                        throw new RequestFailedException(412, "Condition not met.", "ConditionNotMet", null);
                }

                // Update in place
                existing.Content = data;
                existing.BlobType = "Block";
                existing.ContentType = options?.HttpHeaders?.ContentType ?? "application/octet-stream";
                existing.ContentEncoding = options?.HttpHeaders?.ContentEncoding;
                existing.ContentLanguage = options?.HttpHeaders?.ContentLanguage;
                existing.ContentDisposition = options?.HttpHeaders?.ContentDisposition;
                existing.CacheControl = options?.HttpHeaders?.CacheControl;
                existing.ContentHash = md5;
                existing.ETag = etag;
                existing.LastModified = now;
                if (options?.Metadata is not null)
                    existing.Metadata = new Dictionary<string, string>(options.Metadata);
                // Preserve Tags on overwrite (like Sqlite impl)
            }
        }
        else
        {
            // IfNoneMatch = * is fine (blob doesn't exist)
            // IfMatch requires blob to exist
            if (conditions?.IfMatch is not null && conditions.IfMatch != ETag.All)
                throw new RequestFailedException(412, "Condition not met.", "ConditionNotMet", null);

            var entry = new BlobEntry
            {
                Content = data,
                BlobType = "Block",
                ContentType = options?.HttpHeaders?.ContentType ?? "application/octet-stream",
                ContentEncoding = options?.HttpHeaders?.ContentEncoding,
                ContentLanguage = options?.HttpHeaders?.ContentLanguage,
                ContentDisposition = options?.HttpHeaders?.ContentDisposition,
                CacheControl = options?.HttpHeaders?.CacheControl,
                ContentHash = md5,
                ETag = etag,
                CreatedOn = now,
                LastModified = now,
                Metadata = options?.Metadata is not null ? new Dictionary<string, string>(options.Metadata) : null,
            };

            // Use AddOrUpdate in case of race — last writer wins (consistent with overwrite semantics)
            store.Blobs[_blobName] = entry;
        }

        return Response.FromValue(BlobsModelFactory.BlobContentInfo(new ETag(etag), now, md5, null!, null!, null!, 0), StubResponse.Created());
    }

    private async Task<Response<BlobDownloadResult>> DownloadContentCoreAsync(BlobRequestConditions? conditions, CancellationToken ct)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        byte[] content;
        string etag;
        string contentType;
        string? contentEncoding, contentLanguage, contentDisposition, cacheControl;
        byte[]? contentHash;
        DateTimeOffset createdOn, lastModified;
        long length;
        IDictionary<string, string>? metadata;
        string blobTypeStr;

        lock (entry.Lock)
        {
            content = entry.CloneContent();
            etag = entry.ETag;
            contentType = entry.ContentType;
            contentEncoding = entry.ContentEncoding;
            contentLanguage = entry.ContentLanguage;
            contentDisposition = entry.ContentDisposition;
            cacheControl = entry.CacheControl;
            contentHash = entry.ContentHash;
            createdOn = entry.CreatedOn;
            lastModified = entry.LastModified;
            length = entry.Content.Length;
            metadata = entry.CloneMetadata();
            blobTypeStr = entry.BlobType;
        }

        var blobType = blobTypeStr switch { "Append" => BlobType.Append, "Page" => BlobType.Page, _ => BlobType.Block };

        var details = BlobsModelFactory.BlobDownloadDetails(
            blobType: blobType, contentLength: length, contentType: contentType,
            contentHash: contentHash, lastModified: lastModified, metadata: metadata,
            contentEncoding: contentEncoding, contentDisposition: contentDisposition,
            contentLanguage: contentLanguage, cacheControl: cacheControl,
            createdOn: createdOn, eTag: new ETag(etag));

        return Response.FromValue(BlobsModelFactory.BlobDownloadResult(new BinaryData(content), details), StubResponse.Ok());
    }

    private async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingCoreAsync(BlobRequestConditions? conditions, CancellationToken ct)
    {
        var result = await DownloadContentCoreAsync(conditions, ct).ConfigureAwait(false);
        var stream = result.Value.Content.ToStream();
        var details = result.Value.Details;
        return Response.FromValue(BlobsModelFactory.BlobDownloadStreamingResult(stream, details), StubResponse.Ok());
    }

    // ==== GenerateSasUri ====

    /// <inheritdoc/>
    public override Uri GenerateSasUri(global::Azure.Storage.Sas.BlobSasBuilder builder) => Uri;
    /// <inheritdoc/>
    public override Uri GenerateSasUri(global::Azure.Storage.Sas.BlobSasPermissions permissions, DateTimeOffset expiresOn) => Uri;

    // ==== OpenRead / OpenWrite ====

    /// <inheritdoc/>
    public override async Task<Stream> OpenReadAsync(long position = 0, int? bufferSize = null, BlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
    {
        var result = await DownloadContentCoreAsync(conditions, cancellationToken).ConfigureAwait(false);
        var stream = result.Value.Content.ToStream();
        if (position > 0) stream.Seek(position, SeekOrigin.Begin);
        return stream;
    }

    /// <inheritdoc/>
    public override Stream OpenRead(long position = 0, int? bufferSize = null, BlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
        => OpenReadAsync(position, bufferSize, conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Stream> OpenReadAsync(BlobOpenReadOptions options, CancellationToken cancellationToken = default)
        => await OpenReadAsync(options?.Position ?? 0, options?.BufferSize, options?.Conditions!, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Stream OpenRead(BlobOpenReadOptions options, CancellationToken cancellationToken = default)
        => OpenReadAsync(options, cancellationToken).GetAwaiter().GetResult();

    // ==== OpenWrite ====

    /// <inheritdoc/>
    public override async Task<Stream> OpenWriteAsync(bool overwrite, BlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (!overwrite)
        {
            var exists = await ExistsAsync(cancellationToken).ConfigureAwait(false);
            if (exists.Value)
                throw new RequestFailedException(409, "Blob already exists and overwrite is false.", "BlobAlreadyExists", null);
        }
        return new MemoryCommitOnCloseStream(this, options?.HttpHeaders, options?.Metadata);
    }

    /// <inheritdoc/>
    public override Stream OpenWrite(bool overwrite, BlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
        => OpenWriteAsync(overwrite, options, cancellationToken).GetAwaiter().GetResult();

    // ==== DownloadStreaming additional overloads ====

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken cancellationToken = default)
        => await DownloadStreamingCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobDownloadStreamingResult> DownloadStreaming(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken cancellationToken = default)
        => DownloadStreamingAsync(range, conditions, rangeGetContentHash, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, IProgress<long>? progressHandler = null, CancellationToken cancellationToken = default)
        => await DownloadStreamingCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobDownloadStreamingResult> DownloadStreaming(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, IProgress<long>? progressHandler = null, CancellationToken cancellationToken = default)
        => DownloadStreamingAsync(range, conditions, rangeGetContentHash, progressHandler, cancellationToken).GetAwaiter().GetResult();

    // ==== DownloadContent additional overloads ====

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, IProgress<long>? progressHandler, HttpRange range, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, IProgress<long>? progressHandler, HttpRange range, CancellationToken cancellationToken = default)
        => DownloadContentAsync(conditions, progressHandler, range, cancellationToken).GetAwaiter().GetResult();

    // ==== DownloadTo ====

    /// <inheritdoc/>
    public override async Task<Response> DownloadToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        var result = await DownloadContentCoreAsync(null, cancellationToken).ConfigureAwait(false);
        var bytes = result.Value.Content.ToArray();
        await destination.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        return StubResponse.Ok();
    }

    /// <inheritdoc/>
    public override async Task<Response> DownloadToAsync(string path, CancellationToken cancellationToken = default)
    {
        await using var dest = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true);
        return await DownloadToAsync(dest, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response DownloadTo(Stream destination, CancellationToken ct = default) => DownloadToAsync(destination, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response DownloadTo(string path, CancellationToken ct = default) => DownloadToAsync(path, ct).GetAwaiter().GetResult();

    // ==== Download (deprecated BlobDownloadInfo) ====

    private async Task<Response<BlobDownloadInfo>> DownloadCoreAsync(BlobRequestConditions? conditions, CancellationToken ct)
    {
        var streamingResult = await DownloadStreamingCoreAsync(conditions, ct).ConfigureAwait(false);
        var d = streamingResult.Value.Details;
        var info = BlobsModelFactory.BlobDownloadInfo(
            lastModified: d.LastModified, blobSequenceNumber: d.BlobSequenceNumber,
            blobType: d.BlobType, contentCrc64: null, contentLanguage: d.ContentLanguage,
            copyStatusDescription: d.CopyStatusDescription, copyId: d.CopyId,
            copyProgress: d.CopyProgress, copySource: d.CopySource, copyStatus: d.CopyStatus,
            contentDisposition: d.ContentDisposition, leaseDuration: d.LeaseDuration,
            cacheControl: d.CacheControl, leaseState: d.LeaseState, contentEncoding: d.ContentEncoding,
            leaseStatus: d.LeaseStatus, contentHash: d.ContentHash, acceptRanges: d.AcceptRanges,
            eTag: d.ETag, isServerEncrypted: d.IsServerEncrypted, contentRange: d.ContentRange,
            encryptionKeySha256: d.EncryptionKeySha256, contentLength: d.ContentLength,
            contentType: d.ContentType, content: streamingResult.Value.Content, metadata: d.Metadata);
        return Response.FromValue(info, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadInfo>> DownloadAsync()
        => await DownloadCoreAsync(null, default).ConfigureAwait(false);
    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadInfo>> DownloadAsync(CancellationToken ct)
        => await DownloadCoreAsync(null, ct).ConfigureAwait(false);
    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadInfo>> DownloadAsync(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken ct = default)
        => await DownloadCoreAsync(conditions, ct).ConfigureAwait(false);
    /// <inheritdoc/>
    public override Response<BlobDownloadInfo> Download() => DownloadAsync().GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadInfo> Download(CancellationToken ct) => DownloadAsync(ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadInfo> Download(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken ct = default) => DownloadAsync(range, conditions, rangeGetContentHash, ct).GetAwaiter().GetResult();

    // ==== WithSnapshot / WithVersion / WithCustomerProvidedKey / WithEncryptionScope ====

    /// <inheritdoc/>
    public new BlobBaseClient WithSnapshot(string snapshot) => this;
    /// <inheritdoc/>
    public new BlobBaseClient WithVersion(string versionId) => this;
    /// <inheritdoc/>
    public new BlobBaseClient WithCustomerProvidedKey(CustomerProvidedKey? key) => this;
    /// <inheritdoc/>
    public new BlobBaseClient WithEncryptionScope(string scope) => this;

    // ==== SetAccessTier ====

    /// <inheritdoc/>
    public override async Task<Response> SetAccessTierAsync(AccessTier tier, BlobRequestConditions conditions = null!, RehydratePriority? priority = null, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        lock (entry.Lock)
        {
            entry.AccessTier = tier.ToString();
            entry.Touch();
        }
        return StubResponse.Ok();
    }

    /// <inheritdoc/>
    public override Response SetAccessTier(AccessTier tier, BlobRequestConditions conditions = null!, RehydratePriority? priority = null, CancellationToken ct = default)
        => SetAccessTierAsync(tier, conditions, priority, ct).GetAwaiter().GetResult();

    // ==== CreateSnapshot ====

    /// <inheritdoc/>
    public override async Task<Response<BlobSnapshotInfo>> CreateSnapshotAsync(IDictionary<string, string>? metadata = null, BlobRequestConditions conditions = null!, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var snapshotId = DateTimeOffset.UtcNow.ToString("yyyyMMddTHHmmssfffZ");

        // Snapshot is a no-op clone in memory (we don't maintain snapshot storage)
        var info = BlobsModelFactory.BlobSnapshotInfo(snapshot: snapshotId, eTag: new ETag(entry.ETag), lastModified: entry.LastModified, isServerEncrypted: false);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobSnapshotInfo> CreateSnapshot(IDictionary<string, string>? metadata = null, BlobRequestConditions conditions = null!, CancellationToken ct = default)
        => CreateSnapshotAsync(metadata, conditions, ct).GetAwaiter().GetResult();

    // ==== StartCopyFromUri ====

    /// <inheritdoc/>
    public override async Task<CopyFromUriOperation> StartCopyFromUriAsync(Uri source, BlobCopyFromUriOptions options = null!, CancellationToken ct = default)
    {
        Stream sourceStream;

        // Try to resolve from the same service client's in-memory containers
        if (TryResolveBlobFromUri(source, out var srcContainer, out var srcBlob)
            && _serviceClient.Containers.TryGetValue(srcContainer, out var srcStore)
            && srcStore.Blobs.TryGetValue(srcBlob, out var srcEntry))
        {
            sourceStream = new MemoryStream(srcEntry.CloneContent());
        }
        else
        {
            using var http = new HttpClient();
            var bytes = await http.GetByteArrayAsync(source.ToString()).ConfigureAwait(false);
            sourceStream = new MemoryStream(bytes);
        }

        await using (sourceStream)
        {
            await UploadCoreAsync(sourceStream, new BlobUploadOptions
            {
                Metadata = options?.Metadata,
            }, ct).ConfigureAwait(false);
        }

        var copyId = Guid.NewGuid().ToString();
        return new CopyFromUriOperation(copyId, this);
    }

    /// <inheritdoc/>
    public override CopyFromUriOperation StartCopyFromUri(Uri source, BlobCopyFromUriOptions options = null!, CancellationToken ct = default)
        => StartCopyFromUriAsync(source, options, ct).GetAwaiter().GetResult();

    // ==== AbortCopyFromUri ====

    /// <inheritdoc/>
    public override async Task<Response> AbortCopyFromUriAsync(string copyId, BlobRequestConditions conditions = null!, CancellationToken ct = default)
        => StubResponse.Ok();
    /// <inheritdoc/>
    public override Response AbortCopyFromUri(string copyId, BlobRequestConditions conditions = null!, CancellationToken ct = default)
        => AbortCopyFromUriAsync(copyId, conditions, ct).GetAwaiter().GetResult();

    // ==== Undelete ====

    /// <inheritdoc/>
    public override async Task<Response> UndeleteAsync(CancellationToken ct = default)
        => StubResponse.Ok();
    /// <inheritdoc/>
    public override Response Undelete(CancellationToken ct = default)
        => UndeleteAsync(ct).GetAwaiter().GetResult();

    // ==== Tags ====

    /// <inheritdoc/>
    public override async Task<Response<GetBlobTagResult>> GetTagsAsync(BlobRequestConditions conditions = null!, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var tags = entry.CloneTags() ?? new Dictionary<string, string>();
        var result = BlobsModelFactory.GetBlobTagResult(tags);
        return Response.FromValue(result, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<GetBlobTagResult> GetTags(BlobRequestConditions conditions = null!, CancellationToken ct = default)
        => GetTagsAsync(conditions, ct).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> SetTagsAsync(IDictionary<string, string> tags, BlobRequestConditions conditions = null!, CancellationToken ct = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        lock (entry.Lock)
        {
            entry.Tags = new Dictionary<string, string>(tags);
            entry.Touch();
        }
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override Response SetTags(IDictionary<string, string> tags, BlobRequestConditions conditions = null!, CancellationToken ct = default)
        => SetTagsAsync(tags, conditions, ct).GetAwaiter().GetResult();

    // ==== SyncCopyFromUri ====

    /// <inheritdoc/>
    public override async Task<Response<BlobCopyInfo>> SyncCopyFromUriAsync(Uri source, BlobCopyFromUriOptions options = null!, CancellationToken ct = default)
    {
        await StartCopyFromUriAsync(source, options != null ? new BlobCopyFromUriOptions
        {
            Metadata = options.Metadata,
            SourceConditions = options.SourceConditions,
            DestinationConditions = options.DestinationConditions,
        } : null!, ct).ConfigureAwait(false);

        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var info = BlobsModelFactory.BlobCopyInfo(
            eTag: new ETag(entry.ETag),
            lastModified: entry.LastModified,
            copyId: Guid.NewGuid().ToString(),
            copyStatus: CopyStatus.Success);
        return Response.FromValue(info, StubResponse.Accepted());
    }

    /// <inheritdoc/>
    public override Response<BlobCopyInfo> SyncCopyFromUri(Uri source, BlobCopyFromUriOptions options = null!, CancellationToken ct = default)
        => SyncCopyFromUriAsync(source, options, ct).GetAwaiter().GetResult();

    // ==== Internal helpers ====

    private bool TryResolveBlobFromUri(Uri uri, out string container, out string blob)
    {
        container = blob = null!;
        // Expected format: https://{account}.blob.storage.memory.local/{container}/{blob}
        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 2) return false;
        container = segments[0];
        blob = Uri.UnescapeDataString(string.Join("/", segments.Skip(1)));
        return true;
    }

    // ==== Lease ====

    /// <inheritdoc/>
    protected override BlobLeaseClient GetBlobLeaseClientCore(string leaseId)
        => new MemoryBlobLeaseClient(this, leaseId);
}

// ==== CommitOnClose Stream ====

internal sealed class MemoryCommitOnCloseStream : Stream
{
    private readonly MemoryStream _buffer = new();
    private readonly MemoryBlobClient _client;
    private readonly BlobHttpHeaders? _headers;
    private readonly IDictionary<string, string>? _metadata;
    private bool _committed;

    public MemoryCommitOnCloseStream(MemoryBlobClient client, BlobHttpHeaders? headers, IDictionary<string, string>? metadata)
    {
        _client = client;
        _headers = headers;
        _metadata = metadata;
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _buffer.Length;
    public override long Position { get => _buffer.Position; set => _buffer.Position = value; }

    public override void Write(byte[] buffer, int offset, int count) => _buffer.Write(buffer, offset, count);
    public override void Write(ReadOnlySpan<byte> buffer) => _buffer.Write(buffer);
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => _buffer.WriteAsync(buffer, offset, count, ct);
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _buffer.WriteAsync(buffer, ct);
    public override void WriteByte(byte value) => _buffer.WriteByte(value);

    public override void Flush() => _buffer.Flush();
    public override Task FlushAsync(CancellationToken ct) => _buffer.FlushAsync(ct);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    private void Commit()
    {
        if (_committed) return;
        _committed = true;

        _buffer.Position = 0;
        _client.UploadCoreAsync(_buffer, new BlobUploadOptions
        {
            HttpHeaders = _headers,
            Metadata = _metadata,
        }).GetAwaiter().GetResult();
    }

    private async Task CommitAsync()
    {
        if (_committed) return;
        _committed = true;

        _buffer.Position = 0;
        await _client.UploadCoreAsync(_buffer, new BlobUploadOptions
        {
            HttpHeaders = _headers,
            Metadata = _metadata,
        }).ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Commit();
        _buffer.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await CommitAsync().ConfigureAwait(false);
        await _buffer.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
