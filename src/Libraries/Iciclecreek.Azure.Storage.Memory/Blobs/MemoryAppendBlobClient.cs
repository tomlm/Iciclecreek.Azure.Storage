using System.Security.Cryptography;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>In-memory drop-in replacement for <see cref="AppendBlobClient"/>.</summary>
public class MemoryAppendBlobClient : AppendBlobClient
{
    internal readonly MemoryBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    internal MemoryAppendBlobClient(MemoryBlobServiceClient serviceClient, string containerName, string blobName) : base()
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

    // ---- Create ----

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var now = DateTimeOffset.UtcNow;
        var etag = BlobEntry.NewETag();

        // Check IfNoneMatch condition
        if (options?.Conditions?.IfNoneMatch == ETag.All)
        {
            if (store.Blobs.ContainsKey(_blobName))
                throw new RequestFailedException(409, "Blob already exists.", "BlobAlreadyExists", null);
        }

        var entry = new BlobEntry
        {
            Content = Array.Empty<byte>(),
            BlobType = "Append",
            ContentType = options?.HttpHeaders?.ContentType ?? "application/octet-stream",
            ContentEncoding = options?.HttpHeaders?.ContentEncoding,
            ETag = etag,
            CreatedOn = now,
            LastModified = now,
            Metadata = options?.Metadata is not null ? new Dictionary<string, string>(options.Metadata) : null,
        };

        store.Blobs[_blobName] = entry;

        var info = BlobsModelFactory.BlobContentInfo(new ETag(etag), now, null, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => await CreateAsync(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        if (store.Blobs.ContainsKey(_blobName))
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

    // ---- AppendBlock ----

    /// <inheritdoc/>
    public override async Task<Response<BlobAppendInfo>> AppendBlockAsync(Stream content, AppendBlobAppendBlockOptions options = default!, CancellationToken cancellationToken = default)
    {
        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        var appendData = ms.ToArray();

        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Append blob not found. Call Create first.", "BlobNotFound", null);
        if (entry.BlobType != "Append")
            throw new RequestFailedException(409, "Blob is not an append blob.", "InvalidBlobType", null);

        byte[] newContent;
        string etag;
        DateTimeOffset now;
        long newLength;

        lock (entry.Lock)
        {
            newContent = new byte[entry.Content.Length + appendData.Length];
            Buffer.BlockCopy(entry.Content, 0, newContent, 0, entry.Content.Length);
            Buffer.BlockCopy(appendData, 0, newContent, entry.Content.Length, appendData.Length);

            entry.Content = newContent;
            using (var hasher = MD5.Create()) { entry.ContentHash = hasher.ComputeHash(newContent); }
            entry.Touch();

            etag = entry.ETag;
            now = entry.LastModified;
            newLength = newContent.Length;
        }

        var info = BlobsModelFactory.BlobAppendInfo(new ETag(etag), now, entry.ContentHash, null, newLength.ToString(), 0, false, null!, null!);
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

    // ---- Shared blob operations ----

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.ExistsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => ExistsAsync(cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response Delete(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => DeleteAsync(snapshotsOption, conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetPropertiesAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
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
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.OpenWriteAsync(overwrite, null, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Stream OpenWrite(bool overwrite, AppendBlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
        => OpenWriteAsync(overwrite, options, cancellationToken).GetAwaiter().GetResult();

    // ---- Seal ----
    /// <inheritdoc/>
    public new async Task<Response<BlobInfo>> SealAsync(AppendBlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Append blob not found.", "BlobNotFound", null);

        lock (entry.Lock)
        {
            entry.Touch();
        }

        var info = BlobsModelFactory.BlobInfo(new ETag(entry.ETag), entry.LastModified);
        return Response.FromValue(info, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public new Response<BlobInfo> Seal(AppendBlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
        => SealAsync(conditions, cancellationToken).GetAwaiter().GetResult();
}
