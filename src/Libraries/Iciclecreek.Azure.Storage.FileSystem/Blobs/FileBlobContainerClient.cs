using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>Filesystem-backed drop-in replacement for <see cref="Azure.Storage.Blobs.BlobContainerClient"/>. Each container is a subdirectory under the account's blobs path.</summary>
public class FileBlobContainerClient : BlobContainerClient
{
    internal readonly BlobStore _store;
    internal readonly FileBlobServiceClient _serviceClient;

    internal FileBlobContainerClient(FileBlobServiceClient serviceClient, string containerName) : base()
    {
        _serviceClient = serviceClient;
        _store = new BlobStore(serviceClient.BlobsRootPath, containerName, serviceClient.Options);
    }

    /// <inheritdoc/>
    public override string Name => _store.ContainerName;
    /// <inheritdoc/>
    public override string AccountName => _serviceClient.AccountName;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_serviceClient.Uri}{_store.ContainerName}");

    /// <summary>Absolute filesystem path to the container directory.</summary>
    public string ContainerPath => _store.ContainerPath;

    // ---- Create / Delete / Exists ----

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        if (_store.ContainerExists())
            throw new RequestFailedException(409, "Container already exists.", "ContainerAlreadyExists", null);
        _store.CreateContainer();
        if (metadata is not null && metadata.Count > 0)
            PersistContainerMetadata(metadata);
        var info = BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Create(publicAccessType, metadata, cancellationToken);

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        if (_store.ContainerExists())
            return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Ok());
        _store.CreateContainer();
        if (metadata is not null && metadata.Count > 0)
            PersistContainerMetadata(metadata);
        return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => CreateIfNotExists(publicAccessType, metadata, cancellationToken);

    /// <inheritdoc/>
    public override Response Delete(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.DeleteContainer())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        return StubResponse.Accepted();
    }

    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => Response.FromValue(_store.DeleteContainer(), StubResponse.Ok());

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.ContainerExists(), StubResponse.Ok());

    // ---- Async Create / Delete / Exists ----

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(publicAccessType, metadata, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(publicAccessType, metadata, encryptionScopeOptions, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(publicAccessType, metadata, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(publicAccessType, metadata, encryptionScopeOptions, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(conditions, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(conditions, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    // ---- GetBlobClient ----

    /// <inheritdoc/>
    public override BlobClient GetBlobClient(string blobName) => new FileBlobClient(_serviceClient, _store.ContainerName, blobName);

    /// <inheritdoc/>
    protected override BlockBlobClient GetBlockBlobClientCore(string blobName) => new FileBlockBlobClient(_serviceClient, _store.ContainerName, blobName);

    /// <inheritdoc/>
    protected override AppendBlobClient GetAppendBlobClientCore(string blobName) => new FileAppendBlobClient(_serviceClient, _store.ContainerName, blobName);

    /// <inheritdoc/>
    protected override PageBlobClient GetPageBlobClientCore(string blobName) => new FilePageBlobClient(_serviceClient, _store.ContainerName, blobName);

    /// <summary>Strongly-typed alias for <see cref="GetPageBlobClientCore"/>.</summary>
    public FilePageBlobClient GetFilePageBlobClient(string blobName) => new FilePageBlobClient(_serviceClient, _store.ContainerName, blobName);

    /// <inheritdoc/>
    protected override BlobLeaseClient GetBlobLeaseClientCore(string leaseId) => new FileContainerLeaseClient(this, leaseId);

    /// <summary>Strongly-typed alias for <see cref="GetBlockBlobClientCore"/>.</summary>
    public FileBlockBlobClient GetFileBlockBlobClient(string blobName) => new FileBlockBlobClient(_serviceClient, _store.ContainerName, blobName);

    /// <summary>Strongly-typed alias for <see cref="GetAppendBlobClientCore"/>.</summary>
    public FileAppendBlobClient GetFileAppendBlobClient(string blobName) => new FileAppendBlobClient(_serviceClient, _store.ContainerName, blobName);

    // ---- UploadBlob ----

    /// <inheritdoc/>
    public override Response<BlobContentInfo> UploadBlob(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<BlobContentInfo> UploadBlob(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    // ---- DeleteBlob ----

    /// <inheritdoc/>
    public override Response DeleteBlob(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return client.Delete(snapshotsOption, conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<bool> DeleteBlobIfExists(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return client.DeleteIfExists(snapshotsOption, conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteBlobAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return await client.DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteBlobIfExistsAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_serviceClient, _store.ContainerName, blobName);
        return await client.DeleteIfExistsAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    // ---- GetBlobs / GetBlobsByHierarchy ----

    /// <inheritdoc/>
    public override Pageable<BlobItem> GetBlobs(BlobTraits traits = default, BlobStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
    {
        var items = new List<BlobItem>();
        foreach (var (blobName, fullPath, fi) in _store.EnumerateBlobs(prefix))
        {
            BlobSidecar? sidecar = null;
            IDictionary<string, string>? metadata = null;
            if ((traits & BlobTraits.Metadata) != 0)
            {
                sidecar = _store.ReadSidecarAsync(blobName).GetAwaiter().GetResult();
                metadata = sidecar?.Metadata;
            }
            sidecar ??= _store.ReadSidecarAsync(blobName).GetAwaiter().GetResult();

            var props = BlobsModelFactory.BlobItemProperties(
                accessTierInferred: true,
                contentLength: fi.Length,
                contentType: sidecar?.ContentType,
                eTag: sidecar is not null ? new ETag(sidecar.ETag) : default,
                lastModified: sidecar?.LastModifiedUtc ?? fi.LastWriteTimeUtc,
                blobType: sidecar?.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block,
                createdOn: sidecar?.CreatedOnUtc ?? fi.CreationTimeUtc);

            items.Add(BlobsModelFactory.BlobItem(
                name: blobName,
                deleted: false,
                properties: props,
                metadata: metadata));
        }
        return new StaticPageable<BlobItem>(items);
    }

    /// <inheritdoc/>
    public override AsyncPageable<BlobItem> GetBlobsAsync(BlobTraits traits = default, BlobStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobItem>(GetBlobs(traits, states, prefix, cancellationToken));

    /// <inheritdoc/>
    public override Pageable<BlobHierarchyItem> GetBlobsByHierarchy(BlobTraits traits = default, BlobStates states = default, string? delimiter = "/", string? prefix = default, CancellationToken cancellationToken = default)
    {
        delimiter ??= "/";
        var items = new List<BlobHierarchyItem>();
        var prefixes = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (blobName, fullPath, fi) in _store.EnumerateBlobs(prefix))
        {
            var relative = prefix is not null ? blobName[prefix.Length..] : blobName;
            var delimIndex = relative.IndexOf(delimiter, StringComparison.Ordinal);
            if (delimIndex >= 0)
            {
                var pfx = (prefix ?? "") + relative[..(delimIndex + delimiter.Length)];
                if (prefixes.Add(pfx))
                    items.Add(BlobsModelFactory.BlobHierarchyItem(pfx, null!));
            }
            else
            {
                var sidecar = _store.ReadSidecarAsync(blobName).GetAwaiter().GetResult();
                var props = BlobsModelFactory.BlobItemProperties(
                    accessTierInferred: true,
                    contentLength: fi.Length,
                    contentType: sidecar?.ContentType,
                    eTag: sidecar is not null ? new ETag(sidecar.ETag) : default,
                    lastModified: sidecar?.LastModifiedUtc ?? fi.LastWriteTimeUtc,
                    blobType: sidecar?.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block);

                var blob = BlobsModelFactory.BlobItem(name: blobName, deleted: false, properties: props);
                items.Add(BlobsModelFactory.BlobHierarchyItem(null!, blob));
            }
        }
        return new StaticPageable<BlobHierarchyItem>(items);
    }

    /// <inheritdoc/>
    public override AsyncPageable<BlobHierarchyItem> GetBlobsByHierarchyAsync(BlobTraits traits = default, BlobStates states = default, string? delimiter = "/", string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobHierarchyItem>(GetBlobsByHierarchy(traits, states, delimiter, prefix, cancellationToken));

    // ---- GetBlobs / GetBlobsByHierarchy — Options-object overloads ----

    /// <inheritdoc/>
    public override Pageable<BlobItem> GetBlobs(GetBlobsOptions options, CancellationToken cancellationToken = default)
        => GetBlobs(options?.Traits ?? default, options?.States ?? default, options?.Prefix, cancellationToken);

    /// <inheritdoc/>
    public override AsyncPageable<BlobItem> GetBlobsAsync(GetBlobsOptions options, CancellationToken cancellationToken = default)
        => GetBlobsAsync(options?.Traits ?? default, options?.States ?? default, options?.Prefix, cancellationToken);

    /// <inheritdoc/>
    public override Pageable<BlobHierarchyItem> GetBlobsByHierarchy(GetBlobsByHierarchyOptions options, CancellationToken cancellationToken = default)
        => GetBlobsByHierarchy(options?.Traits ?? default, options?.States ?? default, options?.Delimiter ?? "/", options?.Prefix, cancellationToken);

    /// <inheritdoc/>
    public override AsyncPageable<BlobHierarchyItem> GetBlobsByHierarchyAsync(GetBlobsByHierarchyOptions options, CancellationToken cancellationToken = default)
        => GetBlobsByHierarchyAsync(options?.Traits ?? default, options?.States ?? default, options?.Delimiter ?? "/", options?.Prefix, cancellationToken);

    // ---- Container Properties ----

    /// <inheritdoc/>
    public override Response<BlobContainerProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.ContainerExists())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        IDictionary<string, string>? metadata = null;
        var metaPath = Path.Combine(_store.ContainerPath, "_container.meta.json");
        if (File.Exists(metaPath))
        {
            var json = File.ReadAllText(metaPath);
            metadata = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(json, _serviceClient.Options.JsonSerializerOptions);
        }
        var props = BlobsModelFactory.BlobContainerProperties(lastModified: DateTimeOffset.UtcNow, eTag: new ETag("\"0x0\""), metadata: metadata);
        return Response.FromValue(props, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetProperties(conditions, cancellationToken);

    // ---- Item 5: Container SetMetadata ----

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> SetMetadata(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.ContainerExists())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        var metaPath = Path.Combine(_store.ContainerPath, "_container.meta.json");
        var json = System.Text.Json.JsonSerializer.Serialize(metadata, _serviceClient.Options.JsonSerializerOptions);
        Iciclecreek.Azure.Storage.FileSystem.Internal.AtomicFile.WriteAllTextAsync(metaPath, json).GetAwaiter().GetResult();
        return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerInfo>> SetMetadataAsync(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.ContainerExists())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        var metaPath = Path.Combine(_store.ContainerPath, "_container.meta.json");
        var json = System.Text.Json.JsonSerializer.Serialize(metadata, _serviceClient.Options.JsonSerializerOptions);
        await Iciclecreek.Azure.Storage.FileSystem.Internal.AtomicFile.WriteAllTextAsync(metaPath, json, cancellationToken).ConfigureAwait(false);
        return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Ok());
    }

    // ---- GenerateSasUri ----
    /// <inheritdoc/>
    public override Uri GenerateSasUri(global::Azure.Storage.Sas.BlobSasBuilder builder) => Uri;

    // ---- Access Policy ----

    /// <inheritdoc/>
    public override Response<BlobContainerAccessPolicy> GetAccessPolicy(BlobRequestConditions conditions = default!, CancellationToken ct = default)
        => Response.FromValue(BlobsModelFactory.BlobContainerAccessPolicy(blobPublicAccess: PublicAccessType.None, eTag: new ETag("\"0x0\""), lastModified: DateTimeOffset.UtcNow, signedIdentifiers: new List<BlobSignedIdentifier>()), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerAccessPolicy>> GetAccessPolicyAsync(BlobRequestConditions conditions = default!, CancellationToken ct = default)
    { await Task.Yield(); return GetAccessPolicy(conditions, ct); }

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> SetAccessPolicy(PublicAccessType accessType = default, IEnumerable<BlobSignedIdentifier>? permissions = null, BlobRequestConditions conditions = default!, CancellationToken ct = default)
        => Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerInfo>> SetAccessPolicyAsync(PublicAccessType accessType = default, IEnumerable<BlobSignedIdentifier>? permissions = null, BlobRequestConditions conditions = default!, CancellationToken ct = default)
    { await Task.Yield(); return SetAccessPolicy(accessType, permissions, conditions, ct); }

    // ---- Account Info ----

    /// <inheritdoc/>
    public override Response<AccountInfo> GetAccountInfo(CancellationToken ct = default)
        => Response.FromValue(BlobsModelFactory.AccountInfo(skuName: SkuName.StandardLrs, accountKind: AccountKind.StorageV2), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<AccountInfo>> GetAccountInfoAsync(CancellationToken ct = default)
    { await Task.Yield(); return GetAccountInfo(ct); }

    // ---- FindBlobsByTags ----

    /// <inheritdoc/>
    public override Pageable<TaggedBlobItem> FindBlobsByTags(string tagFilterSqlExpression, CancellationToken ct = default)
        => new StaticPageable<TaggedBlobItem>(new List<TaggedBlobItem>());

    /// <inheritdoc/>
    public override AsyncPageable<TaggedBlobItem> FindBlobsByTagsAsync(string tagFilterSqlExpression, CancellationToken ct = default)
        => new StaticAsyncPageable<TaggedBlobItem>(new StaticPageable<TaggedBlobItem>(new List<TaggedBlobItem>()));

    // ---- Remaining virtual methods ----
    /// <inheritdoc/>
    public override Uri GenerateUserDelegationSasUri(global::Azure.Storage.Sas.BlobSasBuilder builder, UserDelegationKey userDelegationKey) => Uri;

    // ---- Helper ----
    private void PersistContainerMetadata(IDictionary<string, string> metadata)
    {
        var metaPath = Path.Combine(_store.ContainerPath, "_container.meta.json");
        var json = System.Text.Json.JsonSerializer.Serialize(metadata, _serviceClient.Options.JsonSerializerOptions);
        Iciclecreek.Azure.Storage.FileSystem.Internal.AtomicFile.WriteAllTextAsync(metaPath, json).GetAwaiter().GetResult();
    }
}

// ---- Pageable helpers ----

internal sealed class StaticPageable<T> : Pageable<T> where T : notnull
{
    private readonly IReadOnlyList<T> _items;
    public StaticPageable(IReadOnlyList<T> items) => _items = items;
    /// <inheritdoc/>
    public override IEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        yield return Page<T>.FromValues(_items.ToList(), null, StubResponse.Ok());
    }
}

internal sealed class StaticAsyncPageable<T> : AsyncPageable<T> where T : notnull
{
    private readonly Pageable<T> _inner;
    public StaticAsyncPageable(Pageable<T> inner) => _inner = inner;
    /// <inheritdoc/>
    public override async IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        foreach (var page in _inner.AsPages(continuationToken, pageSizeHint))
            yield return page;
    }
}

internal sealed class AsyncEnumerablePageable<T> : AsyncPageable<T> where T : notnull
{
    private readonly IAsyncEnumerable<global::Azure.Data.Tables.TableEntity> _source;
    private readonly Func<global::Azure.Data.Tables.TableEntity, T?> _transform;

    public AsyncEnumerablePageable(IAsyncEnumerable<global::Azure.Data.Tables.TableEntity> source, Func<global::Azure.Data.Tables.TableEntity, T?> transform)
    {
        _source = source;
        _transform = transform;
    }

    /// <inheritdoc/>
    public override async IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        var items = new List<T>();
        await foreach (var entity in _source.ConfigureAwait(false))
        {
            var result = _transform(entity);
            if (result is not null) items.Add(result);
        }
        yield return Page<T>.FromValues(items, null, StubResponse.Ok());
    }
}
