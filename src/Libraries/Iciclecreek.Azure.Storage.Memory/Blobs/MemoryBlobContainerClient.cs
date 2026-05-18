using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>In-memory drop-in replacement for <see cref="BlobContainerClient"/>.</summary>
public class MemoryBlobContainerClient : BlobContainerClient
{
    internal readonly MemoryBlobServiceClient _serviceClient;
    internal readonly string _containerName;

    internal MemoryBlobContainerClient(MemoryBlobServiceClient serviceClient, string containerName) : base()
    {
        _serviceClient = serviceClient;
        _containerName = containerName;
    }

    /// <inheritdoc/>
    public override string Name => _containerName;
    /// <inheritdoc/>
    public override string AccountName => _serviceClient.AccountName;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_serviceClient.Uri}{_containerName}");

    // ---- Create / CreateIfNotExists ----

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        var store = new ContainerStore
        {
            Metadata = metadata is not null ? new Dictionary<string, string>(metadata) : null,
            CreatedOn = DateTimeOffset.UtcNow
        };

        if (!_serviceClient.Containers.TryAdd(_containerName, store))
            throw new RequestFailedException(409, "Container already exists.", "ContainerAlreadyExists", null);

        var info = BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Create(publicAccessType, metadata, cancellationToken);

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        var store = new ContainerStore
        {
            Metadata = metadata is not null ? new Dictionary<string, string>(metadata) : null,
            CreatedOn = DateTimeOffset.UtcNow
        };
        _serviceClient.Containers.TryAdd(_containerName, store);

        return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => CreateIfNotExists(publicAccessType, metadata, cancellationToken);

    // ---- Async Create ----

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

    // ---- Delete ----

    /// <inheritdoc/>
    public override Response Delete(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryRemove(_containerName, out _))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        return StubResponse.Accepted();
    }

    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var removed = _serviceClient.Containers.TryRemove(_containerName, out _);
        return Response.FromValue(removed, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
    {
        var exists = _serviceClient.Containers.ContainsKey(_containerName);
        return Response.FromValue(exists, StubResponse.Ok());
    }

    // ---- Async Delete / Exists ----

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(conditions, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(conditions, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    // ---- GetBlobClient / specialized ----

    /// <inheritdoc/>
    public override BlobClient GetBlobClient(string blobName) => new MemoryBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override BlockBlobClient GetBlockBlobClientCore(string blobName) => new MemoryBlockBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override AppendBlobClient GetAppendBlobClientCore(string blobName) => new MemoryAppendBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override PageBlobClient GetPageBlobClientCore(string blobName) => new MemoryPageBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override BlobLeaseClient GetBlobLeaseClientCore(string leaseId) => new MemoryContainerLeaseClient(this, leaseId);

    // ---- UploadBlob ----

    /// <inheritdoc/>
    public override Response<BlobContentInfo> UploadBlob(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<BlobContentInfo> UploadBlob(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    // ---- DeleteBlob ----

    /// <inheritdoc/>
    public override Response DeleteBlob(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return client.Delete(snapshotsOption, conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<bool> DeleteBlobIfExists(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return client.DeleteIfExists(snapshotsOption, conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteBlobAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return await client.DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteBlobIfExistsAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobClient(_serviceClient, _containerName, blobName);
        return await client.DeleteIfExistsAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    // ---- GetBlobs ----

    /// <inheritdoc/>
    public override Pageable<BlobItem> GetBlobs(BlobTraits traits = default, BlobStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var items = new List<BlobItem>();
        foreach (var kvp in store.Blobs)
        {
            var blobName = kvp.Key;
            if (prefix is not null && !blobName.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var entry = kvp.Value;
            IDictionary<string, string>? metadata = null;
            if ((traits & BlobTraits.Metadata) != 0)
                metadata = entry.CloneMetadata();

            var blobType = entry.BlobType switch
            {
                "Append" => BlobType.Append,
                "Page" => BlobType.Page,
                _ => BlobType.Block
            };

            var props = BlobsModelFactory.BlobItemProperties(
                accessTierInferred: true,
                contentLength: entry.Content.Length,
                contentType: entry.ContentType,
                eTag: new ETag(entry.ETag),
                lastModified: entry.LastModified,
                blobType: blobType,
                createdOn: entry.CreatedOn);

            items.Add(BlobsModelFactory.BlobItem(name: blobName, deleted: false, properties: props, metadata: metadata));
        }
        return new StaticPageable<BlobItem>(items);
    }

    /// <inheritdoc/>
    public override AsyncPageable<BlobItem> GetBlobsAsync(BlobTraits traits = default, BlobStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobItem>(GetBlobs(traits, states, prefix, cancellationToken));

    /// <inheritdoc/>
    public override Pageable<BlobItem> GetBlobs(GetBlobsOptions options, CancellationToken cancellationToken = default)
        => GetBlobs(options?.Traits ?? default, options?.States ?? default, options?.Prefix, cancellationToken);

    /// <inheritdoc/>
    public override AsyncPageable<BlobItem> GetBlobsAsync(GetBlobsOptions options, CancellationToken cancellationToken = default)
        => GetBlobsAsync(options?.Traits ?? default, options?.States ?? default, options?.Prefix, cancellationToken);

    // ---- GetBlobsByHierarchy ----

    /// <inheritdoc/>
    public override Pageable<BlobHierarchyItem> GetBlobsByHierarchy(BlobTraits traits = default, BlobStates states = default, string? delimiter = "/", string? prefix = default, CancellationToken cancellationToken = default)
    {
        delimiter ??= "/";
        var items = new List<BlobHierarchyItem>();
        var prefixes = new HashSet<string>(StringComparer.Ordinal);

        foreach (var blobItem in GetBlobs(traits, states, prefix, cancellationToken))
        {
            var blobName = blobItem.Name;
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
                items.Add(BlobsModelFactory.BlobHierarchyItem(null!, blobItem));
            }
        }
        return new StaticPageable<BlobHierarchyItem>(items);
    }

    /// <inheritdoc/>
    public override AsyncPageable<BlobHierarchyItem> GetBlobsByHierarchyAsync(BlobTraits traits = default, BlobStates states = default, string? delimiter = "/", string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobHierarchyItem>(GetBlobsByHierarchy(traits, states, delimiter, prefix, cancellationToken));

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
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var metadata = store.Metadata is not null ? new Dictionary<string, string>(store.Metadata) : null;
        var props = BlobsModelFactory.BlobContainerProperties(lastModified: store.CreatedOn, eTag: new ETag("\"0x0\""), metadata: metadata);
        return Response.FromValue(props, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(conditions, cancellationToken); }

    // ---- SetMetadata ----

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> SetMetadata(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        store.Metadata = new Dictionary<string, string>(metadata);
        return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerInfo>> SetMetadataAsync(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetMetadata(metadata, conditions, cancellationToken); }

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

    // ---- GenerateUserDelegationSasUri ----
    /// <inheritdoc/>
    public override Uri GenerateUserDelegationSasUri(global::Azure.Storage.Sas.BlobSasBuilder builder, UserDelegationKey userDelegationKey) => Uri;
}

// ---- Pageable helpers ----

internal sealed class StaticPageable<T> : Pageable<T> where T : notnull
{
    private readonly IReadOnlyList<T> _items;
    public StaticPageable(IReadOnlyList<T> items) => _items = items;
    public StaticPageable(IEnumerable<T> items) => _items = items.ToList();
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
