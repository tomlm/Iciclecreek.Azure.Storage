using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>In-memory drop-in replacement for <see cref="BlobServiceClient"/>.</summary>
public class MemoryBlobServiceClient : BlobServiceClient
{
    internal readonly ConcurrentDictionary<string, ContainerStore> Containers = new();
    private readonly string _accountName;
    private readonly Uri _blobServiceUri;

    public MemoryBlobServiceClient() : base()
    {
        _accountName = string.Empty;
        _blobServiceUri = new Uri("memory://blob/");
    }

    /// <inheritdoc/>
    public override string AccountName => _accountName;
    /// <inheritdoc/>
    public override Uri Uri => _blobServiceUri;

    // ---- GetBlobContainerClient ----

    /// <inheritdoc/>
    public override BlobContainerClient GetBlobContainerClient(string blobContainerName)
        => new MemoryBlobContainerClient(this, blobContainerName);

    // ---- CreateBlobContainer ----

    /// <inheritdoc/>
    public override Response<BlobContainerClient> CreateBlobContainer(string blobContainerName, PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobContainerClient(this, blobContainerName);
        client.Create(publicAccessType, metadata, cancellationToken);
        return Response.FromValue<BlobContainerClient>(client, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerClient>> CreateBlobContainerAsync(string blobContainerName, PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateBlobContainer(blobContainerName, publicAccessType, metadata, cancellationToken); }

    // ---- DeleteBlobContainer ----

    /// <inheritdoc/>
    public override Response DeleteBlobContainer(string blobContainerName, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new MemoryBlobContainerClient(this, blobContainerName);
        return client.Delete(conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteBlobContainerAsync(string blobContainerName, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteBlobContainer(blobContainerName, conditions, cancellationToken); }

    // ---- GetBlobContainers ----

    /// <inheritdoc/>
    public override Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits = default, BlobContainerStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
    {
        var items = new List<BlobContainerItem>();
        foreach (var kvp in Containers)
        {
            var name = kvp.Key;
            if (prefix is not null && !name.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var store = kvp.Value;
            var metadata = store.Metadata is not null ? new Dictionary<string, string>(store.Metadata) : null;
            items.Add(BlobsModelFactory.BlobContainerItem(name, BlobsModelFactory.BlobContainerProperties(lastModified: store.CreatedOn, eTag: new ETag("\"0x0\""), metadata: metadata)));
        }
        return new StaticPageable<BlobContainerItem>(items);
    }

    /// <inheritdoc/>
    public override Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => GetBlobContainers(traits, default, prefix, cancellationToken);

    /// <inheritdoc/>
    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits = default, BlobContainerStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobContainerItem>(GetBlobContainers(traits, states, prefix, cancellationToken));

    /// <inheritdoc/>
    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobContainerItem>(GetBlobContainers(traits, default, prefix, cancellationToken));

    // ---- GetProperties / SetProperties ----

    /// <inheritdoc/>
    public override Response<BlobServiceProperties> GetProperties(CancellationToken ct = default)
        => Response.FromValue(new BlobServiceProperties(), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<BlobServiceProperties>> GetPropertiesAsync(CancellationToken ct = default)
    { await Task.Yield(); return GetProperties(ct); }

    /// <inheritdoc/>
    public override Response SetProperties(BlobServiceProperties properties, CancellationToken ct = default)
        => StubResponse.Ok();

    /// <inheritdoc/>
    public override async Task<Response> SetPropertiesAsync(BlobServiceProperties properties, CancellationToken ct = default)
    { await Task.Yield(); return SetProperties(properties, ct); }

    // ---- GetStatistics ----

    /// <inheritdoc/>
    public override Response<BlobServiceStatistics> GetStatistics(CancellationToken ct = default)
        => Response.FromValue(BlobsModelFactory.BlobServiceStatistics(geoReplication: null), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<BlobServiceStatistics>> GetStatisticsAsync(CancellationToken ct = default)
    { await Task.Yield(); return GetStatistics(ct); }

    // ---- GetUserDelegationKey ----

    /// <inheritdoc/>
    public override Response<UserDelegationKey> GetUserDelegationKey(DateTimeOffset? startsOn, DateTimeOffset expiresOn, CancellationToken ct = default)
    {
        var objectId = Guid.NewGuid().ToString();
        var tenantId = Guid.NewGuid().ToString();
        var starts = startsOn ?? DateTimeOffset.UtcNow;
        var key = BlobsModelFactory.UserDelegationKey(objectId, tenantId, starts, expiresOn, "b", "2020-02-10", Convert.ToBase64String(new byte[32]));
        return Response.FromValue(key, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<UserDelegationKey>> GetUserDelegationKeyAsync(DateTimeOffset? startsOn, DateTimeOffset expiresOn, CancellationToken ct = default)
    { await Task.Yield(); return GetUserDelegationKey(startsOn, expiresOn, ct); }

    // ---- GetAccountInfo ----

    /// <inheritdoc/>
    public override Response<AccountInfo> GetAccountInfo(CancellationToken ct = default)
        => Response.FromValue(BlobsModelFactory.AccountInfo(skuName: SkuName.StandardLrs, accountKind: AccountKind.StorageV2), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<AccountInfo>> GetAccountInfoAsync(CancellationToken ct = default)
    { await Task.Yield(); return GetAccountInfo(ct); }

    // ---- FindBlobsByTags ----

    /// <inheritdoc/>
    public override Pageable<TaggedBlobItem> FindBlobsByTags(string tagFilterSqlExpression, CancellationToken ct = default)
    {
        var results = new List<TaggedBlobItem>();
        var conditions = ParseSimpleTagFilter(tagFilterSqlExpression);
        if (conditions.Count == 0)
            return new StaticPageable<TaggedBlobItem>(results);

        foreach (var containerKvp in Containers)
        {
            var containerName = containerKvp.Key;
            var store = containerKvp.Value;
            foreach (var blobKvp in store.Blobs)
            {
                var blobName = blobKvp.Key;
                var entry = blobKvp.Value;
                var tags = entry.CloneTags();
                if (tags is null) continue;

                bool allMatch = true;
                foreach (var cond in conditions)
                {
                    if (!tags.TryGetValue(cond.Key, out var val) || val != cond.Value)
                    {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch)
                    results.Add(BlobsModelFactory.TaggedBlobItem(blobName, containerName, new Dictionary<string, string>(tags)));
            }
        }

        return new StaticPageable<TaggedBlobItem>(results);
    }

    /// <inheritdoc/>
    public override AsyncPageable<TaggedBlobItem> FindBlobsByTagsAsync(string tagFilterSqlExpression, CancellationToken ct = default)
        => new StaticAsyncPageable<TaggedBlobItem>(FindBlobsByTags(tagFilterSqlExpression, ct));

    private static List<KeyValuePair<string, string>> ParseSimpleTagFilter(string filter)
    {
        var result = new List<KeyValuePair<string, string>>();
        if (string.IsNullOrWhiteSpace(filter)) return result;

        var parts = filter.Split(new[] { " AND ", " and " }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var part in parts)
        {
            var eqIdx = part.IndexOf('=');
            if (eqIdx < 0) continue;
            var key = part[..eqIdx].Trim().Trim('"');
            var val = part[(eqIdx + 1)..].Trim().Trim('\'').Trim('"');
            result.Add(new KeyValuePair<string, string>(key, val));
        }
        return result;
    }

    // ---- UndeleteBlobContainer ----

    /// <inheritdoc/>
    public override Response<BlobContainerClient> UndeleteBlobContainer(string deletedContainerName, string deletedContainerVersion, CancellationToken ct = default)
        => throw new RequestFailedException(404, "No deleted container found.", "ContainerNotFound", null);

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerClient>> UndeleteBlobContainerAsync(string deletedContainerName, string deletedContainerVersion, CancellationToken ct = default)
    { await Task.Yield(); return UndeleteBlobContainer(deletedContainerName, deletedContainerVersion, ct); }

    /// <inheritdoc/>
    public override Response<BlobContainerClient> UndeleteBlobContainer(string deletedContainerName, string deletedContainerVersion, string destinationContainerName, CancellationToken ct = default)
        => throw new RequestFailedException(404, "No deleted container found.", "ContainerNotFound", null);

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerClient>> UndeleteBlobContainerAsync(string deletedContainerName, string deletedContainerVersion, string destinationContainerName, CancellationToken ct = default)
    { await Task.Yield(); return UndeleteBlobContainer(deletedContainerName, deletedContainerVersion, destinationContainerName, ct); }
}
