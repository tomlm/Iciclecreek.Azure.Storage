using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Linq;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>Filesystem-backed drop-in replacement for <see cref="Azure.Storage.Blobs.BlobServiceClient"/>. Manages blob containers as subdirectories.</summary>
public class FileBlobServiceClient : BlobServiceClient
{
    internal readonly string BlobsRootPath;
    internal readonly FileStorageOptions Options;
    private readonly string _accountName;
    private readonly Uri _blobServiceUri;

    public FileBlobServiceClient(string rootPath, FileStorageOptions? options = null) : base()
    {
        BlobsRootPath = Path.Combine(Path.GetFullPath(rootPath), "blobs");
        Directory.CreateDirectory(BlobsRootPath);
        Options = options ?? new FileStorageOptions();
        _accountName = string.Empty;
        _blobServiceUri = new Uri("file://blob/");
    }

    /// <inheritdoc/>
    public override string AccountName => _accountName;
    /// <inheritdoc/>
    public override Uri Uri => _blobServiceUri;

    // ---- GetBlobContainerClient ----

    /// <inheritdoc/>
    public override BlobContainerClient GetBlobContainerClient(string blobContainerName)
        => new FileBlobContainerClient(this, blobContainerName);

    // ---- CreateBlobContainer ----

    /// <inheritdoc/>
    public override Response<BlobContainerClient> CreateBlobContainer(string blobContainerName, PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobContainerClient(this, blobContainerName);
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
        var client = new FileBlobContainerClient(this, blobContainerName);
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
        if (!Directory.Exists(BlobsRootPath))
            return new StaticPageable<BlobContainerItem>(items);

        foreach (var dir in Directory.EnumerateDirectories(BlobsRootPath).OrderBy(d => Path.GetFileName(d), StringComparer.Ordinal))
        {
            var name = Path.GetFileName(dir);
            if (name.StartsWith('.') || name.StartsWith('_')) continue;
            if (prefix is not null && !name.StartsWith(prefix, StringComparison.Ordinal)) continue;
            items.Add(BlobsModelFactory.BlobContainerItem(name, BlobsModelFactory.BlobContainerProperties(lastModified: Directory.GetLastWriteTimeUtc(dir), eTag: new ETag("\"0x0\""))));
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
        // Use the (objectId, tenantId, startsOn, expiresOn, service, version, value) overload
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

        var blobsRoot = BlobsRootPath;
        if (!Directory.Exists(blobsRoot))
            return new StaticPageable<TaggedBlobItem>(results);

        foreach (var containerDir in Directory.EnumerateDirectories(blobsRoot))
        {
            var containerName = Path.GetFileName(containerDir);
            if (containerName.StartsWith('.') || containerName.StartsWith('_')) continue;

            var store = new Internal.BlobStore(BlobsRootPath, containerName, Options);
            foreach (var (blobName, _, _) in store.EnumerateBlobs(null))
            {
                ct.ThrowIfCancellationRequested();
                var sidecar = store.ReadSidecarAsync(blobName, ct).GetAwaiter().GetResult();
                if (sidecar?.Tags == null || sidecar.Tags.Count == 0) continue;

                bool match = conditions.All(c =>
                    sidecar.Tags.TryGetValue(c.Key, out var val) &&
                    string.Equals(val, c.Value, StringComparison.Ordinal));

                if (match)
                {
                    var item = BlobsModelFactory.TaggedBlobItem(blobName, containerName, sidecar.Tags);
                    results.Add(item);
                }
            }
        }

        return new StaticPageable<TaggedBlobItem>(results);
    }

    /// <inheritdoc/>
    public override AsyncPageable<TaggedBlobItem> FindBlobsByTagsAsync(string tagFilterSqlExpression, CancellationToken ct = default)
        => new StaticAsyncPageable<TaggedBlobItem>(FindBlobsByTags(tagFilterSqlExpression, ct));

    /// <summary>Parses simple tag filter expressions like <c>"key = 'value' AND key2 = 'value2'"</c>.</summary>
    private static List<KeyValuePair<string, string>> ParseSimpleTagFilter(string filter)
    {
        var result = new List<KeyValuePair<string, string>>();
        if (string.IsNullOrWhiteSpace(filter)) return result;

        // Split on AND (case-insensitive)
        var parts = filter.Split(new[] { " AND ", " and " }, StringSplitOptions.RemoveEmptyEntries)
                          .Select(p => p.Trim());
        foreach (var part in parts)
        {
            // Parse: key = 'value'
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
