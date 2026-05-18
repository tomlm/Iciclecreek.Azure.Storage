using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>SQLite-backed drop-in replacement for <see cref="BlobContainerClient"/>.</summary>
public class SqliteBlobContainerClient : BlobContainerClient
{
    internal readonly SqliteBlobServiceClient _serviceClient;
    internal readonly string _containerName;

    internal SqliteBlobContainerClient(SqliteBlobServiceClient serviceClient, string containerName) : base()
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
        using var conn = _serviceClient.Db.Open();
        // Check if exists
        using (var checkCmd = conn.CreateCommand())
        {
            checkCmd.CommandText = "SELECT COUNT(*) FROM Containers WHERE Name = @name";
            checkCmd.Parameters.AddWithValue("@name", _containerName);
            if ((long)checkCmd.ExecuteScalar()! > 0)
                throw new RequestFailedException(409, "Container already exists.", "ContainerAlreadyExists", null);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO Containers (Name, Metadata, CreatedOn) VALUES (@name, @metadata, @createdOn)";
        cmd.Parameters.AddWithValue("@name", _containerName);
        cmd.Parameters.AddWithValue("@metadata", metadata is not null ? JsonSerializer.Serialize(metadata) : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@createdOn", DateTimeOffset.UtcNow.ToString("o"));
        cmd.ExecuteNonQuery();

        var info = BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Create(publicAccessType, metadata, cancellationToken);

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT OR IGNORE INTO Containers (Name, Metadata, CreatedOn) VALUES (@name, @metadata, @createdOn)";
        cmd.Parameters.AddWithValue("@name", _containerName);
        cmd.Parameters.AddWithValue("@metadata", metadata is not null ? JsonSerializer.Serialize(metadata) : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@createdOn", DateTimeOffset.UtcNow.ToString("o"));
        cmd.ExecuteNonQuery();

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
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM Containers WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _containerName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        // Clean up blobs in this container
        using var cleanup = conn.CreateCommand();
        cleanup.CommandText = "DELETE FROM Blobs WHERE ContainerName = @name; DELETE FROM StagedBlocks WHERE ContainerName = @name; DELETE FROM CommittedBlocks WHERE ContainerName = @name; DELETE FROM PageRanges WHERE ContainerName = @name; DELETE FROM Snapshots WHERE ContainerName = @name; DELETE FROM Versions WHERE ContainerName = @name;";
        cleanup.Parameters.AddWithValue("@name", _containerName);
        cleanup.ExecuteNonQuery();

        return StubResponse.Accepted();
    }

    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM Containers WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _containerName);
        var rows = cmd.ExecuteNonQuery();
        if (rows > 0)
        {
            using var cleanup = conn.CreateCommand();
            cleanup.CommandText = "DELETE FROM Blobs WHERE ContainerName = @name; DELETE FROM StagedBlocks WHERE ContainerName = @name; DELETE FROM CommittedBlocks WHERE ContainerName = @name; DELETE FROM PageRanges WHERE ContainerName = @name; DELETE FROM Snapshots WHERE ContainerName = @name; DELETE FROM Versions WHERE ContainerName = @name;";
            cleanup.Parameters.AddWithValue("@name", _containerName);
            cleanup.ExecuteNonQuery();
        }
        return Response.FromValue(rows > 0, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Containers WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _containerName);
        var count = (long)cmd.ExecuteScalar()!;
        return Response.FromValue(count > 0, StubResponse.Ok());
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
    public override BlobClient GetBlobClient(string blobName) => new SqliteBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override BlockBlobClient GetBlockBlobClientCore(string blobName) => new SqliteBlockBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override AppendBlobClient GetAppendBlobClientCore(string blobName) => new SqliteAppendBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override PageBlobClient GetPageBlobClientCore(string blobName) => new SqlitePageBlobClient(_serviceClient, _containerName, blobName);

    /// <inheritdoc/>
    protected override BlobLeaseClient GetBlobLeaseClientCore(string leaseId) => new SqliteContainerLeaseClient(this, leaseId);

    // ---- UploadBlob ----

    /// <inheritdoc/>
    public override Response<BlobContentInfo> UploadBlob(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<BlobContentInfo> UploadBlob(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    // ---- DeleteBlob ----

    /// <inheritdoc/>
    public override Response DeleteBlob(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return client.Delete(snapshotsOption, conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<bool> DeleteBlobIfExists(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return client.DeleteIfExists(snapshotsOption, conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteBlobAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return await client.DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteBlobIfExistsAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new SqliteBlobClient(_serviceClient, _containerName, blobName);
        return await client.DeleteIfExistsAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    // ---- GetBlobs ----

    /// <inheritdoc/>
    public override Pageable<BlobItem> GetBlobs(BlobTraits traits = default, BlobStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
    {
        var items = new List<BlobItem>();
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();

        if (prefix is not null)
        {
            cmd.CommandText = "SELECT BlobName, BlobType, ContentType, ETag, CreatedOn, LastModified, Length, Metadata FROM Blobs WHERE ContainerName = @container AND BlobName LIKE @prefix || '%'";
            cmd.Parameters.AddWithValue("@prefix", prefix);
        }
        else
        {
            cmd.CommandText = "SELECT BlobName, BlobType, ContentType, ETag, CreatedOn, LastModified, Length, Metadata FROM Blobs WHERE ContainerName = @container";
        }
        cmd.Parameters.AddWithValue("@container", _containerName);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var blobName = reader.GetString(0);
            var blobTypeStr = reader.IsDBNull(1) ? "Block" : reader.GetString(1);
            var contentType = reader.IsDBNull(2) ? null : reader.GetString(2);
            var etag = reader.IsDBNull(3) ? "\"0x0\"" : reader.GetString(3);
            var createdOn = reader.IsDBNull(4) ? DateTimeOffset.UtcNow : DateTimeOffset.Parse(reader.GetString(4));
            var lastModified = reader.IsDBNull(5) ? DateTimeOffset.UtcNow : DateTimeOffset.Parse(reader.GetString(5));
            var length = reader.GetInt64(6);
            var metadataJson = reader.IsDBNull(7) ? null : reader.GetString(7);

            IDictionary<string, string>? metadata = null;
            if ((traits & BlobTraits.Metadata) != 0 && metadataJson is not null)
                metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(metadataJson);

            var blobType = blobTypeStr switch
            {
                "Append" => BlobType.Append,
                "Page" => BlobType.Page,
                _ => BlobType.Block
            };

            var props = BlobsModelFactory.BlobItemProperties(
                accessTierInferred: true,
                contentLength: length,
                contentType: contentType,
                eTag: new ETag(etag),
                lastModified: lastModified,
                blobType: blobType,
                createdOn: createdOn);

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
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Metadata, CreatedOn FROM Containers WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _containerName);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var metadataJson = reader.IsDBNull(0) ? null : reader.GetString(0);
        IDictionary<string, string>? metadata = null;
        if (metadataJson is not null)
            metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(metadataJson);

        var props = BlobsModelFactory.BlobContainerProperties(lastModified: DateTimeOffset.UtcNow, eTag: new ETag("\"0x0\""), metadata: metadata);
        return Response.FromValue(props, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(conditions, cancellationToken); }

    // ---- SetMetadata ----

    /// <inheritdoc/>
    public override Response<BlobContainerInfo> SetMetadata(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Containers SET Metadata = @metadata WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _containerName);
        cmd.Parameters.AddWithValue("@metadata", JsonSerializer.Serialize(metadata));
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
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
