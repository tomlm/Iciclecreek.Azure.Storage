using System.Security.Cryptography;
using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>SQLite-backed drop-in replacement for <see cref="BlockBlobClient"/>.</summary>
public class SqliteBlockBlobClient : BlockBlobClient
{
    internal readonly SqliteBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    internal SqliteBlockBlobClient(SqliteBlobServiceClient serviceClient, string containerName, string blobName) : base()
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

    // ---- StageBlock ----

    /// <inheritdoc/>
    public override async Task<Response<BlockInfo>> StageBlockAsync(string base64BlockId, Stream content, byte[] transactionalContentHash = default!, BlobRequestConditions conditions = default!, IProgress<long>? progressHandler = default, CancellationToken cancellationToken = default)
    {
        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        var data = ms.ToArray();

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT OR REPLACE INTO StagedBlocks (ContainerName, BlobName, BlockId, Content, Size) VALUES (@container, @blob, @blockId, @content, @size)";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.Parameters.AddWithValue("@blockId", base64BlockId);
        cmd.Parameters.AddWithValue("@content", data);
        cmd.Parameters.AddWithValue("@size", (long)data.Length);
        cmd.ExecuteNonQuery();

        var info = BlobsModelFactory.BlockInfo(null, null, null!, null!);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlockInfo>> StageBlockAsync(string base64BlockId, Stream content, BlockBlobStageBlockOptions options, CancellationToken cancellationToken = default)
        => await StageBlockAsync(base64BlockId, content, null!, options?.Conditions!, null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlockInfo> StageBlock(string base64BlockId, Stream content, byte[] transactionalContentHash = default!, BlobRequestConditions conditions = default!, IProgress<long>? progressHandler = default, CancellationToken cancellationToken = default)
        => StageBlockAsync(base64BlockId, content, transactionalContentHash, conditions, progressHandler, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Response<BlockInfo> StageBlock(string base64BlockId, Stream content, BlockBlobStageBlockOptions options, CancellationToken cancellationToken = default)
        => StageBlockAsync(base64BlockId, content, options, cancellationToken).GetAwaiter().GetResult();

    // ---- CommitBlockList ----

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CommitBlockListAsync(IEnumerable<string> base64BlockIds, CommitBlockListOptions options = default!, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();

        // Assemble content from staged blocks
        using var assembledStream = new MemoryStream();
        var committedBlocks = new List<(string Id, long Size)>();
        int ordinal = 0;

        foreach (var blockId in base64BlockIds)
        {
            using var readCmd = conn.CreateCommand();
            readCmd.CommandText = "SELECT Content, Size FROM StagedBlocks WHERE ContainerName = @container AND BlobName = @blob AND BlockId = @blockId";
            readCmd.Parameters.AddWithValue("@container", _containerName);
            readCmd.Parameters.AddWithValue("@blob", _blobName);
            readCmd.Parameters.AddWithValue("@blockId", blockId);

            using var reader = readCmd.ExecuteReader();
            if (!reader.Read())
                throw new RequestFailedException(400, $"Block '{blockId}' not found in staging.", "InvalidBlockList", null);

            var blockContent = (byte[])reader[0];
            var blockSize = reader.GetInt64(1);
            assembledStream.Write(blockContent);
            committedBlocks.Add((blockId, blockSize));
            ordinal++;
        }

        var data = assembledStream.ToArray();
        var md5 = MD5.HashData(data);
        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";
        var versionId = now.ToString("yyyyMMddTHHmmssfffffffZ");

        // Insert/replace blob
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT OR REPLACE INTO Blobs
            (ContainerName, BlobName, BlobType, Content, ContentType, ContentEncoding,
             ContentHash, ETag, CreatedOn, LastModified, Length, Metadata, VersionId)
            VALUES (@container, @blob, 'Block', @content, @contentType, @contentEncoding,
                    @contentHash, @etag,
                    COALESCE((SELECT CreatedOn FROM Blobs WHERE ContainerName = @container AND BlobName = @blob), @createdOn),
                    @lastModified, @length, @metadata, @versionId)";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.Parameters.AddWithValue("@content", data);
        cmd.Parameters.AddWithValue("@contentType", (object?)options?.HttpHeaders?.ContentType ?? "application/octet-stream");
        cmd.Parameters.AddWithValue("@contentEncoding", (object?)options?.HttpHeaders?.ContentEncoding ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentHash", Convert.ToBase64String(md5));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@createdOn", now.ToString("o"));
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@length", (long)data.Length);
        cmd.Parameters.AddWithValue("@metadata", options?.Metadata is not null ? JsonSerializer.Serialize(options.Metadata) : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@versionId", versionId);
        cmd.ExecuteNonQuery();

        // Populate CommittedBlocks
        using var delCommitted = conn.CreateCommand();
        delCommitted.CommandText = "DELETE FROM CommittedBlocks WHERE ContainerName = @container AND BlobName = @blob";
        delCommitted.Parameters.AddWithValue("@container", _containerName);
        delCommitted.Parameters.AddWithValue("@blob", _blobName);
        delCommitted.ExecuteNonQuery();

        for (int i = 0; i < committedBlocks.Count; i++)
        {
            using var insertCmd = conn.CreateCommand();
            insertCmd.CommandText = "INSERT INTO CommittedBlocks (ContainerName, BlobName, BlockId, Size, Ordinal) VALUES (@container, @blob, @blockId, @size, @ordinal)";
            insertCmd.Parameters.AddWithValue("@container", _containerName);
            insertCmd.Parameters.AddWithValue("@blob", _blobName);
            insertCmd.Parameters.AddWithValue("@blockId", committedBlocks[i].Id);
            insertCmd.Parameters.AddWithValue("@size", committedBlocks[i].Size);
            insertCmd.Parameters.AddWithValue("@ordinal", i);
            insertCmd.ExecuteNonQuery();
        }

        // Delete staged blocks
        using var delStaged = conn.CreateCommand();
        delStaged.CommandText = "DELETE FROM StagedBlocks WHERE ContainerName = @container AND BlobName = @blob";
        delStaged.Parameters.AddWithValue("@container", _containerName);
        delStaged.Parameters.AddWithValue("@blob", _blobName);
        delStaged.ExecuteNonQuery();

        var info = BlobsModelFactory.BlobContentInfo(new ETag(etag), now, md5, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CommitBlockListAsync(IEnumerable<string> base64BlockIds, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, CancellationToken cancellationToken = default)
        => await CommitBlockListAsync(base64BlockIds, new CommitBlockListOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobContentInfo> CommitBlockList(IEnumerable<string> base64BlockIds, CommitBlockListOptions options = default!, CancellationToken cancellationToken = default)
        => CommitBlockListAsync(base64BlockIds, options, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Response<BlobContentInfo> CommitBlockList(IEnumerable<string> base64BlockIds, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, CancellationToken cancellationToken = default)
        => CommitBlockListAsync(base64BlockIds, httpHeaders, metadata, conditions, accessTier, cancellationToken).GetAwaiter().GetResult();

    // ---- GetBlockList ----

    /// <inheritdoc/>
    public override async Task<Response<BlockList>> GetBlockListAsync(BlockListTypes blockListTypes = BlockListTypes.All, string snapshot = default!, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        var committed = new List<BlobBlock>();
        var uncommitted = new List<BlobBlock>();

        if ((blockListTypes & BlockListTypes.Committed) != 0)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT BlockId, Size FROM CommittedBlocks WHERE ContainerName = @container AND BlobName = @blob ORDER BY Ordinal";
            cmd.Parameters.AddWithValue("@container", _containerName);
            cmd.Parameters.AddWithValue("@blob", _blobName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                committed.Add(BlobsModelFactory.BlobBlock(reader.GetString(0), (int)reader.GetInt64(1)));
        }

        if ((blockListTypes & BlockListTypes.Uncommitted) != 0)
        {
            var committedIds = committed.Select(b => b.Name).ToHashSet();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT BlockId, Size FROM StagedBlocks WHERE ContainerName = @container AND BlobName = @blob";
            cmd.Parameters.AddWithValue("@container", _containerName);
            cmd.Parameters.AddWithValue("@blob", _blobName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var blockId = reader.GetString(0);
                if (!committedIds.Contains(blockId))
                    uncommitted.Add(BlobsModelFactory.BlobBlock(blockId, (int)reader.GetInt64(1)));
            }
        }

        var blockList = BlobsModelFactory.BlockList(committed, uncommitted);
        return Response.FromValue(blockList, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlockList> GetBlockList(BlockListTypes blockListTypes = BlockListTypes.All, string snapshot = default!, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetBlockListAsync(blockListTypes, snapshot, conditions, cancellationToken).GetAwaiter().GetResult();

    // ---- Upload ----

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.UploadCoreAsync(content, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => await UploadAsync(content, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => UploadAsync(content, options, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => UploadAsync(content, httpHeaders, metadata, conditions, accessTier, progressHandler, cancellationToken).GetAwaiter().GetResult();

    // ---- Shared blob operations ----

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.ExistsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => ExistsAsync(cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response Delete(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => DeleteAsync(snapshotsOption, conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetPropertiesAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
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
    public override async Task<Stream> OpenWriteAsync(bool overwrite, BlockBlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.OpenWriteAsync(overwrite, options is null ? null : new BlobOpenWriteOptions
        {
            HttpHeaders = options.HttpHeaders,
            Metadata = options.Metadata,
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Stream OpenWrite(bool overwrite, BlockBlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
        => OpenWriteAsync(overwrite, options, cancellationToken).GetAwaiter().GetResult();

    // ---- SyncUploadFromUri ----
    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> SyncUploadFromUriAsync(Uri copySource, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        await blobClient.StartCopyFromUriAsync(copySource, null!, cancellationToken).ConfigureAwait(false);
        return await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false) is { } props
            ? Response.FromValue(BlobsModelFactory.BlobContentInfo(props.Value.ETag, props.Value.LastModified, null, null!, null!, null!, 0), StubResponse.Created())
            : throw new RequestFailedException(404, "Blob not found after copy.", "BlobNotFound", null);
    }

    /// <inheritdoc/>
    public override Response<BlobContentInfo> SyncUploadFromUri(Uri copySource, bool overwrite = false, CancellationToken cancellationToken = default)
        => SyncUploadFromUriAsync(copySource, overwrite, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> SyncUploadFromUriAsync(Uri copySource, BlobSyncUploadFromUriOptions options, CancellationToken cancellationToken = default)
        => await SyncUploadFromUriAsync(copySource, true, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobContentInfo> SyncUploadFromUri(Uri copySource, BlobSyncUploadFromUriOptions options, CancellationToken cancellationToken = default)
        => SyncUploadFromUriAsync(copySource, options, cancellationToken).GetAwaiter().GetResult();
}
