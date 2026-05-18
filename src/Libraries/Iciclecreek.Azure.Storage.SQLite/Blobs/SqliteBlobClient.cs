using System.Security.Cryptography;
using System.Text.Json;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>SQLite-backed drop-in replacement for <see cref="BlobClient"/>.</summary>
public class SqliteBlobClient : BlobClient
{
    internal readonly SqliteBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    internal SqliteBlobClient(SqliteBlobServiceClient serviceClient, string containerName, string blobName) : base()
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
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var count = (long)cmd.ExecuteScalar()!;
        return Response.FromValue(count > 0, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();

        // Check IfMatch condition
        if (conditions?.IfMatch is not null && conditions.IfMatch != ETag.All)
        {
            using var matchCmd = conn.CreateCommand();
            matchCmd.CommandText = "SELECT ETag FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            matchCmd.Parameters.AddWithValue("@container", _containerName);
            matchCmd.Parameters.AddWithValue("@blob", _blobName);
            var currentEtag = matchCmd.ExecuteScalar() as string;
            if (currentEtag is null || conditions.IfMatch.ToString() != currentEtag)
                throw new RequestFailedException(412, "Condition not met.", "ConditionNotMet", null);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        // Clean up related tables
        using var cleanup = conn.CreateCommand();
        cleanup.CommandText = "DELETE FROM StagedBlocks WHERE ContainerName = @container AND BlobName = @blob; DELETE FROM CommittedBlocks WHERE ContainerName = @container AND BlobName = @blob; DELETE FROM PageRanges WHERE ContainerName = @container AND BlobName = @blob; DELETE FROM Snapshots WHERE ContainerName = @container AND BlobName = @blob;";
        cleanup.Parameters.AddWithValue("@container", _containerName);
        cleanup.Parameters.AddWithValue("@blob", _blobName);
        cleanup.ExecuteNonQuery();

        return StubResponse.Accepted();
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows > 0)
        {
            using var cleanup = conn.CreateCommand();
            cleanup.CommandText = "DELETE FROM StagedBlocks WHERE ContainerName = @container AND BlobName = @blob; DELETE FROM CommittedBlocks WHERE ContainerName = @container AND BlobName = @blob; DELETE FROM PageRanges WHERE ContainerName = @container AND BlobName = @blob; DELETE FROM Snapshots WHERE ContainerName = @container AND BlobName = @blob;";
            cleanup.Parameters.AddWithValue("@container", _containerName);
            cleanup.Parameters.AddWithValue("@blob", _blobName);
            cleanup.ExecuteNonQuery();
        }
        return Response.FromValue(rows > 0, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT BlobType, ContentType, ContentEncoding, ContentLanguage, ContentDisposition,
            CacheControl, ContentHash, ETag, CreatedOn, LastModified, Length, AccessTier, Metadata, SequenceNumber
            FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var blobTypeStr = reader.IsDBNull(0) ? "Block" : reader.GetString(0);
        var contentType = reader.IsDBNull(1) ? "application/octet-stream" : reader.GetString(1);
        var contentEncoding = reader.IsDBNull(2) ? null : reader.GetString(2);
        var contentLanguage = reader.IsDBNull(3) ? null : reader.GetString(3);
        var contentDisposition = reader.IsDBNull(4) ? null : reader.GetString(4);
        var cacheControl = reader.IsDBNull(5) ? null : reader.GetString(5);
        var contentHashBase64 = reader.IsDBNull(6) ? null : reader.GetString(6);
        var etag = reader.GetString(7);
        var createdOn = DateTimeOffset.Parse(reader.GetString(8));
        var lastModified = DateTimeOffset.Parse(reader.GetString(9));
        var length = reader.GetInt64(10);
        var accessTier = reader.IsDBNull(11) ? null : reader.GetString(11);
        var metadataJson = reader.IsDBNull(12) ? null : reader.GetString(12);
        var sequenceNumber = reader.IsDBNull(13) ? 0L : reader.GetInt64(13);

        var md5 = contentHashBase64 is not null ? Convert.FromBase64String(contentHashBase64) : null;
        IDictionary<string, string>? metadata = null;
        if (metadataJson is not null)
            metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(metadataJson);

        var blobType = blobTypeStr switch
        {
            "Append" => BlobType.Append,
            "Page" => BlobType.Page,
            _ => BlobType.Block
        };

        var props = BlobsModelFactory.BlobProperties(
            lastModified: lastModified, createdOn: createdOn,
            metadata: metadata,
            blobType: blobType,
            contentLength: length,
            contentType: contentType,
            eTag: new ETag(etag), contentHash: md5,
            contentEncoding: contentEncoding, contentDisposition: contentDisposition,
            contentLanguage: contentLanguage, cacheControl: cacheControl,
            accessTier: accessTier, blobSequenceNumber: sequenceNumber);
        return Response.FromValue(props, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobInfo>> SetMetadataAsync(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";

        using var conn = _serviceClient.Db.Open();

        // Check IfMatch condition
        if (conditions?.IfMatch is not null && conditions.IfMatch != ETag.All)
        {
            using var matchCmd = conn.CreateCommand();
            matchCmd.CommandText = "SELECT ETag FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            matchCmd.Parameters.AddWithValue("@container", _containerName);
            matchCmd.Parameters.AddWithValue("@blob", _blobName);
            var currentEtag = matchCmd.ExecuteScalar() as string;
            if (currentEtag is null || conditions.IfMatch.ToString() != currentEtag)
                throw new RequestFailedException(412, "Condition not met.", "ConditionNotMet", null);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Blobs SET Metadata = @metadata, LastModified = @lastModified, ETag = @etag WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@metadata", JsonSerializer.Serialize(metadata));
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        return Response.FromValue(BlobsModelFactory.BlobInfo(new ETag(etag), now), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobInfo>> SetHttpHeadersAsync(BlobHttpHeaders httpHeaders, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET
            ContentType = COALESCE(@contentType, ContentType),
            ContentEncoding = COALESCE(@contentEncoding, ContentEncoding),
            ContentLanguage = COALESCE(@contentLanguage, ContentLanguage),
            ContentDisposition = COALESCE(@contentDisposition, ContentDisposition),
            CacheControl = COALESCE(@cacheControl, CacheControl),
            LastModified = @lastModified, ETag = @etag
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@contentType", (object?)httpHeaders.ContentType ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentEncoding", (object?)httpHeaders.ContentEncoding ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentLanguage", (object?)httpHeaders.ContentLanguage ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentDisposition", (object?)httpHeaders.ContentDisposition ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cacheControl", (object?)httpHeaders.CacheControl ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        return Response.FromValue(BlobsModelFactory.BlobInfo(new ETag(etag), now), StubResponse.Ok());
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
        var md5 = MD5.HashData(data);
        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";
        var versionId = now.ToString("yyyyMMddTHHmmssfffffffZ");

        var conditions = options?.Conditions;

        using var conn = _serviceClient.Db.Open();

        // Check IfNoneMatch = * (blob must not exist)
        if (conditions?.IfNoneMatch == ETag.All)
        {
            using var checkCmd = conn.CreateCommand();
            checkCmd.CommandText = "SELECT COUNT(*) FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            checkCmd.Parameters.AddWithValue("@container", _containerName);
            checkCmd.Parameters.AddWithValue("@blob", _blobName);
            if ((long)checkCmd.ExecuteScalar()! > 0)
                throw new RequestFailedException(409, "Blob already exists.", "BlobAlreadyExists", null);
        }

        // Check IfMatch (conditional update — ETag must match current)
        if (conditions?.IfMatch is not null && conditions.IfMatch != ETag.All)
        {
            using var matchCmd = conn.CreateCommand();
            matchCmd.CommandText = "SELECT ETag FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            matchCmd.Parameters.AddWithValue("@container", _containerName);
            matchCmd.Parameters.AddWithValue("@blob", _blobName);
            var currentEtag = matchCmd.ExecuteScalar() as string;
            if (currentEtag is null || conditions.IfMatch.ToString() != currentEtag)
                throw new RequestFailedException(412, "Condition not met.", "ConditionNotMet", null);
        }

        // Save previous version if blob exists
        using (var verCmd = conn.CreateCommand())
        {
            verCmd.CommandText = @"INSERT OR IGNORE INTO Versions (ContainerName, BlobName, VersionId, Content, Sidecar)
                SELECT ContainerName, BlobName, VersionId, Content,
                    json_object('BlobType', BlobType, 'ContentType', ContentType, 'ETag', ETag, 'Length', Length)
                FROM Blobs WHERE ContainerName = @container AND BlobName = @blob AND VersionId IS NOT NULL";
            verCmd.Parameters.AddWithValue("@container", _containerName);
            verCmd.Parameters.AddWithValue("@blob", _blobName);
            verCmd.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT OR REPLACE INTO Blobs
            (ContainerName, BlobName, BlobType, Content, ContentType, ContentEncoding, ContentLanguage,
             ContentDisposition, CacheControl, ContentHash, ETag, CreatedOn, LastModified, Length,
             AccessTier, Metadata, Tags, VersionId)
            VALUES (@container, @blob, 'Block', @content, @contentType, @contentEncoding, @contentLanguage,
                    @contentDisposition, @cacheControl, @contentHash, @etag,
                    COALESCE((SELECT CreatedOn FROM Blobs WHERE ContainerName = @container AND BlobName = @blob), @createdOn),
                    @lastModified, @length, @accessTier, @metadata,
                    (SELECT Tags FROM Blobs WHERE ContainerName = @container AND BlobName = @blob),
                    @versionId)";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.Parameters.AddWithValue("@content", data);
        cmd.Parameters.AddWithValue("@contentType", (object?)options?.HttpHeaders?.ContentType ?? "application/octet-stream");
        cmd.Parameters.AddWithValue("@contentEncoding", (object?)options?.HttpHeaders?.ContentEncoding ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentLanguage", (object?)options?.HttpHeaders?.ContentLanguage ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentDisposition", (object?)options?.HttpHeaders?.ContentDisposition ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cacheControl", (object?)options?.HttpHeaders?.CacheControl ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentHash", Convert.ToBase64String(md5));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@createdOn", now.ToString("o"));
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@length", (long)data.Length);
        cmd.Parameters.AddWithValue("@accessTier", DBNull.Value);
        cmd.Parameters.AddWithValue("@metadata", options?.Metadata is not null ? JsonSerializer.Serialize(options.Metadata) : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@versionId", versionId);
        cmd.ExecuteNonQuery();

        return Response.FromValue(BlobsModelFactory.BlobContentInfo(new ETag(etag), now, md5, null!, null!, null!, 0), StubResponse.Created());
    }

    private async Task<Response<BlobDownloadResult>> DownloadContentCoreAsync(BlobRequestConditions? conditions, CancellationToken ct)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"SELECT Content, BlobType, ContentType, ContentEncoding, ContentLanguage, ContentDisposition,
            CacheControl, ContentHash, ETag, CreatedOn, LastModified, Length, Metadata
            FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var content = reader.IsDBNull(0) ? Array.Empty<byte>() : (byte[])reader[0];
        var blobTypeStr = reader.IsDBNull(1) ? "Block" : reader.GetString(1);
        var contentType = reader.IsDBNull(2) ? "application/octet-stream" : reader.GetString(2);
        var contentEncoding = reader.IsDBNull(3) ? null : reader.GetString(3);
        var contentLanguage = reader.IsDBNull(4) ? null : reader.GetString(4);
        var contentDisposition = reader.IsDBNull(5) ? null : reader.GetString(5);
        var cacheControl = reader.IsDBNull(6) ? null : reader.GetString(6);
        var contentHashBase64 = reader.IsDBNull(7) ? null : reader.GetString(7);
        var etag = reader.GetString(8);
        var createdOn = DateTimeOffset.Parse(reader.GetString(9));
        var lastModified = DateTimeOffset.Parse(reader.GetString(10));
        var length = reader.GetInt64(11);
        var metadataJson = reader.IsDBNull(12) ? null : reader.GetString(12);

        var md5 = contentHashBase64 is not null ? Convert.FromBase64String(contentHashBase64) : null;
        IDictionary<string, string>? metadata = null;
        if (metadataJson is not null)
            metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(metadataJson);

        var blobType = blobTypeStr switch { "Append" => BlobType.Append, "Page" => BlobType.Page, _ => BlobType.Block };

        var details = BlobsModelFactory.BlobDownloadDetails(
            blobType: blobType, contentLength: length, contentType: contentType,
            contentHash: md5, lastModified: lastModified, metadata: metadata,
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
        return new SqliteCommitOnCloseStream(this, options?.HttpHeaders, options?.Metadata);
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
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Blobs SET AccessTier = @tier, LastModified = @lastModified WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@tier", tier.ToString());
        cmd.Parameters.AddWithValue("@lastModified", DateTimeOffset.UtcNow.ToString("o"));
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        return StubResponse.Ok();
    }

    /// <inheritdoc/>
    public override Response SetAccessTier(AccessTier tier, BlobRequestConditions conditions = null!, RehydratePriority? priority = null, CancellationToken ct = default)
        => SetAccessTierAsync(tier, conditions, priority, ct).GetAwaiter().GetResult();

    // ==== CreateSnapshot ====

    /// <inheritdoc/>
    public override async Task<Response<BlobSnapshotInfo>> CreateSnapshotAsync(IDictionary<string, string>? metadata = null, BlobRequestConditions conditions = null!, CancellationToken ct = default)
    {
        var snapshotId = DateTimeOffset.UtcNow.ToString("yyyyMMddTHHmmssfffZ");

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO Snapshots (ContainerName, BlobName, SnapshotId, Content, Sidecar)
            SELECT ContainerName, BlobName, @snapshotId, Content,
                json_object('BlobType', BlobType, 'ContentType', ContentType, 'ETag', ETag, 'Length', Length, 'Metadata', Metadata)
            FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@snapshotId", snapshotId);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        // Get current ETag/LastModified
        using var propCmd = conn.CreateCommand();
        propCmd.CommandText = "SELECT ETag, LastModified FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        propCmd.Parameters.AddWithValue("@container", _containerName);
        propCmd.Parameters.AddWithValue("@blob", _blobName);
        using var reader = propCmd.ExecuteReader();
        reader.Read();
        var etag = reader.GetString(0);
        var lastModified = DateTimeOffset.Parse(reader.GetString(1));

        var info = BlobsModelFactory.BlobSnapshotInfo(snapshot: snapshotId, eTag: new ETag(etag), lastModified: lastModified, isServerEncrypted: false);
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

        // Try to resolve from same database
        var segments = source.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length >= 2)
        {
            var srcContainer = segments[0];
            var srcBlob = Uri.UnescapeDataString(string.Join("/", segments.Skip(1)));
            using var conn = _serviceClient.Db.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT Content FROM Blobs WHERE ContainerName = @c AND BlobName = @b";
            cmd.Parameters.AddWithValue("@c", srcContainer);
            cmd.Parameters.AddWithValue("@b", srcBlob);
            var content = cmd.ExecuteScalar() as byte[];
            if (content != null)
            {
                sourceStream = new MemoryStream(content);
            }
            else
            {
                using var http = new HttpClient();
                var bytes = await http.GetByteArrayAsync(source.ToString()).ConfigureAwait(false);
                sourceStream = new MemoryStream(bytes);
            }
        }
        else
        {
            using var http = new HttpClient();
            var bytes = await http.GetByteArrayAsync(source, ct).ConfigureAwait(false);
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
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Tags FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var tagsJson = cmd.ExecuteScalar() as string;
        if (tagsJson is null)
        {
            // Check if blob exists
            using var checkCmd = conn.CreateCommand();
            checkCmd.CommandText = "SELECT COUNT(*) FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            checkCmd.Parameters.AddWithValue("@container", _containerName);
            checkCmd.Parameters.AddWithValue("@blob", _blobName);
            if ((long)checkCmd.ExecuteScalar()! == 0)
                throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        }
        var tags = tagsJson is not null ? JsonSerializer.Deserialize<Dictionary<string, string>>(tagsJson) ?? new Dictionary<string, string>() : new Dictionary<string, string>();
        var result = BlobsModelFactory.GetBlobTagResult(tags);
        return Response.FromValue(result, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<GetBlobTagResult> GetTags(BlobRequestConditions conditions = null!, CancellationToken ct = default)
        => GetTagsAsync(conditions, ct).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> SetTagsAsync(IDictionary<string, string> tags, BlobRequestConditions conditions = null!, CancellationToken ct = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Blobs SET Tags = @tags, LastModified = @lastModified WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@tags", JsonSerializer.Serialize(tags));
        cmd.Parameters.AddWithValue("@lastModified", DateTimeOffset.UtcNow.ToString("o"));
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
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

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ETag, LastModified FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        using var reader = cmd.ExecuteReader();
        var etag = "\"0x0\"";
        var lastModified = DateTimeOffset.UtcNow;
        if (reader.Read())
        {
            etag = reader.GetString(0);
            lastModified = DateTimeOffset.Parse(reader.GetString(1));
        }

        var info = BlobsModelFactory.BlobCopyInfo(
            eTag: new ETag(etag),
            lastModified: lastModified,
            copyId: Guid.NewGuid().ToString(),
            copyStatus: CopyStatus.Success);
        return Response.FromValue(info, StubResponse.Accepted());
    }

    /// <inheritdoc/>
    public override Response<BlobCopyInfo> SyncCopyFromUri(Uri source, BlobCopyFromUriOptions options = null!, CancellationToken ct = default)
        => SyncCopyFromUriAsync(source, options, ct).GetAwaiter().GetResult();

    // ==== Lease ====

    /// <inheritdoc/>
    protected override BlobLeaseClient GetBlobLeaseClientCore(string leaseId)
        => new SqliteBlobLeaseClient(this, leaseId);
}
