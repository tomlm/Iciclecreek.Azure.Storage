using System.Security.Cryptography;
using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>SQLite-backed drop-in replacement for <see cref="AppendBlobClient"/>.</summary>
public class SqliteAppendBlobClient : AppendBlobClient
{
    internal readonly SqliteBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    internal SqliteAppendBlobClient(SqliteBlobServiceClient serviceClient, string containerName, string blobName) : base()
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
        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";

        using var conn = _serviceClient.Db.Open();

        // Check IfNoneMatch condition
        if (options?.Conditions?.IfNoneMatch == ETag.All)
        {
            using var checkCmd = conn.CreateCommand();
            checkCmd.CommandText = "SELECT COUNT(*) FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            checkCmd.Parameters.AddWithValue("@container", _containerName);
            checkCmd.Parameters.AddWithValue("@blob", _blobName);
            if ((long)checkCmd.ExecuteScalar()! > 0)
                throw new RequestFailedException(409, "Blob already exists.", "BlobAlreadyExists", null);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT OR REPLACE INTO Blobs
            (ContainerName, BlobName, BlobType, Content, ContentType, ContentEncoding,
             ContentHash, ETag, CreatedOn, LastModified, Length, Metadata)
            VALUES (@container, @blob, 'Append', @content, @contentType, @contentEncoding,
                    @contentHash, @etag, @createdOn, @lastModified, 0, @metadata)";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.Parameters.AddWithValue("@content", Array.Empty<byte>());
        cmd.Parameters.AddWithValue("@contentType", (object?)options?.HttpHeaders?.ContentType ?? "application/octet-stream");
        cmd.Parameters.AddWithValue("@contentEncoding", (object?)options?.HttpHeaders?.ContentEncoding ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentHash", DBNull.Value);
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@createdOn", now.ToString("o"));
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@metadata", options?.Metadata is not null ? JsonSerializer.Serialize(options.Metadata) : (object)DBNull.Value);
        cmd.ExecuteNonQuery();

        var info = BlobsModelFactory.BlobContentInfo(new ETag(etag), now, null, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => await CreateAsync(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(*) FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        checkCmd.Parameters.AddWithValue("@container", _containerName);
        checkCmd.Parameters.AddWithValue("@blob", _blobName);
        if ((long)checkCmd.ExecuteScalar()! > 0)
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

        using var conn = _serviceClient.Db.Open();

        // Read existing content
        byte[] existingContent;
        using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText = "SELECT Content FROM Blobs WHERE ContainerName = @container AND BlobName = @blob AND BlobType = 'Append'";
            readCmd.Parameters.AddWithValue("@container", _containerName);
            readCmd.Parameters.AddWithValue("@blob", _blobName);
            var result = readCmd.ExecuteScalar();
            if (result is null || result is DBNull)
                throw new RequestFailedException(404, "Append blob not found. Call Create first.", "BlobNotFound", null);
            existingContent = (byte[])result;
        }

        // Append
        var newContent = new byte[existingContent.Length + appendData.Length];
        existingContent.CopyTo(newContent, 0);
        appendData.CopyTo(newContent, existingContent.Length);

        var md5 = MD5.HashData(newContent);
        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";
        var newLength = (long)newContent.Length;

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET Content = @content, ContentHash = @contentHash, ETag = @etag,
            LastModified = @lastModified, Length = @length
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@content", newContent);
        cmd.Parameters.AddWithValue("@contentHash", Convert.ToBase64String(md5));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@length", newLength);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.ExecuteNonQuery();

        var info = BlobsModelFactory.BlobAppendInfo(new ETag(etag), now, md5, null, newLength.ToString(), 0, false, null!, null!);
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
    public override async Task<Stream> OpenWriteAsync(bool overwrite, AppendBlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.OpenWriteAsync(overwrite, null, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Stream OpenWrite(bool overwrite, AppendBlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
        => OpenWriteAsync(overwrite, options, cancellationToken).GetAwaiter().GetResult();

    // ---- Seal ----
    /// <inheritdoc/>
    public new async Task<Response<BlobInfo>> SealAsync(AppendBlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Blobs SET IsSealed = 1, LastModified = @lastModified, ETag = @etag WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Append blob not found.", "BlobNotFound", null);

        var info = BlobsModelFactory.BlobInfo(new ETag(etag), now);
        return Response.FromValue(info, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public new Response<BlobInfo> Seal(AppendBlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
        => SealAsync(conditions, cancellationToken).GetAwaiter().GetResult();
}
