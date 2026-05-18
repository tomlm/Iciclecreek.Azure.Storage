using System.Security.Cryptography;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>SQLite-backed drop-in replacement for <see cref="PageBlobClient"/>.</summary>
public class SqlitePageBlobClient : PageBlobClient
{
    internal readonly SqliteBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    internal SqlitePageBlobClient(SqliteBlobServiceClient serviceClient, string containerName, string blobName) : base()
    {
        _serviceClient = serviceClient;
        _containerName = containerName;
        _blobName = blobName;
    }

    // ── Properties ──────────────────────────────────────────────────────

    public override string AccountName => _serviceClient.AccountName;
    public override string BlobContainerName => _containerName;
    public override string Name => _blobName;
    public override Uri Uri => new($"{_serviceClient.Uri}{_containerName}/{_blobName}");

    // ── Create ──────────────────────────────────────────────────────────

    public override async Task<Response<BlobContentInfo>> CreateAsync(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
    {
        if (size % 512 != 0)
            throw new RequestFailedException(400, "Page blob size must be a multiple of 512.", "InvalidHeaderValue", null);

        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";
        var sequenceNumber = options?.SequenceNumber ?? 0;

        // Create zero-filled content
        var content = new byte[size];

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT OR REPLACE INTO Blobs
            (ContainerName, BlobName, BlobType, Content, ContentType, ContentEncoding, ContentLanguage,
             ContentDisposition, CacheControl, ETag, CreatedOn, LastModified, Length, SequenceNumber, Metadata, Tags)
            VALUES (@container, @blob, 'Page', @content, @contentType, @contentEncoding, @contentLanguage,
                    @contentDisposition, @cacheControl, @etag, @createdOn, @lastModified, @length, @sequenceNumber, @metadata, @tags)";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.Parameters.AddWithValue("@content", content);
        cmd.Parameters.AddWithValue("@contentType", (object?)options?.HttpHeaders?.ContentType ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentEncoding", (object?)options?.HttpHeaders?.ContentEncoding ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentLanguage", (object?)options?.HttpHeaders?.ContentLanguage ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@contentDisposition", (object?)options?.HttpHeaders?.ContentDisposition ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cacheControl", (object?)options?.HttpHeaders?.CacheControl ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@createdOn", now.ToString("o"));
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@length", size);
        cmd.Parameters.AddWithValue("@sequenceNumber", sequenceNumber);
        cmd.Parameters.AddWithValue("@metadata", options?.Metadata is not null ? System.Text.Json.JsonSerializer.Serialize(options.Metadata) : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@tags", options?.Tags is not null ? System.Text.Json.JsonSerializer.Serialize(options.Tags) : (object)DBNull.Value);
        cmd.ExecuteNonQuery();

        // Clear any existing page ranges
        using var delRanges = conn.CreateCommand();
        delRanges.CommandText = "DELETE FROM PageRanges WHERE ContainerName = @container AND BlobName = @blob";
        delRanges.Parameters.AddWithValue("@container", _containerName);
        delRanges.Parameters.AddWithValue("@blob", _blobName);
        delRanges.ExecuteNonQuery();

        var info = BlobsModelFactory.BlobContentInfo(
            eTag: new ETag(etag), lastModified: now,
            contentHash: null, versionId: null, encryptionKeySha256: null, encryptionScope: null, blobSequenceNumber: sequenceNumber);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<BlobContentInfo> Create(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
        => CreateAsync(size, options, ct).GetAwaiter().GetResult();

    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT ETag, LastModified, SequenceNumber FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        checkCmd.Parameters.AddWithValue("@container", _containerName);
        checkCmd.Parameters.AddWithValue("@blob", _blobName);
        using var reader = checkCmd.ExecuteReader();
        if (reader.Read())
        {
            var existingEtag = reader.GetString(0);
            var existingLastModified = DateTimeOffset.Parse(reader.GetString(1));
            var existingSeqNum = reader.IsDBNull(2) ? 0L : reader.GetInt64(2);
            var info = BlobsModelFactory.BlobContentInfo(
                eTag: new ETag(existingEtag), lastModified: existingLastModified,
                contentHash: null, versionId: null, encryptionKeySha256: null, encryptionScope: null, blobSequenceNumber: existingSeqNum);
            return Response.FromValue(info, StubResponse.Ok());
        }
        return await CreateAsync(size, options, ct).ConfigureAwait(false);
    }

    public override Response<BlobContentInfo> CreateIfNotExists(long size, PageBlobCreateOptions? options = null, CancellationToken ct = default)
        => CreateIfNotExistsAsync(size, options, ct).GetAwaiter().GetResult();

    // ── UploadPages ─────────────────────────────────────────────────────

    public override async Task<Response<PageInfo>> UploadPagesAsync(Stream content, long offset, PageBlobUploadPagesOptions? options = null, CancellationToken ct = default)
    {
        if (offset % 512 != 0)
            throw new RequestFailedException(400, "Page offset must be a multiple of 512.", "InvalidHeaderValue", null);

        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, ct).ConfigureAwait(false);
        var data = ms.ToArray();

        if (data.Length % 512 != 0)
            throw new RequestFailedException(400, "Page data length must be a multiple of 512.", "InvalidHeaderValue", null);

        using var conn = _serviceClient.Db.Open();

        // Read existing content
        byte[] existingContent;
        using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText = "SELECT Content FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            readCmd.Parameters.AddWithValue("@container", _containerName);
            readCmd.Parameters.AddWithValue("@blob", _blobName);
            var result = readCmd.ExecuteScalar();
            if (result is null || result is DBNull)
                throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
            existingContent = (byte[])result;
        }

        // Write pages at offset
        Array.Copy(data, 0, existingContent, offset, data.Length);

        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";

        // Update blob content and increment sequence number
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET Content = @content, LastModified = @lastModified, ETag = @etag,
            SequenceNumber = SequenceNumber + 1
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@content", existingContent);
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.ExecuteNonQuery();

        // Track page range — merge with existing
        MergePageRange(conn, offset, data.Length);

        // Get updated sequence number
        long seqNum;
        using (var seqCmd = conn.CreateCommand())
        {
            seqCmd.CommandText = "SELECT SequenceNumber FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            seqCmd.Parameters.AddWithValue("@container", _containerName);
            seqCmd.Parameters.AddWithValue("@blob", _blobName);
            seqNum = (long)seqCmd.ExecuteScalar()!;
        }

        var info = BlobsModelFactory.PageInfo(new ETag(etag), now, null, null, seqNum, null);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<PageInfo> UploadPages(Stream content, long offset, PageBlobUploadPagesOptions? options = null, CancellationToken ct = default)
        => UploadPagesAsync(content, offset, options, ct).GetAwaiter().GetResult();

    public override async Task<Response<PageInfo>> UploadPagesAsync(Stream content, long offset, byte[]? transactionalContentHash = null, PageBlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, CancellationToken ct = default)
        => await UploadPagesAsync(content, offset, (PageBlobUploadPagesOptions?)null, ct).ConfigureAwait(false);

    public override Response<PageInfo> UploadPages(Stream content, long offset, byte[]? transactionalContentHash = null, PageBlobRequestConditions? conditions = null, IProgress<long>? progressHandler = null, CancellationToken ct = default)
        => UploadPagesAsync(content, offset, (PageBlobUploadPagesOptions?)null, ct).GetAwaiter().GetResult();

    // ── ClearPages ──────────────────────────────────────────────────────

    public override async Task<Response<PageInfo>> ClearPagesAsync(HttpRange range, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        using var conn = _serviceClient.Db.Open();

        // Read existing content
        byte[] existingContent;
        long blobLength;
        using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText = "SELECT Content, Length FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            readCmd.Parameters.AddWithValue("@container", _containerName);
            readCmd.Parameters.AddWithValue("@blob", _blobName);
            using var reader = readCmd.ExecuteReader();
            if (!reader.Read())
                throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
            existingContent = (byte[])reader[0];
            blobLength = reader.GetInt64(1);
        }

        var offset = range.Offset;
        var length = range.Length ?? (blobLength - offset);

        // Clear pages (zero out)
        Array.Clear(existingContent, (int)offset, (int)length);

        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET Content = @content, LastModified = @lastModified, ETag = @etag,
            SequenceNumber = SequenceNumber + 1
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@content", existingContent);
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.ExecuteNonQuery();

        RemovePageRange(conn, offset, length);

        long seqNum;
        using (var seqCmd = conn.CreateCommand())
        {
            seqCmd.CommandText = "SELECT SequenceNumber FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            seqCmd.Parameters.AddWithValue("@container", _containerName);
            seqCmd.Parameters.AddWithValue("@blob", _blobName);
            seqNum = (long)seqCmd.ExecuteScalar()!;
        }

        var info = BlobsModelFactory.PageInfo(new ETag(etag), now, null, null, seqNum, null);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageInfo> ClearPages(HttpRange range, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => ClearPagesAsync(range, conditions, ct).GetAwaiter().GetResult();

    // ── GetPageRanges ───────────────────────────────────────────────────

    public override async Task<Response<PageRangesInfo>> GetPageRangesAsync(HttpRange? range = null, string? snapshot = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        using var conn = _serviceClient.Db.Open();

        // Get blob metadata
        string etag;
        DateTimeOffset lastModified;
        long blobLength;
        using (var blobCmd = conn.CreateCommand())
        {
            blobCmd.CommandText = "SELECT ETag, LastModified, Length FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            blobCmd.Parameters.AddWithValue("@container", _containerName);
            blobCmd.Parameters.AddWithValue("@blob", _blobName);
            using var blobReader = blobCmd.ExecuteReader();
            if (!blobReader.Read())
                throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
            etag = blobReader.GetString(0);
            lastModified = DateTimeOffset.Parse(blobReader.GetString(1));
            blobLength = blobReader.GetInt64(2);
        }

        var pageRanges = new List<HttpRange>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Offset, Length FROM PageRanges WHERE ContainerName = @container AND BlobName = @blob ORDER BY Offset";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            pageRanges.Add(new HttpRange(reader.GetInt64(0), reader.GetInt64(1)));

        var info = BlobsModelFactory.PageRangesInfo(lastModified, new ETag(etag), blobLength, pageRanges, Array.Empty<HttpRange>());
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageRangesInfo> GetPageRanges(HttpRange? range = null, string? snapshot = null, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => GetPageRangesAsync(range, snapshot, conditions, ct).GetAwaiter().GetResult();

    // ── Resize ──────────────────────────────────────────────────────────

    public override async Task<Response<PageBlobInfo>> ResizeAsync(long size, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
    {
        if (size % 512 != 0)
            throw new RequestFailedException(400, "Page blob size must be a multiple of 512.", "InvalidHeaderValue", null);

        using var conn = _serviceClient.Db.Open();

        // Read existing content
        byte[] existingContent;
        using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText = "SELECT Content FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            readCmd.Parameters.AddWithValue("@container", _containerName);
            readCmd.Parameters.AddWithValue("@blob", _blobName);
            var result = readCmd.ExecuteScalar();
            if (result is null || result is DBNull)
                throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
            existingContent = (byte[])result;
        }

        // Resize content
        var newContent = new byte[size];
        Array.Copy(existingContent, newContent, Math.Min(existingContent.Length, (int)size));

        var now = DateTimeOffset.UtcNow;
        var etag = $"\"0x{Guid.NewGuid():N}\"";

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET Content = @content, Length = @length, LastModified = @lastModified,
            ETag = @etag, SequenceNumber = SequenceNumber + 1
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@content", newContent);
        cmd.Parameters.AddWithValue("@length", size);
        cmd.Parameters.AddWithValue("@lastModified", now.ToString("o"));
        cmd.Parameters.AddWithValue("@etag", etag);
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        cmd.ExecuteNonQuery();

        // Remove page ranges beyond new size
        using var delRanges = conn.CreateCommand();
        delRanges.CommandText = "DELETE FROM PageRanges WHERE ContainerName = @container AND BlobName = @blob AND Offset >= @size";
        delRanges.Parameters.AddWithValue("@container", _containerName);
        delRanges.Parameters.AddWithValue("@blob", _blobName);
        delRanges.Parameters.AddWithValue("@size", size);
        delRanges.ExecuteNonQuery();

        long seqNum;
        using (var seqCmd = conn.CreateCommand())
        {
            seqCmd.CommandText = "SELECT SequenceNumber FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            seqCmd.Parameters.AddWithValue("@container", _containerName);
            seqCmd.Parameters.AddWithValue("@blob", _blobName);
            seqNum = (long)seqCmd.ExecuteScalar()!;
        }

        var info = BlobsModelFactory.PageBlobInfo(new ETag(etag), now, seqNum);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override Response<PageBlobInfo> Resize(long size, PageBlobRequestConditions? conditions = null, CancellationToken ct = default)
        => ResizeAsync(size, conditions, ct).GetAwaiter().GetResult();

    // ── GetProperties ───────────────────────────────────────────────────

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken ct = default)
    {
        var blobClient = new SqliteBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, ct).ConfigureAwait(false);
    }

    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken ct = default)
        => GetPropertiesAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Helpers ──────────────────────────────────────────────────────────

    private void MergePageRange(Microsoft.Data.Sqlite.SqliteConnection conn, long offset, long length)
    {
        // Insert new range
        using var insertCmd = conn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO PageRanges (ContainerName, BlobName, Offset, Length) VALUES (@container, @blob, @offset, @length)";
        insertCmd.Parameters.AddWithValue("@container", _containerName);
        insertCmd.Parameters.AddWithValue("@blob", _blobName);
        insertCmd.Parameters.AddWithValue("@offset", offset);
        insertCmd.Parameters.AddWithValue("@length", length);
        insertCmd.ExecuteNonQuery();

        // Read all ranges, merge, and rewrite
        var ranges = ReadPageRanges(conn);
        var merged = MergeRanges(ranges);
        WritePageRanges(conn, merged);
    }

    private void RemovePageRange(Microsoft.Data.Sqlite.SqliteConnection conn, long offset, long length)
    {
        var ranges = ReadPageRanges(conn);
        var clearEnd = offset + length;
        var result = new List<(long Offset, long Length)>();

        foreach (var r in ranges)
        {
            var rEnd = r.Offset + r.Length;
            if (rEnd <= offset || r.Offset >= clearEnd)
            {
                result.Add(r);
            }
            else
            {
                if (r.Offset < offset)
                    result.Add((r.Offset, offset - r.Offset));
                if (rEnd > clearEnd)
                    result.Add((clearEnd, rEnd - clearEnd));
            }
        }
        WritePageRanges(conn, result);
    }

    private List<(long Offset, long Length)> ReadPageRanges(Microsoft.Data.Sqlite.SqliteConnection conn)
    {
        var ranges = new List<(long Offset, long Length)>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Offset, Length FROM PageRanges WHERE ContainerName = @container AND BlobName = @blob ORDER BY Offset";
        cmd.Parameters.AddWithValue("@container", _containerName);
        cmd.Parameters.AddWithValue("@blob", _blobName);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            ranges.Add((reader.GetInt64(0), reader.GetInt64(1)));
        return ranges;
    }

    private void WritePageRanges(Microsoft.Data.Sqlite.SqliteConnection conn, List<(long Offset, long Length)> ranges)
    {
        using var delCmd = conn.CreateCommand();
        delCmd.CommandText = "DELETE FROM PageRanges WHERE ContainerName = @container AND BlobName = @blob";
        delCmd.Parameters.AddWithValue("@container", _containerName);
        delCmd.Parameters.AddWithValue("@blob", _blobName);
        delCmd.ExecuteNonQuery();

        foreach (var r in ranges)
        {
            using var insertCmd = conn.CreateCommand();
            insertCmd.CommandText = "INSERT INTO PageRanges (ContainerName, BlobName, Offset, Length) VALUES (@container, @blob, @offset, @length)";
            insertCmd.Parameters.AddWithValue("@container", _containerName);
            insertCmd.Parameters.AddWithValue("@blob", _blobName);
            insertCmd.Parameters.AddWithValue("@offset", r.Offset);
            insertCmd.Parameters.AddWithValue("@length", r.Length);
            insertCmd.ExecuteNonQuery();
        }
    }

    private static List<(long Offset, long Length)> MergeRanges(List<(long Offset, long Length)> ranges)
    {
        if (ranges.Count == 0) return ranges;
        ranges.Sort((a, b) => a.Offset.CompareTo(b.Offset));
        var merged = new List<(long Offset, long Length)> { ranges[0] };
        for (int i = 1; i < ranges.Count; i++)
        {
            var last = merged[^1];
            var lastEnd = last.Offset + last.Length;
            if (lastEnd >= ranges[i].Offset)
            {
                var newEnd = Math.Max(lastEnd, ranges[i].Offset + ranges[i].Length);
                merged[^1] = (last.Offset, newEnd - last.Offset);
            }
            else
            {
                merged.Add(ranges[i]);
            }
        }
        return merged;
    }
}
