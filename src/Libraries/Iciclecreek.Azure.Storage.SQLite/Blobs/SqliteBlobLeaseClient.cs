using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>
/// SQLite-backed drop-in replacement for <see cref="BlobLeaseClient"/>.
/// Lease state is stored in the Blobs table columns (LeaseId, LeaseExpiresOn, LeaseDurationSeconds).
/// </summary>
public class SqliteBlobLeaseClient : BlobLeaseClient
{
    private readonly SqliteBlobClient _blob;
    private string _leaseId;

    internal SqliteBlobLeaseClient(SqliteBlobClient blob, string? leaseId) : base()
    {
        _blob = blob;
        _leaseId = leaseId ?? Guid.NewGuid().ToString();
    }

    /// <inheritdoc/>
    public override string LeaseId => _leaseId;

    public new Uri Uri => _blob.Uri;

    // ── Acquire ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> AcquireAsync(TimeSpan duration, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var durationSecs = (int)duration.TotalSeconds;
        if (durationSecs != -1 && (durationSecs < 15 || durationSecs > 60))
            throw new RequestFailedException(400, "Lease duration must be 15-60 seconds or -1 (infinite).", "InvalidHeaderValue", null);

        using var conn = _blob._serviceClient.Db.Open();

        // Check for existing active lease
        using (var checkCmd = conn.CreateCommand())
        {
            checkCmd.CommandText = @"SELECT LeaseId, LeaseExpiresOn, LeaseDurationSeconds
                FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
            checkCmd.Parameters.AddWithValue("@container", _blob._containerName);
            checkCmd.Parameters.AddWithValue("@blob", _blob._blobName);
            using var reader = checkCmd.ExecuteReader();
            if (!reader.Read())
                throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

            var existingLeaseId = reader.IsDBNull(0) ? null : reader.GetString(0);
            var expiresOnStr = reader.IsDBNull(1) ? null : reader.GetString(1);
            var existingDuration = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

            if (existingLeaseId is not null)
            {
                // Check if lease is still active
                bool isActive = existingDuration == -1 ||
                    (expiresOnStr is not null && DateTimeOffset.Parse(expiresOnStr) > DateTimeOffset.UtcNow);
                if (isActive)
                    throw new RequestFailedException(409, "There is already a lease present.", "LeaseAlreadyPresent", null);
            }
        }

        var expiresOn = durationSecs == -1
            ? (DateTimeOffset?)null
            : DateTimeOffset.UtcNow.AddSeconds(durationSecs);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET LeaseId = @leaseId, LeaseExpiresOn = @expiresOn,
            LeaseDurationSeconds = @duration, LastModified = @lastModified
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@leaseId", _leaseId);
        cmd.Parameters.AddWithValue("@expiresOn", expiresOn?.ToString("o") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@duration", durationSecs);
        cmd.Parameters.AddWithValue("@lastModified", DateTimeOffset.UtcNow.ToString("o"));
        cmd.Parameters.AddWithValue("@container", _blob._containerName);
        cmd.Parameters.AddWithValue("@blob", _blob._blobName);
        cmd.ExecuteNonQuery();

        // Get ETag
        using var etagCmd = conn.CreateCommand();
        etagCmd.CommandText = "SELECT ETag, LastModified FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        etagCmd.Parameters.AddWithValue("@container", _blob._containerName);
        etagCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        using var etagReader = etagCmd.ExecuteReader();
        etagReader.Read();
        var etag = etagReader.GetString(0);
        var lastModified = DateTimeOffset.Parse(etagReader.GetString(1));

        var lease = BlobsModelFactory.BlobLease(new ETag(etag), lastModified, _leaseId);
        return Response.FromValue(lease, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Acquire(TimeSpan duration, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => AcquireAsync(duration, conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Renew ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> RenewAsync(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        using var conn = _blob._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId, LeaseDurationSeconds FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        checkCmd.Parameters.AddWithValue("@container", _blob._containerName);
        checkCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        using var reader = checkCmd.ExecuteReader();
        if (!reader.Read())
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var existingLeaseId = reader.IsDBNull(0) ? null : reader.GetString(0);
        if (existingLeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        var durationSecs = reader.GetInt32(1);
        reader.Close();

        var expiresOn = durationSecs == -1
            ? (DateTimeOffset?)null
            : DateTimeOffset.UtcNow.AddSeconds(durationSecs);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET LeaseExpiresOn = @expiresOn, LastModified = @lastModified
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@expiresOn", expiresOn?.ToString("o") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@lastModified", DateTimeOffset.UtcNow.ToString("o"));
        cmd.Parameters.AddWithValue("@container", _blob._containerName);
        cmd.Parameters.AddWithValue("@blob", _blob._blobName);
        cmd.ExecuteNonQuery();

        using var etagCmd = conn.CreateCommand();
        etagCmd.CommandText = "SELECT ETag, LastModified FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        etagCmd.Parameters.AddWithValue("@container", _blob._containerName);
        etagCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        using var etagReader = etagCmd.ExecuteReader();
        etagReader.Read();

        var lease = BlobsModelFactory.BlobLease(new ETag(etagReader.GetString(0)), DateTimeOffset.Parse(etagReader.GetString(1)), _leaseId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Renew(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => RenewAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Release ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<ReleasedObjectInfo>> ReleaseAsync(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        using var conn = _blob._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        checkCmd.Parameters.AddWithValue("@container", _blob._containerName);
        checkCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        var existingLeaseId = checkCmd.ExecuteScalar() as string;
        if (existingLeaseId is null)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        if (existingLeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET LeaseId = NULL, LeaseExpiresOn = NULL, LeaseDurationSeconds = 0,
            LastModified = @lastModified
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@lastModified", DateTimeOffset.UtcNow.ToString("o"));
        cmd.Parameters.AddWithValue("@container", _blob._containerName);
        cmd.Parameters.AddWithValue("@blob", _blob._blobName);
        cmd.ExecuteNonQuery();

        using var etagCmd = conn.CreateCommand();
        etagCmd.CommandText = "SELECT ETag, LastModified FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        etagCmd.Parameters.AddWithValue("@container", _blob._containerName);
        etagCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        using var etagReader = etagCmd.ExecuteReader();
        etagReader.Read();

        var info = new ReleasedObjectInfo(new ETag(etagReader.GetString(0)), DateTimeOffset.Parse(etagReader.GetString(1)));
        return Response.FromValue(info, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<ReleasedObjectInfo> Release(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => ReleaseAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Change ──────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> ChangeAsync(string proposedId, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        using var conn = _blob._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        checkCmd.Parameters.AddWithValue("@container", _blob._containerName);
        checkCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        var existingLeaseId = checkCmd.ExecuteScalar() as string;
        if (existingLeaseId is null)
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        if (existingLeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET LeaseId = @newLeaseId, LastModified = @lastModified
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@newLeaseId", proposedId);
        cmd.Parameters.AddWithValue("@lastModified", DateTimeOffset.UtcNow.ToString("o"));
        cmd.Parameters.AddWithValue("@container", _blob._containerName);
        cmd.Parameters.AddWithValue("@blob", _blob._blobName);
        cmd.ExecuteNonQuery();

        _leaseId = proposedId;

        using var etagCmd = conn.CreateCommand();
        etagCmd.CommandText = "SELECT ETag, LastModified FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        etagCmd.Parameters.AddWithValue("@container", _blob._containerName);
        etagCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        using var etagReader = etagCmd.ExecuteReader();
        etagReader.Read();

        var lease = BlobsModelFactory.BlobLease(new ETag(etagReader.GetString(0)), DateTimeOffset.Parse(etagReader.GetString(1)), proposedId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Change(string proposedId, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => ChangeAsync(proposedId, conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Break ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> BreakAsync(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        using var conn = _blob._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId, LeaseExpiresOn, LeaseDurationSeconds FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        checkCmd.Parameters.AddWithValue("@container", _blob._containerName);
        checkCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        using var reader = checkCmd.ExecuteReader();
        if (!reader.Read())
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        var existingLeaseId = reader.IsDBNull(0) ? null : reader.GetString(0);
        var expiresOnStr = reader.IsDBNull(1) ? null : reader.GetString(1);
        var durationSecs = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

        bool isActive = existingLeaseId is not null &&
            (durationSecs == -1 || (expiresOnStr is not null && DateTimeOffset.Parse(expiresOnStr) > DateTimeOffset.UtcNow));

        if (!isActive)
            throw new RequestFailedException(409, "There is currently no lease on the blob.", "LeaseNotPresentWithLeaseOperation", null);

        reader.Close();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"UPDATE Blobs SET LeaseId = NULL, LeaseExpiresOn = NULL, LeaseDurationSeconds = 0,
            LastModified = @lastModified
            WHERE ContainerName = @container AND BlobName = @blob";
        cmd.Parameters.AddWithValue("@lastModified", DateTimeOffset.UtcNow.ToString("o"));
        cmd.Parameters.AddWithValue("@container", _blob._containerName);
        cmd.Parameters.AddWithValue("@blob", _blob._blobName);
        cmd.ExecuteNonQuery();

        using var etagCmd = conn.CreateCommand();
        etagCmd.CommandText = "SELECT ETag, LastModified FROM Blobs WHERE ContainerName = @container AND BlobName = @blob";
        etagCmd.Parameters.AddWithValue("@container", _blob._containerName);
        etagCmd.Parameters.AddWithValue("@blob", _blob._blobName);
        using var etagReader = etagCmd.ExecuteReader();
        etagReader.Read();

        var lease = BlobsModelFactory.BlobLease(new ETag(etagReader.GetString(0)), DateTimeOffset.Parse(etagReader.GetString(1)), existingLeaseId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Break(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => BreakAsync(breakPeriod, conditions, cancellationToken).GetAwaiter().GetResult();
}
