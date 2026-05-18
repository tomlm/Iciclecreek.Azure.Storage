using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>
/// SQLite-backed lease client for containers.
/// Lease state is stored in the Containers table columns (LeaseId, LeaseExpiresOn, LeaseDurationSeconds).
/// </summary>
public class SqliteContainerLeaseClient : BlobLeaseClient
{
    private readonly SqliteBlobContainerClient _container;
    private string _leaseId;

    internal SqliteContainerLeaseClient(SqliteBlobContainerClient container, string? leaseId) : base()
    {
        _container = container;
        _leaseId = leaseId ?? Guid.NewGuid().ToString();
    }

    public override string LeaseId => _leaseId;
    public new Uri Uri => _container.Uri;

    // ── Acquire ─────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> AcquireAsync(TimeSpan duration, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var secs = (int)duration.TotalSeconds;
        if (secs != -1 && (secs < 15 || secs > 60))
            throw new RequestFailedException(400, "Lease duration must be 15-60 seconds or -1 (infinite).", "InvalidHeaderValue", null);

        using var conn = _container._serviceClient.Db.Open();

        // Check for existing active lease
        using (var checkCmd = conn.CreateCommand())
        {
            checkCmd.CommandText = "SELECT LeaseId, LeaseExpiresOn, LeaseDurationSeconds FROM Containers WHERE Name = @name";
            checkCmd.Parameters.AddWithValue("@name", _container._containerName);
            using var reader = checkCmd.ExecuteReader();
            if (!reader.Read())
                throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

            var existingLeaseId = reader.IsDBNull(0) ? null : reader.GetString(0);
            var expiresOnStr = reader.IsDBNull(1) ? null : reader.GetString(1);
            var existingDuration = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

            if (existingLeaseId is not null)
            {
                bool isActive = existingDuration == -1 ||
                    (expiresOnStr is not null && DateTimeOffset.Parse(expiresOnStr) > DateTimeOffset.UtcNow);
                if (isActive)
                    throw new RequestFailedException(409, "There is already a lease present.", "LeaseAlreadyPresent", null);
            }
        }

        var expiresOn = secs == -1 ? (DateTimeOffset?)null : DateTimeOffset.UtcNow.AddSeconds(secs);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Containers SET LeaseId = @leaseId, LeaseExpiresOn = @expiresOn, LeaseDurationSeconds = @duration WHERE Name = @name";
        cmd.Parameters.AddWithValue("@leaseId", _leaseId);
        cmd.Parameters.AddWithValue("@expiresOn", expiresOn?.ToString("o") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@duration", secs);
        cmd.Parameters.AddWithValue("@name", _container._containerName);
        cmd.ExecuteNonQuery();

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, _leaseId), StubResponse.Created());
    }

    public override Response<BlobLease> Acquire(TimeSpan duration, RequestConditions? conditions = null, CancellationToken ct = default)
        => AcquireAsync(duration, conditions, ct).GetAwaiter().GetResult();

    // ── Renew ───────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> RenewAsync(RequestConditions? conditions = null, CancellationToken ct = default)
    {
        using var conn = _container._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId, LeaseDurationSeconds FROM Containers WHERE Name = @name";
        checkCmd.Parameters.AddWithValue("@name", _container._containerName);
        using var reader = checkCmd.ExecuteReader();
        if (!reader.Read())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var existingLeaseId = reader.IsDBNull(0) ? null : reader.GetString(0);
        if (existingLeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        var durationSecs = reader.GetInt32(1);
        reader.Close();

        var expiresOn = durationSecs == -1 ? (DateTimeOffset?)null : DateTimeOffset.UtcNow.AddSeconds(durationSecs);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Containers SET LeaseExpiresOn = @expiresOn WHERE Name = @name";
        cmd.Parameters.AddWithValue("@expiresOn", expiresOn?.ToString("o") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@name", _container._containerName);
        cmd.ExecuteNonQuery();

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, _leaseId), StubResponse.Ok());
    }

    public override Response<BlobLease> Renew(RequestConditions? conditions = null, CancellationToken ct = default)
        => RenewAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Release ─────────────────────────────────────────────────────────

    public override async Task<Response<ReleasedObjectInfo>> ReleaseAsync(RequestConditions? conditions = null, CancellationToken ct = default)
    {
        using var conn = _container._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId FROM Containers WHERE Name = @name";
        checkCmd.Parameters.AddWithValue("@name", _container._containerName);
        var existingLeaseId = checkCmd.ExecuteScalar() as string;
        if (existingLeaseId is null)
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (existingLeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Containers SET LeaseId = NULL, LeaseExpiresOn = NULL, LeaseDurationSeconds = 0 WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _container._containerName);
        cmd.ExecuteNonQuery();

        return Response.FromValue(new ReleasedObjectInfo(ETag.All, DateTimeOffset.UtcNow), StubResponse.Ok());
    }

    public override Response<ReleasedObjectInfo> Release(RequestConditions? conditions = null, CancellationToken ct = default)
        => ReleaseAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Change ──────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> ChangeAsync(string proposedId, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        using var conn = _container._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId FROM Containers WHERE Name = @name";
        checkCmd.Parameters.AddWithValue("@name", _container._containerName);
        var existingLeaseId = checkCmd.ExecuteScalar() as string;
        if (existingLeaseId is null)
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (existingLeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Containers SET LeaseId = @newLeaseId WHERE Name = @name";
        cmd.Parameters.AddWithValue("@newLeaseId", proposedId);
        cmd.Parameters.AddWithValue("@name", _container._containerName);
        cmd.ExecuteNonQuery();

        _leaseId = proposedId;

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, proposedId), StubResponse.Ok());
    }

    public override Response<BlobLease> Change(string proposedId, RequestConditions? conditions = null, CancellationToken ct = default)
        => ChangeAsync(proposedId, conditions, ct).GetAwaiter().GetResult();

    // ── Break ───────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> BreakAsync(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        using var conn = _container._serviceClient.Db.Open();

        using var checkCmd = conn.CreateCommand();
        checkCmd.CommandText = "SELECT LeaseId, LeaseExpiresOn, LeaseDurationSeconds FROM Containers WHERE Name = @name";
        checkCmd.Parameters.AddWithValue("@name", _container._containerName);
        using var reader = checkCmd.ExecuteReader();
        if (!reader.Read())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var existingLeaseId = reader.IsDBNull(0) ? null : reader.GetString(0);
        var expiresOnStr = reader.IsDBNull(1) ? null : reader.GetString(1);
        var durationSecs = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

        bool isActive = existingLeaseId is not null &&
            (durationSecs == -1 || (expiresOnStr is not null && DateTimeOffset.Parse(expiresOnStr) > DateTimeOffset.UtcNow));

        if (!isActive)
            throw new RequestFailedException(409, "There is currently no lease on the container.", "LeaseNotPresentWithLeaseOperation", null);

        reader.Close();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Containers SET LeaseId = NULL, LeaseExpiresOn = NULL, LeaseDurationSeconds = 0 WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _container._containerName);
        cmd.ExecuteNonQuery();

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, existingLeaseId), StubResponse.Ok());
    }

    public override Response<BlobLease> Break(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken ct = default)
        => BreakAsync(breakPeriod, conditions, ct).GetAwaiter().GetResult();
}
