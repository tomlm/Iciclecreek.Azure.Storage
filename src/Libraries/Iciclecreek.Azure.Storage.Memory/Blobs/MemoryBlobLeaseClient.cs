using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>
/// In-memory drop-in replacement for <see cref="BlobLeaseClient"/> (blob-level leases).
/// Lease state is stored in <see cref="BlobEntry.Lease"/>.
/// </summary>
public class MemoryBlobLeaseClient : BlobLeaseClient
{
    private readonly MemoryBlobClient _blob;
    private string _leaseId;

    internal MemoryBlobLeaseClient(MemoryBlobClient blob, string? leaseId) : base()
    {
        _blob = blob;
        _leaseId = leaseId ?? Guid.NewGuid().ToString();
    }

    /// <inheritdoc/>
    public override string LeaseId => _leaseId;

    public new Uri Uri => _blob.Uri;

    private BlobEntry GetEntry()
    {
        if (!_blob._serviceClient.Containers.TryGetValue(_blob._containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        if (!store.Blobs.TryGetValue(_blob._blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        return entry;
    }

    // ── Acquire ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> AcquireAsync(TimeSpan duration, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var durationSecs = (int)duration.TotalSeconds;
        if (durationSecs != -1 && (durationSecs < 15 || durationSecs > 60))
            throw new RequestFailedException(400, "Lease duration must be 15-60 seconds or -1 (infinite).", "InvalidHeaderValue", null);

        var entry = GetEntry();
        lock (entry.Lock)
        {
            if (entry.Lease.IsActive)
                throw new RequestFailedException(409, "There is already a lease present.", "LeaseAlreadyPresent", null);

            entry.Lease.LeaseId = _leaseId;
            entry.Lease.Duration = TimeSpan.FromSeconds(durationSecs);
            entry.Lease.ExpiresAt = durationSecs == -1 ? null : DateTimeOffset.UtcNow.AddSeconds(durationSecs);
        }

        var lease = BlobsModelFactory.BlobLease(new ETag(entry.ETag), entry.LastModified, _leaseId);
        return Response.FromValue(lease, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Acquire(TimeSpan duration, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => AcquireAsync(duration, conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Renew ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> RenewAsync(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var entry = GetEntry();
        lock (entry.Lock)
        {
            if (entry.Lease.LeaseId != _leaseId)
                throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

            if (entry.Lease.Duration?.TotalSeconds != -1)
                entry.Lease.ExpiresAt = DateTimeOffset.UtcNow.Add(entry.Lease.Duration ?? TimeSpan.Zero);
        }

        var lease = BlobsModelFactory.BlobLease(new ETag(entry.ETag), entry.LastModified, _leaseId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Renew(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => RenewAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Release ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<ReleasedObjectInfo>> ReleaseAsync(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var entry = GetEntry();
        lock (entry.Lock)
        {
            if (entry.Lease.LeaseId != _leaseId)
                throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

            entry.Lease.LeaseId = null;
            entry.Lease.Duration = null;
            entry.Lease.ExpiresAt = null;
        }

        var info = new ReleasedObjectInfo(new ETag(entry.ETag), entry.LastModified);
        return Response.FromValue(info, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<ReleasedObjectInfo> Release(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => ReleaseAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Change ──────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> ChangeAsync(string proposedId, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var entry = GetEntry();
        lock (entry.Lock)
        {
            if (entry.Lease.LeaseId != _leaseId)
                throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

            entry.Lease.LeaseId = proposedId;
        }

        _leaseId = proposedId;

        var lease = BlobsModelFactory.BlobLease(new ETag(entry.ETag), entry.LastModified, proposedId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Change(string proposedId, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => ChangeAsync(proposedId, conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Break ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> BreakAsync(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var entry = GetEntry();
        string? existingLeaseId;
        lock (entry.Lock)
        {
            if (!entry.Lease.IsActive)
                throw new RequestFailedException(409, "There is currently no lease on the blob.", "LeaseNotPresentWithLeaseOperation", null);

            existingLeaseId = entry.Lease.LeaseId;
            entry.Lease.LeaseId = null;
            entry.Lease.Duration = null;
            entry.Lease.ExpiresAt = null;
        }

        var lease = BlobsModelFactory.BlobLease(new ETag(entry.ETag), entry.LastModified, existingLeaseId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Break(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => BreakAsync(breakPeriod, conditions, cancellationToken).GetAwaiter().GetResult();
}
