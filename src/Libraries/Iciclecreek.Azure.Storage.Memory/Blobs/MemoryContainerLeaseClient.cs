using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>
/// In-memory drop-in replacement for <see cref="BlobLeaseClient"/> (container-level leases).
/// Lease state is stored in <see cref="ContainerStore.Lease"/>.
/// </summary>
public class MemoryContainerLeaseClient : BlobLeaseClient
{
    private readonly MemoryBlobContainerClient _container;
    private string _leaseId;

    internal MemoryContainerLeaseClient(MemoryBlobContainerClient container, string? leaseId) : base()
    {
        _container = container;
        _leaseId = leaseId ?? Guid.NewGuid().ToString();
    }

    public override string LeaseId => _leaseId;
    public new Uri Uri => _container.Uri;

    private ContainerStore GetStore()
    {
        if (!_container._serviceClient.Containers.TryGetValue(_container._containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        return store;
    }

    // ── Acquire ─────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> AcquireAsync(TimeSpan duration, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var secs = (int)duration.TotalSeconds;
        if (secs != -1 && (secs < 15 || secs > 60))
            throw new RequestFailedException(400, "Lease duration must be 15-60 seconds or -1 (infinite).", "InvalidHeaderValue", null);

        var store = GetStore();
        var lease = store.Lease;

        // No dedicated lock object on ContainerStore, but LeaseState is simple enough to sync on itself
        lock (lease)
        {
            if (lease.IsActive)
                throw new RequestFailedException(409, "There is already a lease present.", "LeaseAlreadyPresent", null);

            lease.LeaseId = _leaseId;
            lease.Duration = TimeSpan.FromSeconds(secs);
            lease.ExpiresAt = secs == -1 ? null : DateTimeOffset.UtcNow.AddSeconds(secs);
        }

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, _leaseId), StubResponse.Created());
    }

    public override Response<BlobLease> Acquire(TimeSpan duration, RequestConditions? conditions = null, CancellationToken ct = default)
        => AcquireAsync(duration, conditions, ct).GetAwaiter().GetResult();

    // ── Renew ───────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> RenewAsync(RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var store = GetStore();
        var lease = store.Lease;
        lock (lease)
        {
            if (lease.LeaseId != _leaseId)
                throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

            if (lease.Duration?.TotalSeconds != -1)
                lease.ExpiresAt = DateTimeOffset.UtcNow.Add(lease.Duration ?? TimeSpan.Zero);
        }

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, _leaseId), StubResponse.Ok());
    }

    public override Response<BlobLease> Renew(RequestConditions? conditions = null, CancellationToken ct = default)
        => RenewAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Release ─────────────────────────────────────────────────────────

    public override async Task<Response<ReleasedObjectInfo>> ReleaseAsync(RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var store = GetStore();
        var lease = store.Lease;
        lock (lease)
        {
            if (lease.LeaseId != _leaseId)
                throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

            lease.LeaseId = null;
            lease.Duration = null;
            lease.ExpiresAt = null;
        }

        return Response.FromValue(new ReleasedObjectInfo(ETag.All, DateTimeOffset.UtcNow), StubResponse.Ok());
    }

    public override Response<ReleasedObjectInfo> Release(RequestConditions? conditions = null, CancellationToken ct = default)
        => ReleaseAsync(conditions, ct).GetAwaiter().GetResult();

    // ── Change ──────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> ChangeAsync(string proposedId, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var store = GetStore();
        var lease = store.Lease;
        lock (lease)
        {
            if (lease.LeaseId != _leaseId)
                throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

            lease.LeaseId = proposedId;
        }

        _leaseId = proposedId;

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, proposedId), StubResponse.Ok());
    }

    public override Response<BlobLease> Change(string proposedId, RequestConditions? conditions = null, CancellationToken ct = default)
        => ChangeAsync(proposedId, conditions, ct).GetAwaiter().GetResult();

    // ── Break ───────────────────────────────────────────────────────────

    public override async Task<Response<BlobLease>> BreakAsync(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken ct = default)
    {
        var store = GetStore();
        var lease = store.Lease;
        string? existingLeaseId;
        lock (lease)
        {
            if (!lease.IsActive)
                throw new RequestFailedException(409, "There is currently no lease on the container.", "LeaseNotPresentWithLeaseOperation", null);

            existingLeaseId = lease.LeaseId;
            lease.LeaseId = null;
            lease.Duration = null;
            lease.ExpiresAt = null;
        }

        return Response.FromValue(BlobsModelFactory.BlobLease(ETag.All, DateTimeOffset.UtcNow, existingLeaseId), StubResponse.Ok());
    }

    public override Response<BlobLease> Break(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken ct = default)
        => BreakAsync(breakPeriod, conditions, ct).GetAwaiter().GetResult();
}
