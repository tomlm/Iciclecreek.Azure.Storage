using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>
/// Filesystem-backed drop-in replacement for <see cref="BlobLeaseClient"/>.
/// Lease state is stored in the blob's sidecar JSON file.
/// </summary>
public class FileBlobLeaseClient : BlobLeaseClient
{
    private readonly FileBlobClient _blob;
    private string _leaseId;

    internal FileBlobLeaseClient(FileBlobClient blob, string? leaseId) : base()
    {
        _blob = blob;
        _leaseId = leaseId ?? Guid.NewGuid().ToString();
    }

    /// <inheritdoc/>
    public override string LeaseId => _leaseId;

    /// <summary>The URI of the blob this lease client is associated with.</summary>
    public new Uri Uri => _blob.Uri;

    // ── Acquire ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> AcquireAsync(TimeSpan duration, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var sidecar = await _blob._store.ReadSidecarAsync(_blob._blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        if (sidecar.IsLeased)
            throw new RequestFailedException(409, "There is already a lease present.", "LeaseAlreadyPresent", null);

        var durationSecs = (int)duration.TotalSeconds;
        if (durationSecs != -1 && (durationSecs < 15 || durationSecs > 60))
            throw new RequestFailedException(400, "Lease duration must be 15-60 seconds or -1 (infinite).", "InvalidHeaderValue", null);

        sidecar.LeaseId = _leaseId;
        sidecar.LeaseDurationSeconds = durationSecs;
        sidecar.LeaseExpiresOn = durationSecs == -1
            ? (DateTimeOffset?)null
            : DateTimeOffset.UtcNow.AddSeconds(durationSecs);
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;

        await _blob._store.WriteSidecarAsync(_blob._blobName, sidecar, cancellationToken).ConfigureAwait(false);

        var lease = BlobsModelFactory.BlobLease(new ETag(sidecar.ETag), sidecar.LastModifiedUtc, _leaseId);
        return Response.FromValue(lease, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Acquire(TimeSpan duration, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => AcquireAsync(duration, conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Renew ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> RenewAsync(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var sidecar = await _blob._store.ReadSidecarAsync(_blob._blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        if (sidecar.LeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        if (sidecar.LeaseDurationSeconds != -1)
            sidecar.LeaseExpiresOn = DateTimeOffset.UtcNow.AddSeconds(sidecar.LeaseDurationSeconds);
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;

        await _blob._store.WriteSidecarAsync(_blob._blobName, sidecar, cancellationToken).ConfigureAwait(false);

        var lease = BlobsModelFactory.BlobLease(new ETag(sidecar.ETag), sidecar.LastModifiedUtc, _leaseId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Renew(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => RenewAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Release ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<ReleasedObjectInfo>> ReleaseAsync(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var sidecar = await _blob._store.ReadSidecarAsync(_blob._blobName, cancellationToken).ConfigureAwait(false);
        if (sidecar == null)
        {
            // Blob already deleted — lock is implicitly released
            var now = DateTimeOffset.UtcNow;
            return Response.FromValue(new ReleasedObjectInfo(new ETag("\"0x0\""), now), StubResponse.Ok());
        }

        if (sidecar.LeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        sidecar.LeaseId = null;
        sidecar.LeaseExpiresOn = null;
        sidecar.LeaseDurationSeconds = 0;
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;

        await _blob._store.WriteSidecarAsync(_blob._blobName, sidecar, cancellationToken).ConfigureAwait(false);

        var info = new ReleasedObjectInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc);
        return Response.FromValue(info, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<ReleasedObjectInfo> Release(RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => ReleaseAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Change ──────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> ChangeAsync(string proposedId, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var sidecar = await _blob._store.ReadSidecarAsync(_blob._blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        if (sidecar.LeaseId != _leaseId)
            throw new RequestFailedException(409, "Lease ID mismatch.", "LeaseIdMismatchWithLeaseOperation", null);

        sidecar.LeaseId = proposedId;
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;

        await _blob._store.WriteSidecarAsync(_blob._blobName, sidecar, cancellationToken).ConfigureAwait(false);

        _leaseId = proposedId;
        var lease = BlobsModelFactory.BlobLease(new ETag(sidecar.ETag), sidecar.LastModifiedUtc, proposedId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Change(string proposedId, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => ChangeAsync(proposedId, conditions, cancellationToken).GetAwaiter().GetResult();

    // ── Break ───────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override async Task<Response<BlobLease>> BreakAsync(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var sidecar = await _blob._store.ReadSidecarAsync(_blob._blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);

        if (!sidecar.IsLeased)
            throw new RequestFailedException(409, "There is currently no lease on the blob.", "LeaseNotPresentWithLeaseOperation", null);

        // Break immediately (or after breakPeriod, but we simplify to immediate)
        var oldLeaseId = sidecar.LeaseId;
        sidecar.LeaseId = null;
        sidecar.LeaseExpiresOn = null;
        sidecar.LeaseDurationSeconds = 0;
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;

        await _blob._store.WriteSidecarAsync(_blob._blobName, sidecar, cancellationToken).ConfigureAwait(false);

        var lease = BlobsModelFactory.BlobLease(new ETag(sidecar.ETag), sidecar.LastModifiedUtc, oldLeaseId);
        return Response.FromValue(lease, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<BlobLease> Break(TimeSpan? breakPeriod = null, RequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => BreakAsync(breakPeriod, conditions, cancellationToken).GetAwaiter().GetResult();
}
