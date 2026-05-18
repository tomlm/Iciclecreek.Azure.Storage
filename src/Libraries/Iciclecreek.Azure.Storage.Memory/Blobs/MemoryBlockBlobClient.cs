using System.Security.Cryptography;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Blobs;

/// <summary>In-memory drop-in replacement for <see cref="BlockBlobClient"/>.</summary>
public class MemoryBlockBlobClient : BlockBlobClient
{
    internal readonly MemoryBlobServiceClient _serviceClient;
    internal readonly string _containerName;
    internal readonly string _blobName;

    internal MemoryBlockBlobClient(MemoryBlobServiceClient serviceClient, string containerName, string blobName) : base()
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

    private BlobEntry GetOrCreateEntry()
    {
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        return store.Blobs.GetOrAdd(_blobName, _ => new BlobEntry { BlobType = "Block" });
    }

    // ---- StageBlock ----

    /// <inheritdoc/>
    public override async Task<Response<BlockInfo>> StageBlockAsync(string base64BlockId, Stream content, byte[] transactionalContentHash = default!, BlobRequestConditions conditions = default!, IProgress<long>? progressHandler = default, CancellationToken cancellationToken = default)
    {
        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        var data = ms.ToArray();

        var entry = GetOrCreateEntry();
        entry.StagedBlocks[base64BlockId] = data;

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
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        if (!store.Blobs.TryGetValue(_blobName, out var entry))
            throw new RequestFailedException(404, "Blob not found. Stage blocks first.", "BlobNotFound", null);

        // Assemble content from staged blocks
        using var assembledStream = new MemoryStream();
        var committedBlockIds = new List<string>();

        foreach (var blockId in base64BlockIds)
        {
            if (!entry.StagedBlocks.TryGetValue(blockId, out var blockContent))
                throw new RequestFailedException(400, $"Block '{blockId}' not found in staging.", "InvalidBlockList", null);

            assembledStream.Write(blockContent);
            committedBlockIds.Add(blockId);
        }

        var data = assembledStream.ToArray();
        byte[] md5;
        using (var hasher = MD5.Create()) { md5 = hasher.ComputeHash(data); }
        var now = DateTimeOffset.UtcNow;
        var etag = BlobEntry.NewETag();

        lock (entry.Lock)
        {
            entry.Content = data;
            entry.BlobType = "Block";
            entry.ContentType = options?.HttpHeaders?.ContentType ?? "application/octet-stream";
            entry.ContentEncoding = options?.HttpHeaders?.ContentEncoding;
            entry.ContentHash = md5;
            entry.ETag = etag;
            entry.LastModified = now;
            if (options?.Metadata is not null)
                entry.Metadata = new Dictionary<string, string>(options.Metadata);

            // Record committed block IDs and clear staged blocks
            entry.CommittedBlockIds = committedBlockIds;
            entry.StagedBlocks.Clear();
        }

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
        if (!_serviceClient.Containers.TryGetValue(_containerName, out var store))
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);

        var committed = new List<BlobBlock>();
        var uncommitted = new List<BlobBlock>();

        if (store.Blobs.TryGetValue(_blobName, out var entry))
        {
            if ((blockListTypes & BlockListTypes.Committed) != 0 && entry.CommittedBlockIds is not null)
            {
                foreach (var blockId in entry.CommittedBlockIds)
                {
                    // Try to find the block size from staged (may have been cleared) or committed content
                    // We store the IDs but not individual sizes after commit, so estimate from total
                    committed.Add(BlobsModelFactory.BlobBlock(blockId, entry.CommittedBlockIds.Count > 0 ? (int)(entry.Content.Length / entry.CommittedBlockIds.Count) : 0));
                }
            }

            if ((blockListTypes & BlockListTypes.Uncommitted) != 0)
            {
                var committedIds = entry.CommittedBlockIds?.ToHashSet() ?? new HashSet<string>();
                foreach (var kvp in entry.StagedBlocks)
                {
                    if (!committedIds.Contains(kvp.Key))
                        uncommitted.Add(BlobsModelFactory.BlobBlock(kvp.Key, kvp.Value.Length));
                }
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
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
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
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.ExistsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => ExistsAsync(cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response Delete(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => DeleteAsync(snapshotsOption, conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetPropertiesAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
    {
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
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
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
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
        var blobClient = new MemoryBlobClient(_serviceClient, _containerName, _blobName);
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
