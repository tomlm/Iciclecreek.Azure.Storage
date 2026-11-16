using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Lucene.Internal;
using System.Collections.Concurrent;
using Lucene.Net.Store;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Blobs;

/// <summary>
/// Drop-in replacement for <see cref="BlobServiceClient"/> that keeps blobs in Lucene
/// directories: each container is a directory of its own, <c>blobs/{container}</c>, with a
/// file per blob. It supports upload (with If-None-Match), download, delete,
/// properties and listing (flat or by hierarchy); it does not support block, append or page
/// blob operations, leases, tags, snapshots or copies.
/// <para>
/// Containers are not kept anywhere: a container is only the place its blobs are in. So every
/// container exists, creating one does nothing and always succeeds, deleting one deletes its
/// blobs, and the containers cannot be listed.
/// </para>
/// <para>
/// In the directory name, any character of the container's name other than a lowercase letter, a
/// digit or a hyphen is written as <c>_</c> and its four-digit hexadecimal code, so that every
/// name is a folder of its own inside <c>blobs</c> on any file system.
/// </para>
/// </summary>
public sealed class LuceneBlobServiceClient : BlobServiceClient, IDisposable
{
    // Lazy, so that a container has one store, and so one directory, however many threads first ask for it together.
    private readonly ConcurrentDictionary<string, Lazy<LuceneBlobStore>> _stores = new();
    private readonly Func<string, LuceneDirectory> _directoryFactory;

    /// <summary>Blobs in memory: gone when this client is.</summary>
    public LuceneBlobServiceClient() : this(InMemory())
    {
    }

    /// <summary>Blobs in folders under <paramref name="rootPath"/>, one folder per container.</summary>
    public LuceneBlobServiceClient(string rootPath) : this(InFolder(rootPath))
    {
    }

    /// <summary>
    /// Blobs in the directories the factory gives. It is asked for <c>blobs/{container}</c> when a
    /// container is first used, once for each container, and the directory is disposed with this client.
    /// </summary>
    public LuceneBlobServiceClient(Func<string, LuceneDirectory> directoryFactory) : base()
    {
        _directoryFactory = directoryFactory ?? throw new ArgumentNullException(nameof(directoryFactory));
    }

    private static Func<string, LuceneDirectory> InMemory()
    {
        var directories = new ConcurrentDictionary<string, LuceneDirectory>(StringComparer.Ordinal);
        return path => directories.GetOrAdd(path, _ => new RAMDirectory());
    }

    private static Func<string, LuceneDirectory> InFolder(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
            throw new ArgumentException("A folder is needed.", nameof(rootPath));
        var root = Path.GetFullPath(rootPath);
        return path => FSDirectory.Open(Path.Combine(root, path.Replace('/', Path.DirectorySeparatorChar)));
    }

    // The container's name is made safe as a folder name, so that containers whose names differ only
    // by case stay apart on any file system and no name leads outside "blobs".
    internal LuceneBlobStore GetStore(string containerName)
        => _stores.GetOrAdd(containerName, name => new Lazy<LuceneBlobStore>(
            () => new LuceneBlobStore(() => _directoryFactory($"blobs/{StorageNames.Directory(name)}")))).Value;

    /// <summary>Closes the directories the blobs are kept in.</summary>
    public void Dispose()
    {
        foreach (var store in _stores.Values.Where(store => store.IsValueCreated))
            store.Value.Dispose();
        _stores.Clear();
    }

    /// <inheritdoc/>
    public override string AccountName => "lucene";

    /// <inheritdoc/>
    public override Uri Uri => new("lucene://blob/");

    /// <inheritdoc/>
    public override BlobContainerClient GetBlobContainerClient(string blobContainerName)
        => new LuceneBlobContainerClient(this, blobContainerName);

    // ---- Listing containers: not supported, since containers are not kept ----

    private const string ContainersAreNotKept = "The Lucene-backed client does not keep a list of containers, so they cannot be listed.";

    /// <inheritdoc/>
    public override Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits = default, BlobContainerStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => NotSupported.ThrowWithMessage<Pageable<BlobContainerItem>>(ContainersAreNotKept);

    /// <inheritdoc/>
    public override Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => NotSupported.ThrowWithMessage<Pageable<BlobContainerItem>>(ContainersAreNotKept);

    /// <inheritdoc/>
    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits = default, BlobContainerStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => NotSupported.ThrowWithMessage<AsyncPageable<BlobContainerItem>>(ContainersAreNotKept);

    /// <inheritdoc/>
    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => NotSupported.ThrowWithMessage<AsyncPageable<BlobContainerItem>>(ContainersAreNotKept);
}

/// <summary>A container of <see cref="LuceneBlobServiceClient"/>. Containers always exist.</summary>
internal sealed class LuceneBlobContainerClient : BlobContainerClient
{
    private readonly LuceneBlobServiceClient _service;
    private readonly string _name;

    internal LuceneBlobContainerClient(LuceneBlobServiceClient service, string name) : base()
    {
        _service = service;
        _name = name;
    }

    private LuceneBlobStore Store => _service.GetStore(_name);

    public override string Name => _name;
    public override string AccountName => _service.AccountName;
    public override Uri Uri => new($"{_service.Uri}{_name}");

    public override BlobClient GetBlobClient(string blobName) => new LuceneBlobClient(_service, _name, blobName);

    // ---- Create / Delete / Exists ----

    private static Response<BlobContainerInfo> Created()
        => Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Created());

    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
        => Created();

    public override Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Created());

    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
        => Created();

    public override Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Created());

    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Created();

    public override Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Task.FromResult(Created());

    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Created();

    public override Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Task.FromResult(Created());

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => Response.FromValue(true, StubResponse.Ok());

    public override Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(Exists(cancellationToken));

    public override Response Delete(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        Store.DeleteAll();
        return StubResponse.Accepted();
    }

    public override Task<Response> DeleteAsync(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Delete(conditions, cancellationToken));

    public override Response<bool> DeleteIfExists(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => Response.FromValue(Store.DeleteAll(), StubResponse.Ok());

    public override Task<Response<bool>> DeleteIfExistsAsync(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteIfExists(conditions, cancellationToken));

    // ---- Blobs by name ----

    public override Response<BlobContentInfo> UploadBlob(string blobName, Stream content, CancellationToken cancellationToken = default)
        => GetBlobClient(blobName).Upload(content, overwrite: false, cancellationToken);

    public override Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, Stream content, CancellationToken cancellationToken = default)
        => GetBlobClient(blobName).UploadAsync(content, overwrite: false, cancellationToken);

    public override Response DeleteBlob(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => GetBlobClient(blobName).Delete(snapshotsOption, conditions, cancellationToken);

    public override Task<Response> DeleteBlobAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => GetBlobClient(blobName).DeleteAsync(snapshotsOption, conditions, cancellationToken);

    public override Response<bool> DeleteBlobIfExists(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => GetBlobClient(blobName).DeleteIfExists(snapshotsOption, conditions, cancellationToken);

    public override Task<Response<bool>> DeleteBlobIfExistsAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => GetBlobClient(blobName).DeleteIfExistsAsync(snapshotsOption, conditions, cancellationToken);

    // ---- Listing ----

    private List<BlobItem> ListItems(BlobTraits traits, string? prefix)
        => Store.List(prefix).Select(header => BlobsModelFactory.BlobItem(
            name: header.Name,
            deleted: false,
            properties: BlobsModelFactory.BlobItemProperties(
                accessTierInferred: true,
                contentLength: header.ContentLength,
                contentType: header.ContentType,
                eTag: header.ETag,
                lastModified: header.LastModified,
                blobType: BlobType.Block,
                createdOn: header.CreatedOn),
            metadata: (traits & BlobTraits.Metadata) != 0 ? new Dictionary<string, string>(header.Metadata) : null)).ToList();

    private List<BlobHierarchyItem> ListHierarchy(BlobTraits traits, string? delimiter, string? prefix)
    {
        delimiter = string.IsNullOrEmpty(delimiter) ? null : delimiter;
        var items = new List<BlobHierarchyItem>();
        var prefixes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in ListItems(traits, prefix))
        {
            var relative = prefix != null ? item.Name[prefix.Length..] : item.Name;
            var index = delimiter == null ? -1 : relative.IndexOf(delimiter, StringComparison.Ordinal);
            if (index < 0)
                items.Add(BlobsModelFactory.BlobHierarchyItem(null, item));
            else if (prefixes.Add((prefix ?? "") + relative[..(index + delimiter!.Length)]))
                items.Add(BlobsModelFactory.BlobHierarchyItem((prefix ?? "") + relative[..(index + delimiter.Length)], null));
        }
        return items;
    }

    public override Pageable<BlobItem> GetBlobs(BlobTraits traits = default, BlobStates states = default, string? prefix = null, CancellationToken cancellationToken = default)
        => Pageable<BlobItem>.FromPages([Page<BlobItem>.FromValues(ListItems(traits, prefix), null, StubResponse.Ok())]);

    public override AsyncPageable<BlobItem> GetBlobsAsync(BlobTraits traits = default, BlobStates states = default, string? prefix = null, CancellationToken cancellationToken = default)
        => AsyncPageable<BlobItem>.FromPages([Page<BlobItem>.FromValues(ListItems(traits, prefix), null, StubResponse.Ok())]);

    public override Pageable<BlobItem> GetBlobs(GetBlobsOptions options, CancellationToken cancellationToken = default)
        => GetBlobs(options?.Traits ?? default, options?.States ?? default, options?.Prefix, cancellationToken);

    public override AsyncPageable<BlobItem> GetBlobsAsync(GetBlobsOptions options, CancellationToken cancellationToken = default)
        => GetBlobsAsync(options?.Traits ?? default, options?.States ?? default, options?.Prefix, cancellationToken);

    public override Pageable<BlobHierarchyItem> GetBlobsByHierarchy(BlobTraits traits = default, BlobStates states = default, string? delimiter = null, string? prefix = null, CancellationToken cancellationToken = default)
        => Pageable<BlobHierarchyItem>.FromPages([Page<BlobHierarchyItem>.FromValues(ListHierarchy(traits, delimiter, prefix), null, StubResponse.Ok())]);

    public override AsyncPageable<BlobHierarchyItem> GetBlobsByHierarchyAsync(BlobTraits traits = default, BlobStates states = default, string? delimiter = null, string? prefix = null, CancellationToken cancellationToken = default)
        => AsyncPageable<BlobHierarchyItem>.FromPages([Page<BlobHierarchyItem>.FromValues(ListHierarchy(traits, delimiter, prefix), null, StubResponse.Ok())]);

    public override Pageable<BlobHierarchyItem> GetBlobsByHierarchy(GetBlobsByHierarchyOptions options, CancellationToken cancellationToken = default)
        => GetBlobsByHierarchy(options?.Traits ?? default, options?.States ?? default, options?.Delimiter, options?.Prefix, cancellationToken);

    public override AsyncPageable<BlobHierarchyItem> GetBlobsByHierarchyAsync(GetBlobsByHierarchyOptions options, CancellationToken cancellationToken = default)
        => GetBlobsByHierarchyAsync(options?.Traits ?? default, options?.States ?? default, options?.Delimiter, options?.Prefix, cancellationToken);
}

/// <summary>A blob of <see cref="LuceneBlobServiceClient"/>.</summary>
internal sealed class LuceneBlobClient : BlobClient
{
    private readonly LuceneBlobServiceClient _service;
    private readonly string _containerName;
    private readonly string _name;

    internal LuceneBlobClient(LuceneBlobServiceClient service, string containerName, string name) : base()
    {
        _service = service;
        _containerName = containerName;
        _name = name;
    }

    private LuceneBlobStore Store => _service.GetStore(_containerName);

    public override string Name => _name;
    public override string BlobContainerName => _containerName;
    public override string AccountName => _service.AccountName;
    public override Uri Uri => new($"{_service.Uri}{_containerName}/{Uri.EscapeDataString(_name)}");

    // ---- Upload ----

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        var header = await Store.UploadAsync(_name, content, options?.HttpHeaders, options?.Metadata, options?.Conditions, cancellationToken).ConfigureAwait(false);
        var info = BlobsModelFactory.BlobContentInfo(header.ETag, header.LastModified, null, null, null, null, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, CancellationToken cancellationToken = default)
        => UploadAsync(content, overwrite: false, cancellationToken);

    public override Task<Response<BlobContentInfo>> UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken = default)
        => UploadAsync(content, Options(overwrite), cancellationToken);

    public override Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => UploadAsync(content.ToStream(), options, cancellationToken);

    public override Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, CancellationToken cancellationToken = default)
        => UploadAsync(content, overwrite: false, cancellationToken);

    public override Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, bool overwrite, CancellationToken cancellationToken = default)
        => UploadAsync(content.ToStream(), Options(overwrite), cancellationToken);

    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => UploadAsync(content, options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> Upload(Stream content, CancellationToken cancellationToken = default)
        => UploadAsync(content, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> Upload(Stream content, bool overwrite, CancellationToken cancellationToken = default)
        => UploadAsync(content, overwrite, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> Upload(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => UploadAsync(content, options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> Upload(BinaryData content, CancellationToken cancellationToken = default)
        => UploadAsync(content, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> Upload(BinaryData content, bool overwrite, CancellationToken cancellationToken = default)
        => UploadAsync(content, overwrite, cancellationToken).GetAwaiter().GetResult();

    private static BlobUploadOptions Options(bool overwrite)
        => new() { Conditions = overwrite ? null : new BlobRequestConditions { IfNoneMatch = ETag.All } };

    // ---- Download ----

    private async Task<(LuceneBlobStore.Header header, byte[] content)> ReadAsync(BlobRequestConditions? conditions, CancellationToken cancellationToken)
        => await Store.ReadAsync(_name, conditions, cancellationToken).ConfigureAwait(false) ?? throw BlobNotFound();

    private static BlobDownloadDetails Details(LuceneBlobStore.Header header)
        => BlobsModelFactory.BlobDownloadDetails(
            blobType: BlobType.Block,
            contentLength: header.ContentLength,
            contentType: header.ContentType,
            lastModified: header.LastModified,
            metadata: new Dictionary<string, string>(header.Metadata),
            createdOn: header.CreatedOn,
            eTag: header.ETag);

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobDownloadOptions options, CancellationToken cancellationToken = default)
    {
        var (header, content) = await ReadAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);
        return Response.FromValue(BlobsModelFactory.BlobDownloadResult(new BinaryData(content), Details(header)), StubResponse.Ok());
    }

    public override Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
        => DownloadContentAsync(new BlobDownloadOptions(), cancellationToken);

    public override Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => DownloadContentAsync(new BlobDownloadOptions { Conditions = conditions }, cancellationToken);

    public override Response<BlobDownloadResult> DownloadContent(BlobDownloadOptions options, CancellationToken cancellationToken = default)
        => DownloadContentAsync(options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobDownloadResult> DownloadContent(CancellationToken cancellationToken = default)
        => DownloadContentAsync(cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => DownloadContentAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(BlobDownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var (header, content) = await ReadAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);
        var stream = new MemoryStream(content, writable: false);
        return Response.FromValue(BlobsModelFactory.BlobDownloadStreamingResult(stream, Details(header)), StubResponse.Ok());
    }

    public override Response<BlobDownloadStreamingResult> DownloadStreaming(BlobDownloadOptions? options = null, CancellationToken cancellationToken = default)
        => DownloadStreamingAsync(options, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> DownloadToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        var (_, content) = await ReadAsync(null, cancellationToken).ConfigureAwait(false);
        await destination.WriteAsync(content, cancellationToken).ConfigureAwait(false);
        return StubResponse.Ok();
    }

    public override Response DownloadTo(Stream destination, CancellationToken cancellationToken = default)
        => DownloadToAsync(destination, cancellationToken).GetAwaiter().GetResult();

    // ---- Downloads that are not supported ----
    // Left alone, these reach the base client, which has no connection and fails with a NullReferenceException.

    public override Response DownloadTo(string path, CancellationToken cancellationToken = default) => NotSupported.Throw<Response>();
    public override Task<Response> DownloadToAsync(string path, CancellationToken cancellationToken = default) => NotSupported.Throw<Task<Response>>();
    public override Response DownloadTo(Stream destination, BlobDownloadToOptions options, CancellationToken cancellationToken = default) => NotSupported.Throw<Response>();
    public override Task<Response> DownloadToAsync(Stream destination, BlobDownloadToOptions options, CancellationToken cancellationToken = default) => NotSupported.Throw<Task<Response>>();
    public override Response DownloadTo(string path, BlobDownloadToOptions options, CancellationToken cancellationToken = default) => NotSupported.Throw<Response>();
    public override Task<Response> DownloadToAsync(string path, BlobDownloadToOptions options, CancellationToken cancellationToken = default) => NotSupported.Throw<Task<Response>>();

    public override Response<BlobDownloadInfo> Download() => NotSupported.Throw<Response<BlobDownloadInfo>>();
    public override Response<BlobDownloadInfo> Download(CancellationToken cancellationToken) => NotSupported.Throw<Response<BlobDownloadInfo>>();
    public override Response<BlobDownloadInfo> Download(HttpRange range, BlobRequestConditions? conditions = null, bool rangeGetContentHash = false, CancellationToken cancellationToken = default) => NotSupported.Throw<Response<BlobDownloadInfo>>();
    public override Task<Response<BlobDownloadInfo>> DownloadAsync() => NotSupported.Throw<Task<Response<BlobDownloadInfo>>>();
    public override Task<Response<BlobDownloadInfo>> DownloadAsync(CancellationToken cancellationToken) => NotSupported.Throw<Task<Response<BlobDownloadInfo>>>();
    public override Task<Response<BlobDownloadInfo>> DownloadAsync(HttpRange range, BlobRequestConditions? conditions = null, bool rangeGetContentHash = false, CancellationToken cancellationToken = default) => NotSupported.Throw<Task<Response<BlobDownloadInfo>>>();

    public override Stream OpenRead(long position = 0, int? bufferSize = null, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default) => NotSupported.Throw<Stream>();
    public override Stream OpenRead(BlobOpenReadOptions options, CancellationToken cancellationToken = default) => NotSupported.Throw<Stream>();
    public override Task<Stream> OpenReadAsync(long position = 0, int? bufferSize = null, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default) => NotSupported.Throw<Task<Stream>>();
    public override Task<Stream> OpenReadAsync(BlobOpenReadOptions options, CancellationToken cancellationToken = default) => NotSupported.Throw<Task<Stream>>();

    // ---- Properties / Exists / Delete ----

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
    {
        var header = await Store.GetPropertiesAsync(_name, cancellationToken).ConfigureAwait(false) ?? throw BlobNotFound();
        var properties = BlobsModelFactory.BlobProperties(
            lastModified: header.LastModified,
            createdOn: header.CreatedOn,
            metadata: new Dictionary<string, string>(header.Metadata),
            blobType: BlobType.Block,
            contentLength: header.ContentLength,
            contentType: header.ContentType,
            eTag: header.ETag);
        return Response.FromValue(properties, StubResponse.Ok());
    }

    public override Response<BlobProperties> GetProperties(BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => GetPropertiesAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
        => Response.FromValue(await Store.GetPropertiesAsync(_name, cancellationToken).ConfigureAwait(false) != null, StubResponse.Ok());

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => ExistsAsync(cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => await Store.DeleteAsync(_name, conditions, cancellationToken).ConfigureAwait(false)
            ? StubResponse.Accepted()
            : throw BlobNotFound();

    public override Response Delete(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => DeleteAsync(snapshotsOption, conditions, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response<bool>> DeleteIfExistsAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => Response.FromValue(await Store.DeleteAsync(_name, conditions, cancellationToken).ConfigureAwait(false), StubResponse.Ok());

    public override Response<bool> DeleteIfExists(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions? conditions = null, CancellationToken cancellationToken = default)
        => DeleteIfExistsAsync(snapshotsOption, conditions, cancellationToken).GetAwaiter().GetResult();

    private static RequestFailedException BlobNotFound()
        => new(404, "The specified blob does not exist.", "BlobNotFound", null);
}
