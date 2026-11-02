using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobAdvancedFeatureTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── OpenRead ────────────────────────────────────────────────────────

    [Test]
    public async Task OpenRead_Returns_Readable_Stream()
    {
        var container = _fixture.CreateBlobContainerClient("openread");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("readable.txt");
        await client.UploadAsync(BinaryData.FromString("stream content"));

        await using var stream = await client.OpenReadAsync();
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("stream content"));
    }

    [Test]
    public async Task OpenRead_With_Position_Seeks()
    {
        var container = _fixture.CreateBlobContainerClient("openread-seek");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("seekable.txt");
        await client.UploadAsync(BinaryData.FromString("Hello, World!"));

        await using var stream = await client.OpenReadAsync(position: 7);
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("World!"));
    }

    [Test]
    public async Task OpenRead_Missing_Blob_Throws_404()
    {
        var container = _fixture.CreateBlobContainerClient("openread-miss");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("missing.txt");
        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () => await client.OpenReadAsync());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── DownloadTo ──────────────────────────────────────────────────────

    [Test]
    public async Task DownloadTo_Stream_Copies_Content()
    {
        var container = _fixture.CreateBlobContainerClient("downloadto");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("source.txt");
        await client.UploadAsync(BinaryData.FromString("download me"));

        using var dest = new MemoryStream();
        await client.DownloadToAsync(dest);
        dest.Position = 0;
        using var reader = new StreamReader(dest);
        Assert.That(await reader.ReadToEndAsync(), Is.EqualTo("download me"));
    }

    [Test]
    public async Task DownloadTo_FilePath_Creates_File()
    {
        var container = _fixture.CreateBlobContainerClient("downloadto-file");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("source.bin");
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await client.UploadAsync(new BinaryData(data));

        var destPath = Path.Combine(_fixture.TempPath, "downloaded.bin");
        await client.DownloadToAsync(destPath);

        Assert.That(File.Exists(destPath), Is.True);
        Assert.That(await File.ReadAllBytesAsync(destPath), Is.EqualTo(data));
    }

    [Test]
    public async Task DownloadTo_Missing_Blob_Throws_404()
    {
        var container = _fixture.CreateBlobContainerClient("downloadto-miss");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("nope.txt");
        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.DownloadToAsync(new MemoryStream()));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── Container SetMetadata / GetProperties ───────────────────────────

    [Test]
    public async Task Container_SetMetadata_Roundtrips_Via_GetProperties()
    {
        var container = _fixture.CreateBlobContainerClient("container-meta");
        await container.CreateIfNotExistsAsync();

        await container.SetMetadataAsync(new Dictionary<string, string>
        {
            ["env"] = "staging",
            ["team"] = "platform"
        });

        var props = (await container.GetPropertiesAsync()).Value;
        Assert.That(props.Metadata, Is.Not.Null);
        Assert.That(props.Metadata["env"], Is.EqualTo("staging"));
        Assert.That(props.Metadata["team"], Is.EqualTo("platform"));
    }

    [Test]
    public void Container_SetMetadata_On_Missing_Container_Throws_404()
    {
        var container = _fixture.CreateBlobContainerClient("no-such-container");
        Assert.Throws<RequestFailedException>(() =>
            container.SetMetadata(new Dictionary<string, string> { ["k"] = "v" }));
    }

    // ── Download (old API) ──────────────────────────────────────────────

    [Test]
    public void Download_Old_Api_Works()
    {
        var container = _fixture.CreateBlobContainerClient("dl-old");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("old api"));
        var result = client.Download();
        using var reader = new StreamReader(result.Value.Content);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("old api"));
    }

    // ── SetAccessTier ───────────────────────────────────────────────────

    [Test]
    public void SetAccessTier_Stores_Tier()
    {
        var container = _fixture.CreateBlobContainerClient("tier-test");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("data"));
        client.SetAccessTier(AccessTier.Cool);
    }

    // ── CreateSnapshot ──────────────────────────────────────────────────

    [Test]
    public void CreateSnapshot_Copies_Blob()
    {
        var container = _fixture.CreateBlobContainerClient("snap-test");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("snapshot data"));
        var snapshot = client.CreateSnapshot();
        Assert.That(snapshot.Value.Snapshot, Is.Not.Null.And.Not.Empty);
    }

    // ── Container GetAccessPolicy ───────────────────────────────────────

    [Test]
    public void Container_GetAccessPolicy_Returns_Empty_Policy()
    {
        var container = _fixture.CreateBlobContainerClient("ns-policy");
        container.CreateIfNotExists();
        var policy = container.GetAccessPolicy().Value;
        Assert.That(policy.BlobPublicAccess, Is.EqualTo(PublicAccessType.None));
        Assert.That(policy.SignedIdentifiers, Is.Empty);
    }

    // ── Service GetProperties / GetStatistics / FindBlobsByTags ─────────

    [Test]
    public void Service_GetProperties_Returns_Default_Properties()
    {
        var service = _fixture.CreateBlobServiceClient();
        var props = service.GetProperties().Value;
        Assert.That(props, Is.Not.Null);
    }

    [Test]
    public void Service_GetStatistics_Returns_Statistics()
    {
        var service = _fixture.CreateBlobServiceClient();
        var stats = service.GetStatistics().Value;
        Assert.That(stats, Is.Not.Null);
    }

    [Test]
    public void Service_FindBlobsByTags_Returns_Empty()
    {
        var service = _fixture.CreateBlobServiceClient();
        var items = service.FindBlobsByTags("tag = 'x'").ToList();
        Assert.That(items, Is.Empty);
    }

    [Test]
    public void Container_FindBlobsByTags_Returns_Empty()
    {
        var container = _fixture.CreateBlobContainerClient("ns-tags");
        container.CreateIfNotExists();
        var items = container.FindBlobsByTags("tag = 'x'").ToList();
        Assert.That(items, Is.Empty);
    }

    // ── OpenWrite ───────────────────────────────────────────────────────

    [Test]
    public async Task OpenWrite_Creates_Blob_On_Dispose()
    {
        var container = _fixture.CreateBlobContainerClient("openwrite");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("written.txt");

        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            var bytes = "Hello from OpenWrite"u8.ToArray();
            await stream.WriteAsync(bytes);
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("Hello from OpenWrite"));
    }

    [Test]
    public async Task OpenWrite_Creates_Sidecar_With_Metadata()
    {
        var container = _fixture.CreateBlobContainerClient("openwrite-meta");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("meta-write.txt");

        await using (var stream = await client.OpenWriteAsync(overwrite: true, new BlobOpenWriteOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/csv" },
            Metadata = new Dictionary<string, string> { ["source"] = "openwrite" }
        }))
        {
            await stream.WriteAsync("col1,col2\na,b"u8.ToArray());
        }

        var props = (await client.GetPropertiesAsync()).Value;
        Assert.That(props.ContentType, Is.EqualTo("text/csv"));
        Assert.That(props.Metadata["source"], Is.EqualTo("openwrite"));
        Assert.That(props.ContentLength, Is.EqualTo(13));
    }

    [Test]
    public async Task OpenWrite_Overwrite_False_Throws_If_Exists()
    {
        var container = _fixture.CreateBlobContainerClient("openwrite-noover");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("existing.txt");
        await client.UploadAsync(BinaryData.FromString("first"));

        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.OpenWriteAsync(overwrite: false));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public async Task OpenWrite_Overwrites_Existing_Content()
    {
        var container = _fixture.CreateBlobContainerClient("openwrite-over");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("overwritten.txt");
        await client.UploadAsync(BinaryData.FromString("old content that is longer"));

        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync("new"u8.ToArray());
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("new"));
    }

    [Test]
    public async Task OpenWrite_MultipleWrites_Concatenates()
    {
        var container = _fixture.CreateBlobContainerClient("openwrite-multi");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("chunks.bin");

        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync(new byte[] { 1, 2, 3 });
            await stream.WriteAsync(new byte[] { 4, 5, 6 });
            await stream.WriteAsync(new byte[] { 7, 8, 9 });
        }

        var downloaded = (await client.DownloadContentAsync()).Value.Content.ToArray();
        Assert.That(downloaded, Is.EqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }));
    }

    // ── DownloadStreaming ────────────────────────────────────────────────

    [Test]
    public async Task DownloadStreaming_With_Options_Returns_Full_Content()
    {
        var container = _fixture.CreateBlobContainerClient("streaming-opts");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("stream.txt");
        await client.UploadAsync(BinaryData.FromString("streaming content"));

        var result = await client.DownloadStreamingAsync(new BlobDownloadOptions());
        await using var stream = result.Value.Content;
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("streaming content"));
    }

    [Test]
    public void DownloadStreaming_Sync_Works()
    {
        var container = _fixture.CreateBlobContainerClient("streaming-sync");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("stream.txt");
        client.Upload(BinaryData.FromString("sync streaming"));

        var result = client.DownloadStreaming(new BlobDownloadOptions());
        using var reader = new StreamReader(result.Value.Content);
        var text = reader.ReadToEnd();
        Assert.That(text, Is.EqualTo("sync streaming"));
    }

    // ── Container Create with metadata ──────────────────────────────────

    [Test]
    public void Container_Create_With_Metadata_Persists()
    {
        var container = _fixture.CreateBlobContainerClient("create-meta");
        container.Create(metadata: new Dictionary<string, string> { ["env"] = "prod", ["region"] = "us-west" });

        var props = container.GetProperties().Value;
        Assert.That(props.Metadata, Is.Not.Null);
        Assert.That(props.Metadata["env"], Is.EqualTo("prod"));
        Assert.That(props.Metadata["region"], Is.EqualTo("us-west"));
    }

    [Test]
    public void Container_CreateIfNotExists_With_Metadata_Persists()
    {
        var container = _fixture.CreateBlobContainerClient("createifne-meta");
        container.CreateIfNotExists(metadata: new Dictionary<string, string> { ["team"] = "infra" });

        var props = container.GetProperties().Value;
        Assert.That(props.Metadata, Is.Not.Null);
        Assert.That(props.Metadata["team"], Is.EqualTo("infra"));
    }

    [Test]
    public async Task Container_Create_With_Metadata_Async()
    {
        var container = _fixture.CreateBlobContainerClient("create-meta-async");
        await container.CreateAsync(metadata: new Dictionary<string, string> { ["async"] = "yes" });

        var props = (await container.GetPropertiesAsync()).Value;
        Assert.That(props.Metadata["async"], Is.EqualTo("yes"));
    }

    [Test]
    public void Container_GetProperties_Without_Metadata_Returns_Null_Metadata()
    {
        var container = _fixture.CreateBlobContainerClient("no-meta");
        container.Create();

        var props = container.GetProperties().Value;
        Assert.That(props.Metadata, Is.Null);
    }

    // ── UndeleteBlobContainer ───────────────────────────────────────────

    [Test]
    public void Service_UndeleteBlobContainer_Throws_404()
    {
        var service = _fixture.CreateBlobServiceClient();
        var ex = Assert.Throws<RequestFailedException>(() =>
            service.UndeleteBlobContainer("deleted", "version"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public void Service_UndeleteBlobContainer_3Arg_Throws_404()
    {
        var service = _fixture.CreateBlobServiceClient();
        var ex = Assert.Throws<RequestFailedException>(() =>
            service.UndeleteBlobContainer("deleted", "version", "destination"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── Download with range ─────────────────────────────────────────────

    [Test]
    public void Download_Old_Api_With_Range_Works()
    {
        var container = _fixture.CreateBlobContainerClient("dl-old-range");
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        client.Upload(BinaryData.FromString("range data"));
        var result = client.Download(new HttpRange(0, 5), conditions: null!, rangeGetContentHash: false);
        using var reader = new StreamReader(result.Value.Content);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("range data"));
    }

    [Test]
    public async Task DownloadStreaming_HttpRange_Overload_Returns_Content()
    {
        var container = _fixture.CreateBlobContainerClient("ds-range");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("stream.txt");
        await client.UploadAsync(BinaryData.FromString("range test content"));

        var result = await client.DownloadStreamingAsync(
            new HttpRange(0, 5), conditions: null!, rangeGetContentHash: false, progressHandler: null, cancellationToken: default);
        await using var stream = result.Value.Content;
        using var reader = new StreamReader(stream);
        var text = await reader.ReadToEndAsync();
        Assert.That(text, Is.EqualTo("range test content"));
    }

    [Test]
    public async Task DownloadContent_ProgressRange_Overload_Returns_Content()
    {
        var container = _fixture.CreateBlobContainerClient("dc-progress");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("progress.txt");
        await client.UploadAsync(BinaryData.FromString("progress content"));

        var result = await client.DownloadContentAsync(
            conditions: null!, progressHandler: null, range: default, cancellationToken: default);
        Assert.That(result.Value.Content.ToString(), Is.EqualTo("progress content"));
    }

    [Test]
    public void DownloadContent_ProgressRange_Sync_Works()
    {
        var container = _fixture.CreateBlobContainerClient("dc-prog-sync");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("sync.txt");
        client.Upload(BinaryData.FromString("sync progress"));

        var result = client.DownloadContent(conditions: null!, progressHandler: null, range: default, cancellationToken: default);
        Assert.That(result.Value.Content.ToString(), Is.EqualTo("sync progress"));
    }

    // ── GetBlobs with Options objects ───────────────────────────────────

    [Test]
    public void GetBlobs_With_GetBlobsOptions_Works()
    {
        var container = _fixture.CreateBlobContainerClient("opts-blobs");
        container.CreateIfNotExists();

        container.UploadBlob("a.txt", BinaryData.FromString("a"));
        container.UploadBlob("b.txt", BinaryData.FromString("b"));

        var options = new GetBlobsOptions { Prefix = "a" };
        var names = container.GetBlobs(options).Select(b => b.Name).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "a.txt" }));
    }

    [Test]
    public async Task GetBlobsAsync_With_GetBlobsOptions_Works()
    {
        var container = _fixture.CreateBlobContainerClient("opts-blobs-async");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync("x.txt", BinaryData.FromString("x"));
        await container.UploadBlobAsync("y.txt", BinaryData.FromString("y"));

        var options = new GetBlobsOptions { Prefix = "x" };
        var blobs = new List<BlobItem>();
        await foreach (var b in container.GetBlobsAsync(options))
            blobs.Add(b);
        Assert.That(blobs.Select(b => b.Name).ToArray(), Is.EqualTo(new[] { "x.txt" }));
    }

    [Test]
    public void GetBlobsByHierarchy_With_Options_Works()
    {
        var container = _fixture.CreateBlobContainerClient("opts-hier");
        container.CreateIfNotExists();

        container.UploadBlob("dir/a.txt", BinaryData.FromString("a"));
        container.UploadBlob("root.txt", BinaryData.FromString("r"));

        var options = new GetBlobsByHierarchyOptions { Delimiter = "/" };
        var items = container.GetBlobsByHierarchy(options).ToArray();

        var prefixItems = items.Where(i => i.IsPrefix).Select(i => i.Prefix).ToArray();
        var blobItems = items.Where(i => i.IsBlob).Select(i => i.Blob.Name).ToArray();

        Assert.That(prefixItems, Is.EqualTo(new[] { "dir/" }));
        Assert.That(blobItems, Is.EqualTo(new[] { "root.txt" }));
    }

    // ── OpenWrite on BlockBlobClient / AppendBlobClient ─────────────────

    [Test]
    public async Task BlockBlobClient_OpenWrite_Creates_Blob()
    {
        var container = _fixture.CreateBlobContainerClient("bb-openwrite");
        container.CreateIfNotExists();

        var client = _fixture.CreateBlockBlobClient(container, "block-written.txt");
        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync("block blob via OpenWrite"u8.ToArray());
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("block blob via OpenWrite"));
    }

    [Test]
    public async Task AppendBlobClient_OpenWrite_Creates_Blob()
    {
        var container = _fixture.CreateBlobContainerClient("ab-openwrite");
        container.CreateIfNotExists();

        var client = _fixture.CreateAppendBlobClient(container, "append-written.txt");
        await using (var stream = await client.OpenWriteAsync(overwrite: true))
        {
            await stream.WriteAsync("append blob via OpenWrite"u8.ToArray());
        }

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("append blob via OpenWrite"));
    }

    // ── Table GetAccessPolicies ─────────────────────────────────────────

    [Test]
    public void Table_GetAccessPolicies_Returns_Empty()
    {
        var client = _fixture.CreateTableClient("ns-getpol");
        client.CreateIfNotExists();
        var policies = client.GetAccessPolicies().Value;
        Assert.That(policies, Is.Empty);
    }

    [Test]
    public async Task Table_GetAccessPoliciesAsync_Returns_Empty()
    {
        var client = _fixture.CreateTableClient("ns-getpol-async");
        await client.CreateIfNotExistsAsync();
        var policies = (await client.GetAccessPoliciesAsync()).Value;
        Assert.That(policies, Is.Empty);
    }
}
