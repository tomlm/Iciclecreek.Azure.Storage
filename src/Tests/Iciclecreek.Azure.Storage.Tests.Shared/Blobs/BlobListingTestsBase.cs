using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobListingTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── GetBlobs_Lists_Uploaded_Blobs ──────────────────────────────────

    [Test]
    public void GetBlobs_Lists_Uploaded_Blobs()
    {
        var container = _fixture.CreateBlobContainerClient("listing");
        container.CreateIfNotExists();

        container.UploadBlob("a.txt", BinaryData.FromString("a"));
        container.UploadBlob("b.txt", BinaryData.FromString("b"));

        var names = container.GetBlobs(BlobTraits.None, BlobStates.None, null!, default).Select(b => b.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "a.txt", "b.txt" }));
    }

    [Test]
    public async Task GetBlobs_Lists_Uploaded_Blobs_Async()
    {
        var container = _fixture.CreateBlobContainerClient("listing");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync("a.txt", BinaryData.FromString("a"));
        await container.UploadBlobAsync("b.txt", BinaryData.FromString("b"));

        var blobs = new List<BlobItem>();
        await foreach (var blob in container.GetBlobsAsync(BlobTraits.None, BlobStates.None, null!, default))
            blobs.Add(blob);

        var names = blobs.Select(b => b.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "a.txt", "b.txt" }));
    }

    // ── Does_Not_Include_Internal_Artifacts ─────────────────────────────

    [Test]
    public void Does_Not_Include_Internal_Artifacts()
    {
        var container = _fixture.CreateBlobContainerClient("sidecars");
        container.CreateIfNotExists();

        container.UploadBlob("data.txt", BinaryData.FromString("stuff"));
        // Overwrite to trigger version file creation
        container.GetBlobClient("data.txt").Upload(BinaryData.FromString("updated"), overwrite: true);
        // Blobs with dot or underscore prefixes should NOT be filtered
        container.UploadBlob(".hidden", BinaryData.FromString("dotfile"));
        container.UploadBlob("_internal", BinaryData.FromString("underscored"));

        var names = container.GetBlobs(BlobTraits.None, BlobStates.None, null!, default).Select(b => b.Name).ToArray();
        Assert.That(names, Does.Not.Contain("data.txt.meta.json"));
        Assert.That(names.Any(n => n.Contains(".version.")), Is.False, "Version files should not appear in blob listings");
        Assert.That(names, Does.Contain(".hidden"), "Dot-prefixed blobs should be listed");
        Assert.That(names, Does.Contain("_internal"), "Underscore-prefixed blobs should be listed");
        Assert.That(names, Has.Length.EqualTo(3));
    }

    [Test]
    public async Task Does_Not_Include_Internal_Artifacts_Async()
    {
        var container = _fixture.CreateBlobContainerClient("sidecars");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync("data.txt", BinaryData.FromString("stuff"));
        // Overwrite to trigger version file creation
        await container.GetBlobClient("data.txt").UploadAsync(BinaryData.FromString("updated"), overwrite: true);
        // Blobs with dot or underscore prefixes should NOT be filtered
        await container.UploadBlobAsync(".hidden", BinaryData.FromString("dotfile"));
        await container.UploadBlobAsync("_internal", BinaryData.FromString("underscored"));

        var blobs = new List<BlobItem>();
        await foreach (var blob in container.GetBlobsAsync(BlobTraits.None, BlobStates.None, null!, default))
            blobs.Add(blob);

        var names = blobs.Select(b => b.Name).ToArray();
        Assert.That(names, Does.Not.Contain("data.txt.meta.json"));
        Assert.That(names.Any(n => n.Contains(".version.")), Is.False, "Version files should not appear in blob listings");
        Assert.That(names, Does.Contain(".hidden"), "Dot-prefixed blobs should be listed");
        Assert.That(names, Does.Contain("_internal"), "Underscore-prefixed blobs should be listed");
        Assert.That(names, Has.Length.EqualTo(3));
    }

    // ── GetBlobs_With_Prefix_Filters ───────────────────────────────────

    [Test]
    public void GetBlobs_With_Prefix_Filters()
    {
        var container = _fixture.CreateBlobContainerClient("prefix");
        container.CreateIfNotExists();

        container.UploadBlob("images/cat.jpg", BinaryData.FromString("cat"));
        container.UploadBlob("images/dog.jpg", BinaryData.FromString("dog"));
        container.UploadBlob("docs/readme.md", BinaryData.FromString("readme"));

        var names = container.GetBlobs(BlobTraits.None, BlobStates.None, "images/", default).Select(b => b.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "images/cat.jpg", "images/dog.jpg" }));
    }

    [Test]
    public async Task GetBlobs_With_Prefix_Filters_Async()
    {
        var container = _fixture.CreateBlobContainerClient("prefix");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync("images/cat.jpg", BinaryData.FromString("cat"));
        await container.UploadBlobAsync("images/dog.jpg", BinaryData.FromString("dog"));
        await container.UploadBlobAsync("docs/readme.md", BinaryData.FromString("readme"));

        var blobs = new List<BlobItem>();
        await foreach (var blob in container.GetBlobsAsync(BlobTraits.None, BlobStates.None, "images/", default))
            blobs.Add(blob);

        var names = blobs.Select(b => b.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "images/cat.jpg", "images/dog.jpg" }));
    }

    // ── GetBlobsByHierarchy_Returns_Prefixes_And_Blobs ─────────────────

    [Test]
    public void GetBlobsByHierarchy_Returns_Prefixes_And_Blobs()
    {
        var container = _fixture.CreateBlobContainerClient("hierarchy");
        container.CreateIfNotExists();

        container.UploadBlob("images/cat.jpg", BinaryData.FromString("cat"));
        container.UploadBlob("images/dog.jpg", BinaryData.FromString("dog"));
        container.UploadBlob("readme.md", BinaryData.FromString("readme"));

        var items = container.GetBlobsByHierarchy(BlobTraits.None, BlobStates.None, "/", null!, default).ToArray();

        var prefixes = items.Where(i => i.IsPrefix).Select(i => i.Prefix).OrderBy(p => p).ToArray();
        var blobs = items.Where(i => i.IsBlob).Select(i => i.Blob.Name).ToArray();

        Assert.That(prefixes, Is.EqualTo(new[] { "images/" }));
        Assert.That(blobs, Is.EqualTo(new[] { "readme.md" }));
    }

    [Test]
    public async Task GetBlobsByHierarchy_Returns_Prefixes_And_Blobs_Async()
    {
        var container = _fixture.CreateBlobContainerClient("hierarchy");
        await container.CreateIfNotExistsAsync();

        await container.UploadBlobAsync("images/cat.jpg", BinaryData.FromString("cat"));
        await container.UploadBlobAsync("images/dog.jpg", BinaryData.FromString("dog"));
        await container.UploadBlobAsync("readme.md", BinaryData.FromString("readme"));

        var items = new List<BlobHierarchyItem>();
        await foreach (var item in container.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, "/", null!, default))
            items.Add(item);

        var prefixes = items.Where(i => i.IsPrefix).Select(i => i.Prefix).OrderBy(p => p).ToArray();
        var blobs = items.Where(i => i.IsBlob).Select(i => i.Blob.Name).ToArray();

        Assert.That(prefixes, Is.EqualTo(new[] { "images/" }));
        Assert.That(blobs, Is.EqualTo(new[] { "readme.md" }));
    }

    // ── GetBlobs_With_Metadata_Trait ───────────────────────────────────

    [Test]
    public void GetBlobs_With_Metadata_Trait()
    {
        var container = _fixture.CreateBlobContainerClient("metatrait");
        container.CreateIfNotExists();

        var client = container.GetBlobClient("tagged.txt");
        client.Upload(BinaryData.FromString("tagged"));
        client.SetMetadata(new Dictionary<string, string> { ["env"] = "test" });

        var blob = container.GetBlobs(BlobTraits.Metadata, BlobStates.None, null!, default).First();
        Assert.That(blob.Metadata["env"], Is.EqualTo("test"));
    }

    [Test]
    public async Task GetBlobs_With_Metadata_Trait_Async()
    {
        var container = _fixture.CreateBlobContainerClient("metatrait");
        await container.CreateIfNotExistsAsync();

        var client = container.GetBlobClient("tagged.txt");
        await client.UploadAsync(BinaryData.FromString("tagged"));
        await client.SetMetadataAsync(new Dictionary<string, string> { ["env"] = "test" });

        var blobs = new List<BlobItem>();
        await foreach (var blob in container.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, null!, default))
            blobs.Add(blob);

        Assert.That(blobs.First().Metadata["env"], Is.EqualTo("test"));
    }
}
