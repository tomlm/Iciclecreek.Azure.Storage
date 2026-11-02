using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobETagTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Upload_Returns_ETag ────────────────────────────────────────────

    [Test]
    public void Upload_Returns_ETag()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        var response = client.Upload(BinaryData.FromString("data"));

        Assert.That(response.Value.ETag, Is.Not.EqualTo(default(ETag)));
    }

    [Test]
    public async Task Upload_Returns_ETag_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        var response = await client.UploadAsync(BinaryData.FromString("data"));

        Assert.That(response.Value.ETag, Is.Not.EqualTo(default(ETag)));
    }

    // ── ETag_Changes_On_Every_Write ────────────────────────────────────

    [Test]
    public void ETag_Changes_On_Every_Write()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = client.Upload(BinaryData.FromString("v1"));
        var r2 = client.Upload(BinaryData.FromString("v2"), overwrite: true);

        Assert.That(r2.Value.ETag, Is.Not.EqualTo(r1.Value.ETag));
    }

    [Test]
    public async Task ETag_Changes_On_Every_Write_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = await client.UploadAsync(BinaryData.FromString("v1"));
        var r2 = await client.UploadAsync(BinaryData.FromString("v2"), overwrite: true);

        Assert.That(r2.Value.ETag, Is.Not.EqualTo(r1.Value.ETag));
    }

    // ── Upload_With_Correct_IfMatch_Succeeds ───────────────────────────

    [Test]
    public void Upload_With_Correct_IfMatch_Succeeds()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = client.Upload(BinaryData.FromString("v1"));
        var etag = r1.Value.ETag;

        var r2 = client.Upload(BinaryData.FromString("v2"), new BlobUploadOptions
        {
            Conditions = new BlobRequestConditions { IfMatch = etag }
        });

        Assert.That(r2.Value.ETag, Is.Not.EqualTo(etag));
    }

    [Test]
    public async Task Upload_With_Correct_IfMatch_Succeeds_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = await client.UploadAsync(BinaryData.FromString("v1"));
        var etag = r1.Value.ETag;

        var r2 = await client.UploadAsync(BinaryData.FromString("v2"), new BlobUploadOptions
        {
            Conditions = new BlobRequestConditions { IfMatch = etag }
        });

        Assert.That(r2.Value.ETag, Is.Not.EqualTo(etag));
    }

    // ── Upload_With_Stale_IfMatch_Throws_412 ───────────────────────────

    [Test]
    public void Upload_With_Stale_IfMatch_Throws_412()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = client.Upload(BinaryData.FromString("v1"));
        var staleEtag = r1.Value.ETag;

        client.Upload(BinaryData.FromString("v2"), overwrite: true);

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Upload(BinaryData.FromString("v3"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = staleEtag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task Upload_With_Stale_IfMatch_Throws_412_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = await client.UploadAsync(BinaryData.FromString("v1"));
        var staleEtag = r1.Value.ETag;

        await client.UploadAsync(BinaryData.FromString("v2"), overwrite: true);

        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UploadAsync(BinaryData.FromString("v3"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = staleEtag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    // ── Upload_With_IfNoneMatch_Star_Prevents_Overwrite ────────────────

    [Test]
    public void Upload_With_IfNoneMatch_Star_Prevents_Overwrite()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        client.Upload(BinaryData.FromString("v1"));

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Upload(BinaryData.FromString("v2"), overwrite: false));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public async Task Upload_With_IfNoneMatch_Star_Prevents_Overwrite_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        await client.UploadAsync(BinaryData.FromString("v1"));

        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UploadAsync(BinaryData.FromString("v2"), overwrite: false));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    // ── IfNoneMatch_Star_Allows_First_Upload ───────────────────────────

    [Test]
    public void IfNoneMatch_Star_Allows_First_Upload()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("new-blob.txt");
        var response = client.Upload(BinaryData.FromString("v1"), overwrite: false);

        Assert.That(response.Value.ETag, Is.Not.EqualTo(default(ETag)));
    }

    [Test]
    public async Task IfNoneMatch_Star_Allows_First_Upload_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("new-blob.txt");
        var response = await client.UploadAsync(BinaryData.FromString("v1"), overwrite: false);

        Assert.That(response.Value.ETag, Is.Not.EqualTo(default(ETag)));
    }

    // ── Delete_With_Correct_IfMatch_Succeeds ───────────────────────────

    [Test]
    public void Delete_With_Correct_IfMatch_Succeeds()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = client.Upload(BinaryData.FromString("v1"));
        var etag = r1.Value.ETag;

        client.Delete(conditions: new BlobRequestConditions { IfMatch = etag });
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public async Task Delete_With_Correct_IfMatch_Succeeds_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = await client.UploadAsync(BinaryData.FromString("v1"));
        var etag = r1.Value.ETag;

        await client.DeleteAsync(conditions: new BlobRequestConditions { IfMatch = etag });
        Assert.That((await client.ExistsAsync()).Value, Is.False);
    }

    // ── Delete_With_Stale_IfMatch_Throws_412 ───────────────────────────

    [Test]
    public void Delete_With_Stale_IfMatch_Throws_412()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = client.Upload(BinaryData.FromString("v1"));
        var staleEtag = r1.Value.ETag;

        client.Upload(BinaryData.FromString("v2"), overwrite: true);

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Delete(conditions: new BlobRequestConditions { IfMatch = staleEtag }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task Delete_With_Stale_IfMatch_Throws_412_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = await client.UploadAsync(BinaryData.FromString("v1"));
        var staleEtag = r1.Value.ETag;

        await client.UploadAsync(BinaryData.FromString("v2"), overwrite: true);

        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.DeleteAsync(conditions: new BlobRequestConditions { IfMatch = staleEtag }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    // ── SetMetadata_With_Stale_IfMatch_Throws_412 ──────────────────────

    [Test]
    public void SetMetadata_With_Stale_IfMatch_Throws_412()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = client.Upload(BinaryData.FromString("v1"));
        var staleEtag = r1.Value.ETag;

        client.Upload(BinaryData.FromString("v2"), overwrite: true);

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.SetMetadata(
                new Dictionary<string, string> { ["key"] = "value" },
                new BlobRequestConditions { IfMatch = staleEtag }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task SetMetadata_With_Stale_IfMatch_Throws_412_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        var r1 = await client.UploadAsync(BinaryData.FromString("v1"));
        var staleEtag = r1.Value.ETag;

        await client.UploadAsync(BinaryData.FromString("v2"), overwrite: true);

        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.SetMetadataAsync(
                new Dictionary<string, string> { ["key"] = "value" },
                new BlobRequestConditions { IfMatch = staleEtag }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    // ── Optimistic_Concurrency_ReadModifyWrite_Pattern ─────────────────

    [Test]
    public void Optimistic_Concurrency_ReadModifyWrite_Pattern()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("blob.txt");
        client.Upload(BinaryData.FromString("v1"));

        // Read
        var props = client.GetProperties().Value;
        var currentEtag = props.ETag;

        // Modify + Write with correct ETag
        var r2 = client.Upload(BinaryData.FromString("v2"), new BlobUploadOptions
        {
            Conditions = new BlobRequestConditions { IfMatch = currentEtag }
        });

        // Verify new ETag differs
        Assert.That(r2.Value.ETag, Is.Not.EqualTo(currentEtag));

        // Second writer with stale ETag fails
        var ex = Assert.Throws<RequestFailedException>(() =>
            client.Upload(BinaryData.FromString("v3"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = currentEtag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task Optimistic_Concurrency_ReadModifyWrite_Pattern_Async()
    {
        var container = _fixture.CreateBlobContainerClient("etag-container");
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("blob.txt");
        await client.UploadAsync(BinaryData.FromString("v1"));

        // Read
        var props = (await client.GetPropertiesAsync()).Value;
        var currentEtag = props.ETag;

        // Modify + Write with correct ETag
        var r2 = await client.UploadAsync(BinaryData.FromString("v2"), new BlobUploadOptions
        {
            Conditions = new BlobRequestConditions { IfMatch = currentEtag }
        });

        // Verify new ETag differs
        Assert.That(r2.Value.ETag, Is.Not.EqualTo(currentEtag));

        // Second writer with stale ETag fails
        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UploadAsync(BinaryData.FromString("v3"), new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfMatch = currentEtag }
            }));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }
}
