using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class AppendBlobTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Create_And_AppendBlock_Roundtrip ───────────────────────────────

    [Test]
    public void Create_And_AppendBlock_Roundtrip()
    {
        var container = _fixture.CreateBlobContainerClient("append");
        container.CreateIfNotExists();
        var client = _fixture.CreateAppendBlobClient(container, "log.txt");
        client.Create(new AppendBlobCreateOptions());

        client.AppendBlock(new MemoryStream("line1\n"u8.ToArray()));
        client.AppendBlock(new MemoryStream("line2\n"u8.ToArray()));

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("line1\nline2\n"));
    }

    [Test]
    public async Task Create_And_AppendBlock_Roundtrip_Async()
    {
        var container = _fixture.CreateBlobContainerClient("append-async");
        await container.CreateIfNotExistsAsync();
        var client = _fixture.CreateAppendBlobClient(container, "log.txt");
        await client.CreateAsync(new AppendBlobCreateOptions());

        await client.AppendBlockAsync(new MemoryStream("line1\n"u8.ToArray()));
        await client.AppendBlockAsync(new MemoryStream("line2\n"u8.ToArray()));

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("line1\nline2\n"));
    }

    // ── Create_Sets_BlobType_Append ────────────────────────────────────

    [Test]
    public void Create_Sets_BlobType_Append()
    {
        var container = _fixture.CreateBlobContainerClient("append-type");
        container.CreateIfNotExists();
        var client = _fixture.CreateAppendBlobClient(container, "typed.txt");
        client.Create(new AppendBlobCreateOptions());

        var props = client.GetProperties();
        Assert.That(props.Value.BlobType, Is.EqualTo(BlobType.Append));
    }

    [Test]
    public async Task Create_Sets_BlobType_Append_Async()
    {
        var container = _fixture.CreateBlobContainerClient("append-type-async");
        await container.CreateIfNotExistsAsync();
        var client = _fixture.CreateAppendBlobClient(container, "typed.txt");
        await client.CreateAsync(new AppendBlobCreateOptions());

        var props = await client.GetPropertiesAsync();
        Assert.That(props.Value.BlobType, Is.EqualTo(BlobType.Append));
    }

    // ── AppendBlock_Without_Create_Throws_404 ──────────────────────────

    [Test]
    public void AppendBlock_Without_Create_Throws_404()
    {
        var container = _fixture.CreateBlobContainerClient("append-nocreate");
        container.CreateIfNotExists();
        var client = _fixture.CreateAppendBlobClient(container, "missing.txt");

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.AppendBlock(new MemoryStream("data"u8.ToArray())));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public async Task AppendBlock_Without_Create_Throws_404_Async()
    {
        var container = _fixture.CreateBlobContainerClient("append-nocreate-async");
        container.CreateIfNotExists();
        var client = _fixture.CreateAppendBlobClient(container, "missing.txt");

        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.AppendBlockAsync(new MemoryStream("data"u8.ToArray())));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── AppendBlock_Grows_File_Size ────────────────────────────────────

    [Test]
    public void AppendBlock_Grows_File_Size()
    {
        var container = _fixture.CreateBlobContainerClient("append-grow");
        container.CreateIfNotExists();
        var client = _fixture.CreateAppendBlobClient(container, "growing.bin");
        client.Create(new AppendBlobCreateOptions());

        client.AppendBlock(new MemoryStream(new byte[100]));
        var props1 = client.GetProperties();
        Assert.That(props1.Value.ContentLength, Is.EqualTo(100));

        client.AppendBlock(new MemoryStream(new byte[50]));
        var props2 = client.GetProperties();
        Assert.That(props2.Value.ContentLength, Is.EqualTo(150));
    }

    [Test]
    public async Task AppendBlock_Grows_File_Size_Async()
    {
        var container = _fixture.CreateBlobContainerClient("append-grow-async");
        await container.CreateIfNotExistsAsync();
        var client = _fixture.CreateAppendBlobClient(container, "growing.bin");
        await client.CreateAsync(new AppendBlobCreateOptions());

        await client.AppendBlockAsync(new MemoryStream(new byte[100]));
        var props1 = await client.GetPropertiesAsync();
        Assert.That(props1.Value.ContentLength, Is.EqualTo(100));

        await client.AppendBlockAsync(new MemoryStream(new byte[50]));
        var props2 = await client.GetPropertiesAsync();
        Assert.That(props2.Value.ContentLength, Is.EqualTo(150));
    }
}
