using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobContainerCrudTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Create_Throws_If_Already_Exists ────────────────────────────────

    [Test]
    public void Create_Throws_If_Already_Exists()
    {
        var container = _fixture.CreateBlobContainerClient("duplicate");
        container.Create();
        Assert.Throws<RequestFailedException>(() => container.Create());
    }

    [Test]
    public async Task Create_Throws_If_Already_Exists_Async()
    {
        var container = _fixture.CreateBlobContainerClient("duplicate");
        await container.CreateAsync();
        await Assert.ThrowsAsync<RequestFailedException>(async () => await container.CreateAsync());
    }

    // ── CreateIfNotExists_Is_Idempotent ────────────────────────────────

    [Test]
    public void CreateIfNotExists_Is_Idempotent()
    {
        var container = _fixture.CreateBlobContainerClient("idempotent");
        container.CreateIfNotExists();
        container.CreateIfNotExists();
        Assert.That(container.Exists().Value, Is.True);
    }

    [Test]
    public async Task CreateIfNotExists_Is_Idempotent_Async()
    {
        var container = _fixture.CreateBlobContainerClient("idempotent");
        await container.CreateIfNotExistsAsync();
        await container.CreateIfNotExistsAsync();
        Assert.That((await container.ExistsAsync()).Value, Is.True);
    }

    // ── Delete_Removes_Container ───────────────────────────────────────

    [Test]
    public void Delete_Removes_Container()
    {
        var container = _fixture.CreateBlobContainerClient("to-delete");
        container.CreateIfNotExists();
        Assert.That(container.Exists().Value, Is.True);

        container.Delete();
        Assert.That(container.Exists().Value, Is.False);
    }

    [Test]
    public async Task Delete_Removes_Container_Async()
    {
        var container = _fixture.CreateBlobContainerClient("to-delete");
        await container.CreateIfNotExistsAsync();
        Assert.That((await container.ExistsAsync()).Value, Is.True);

        await container.DeleteAsync();
        Assert.That((await container.ExistsAsync()).Value, Is.False);
    }

    // ── DeleteIfExists_Returns_False_When_Missing ──────────────────────

    [Test]
    public void DeleteIfExists_Returns_False_When_Missing()
    {
        var container = _fixture.CreateBlobContainerClient("never-created");
        var response = container.DeleteIfExists();
        Assert.That(response.Value, Is.False);
    }

    [Test]
    public async Task DeleteIfExists_Returns_False_When_Missing_Async()
    {
        var container = _fixture.CreateBlobContainerClient("never-created");
        var response = await container.DeleteIfExistsAsync();
        Assert.That(response.Value, Is.False);
    }

    // ── ServiceClient_GetBlobContainers_Lists_Created_Containers ───────

    [Test]
    public void ServiceClient_GetBlobContainers_Lists_Created_Containers()
    {
        var service = _fixture.CreateBlobServiceClient();
        var c1 = _fixture.CreateBlobContainerClient("one");
        var c2 = _fixture.CreateBlobContainerClient("two");
        c1.CreateIfNotExists();
        c2.CreateIfNotExists();

        var names = new List<string>();
        foreach (var c in service.GetBlobContainers())
            names.Add(c.Name);
        names.Sort();

        Assert.That(names.ToArray(), Is.EqualTo(new[] { "one", "two" }));
    }

    [Test]
    public async Task ServiceClient_GetBlobContainers_Lists_Created_Containers_Async()
    {
        var service = _fixture.CreateBlobServiceClient();
        var c1 = _fixture.CreateBlobContainerClient("one");
        var c2 = _fixture.CreateBlobContainerClient("two");
        await c1.CreateIfNotExistsAsync();
        await c2.CreateIfNotExistsAsync();

        var names = new List<string>();
        await foreach (var c in service.GetBlobContainersAsync())
            names.Add(c.Name);
        names.Sort();

        Assert.That(names.ToArray(), Is.EqualTo(new[] { "one", "two" }));
    }
}
