using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobContainerCrudTests : BlobContainerCrudTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    private FileStorageTestFixture FileFixture => (FileStorageTestFixture)_fixture;

    [Test]
    public void Create_Creates_Directory_On_Disk()
    {
        var client = (FileBlobContainerClient)FileFixture.BlobService.GetBlobContainerClient("my-container");
        client.Create();
        Assert.That(Directory.Exists(Path.Combine(FileFixture.BlobsPath, "my-container")), Is.True);
    }

    [Test]
    public async Task Create_Creates_Directory_On_Disk_Async()
    {
        var client = (FileBlobContainerClient)FileFixture.BlobService.GetBlobContainerClient("my-container");
        await client.CreateAsync();
        Assert.That(Directory.Exists(Path.Combine(FileFixture.BlobsPath, "my-container")), Is.True);
    }
}
