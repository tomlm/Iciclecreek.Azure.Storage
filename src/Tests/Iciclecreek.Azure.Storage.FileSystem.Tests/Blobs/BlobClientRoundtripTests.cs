using Azure.Storage.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobClientRoundtripTests : BlobClientRoundtripTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    private FileStorageTestFixture FileFixture => (FileStorageTestFixture)_fixture;

    [Test]
    public void Upload_Creates_Real_File_On_Disk()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("disk-container") as FileBlobContainerClient;
        container.CreateIfNotExists();

        BlobClient client = container.GetBlobClient("real-file.txt");
        client.Upload(BinaryData.FromString("filesystem test"));

        var expectedPath = Path.Combine(FileFixture.BlobsPath, "disk-container", "real-file.txt");
        Assert.That(File.Exists(expectedPath), Is.True);
        Assert.That(File.ReadAllText(expectedPath), Is.EqualTo("filesystem test"));
    }

    [Test]
    public async Task Upload_Creates_Real_File_On_Disk_Async()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("disk-container") as FileBlobContainerClient;
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("real-file.txt");
        await client.UploadAsync(BinaryData.FromString("filesystem test"));

        var expectedPath = Path.Combine(FileFixture.BlobsPath, "disk-container", "real-file.txt");
        Assert.That(File.Exists(expectedPath), Is.True);
        Assert.That(File.ReadAllText(expectedPath), Is.EqualTo("filesystem test"));
    }

    [Test]
    public async Task Upload_Download_Async_Works()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("async-test") as FileBlobContainerClient;
        await container.CreateIfNotExistsAsync();

        BlobClient client = container.GetBlobClient("async.txt");
        await client.UploadAsync(BinaryData.FromString("async data"));

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("async data"));
    }
}
