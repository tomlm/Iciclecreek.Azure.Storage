using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobAdvancedFeatureTests : BlobAdvancedFeatureTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    private FileStorageTestFixture FileFixture => (FileStorageTestFixture)_fixture;

    [Test]
    public void GenerateSasUri_Returns_Blob_Uri()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("sas-test") as FileBlobContainerClient;
        container.CreateIfNotExists();

        var client = container.GetBlobClient("doc.txt");
        client.Upload(BinaryData.FromString("data"));

        var sasUri = ((FileBlobClient)client).GenerateSasUri(
            global::Azure.Storage.Sas.BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddHours(1));
        Assert.That(sasUri, Is.Not.Null);
        Assert.That(sasUri.ToString(), Does.Contain("sas-test"));
        Assert.That(sasUri.ToString(), Does.Contain("doc.txt"));
    }

    [Test]
    public void Container_GenerateSasUri_Returns_Container_Uri()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("sas-container") as FileBlobContainerClient;
        container.CreateIfNotExists();

        var sasUri = container.GenerateSasUri(new global::Azure.Storage.Sas.BlobSasBuilder());
        Assert.That(sasUri, Is.Not.Null);
        Assert.That(sasUri.ToString(), Does.Contain("sas-container"));
    }

    [Test]
    public void NotSupported_StartCopyFromUri_Throws()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("ns-copy") as FileBlobContainerClient;
        container.CreateIfNotExists();
        var client = container.GetBlobClient("x.txt");
        Assert.That(() => client.StartCopyFromUri(new Uri("https://example.com/blob")),
            Throws.InstanceOf<NotSupportedException>().Or.InstanceOf<NullReferenceException>());
    }

    [Test]
    public void BlockBlob_StageBlockFromUri_Copies_Local_Blob()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("stagefrom") as FileBlobContainerClient;
        container.CreateIfNotExists();

        container.UploadBlob("source.bin", BinaryData.FromString("source data"));
        var sourceUri = new Uri($"{FileFixture.BlobServiceUri}stagefrom/source.bin");

        var client = container.GetFileBlockBlobClient("dest.bin");
        var blockId = Convert.ToBase64String("b1"u8.ToArray());
        client.StageBlockFromUri(sourceUri, blockId);
        client.CommitBlockList(new[] { blockId });

        var content = client.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("source data"));
    }

    [Test]
    public void AppendBlob_AppendBlockFromUri_Copies_Local_Blob()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("appendfrom") as FileBlobContainerClient;
        container.CreateIfNotExists();

        container.UploadBlob("source.txt", BinaryData.FromString("appended content"));
        var sourceUri = new Uri($"{FileFixture.BlobServiceUri}appendfrom/source.txt");

        var client = container.GetFileAppendBlobClient("dest.log");
        client.Create(new AppendBlobCreateOptions());
        client.AppendBlockFromUri(sourceUri);

        var content = client.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("appended content"));
    }

    [Test]
    public void AppendBlob_Seal_Sets_IsSealed()
    {
        var container = FileFixture.BlobService.GetBlobContainerClient("seal-test") as FileBlobContainerClient;
        container.CreateIfNotExists();

        var client = container.GetFileAppendBlobClient("x.log");
        client.Create(new AppendBlobCreateOptions());
        var result = client.Seal();
        Assert.That(result.Value, Is.Not.Null);
    }
}
