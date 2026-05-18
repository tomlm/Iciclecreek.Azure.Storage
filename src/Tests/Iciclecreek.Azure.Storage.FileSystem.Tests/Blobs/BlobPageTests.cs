using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobPageTests : BlobPageTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    private FileStorageTestFixture FileFixture => (FileStorageTestFixture)_fixture;

    [Test]
    public void Create_CreatesPreAllocatedFile()
    {
        var cc = FileFixture.BlobService.GetBlobContainerClient("page-create-fs") as FileBlobContainerClient;
        cc.CreateIfNotExists();
        var pb = ((FileBlobContainerClient)cc).GetFilePageBlobClient("blob.bin");

        pb.Create(4096, (global::Azure.Storage.Blobs.Models.PageBlobCreateOptions?)null);

        var blobPath = Path.Combine(cc.ContainerPath, "blob.bin");
        Assert.That(File.Exists(blobPath), Is.True);
        Assert.That(new FileInfo(blobPath).Length, Is.EqualTo(4096));
    }

    [Test]
    public void Resize_ChangesFileSize()
    {
        var cc = FileFixture.BlobService.GetBlobContainerClient("page-resize-fs") as FileBlobContainerClient;
        cc.CreateIfNotExists();
        var pb = ((FileBlobContainerClient)cc).GetFilePageBlobClient("blob.bin");
        pb.Create(1024, (global::Azure.Storage.Blobs.Models.PageBlobCreateOptions?)null);

        pb.Resize(4096);

        var blobPath = Path.Combine(cc.ContainerPath, "blob.bin");
        Assert.That(new FileInfo(blobPath).Length, Is.EqualTo(4096));
    }
}
