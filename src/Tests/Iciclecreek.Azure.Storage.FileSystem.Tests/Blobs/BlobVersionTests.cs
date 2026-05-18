using Azure;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobVersionTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void Upload_CreatesVersionFile_OnReupload()
    {
        var cc = _root.BlobService.GetBlobContainerClient("ver-reupload") as FileBlobContainerClient;
        cc.CreateIfNotExists();
        var bc = (FileBlobClient)cc.GetBlobClient("doc.txt");

        bc.Upload(BinaryData.FromString("v1"));
        bc.Upload(BinaryData.FromString("v2"));

        // A .version. file should exist for the previous version
        var blobPath = Path.Combine(cc.ContainerPath, "doc.txt");
        var dir = Path.GetDirectoryName(blobPath)!;
        var versionFiles = Directory.GetFiles(dir, "doc.txt.version.*")
            .Where(f => !f.EndsWith(".sidecar.json"))
            .ToArray();
        Assert.That(versionFiles.Length, Is.GreaterThanOrEqualTo(1));

        // Current content should be v2
        var content = bc.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("v2"));
    }

    [Test]
    public void VersionId_IsSet_OnSidecar()
    {
        var cc = _root.BlobService.GetBlobContainerClient("ver-sidecar") as FileBlobContainerClient;
        cc.CreateIfNotExists();
        var bc = (FileBlobClient)cc.GetBlobClient("doc.txt");

        bc.Upload(BinaryData.FromString("content"));

        // Read the sidecar JSON directly to verify VersionId is persisted
        var sidecarPath = Path.Combine(cc.ContainerPath, "doc.txt.meta.json");
        Assert.That(File.Exists(sidecarPath), Is.True);
        var json = File.ReadAllText(sidecarPath);
        Assert.That(json, Does.Contain("VersionId"));
    }

    [Test]
    public void Multiple_Uploads_Create_Multiple_Versions()
    {
        var cc = _root.BlobService.GetBlobContainerClient("ver-multi") as FileBlobContainerClient;
        cc.CreateIfNotExists();
        var bc = (FileBlobClient)cc.GetBlobClient("doc.txt");

        bc.Upload(BinaryData.FromString("v1"));
        bc.Upload(BinaryData.FromString("v2"));
        bc.Upload(BinaryData.FromString("v3"));

        // Two previous versions should exist (v1 and v2 snapshots)
        var blobPath = Path.Combine(cc.ContainerPath, "doc.txt");
        var dir = Path.GetDirectoryName(blobPath)!;
        var versionFiles = Directory.GetFiles(dir, "doc.txt.version.*")
            .Where(f => !f.EndsWith(".sidecar.json"))
            .ToArray();
        Assert.That(versionFiles.Length, Is.GreaterThanOrEqualTo(2));

        // Current content should be v3
        var content = bc.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("v3"));
    }
}
