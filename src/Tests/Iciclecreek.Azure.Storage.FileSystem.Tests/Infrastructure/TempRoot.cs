using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Queues;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

public sealed class TempRoot : IDisposable
{
    public TempRoot()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            "fs-tests",
            Guid.NewGuid().ToString("N"));
        BlobsPath = System.IO.Path.Combine(Path, "blobs");
        TablesPath = System.IO.Path.Combine(Path, "tables");
        QueuesPath = System.IO.Path.Combine(Path, "queues");
        Directory.CreateDirectory(Path);
        BlobService = new FileBlobServiceClient(BlobsPath);
        TableService = new FileTableServiceClient(TablesPath);
        QueueService = new FileQueueServiceClient(QueuesPath);
    }

    public string Path { get; }
    public string BlobsPath { get; }
    public string TablesPath { get; }
    public string QueuesPath { get; }
    public FileBlobServiceClient BlobService { get; }
    public FileTableServiceClient TableService { get; }
    public FileQueueServiceClient QueueService { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, true); } catch { }
    }
}
