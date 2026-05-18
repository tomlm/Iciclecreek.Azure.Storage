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
        Directory.CreateDirectory(Path);
        BlobService = new FileBlobServiceClient(Path);
        TableService = new FileTableServiceClient(Path);
        QueueService = new FileQueueServiceClient(Path);
    }

    public string Path { get; }
    public string BlobsPath => System.IO.Path.Combine(Path, "blobs");
    public string TablesPath => System.IO.Path.Combine(Path, "tables");
    public string QueuesPath => System.IO.Path.Combine(Path, "queues");
    public FileBlobServiceClient BlobService { get; }
    public FileTableServiceClient TableService { get; }
    public FileQueueServiceClient QueueService { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, true); } catch { }
    }
}
