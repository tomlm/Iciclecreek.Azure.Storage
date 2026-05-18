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
    public string BlobsPath => BlobService.BlobsRootPath;
    public string TablesPath => TableService.TablesRootPath;
    public string QueuesPath => QueueService.QueuesRootPath;
    public FileBlobServiceClient BlobService { get; }
    public FileTableServiceClient TableService { get; }
    public FileQueueServiceClient QueueService { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, true); } catch { }
    }
}
