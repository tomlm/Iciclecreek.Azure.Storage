using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

public sealed class FileStorageTestFixture : StorageTestFixture
{
    private readonly TempRoot _root = new();

    public FileStorageTestFixture()
    {
    }

    public FileBlobServiceClient BlobService => _root.BlobService;
    public FileTableServiceClient TableService => _root.TableService;
    public FileQueueServiceClient QueueService => _root.QueueService;
    public string BlobsPath => _root.BlobsPath;
    public string TablesPath => _root.TablesPath;
    public string QueuesPath => _root.QueuesPath;

    public override string TempPath => _root.Path;
    public override Uri BlobServiceUri => _root.BlobService.Uri;

    public override BlobContainerClient CreateBlobContainerClient(string name)
        => _root.BlobService.GetBlobContainerClient(name);
    public override BlobServiceClient CreateBlobServiceClient()
        => _root.BlobService;
    public override BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name)
        => ((FileBlobContainerClient)_root.BlobService.GetBlobContainerClient(container.Name)).GetBlockBlobClient(name);
    public override AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name)
        => ((FileBlobContainerClient)_root.BlobService.GetBlobContainerClient(container.Name)).GetAppendBlobClient(name);
    public override PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name)
        => ((FileBlobContainerClient)_root.BlobService.GetBlobContainerClient(container.Name)).GetPageBlobClient(name);
    public override TableClient CreateTableClient(string name)
        => _root.TableService.GetTableClient(name);
    public override TableServiceClient CreateTableServiceClient()
        => _root.TableService;
    public override QueueClient CreateQueueClient(string name)
        => _root.QueueService.GetQueueClient(name);
    public override QueueServiceClient CreateQueueServiceClient()
        => _root.QueueService;
    public override void Dispose() => _root.Dispose();
}
