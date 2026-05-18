using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;
using Iciclecreek.Azure.Storage.Memory.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Memory.Tests.Infrastructure;

public sealed class MemoryStorageTestFixture : StorageTestFixture
{
    private readonly string _tempPath;
    private readonly MemoryBlobServiceClient _blobService = new();
    private readonly MemoryTableServiceClient _tableService = new();
    private readonly MemoryQueueServiceClient _queueService = new();

    public MemoryStorageTestFixture()
    {
        _tempPath = Path.Combine(Path.GetTempPath(), "mem-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempPath);
    }

    public override string TempPath => _tempPath;
    public override Uri BlobServiceUri => _blobService.Uri;

    public override BlobContainerClient CreateBlobContainerClient(string name)
        => _blobService.GetBlobContainerClient(name);

    public override BlobServiceClient CreateBlobServiceClient()
        => _blobService;

    public override BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name)
        => ((MemoryBlobContainerClient)_blobService.GetBlobContainerClient(container.Name)).GetBlockBlobClient(name);

    public override AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name)
        => ((MemoryBlobContainerClient)_blobService.GetBlobContainerClient(container.Name)).GetAppendBlobClient(name);

    public override PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name)
        => ((MemoryBlobContainerClient)_blobService.GetBlobContainerClient(container.Name)).GetPageBlobClient(name);

    public override TableClient CreateTableClient(string name)
        => _tableService.GetTableClient(name);

    public override TableServiceClient CreateTableServiceClient()
        => _tableService;

    public override QueueClient CreateQueueClient(string name)
        => _queueService.GetQueueClient(name);

    public override QueueServiceClient CreateQueueServiceClient()
        => _queueService;

    public override void Dispose()
    {
        try { Directory.Delete(_tempPath, true); } catch { }
    }
}
