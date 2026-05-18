using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tables;
using Iciclecreek.Azure.Storage.SQLite.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;

public sealed class SqliteStorageTestFixture : StorageTestFixture
{
    private readonly TempDb _db = new();
    private readonly SqliteBlobServiceClient _blobService;
    private readonly SqliteTableServiceClient _tableService;
    private readonly SqliteQueueServiceClient _queueService;

    public SqliteStorageTestFixture()
    {
        _blobService = new SqliteBlobServiceClient(_db.DbPath);
        _tableService = new SqliteTableServiceClient(_db.DbPath);
        _queueService = new SqliteQueueServiceClient(_db.DbPath);
    }

    public override string TempPath => _db.Path;
    public override Uri BlobServiceUri => _blobService.Uri;

    public override BlobContainerClient CreateBlobContainerClient(string name)
        => _blobService.GetBlobContainerClient(name);
    public override BlobServiceClient CreateBlobServiceClient()
        => _blobService;
    public override BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name)
        => ((SqliteBlobContainerClient)_blobService.GetBlobContainerClient(container.Name)).GetBlockBlobClient(name);
    public override AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name)
        => ((SqliteBlobContainerClient)_blobService.GetBlobContainerClient(container.Name)).GetAppendBlobClient(name);
    public override PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name)
        => ((SqliteBlobContainerClient)_blobService.GetBlobContainerClient(container.Name)).GetPageBlobClient(name);
    public override TableClient CreateTableClient(string name)
        => _tableService.GetTableClient(name);
    public override TableServiceClient CreateTableServiceClient()
        => _tableService;
    public override QueueClient CreateQueueClient(string name)
        => _queueService.GetQueueClient(name);
    public override QueueServiceClient CreateQueueServiceClient()
        => _queueService;
    public override void Dispose() => _db.Dispose();
}
