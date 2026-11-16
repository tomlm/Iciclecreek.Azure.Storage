using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;

/// <summary>
/// Queues and tables kept in Lucene indexes, in memory or in a temporary folder. The Lucene blob
/// client supports only part of the blob API and has tests of its own, so the shared blob
/// factories are not available.
/// </summary>
public sealed class LuceneStorageTestFixture : StorageTestFixture
{
    private readonly string _folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneQueues-" + Guid.NewGuid().ToString("N"));

    private readonly bool _queuesAsFiles;

    /// <param name="onDisk">True to keep the indexes in a folder, as a real deployment would.</param>
    /// <param name="queuesAsFiles">True to keep each queue as a directory of files rather than as an index.</param>
    public LuceneStorageTestFixture(bool onDisk = false, bool queuesAsFiles = false)
    {
        _queuesAsFiles = queuesAsFiles;
        QueueService = onDisk ? QueueServices.InFolder(_folder, queuesAsFiles) : QueueServices.InMemory(queuesAsFiles);
        TableService = onDisk ? new LuceneTableServiceClient(_folder) : new LuceneTableServiceClient();
    }

    /// <summary>Another queue service client on the fixture's folder, keeping its queues the same way.</summary>
    public LuceneQueueServiceClient OpenQueueService() => QueueServices.InFolder(_folder, _queuesAsFiles);

    public LuceneTableServiceClient TableService { get; }

    public LuceneQueueServiceClient QueueService { get; }

    public override string TempPath => _folder;
    public override Uri BlobServiceUri => throw NoSuch("blobs");

    public override QueueClient CreateQueueClient(string name) => QueueService.GetQueueClient(name);
    public override QueueServiceClient CreateQueueServiceClient() => QueueService;

    public override BlobContainerClient CreateBlobContainerClient(string name) => throw NoSuch("blobs");
    public override BlobServiceClient CreateBlobServiceClient() => throw NoSuch("blobs");
    public override BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name) => throw NoSuch("blobs");
    public override AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name) => throw NoSuch("blobs");
    public override PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name) => throw NoSuch("blobs");
    public override TableClient CreateTableClient(string name) => TableService.GetTableClient(name);
    public override TableServiceClient CreateTableServiceClient() => TableService;

    private static NotSupportedException NoSuch(string what) => new($"The shared {what} tests do not apply to Iciclecreek.Azure.Storage.Lucene.");

    public override void Dispose()
    {
        QueueService.Dispose();
        TableService.Dispose();
        if (Directory.Exists(_folder))
            Directory.Delete(_folder, recursive: true);
    }
}
