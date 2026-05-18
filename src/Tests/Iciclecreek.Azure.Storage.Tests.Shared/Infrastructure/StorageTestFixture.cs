using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Azure.Data.Tables;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

/// <summary>
/// Abstract factory for creating storage clients in tests.
/// Each provider (FileSystem, Sqlite) implements this to provide its own client instances.
/// </summary>
public abstract class StorageTestFixture : IDisposable
{
    // ── Blob factories ──────────────────────────────────────────────────

    public abstract BlobContainerClient CreateBlobContainerClient(string name);

    public abstract BlobServiceClient CreateBlobServiceClient();

    public abstract BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name);

    public abstract AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name);

    public abstract PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name);

    // ── Table factories ─────────────────────────────────────────────────

    public abstract TableClient CreateTableClient(string name);

    public abstract TableServiceClient CreateTableServiceClient();

    // ── Queue factories ─────────────────────────────────────────────────

    public abstract QueueClient CreateQueueClient(string name);

    public abstract QueueServiceClient CreateQueueServiceClient();

    // ── General ─────────────────────────────────────────────────────────

    /// <summary>Temp directory path for this test run (for file-based assertions).</summary>
    public abstract string TempPath { get; }

    /// <summary>The synthetic blob service URI for the test account.</summary>
    public abstract Uri BlobServiceUri { get; }

    public abstract void Dispose();
}
