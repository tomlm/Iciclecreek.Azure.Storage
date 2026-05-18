using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;

namespace Iciclecreek.Azure.Storage.FileSystem.Queues;

/// <summary>
/// Filesystem-backed drop-in replacement for <see cref="QueueServiceClient"/>.
/// </summary>
public class FileQueueServiceClient : QueueServiceClient
{
    internal readonly string QueuesRootPath;
    internal readonly FileStorageOptions Options;
    private readonly string _accountName;
    private readonly Uri _queueServiceUri;

    public FileQueueServiceClient(string queuesRootPath, FileStorageOptions? options = null) : base()
    {
        QueuesRootPath = Path.GetFullPath(queuesRootPath);
        Directory.CreateDirectory(QueuesRootPath);
        Options = options ?? new FileStorageOptions();
        _accountName = string.Empty;
        _queueServiceUri = new Uri("file://queue/");
    }

    // ── Properties ──────────────────────────────────────────────────────

    public override string AccountName => _accountName;
    public override Uri Uri => _queueServiceUri;

    // ── GetQueueClient ──────────────────────────────────────────────────

    public override QueueClient GetQueueClient(string queueName) => new FileQueueClient(this, queueName);

    // ── CreateQueue ─────────────────────────────────────────────────────

    public override Response<QueueClient> CreateQueue(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var client = new FileQueueClient(this, queueName);
        client.Create(metadata, cancellationToken);
        return Response.FromValue<QueueClient>(client, StubResponse.Created());
    }

    public override async Task<Response<QueueClient>> CreateQueueAsync(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return CreateQueue(queueName, metadata, cancellationToken);
    }

    // ── DeleteQueue ─────────────────────────────────────────────────────

    public override Response DeleteQueue(string queueName, CancellationToken cancellationToken = default)
    {
        var client = new FileQueueClient(this, queueName);
        client.Delete(cancellationToken);
        return StubResponse.NoContent();
    }

    public override async Task<Response> DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return DeleteQueue(queueName, cancellationToken);
    }

    // ── GetQueues ───────────────────────────────────────────────────────

    public override Pageable<QueueItem> GetQueues(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(QueuesRootPath))
            return new StaticPageable<QueueItem>(Array.Empty<QueueItem>());

        var items = Directory.EnumerateDirectories(QueuesRootPath)
            .Select(d => Path.GetFileName(d))
            .Where(n => !string.IsNullOrEmpty(n) && !n.StartsWith('.') && !n.StartsWith('_'))
            .Where(n => prefix == null || n.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .OrderBy(n => n, StringComparer.Ordinal)
            .Select(n => QueuesModelFactory.QueueItem(n, metadata: null))
            .ToList();

        return new StaticPageable<QueueItem>(items);
    }

    public override AsyncPageable<QueueItem> GetQueuesAsync(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<QueueItem>(GetQueues(traits, prefix, cancellationToken));

    // ── Service Properties (stubs) ──────────────────────────────────────

    public override Response<QueueServiceProperties> GetProperties(CancellationToken cancellationToken = default)
        => Response.FromValue(new QueueServiceProperties(), StubResponse.Ok());

    public override async Task<Response<QueueServiceProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    public override Response SetProperties(QueueServiceProperties properties, CancellationToken cancellationToken = default)
        => StubResponse.Ok();

    public override async Task<Response> SetPropertiesAsync(QueueServiceProperties properties, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetProperties(properties, cancellationToken); }
}
