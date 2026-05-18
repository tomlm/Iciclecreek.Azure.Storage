using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Queues;

/// <summary>
/// In-memory drop-in replacement for <see cref="QueueServiceClient"/>.
/// </summary>
public class MemoryQueueServiceClient : QueueServiceClient
{
    internal readonly ConcurrentDictionary<string, QueueStore> Queues = new();
    private readonly string _accountName;
    private readonly Uri _queueServiceUri;

    public MemoryQueueServiceClient() : base()
    {
        _accountName = string.Empty;
        _queueServiceUri = new Uri("memory://queue/");
    }

    // ── Properties ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override string AccountName => _accountName;
    /// <inheritdoc/>
    public override Uri Uri => _queueServiceUri;

    // ── GetQueueClient ──────────────────────────────────────────────────

    /// <inheritdoc/>
    public override QueueClient GetQueueClient(string queueName) => new MemoryQueueClient(this, queueName);

    // ── CreateQueue ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueClient> CreateQueue(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var client = new MemoryQueueClient(this, queueName);
        client.Create(metadata, cancellationToken);
        return Response.FromValue<QueueClient>(client, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<QueueClient>> CreateQueueAsync(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return CreateQueue(queueName, metadata, cancellationToken);
    }

    // ── DeleteQueue ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response DeleteQueue(string queueName, CancellationToken cancellationToken = default)
    {
        var client = new MemoryQueueClient(this, queueName);
        client.Delete(cancellationToken);
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return DeleteQueue(queueName, cancellationToken);
    }

    // ── GetQueues ───────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Pageable<QueueItem> GetQueues(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
    {
        var items = new List<QueueItem>();
        foreach (var kvp in Queues.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            if (prefix != null && !kvp.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                continue;
            items.Add(QueuesModelFactory.QueueItem(kvp.Key, metadata: kvp.Value.Metadata));
        }
        return new StaticPageable<QueueItem>(items);
    }

    /// <inheritdoc/>
    public override AsyncPageable<QueueItem> GetQueuesAsync(QueueTraits traits = QueueTraits.None, string? prefix = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<QueueItem>(GetQueues(traits, prefix, cancellationToken));

    // ── Service Properties (stubs) ──────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueServiceProperties> GetProperties(CancellationToken cancellationToken = default)
        => Response.FromValue(new QueueServiceProperties(), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<QueueServiceProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    /// <inheritdoc/>
    public override Response SetProperties(QueueServiceProperties properties, CancellationToken cancellationToken = default)
        => StubResponse.Ok();

    /// <inheritdoc/>
    public override async Task<Response> SetPropertiesAsync(QueueServiceProperties properties, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetProperties(properties, cancellationToken); }
}
