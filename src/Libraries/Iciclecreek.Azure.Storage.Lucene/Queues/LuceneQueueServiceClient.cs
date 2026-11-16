using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.Lucene.Internal;
using Lucene.Net.Store;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Queues;

/// <summary>
/// Drop-in replacement for <see cref="QueueServiceClient"/> that keeps queues in Lucene indexes:
/// one index per queue, with a document per message. The indexes live wherever the directory
/// factory puts them: in memory, in a folder, or in any other Lucene <c>Directory</c>.
/// <para>
/// In the directory name, any character of the queue's name other than a lowercase letter, a
/// digit or a hyphen is written as <c>_</c> and its four-digit hexadecimal code, so that every
/// queue is a folder of its own inside <c>queues</c> on any file system. The name
/// <c>__registry__</c> is refused with <c>400 InvalidResourceName</c>.
/// </para>
/// </summary>
public class LuceneQueueServiceClient : QueueServiceClient, IDisposable
{
    private readonly Uri _queueServiceUri = new("lucene://queue/");

    internal LuceneQueueStore Store { get; }

    /// <summary>Queues in memory: gone when this client is.</summary>
    public LuceneQueueServiceClient() : this(InMemory())
    {
    }

    /// <summary>Queues in folders under <paramref name="rootPath"/>, one folder per queue.</summary>
    public LuceneQueueServiceClient(string rootPath) : this(InFolder(rootPath))
    {
    }

    /// <summary>
    /// Queues in the directories the factory gives. It is asked for <c>queues/{queueName}</c> for
    /// each queue and <c>queues/__registry__</c> for the list of queues; each name is asked for
    /// once, and the directory is disposed with this client.
    /// </summary>
    public LuceneQueueServiceClient(Func<string, LuceneDirectory> directoryFactory) : base()
    {
        Store = new LuceneQueueStore(directoryFactory ?? throw new ArgumentNullException(nameof(directoryFactory)));
    }

    private static Func<string, LuceneDirectory> InMemory()
    {
        var directories = new ConcurrentDictionary<string, LuceneDirectory>(StringComparer.Ordinal);
        return path => directories.GetOrAdd(path, _ => new RAMDirectory());
    }

    private static Func<string, LuceneDirectory> InFolder(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
            throw new ArgumentException("A folder is needed.", nameof(rootPath));
        var root = Path.GetFullPath(rootPath);
        return path => FSDirectory.Open(Path.Combine(root, path.Replace('/', Path.DirectorySeparatorChar)));
    }

    // ── Properties ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override string AccountName => string.Empty;
    /// <inheritdoc/>
    public override Uri Uri => _queueServiceUri;

    // ── GetQueueClient ──────────────────────────────────────────────────

    /// <inheritdoc/>
    public override QueueClient GetQueueClient(string queueName) => new LuceneQueueClient(this, queueName);

    // ── CreateQueue ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueClient> CreateQueue(string queueName, IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var client = new LuceneQueueClient(this, queueName);
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
        new LuceneQueueClient(this, queueName).Delete(cancellationToken);
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
        foreach (var queue in Store.ListQueues())
        {
            if (prefix != null && !queue.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                continue;
            items.Add(QueuesModelFactory.QueueItem(queue.Key, metadata: queue.Value));
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

    /// <summary>Closes the directories the queues are kept in.</summary>
    public void Dispose()
    {
        Store.Dispose();
        GC.SuppressFinalize(this);
    }
}
