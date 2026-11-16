using System.Collections.Concurrent;
using System.Text.Json;
using Azure;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>One message as it is kept in a queue's index.</summary>
internal sealed class StoredMessage
{
    public string MessageId = "";
    public string PopReceipt = "";
    public string Text = "";
    /// <summary>The message's place in line: higher was sent later.</summary>
    public long Sequence;
    public DateTimeOffset InsertedOn;
    public DateTimeOffset ExpiresOn;
    public DateTimeOffset NextVisibleOn;
    public int DequeueCount;
}

/// <summary>
/// Queues kept in Lucene indexes: one index per queue, one document per message, and a small
/// registry index that lists the queues and their metadata.
/// <para>
/// Every operation opens the index, does its work and commits, holding the index's write lock
/// only for that long. So nothing is lost if the process dies between operations, and several
/// clients and processes can share the queues. Clients in one process take turns in the order
/// they asked. Another process has no place in that line: it keeps trying for the write lock,
/// and is refused with <c>503 ServerBusy</c> if it has not got it in ten seconds
/// (see <see cref="IndexWriteLock"/>).
/// </para>
/// </summary>
internal sealed class LuceneQueueStore : IDisposable
{
    private const string Registry = "queues/" + StorageNames.Registry;
    private const string SequenceKey = "sequence";

    private const string IdField = "id";
    private const string PopField = "pop";
    private const string TextField = "text";
    private const string SequenceField = "sequence";
    private const string InsertedField = "inserted";
    private const string ExpiresField = "expires";
    private const string VisibleField = "visible";
    private const string DequeueField = "dequeue";

    private const string NameField = "name";
    private const string MetadataField = "metadata";

    private readonly Func<string, LuceneDirectory> _directoryFactory;
    // Lazy, so that the factory is asked once for a path however many threads first want it together;
    // a directory made twice would leave one of the two never disposed.
    private readonly ConcurrentDictionary<string, Lazy<LuceneDirectory>> _directories = new(StringComparer.Ordinal);

    public LuceneQueueStore(Func<string, LuceneDirectory> directoryFactory) => _directoryFactory = directoryFactory;

    /// <summary>
    /// Where a queue's index is kept. The name is made safe as a folder name, so that queues whose
    /// names differ only by case stay apart on any file system and no name leads outside <c>queues</c>.
    /// </summary>
    private static string QueuePath(string queueName) => "queues/" + StorageNames.Directory(queueName);

    private LuceneDirectory DirectoryFor(string path)
    {
        var directory = _directories.GetOrAdd(path, p => new Lazy<LuceneDirectory>(() => _directoryFactory(p)));
        try
        {
            return directory.Value;
        }
        catch
        {
            // A Lazy keeps a failure for good; forget it, so that the factory is asked again next time.
            _directories.TryRemove(new KeyValuePair<string, Lazy<LuceneDirectory>>(path, directory));
            throw;
        }
    }

    /// <summary>
    /// Changes an index and commits. One writer at a time: clients in this process take turns at
    /// the index's gate, and the writer is opened against other processes by <see cref="IndexWriteLock"/>.
    /// </summary>
    private T Write<T>(string path, Func<IndexWriter, T> change)
    {
        var directory = DirectoryFor(path);
        var gate = IndexWriteLock.GateFor(directory);
        gate.Enter();
        try
        {
            using var writer = IndexWriteLock.OpenWriter(directory);
            var result = change(writer);
            writer.Commit();
            return result;
        }
        finally
        {
            gate.Exit();
        }
    }

    /// <summary>Reads an index as last committed; the searcher is null when nothing has been written yet.</summary>
    private T Read<T>(string path, Func<IndexSearcher?, T> read)
    {
        var directory = DirectoryFor(path);
        if (!DirectoryReader.IndexExists(directory))
            return read(null);
        using var reader = DirectoryReader.Open(directory);
        return read(new IndexSearcher(reader));
    }

    // ── Queues ──────────────────────────────────────────────────────────

    public bool QueueExists(string queueName) =>
        Read(Registry, searcher => searcher != null && searcher.Search(new TermQuery(new Term(NameField, queueName)), 1).TotalHits > 0);

    /// <summary>Makes the queue; false when it was already there. Refuses a name that is empty or reserved.</summary>
    public bool CreateQueue(string queueName, IDictionary<string, string>? metadata) =>
        Write(Registry, writer =>
        {
            StorageNames.Check(queueName);
            using var reader = DirectoryReader.Open(writer, true);
            if (new IndexSearcher(reader).Search(new TermQuery(new Term(NameField, queueName)), 1).TotalHits > 0)
                return false;
            // Nothing in the index of a queue that does not exist can be live: it was sent while the
            // queue was being deleted, after the delete had emptied the index and before it took the
            // queue off the list. It is emptied before the queue is listed, so the queue is empty by
            // the time anyone can find it.
            Write(QueuePath(queueName), Empty);
            writer.AddDocument(RegistryDocument(queueName, metadata));
            return true;
        });

    /// <summary>Removes the queue and its messages; false when there was no such queue.</summary>
    public bool DeleteQueue(string queueName) =>
        Write(Registry, writer =>
        {
            using var reader = DirectoryReader.Open(writer, true);
            if (new IndexSearcher(reader).Search(new TermQuery(new Term(NameField, queueName)), 1).TotalHits == 0)
                return false;
            // Emptied while the registry is held, and before the queue is taken off the list. Nobody
            // can make the queue again until the registry is let go, so what is emptied here is the
            // queue being deleted and never one made since. The registry and then the queue's index
            // is also the order CreateQueue takes them in, and nothing takes them the other way round.
            // If the index cannot be had (503), the queue stays listed with its messages.
            Write(QueuePath(queueName), queue => { queue.DeleteAll(); return true; });
            writer.DeleteDocuments(new Term(NameField, queueName));
            return true;
        });

    /// <summary>The queue's metadata, or null when there is no such queue.</summary>
    public IDictionary<string, string>? GetMetadata(string queueName) =>
        Read(Registry, searcher =>
        {
            if (searcher == null)
                return null;
            var hits = searcher.Search(new TermQuery(new Term(NameField, queueName)), 1);
            if (hits.TotalHits == 0)
                return null;
            return ParseMetadata(searcher.Doc(hits.ScoreDocs[0].Doc).Get(MetadataField));
        });

    /// <summary>Replaces the queue's metadata; false when there is no such queue.</summary>
    public bool SetMetadata(string queueName, IDictionary<string, string>? metadata) =>
        Write(Registry, writer =>
        {
            // Looked for here, with the registry held: writing the entry of a queue that has just
            // been deleted would list the queue again.
            using var reader = DirectoryReader.Open(writer, true);
            if (new IndexSearcher(reader).Search(new TermQuery(new Term(NameField, queueName)), 1).TotalHits == 0)
                return false;
            writer.UpdateDocument(new Term(NameField, queueName), RegistryDocument(queueName, metadata));
            return true;
        });

    /// <summary>Every queue and its metadata, by name.</summary>
    public List<KeyValuePair<string, IDictionary<string, string>>> ListQueues() =>
        Read(Registry, searcher =>
        {
            var queues = new List<KeyValuePair<string, IDictionary<string, string>>>();
            if (searcher == null || searcher.IndexReader.NumDocs == 0)
                return queues;
            foreach (var hit in searcher.Search(new MatchAllDocsQuery(), searcher.IndexReader.NumDocs).ScoreDocs)
            {
                var document = searcher.Doc(hit.Doc);
                queues.Add(new KeyValuePair<string, IDictionary<string, string>>(document.Get(NameField), ParseMetadata(document.Get(MetadataField))));
            }
            queues.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
            return queues;
        });

    private static Document RegistryDocument(string queueName, IDictionary<string, string>? metadata) => new()
    {
        new StringField(NameField, queueName, Field.Store.YES),
        new StoredField(MetadataField, JsonSerializer.Serialize(metadata ?? new Dictionary<string, string>())),
    };

    private static IDictionary<string, string> ParseMetadata(string? json) =>
        (string.IsNullOrEmpty(json) ? null : JsonSerializer.Deserialize<Dictionary<string, string>>(json!)) ?? new Dictionary<string, string>();

    // ── Messages ────────────────────────────────────────────────────────

    public int Count(string queueName) => Read(QueuePath(queueName), searcher => searcher?.IndexReader.NumDocs ?? 0);

    internal static RequestFailedException QueueNotFound() => new(404, "Queue not found.", "QueueNotFound", null);

    /// <summary>
    /// Called with the queue's index held, before anything is written to it. A caller may have
    /// found the queue and then waited for the index while the queue was deleted; what it wrote
    /// then would be in the index of a queue that is not there, and would turn up in the queue
    /// when it is made again.
    /// </summary>
    private void RequireQueue(string queueName)
    {
        if (!QueueExists(queueName))
            throw QueueNotFound();
    }

    /// <summary>Removes every message. The place in line is kept: a message sent afterwards still sorts after everything before it.</summary>
    private static bool Empty(IndexWriter writer)
    {
        var commitData = writer.CommitData;
        writer.DeleteAll();
        if (commitData != null)
            writer.SetCommitData(new Dictionary<string, string>(commitData));
        return true;
    }

    public StoredMessage Enqueue(string queueName, string text, TimeSpan? visibilityTimeout, TimeSpan? timeToLive) =>
        Write(QueuePath(queueName), writer =>
        {
            RequireQueue(queueName);
            var now = DateTimeOffset.UtcNow;
            // The place in line is kept with the index itself, so it only ever goes up, whichever process sends.
            var last = writer.CommitData != null && writer.CommitData.TryGetValue(SequenceKey, out var saved) && long.TryParse(saved, out var parsed) ? parsed : 0;
            var sequence = Math.Max(now.UtcTicks, last + 1);
            writer.SetCommitData(new Dictionary<string, string> { [SequenceKey] = sequence.ToString() });

            var ttl = timeToLive ?? TimeSpan.FromDays(7);
            var message = new StoredMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                PopReceipt = Guid.NewGuid().ToString(),
                Text = text,
                Sequence = sequence,
                InsertedOn = now,
                // A time to live of -1 second means never, as in Azure.
                ExpiresOn = ttl < TimeSpan.Zero || ttl >= DateTimeOffset.MaxValue - now ? DateTimeOffset.MaxValue : now + ttl,
                NextVisibleOn = now + (visibilityTimeout ?? TimeSpan.Zero),
                DequeueCount = 0,
            };
            writer.AddDocument(ToDocument(message));
            return message;
        });

    /// <summary>Takes up to <paramref name="max"/> visible messages, oldest first, and hides them for the timeout.</summary>
    public List<StoredMessage> Dequeue(string queueName, int max, TimeSpan visibilityTimeout)
    {
        // Nothing to take and nothing to expire: say so without opening the writer, so that
        // receivers polling an empty queue do not keep the index's lock from those who send to it.
        if (!AnythingToTakeOrExpire(queueName))
            return new List<StoredMessage>();

        return Write(QueuePath(queueName), writer =>
        {
            RequireQueue(queueName);
            var now = DateTimeOffset.UtcNow;
            writer.DeleteDocuments(NumericRangeQuery.NewInt64Range(ExpiresField, null, now.UtcTicks, true, true));

            var taken = new List<StoredMessage>();
            using var reader = DirectoryReader.Open(writer, true);
            if (reader.NumDocs == 0 || max <= 0)
                return taken;
            var searcher = new IndexSearcher(reader);
            foreach (var hit in searcher.Search(VisibleAt(now), max, OldestFirst).ScoreDocs)
            {
                var message = FromDocument(searcher.Doc(hit.Doc));
                message.DequeueCount++;
                message.PopReceipt = Guid.NewGuid().ToString();
                message.NextVisibleOn = now + visibilityTimeout;
                writer.UpdateDocument(new Term(IdField, message.MessageId), ToDocument(message));
                taken.Add(message);
            }
            return taken;
        });
    }

    /// <summary>Whether the queue, as last committed, holds a message that is visible or has expired.</summary>
    private bool AnythingToTakeOrExpire(string queueName) =>
        Read(QueuePath(queueName), searcher =>
        {
            if (searcher == null || searcher.IndexReader.NumDocs == 0)
                return false;
            var now = DateTimeOffset.UtcNow;
            var visibleOrExpired = new BooleanQuery
            {
                { VisibleAt(now), Occur.SHOULD },
                { NumericRangeQuery.NewInt64Range(ExpiresField, null, now.UtcTicks, true, true), Occur.SHOULD },
            };
            return searcher.Search(visibleOrExpired, 1).TotalHits > 0;
        });

    /// <summary>Up to <paramref name="max"/> visible messages, oldest first, without taking them.</summary>
    public List<StoredMessage> Peek(string queueName, int max) =>
        Read(QueuePath(queueName), searcher =>
        {
            var seen = new List<StoredMessage>();
            if (searcher == null || searcher.IndexReader.NumDocs == 0 || max <= 0)
                return seen;
            var now = DateTimeOffset.UtcNow;
            var visibleAndAlive = new BooleanQuery
            {
                { VisibleAt(now), Occur.MUST },
                { NumericRangeQuery.NewInt64Range(ExpiresField, now.UtcTicks, null, false, true), Occur.MUST },
            };
            foreach (var hit in searcher.Search(visibleAndAlive, max, OldestFirst).ScoreDocs)
                seen.Add(FromDocument(searcher.Doc(hit.Doc)));
            return seen;
        });

    /// <summary>Deletes the message if the pop receipt is its current one.</summary>
    public bool Delete(string queueName, string messageId, string popReceipt) =>
        Write(QueuePath(queueName), writer =>
        {
            if (Find(writer, messageId) is not { } message || message.PopReceipt != popReceipt)
                return false;
            writer.DeleteDocuments(new Term(IdField, messageId));
            return true;
        });

    /// <summary>Changes the message's text and visibility if the pop receipt is its current one; it gets a new receipt.</summary>
    public StoredMessage? Update(string queueName, string messageId, string popReceipt, string? text, TimeSpan visibilityTimeout) =>
        Write(QueuePath(queueName), writer =>
        {
            if (Find(writer, messageId) is not { } message || message.PopReceipt != popReceipt)
                return null;
            if (text != null)
                message.Text = text;
            message.PopReceipt = Guid.NewGuid().ToString();
            message.NextVisibleOn = DateTimeOffset.UtcNow + visibilityTimeout;
            writer.UpdateDocument(new Term(IdField, messageId), ToDocument(message));
            return message;
        });

    public void Clear(string queueName) =>
        Write(QueuePath(queueName), writer =>
        {
            RequireQueue(queueName);
            return Empty(writer);
        });

    private static StoredMessage? Find(IndexWriter writer, string messageId)
    {
        using var reader = DirectoryReader.Open(writer, true);
        if (reader.NumDocs == 0)
            return null;
        var searcher = new IndexSearcher(reader);
        var hits = searcher.Search(new TermQuery(new Term(IdField, messageId)), 1);
        return hits.TotalHits == 0 ? null : FromDocument(searcher.Doc(hits.ScoreDocs[0].Doc));
    }

    private static Query VisibleAt(DateTimeOffset now) => NumericRangeQuery.NewInt64Range(VisibleField, null, now.UtcTicks, true, true);

    private static Sort OldestFirst => new(new SortField(SequenceField, SortFieldType.INT64));

    private static Document ToDocument(StoredMessage message) => new()
    {
        new StringField(IdField, message.MessageId, Field.Store.YES),
        new StoredField(PopField, message.PopReceipt),
        new StoredField(TextField, message.Text),
        new Int64Field(SequenceField, message.Sequence, Field.Store.YES),
        new Int64Field(InsertedField, message.InsertedOn.UtcTicks, Field.Store.YES),
        new Int64Field(ExpiresField, message.ExpiresOn.UtcTicks, Field.Store.YES),
        new Int64Field(VisibleField, message.NextVisibleOn.UtcTicks, Field.Store.YES),
        new StoredField(DequeueField, message.DequeueCount),
    };

    private static StoredMessage FromDocument(Document document) => new()
    {
        MessageId = document.Get(IdField),
        PopReceipt = document.Get(PopField),
        Text = document.Get(TextField),
        Sequence = Ticks(document, SequenceField),
        InsertedOn = new DateTimeOffset(Ticks(document, InsertedField), TimeSpan.Zero),
        ExpiresOn = new DateTimeOffset(Ticks(document, ExpiresField), TimeSpan.Zero),
        NextVisibleOn = new DateTimeOffset(Ticks(document, VisibleField), TimeSpan.Zero),
        DequeueCount = document.GetField(DequeueField).GetInt32Value() ?? 0,
    };

    private static long Ticks(Document document, string field) => document.GetField(field).GetInt64Value() ?? 0;

    public void Dispose()
    {
        foreach (var directory in _directories.Values.Where(directory => directory.IsValueCreated))
            directory.Value.Dispose();
        _directories.Clear();
    }
}
