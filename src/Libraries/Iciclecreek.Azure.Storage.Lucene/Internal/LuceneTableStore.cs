using System.Collections;
using System.Collections.Concurrent;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>One entity as it is kept in a table's index: its properties as JSON, its ETag and its timestamp.</summary>
internal sealed class EntityEntry
{
    public string PropertiesJson = "{}";
    public string ETag = NewETag();
    public DateTimeOffset Timestamp = DateTimeOffset.UtcNow;

    /// <summary>The table's own lock: holding it is what makes a read-check-write of an entity safe.</summary>
    public object Lock = new();

    /// <summary>Where the entry is stored, once it has been read from or added to a table.</summary>
    internal LuceneEntityMap? Owner;
    internal string? Key;

    public static string NewETag() => $"0x{Guid.NewGuid():N}";

    /// <summary>Gives the entry a new ETag and timestamp and writes it to its table.</summary>
    public void Touch()
    {
        ETag = NewETag();
        Timestamp = DateTimeOffset.UtcNow;
        Owner?.Put(Key!, this);
    }
}

/// <summary>
/// One table: a Lucene index of its own, with its own writer, holding a document per entity.
/// </summary>
internal sealed class TableStore
{
    private readonly LuceneDirectory _directory;
    // Shared with every other client in this process that has the same index open.
    private readonly WriteGate _gate;
    private IndexWriter? _writer;
    private int _depth;

    public TableStore(LuceneDirectory directory)
    {
        _directory = directory;
        _gate = IndexWriteLock.GateFor(directory);
        Entities = new LuceneEntityMap(this);
    }

    public LuceneEntityMap Entities { get; }

    public static string EntityKey(string partitionKey, string rowKey) => $"{partitionKey}\0{rowKey}";

    internal object Gate => _gate;

    /// <summary>
    /// Starts a change to the table: takes the table's turn in this process and the index's write
    /// lock against other processes, so everything read and written until the returned object is
    /// disposed is one step. Disposing commits. May be nested.
    /// <para>
    /// Clients in this process are let in in the order they asked. Another process is not in that
    /// line: if it keeps the write lock for ten seconds, this throws <c>503 ServerBusy</c>
    /// (see <see cref="IndexWriteLock"/>).
    /// </para>
    /// </summary>
    public IDisposable Enter()
    {
        _gate.Enter();
        try
        {
            if (_depth == 0)
                _writer = IndexWriteLock.OpenWriter(_directory);
            _depth++;
        }
        catch
        {
            _gate.Exit();
            throw;
        }
        return new Session(this);
    }

    private void Leave()
    {
        try
        {
            if (--_depth == 0)
            {
                var writer = _writer!;
                _writer = null;
                try
                {
                    writer.Commit();
                }
                finally
                {
                    writer.Dispose();
                }
            }
        }
        finally
        {
            _gate.Exit();
        }
    }

    /// <summary>The writer of the change in progress on this thread.</summary>
    internal IndexWriter Writer => _writer ?? throw new InvalidOperationException("The table is not being changed.");

    /// <summary>Reads the table: what the change in progress on this thread has written so far, or else what was last committed.</summary>
    internal T Read<T>(Func<IndexSearcher?, T> read)
    {
        if (_gate.IsHeldByCurrentThread && _writer != null)
        {
            using var live = DirectoryReader.Open(_writer, true);
            return read(new IndexSearcher(live));
        }
        if (!DirectoryReader.IndexExists(_directory))
            return read(null);
        using var reader = DirectoryReader.Open(_directory);
        return read(new IndexSearcher(reader));
    }

    private sealed class Session(TableStore table) : IDisposable
    {
        private bool _left;

        public void Dispose()
        {
            if (_left)
                return;
            _left = true;
            table.Leave();
        }
    }
}

/// <summary>
/// A table's entities, by key, with the few dictionary operations the table client uses. Reads
/// come from the index and writes go straight to it.
/// </summary>
internal sealed class LuceneEntityMap(TableStore table) : IEnumerable<KeyValuePair<string, EntityEntry>>
{
    private const string KeyField = "key";
    private const string JsonField = "json";
    private const string ETagField = "etag";
    private const string TimestampField = "timestamp";

    public bool TryGetValue(string key, out EntityEntry entry)
    {
        entry = table.Read(searcher =>
        {
            if (searcher == null)
                return null;
            var hits = searcher.Search(new TermQuery(new Term(KeyField, key)), 1);
            return hits.TotalHits == 0 ? null : FromDocument(searcher.Doc(hits.ScoreDocs[0].Doc));
        })!;
        return entry != null;
    }

    /// <summary>Adds the entity; false when there is one under the key already.</summary>
    public bool TryAdd(string key, EntityEntry entry)
    {
        using var change = table.Enter();
        if (TryGetValue(key, out _))
            return false;
        Put(key, entry);
        return true;
    }

    public bool TryRemove(string key, out EntityEntry removed)
    {
        using var change = table.Enter();
        if (!TryGetValue(key, out removed))
            return false;
        table.Writer.DeleteDocuments(new Term(KeyField, key));
        return true;
    }

    /// <summary>Adds what <paramref name="add"/> makes, or hands the stored entity to <paramref name="update"/>, which writes it by touching it.</summary>
    public EntityEntry AddOrUpdate(string key, Func<string, EntityEntry> add, Func<string, EntityEntry, EntityEntry> update)
    {
        using var change = table.Enter();
        if (TryGetValue(key, out var existing))
            return update(key, existing);
        var added = add(key);
        Put(key, added);
        return added;
    }

    /// <summary>Writes the entity as it stands, replacing whatever is stored under the key.</summary>
    public void Put(string key, EntityEntry entry)
    {
        using var change = table.Enter();
        entry.Owner = this;
        entry.Key = key;
        entry.Lock = table.Gate;
        table.Writer.UpdateDocument(new Term(KeyField, key), new Document
        {
            new StringField(KeyField, key, Field.Store.YES),
            new StoredField(JsonField, entry.PropertiesJson),
            new StoredField(ETagField, entry.ETag),
            new StoredField(TimestampField, entry.Timestamp.UtcTicks),
        });
    }

    /// <summary>Removes every entity.</summary>
    public void Clear()
    {
        using var change = table.Enter();
        table.Writer.DeleteAll();
    }

    public IEnumerator<KeyValuePair<string, EntityEntry>> GetEnumerator()
    {
        // Read in one go: the reader cannot stay open while the caller works through the entities.
        var all = table.Read(searcher =>
        {
            var entries = new List<KeyValuePair<string, EntityEntry>>();
            if (searcher == null || searcher.IndexReader.NumDocs == 0)
                return entries;
            foreach (var hit in searcher.Search(new MatchAllDocsQuery(), searcher.IndexReader.NumDocs).ScoreDocs)
            {
                var entry = FromDocument(searcher.Doc(hit.Doc));
                entries.Add(new KeyValuePair<string, EntityEntry>(entry.Key!, entry));
            }
            return entries;
        });
        return all.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private EntityEntry FromDocument(Document document) => new()
    {
        PropertiesJson = document.Get(JsonField),
        ETag = document.Get(ETagField),
        Timestamp = new DateTimeOffset(document.GetField(TimestampField).GetInt64Value() ?? 0, TimeSpan.Zero),
        Lock = table.Gate,
        Owner = this,
        Key = document.Get(KeyField),
    };
}

/// <summary>
/// The tables of one service client: which exist, kept in a small registry index, and the store
/// of each. Every table is the directory <c>tables/{name}</c>, with the name in lower case and
/// made safe as a folder name by <see cref="StorageNames.Directory"/>.
/// <para>
/// As in Azure, a table's name is not case sensitive: <c>Orders</c> and <c>orders</c> are one
/// table, listed under the name it was created with.
/// </para>
/// </summary>
internal sealed class LuceneTableRegistry : IDisposable
{
    private const string RegistryPath = "tables/" + StorageNames.Registry;
    /// <summary>The table's name in lower case: what it is found by.</summary>
    private const string KeyField = "key";
    /// <summary>The table's name as it was created.</summary>
    private const string NameField = "name";

    private readonly Func<string, LuceneDirectory> _directoryFactory;
    private readonly ConcurrentDictionary<string, LuceneDirectory> _directories = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, TableStore> _stores = new(StringComparer.Ordinal);
    private readonly TableStore _registry;

    public LuceneTableRegistry(Func<string, LuceneDirectory> directoryFactory)
    {
        _directoryFactory = directoryFactory;
        _registry = new TableStore(DirectoryFor(RegistryPath));
    }

    private LuceneDirectory DirectoryFor(string path) => _directories.GetOrAdd(path, _directoryFactory);

    /// <summary>What a table is known by, however its name is capitalised. Refuses a name that is empty or reserved.</summary>
    private static string Key(string tableName) => StorageNames.Check(tableName).ToLowerInvariant();

    /// <summary>The store for a table name, whether or not the table exists.</summary>
    public TableStore NewStore(string tableName) =>
        _stores.GetOrAdd(Key(tableName), key => new TableStore(DirectoryFor("tables/" + StorageNames.Directory(key))));

    private bool Exists(string tableName) =>
        _registry.Read(searcher => searcher != null && searcher.Search(new TermQuery(new Term(KeyField, Key(tableName))), 1).TotalHits > 0);

    public bool ContainsKey(string tableName) => Exists(tableName);

    public bool TryGetValue(string tableName, out TableStore store)
    {
        store = NewStore(tableName);
        return Exists(tableName);
    }

    /// <summary>Makes the table; false when it was already there.</summary>
    public bool TryAdd(string tableName, TableStore store)
    {
        using var change = _registry.Enter();
        if (Exists(tableName))
            return false;
        _registry.Writer.AddDocument(new Document
        {
            new StringField(KeyField, Key(tableName), Field.Store.YES),
            new StoredField(NameField, tableName),
        });
        return true;
    }

    public TableStore GetOrAdd(string tableName, Func<string, TableStore> create)
    {
        var store = NewStore(tableName);
        TryAdd(tableName, store);
        return store;
    }

    /// <summary>Removes the table and its entities; false when there was no such table.</summary>
    public bool TryRemove(string tableName, out TableStore store)
    {
        store = NewStore(tableName);
        using (_registry.Enter())
        {
            if (!Exists(tableName))
                return false;
            _registry.Writer.DeleteDocuments(new Term(KeyField, Key(tableName)));
        }
        store.Entities.Clear();
        return true;
    }

    public IEnumerable<string> Keys =>
        _registry.Read(searcher =>
        {
            var names = new List<string>();
            if (searcher == null || searcher.IndexReader.NumDocs == 0)
                return names;
            foreach (var hit in searcher.Search(new MatchAllDocsQuery(), searcher.IndexReader.NumDocs).ScoreDocs)
                names.Add(searcher.Doc(hit.Doc).Get(NameField));
            return names;
        });

    public void Dispose()
    {
        foreach (var directory in _directories.Values)
            directory.Dispose();
        _directories.Clear();
    }
}
