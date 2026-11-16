using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Azure;
using Lucene.Net.Codecs;
using Lucene.Net.Store;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>One message of a queue.</summary>
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
/// Queues kept as files through the Lucene <see cref="LuceneDirectory">Directory</see> abstraction,
/// with no index: one Lucene <c>Directory</c> per queue holding a file per message, and one for the
/// registry holding a file per queue. Only what every Lucene <c>Directory</c> gives is used (list,
/// create, read, delete and sync a file, and its lock), so a queue can be kept in any of them.
/// <para>
/// A directory's files are written once and cannot be renamed, so everything about a message that
/// changes is in the name of its file: its place in line, when it expires, when it is next
/// visible, how often it has been received, and a version. The file holds what does not change
/// and is only needed once the message is handed out: when it was sent, and its text. So finding
/// the messages to receive is one listing of the directory, with no file opened, and a change to a
/// message is a new file under the new name followed by the removal of the old one. If the process
/// dies part way, both files are there, and the message is the newest of them that is whole: the
/// new one if it was finished, and otherwise the old one, as if the change had never begun.
/// </para>
/// <para>
/// A message's pop receipt is the changing part of its file name, so the message a receipt belongs
/// to is found without a listing: there is a file of that name, or the receipt is out of date.
/// </para>
/// <para>
/// Whoever changes a queue holds the write lock of its directory for as long as the change takes,
/// as whoever changes a table holds the lock of its index: clients in one process take
/// turns in the order they asked, and another process keeps trying for the lock and is refused
/// with <c>503 ServerBusy</c> if it has not got it in ten seconds (see <see cref="IndexWriteLock"/>).
/// Reading takes no lock.
/// </para>
/// <para>
/// Messages are in line by the clock at the time they were sent, which the lock makes the same
/// order they were sent in for as long as the clock does not go backwards.
/// </para>
/// <para>
/// Every member is safe to call from several threads, and from several clients and processes that
/// share the directories.
/// </para>
/// </summary>
internal sealed class LuceneQueueStore : IDisposable
{
    private const string Registry = "queues/" + StorageNames.Registry;
    private const int FormatVersion = 0;
    private const string MessageCodec = "QueueMessage";
    private const string QueueCodec = "Queue";
    private const string QueueExtension = ".queue";

    // The place in line last given out by this process, so that two messages never share one.
    private static long s_lastSequence;

    private readonly Func<string, LuceneDirectory> _directoryFactory;
    // Lazy, so that the factory is asked once for a path however many threads first want it together;
    // a directory made twice would leave one of the two never disposed.
    private readonly ConcurrentDictionary<string, Lazy<LuceneDirectory>> _directories = new(StringComparer.Ordinal);

    public LuceneQueueStore(Func<string, LuceneDirectory> directoryFactory) => _directoryFactory = directoryFactory;

    /// <summary>
    /// Where a queue's messages are kept. The name is made safe as a folder name, so that queues whose
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
    /// Changes the files of a directory with its write lock held. One caller at a time: clients in
    /// this process take turns at the directory's gate, and the lock keeps other processes out.
    /// </summary>
    private T Locked<T>(string path, Func<LuceneDirectory, T> change)
    {
        var directory = DirectoryFor(path);
        var gate = IndexWriteLock.GateFor(directory);
        gate.Enter();
        try
        {
            using var held = IndexWriteLock.ObtainLock(directory);
            return change(directory);
        }
        finally
        {
            gate.Exit();
        }
    }

    // ── Queues ──────────────────────────────────────────────────────────
    //
    // A queue is there when the registry holds a file "{hash of its name}.{version}.queue". The
    // file holds the name and the metadata; setting the metadata writes the next version.

    public bool QueueExists(string queueName) => RegistryFiles(DirectoryFor(Registry), HashOf(queueName)).Count > 0;

    /// <summary>Makes the queue; false when it was already there. Refuses a name that is empty or reserved.</summary>
    public bool CreateQueue(string queueName, IDictionary<string, string>? metadata)
    {
        var path = QueuePath(queueName);
        return Locked(Registry, registry =>
        {
            var hash = HashOf(queueName);
            if (RegistryFiles(registry, hash).Count > 0)
                return false;
            // Nothing in the directory of a queue that does not exist can be live: it was sent while
            // the queue was being deleted, after the delete had emptied the directory and before it
            // took the queue off the list. It is emptied before the queue is listed, so the queue is
            // empty by the time anyone can find it.
            Locked(path, RemoveMessages);
            WriteQueue(registry, new RegistryFile(hash, 0), queueName, metadata);
            return true;
        });
    }

    /// <summary>Removes the queue and its messages; false when there was no such queue.</summary>
    public bool DeleteQueue(string queueName) =>
        Locked(Registry, registry =>
        {
            var files = RegistryFiles(registry, HashOf(queueName));
            if (files.Count == 0)
                return false;
            // Emptied while the registry is held, and before the queue is taken off the list. Nobody
            // can make the queue again until the registry is let go, so what is emptied here is the
            // queue being deleted and never one made since. The registry and then the queue's
            // directory is also the order CreateQueue takes them in, and nothing takes them the other
            // way round. If the directory cannot be had (503), the queue stays listed with its messages.
            Locked(QueuePath(queueName), RemoveMessages);
            // Oldest first: if one cannot be removed, what is left is the queue as it last was.
            foreach (var file in files)
                Remove(registry, file.Name);
            return true;
        });

    /// <summary>The queue's metadata, or null when there is no such queue.</summary>
    public IDictionary<string, string>? GetMetadata(string queueName)
    {
        var registry = DirectoryFor(Registry);
        var files = RegistryFiles(registry, HashOf(queueName));
        if (files.Count == 0)
            return null;
        // The newest that is whole: a newer one may be half written by whoever is setting the metadata now.
        for (var i = files.Count - 1; i >= 0; i--)
        {
            if (ReadQueue(registry, files[i].Name) is { } queue)
                return queue.Value;
        }
        return new Dictionary<string, string>();
    }

    /// <summary>Replaces the queue's metadata; false when there is no such queue.</summary>
    public bool SetMetadata(string queueName, IDictionary<string, string>? metadata) =>
        Locked(Registry, registry =>
        {
            // Looked for here, with the registry held: writing the entry of a queue that has just
            // been deleted would list the queue again.
            var files = RegistryFiles(registry, HashOf(queueName));
            if (files.Count == 0)
                return false;
            WriteQueue(registry, files[^1] with { Version = files[^1].Version + 1 }, queueName, metadata);
            foreach (var file in files)
                Remove(registry, file.Name);
            return true;
        });

    /// <summary>Every queue and its metadata, by name.</summary>
    public List<KeyValuePair<string, IDictionary<string, string>>> ListQueues()
    {
        var registry = DirectoryFor(Registry);
        var queues = new List<KeyValuePair<string, IDictionary<string, string>>>();
        // By queue, newest first; the first of each queue that is whole is the queue.
        var files = ListFiles(registry)
            .Select(name => RegistryFile.TryParse(name, out var file) ? file : (RegistryFile?)null)
            .OfType<RegistryFile>()
            .OrderBy(file => file.Hash, StringComparer.Ordinal)
            .ThenByDescending(file => file.Version);
        string? listed = null;
        foreach (var file in files)
        {
            if (file.Hash == listed || ReadQueue(registry, file.Name) is not { } queue)
                continue;
            listed = file.Hash;
            queues.Add(queue);
        }
        queues.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
        return queues;
    }

    /// <summary>The name of a queue's file in the registry, without its extension.</summary>
    private readonly record struct RegistryFile(string Hash, uint Version)
    {
        public string Name => $"{Hash}.{Version:x8}{QueueExtension}";

        public static bool TryParse(string fileName, out RegistryFile file)
        {
            var rest = fileName.AsSpan();
            if (Hex.Take(ref rest, 32, out _, out _) && Hex.Skip(ref rest, '.') && Hex.Take(ref rest, 8, out var version)
                && rest.SequenceEqual(QueueExtension))
            {
                file = new RegistryFile(fileName[..32], (uint)version);
                return true;
            }
            file = default;
            return false;
        }
    }

    /// <summary>A queue's files in the registry, oldest version first. There is more than one only while its metadata is being set.</summary>
    private static List<RegistryFile> RegistryFiles(LuceneDirectory registry, string hash)
    {
        var files = new List<RegistryFile>();
        foreach (var name in ListFiles(registry))
        {
            if (name.StartsWith(hash, StringComparison.Ordinal) && RegistryFile.TryParse(name, out var file))
                files.Add(file);
        }
        files.Sort((a, b) => a.Version.CompareTo(b.Version));
        return files;
    }

    /// <summary>
    /// A queue's name is its own and may be as long as a folder name, so its file in the registry
    /// is named by a hash of it; the name itself is in the file.
    /// </summary>
    private static string HashOf(string queueName) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(queueName)), 0, 16).ToLowerInvariant();

    private static void WriteQueue(LuceneDirectory registry, RegistryFile file, string queueName, IDictionary<string, string>? metadata) =>
        WriteFile(registry, file.Name, QueueCodec, output =>
        {
            output.WriteString(queueName);
            output.WriteStringStringMap(metadata ?? new Dictionary<string, string>());
        });

    /// <summary>The queue a registry file is for, or null when the file is not whole or cannot be read now.</summary>
    private static KeyValuePair<string, IDictionary<string, string>>? ReadQueue(LuceneDirectory registry, string fileName)
    {
        KeyValuePair<string, IDictionary<string, string>> queue = default;
        return ReadFile(registry, fileName, QueueCodec, input => queue = new(input.ReadString(), input.ReadStringStringMap())) == Read.Whole ? queue : null;
    }

    // ── Messages ────────────────────────────────────────────────────────

    /// <summary>How many messages the queue holds, hidden ones included.</summary>
    public int Count(string queueName)
    {
        var directory = DirectoryFor(QueuePath(queueName));
        return Messages(directory, ListFiles(directory), tidy: false, out _).Count;
    }

    /// <summary>
    /// Called with the queue's directory held, before anything is written to it. A caller may have
    /// found the queue and then waited for the directory while the queue was deleted; what it
    /// wrote then would be in the directory of a queue that is not there, and would turn up in the
    /// queue when it is made again.
    /// </summary>
    private void RequireQueue(string queueName)
    {
        if (!QueueExists(queueName))
            throw QueueNotFound();
    }

    internal static RequestFailedException QueueNotFound() => new(404, "Queue not found.", "QueueNotFound", null);

    /// <summary>Adds a message at the end of the line.</summary>
    public StoredMessage Enqueue(string queueName, string text, TimeSpan? visibilityTimeout, TimeSpan? timeToLive) =>
        Locked(QueuePath(queueName), directory =>
        {
            RequireQueue(queueName);
            var now = DateTimeOffset.UtcNow;
            var ttl = timeToLive ?? TimeSpan.FromDays(7);
            var id = Guid.NewGuid().ToByteArray();
            var file = new MessageFile(
                Sequence: NextSequence(now),
                IdHigh: BitConverter.ToUInt64(id, 0),
                IdLow: BitConverter.ToUInt64(id, 8),
                // A time to live of -1 second means never, as in Azure.
                ExpiresTicks: ttl < TimeSpan.Zero || ttl >= DateTimeOffset.MaxValue - now ? DateTimeOffset.MaxValue.UtcTicks : (now + ttl).UtcTicks,
                VisibleTicks: (now + (visibilityTimeout ?? TimeSpan.Zero)).UtcTicks,
                DequeueCount: 0,
                Version: 0);
            var content = new MessageContent(now.UtcTicks, text);
            WriteMessage(directory, file, content);
            return ToMessage(file, content);
        });

    /// <summary>The next place in line: the time now, or one past the last place given out if the clock has not moved on since.</summary>
    private static long NextSequence(DateTimeOffset now)
    {
        while (true)
        {
            var last = Volatile.Read(ref s_lastSequence);
            var next = Math.Max(now.UtcTicks, last + 1);
            if (Interlocked.CompareExchange(ref s_lastSequence, next, last) == last)
                return next;
        }
    }

    /// <summary>Takes up to <paramref name="max"/> visible messages, oldest first, and hides them for the timeout.</summary>
    public List<StoredMessage> Dequeue(string queueName, int max, TimeSpan visibilityTimeout)
    {
        var path = QueuePath(queueName);
        // Listed without the lock. Nothing to take and nothing to expire: say so here, so that
        // receivers polling an empty queue do not keep the directory from those who send to it.
        var listed = ListFiles(DirectoryFor(path));
        if (!AnythingToTakeOrExpire(listed))
            return new List<StoredMessage>();

        return Locked(path, directory =>
        {
            RequireQueue(queueName);
            var taken = new List<StoredMessage>();
            // The queue is not listed a second time now that it is held: a file's name is its
            // state, so every file that was listed is either still there and as it was, or gone.
            // Only if one has gone, and there is still room for more, is the queue listed again.
            if (!Take(directory, listed, max, visibilityTimeout, taken))
                Take(directory, ListFiles(directory), max, visibilityTimeout, taken);
            return taken;
        });
    }

    /// <summary>Whether the files listed hold a message that is visible or has expired.</summary>
    private static bool AnythingToTakeOrExpire(string[] listed)
    {
        var now = DateTimeOffset.UtcNow.UtcTicks;
        foreach (var name in listed)
        {
            if (MessageFile.TryParse(name, out var file) && (file.VisibleTicks <= now || file.ExpiresTicks <= now))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Hands out the first visible messages among the files listed, until <paramref name="taken"/>
    /// holds <paramref name="max"/>, and removes the messages that have expired. Called with the
    /// directory held. False when it came up short and a file that was listed was no longer
    /// there: the listing was out of date, and a newer one may hold more.
    /// </summary>
    private static bool Take(LuceneDirectory directory, string[] listed, int max, TimeSpan visibilityTimeout, List<StoredMessage> taken)
    {
        var now = DateTimeOffset.UtcNow;
        var messages = Messages(directory, listed, tidy: true, out var someGone);
        foreach (var file in messages)
        {
            if (file.ExpiresTicks <= now.UtcTicks)
            {
                Remove(directory, file.Name);
                continue;
            }
            if (taken.Count >= max || file.VisibleTicks > now.UtcTicks)
                continue;

            switch (ReadMessage(directory, file, out var content))
            {
                case Read.Whole:
                    var claimed = file with
                    {
                        VisibleTicks = (now + visibilityTimeout).UtcTicks,
                        DequeueCount = file.DequeueCount + 1,
                        Version = file.Version + 1,
                    };
                    WriteMessage(directory, claimed, content);
                    Remove(directory, file.Name);
                    taken.Add(ToMessage(claimed, content));
                    break;
                case Read.Broken:
                    // The only file of its message, and never finished: every writer holds the
                    // lock, so its sender died writing it and was never told it was sent.
                    Remove(directory, file.Name);
                    break;
                default:
                    someGone = true;
                    break;
            }
        }
        return taken.Count >= max || !someGone;
    }

    /// <summary>Up to <paramref name="max"/> visible messages, oldest first, without taking them.</summary>
    public List<StoredMessage> Peek(string queueName, int max)
    {
        var directory = DirectoryFor(QueuePath(queueName));
        var seen = new List<StoredMessage>();
        var now = DateTimeOffset.UtcNow.UtcTicks;
        foreach (var file in Messages(directory, ListFiles(directory), tidy: false, out _))
        {
            if (seen.Count >= max)
                break;
            if (file.VisibleTicks <= now && file.ExpiresTicks > now && ReadMessage(directory, file, out var content) == Read.Whole)
                seen.Add(ToMessage(file, content));
        }
        return seen;
    }

    /// <summary>Deletes the message if the pop receipt is its current one.</summary>
    public bool Delete(string queueName, string messageId, string popReceipt) =>
        Locked(QueuePath(queueName), directory =>
        {
            if (!MessageFile.TryParse(messageId, popReceipt, out var file) || !directory.FileExists(file.Name))
                return false;
            Remove(directory, file.Name);
            return true;
        });

    /// <summary>Changes the message's text and visibility if the pop receipt is its current one; it gets a new receipt.</summary>
    public StoredMessage? Update(string queueName, string messageId, string popReceipt, string? text, TimeSpan visibilityTimeout) =>
        Locked(QueuePath(queueName), directory =>
        {
            if (!MessageFile.TryParse(messageId, popReceipt, out var file) || ReadMessage(directory, file, out var content) != Read.Whole)
                return null;
            var updated = file with
            {
                VisibleTicks = (DateTimeOffset.UtcNow + visibilityTimeout).UtcTicks,
                Version = file.Version + 1,
            };
            if (text != null)
                content = content with { Text = text };
            WriteMessage(directory, updated, content);
            Remove(directory, file.Name);
            return ToMessage(updated, content);
        });

    /// <summary>Removes every message.</summary>
    public void Clear(string queueName) =>
        Locked(QueuePath(queueName), directory =>
        {
            RequireQueue(queueName);
            return RemoveMessages(directory);
        });

    private static bool RemoveMessages(LuceneDirectory directory)
    {
        foreach (var name in ListFiles(directory))
        {
            if (name.EndsWith(MessageFile.Extension, StringComparison.Ordinal))
                Remove(directory, name);
        }
        return true;
    }

    /// <summary>
    /// The messages among the files listed, in line, oldest first. A message has more than one
    /// file only if whoever was changing it has not yet removed the older one, or died at it; see
    /// <see cref="NewestWhole"/>. <paramref name="someGone"/> says that one of those files was
    /// no longer there, so the listing is out of date.
    /// </summary>
    private static ArraySegment<MessageFile> Messages(LuceneDirectory directory, string[] listed, bool tidy, out bool someGone)
    {
        someGone = false;
        var files = new MessageFile[listed.Length];
        var places = new long[listed.Length];
        var count = 0;
        foreach (var name in listed)
        {
            if (!MessageFile.TryParse(name, out var file))
                continue;
            files[count] = file;
            places[count++] = file.Sequence;
        }
        // By place in line alone, which is a sort of numbers. Files that share a place are nearly
        // always the files of one message, and are few: they are put in order among themselves below.
        Array.Sort(places, files, 0, count);

        var kept = 0;
        for (var first = 0; first < count;)
        {
            var end = first + 1;
            while (end < count && places[end] == places[first])
                end++;
            if (end - first == 1)
            {
                files[kept++] = files[first];
            }
            else
            {
                Array.Sort(files, first, end - first, MessageFile.InLine);
                for (var oldest = first; oldest < end;)
                {
                    var newest = oldest;
                    while (newest + 1 < end && files[newest + 1].IsSameMessageAs(files[oldest]))
                        newest++;
                    if (newest == oldest)
                        files[kept++] = files[oldest];
                    else if (NewestWhole(directory, files, oldest, newest, tidy, ref someGone) is { } message)
                        files[kept++] = message;
                    oldest = newest + 1;
                }
            }
            first = end;
        }
        return new ArraySegment<MessageFile>(files, 0, kept);
    }

    /// <summary>
    /// The message among several files of it, <paramref name="oldest"/> to <paramref name="newest"/>:
    /// the newest that is whole. A newer one that is not whole was being written when its writer
    /// died, with the file it was to replace still there. With <paramref name="tidy"/>, which needs
    /// the directory's lock, the others are removed: nobody is writing them any more.
    /// </summary>
    private static MessageFile? NewestWhole(LuceneDirectory directory, MessageFile[] files, int oldest, int newest, bool tidy, ref bool someGone)
    {
        for (var i = newest; i >= oldest; i--)
        {
            switch (ReadMessage(directory, files[i], out _))
            {
                case Read.Whole:
                    for (var older = oldest; tidy && older < i; older++)
                        Remove(directory, files[older].Name);
                    return files[i];
                case Read.Broken:
                    if (tidy)
                        Remove(directory, files[i].Name);
                    break;
                default:
                    someGone = true;
                    break;
            }
        }
        return null;
    }

    /// <summary>What a message's file holds: all of the message that never changes when it is received.</summary>
    private readonly record struct MessageContent(long InsertedTicks, string Text);

    private static void WriteMessage(LuceneDirectory directory, MessageFile file, MessageContent content) =>
        WriteFile(directory, file.Name, MessageCodec, output =>
        {
            output.WriteInt64(content.InsertedTicks);
            output.WriteString(content.Text);
        });

    /// <summary>Reads what the message's file holds; <paramref name="content"/> is only that when the file is whole.</summary>
    private static Read ReadMessage(LuceneDirectory directory, MessageFile file, out MessageContent content)
    {
        MessageContent read = default;
        var outcome = ReadFile(directory, file.Name, MessageCodec, input => read = new MessageContent(input.ReadInt64(), input.ReadString()));
        content = read;
        return outcome;
    }

    private static StoredMessage ToMessage(MessageFile file, MessageContent content) => new()
    {
        MessageId = file.MessageId,
        PopReceipt = file.PopReceipt,
        Text = content.Text,
        Sequence = file.Sequence,
        InsertedOn = new DateTimeOffset(content.InsertedTicks, TimeSpan.Zero),
        ExpiresOn = new DateTimeOffset(file.ExpiresTicks, TimeSpan.Zero),
        NextVisibleOn = new DateTimeOffset(file.VisibleTicks, TimeSpan.Zero),
        DequeueCount = file.DequeueCount,
    };

    /// <summary>
    /// The name of a message's file, which is the message's state:
    /// <c>{sequence}-{id}-{expires}-{visible}-{dequeue count}-{version}.msg</c>, each part in
    /// lowercase hexadecimal of a fixed width. Everything but the id is the pop receipt.
    /// </summary>
    private readonly record struct MessageFile(long Sequence, ulong IdHigh, ulong IdLow, long ExpiresTicks, long VisibleTicks, int DequeueCount, int Version)
    {
        public const string Extension = ".msg";

        public string MessageId => $"{IdHigh:x16}{IdLow:x16}";

        public string PopReceipt => $"{Sequence:x16}-{ExpiresTicks:x16}-{VisibleTicks:x16}-{DequeueCount:x8}-{Version:x8}";

        public string Name => $"{Sequence:x16}-{IdHigh:x16}{IdLow:x16}-{ExpiresTicks:x16}-{VisibleTicks:x16}-{DequeueCount:x8}-{Version:x8}{Extension}";

        public bool IsSameMessageAs(MessageFile other) => Sequence == other.Sequence && IdHigh == other.IdHigh && IdLow == other.IdLow;

        /// <summary>By place in line, and for one message by version, so that the last file of a message is its newest.</summary>
        public static readonly IComparer<MessageFile> InLine = Comparer<MessageFile>.Create((a, b) =>
        {
            var order = a.Sequence.CompareTo(b.Sequence);
            if (order == 0) order = a.IdHigh.CompareTo(b.IdHigh);
            if (order == 0) order = a.IdLow.CompareTo(b.IdLow);
            if (order == 0) order = a.Version.CompareTo(b.Version);
            // Two files of one version are only left by a writer that died; either will do, as long as it is always the same one.
            if (order == 0) order = a.VisibleTicks.CompareTo(b.VisibleTicks);
            if (order == 0) order = a.DequeueCount.CompareTo(b.DequeueCount);
            return order;
        });

        public static bool TryParse(string fileName, out MessageFile file)
        {
            var rest = fileName.AsSpan();
            if (Hex.Take(ref rest, 16, out var sequence) && Hex.Skip(ref rest, '-')
                && Hex.Take(ref rest, 32, out var idHigh, out var idLow) && Hex.Skip(ref rest, '-')
                && TryParseState(ref rest, out var expires, out var visible, out var dequeueCount, out var version)
                && rest.SequenceEqual(Extension))
            {
                file = new MessageFile((long)sequence, idHigh, idLow, expires, visible, dequeueCount, version);
                return true;
            }
            file = default;
            return false;
        }

        /// <summary>
        /// The file a message id and pop receipt name. Both come from the caller, and only what
        /// reads exactly as this class writes them is taken, so they cannot name any other file.
        /// </summary>
        public static bool TryParse(string? messageId, string? popReceipt, out MessageFile file)
        {
            var id = messageId.AsSpan();
            var rest = popReceipt.AsSpan();
            if (Hex.Take(ref id, 32, out var idHigh, out var idLow) && id.IsEmpty
                && Hex.Take(ref rest, 16, out var sequence) && Hex.Skip(ref rest, '-')
                && TryParseState(ref rest, out var expires, out var visible, out var dequeueCount, out var version)
                && rest.IsEmpty)
            {
                file = new MessageFile((long)sequence, idHigh, idLow, expires, visible, dequeueCount, version);
                return true;
            }
            file = default;
            return false;
        }

        private static bool TryParseState(ref ReadOnlySpan<char> rest, out long expires, out long visible, out int dequeueCount, out int version)
        {
            expires = visible = 0;
            dequeueCount = version = 0;
            if (!(Hex.Take(ref rest, 16, out var expiresBits) && Hex.Skip(ref rest, '-')
                && Hex.Take(ref rest, 16, out var visibleBits) && Hex.Skip(ref rest, '-')
                && Hex.Take(ref rest, 8, out var dequeueBits) && Hex.Skip(ref rest, '-')
                && Hex.Take(ref rest, 8, out var versionBits)))
                return false;
            expires = (long)expiresBits;
            visible = (long)visibleBits;
            dequeueCount = (int)dequeueBits;
            version = (int)versionBits;
            return true;
        }
    }

    /// <summary>Reads the fixed-width lowercase hexadecimal numbers that file names are made of.</summary>
    private static class Hex
    {
        public static bool Take(ref ReadOnlySpan<char> rest, int digits, out ulong value)
        {
            value = 0;
            if (rest.Length < digits)
                return false;
            foreach (var c in rest[..digits])
            {
                var digit = c is >= '0' and <= '9' ? c - '0' : c is >= 'a' and <= 'f' ? c - 'a' + 10 : -1;
                if (digit < 0)
                    return false;
                value = (value << 4) | (uint)digit;
            }
            rest = rest[digits..];
            return true;
        }

        public static bool Take(ref ReadOnlySpan<char> rest, int digits, out ulong high, out ulong low)
        {
            low = 0;
            return Take(ref rest, digits / 2, out high) && Take(ref rest, digits / 2, out low);
        }

        public static bool Skip(ref ReadOnlySpan<char> rest, char separator)
        {
            if (rest.IsEmpty || rest[0] != separator)
                return false;
            rest = rest[1..];
            return true;
        }
    }

    // ── Files ───────────────────────────────────────────────────────────

    /// <summary>
    /// Every file in the directory. A directory in a folder is only made by its first write, so
    /// until then there is no folder: that is an empty directory.
    /// </summary>
    private static string[] ListFiles(LuceneDirectory directory)
    {
        try
        {
            return directory.ListAll();
        }
        catch (DirectoryNotFoundException)
        {
            return [];
        }
    }

    /// <summary>
    /// Writes a file and has it on disk before returning: a header, what <paramref name="write"/>
    /// writes, and a checksum, so that a file that was not finished is known as such when read.
    /// </summary>
    private static void WriteFile(LuceneDirectory directory, string name, string codec, Action<IndexOutput> write)
    {
        try
        {
            using (var output = directory.CreateOutput(name, IOContext.DEFAULT))
            {
                CodecUtil.WriteHeader(output, codec, FormatVersion);
                write(output);
                CodecUtil.WriteFooter(output);
            }
            directory.Sync([name]);
        }
        catch
        {
            try
            {
                Remove(directory, name);
            }
            catch (RequestFailedException)
            {
                // What is left is not whole, and is removed by whoever next finds it.
            }
            throw;
        }
    }

    private enum Read
    {
        /// <summary>The file was read and its checksum is right.</summary>
        Whole,
        /// <summary>The file was opened and is not whole: it was never finished, has been damaged, or is not one of this store's.</summary>
        Broken,
        /// <summary>The file is gone, or cannot be opened just now.</summary>
        Unavailable,
    }

    private static Read ReadFile(LuceneDirectory directory, string name, string codec, Action<ChecksumIndexInput> read)
    {
        ChecksumIndexInput input;
        try
        {
            input = directory.OpenChecksumInput(name, IOContext.READ_ONCE);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Removed since it was listed, or held for a moment by something else.
            return Read.Unavailable;
        }
        using (input)
        {
            try
            {
                CodecUtil.CheckHeader(input, codec, FormatVersion, FormatVersion);
                read(input);
                CodecUtil.CheckFooter(input);
                return Read.Whole;
            }
            catch (IOException)
            {
                // Told from a file that could not be opened by where it is thrown, not by its type:
                // Lucene reports a wrong header, footer or checksum as a plain IOException, and a
                // file cut short as an EndOfStreamException, which is one.
                return Read.Broken;
            }
        }
    }

    /// <summary>
    /// Removes a file, which may already be gone. On Windows a file cannot be removed while someone
    /// is reading it; a reader is done in microseconds, so it is tried again, for as long as a lock
    /// is waited for, and then refused with <c>503 ServerBusy</c>.
    /// </summary>
    private static void Remove(LuceneDirectory directory, string name)
    {
        var waited = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            try
            {
                directory.DeleteFile(name);
                return;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                if (!directory.FileExists(name))
                    return;
                if (waited.Elapsed >= IndexWriteLock.LockWait)
                    throw new RequestFailedException(503,
                        $"A file of the queue is in use and did not become free within {IndexWriteLock.LockWait.TotalSeconds:0} seconds. Try again.",
                        "ServerBusy", ex);
                Thread.Sleep(1);
            }
        }
    }

    public void Dispose()
    {
        foreach (var directory in _directories.Values.Where(directory => directory.IsValueCreated))
            directory.Value.Dispose();
        _directories.Clear();
    }
}
