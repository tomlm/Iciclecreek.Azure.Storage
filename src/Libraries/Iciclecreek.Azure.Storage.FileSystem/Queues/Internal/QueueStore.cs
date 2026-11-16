using System.Text.Json;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Queues.Internal;

/// <summary>
/// Filesystem-backed queue storage. Each queue is a directory; each message is a JSON file.
/// Layout: {QueuesRootPath}/{queueName}/messages/{messageId}.json. Message IDs sort in the order
/// the messages were sent, which is what makes the queue first in, first out.
/// </summary>
internal sealed class QueueStore
{
    private readonly string _rootPath;
    internal readonly FileStorageOptions Options;

    public QueueStore(string queuesRootPath, string queueName, FileStorageOptions options)
    {
        _rootPath = queuesRootPath;
        QueueName = queueName;
        QueuePath = Path.Combine(queuesRootPath, queueName);
        MessagesPath = Path.Combine(QueuePath, "messages");
        MetadataPath = Path.Combine(QueuePath, "_meta.json");
        Options = options;
    }

    public string QueueName { get; }
    public string QueuePath { get; }
    public string MessagesPath { get; }
    public string MetadataPath { get; }

    // ── Queue CRUD ──────────────────────────────────────────────────────

    public bool QueueExists() => Directory.Exists(QueuePath);

    public bool CreateQueue()
    {
        if (Directory.Exists(QueuePath)) return false;
        Directory.CreateDirectory(MessagesPath);
        return true;
    }

    public bool DeleteQueue()
    {
        if (!Directory.Exists(QueuePath)) return false;
        Directory.Delete(QueuePath, true);
        return true;
    }

    // ── Metadata ────────────────────────────────────────────────────────

    public IDictionary<string, string> ReadMetadata()
    {
        if (!File.Exists(MetadataPath))
            return new Dictionary<string, string>();
        var json = File.ReadAllText(MetadataPath);
        return JsonSerializer.Deserialize<Dictionary<string, string>>(json, Options.JsonSerializerOptions)
            ?? new Dictionary<string, string>();
    }

    public void WriteMetadata(IDictionary<string, string> metadata)
    {
        Directory.CreateDirectory(QueuePath);
        var json = JsonSerializer.Serialize(metadata, Options.JsonSerializerOptions);
        AtomicFile.WriteAllTextAsync(MetadataPath, json).GetAwaiter().GetResult();
    }

    // ── Messages ────────────────────────────────────────────────────────

    private static long _lastTicks;

    public string MessagePath(string messageId) => Path.Combine(MessagesPath, $"{messageId}.json");

    /// <summary>
    /// A new message ID that sorts after every ID this process made before it: the time in
    /// ticks, never repeated, then a GUID. Messages are files named by their ID, so file-name
    /// order is the order the messages were sent in, and a message is still found by its ID alone.
    /// </summary>
    private static string NewMessageId()
    {
        long ticks;
        while (true)
        {
            var last = Interlocked.Read(ref _lastTicks);
            ticks = Math.Max(DateTimeOffset.UtcNow.UtcTicks, last + 1);
            if (Interlocked.CompareExchange(ref _lastTicks, ticks, last) == last)
                break;
        }
        return $"{ticks:D19}-{Guid.NewGuid():N}";
    }

    /// <summary>When a message sent now expires; a time to live of -1 second means never, as in Azure.</summary>
    private static DateTimeOffset ExpiryFor(DateTimeOffset now, TimeSpan? timeToLive)
    {
        var ttl = timeToLive ?? TimeSpan.FromDays(7);
        return ttl < TimeSpan.Zero || ttl >= DateTimeOffset.MaxValue - now ? DateTimeOffset.MaxValue : now + ttl;
    }

    /// <summary>The message files in the order the messages were sent.</summary>
    private List<string> MessageFiles() =>
        Directory.Exists(MessagesPath)
            ? Directory.EnumerateFiles(MessagesPath, "*.json").OrderBy(Path.GetFileName, StringComparer.Ordinal).ToList()
            : new List<string>();

    /// <summary>The message in the text, or null when the file is damaged (a write cut short by a crash).</summary>
    private QueueMessageFile? Parse(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<QueueMessageFile>(json, Options.JsonSerializerOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public async Task<QueueMessageFile> EnqueueAsync(string messageText, TimeSpan? visibilityTimeout, TimeSpan? timeToLive, CancellationToken ct = default)
    {
        Directory.CreateDirectory(MessagesPath);

        var now = DateTimeOffset.UtcNow;
        var msg = new QueueMessageFile
        {
            MessageId = NewMessageId(),
            PopReceipt = Guid.NewGuid().ToString(),
            MessageText = messageText,
            InsertedOn = now,
            ExpiresOn = ExpiryFor(now, timeToLive),
            NextVisibleOn = now + (visibilityTimeout ?? TimeSpan.Zero),
            DequeueCount = 0
        };

        // Written under another name and then renamed, so a receiver never sees half a message.
        var json = JsonSerializer.Serialize(msg, Options.JsonSerializerOptions);
        var path = MessagePath(msg.MessageId);
        var partial = path + ".tmp";
        await AtomicFile.WriteAllTextAsync(partial, json, ct).ConfigureAwait(false);
        File.Move(partial, path);
        return msg;
    }

    public async Task<List<QueueMessageFile>> DequeueAsync(int maxMessages, TimeSpan visibilityTimeout, CancellationToken ct = default)
    {
        var result = new List<QueueMessageFile>();
        foreach (var file in MessageFiles())
        {
            if (result.Count >= maxMessages) break;
            ct.ThrowIfCancellationRequested();

            // Read, check and claim under one exclusive hold on the file, so no two receivers get the same message.
            QueueMessageFile? claimed;
            try
            {
                claimed = await AtomicFile.ReadModifyWriteAsync<QueueMessageFile?>(file, json =>
                {
                    var now = DateTimeOffset.UtcNow;
                    var msg = Parse(json);
                    if (msg == null) return (AtomicFile.Change.Keep, "", null);
                    if (msg.ExpiresOn <= now) return (AtomicFile.Change.Delete, "", null);
                    if (msg.NextVisibleOn > now) return (AtomicFile.Change.Keep, "", null);

                    msg.DequeueCount++;
                    msg.PopReceipt = Guid.NewGuid().ToString();
                    msg.NextVisibleOn = now + visibilityTimeout;
                    return (AtomicFile.Change.Write, JsonSerializer.Serialize(msg, Options.JsonSerializerOptions), msg);
                }, null, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                // Still held by someone else after every retry. Pass over it, as over a hidden message:
                // throwing here would lose the messages already claimed for this batch, which are hidden
                // on disk by now and would never reach the caller.
                continue;
            }

            if (claimed != null)
                result.Add(claimed);
        }
        return result;
    }

    public async Task<List<QueueMessageFile>> PeekAsync(int maxMessages, CancellationToken ct = default)
    {
        var result = new List<QueueMessageFile>();
        var now = DateTimeOffset.UtcNow;
        foreach (var file in MessageFiles())
        {
            if (result.Count >= maxMessages) break;
            ct.ThrowIfCancellationRequested();

            try
            {
                var msg = Parse(await File.ReadAllTextAsync(file, ct).ConfigureAwait(false));
                if (msg == null || msg.ExpiresOn <= now || msg.NextVisibleOn > now) continue;
                result.Add(msg);
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                // Deleted meanwhile, or held by a receiver that is claiming it or (access denied, on Windows) deleting it.
            }
        }
        return result;
    }

    public bool DeleteMessage(string messageId, string popReceipt)
    {
        return AtomicFile.ReadModifyWriteAsync(MessagePath(messageId), json =>
        {
            var msg = Parse(json);
            var matches = msg != null && msg.PopReceipt == popReceipt;
            return (matches ? AtomicFile.Change.Delete : AtomicFile.Change.Keep, "", matches);
        }, false).GetAwaiter().GetResult();
    }

    public async Task<QueueMessageFile?> UpdateMessageAsync(string messageId, string popReceipt, string? messageText, TimeSpan visibilityTimeout, CancellationToken ct = default)
    {
        return await AtomicFile.ReadModifyWriteAsync<QueueMessageFile?>(MessagePath(messageId), json =>
        {
            var msg = Parse(json);
            if (msg == null || msg.PopReceipt != popReceipt) return (AtomicFile.Change.Keep, "", null);

            if (messageText != null)
                msg.MessageText = messageText;
            msg.PopReceipt = Guid.NewGuid().ToString();
            msg.NextVisibleOn = DateTimeOffset.UtcNow + visibilityTimeout;
            return (AtomicFile.Change.Write, JsonSerializer.Serialize(msg, Options.JsonSerializerOptions), msg);
        }, null, ct).ConfigureAwait(false);
    }

    public void ClearMessages()
    {
        if (!Directory.Exists(MessagesPath)) return;
        foreach (var file in Directory.EnumerateFiles(MessagesPath, "*.json"))
        {
            try { File.Delete(file); } catch { }
        }
    }

    public int GetApproximateMessageCount()
    {
        if (!Directory.Exists(MessagesPath)) return 0;
        return Directory.EnumerateFiles(MessagesPath, "*.json").Count();
    }
}
