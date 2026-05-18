using System.Text.Json;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Queues.Internal;

/// <summary>
/// Filesystem-backed queue storage. Each queue is a directory; each message is a JSON file.
/// Layout: {QueuesRootPath}/{queueName}/messages/{messageId}.json
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

    public string MessagePath(string messageId) => Path.Combine(MessagesPath, $"{messageId}.json");

    public async Task<QueueMessageFile> EnqueueAsync(string messageText, TimeSpan? visibilityTimeout, TimeSpan? timeToLive, CancellationToken ct = default)
    {
        Directory.CreateDirectory(MessagesPath);

        var now = DateTimeOffset.UtcNow;
        var msg = new QueueMessageFile
        {
            MessageId = Guid.NewGuid().ToString(),
            PopReceipt = Guid.NewGuid().ToString(),
            MessageText = messageText,
            InsertedOn = now,
            ExpiresOn = now + (timeToLive ?? TimeSpan.FromDays(7)),
            NextVisibleOn = now + (visibilityTimeout ?? TimeSpan.Zero),
            DequeueCount = 0
        };

        var json = JsonSerializer.Serialize(msg, Options.JsonSerializerOptions);
        await AtomicFile.WriteAllTextAsync(MessagePath(msg.MessageId), json, ct).ConfigureAwait(false);
        return msg;
    }

    public async Task<List<QueueMessageFile>> DequeueAsync(int maxMessages, TimeSpan visibilityTimeout, CancellationToken ct = default)
    {
        var result = new List<QueueMessageFile>();
        if (!Directory.Exists(MessagesPath)) return result;

        var now = DateTimeOffset.UtcNow;
        var files = Directory.EnumerateFiles(MessagesPath, "*.json")
            .OrderBy(f => f) // deterministic order
            .ToList();

        foreach (var file in files)
        {
            if (result.Count >= maxMessages) break;
            ct.ThrowIfCancellationRequested();

            try
            {
                var json = await File.ReadAllTextAsync(file, ct).ConfigureAwait(false);
                var msg = JsonSerializer.Deserialize<QueueMessageFile>(json, Options.JsonSerializerOptions);
                if (msg == null) continue;

                // Skip expired
                if (msg.ExpiresOn <= now)
                {
                    try { File.Delete(file); } catch { }
                    continue;
                }

                // Skip not yet visible
                if (msg.NextVisibleOn > now) continue;

                // Dequeue: update visibility + count
                msg.DequeueCount++;
                msg.PopReceipt = Guid.NewGuid().ToString();
                msg.NextVisibleOn = now + visibilityTimeout;

                var updated = JsonSerializer.Serialize(msg, Options.JsonSerializerOptions);
                await AtomicFile.WriteAllTextAsync(file, updated, ct).ConfigureAwait(false);
                result.Add(msg);
            }
            catch (IOException) { /* file locked or deleted concurrently */ }
        }

        return result;
    }

    public async Task<List<QueueMessageFile>> PeekAsync(int maxMessages, CancellationToken ct = default)
    {
        var result = new List<QueueMessageFile>();
        if (!Directory.Exists(MessagesPath)) return result;

        var now = DateTimeOffset.UtcNow;
        var files = Directory.EnumerateFiles(MessagesPath, "*.json")
            .OrderBy(f => f)
            .ToList();

        foreach (var file in files)
        {
            if (result.Count >= maxMessages) break;
            ct.ThrowIfCancellationRequested();

            try
            {
                var json = await File.ReadAllTextAsync(file, ct).ConfigureAwait(false);
                var msg = JsonSerializer.Deserialize<QueueMessageFile>(json, Options.JsonSerializerOptions);
                if (msg == null) continue;

                if (msg.ExpiresOn <= now) continue;
                if (msg.NextVisibleOn > now) continue;

                result.Add(msg);
            }
            catch (IOException) { }
        }

        return result;
    }

    public bool DeleteMessage(string messageId, string popReceipt)
    {
        var path = MessagePath(messageId);
        if (!File.Exists(path)) return false;

        var json = File.ReadAllText(path);
        var msg = JsonSerializer.Deserialize<QueueMessageFile>(json, Options.JsonSerializerOptions);
        if (msg == null || msg.PopReceipt != popReceipt) return false;

        File.Delete(path);
        return true;
    }

    public async Task<QueueMessageFile?> UpdateMessageAsync(string messageId, string popReceipt, string? messageText, TimeSpan visibilityTimeout, CancellationToken ct = default)
    {
        var path = MessagePath(messageId);
        if (!File.Exists(path)) return null;

        var json = await File.ReadAllTextAsync(path, ct).ConfigureAwait(false);
        var msg = JsonSerializer.Deserialize<QueueMessageFile>(json, Options.JsonSerializerOptions);
        if (msg == null || msg.PopReceipt != popReceipt) return null;

        if (messageText != null)
            msg.MessageText = messageText;
        msg.PopReceipt = Guid.NewGuid().ToString();
        msg.NextVisibleOn = DateTimeOffset.UtcNow + visibilityTimeout;

        var updated = JsonSerializer.Serialize(msg, Options.JsonSerializerOptions);
        await AtomicFile.WriteAllTextAsync(path, updated, ct).ConfigureAwait(false);
        return msg;
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
