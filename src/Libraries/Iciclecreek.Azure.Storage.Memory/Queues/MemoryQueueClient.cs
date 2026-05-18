using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Queues;

/// <summary>
/// In-memory drop-in replacement for <see cref="QueueClient"/>.
/// Messages are stored in the <see cref="QueueStore.Messages"/> list.
/// All message operations are protected by <c>lock(store.Lock)</c> for thread safety.
/// </summary>
public class MemoryQueueClient : QueueClient
{
    private readonly MemoryQueueServiceClient _serviceClient;
    private readonly string _queueName;

    internal MemoryQueueClient(MemoryQueueServiceClient serviceClient, string queueName) : base()
    {
        _serviceClient = serviceClient;
        _queueName = queueName;
    }

    // ── Properties ──────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override string AccountName => _serviceClient.AccountName;
    /// <inheritdoc/>
    public override string Name => _queueName;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_serviceClient.Uri}{_queueName}");

    // ── Queue Lifecycle ─────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response Create(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var store = new QueueStore { Metadata = metadata };
        if (!_serviceClient.Queues.TryAdd(_queueName, store))
            throw new RequestFailedException(409, "Queue already exists.", "QueueAlreadyExists", null);
        return StubResponse.Created();
    }

    /// <inheritdoc/>
    public override async Task<Response> CreateAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(metadata, cancellationToken); }

    /// <inheritdoc/>
    public override Response CreateIfNotExists(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var store = new QueueStore { Metadata = metadata };
        var added = _serviceClient.Queues.TryAdd(_queueName, store);
        return added ? StubResponse.Created() : StubResponse.Ok();
    }

    /// <inheritdoc/>
    public override async Task<Response> CreateIfNotExistsAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(metadata, cancellationToken); }

    /// <inheritdoc/>
    public override Response Delete(CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Queues.TryRemove(_queueName, out _))
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(cancellationToken); }

    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(CancellationToken cancellationToken = default)
    {
        var deleted = _serviceClient.Queues.TryRemove(_queueName, out _);
        return Response.FromValue(deleted, deleted ? StubResponse.NoContent() : StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(cancellationToken); }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
    {
        var exists = _serviceClient.Queues.ContainsKey(_queueName);
        return Response.FromValue(exists, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    // ── Properties & Metadata ───────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueProperties> GetProperties(CancellationToken cancellationToken = default)
    {
        var store = GetStore();
        int count;
        IDictionary<string, string>? metadata;

        lock (store.Lock)
        {
            count = store.Messages.Count(m => !m.Deleted);
            metadata = store.Metadata is not null ? new Dictionary<string, string>(store.Metadata) : new Dictionary<string, string>();
        }

        var props = QueuesModelFactory.QueueProperties(metadata, count);
        return Response.FromValue(props, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<QueueProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    /// <inheritdoc/>
    public override Response SetMetadata(IDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    {
        var store = GetStore();
        lock (store.Lock)
        {
            store.Metadata = metadata is not null ? new Dictionary<string, string>(metadata) : null;
        }
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> SetMetadataAsync(IDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetMetadata(metadata, cancellationToken); }

    // ── SendMessage ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(string messageText)
        => SendMessage(messageText, default(CancellationToken));

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(string messageText, CancellationToken cancellationToken)
        => SendMessage(messageText, null, null, cancellationToken);

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(string messageText, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    {
        var store = GetStore();

        var now = DateTimeOffset.UtcNow;
        var messageId = Guid.NewGuid().ToString();
        var popReceipt = Guid.NewGuid().ToString();
        var ttl = timeToLive ?? TimeSpan.FromDays(7);
        var expiresOn = ttl == TimeSpan.FromSeconds(-1) ? DateTimeOffset.MaxValue : now.Add(ttl);
        var nextVisibleOn = now.Add(visibilityTimeout ?? TimeSpan.Zero);

        var entry = new MessageEntry
        {
            MessageId = messageId,
            Body = messageText,
            PopReceipt = popReceipt,
            InsertionTime = now,
            ExpirationTime = expiresOn,
            VisibleAt = nextVisibleOn,
            DequeueCount = 0,
            Deleted = false,
        };

        lock (store.Lock)
        {
            store.Messages.Add(entry);
        }

        var receipt = QueuesModelFactory.SendReceipt(messageId, now, expiresOn, popReceipt, nextVisibleOn);
        return Response.FromValue(receipt, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(BinaryData message, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
        => SendMessage(message.ToString(), visibilityTimeout, timeToLive, cancellationToken);

    /// <inheritdoc/>
    public override async Task<Response<SendReceipt>> SendMessageAsync(string messageText, CancellationToken cancellationToken)
        => await SendMessageAsync(messageText, null, null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<SendReceipt>> SendMessageAsync(string messageText, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return SendMessage(messageText, visibilityTimeout, timeToLive, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<SendReceipt>> SendMessageAsync(BinaryData message, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
        => await SendMessageAsync(message.ToString(), visibilityTimeout, timeToLive, cancellationToken).ConfigureAwait(false);

    // ── ReceiveMessages ─────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueMessage[]> ReceiveMessages(int? maxMessages = null, TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        var store = GetStore();

        var now = DateTimeOffset.UtcNow;
        var max = maxMessages ?? 1;
        var visTimeout = visibilityTimeout ?? TimeSpan.FromSeconds(30);
        var messages = new List<QueueMessage>();

        lock (store.Lock)
        {
            // Remove expired messages
            store.Messages.RemoveAll(m => m.ExpirationTime <= now);

            // Find visible, non-deleted messages
            foreach (var entry in store.Messages)
            {
                if (messages.Count >= max)
                    break;

                if (entry.Deleted || entry.VisibleAt > now)
                    continue;

                // Mark invisible and increment dequeue count
                var newPopReceipt = Guid.NewGuid().ToString();
                var newNextVisibleOn = now.Add(visTimeout);
                entry.PopReceipt = newPopReceipt;
                entry.VisibleAt = newNextVisibleOn;
                entry.DequeueCount++;

                // Clone on read
                messages.Add(QueuesModelFactory.QueueMessage(
                    entry.MessageId,
                    newPopReceipt,
                    BinaryData.FromString(entry.Body),
                    entry.DequeueCount,
                    entry.InsertionTime,
                    entry.ExpirationTime,
                    newNextVisibleOn));
            }
        }

        return Response.FromValue(messages.ToArray(), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<QueueMessage[]> ReceiveMessages(CancellationToken cancellationToken = default)
        => ReceiveMessages(null, null, cancellationToken);

    /// <inheritdoc/>
    public override Response<QueueMessage[]> ReceiveMessages()
        => ReceiveMessages(null, null, default);

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync(int? maxMessages = null, TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return ReceiveMessages(maxMessages, visibilityTimeout, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync(CancellationToken cancellationToken = default)
        => await ReceiveMessagesAsync(null, null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync()
        => await ReceiveMessagesAsync(null, null, default).ConfigureAwait(false);

    // ── ReceiveMessage (single) ─────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueMessage> ReceiveMessage(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        var result = ReceiveMessages(1, visibilityTimeout, cancellationToken);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage>> ReceiveMessageAsync(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        var result = await ReceiveMessagesAsync(1, visibilityTimeout, cancellationToken).ConfigureAwait(false);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    // ── PeekMessages ────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<PeekedMessage[]> PeekMessages(int? maxMessages = null, CancellationToken cancellationToken = default)
    {
        var store = GetStore();

        var now = DateTimeOffset.UtcNow;
        var max = maxMessages ?? 1;
        var messages = new List<PeekedMessage>();

        lock (store.Lock)
        {
            foreach (var entry in store.Messages)
            {
                if (messages.Count >= max)
                    break;

                if (entry.Deleted || entry.VisibleAt > now || entry.ExpirationTime <= now)
                    continue;

                // Clone on read - do NOT change visibility or dequeue count
                messages.Add(QueuesModelFactory.PeekedMessage(
                    entry.MessageId,
                    BinaryData.FromString(entry.Body),
                    entry.DequeueCount,
                    entry.InsertionTime,
                    entry.ExpirationTime));
            }
        }

        return Response.FromValue(messages.ToArray(), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<PeekedMessage> PeekMessage(CancellationToken cancellationToken = default)
    {
        var result = PeekMessages(1, cancellationToken);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<PeekedMessage[]>> PeekMessagesAsync(int? maxMessages = null, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return PeekMessages(maxMessages, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<PeekedMessage>> PeekMessageAsync(CancellationToken cancellationToken = default)
    {
        var result = await PeekMessagesAsync(1, cancellationToken).ConfigureAwait(false);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    // ── DeleteMessage ───────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response DeleteMessage(string messageId, string popReceipt, CancellationToken cancellationToken = default)
    {
        var store = GetStore();

        lock (store.Lock)
        {
            var entry = store.Messages.FirstOrDefault(m => m.MessageId == messageId && !m.Deleted);
            if (entry == null || entry.PopReceipt != popReceipt)
                throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);

            entry.Deleted = true;
            store.Messages.Remove(entry);
        }

        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteMessageAsync(string messageId, string popReceipt, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteMessage(messageId, popReceipt, cancellationToken); }

    // ── UpdateMessage ───────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<UpdateReceipt> UpdateMessage(string messageId, string popReceipt, string? messageText = null, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
    {
        var store = GetStore();

        var now = DateTimeOffset.UtcNow;
        var newPopReceipt = Guid.NewGuid().ToString();
        var newNextVisibleOn = now.Add(visibilityTimeout);

        lock (store.Lock)
        {
            var entry = store.Messages.FirstOrDefault(m => m.MessageId == messageId && !m.Deleted);
            if (entry == null || entry.PopReceipt != popReceipt)
                throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);

            if (messageText != null)
                entry.Body = messageText;

            entry.PopReceipt = newPopReceipt;
            entry.VisibleAt = newNextVisibleOn;
        }

        var receipt = QueuesModelFactory.UpdateReceipt(newPopReceipt, newNextVisibleOn);
        return Response.FromValue(receipt, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<UpdateReceipt> UpdateMessage(string messageId, string popReceipt, BinaryData message, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
        => UpdateMessage(messageId, popReceipt, message?.ToString(), visibilityTimeout, cancellationToken);

    /// <inheritdoc/>
    public override async Task<Response<UpdateReceipt>> UpdateMessageAsync(string messageId, string popReceipt, string? messageText = null, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return UpdateMessage(messageId, popReceipt, messageText, visibilityTimeout, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<UpdateReceipt>> UpdateMessageAsync(string messageId, string popReceipt, BinaryData message, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
        => await UpdateMessageAsync(messageId, popReceipt, message?.ToString(), visibilityTimeout, cancellationToken).ConfigureAwait(false);

    // ── ClearMessages ───────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response ClearMessages(CancellationToken cancellationToken = default)
    {
        var store = GetStore();

        lock (store.Lock)
        {
            store.Messages.Clear();
        }

        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> ClearMessagesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return ClearMessages(cancellationToken); }

    // ── Helpers ─────────────────────────────────────────────────────────

    private QueueStore GetStore()
    {
        if (_serviceClient.Queues.TryGetValue(_queueName, out var store))
            return store;
        throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);
    }
}
