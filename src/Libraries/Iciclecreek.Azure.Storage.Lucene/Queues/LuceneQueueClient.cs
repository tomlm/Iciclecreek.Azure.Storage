using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.Lucene.Internal;

namespace Iciclecreek.Azure.Storage.Lucene.Queues;

/// <summary>
/// Drop-in replacement for <see cref="QueueClient"/> over a queue kept as files in a Lucene
/// <c>Directory</c> (<c>Lucene.Net.Store.Directory</c>).
/// Messages come out first in, first out; receiving one hides it from every other receiver, in
/// this process or another, until its visibility timeout runs out.
/// </summary>
public class LuceneQueueClient : QueueClient
{
    private readonly LuceneQueueServiceClient _serviceClient;
    private readonly string _queueName;

    internal LuceneQueueClient(LuceneQueueServiceClient serviceClient, string queueName) : base()
    {
        _serviceClient = serviceClient;
        _queueName = queueName;
    }

    private LuceneQueueStore Store => _serviceClient.Store;

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
        if (!Store.CreateQueue(_queueName, metadata))
            throw new RequestFailedException(409, "Queue already exists.", "QueueAlreadyExists", null);
        return StubResponse.Created();
    }

    /// <inheritdoc/>
    public override async Task<Response> CreateAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(metadata, cancellationToken); }

    /// <inheritdoc/>
    public override Response CreateIfNotExists(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
        => Store.CreateQueue(_queueName, metadata) ? StubResponse.Created() : StubResponse.Ok();

    /// <inheritdoc/>
    public override async Task<Response> CreateIfNotExistsAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(metadata, cancellationToken); }

    /// <inheritdoc/>
    public override Response Delete(CancellationToken cancellationToken = default)
    {
        if (!Store.DeleteQueue(_queueName))
            throw NotFound();
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(cancellationToken); }

    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(CancellationToken cancellationToken = default)
    {
        var deleted = Store.DeleteQueue(_queueName);
        return Response.FromValue(deleted, deleted ? StubResponse.NoContent() : StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(cancellationToken); }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => Response.FromValue(Store.QueueExists(_queueName), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    // ── Properties & Metadata ───────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueProperties> GetProperties(CancellationToken cancellationToken = default)
    {
        var metadata = Store.GetMetadata(_queueName) ?? throw NotFound();
        return Response.FromValue(QueuesModelFactory.QueueProperties(metadata, Store.Count(_queueName)), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<QueueProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    /// <inheritdoc/>
    public override Response SetMetadata(IDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    {
        if (!Store.SetMetadata(_queueName, metadata))
            throw NotFound();
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> SetMetadataAsync(IDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetMetadata(metadata, cancellationToken); }

    // ── SendMessage ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(string messageText)
        => SendMessage(messageText, null, null, default);

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(string messageText, CancellationToken cancellationToken)
        => SendMessage(messageText, null, null, cancellationToken);

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(string messageText, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    {
        EnsureQueueExists();
        var message = Store.Enqueue(_queueName, messageText, visibilityTimeout, timeToLive);
        var receipt = QueuesModelFactory.SendReceipt(message.MessageId, message.InsertedOn, message.ExpiresOn, message.PopReceipt, message.NextVisibleOn);
        return Response.FromValue(receipt, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<SendReceipt> SendMessage(BinaryData message, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
        => SendMessage(message.ToString(), visibilityTimeout, timeToLive, cancellationToken);

    /// <inheritdoc/>
    public override async Task<Response<SendReceipt>> SendMessageAsync(string messageText)
    { await Task.Yield(); return SendMessage(messageText, null, null, default); }

    /// <inheritdoc/>
    public override async Task<Response<SendReceipt>> SendMessageAsync(string messageText, CancellationToken cancellationToken)
    { await Task.Yield(); return SendMessage(messageText, null, null, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<SendReceipt>> SendMessageAsync(string messageText, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SendMessage(messageText, visibilityTimeout, timeToLive, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<SendReceipt>> SendMessageAsync(BinaryData message, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SendMessage(message.ToString(), visibilityTimeout, timeToLive, cancellationToken); }

    // ── ReceiveMessages ─────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueMessage[]> ReceiveMessages(int? maxMessages = null, TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        EnsureQueueExists();
        var messages = Store.Dequeue(_queueName, maxMessages ?? 1, visibilityTimeout ?? TimeSpan.FromSeconds(30))
            .Select(m => QueuesModelFactory.QueueMessage(
                m.MessageId, m.PopReceipt, BinaryData.FromString(m.Text), m.DequeueCount,
                nextVisibleOn: m.NextVisibleOn, insertedOn: m.InsertedOn, expiresOn: m.ExpiresOn))
            .ToArray();
        return Response.FromValue(messages, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<QueueMessage[]> ReceiveMessages(CancellationToken cancellationToken = default)
        => ReceiveMessages(null, null, cancellationToken);

    /// <inheritdoc/>
    public override Response<QueueMessage[]> ReceiveMessages()
        => ReceiveMessages(null, null, default);

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync(int? maxMessages = null, TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return ReceiveMessages(maxMessages, visibilityTimeout, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return ReceiveMessages(null, null, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync()
    { await Task.Yield(); return ReceiveMessages(null, null, default); }

    /// <inheritdoc/>
    public override Response<QueueMessage> ReceiveMessage(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
        => Response.FromValue(ReceiveMessages(1, visibilityTimeout, cancellationToken).Value.FirstOrDefault()!, StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<QueueMessage>> ReceiveMessageAsync(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return ReceiveMessage(visibilityTimeout, cancellationToken); }

    // ── PeekMessages ────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<PeekedMessage[]> PeekMessages(int? maxMessages = null, CancellationToken cancellationToken = default)
    {
        EnsureQueueExists();
        var messages = Store.Peek(_queueName, maxMessages ?? 1)
            .Select(m => QueuesModelFactory.PeekedMessage(m.MessageId, BinaryData.FromString(m.Text), m.DequeueCount, insertedOn: m.InsertedOn, expiresOn: m.ExpiresOn))
            .ToArray();
        return Response.FromValue(messages, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<PeekedMessage> PeekMessage(CancellationToken cancellationToken = default)
        => Response.FromValue(PeekMessages(1, cancellationToken).Value.FirstOrDefault()!, StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response<PeekedMessage[]>> PeekMessagesAsync(int? maxMessages = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return PeekMessages(maxMessages, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<PeekedMessage>> PeekMessageAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return PeekMessage(cancellationToken); }

    // ── DeleteMessage ───────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response DeleteMessage(string messageId, string popReceipt, CancellationToken cancellationToken = default)
    {
        if (!Store.Delete(_queueName, messageId, popReceipt))
            throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteMessageAsync(string messageId, string popReceipt, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteMessage(messageId, popReceipt, cancellationToken); }

    // ── UpdateMessage ───────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<UpdateReceipt> UpdateMessage(string messageId, string popReceipt, string? messageText = null, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
    {
        var message = Store.Update(_queueName, messageId, popReceipt, messageText, visibilityTimeout)
            ?? throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);
        return Response.FromValue(QueuesModelFactory.UpdateReceipt(message.PopReceipt, message.NextVisibleOn), StubResponse.NoContent());
    }

    /// <inheritdoc/>
    public override Response<UpdateReceipt> UpdateMessage(string messageId, string popReceipt, BinaryData message, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
        => UpdateMessage(messageId, popReceipt, message?.ToString(), visibilityTimeout, cancellationToken);

    /// <inheritdoc/>
    public override async Task<Response<UpdateReceipt>> UpdateMessageAsync(string messageId, string popReceipt, string? messageText = null, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return UpdateMessage(messageId, popReceipt, messageText, visibilityTimeout, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<UpdateReceipt>> UpdateMessageAsync(string messageId, string popReceipt, BinaryData message, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return UpdateMessage(messageId, popReceipt, message?.ToString(), visibilityTimeout, cancellationToken); }

    // ── ClearMessages ───────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Response ClearMessages(CancellationToken cancellationToken = default)
    {
        EnsureQueueExists();
        Store.Clear(_queueName);
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> ClearMessagesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return ClearMessages(cancellationToken); }

    private void EnsureQueueExists()
    {
        if (!Store.QueueExists(_queueName))
            throw NotFound();
    }

    private static RequestFailedException NotFound() => LuceneQueueStore.QueueNotFound();
}
