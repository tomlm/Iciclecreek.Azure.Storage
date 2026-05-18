using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.FileSystem.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Queues.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Queues;

/// <summary>
/// Filesystem-backed drop-in replacement for <see cref="QueueClient"/>.
/// Messages are stored as individual JSON files under a <c>messages/</c> subdirectory.
/// </summary>
public class FileQueueClient : QueueClient
{
    private readonly FileQueueServiceClient _serviceClient;
    private readonly QueueStore _store;

    internal FileQueueClient(FileQueueServiceClient serviceClient, string queueName) : base()
    {
        _serviceClient = serviceClient;
        _store = new QueueStore(serviceClient.QueuesRootPath, queueName, serviceClient.Options);
    }

    // ── Properties ──────────────────────────────────────────────────────

    public override string AccountName => _serviceClient.AccountName;
    public override string Name => _store.QueueName;
    public override Uri Uri => new($"{_serviceClient.Uri}{_store.QueueName}");

    // ── Queue Lifecycle ─────────────────────────────────────────────────

    public override Response Create(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        _store.CreateQueue();
        if (metadata != null && metadata.Count > 0)
            _store.WriteMetadata(metadata);
        return StubResponse.Created();
    }

    public override async Task<Response> CreateAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(metadata, cancellationToken); }

    public override Response CreateIfNotExists(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        var created = _store.CreateQueue();
        if (created && metadata != null && metadata.Count > 0)
            _store.WriteMetadata(metadata);
        return created ? StubResponse.Created() : StubResponse.Ok();
    }

    public override async Task<Response> CreateIfNotExistsAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(metadata, cancellationToken); }

    public override Response Delete(CancellationToken cancellationToken = default)
    {
        if (!_store.DeleteQueue())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);
        return StubResponse.NoContent();
    }

    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(cancellationToken); }

    public override Response<bool> DeleteIfExists(CancellationToken cancellationToken = default)
    {
        var deleted = _store.DeleteQueue();
        return Response.FromValue(deleted, deleted ? StubResponse.NoContent() : StubResponse.Ok());
    }

    public override async Task<Response<bool>> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(cancellationToken); }

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.QueueExists(), StubResponse.Ok());

    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    // ── Properties & Metadata ───────────────────────────────────────────

    public override Response<QueueProperties> GetProperties(CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var metadata = _store.ReadMetadata();
        var count = _store.GetApproximateMessageCount();
        var props = QueuesModelFactory.QueueProperties(metadata, count);
        return Response.FromValue(props, StubResponse.Ok());
    }

    public override async Task<Response<QueueProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    public override Response SetMetadata(IDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);
        _store.WriteMetadata(metadata ?? new Dictionary<string, string>());
        return StubResponse.NoContent();
    }

    public override async Task<Response> SetMetadataAsync(IDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetMetadata(metadata, cancellationToken); }

    // ── SendMessage ─────────────────────────────────────────────────────

    public override Response<SendReceipt> SendMessage(string messageText)
        => SendMessage(messageText, default(CancellationToken));

    public override Response<SendReceipt> SendMessage(string messageText, CancellationToken cancellationToken)
        => SendMessage(messageText, null, null, cancellationToken);

    public override Response<SendReceipt> SendMessage(string messageText, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var msg = _store.EnqueueAsync(messageText, visibilityTimeout, timeToLive, cancellationToken).GetAwaiter().GetResult();
        var receipt = QueuesModelFactory.SendReceipt(msg.MessageId, msg.InsertedOn, msg.ExpiresOn, msg.PopReceipt, msg.NextVisibleOn);
        return Response.FromValue(receipt, StubResponse.Created());
    }

    public override Response<SendReceipt> SendMessage(BinaryData message, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
        => SendMessage(message.ToString(), visibilityTimeout, timeToLive, cancellationToken);

    public override async Task<Response<SendReceipt>> SendMessageAsync(string messageText, CancellationToken cancellationToken)
        => await SendMessageAsync(messageText, null, null, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<SendReceipt>> SendMessageAsync(string messageText, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var msg = await _store.EnqueueAsync(messageText, visibilityTimeout, timeToLive, cancellationToken).ConfigureAwait(false);
        var receipt = QueuesModelFactory.SendReceipt(msg.MessageId, msg.InsertedOn, msg.ExpiresOn, msg.PopReceipt, msg.NextVisibleOn);
        return Response.FromValue(receipt, StubResponse.Created());
    }

    public override async Task<Response<SendReceipt>> SendMessageAsync(BinaryData message, TimeSpan? visibilityTimeout = null, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
        => await SendMessageAsync(message.ToString(), visibilityTimeout, timeToLive, cancellationToken).ConfigureAwait(false);

    // ── ReceiveMessages ─────────────────────────────────────────────────

    public override Response<QueueMessage[]> ReceiveMessages(int? maxMessages = null, TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var msgs = _store.DequeueAsync(maxMessages ?? 1, visibilityTimeout ?? TimeSpan.FromSeconds(30), cancellationToken).GetAwaiter().GetResult();
        var result = msgs.Select(m => QueuesModelFactory.QueueMessage(
            m.MessageId, m.PopReceipt, BinaryData.FromString(m.MessageText), m.DequeueCount, m.InsertedOn, m.ExpiresOn, m.NextVisibleOn)).ToArray();
        return Response.FromValue(result, StubResponse.Ok());
    }

    public override Response<QueueMessage[]> ReceiveMessages(CancellationToken cancellationToken = default)
        => ReceiveMessages(null, null, cancellationToken);

    public override Response<QueueMessage[]> ReceiveMessages()
        => ReceiveMessages(null, null, default);

    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync(int? maxMessages = null, TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var msgs = await _store.DequeueAsync(maxMessages ?? 1, visibilityTimeout ?? TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
        var result = msgs.Select(m => QueuesModelFactory.QueueMessage(
            m.MessageId, m.PopReceipt, BinaryData.FromString(m.MessageText), m.DequeueCount, m.InsertedOn, m.ExpiresOn, m.NextVisibleOn)).ToArray();
        return Response.FromValue(result, StubResponse.Ok());
    }

    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync(CancellationToken cancellationToken = default)
        => await ReceiveMessagesAsync(null, null, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<QueueMessage[]>> ReceiveMessagesAsync()
        => await ReceiveMessagesAsync(null, null, default).ConfigureAwait(false);

    // ── ReceiveMessage (single) ─────────────────────────────────────────

    public override Response<QueueMessage> ReceiveMessage(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        var result = ReceiveMessages(1, visibilityTimeout, cancellationToken);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    public override async Task<Response<QueueMessage>> ReceiveMessageAsync(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
    {
        var result = await ReceiveMessagesAsync(1, visibilityTimeout, cancellationToken).ConfigureAwait(false);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    // ── PeekMessages ────────────────────────────────────────────────────

    public override Response<PeekedMessage[]> PeekMessages(int? maxMessages = null, CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var msgs = _store.PeekAsync(maxMessages ?? 1, cancellationToken).GetAwaiter().GetResult();
        var result = msgs.Select(m => QueuesModelFactory.PeekedMessage(
            m.MessageId, BinaryData.FromString(m.MessageText), m.DequeueCount, m.InsertedOn, m.ExpiresOn)).ToArray();
        return Response.FromValue(result, StubResponse.Ok());
    }

    public override Response<PeekedMessage> PeekMessage(CancellationToken cancellationToken = default)
    {
        var result = PeekMessages(1, cancellationToken);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    public override async Task<Response<PeekedMessage[]>> PeekMessagesAsync(int? maxMessages = null, CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var msgs = await _store.PeekAsync(maxMessages ?? 1, cancellationToken).ConfigureAwait(false);
        var result = msgs.Select(m => QueuesModelFactory.PeekedMessage(
            m.MessageId, BinaryData.FromString(m.MessageText), m.DequeueCount, m.InsertedOn, m.ExpiresOn)).ToArray();
        return Response.FromValue(result, StubResponse.Ok());
    }

    public override async Task<Response<PeekedMessage>> PeekMessageAsync(CancellationToken cancellationToken = default)
    {
        var result = await PeekMessagesAsync(1, cancellationToken).ConfigureAwait(false);
        var msg = result.Value.Length > 0 ? result.Value[0] : null;
        return Response.FromValue(msg!, StubResponse.Ok());
    }

    // ── DeleteMessage ───────────────────────────────────────────────────

    public override Response DeleteMessage(string messageId, string popReceipt, CancellationToken cancellationToken = default)
    {
        if (!_store.DeleteMessage(messageId, popReceipt))
            throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);
        return StubResponse.NoContent();
    }

    public override async Task<Response> DeleteMessageAsync(string messageId, string popReceipt, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteMessage(messageId, popReceipt, cancellationToken); }

    // ── UpdateMessage ───────────────────────────────────────────────────

    public override Response<UpdateReceipt> UpdateMessage(string messageId, string popReceipt, string? messageText = null, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
    {
        var msg = _store.UpdateMessageAsync(messageId, popReceipt, messageText, visibilityTimeout, cancellationToken).GetAwaiter().GetResult();
        if (msg == null)
            throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);

        var receipt = QueuesModelFactory.UpdateReceipt(msg.PopReceipt, msg.NextVisibleOn);
        return Response.FromValue(receipt, StubResponse.Ok());
    }

    public override Response<UpdateReceipt> UpdateMessage(string messageId, string popReceipt, BinaryData message, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
        => UpdateMessage(messageId, popReceipt, message?.ToString(), visibilityTimeout, cancellationToken);

    public override async Task<Response<UpdateReceipt>> UpdateMessageAsync(string messageId, string popReceipt, string? messageText = null, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
    {
        var msg = await _store.UpdateMessageAsync(messageId, popReceipt, messageText, visibilityTimeout, cancellationToken).ConfigureAwait(false);
        if (msg == null)
            throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);

        var receipt = QueuesModelFactory.UpdateReceipt(msg.PopReceipt, msg.NextVisibleOn);
        return Response.FromValue(receipt, StubResponse.Ok());
    }

    public override async Task<Response<UpdateReceipt>> UpdateMessageAsync(string messageId, string popReceipt, BinaryData message, TimeSpan visibilityTimeout = default, CancellationToken cancellationToken = default)
        => await UpdateMessageAsync(messageId, popReceipt, message?.ToString(), visibilityTimeout, cancellationToken).ConfigureAwait(false);

    // ── ClearMessages ───────────────────────────────────────────────────

    public override Response ClearMessages(CancellationToken cancellationToken = default)
    {
        if (!_store.QueueExists())
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);
        _store.ClearMessages();
        return StubResponse.NoContent();
    }

    public override async Task<Response> ClearMessagesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return ClearMessages(cancellationToken); }
}
