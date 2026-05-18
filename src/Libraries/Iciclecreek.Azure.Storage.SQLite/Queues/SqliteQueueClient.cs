using System.Text.Json;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.SQLite.Internal;
using Microsoft.Data.Sqlite;

namespace Iciclecreek.Azure.Storage.SQLite.Queues;

/// <summary>
/// SQLite-backed drop-in replacement for <see cref="QueueClient"/>.
/// Messages are stored in the Messages table of the SQLite database.
/// </summary>
public class SqliteQueueClient : QueueClient
{
    private readonly SqliteQueueServiceClient _serviceClient;
    private readonly string _queueName;

    internal SqliteQueueClient(SqliteQueueServiceClient serviceClient, string queueName) : base()
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
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO Queues (Name, Metadata) VALUES (@name, @metadata)";
        cmd.Parameters.AddWithValue("@name", _queueName);
        cmd.Parameters.AddWithValue("@metadata", metadata != null && metadata.Count > 0 ? JsonSerializer.Serialize(metadata) : (object)DBNull.Value);

        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            throw new RequestFailedException(409, "Queue already exists.", "QueueAlreadyExists", null);
        }
        return StubResponse.Created();
    }

    /// <inheritdoc/>
    public override async Task<Response> CreateAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(metadata, cancellationToken); }

    /// <inheritdoc/>
    public override Response CreateIfNotExists(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT OR IGNORE INTO Queues (Name, Metadata) VALUES (@name, @metadata)";
        cmd.Parameters.AddWithValue("@name", _queueName);
        cmd.Parameters.AddWithValue("@metadata", metadata != null && metadata.Count > 0 ? JsonSerializer.Serialize(metadata) : (object)DBNull.Value);
        var rows = cmd.ExecuteNonQuery();
        return rows > 0 ? StubResponse.Created() : StubResponse.Ok();
    }

    /// <inheritdoc/>
    public override async Task<Response> CreateIfNotExistsAsync(IDictionary<string, string>? metadata = null, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(metadata, cancellationToken); }

    /// <inheritdoc/>
    public override Response Delete(CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var tx = conn.BeginTransaction();

        using var delMsgs = conn.CreateCommand();
        delMsgs.Transaction = tx;
        delMsgs.CommandText = "DELETE FROM Messages WHERE QueueName = @name";
        delMsgs.Parameters.AddWithValue("@name", _queueName);
        delMsgs.ExecuteNonQuery();

        using var delQueue = conn.CreateCommand();
        delQueue.Transaction = tx;
        delQueue.CommandText = "DELETE FROM Queues WHERE Name = @name";
        delQueue.Parameters.AddWithValue("@name", _queueName);
        var rows = delQueue.ExecuteNonQuery();

        tx.Commit();

        if (rows == 0)
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(cancellationToken); }

    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var tx = conn.BeginTransaction();

        using var delMsgs = conn.CreateCommand();
        delMsgs.Transaction = tx;
        delMsgs.CommandText = "DELETE FROM Messages WHERE QueueName = @name";
        delMsgs.Parameters.AddWithValue("@name", _queueName);
        delMsgs.ExecuteNonQuery();

        using var delQueue = conn.CreateCommand();
        delQueue.Transaction = tx;
        delQueue.CommandText = "DELETE FROM Queues WHERE Name = @name";
        delQueue.Parameters.AddWithValue("@name", _queueName);
        var rows = delQueue.ExecuteNonQuery();

        tx.Commit();

        var deleted = rows > 0;
        return Response.FromValue(deleted, deleted ? StubResponse.NoContent() : StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(cancellationToken); }

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM Queues WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _queueName);
        var exists = cmd.ExecuteScalar() != null;
        return Response.FromValue(exists, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    // ── Properties & Metadata ───────────────────────────────────────────

    /// <inheritdoc/>
    public override Response<QueueProperties> GetProperties(CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();

        // Check queue exists and get metadata
        using var qCmd = conn.CreateCommand();
        qCmd.CommandText = "SELECT Metadata FROM Queues WHERE Name = @name";
        qCmd.Parameters.AddWithValue("@name", _queueName);
        var metadataObj = qCmd.ExecuteScalar();
        if (metadataObj == null)
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        var metadata = new Dictionary<string, string>();
        if (metadataObj is string metaJson && !string.IsNullOrWhiteSpace(metaJson))
        {
            metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(metaJson) ?? new Dictionary<string, string>();
        }

        // Count messages
        using var cCmd = conn.CreateCommand();
        cCmd.CommandText = "SELECT COUNT(*) FROM Messages WHERE QueueName = @name";
        cCmd.Parameters.AddWithValue("@name", _queueName);
        var count = Convert.ToInt32(cCmd.ExecuteScalar());

        var props = QueuesModelFactory.QueueProperties(metadata, count);
        return Response.FromValue(props, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override async Task<Response<QueueProperties>> GetPropertiesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(cancellationToken); }

    /// <inheritdoc/>
    public override Response SetMetadata(IDictionary<string, string>? metadata, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();

        // Check exists
        using var chk = conn.CreateCommand();
        chk.CommandText = "SELECT 1 FROM Queues WHERE Name = @name";
        chk.Parameters.AddWithValue("@name", _queueName);
        if (chk.ExecuteScalar() == null)
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Queues SET Metadata = @metadata WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _queueName);
        cmd.Parameters.AddWithValue("@metadata", metadata != null && metadata.Count > 0 ? JsonSerializer.Serialize(metadata) : (object)DBNull.Value);
        cmd.ExecuteNonQuery();

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
        EnsureQueueExists();

        var now = DateTimeOffset.UtcNow;
        var messageId = Guid.NewGuid().ToString();
        var popReceipt = Guid.NewGuid().ToString();
        var insertedOn = now;
        var ttl = timeToLive ?? TimeSpan.FromDays(7);
        var expiresOn = ttl == TimeSpan.FromSeconds(-1) ? DateTimeOffset.MaxValue : now.Add(ttl);
        var nextVisibleOn = now.Add(visibilityTimeout ?? TimeSpan.Zero);

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO Messages (QueueName, MessageId, PopReceipt, MessageText, InsertedOn, ExpiresOn, NextVisibleOn, DequeueCount) VALUES (@queue, @mid, @pop, @text, @ins, @exp, @nvo, 0)";
        cmd.Parameters.AddWithValue("@queue", _queueName);
        cmd.Parameters.AddWithValue("@mid", messageId);
        cmd.Parameters.AddWithValue("@pop", popReceipt);
        cmd.Parameters.AddWithValue("@text", messageText);
        cmd.Parameters.AddWithValue("@ins", insertedOn.ToString("O"));
        cmd.Parameters.AddWithValue("@exp", expiresOn.ToString("O"));
        cmd.Parameters.AddWithValue("@nvo", nextVisibleOn.ToString("O"));
        cmd.ExecuteNonQuery();

        var receipt = QueuesModelFactory.SendReceipt(messageId, insertedOn, expiresOn, popReceipt, nextVisibleOn);
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
        EnsureQueueExists();

        var now = DateTimeOffset.UtcNow;
        var max = maxMessages ?? 1;
        var visTimeout = visibilityTimeout ?? TimeSpan.FromSeconds(30);
        var messages = new List<QueueMessage>();

        using var conn = _serviceClient.Db.Open();
        using var tx = conn.BeginTransaction();

        // First, delete expired messages
        using var delExpired = conn.CreateCommand();
        delExpired.Transaction = tx;
        delExpired.CommandText = "DELETE FROM Messages WHERE QueueName = @queue AND ExpiresOn <= @now";
        delExpired.Parameters.AddWithValue("@queue", _queueName);
        delExpired.Parameters.AddWithValue("@now", now.ToString("O"));
        delExpired.ExecuteNonQuery();

        // Select visible messages
        using var selectCmd = conn.CreateCommand();
        selectCmd.Transaction = tx;
        selectCmd.CommandText = "SELECT MessageId, PopReceipt, MessageText, InsertedOn, ExpiresOn, NextVisibleOn, DequeueCount FROM Messages WHERE QueueName = @queue AND NextVisibleOn <= @now ORDER BY InsertedOn LIMIT @max";
        selectCmd.Parameters.AddWithValue("@queue", _queueName);
        selectCmd.Parameters.AddWithValue("@now", now.ToString("O"));
        selectCmd.Parameters.AddWithValue("@max", max);

        var toUpdate = new List<(string MessageId, string MessageText, DateTimeOffset InsertedOn, DateTimeOffset ExpiresOn, long DequeueCount)>();
        using (var reader = selectCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                toUpdate.Add((
                    reader.GetString(0),
                    reader.GetString(2),
                    DateTimeOffset.Parse(reader.GetString(3)),
                    DateTimeOffset.Parse(reader.GetString(4)),
                    reader.GetInt64(6)
                ));
            }
        }

        // Update each message with new pop receipt, dequeue count, next visible on
        foreach (var msg in toUpdate)
        {
            var newPopReceipt = Guid.NewGuid().ToString();
            var newNextVisibleOn = now.Add(visTimeout);
            var newDequeueCount = msg.DequeueCount + 1;

            using var updateCmd = conn.CreateCommand();
            updateCmd.Transaction = tx;
            updateCmd.CommandText = "UPDATE Messages SET PopReceipt = @pop, NextVisibleOn = @nvo, DequeueCount = @dc WHERE QueueName = @queue AND MessageId = @mid";
            updateCmd.Parameters.AddWithValue("@pop", newPopReceipt);
            updateCmd.Parameters.AddWithValue("@nvo", newNextVisibleOn.ToString("O"));
            updateCmd.Parameters.AddWithValue("@dc", newDequeueCount);
            updateCmd.Parameters.AddWithValue("@queue", _queueName);
            updateCmd.Parameters.AddWithValue("@mid", msg.MessageId);
            updateCmd.ExecuteNonQuery();

            messages.Add(QueuesModelFactory.QueueMessage(
                msg.MessageId, newPopReceipt, BinaryData.FromString(msg.MessageText),
                newDequeueCount, msg.InsertedOn, msg.ExpiresOn, newNextVisibleOn));
        }

        tx.Commit();

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
        EnsureQueueExists();

        var now = DateTimeOffset.UtcNow;
        var max = maxMessages ?? 1;
        var messages = new List<PeekedMessage>();

        using var conn = _serviceClient.Db.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT MessageId, MessageText, DequeueCount, InsertedOn, ExpiresOn FROM Messages WHERE QueueName = @queue AND NextVisibleOn <= @now AND ExpiresOn > @now ORDER BY InsertedOn LIMIT @max";
        cmd.Parameters.AddWithValue("@queue", _queueName);
        cmd.Parameters.AddWithValue("@now", now.ToString("O"));
        cmd.Parameters.AddWithValue("@max", max);

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            messages.Add(QueuesModelFactory.PeekedMessage(
                reader.GetString(0),
                BinaryData.FromString(reader.GetString(1)),
                reader.GetInt64(2),
                DateTimeOffset.Parse(reader.GetString(3)),
                DateTimeOffset.Parse(reader.GetString(4))));
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
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM Messages WHERE QueueName = @queue AND MessageId = @mid AND PopReceipt = @pop";
        cmd.Parameters.AddWithValue("@queue", _queueName);
        cmd.Parameters.AddWithValue("@mid", messageId);
        cmd.Parameters.AddWithValue("@pop", popReceipt);
        var rows = cmd.ExecuteNonQuery();

        if (rows == 0)
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
        var now = DateTimeOffset.UtcNow;
        var newPopReceipt = Guid.NewGuid().ToString();
        var newNextVisibleOn = now.Add(visibilityTimeout);

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();

        if (messageText != null)
        {
            cmd.CommandText = "UPDATE Messages SET MessageText = @text, PopReceipt = @newPop, NextVisibleOn = @nvo WHERE QueueName = @queue AND MessageId = @mid AND PopReceipt = @pop";
            cmd.Parameters.AddWithValue("@text", messageText);
        }
        else
        {
            cmd.CommandText = "UPDATE Messages SET PopReceipt = @newPop, NextVisibleOn = @nvo WHERE QueueName = @queue AND MessageId = @mid AND PopReceipt = @pop";
        }

        cmd.Parameters.AddWithValue("@newPop", newPopReceipt);
        cmd.Parameters.AddWithValue("@nvo", newNextVisibleOn.ToString("O"));
        cmd.Parameters.AddWithValue("@queue", _queueName);
        cmd.Parameters.AddWithValue("@mid", messageId);
        cmd.Parameters.AddWithValue("@pop", popReceipt);

        var rows = cmd.ExecuteNonQuery();
        if (rows == 0)
            throw new RequestFailedException(404, "Message not found or pop receipt mismatch.", "MessageNotFound", null);

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
        EnsureQueueExists();

        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM Messages WHERE QueueName = @queue";
        cmd.Parameters.AddWithValue("@queue", _queueName);
        cmd.ExecuteNonQuery();

        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response> ClearMessagesAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return ClearMessages(cancellationToken); }

    // ── Helpers ─────────────────────────────────────────────────────────

    private void EnsureQueueExists()
    {
        using var conn = _serviceClient.Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM Queues WHERE Name = @name";
        cmd.Parameters.AddWithValue("@name", _queueName);
        if (cmd.ExecuteScalar() == null)
            throw new RequestFailedException(404, "Queue not found.", "QueueNotFound", null);
    }
}
