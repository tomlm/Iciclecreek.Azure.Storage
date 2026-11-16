namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>How a queue service client keeps its queues in the directories it is given.</summary>
internal enum QueueStorage
{
    /// <summary>Each queue is a Lucene index with a document per message (<see cref="LuceneQueueStore"/>). Not what the public constructors give.</summary>
    Index,

    /// <summary>Each queue is a directory of files, one per message (<see cref="DirectoryQueueStore"/>). What the public constructors give.</summary>
    Files,
}

/// <summary>
/// The queues of one queue service client and the messages in them. Every member is safe to call
/// from several threads, and from several clients and processes that share the directories.
/// </summary>
internal interface IQueueStore : IDisposable
{
    bool QueueExists(string queueName);

    /// <summary>Makes the queue; false when it was already there. Refuses a name that is empty or reserved.</summary>
    bool CreateQueue(string queueName, IDictionary<string, string>? metadata);

    /// <summary>Removes the queue and its messages; false when there was no such queue.</summary>
    bool DeleteQueue(string queueName);

    /// <summary>The queue's metadata, or null when there is no such queue.</summary>
    IDictionary<string, string>? GetMetadata(string queueName);

    /// <summary>Replaces the queue's metadata; false when there is no such queue.</summary>
    bool SetMetadata(string queueName, IDictionary<string, string>? metadata);

    /// <summary>Every queue and its metadata, by name.</summary>
    List<KeyValuePair<string, IDictionary<string, string>>> ListQueues();

    /// <summary>How many messages the queue holds, hidden ones included.</summary>
    int Count(string queueName);

    /// <summary>Adds a message at the end of the line.</summary>
    StoredMessage Enqueue(string queueName, string text, TimeSpan? visibilityTimeout, TimeSpan? timeToLive);

    /// <summary>Takes up to <paramref name="max"/> visible messages, oldest first, and hides them for the timeout.</summary>
    List<StoredMessage> Dequeue(string queueName, int max, TimeSpan visibilityTimeout);

    /// <summary>Up to <paramref name="max"/> visible messages, oldest first, without taking them.</summary>
    List<StoredMessage> Peek(string queueName, int max);

    /// <summary>Deletes the message if the pop receipt is its current one.</summary>
    bool Delete(string queueName, string messageId, string popReceipt);

    /// <summary>Changes the message's text and visibility if the pop receipt is its current one; it gets a new receipt.</summary>
    StoredMessage? Update(string queueName, string messageId, string popReceipt, string? text, TimeSpan visibilityTimeout);

    /// <summary>Removes every message.</summary>
    void Clear(string queueName);
}
