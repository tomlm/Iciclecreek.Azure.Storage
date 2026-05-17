using System.Collections.Concurrent;

namespace Iciclecreek.Azure.Storage.Memory.Internal;

internal class ContainerStore
{
    public readonly ConcurrentDictionary<string, BlobEntry> Blobs = new();
    public IDictionary<string, string>? Metadata;
    public readonly LeaseState Lease = new();
    public DateTimeOffset CreatedOn = DateTimeOffset.UtcNow;
}

internal class BlobEntry
{
    public byte[] Content = Array.Empty<byte>();
    public string BlobType = "Block";
    public string ContentType = "application/octet-stream";
    public string? ContentEncoding;
    public string? ContentLanguage;
    public string? ContentDisposition;
    public string? CacheControl;
    public byte[]? ContentHash;
    public string ETag = NewETag();
    public DateTimeOffset CreatedOn = DateTimeOffset.UtcNow;
    public DateTimeOffset LastModified = DateTimeOffset.UtcNow;
    public IDictionary<string, string>? Metadata;
    public IDictionary<string, string>? Tags;
    public string? AccessTier;
    public string? VersionId;
    public long SequenceNumber;

    // Block staging
    public readonly ConcurrentDictionary<string, byte[]> StagedBlocks = new();
    public List<string>? CommittedBlockIds;

    // Lease
    public readonly LeaseState Lease = new();

    // Lock for compound ETag check-then-write
    public readonly object Lock = new();

    public static string NewETag() => $"0x{Guid.NewGuid():N}";

    public void Touch()
    {
        ETag = NewETag();
        LastModified = DateTimeOffset.UtcNow;
    }

    /// <summary>Clone content bytes for read operations (simulates network semantics).</summary>
    public byte[] CloneContent()
    {
        var clone = new byte[Content.Length];
        Buffer.BlockCopy(Content, 0, clone, 0, Content.Length);
        return clone;
    }

    /// <summary>Clone metadata dictionary for read operations.</summary>
    public IDictionary<string, string>? CloneMetadata()
        => Metadata is not null ? new Dictionary<string, string>(Metadata) : null;

    /// <summary>Clone tags dictionary for read operations.</summary>
    public IDictionary<string, string>? CloneTags()
        => Tags is not null ? new Dictionary<string, string>(Tags) : null;
}

internal class LeaseState
{
    public string? LeaseId;
    public TimeSpan? Duration;
    public DateTimeOffset? ExpiresAt;

    public bool IsActive => LeaseId is not null && (Duration?.TotalSeconds == -1 || ExpiresAt > DateTimeOffset.UtcNow);
}

internal class TableStore
{
    public readonly ConcurrentDictionary<string, EntityEntry> Entities = new();

    public static string EntityKey(string partitionKey, string rowKey) => $"{partitionKey}\0{rowKey}";
}

internal class EntityEntry
{
    public string PropertiesJson = "{}";
    public string ETag = BlobEntry.NewETag();
    public DateTimeOffset Timestamp = DateTimeOffset.UtcNow;
    public readonly object Lock = new();

    public void Touch()
    {
        ETag = BlobEntry.NewETag();
        Timestamp = DateTimeOffset.UtcNow;
    }
}

internal class QueueStore
{
    public readonly List<MessageEntry> Messages = new();
    public IDictionary<string, string>? Metadata;
    public readonly object Lock = new();
}

internal class MessageEntry
{
    public string MessageId = Guid.NewGuid().ToString();
    public string Body = string.Empty;
    public string PopReceipt = Guid.NewGuid().ToString();
    public DateTimeOffset InsertionTime = DateTimeOffset.UtcNow;
    public DateTimeOffset ExpirationTime = DateTimeOffset.UtcNow.AddDays(7);
    public DateTimeOffset VisibleAt = DateTimeOffset.UtcNow;
    public long DequeueCount;
    public bool Deleted;
}
