using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Azure.Storage.Queues;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Iciclecreek.Azure.Storage.Lucene.Internal;
using Iciclecreek.Azure.Storage.Lucene.Queues;

// The two ways the Lucene provider can keep a queue, measured against each other:
//
//   dotnet run -c Release --project src/BenchmarkSuite1 -- --filter *LuceneQueue*
//
// The on-disk classes take about an hour, nearly all of it sending a backlog of 10,000 messages to
// an index one message at a time. To measure one of the two alone, name it in QUEUE_KEPT_AS:
//
//   $env:QUEUE_KEPT_AS = "Files"

/// <summary>How the Lucene provider keeps a queue.</summary>
public enum QueueKeptAs
{
    /// <summary>An index with a document per message. Not what the public constructors give.</summary>
    Index,
    /// <summary>A directory with a file per message. What the public constructors give.</summary>
    Files,
}

public static class QueuesKeptAs
{
    /// <summary>Both ways, or the one named in the environment variable <c>QUEUE_KEPT_AS</c>.</summary>
    public static IEnumerable<QueueKeptAs> Measured =>
        Enum.TryParse<QueueKeptAs>(Environment.GetEnvironmentVariable("QUEUE_KEPT_AS"), ignoreCase: true, out var only)
            ? [only]
            : [QueueKeptAs.Index, QueueKeptAs.Files];
}

/// <summary>A queue of the Lucene provider, in memory or in a folder that is emptied before use and removed after.</summary>
public sealed class BenchmarkQueue : IDisposable
{
    private const string Root = @"c:\temp\benchmarks\queues";

    // About what a queued work item is: a few identifiers as JSON.
    public static readonly string Text = new('m', 256);

    private readonly string? _folder;
    private readonly LuceneQueueServiceClient _service;

    public QueueClient Queue { get; }

    public BenchmarkQueue(QueueKeptAs keptAs, bool onDisk, string name)
    {
        var storage = keptAs == QueueKeptAs.Files ? QueueStorage.Files : QueueStorage.Index;
        if (onDisk)
        {
            _folder = Path.Combine(Root, $"{keptAs}-{name}");
            if (Directory.Exists(_folder))
                Directory.Delete(_folder, true);
            Directory.CreateDirectory(_folder);
            _service = new LuceneQueueServiceClient(LuceneQueueServiceClient.InFolder(_folder), storage);
        }
        else
        {
            _service = new LuceneQueueServiceClient(LuceneQueueServiceClient.InMemory(), storage);
        }
        Queue = _service.GetQueueClient("work");
        Queue.CreateIfNotExists();
    }

    public void Send(int count)
    {
        for (var i = 0; i < count; i++)
            Queue.SendMessage(Text);
    }

    /// <summary>Receives and deletes <paramref name="count"/> messages, <paramref name="batch"/> at a time.</summary>
    public void ReceiveAndDelete(int count, int batch)
    {
        var left = count;
        while (left > 0)
        {
            var messages = Queue.ReceiveMessages(Math.Min(batch, left)).Value;
            if (messages.Length == 0)
                throw new InvalidOperationException($"The queue ran dry with {left} of {count} messages still to receive.");
            foreach (var message in messages)
                Queue.DeleteMessage(message.MessageId, message.PopReceipt);
            left -= messages.Length;
        }
    }

    public void Dispose()
    {
        _service.Dispose();
        try
        {
            if (_folder != null)
                Directory.Delete(_folder, true);
        }
        catch (IOException)
        {
            // Left for the next run, which empties the folder before using it.
        }
    }
}

/// <summary>
/// What a worker queue does, message by message: send, receive, delete. Each is measured with a
/// backlog of <see cref="Depth"/> messages already waiting, and the backlog is the same size at
/// the start of every iteration: what one benchmark adds or takes, its setup or cleanup puts back.
/// Times and allocations are per message.
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 2, iterationCount: 8)]
[MemoryDiagnoser]
public abstract class LuceneQueueBenchmarks
{
    private const int Messages = 64;
    private BenchmarkQueue _queue = null!;

    protected abstract bool OnDisk { get; }

    [ParamsSource(typeof(QueuesKeptAs), nameof(QueuesKeptAs.Measured))]
    public QueueKeptAs KeptAs { get; set; }

    [Params(0, 1000, 10000)]
    public int Depth { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _queue = new BenchmarkQueue(KeptAs, OnDisk, $"depth{Depth}");
        _queue.Send(Depth);
    }

    [GlobalCleanup]
    public void Cleanup() => _queue.Dispose();

    [Benchmark(OperationsPerInvoke = Messages)]
    public void Send() => _queue.Send(Messages);

    [IterationCleanup(Target = nameof(Send))]
    public void TakeBackWhatWasSent() => _queue.ReceiveAndDelete(Messages, 32);

    [IterationSetup(Targets = [nameof(ReceiveOneAndDelete), nameof(ReceiveBatchAndDelete)])]
    public void SendWhatWillBeTaken() => _queue.Send(Messages);

    /// <summary>A receive and a delete for every message.</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public void ReceiveOneAndDelete() => _queue.ReceiveAndDelete(Messages, 1);

    /// <summary>A receive for every 32 messages, and a delete for every message.</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public void ReceiveBatchAndDelete() => _queue.ReceiveAndDelete(Messages, 32);
}

public class LuceneQueueOnDiskBenchmarks : LuceneQueueBenchmarks
{
    protected override bool OnDisk => true;
}

public class LuceneQueueInMemoryBenchmarks : LuceneQueueBenchmarks
{
    protected override bool OnDisk => false;
}

/// <summary>
/// Eight threads at one queue at once: how long a message takes when callers have to take turns.
/// Times and allocations are per message.
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 2, iterationCount: 8)]
[MemoryDiagnoser]
public abstract class LuceneQueueContentionBenchmarks
{
    private const int Threads = 8;
    private const int PerThread = 16;
    private const int Messages = Threads * PerThread;
    private BenchmarkQueue _queue = null!;

    protected abstract bool OnDisk { get; }

    [ParamsSource(typeof(QueuesKeptAs), nameof(QueuesKeptAs.Measured))]
    public QueueKeptAs KeptAs { get; set; }

    [GlobalSetup]
    public void Setup() => _queue = new BenchmarkQueue(KeptAs, OnDisk, "contended");

    [GlobalCleanup]
    public void Cleanup() => _queue.Dispose();

    private static void OnThreads(int count, Action<int> work)
    {
        var threads = Enumerable.Range(0, count).Select(n => new Thread(() => work(n))).ToList();
        threads.ForEach(thread => thread.Start());
        threads.ForEach(thread => thread.Join());
    }

    /// <summary>Eight senders.</summary>
    [Benchmark(OperationsPerInvoke = Messages)]
    public void Send() => OnThreads(Threads, _ => _queue.Send(PerThread));

    [IterationCleanup(Target = nameof(Send))]
    public void TakeBackWhatWasSent() => _queue.ReceiveAndDelete(Messages, 32);

    /// <summary>Four senders and four workers, each worker receiving and deleting as many messages as a sender sends.</summary>
    [Benchmark(OperationsPerInvoke = Messages / 2)]
    public void SendWhileOthersReceiveAndDelete() => OnThreads(Threads, n =>
    {
        if (n % 2 == 0)
        {
            _queue.Send(PerThread);
            return;
        }
        var left = PerThread;
        while (left > 0)
        {
            foreach (var message in _queue.Queue.ReceiveMessages(Math.Min(4, left)).Value)
            {
                _queue.Queue.DeleteMessage(message.MessageId, message.PopReceipt);
                left--;
            }
        }
    });
}

public class LuceneQueueContentionOnDiskBenchmarks : LuceneQueueContentionBenchmarks
{
    protected override bool OnDisk => true;
}

public class LuceneQueueContentionInMemoryBenchmarks : LuceneQueueContentionBenchmarks
{
    protected override bool OnDisk => false;
}
