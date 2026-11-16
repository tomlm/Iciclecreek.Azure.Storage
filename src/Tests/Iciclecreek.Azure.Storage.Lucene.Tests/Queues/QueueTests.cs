using Iciclecreek.Azure.Storage.Lucene.Queues;
using Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Queues;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Queues;

// The queue behaviour every provider shares, in memory, with the queues kept as indexes and as files.
[TestFixture(false)]
[TestFixture(true)]
public class QueueCrudTests(bool queuesAsFiles) : QueueCrudTestsBase
{
    protected override StorageTestFixture CreateFixture() => new LuceneStorageTestFixture(queuesAsFiles: queuesAsFiles);
}

[TestFixture(false)]
[TestFixture(true)]
public class QueueMessageTests(bool queuesAsFiles) : QueueMessageTestsBase
{
    protected override StorageTestFixture CreateFixture() => new LuceneStorageTestFixture(queuesAsFiles: queuesAsFiles);
}

[TestFixture(false)]
[TestFixture(true)]
public class QueueServiceClientTests(bool queuesAsFiles) : QueueServiceClientTestsBase
{
    protected override StorageTestFixture CreateFixture() => new LuceneStorageTestFixture(queuesAsFiles: queuesAsFiles);
}

/// <summary>The same message behaviour in a folder, and what only a folder can show.</summary>
[TestFixture(false)]
[TestFixture(true)]
public class QueueMessageOnDiskTests(bool queuesAsFiles) : QueueMessageTestsBase
{
    protected override StorageTestFixture CreateFixture() => new LuceneStorageTestFixture(onDisk: true, queuesAsFiles);

    private LuceneStorageTestFixture Fixture => (LuceneStorageTestFixture)_fixture;

    [Test]
    public void Messages_Survive_Closing_And_Reopening_In_Order_With_Their_State()
    {
        _client.SendMessage("first");
        _client.SendMessage("second");
        var taken = _client.ReceiveMessages(1, TimeSpan.FromSeconds(60)).Value.Single();
        Fixture.QueueService.Dispose();

        using var reopened = Fixture.OpenQueueService();
        var queue = reopened.GetQueueClient("msgq");

        Assert.That(queue.Exists().Value, Is.True);
        Assert.That(queue.GetProperties().Value.ApproximateMessagesCount, Is.EqualTo(2));
        // "first" is still hidden by the receive made before closing.
        Assert.That(queue.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "second" }));
        queue.DeleteMessage(taken.MessageId, taken.PopReceipt);

        // A message sent after reopening still comes after what was there.
        queue.SendMessage("third");
        Assert.That(queue.ReceiveMessages(10, TimeSpan.FromSeconds(60)).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "second", "third" }));
    }

    [Test]
    public void Two_Clients_On_One_Folder_Share_The_Queue_And_Its_Order()
    {
        using var otherService = Fixture.OpenQueueService();
        var other = otherService.GetQueueClient("msgq");

        _client.SendMessage("from the first");
        other.SendMessage("from the second");
        _client.SendMessage("from the first again");

        var received = other.ReceiveMessages(3, TimeSpan.FromSeconds(60)).Value;
        Assert.That(received.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "from the first", "from the second", "from the first again" }));
        Assert.That(_client.ReceiveMessages(3).Value, Is.Empty, "what one client received is hidden from the other");

        _client.DeleteMessage(received[0].MessageId, received[0].PopReceipt);
        Assert.That(other.GetProperties().Value.ApproximateMessagesCount, Is.EqualTo(2));
    }

    [Test]
    public void A_Second_Client_On_The_Folder_Gets_Its_Turn_While_The_First_Is_Busy()
    {
        using var otherService = Fixture.OpenQueueService();
        var other = otherService.GetQueueClient("msgq");

        // The first client never leaves the queue's index alone: four threads send without a pause.
        using var stop = new CancellationTokenSource();
        var sent = 0;
        var busy = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                _client.SendMessage("busy");
                Interlocked.Increment(ref sent);
            }
        })).ToArray();
        try
        {
            // Once the senders are under way there is always something to receive: they have four turns to the receiver's one.
            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref sent) >= 8, TimeSpan.FromSeconds(30)), Is.True);
            for (var n = 0; n < 12; n++)
            {
                var waited = System.Diagnostics.Stopwatch.StartNew();
                Assert.That(other.ReceiveMessages(1, TimeSpan.FromSeconds(60)).Value, Has.Length.EqualTo(1));
                // In line behind the four senders, not shut out until a ten-second limit.
                Assert.That(waited.Elapsed, Is.LessThan(TimeSpan.FromSeconds(8)));
            }
        }
        finally
        {
            stop.Cancel();
            Task.WaitAll(busy);
        }
    }

    [Test]
    public void Clearing_A_Queue_Keeps_Later_Messages_In_Order()
    {
        _client.SendMessage("gone");
        _client.ClearMessages();
        _client.SendMessage("a");
        _client.SendMessage("b");

        Assert.That(_client.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void Deleting_A_Queue_Removes_Its_Messages_Even_If_It_Is_Made_Again()
    {
        _client.SendMessage("old");
        _client.Delete();
        _client.Create();

        Assert.That(_client.GetProperties().Value.ApproximateMessagesCount, Is.Zero);
        Assert.That(_client.ReceiveMessages(10).Value, Is.Empty);
    }
}
