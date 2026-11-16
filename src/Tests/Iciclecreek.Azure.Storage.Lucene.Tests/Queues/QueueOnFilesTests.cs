using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using FilterDirectory = Lucene.Net.Store.FilterDirectory;
using LuceneDirectory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Queues;

/// <summary>
/// How a queue is kept in its Lucene <c>Directory</c>: a message is a file whose name is its state, so a
/// writer that dies part way leaves files behind, and what a caller hands in as a message id and
/// pop receipt is the name of a file.
/// </summary>
public class QueueOnFilesTests
{
    private string _folder = null!;
    private string _messages = null!;
    private LuceneQueueServiceClient _service = null!;
    private QueueClient _queue = null!;

    [SetUp]
    public void SetUp()
    {
        _folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneQueueFiles-" + Guid.NewGuid().ToString("N"));
        _messages = Path.Combine(_folder, "queues", "work");
        _service = new LuceneQueueServiceClient(_folder);
        _queue = _service.GetQueueClient("work");
        _queue.Create();
    }

    [TearDown]
    public void TearDown()
    {
        _service.Dispose();
        if (Directory.Exists(_folder))
            Directory.Delete(_folder, recursive: true);
    }

    private string[] MessageFiles() => Directory.GetFiles(_messages, "*.msg");

    [Test]
    public void A_Message_Is_One_File_And_A_Queue_Is_One_File_In_The_Registry()
    {
        _queue.SendMessage("a");
        _queue.SendMessage("b");
        Assert.That(MessageFiles(), Has.Length.EqualTo(2));

        // Receiving and updating a message replace its file; they do not add one.
        var taken = _queue.ReceiveMessages(1, TimeSpan.FromSeconds(60)).Value.Single();
        var updated = _queue.UpdateMessage(taken.MessageId, taken.PopReceipt, "a, changed", TimeSpan.Zero).Value;
        Assert.That(MessageFiles(), Has.Length.EqualTo(2));
        _queue.SetMetadata(new Dictionary<string, string> { ["owner"] = "tests" });
        Assert.That(Directory.GetFiles(Path.Combine(_folder, "queues", "__registry__"), "*.queue"), Has.Length.EqualTo(1));

        _queue.DeleteMessage(taken.MessageId, updated.PopReceipt);
        Assert.That(MessageFiles(), Has.Length.EqualTo(1));
        _queue.Delete();
        Assert.That(MessageFiles(), Is.Empty);
        Assert.That(Directory.GetFiles(Path.Combine(_folder, "queues", "__registry__"), "*.queue"), Is.Empty);
    }

    [Test]
    public void A_Message_Left_With_Two_Files_By_A_Writer_That_Died_Is_One_Message_In_Its_Newer_State()
    {
        _queue.SendMessage("once");
        var asSent = MessageFiles().Single();
        var aside = Path.Combine(_folder, "aside");
        File.Copy(asSent, aside);
        var taken = _queue.ReceiveMessages(1, TimeSpan.FromSeconds(60)).Value.Single();
        // As if the receiver had died after writing the message's new file and before removing the old one.
        File.Copy(aside, asSent);
        Assert.That(MessageFiles(), Has.Length.EqualTo(2));

        Assert.That(_queue.GetProperties().Value.ApproximateMessagesCount, Is.EqualTo(1));
        Assert.That(_queue.PeekMessages(10).Value, Is.Empty, "the message is as it was received: hidden");
        Assert.That(_queue.ReceiveMessages(10).Value, Is.Empty);
        Assert.That(MessageFiles(), Has.Length.EqualTo(1), "the next receive removes the older file");

        _queue.DeleteMessage(taken.MessageId, taken.PopReceipt);
        Assert.That(MessageFiles(), Is.Empty);
    }

    [Test]
    public void A_Message_Whose_New_File_Was_Never_Finished_Is_Still_The_Message_It_Was()
    {
        _queue.SendMessage("once");
        var asSent = MessageFiles().Single();
        // As if a receiver had died while writing the message's new file: the next version of the
        // message, cut short, with the file it was to replace still there.
        var unfinished = asSent[..^"00000000.msg".Length] + "00000001.msg";
        File.WriteAllBytes(unfinished, File.ReadAllBytes(asSent)[..^6]);

        Assert.That(_queue.GetProperties().Value.ApproximateMessagesCount, Is.EqualTo(1));
        Assert.That(_queue.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "once" }));

        var taken = _queue.ReceiveMessages(10, TimeSpan.FromSeconds(60)).Value.Single();
        Assert.That(taken.Body.ToString(), Is.EqualTo("once"));
        Assert.That(taken.DequeueCount, Is.EqualTo(1));
        Assert.That(MessageFiles(), Has.Length.EqualTo(1), "the receive removes the unfinished file");
        Assert.That(File.Exists(unfinished), Is.False);

        _queue.DeleteMessage(taken.MessageId, taken.PopReceipt);
        Assert.That(MessageFiles(), Is.Empty);
    }

    [Test]
    public void A_Receive_That_Finds_The_Queue_Changed_While_It_Waited_Looks_Again()
    {
        // Two clients on the same directories. The first one's listing of the queue is followed,
        // before it gets any further, by the second one receiving the message and putting it back.
        var directories = new ConcurrentDictionary<string, LuceneDirectory>();
        LuceneDirectory Shared(string path) => directories.GetOrAdd(path, _ => new RAMDirectory());
        Action afterListing = () => { };
        using var second = new LuceneQueueServiceClient(Shared);
        using var first = new LuceneQueueServiceClient(path => path == "queues/work" ? new ListedDirectory(Shared(path), () => afterListing()) : Shared(path));
        var queue = first.GetQueueClient("work");
        var others = second.GetQueueClient("work");
        queue.Create();
        queue.SendMessage("moved on");

        afterListing = () =>
        {
            afterListing = () => { };
            var theirs = others.ReceiveMessages(1, TimeSpan.FromSeconds(60)).Value.Single();
            others.UpdateMessage(theirs.MessageId, theirs.PopReceipt, (string?)null, TimeSpan.Zero);
        };
        var taken = queue.ReceiveMessages(1, TimeSpan.FromSeconds(60)).Value;

        // The file that was listed is gone; the message is in line again under another name.
        Assert.That(taken.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "moved on" }));
        Assert.That(taken.Single().DequeueCount, Is.EqualTo(2));
    }

    /// <summary>A directory that says when it has been listed.</summary>
    private sealed class ListedDirectory(LuceneDirectory inner, Action listed) : FilterDirectory(inner)
    {
        public override string[] ListAll()
        {
            var files = base.ListAll();
            listed();
            return files;
        }
    }

    [Test]
    public void A_File_That_Was_Never_Finished_Is_Not_A_Message_And_Is_Removed_By_The_Next_Receive()
    {
        _queue.SendMessage("whole");
        var whole = MessageFiles().Single();
        var name = Path.GetFileName(whole);
        // Two files that a sender died writing, both ahead of the whole message in line: one cut short, one empty.
        File.WriteAllBytes(Path.Combine(_messages, "0000000000000001" + name[16..]), File.ReadAllBytes(whole)[..^6]);
        File.WriteAllBytes(Path.Combine(_messages, "0000000000000002" + name[16..]), []);
        // And a file that is nothing to do with the queue.
        File.WriteAllText(Path.Combine(_messages, "notes.txt"), "not a message");

        Assert.That(_queue.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "whole" }));
        Assert.That(_queue.ReceiveMessages(10, TimeSpan.FromSeconds(60)).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "whole" }));

        Assert.That(MessageFiles(), Has.Length.EqualTo(1));
        Assert.That(_queue.GetProperties().Value.ApproximateMessagesCount, Is.EqualTo(1));
        Assert.That(File.Exists(Path.Combine(_messages, "notes.txt")), Is.True);
    }

    [Test]
    public void A_Message_Id_Or_Pop_Receipt_That_Is_Not_One_Names_No_File()
    {
        var sent = _queue.SendMessage("here").Value;
        File.WriteAllText(Path.Combine(_folder, "outside.msg"), "not the queue's");
        var fileName = Path.GetFileName(MessageFiles().Single());

        (string Id, string Receipt)[] attempts =
        [
            ("../../outside", "msg"),
            (@"..\..\outside.msg", ""),
            (fileName, ""),
            (sent.MessageId, ""),
            (sent.MessageId, sent.PopReceipt + ".msg"),
            (sent.MessageId, sent.PopReceipt.ToUpperInvariant()),
            (sent.MessageId.ToUpperInvariant(), sent.PopReceipt),
            (sent.MessageId, sent.PopReceipt[..^1] + "1"),
        ];
        foreach (var (id, receipt) in attempts)
        {
            Assert.That(Assert.Throws<RequestFailedException>(() => _queue.DeleteMessage(id, receipt))!.ErrorCode, Is.EqualTo("MessageNotFound"), $"delete '{id}' '{receipt}'");
            Assert.That(Assert.Throws<RequestFailedException>(() => _queue.UpdateMessage(id, receipt, "x", TimeSpan.Zero))!.ErrorCode, Is.EqualTo("MessageNotFound"), $"update '{id}' '{receipt}'");
        }

        Assert.That(File.Exists(Path.Combine(_folder, "outside.msg")), Is.True);
        Assert.That(_queue.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "here" }));
        _queue.DeleteMessage(sent.MessageId, sent.PopReceipt);
        Assert.That(_queue.GetProperties().Value.ApproximateMessagesCount, Is.Zero);
    }
}

/// <summary>
/// Two clients on one folder, sending, receiving, deleting and peeking all at once: every message
/// sent is received once, and nobody is refused.
/// </summary>
public class QueueUnderLoadTests
{
    [Test]
    public void Every_Message_Sent_Is_Received_Once_While_Others_Send_Receive_And_Peek()
    {
        const int Senders = 4, PerSender = 40, Total = Senders * PerSender;
        var folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneLoad-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var first = new LuceneQueueServiceClient(folder);
            using var second = new LuceneQueueServiceClient(folder);
            QueueClient[] queues = [first.GetQueueClient("busy"), second.GetQueueClient("busy")];
            queues[0].Create();

            var received = new ConcurrentBag<string>();
            using var stop = new CancellationTokenSource();
            var senders = Enumerable.Range(0, Senders).Select(s => Task.Run(() =>
            {
                for (var n = 0; n < PerSender; n++)
                    queues[s % 2].SendMessage($"{s}:{n}");
            }));
            var receivers = Enumerable.Range(0, 3).Select(r => Task.Run(() =>
            {
                var queue = queues[r % 2];
                while (received.Count < Total && !stop.IsCancellationRequested)
                {
                    foreach (var message in queue.ReceiveMessages(4, TimeSpan.FromMinutes(10)).Value)
                    {
                        received.Add(message.Body.ToString());
                        queue.DeleteMessage(message.MessageId, message.PopReceipt);
                    }
                }
            }));
            var working = senders.Concat(receivers).ToArray();
            var peekers = Enumerable.Range(0, 2).Select(p => Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    queues[p % 2].PeekMessages(32);
                    _ = queues[p % 2].GetProperties().Value.ApproximateMessagesCount;
                }
            })).ToArray();

            try
            {
                Assert.That(Task.WaitAll(working, TimeSpan.FromMinutes(3)), Is.True, "everything was sent and received in time");
            }
            finally
            {
                stop.Cancel();
            }
            Task.WaitAll(peekers);
            Task.WaitAll(working);

            var sent = Enumerable.Range(0, Senders).SelectMany(s => Enumerable.Range(0, PerSender).Select(n => $"{s}:{n}"));
            Assert.That(received, Is.EquivalentTo(sent));
            Assert.That(queues[0].GetProperties().Value.ApproximateMessagesCount, Is.Zero);
        }
        finally
        {
            if (Directory.Exists(folder))
                Directory.Delete(folder, recursive: true);
        }
    }
}
