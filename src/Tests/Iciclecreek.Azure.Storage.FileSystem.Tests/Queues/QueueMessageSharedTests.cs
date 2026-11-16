using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Queues;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Queues;

/// <summary>
/// The queue behaviour every provider shares, run against the file system: order, expiry,
/// pop receipts and concurrent receivers. <see cref="QueueMessageTests"/> holds the older
/// file-system-only copy of the basic tests.
/// </summary>
public class QueueMessageSharedTests : QueueMessageTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    private string MessagesFolder => Path.Combine(((FileStorageTestFixture)_fixture).QueuesPath, "msgq", "messages");

    [Test]
    public void Two_Clients_On_One_Folder_Share_The_Queue_And_Its_Order()
    {
        var other = new FileQueueServiceClient(_fixture.TempPath).GetQueueClient("msgq");

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
    public void A_Damaged_Message_File_Is_Passed_Over()
    {
        _client.SendMessage("before");
        // A write cut short by a crash: not JSON.
        File.WriteAllText(Path.Combine(MessagesFolder, "0000000000000000001-damaged.json"), "{ \"MessageId\": \"0000");
        _client.SendMessage("after");

        Assert.That(_client.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "before", "after" }));
        Assert.That(_client.ReceiveMessages(10, TimeSpan.FromSeconds(60)).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "before", "after" }));
    }

    [Test]
    public void A_Message_Written_By_An_Earlier_Version_Is_Still_Delivered_And_Deleted()
    {
        // Earlier versions named a message file by a bare GUID.
        var id = Guid.NewGuid().ToString();
        var now = DateTimeOffset.UtcNow;
        var json = System.Text.Json.JsonSerializer.Serialize(new
        {
            MessageId = id,
            PopReceipt = Guid.NewGuid().ToString(),
            MessageText = "from before",
            InsertedOn = now,
            ExpiresOn = now.AddDays(7),
            NextVisibleOn = now,
            DequeueCount = 0,
        });
        File.WriteAllText(Path.Combine(MessagesFolder, id + ".json"), json);

        var received = _client.ReceiveMessages(1, TimeSpan.FromSeconds(60)).Value.Single();
        Assert.That((received.MessageId, received.Body.ToString()), Is.EqualTo((id, "from before")));

        _client.DeleteMessage(received.MessageId, received.PopReceipt);
        Assert.That(Directory.EnumerateFiles(MessagesFolder), Is.Empty);
    }

    [Test]
    public void Nothing_But_Whole_Messages_Is_Left_In_The_Folder()
    {
        for (var n = 0; n < 20; n++)
            _client.SendMessage($"m{n}");

        Assert.That(Directory.EnumerateFiles(MessagesFolder).Select(Path.GetExtension), Has.All.EqualTo(".json"));
        Assert.That(Directory.EnumerateFiles(MessagesFolder).Count(), Is.EqualTo(20));
    }
}
