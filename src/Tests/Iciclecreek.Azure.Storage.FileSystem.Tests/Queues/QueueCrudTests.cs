using Azure;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Queues;

public class QueueCrudTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    // ── Queue Create / Exists / Delete ──────────────────────────────────

    [Test]
    public void Create_Creates_Directory_On_Disk()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("my-queue");
        client.Create();

        var dir = Path.Combine(_root.QueuesPath, "my-queue");
        Assert.That(Directory.Exists(dir), Is.True);
    }

    [Test]
    public async Task CreateAsync_Creates_Directory_On_Disk()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("my-queue-async");
        await client.CreateAsync();

        var dir = Path.Combine(_root.QueuesPath, "my-queue-async");
        Assert.That(Directory.Exists(dir), Is.True);
    }

    [Test]
    public void CreateIfNotExists_Is_Idempotent()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("idempotent-q");
        client.CreateIfNotExists();
        Assert.DoesNotThrow(() => client.CreateIfNotExists());
    }

    [Test]
    public void Exists_Returns_False_For_Missing_Queue()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("nope");
        Assert.That(client.Exists().Value, Is.False);
    }

    [Test]
    public void Exists_Returns_True_After_Create()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("exists-q");
        client.Create();
        Assert.That(client.Exists().Value, Is.True);
    }

    [Test]
    public void Delete_Removes_Directory()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("del-q");
        client.Create();
        client.Delete();

        var dir = Path.Combine(_root.QueuesPath, "del-q");
        Assert.That(Directory.Exists(dir), Is.False);
    }

    [Test]
    public void Delete_Throws_404_When_Missing()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("missing-q");
        var ex = Assert.Throws<RequestFailedException>(() => client.Delete());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public void DeleteIfExists_Returns_False_When_Missing()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("missing-q");
        Assert.That(client.DeleteIfExists().Value, Is.False);
    }

    [Test]
    public void DeleteIfExists_Returns_True_And_Deletes()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("del-q2");
        client.Create();
        Assert.That(client.DeleteIfExists().Value, Is.True);
        Assert.That(client.Exists().Value, Is.False);
    }

    // ── Properties ──────────────────────────────────────────────────────

    [Test]
    public void Name_Returns_Queue_Name()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("named-q");
        Assert.That(client.Name, Is.EqualTo("named-q"));
    }

    [Test]
    public void AccountName_Returns_Account_Name()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("named-q");
        Assert.That(client.AccountName, Is.Empty);
    }

    // ── Metadata ────────────────────────────────────────────────────────

    [Test]
    public void SetMetadata_And_GetProperties_Roundtrip()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("meta-q");
        client.Create();

        client.SetMetadata(new Dictionary<string, string> { ["env"] = "test", ["region"] = "us" });

        var props = client.GetProperties().Value;
        Assert.That(props.Metadata["env"], Is.EqualTo("test"));
        Assert.That(props.Metadata["region"], Is.EqualTo("us"));
    }

    [Test]
    public void GetProperties_Returns_ApproximateMessageCount()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("count-q");
        client.Create();

        client.SendMessage("msg1");
        client.SendMessage("msg2");

        var props = client.GetProperties().Value;
        Assert.That(props.ApproximateMessagesCount, Is.EqualTo(2));
    }

    [Test]
    public void GetProperties_Throws_404_When_Missing()
    {
        var client = (FileQueueClient)_root.QueueService.GetQueueClient("missing-q");
        var ex = Assert.Throws<RequestFailedException>(() => client.GetProperties());
        Assert.That(ex!.Status, Is.EqualTo(404));
    }
}
