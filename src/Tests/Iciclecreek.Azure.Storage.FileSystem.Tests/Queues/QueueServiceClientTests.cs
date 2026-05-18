using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Queues;

public class QueueServiceClientTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void FromAccount_Returns_Service_Client()
    {
        var svc = _root.QueueService;
        Assert.That(svc, Is.Not.Null);
        Assert.That(svc.AccountName, Is.Empty);
    }

    [Test]
    public void GetQueueClient_Returns_FileQueueClient()
    {
        var svc = _root.QueueService;
        var client = svc.GetQueueClient("test-queue");
        Assert.That(client, Is.InstanceOf<FileQueueClient>());
        Assert.That(client.Name, Is.EqualTo("test-queue"));
    }

    [Test]
    public void GetQueues_Lists_Created_Queues()
    {
        var svc = _root.QueueService;
        svc.GetQueueClient("queue-a").Create();
        svc.GetQueueClient("queue-b").Create();

        var names = svc.GetQueues().Select(q => q.Name).ToList();
        Assert.That(names, Does.Contain("queue-a"));
        Assert.That(names, Does.Contain("queue-b"));
    }

    [Test]
    public void GetQueues_With_Prefix_Filters()
    {
        var svc = _root.QueueService;
        svc.GetQueueClient("prefix-a").Create();
        svc.GetQueueClient("prefix-b").Create();
        svc.GetQueueClient("other-c").Create();

        var names = svc.GetQueues(prefix: "prefix-").Select(q => q.Name).ToList();
        Assert.That(names, Has.Count.EqualTo(2));
        Assert.That(names, Does.Contain("prefix-a"));
        Assert.That(names, Does.Contain("prefix-b"));
        Assert.That(names, Does.Not.Contain("other-c"));
    }

    [Test]
    public async Task GetQueuesAsync_Lists_Created_Queues()
    {
        var svc = _root.QueueService;
        await svc.GetQueueClient("async-q1").CreateAsync();
        await svc.GetQueueClient("async-q2").CreateAsync();

        var names = new List<string>();
        await foreach (var q in svc.GetQueuesAsync())
            names.Add(q.Name);

        Assert.That(names, Does.Contain("async-q1"));
        Assert.That(names, Does.Contain("async-q2"));
    }

    [Test]
    public void GetQueues_Returns_Empty_When_None()
    {
        var svc = _root.QueueService;
        var queues = svc.GetQueues().ToList();
        Assert.That(queues, Is.Empty);
    }

    [Test]
    public void CreateQueue_Via_ServiceClient()
    {
        var svc = _root.QueueService;
        var result = svc.CreateQueue("svc-created");
        Assert.That(result.Value.Name, Is.EqualTo("svc-created"));
    }

    [Test]
    public void DeleteQueue_Via_ServiceClient()
    {
        var svc = _root.QueueService;
        svc.CreateQueue("svc-delete");

        Assert.DoesNotThrow(() => svc.DeleteQueue("svc-delete"));

        var names = svc.GetQueues().Select(q => q.Name).ToList();
        Assert.That(names, Does.Not.Contain("svc-delete"));
    }

    [Test]
    public void Uri_Contains_Account_Name()
    {
        var svc = _root.QueueService;
        Assert.That(svc.Uri.ToString(), Is.Not.Null);
        Assert.That(svc.Uri.ToString(), Does.Contain("queue"));
    }
}
