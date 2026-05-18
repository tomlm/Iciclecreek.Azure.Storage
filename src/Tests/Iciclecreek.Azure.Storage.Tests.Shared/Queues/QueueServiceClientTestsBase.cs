using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Queues;

[TestFixture]
public abstract class QueueServiceClientTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void Setup() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── FromAccount_Returns_Service_Client ──────────────────────────────

    [Test]
    public void FromAccount_Returns_Service_Client()
    {
        var svc = _fixture.CreateQueueServiceClient();
        Assert.That(svc, Is.Not.Null);
        Assert.That(svc.AccountName, Is.Not.Null);
    }

    // ── GetQueues_Lists_Created_Queues ──────────────────────────────────

    [Test]
    public void GetQueues_Lists_Created_Queues()
    {
        var svc = _fixture.CreateQueueServiceClient();
        _fixture.CreateQueueClient("queue-a").Create();
        _fixture.CreateQueueClient("queue-b").Create();

        var names = svc.GetQueues().Select(q => q.Name).ToList();
        Assert.That(names, Does.Contain("queue-a"));
        Assert.That(names, Does.Contain("queue-b"));
    }

    // ── GetQueues_With_Prefix_Filters ───────────────────────────────────

    [Test]
    public void GetQueues_With_Prefix_Filters()
    {
        var svc = _fixture.CreateQueueServiceClient();
        _fixture.CreateQueueClient("prefix-a").Create();
        _fixture.CreateQueueClient("prefix-b").Create();
        _fixture.CreateQueueClient("other-c").Create();

        var names = svc.GetQueues(prefix: "prefix-").Select(q => q.Name).ToList();
        Assert.That(names, Has.Count.EqualTo(2));
        Assert.That(names, Does.Contain("prefix-a"));
        Assert.That(names, Does.Contain("prefix-b"));
        Assert.That(names, Does.Not.Contain("other-c"));
    }

    // ── GetQueuesAsync_Lists_Created_Queues ─────────────────────────────

    [Test]
    public async Task GetQueuesAsync_Lists_Created_Queues()
    {
        var svc = _fixture.CreateQueueServiceClient();
        _fixture.CreateQueueClient("async-q1").Create();
        _fixture.CreateQueueClient("async-q2").Create();

        var names = new List<string>();
        await foreach (var q in svc.GetQueuesAsync())
            names.Add(q.Name);

        Assert.That(names, Does.Contain("async-q1"));
        Assert.That(names, Does.Contain("async-q2"));
    }

    // ── GetQueues_Returns_Empty_When_None ────────────────────────────────

    [Test]
    public void GetQueues_Returns_Empty_When_None()
    {
        var svc = _fixture.CreateQueueServiceClient();
        var queues = svc.GetQueues().ToList();
        Assert.That(queues, Is.Empty);
    }

    // ── CreateQueue_Via_ServiceClient ────────────────────────────────────

    [Test]
    public void CreateQueue_Via_ServiceClient()
    {
        var svc = _fixture.CreateQueueServiceClient();
        var result = svc.CreateQueue("svc-created");
        Assert.That(result.Value.Name, Is.EqualTo("svc-created"));
    }

    // ── DeleteQueue_Via_ServiceClient ────────────────────────────────────

    [Test]
    public void DeleteQueue_Via_ServiceClient()
    {
        var svc = _fixture.CreateQueueServiceClient();
        svc.CreateQueue("svc-delete");

        Assert.DoesNotThrow(() => svc.DeleteQueue("svc-delete"));

        var names = svc.GetQueues().Select(q => q.Name).ToList();
        Assert.That(names, Does.Not.Contain("svc-delete"));
    }

    // ── Uri_Contains_Account_Name ───────────────────────────────────────

    [Test]
    public void Uri_Contains_Account_Name()
    {
        var svc = _fixture.CreateQueueServiceClient();
        Assert.That(svc.Uri.ToString(), Does.Contain(svc.AccountName));
        Assert.That(svc.Uri.ToString(), Does.Contain("queue"));
    }
}
