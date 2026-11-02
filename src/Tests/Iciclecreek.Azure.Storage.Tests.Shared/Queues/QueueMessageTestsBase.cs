using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Queues;

[TestFixture]
public abstract class QueueMessageTestsBase
{
    protected StorageTestFixture _fixture = null!;
    protected QueueClient _client = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void Setup()
    {
        _fixture = CreateFixture();
        _client = _fixture.CreateQueueClient("msgq");
        _client.Create();
    }

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── SendMessage_Returns_SendReceipt ─────────────────────────────────

    [Test]
    public void SendMessage_Returns_SendReceipt()
    {
        var receipt = _client.SendMessage("hello").Value;
        Assert.That(receipt.MessageId, Is.Not.Null.And.Not.Empty);
        Assert.That(receipt.PopReceipt, Is.Not.Null.And.Not.Empty);
        Assert.That(receipt.InsertionTime, Is.GreaterThan(DateTimeOffset.MinValue));
        Assert.That(receipt.ExpirationTime, Is.GreaterThan(receipt.InsertionTime));
    }

    // ── SendMessageAsync_Returns_SendReceipt ────────────────────────────

    [Test]
    public async Task SendMessageAsync_Returns_SendReceipt()
    {
        var receipt = (await _client.SendMessageAsync("hello async")).Value;
        Assert.That(receipt.MessageId, Is.Not.Null.And.Not.Empty);
    }

    // ── SendMessage_With_BinaryData ─────────────────────────────────────

    [Test]
    public void SendMessage_With_BinaryData()
    {
        var receipt = _client.SendMessage(BinaryData.FromString("binary hello")).Value;
        Assert.That(receipt.MessageId, Is.Not.Null.And.Not.Empty);
    }

    // ── SendMessage_Throws_404_When_Queue_Missing ───────────────────────

    [Test]
    public void SendMessage_Throws_404_When_Queue_Missing()
    {
        var client = _fixture.CreateQueueClient("nope");
        var ex = Assert.Throws<RequestFailedException>(() => client.SendMessage("fail"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── ReceiveMessages_Returns_Sent_Message ────────────────────────────

    [Test]
    public void ReceiveMessages_Returns_Sent_Message()
    {
        _client.SendMessage("hello world");

        var messages = _client.ReceiveMessages(1).Value;
        Assert.That(messages.Length, Is.EqualTo(1));
        Assert.That(messages[0].Body.ToString(), Is.EqualTo("hello world"));
        Assert.That(messages[0].DequeueCount, Is.EqualTo(1));
        Assert.That(messages[0].PopReceipt, Is.Not.Null.And.Not.Empty);
    }

    // ── ReceiveMessagesAsync_Returns_Sent_Message ───────────────────────

    [Test]
    public async Task ReceiveMessagesAsync_Returns_Sent_Message()
    {
        await _client.SendMessageAsync("async msg");

        var messages = (await _client.ReceiveMessagesAsync(1)).Value;
        Assert.That(messages.Length, Is.EqualTo(1));
        Assert.That(messages[0].Body.ToString(), Is.EqualTo("async msg"));
    }

    // ── ReceiveMessages_With_Multiple ───────────────────────────────────

    [Test]
    public void ReceiveMessages_With_Multiple()
    {
        _client.SendMessage("msg1");
        _client.SendMessage("msg2");
        _client.SendMessage("msg3");

        var messages = _client.ReceiveMessages(3).Value;
        Assert.That(messages.Length, Is.EqualTo(3));
    }

    // ── ReceiveMessages_Returns_Empty_When_No_Messages ──────────────────

    [Test]
    public void ReceiveMessages_Returns_Empty_When_No_Messages()
    {
        var messages = _client.ReceiveMessages(1).Value;
        Assert.That(messages.Length, Is.EqualTo(0));
    }

    // ── ReceiveMessages_Makes_Message_Invisible ─────────────────────────

    [Test]
    public void ReceiveMessages_Makes_Message_Invisible()
    {
        _client.SendMessage("invisible");

        // Dequeue with 30s visibility
        var first = _client.ReceiveMessages(1, TimeSpan.FromSeconds(30)).Value;
        Assert.That(first.Length, Is.EqualTo(1));

        // Second dequeue should find nothing (message is invisible)
        var second = _client.ReceiveMessages(1).Value;
        Assert.That(second.Length, Is.EqualTo(0));
    }

    // ── ReceiveMessages_Increments_DequeueCount ─────────────────────────

    [Test]
    public void ReceiveMessages_Increments_DequeueCount()
    {
        _client.SendMessage("count me");

        // Dequeue with 0s visibility so it becomes visible again immediately
        var first = _client.ReceiveMessages(1, TimeSpan.FromSeconds(0)).Value;
        Assert.That(first[0].DequeueCount, Is.EqualTo(1));

        // Small wait for visibility
        Thread.Sleep(50);

        var second = _client.ReceiveMessages(1, TimeSpan.FromSeconds(0)).Value;
        Assert.That(second[0].DequeueCount, Is.EqualTo(2));
    }

    // ── ReceiveMessage_Returns_Single_Message ───────────────────────────

    [Test]
    public void ReceiveMessage_Returns_Single_Message()
    {
        _client.SendMessage("single");

        var msg = _client.ReceiveMessage().Value;
        Assert.That(msg, Is.Not.Null);
        Assert.That(msg.Body.ToString(), Is.EqualTo("single"));
    }

    // ── PeekMessages_Returns_Message_Without_Removing ───────────────────

    [Test]
    public void PeekMessages_Returns_Message_Without_Removing()
    {
        _client.SendMessage("peekable");

        var peeked = _client.PeekMessages(1).Value;
        Assert.That(peeked.Length, Is.EqualTo(1));
        Assert.That(peeked[0].Body.ToString(), Is.EqualTo("peekable"));

        // Peek again — still there
        var peeked2 = _client.PeekMessages(1).Value;
        Assert.That(peeked2.Length, Is.EqualTo(1));
    }

    // ── PeekMessage_Returns_Single ──────────────────────────────────────

    [Test]
    public void PeekMessage_Returns_Single()
    {
        _client.SendMessage("peek single");

        var msg = _client.PeekMessage().Value;
        Assert.That(msg, Is.Not.Null);
        Assert.That(msg.Body.ToString(), Is.EqualTo("peek single"));
    }

    // ── PeekMessagesAsync_Returns_Messages ──────────────────────────────

    [Test]
    public async Task PeekMessagesAsync_Returns_Messages()
    {
        await _client.SendMessageAsync("async peek");

        var peeked = (await _client.PeekMessagesAsync(1)).Value;
        Assert.That(peeked.Length, Is.EqualTo(1));
    }

    // ── DeleteMessage_Removes_Message ───────────────────────────────────

    [Test]
    public void DeleteMessage_Removes_Message()
    {
        _client.SendMessage("delete me");

        var msg = _client.ReceiveMessage().Value;
        _client.DeleteMessage(msg.MessageId, msg.PopReceipt);

        // Should be gone even after visibility expires
        Thread.Sleep(50);
        var peeked = _client.PeekMessages(1).Value;
        Assert.That(peeked.Length, Is.EqualTo(0));
    }

    // ── DeleteMessage_Throws_404_With_Wrong_PopReceipt ──────────────────

    [Test]
    public void DeleteMessage_Throws_404_With_Wrong_PopReceipt()
    {
        _client.SendMessage("wrong pop");

        var msg = _client.ReceiveMessage().Value;
        var ex = Assert.Throws<RequestFailedException>(() =>
            _client.DeleteMessage(msg.MessageId, "wrong-receipt"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── DeleteMessageAsync_Works ────────────────────────────────────────

    [Test]
    public async Task DeleteMessageAsync_Works()
    {
        await _client.SendMessageAsync("async delete");

        var msg = (await _client.ReceiveMessageAsync()).Value;
        await Assert.DoesNotThrowAsync(() =>
            _client.DeleteMessageAsync(msg.MessageId, msg.PopReceipt));
    }

    // ── UpdateMessage_Changes_Content ───────────────────────────────────

    [Test]
    public void UpdateMessage_Changes_Content()
    {
        _client.SendMessage("original");

        var msg = _client.ReceiveMessage(TimeSpan.FromSeconds(30)).Value;
        var receipt = _client.UpdateMessage(msg.MessageId, msg.PopReceipt, "updated", TimeSpan.FromSeconds(0)).Value;

        Assert.That(receipt.PopReceipt, Is.Not.Null.And.Not.Empty);

        // Small wait for visibility
        Thread.Sleep(50);

        var peeked = _client.PeekMessage().Value;
        Assert.That(peeked.Body.ToString(), Is.EqualTo("updated"));
    }

    // ── UpdateMessage_Throws_404_With_Wrong_PopReceipt ──────────────────

    [Test]
    public void UpdateMessage_Throws_404_With_Wrong_PopReceipt()
    {
        _client.SendMessage("nope");

        var msg = _client.ReceiveMessage().Value;
        var ex = Assert.Throws<RequestFailedException>(() =>
            _client.UpdateMessage(msg.MessageId, "wrong", "fail", TimeSpan.Zero));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ── ClearMessages_Removes_All ───────────────────────────────────────

    [Test]
    public void ClearMessages_Removes_All()
    {
        _client.SendMessage("msg1");
        _client.SendMessage("msg2");
        _client.SendMessage("msg3");

        _client.ClearMessages();

        var peeked = _client.PeekMessages(10).Value;
        Assert.That(peeked.Length, Is.EqualTo(0));
    }

    // ── ClearMessagesAsync_Removes_All ──────────────────────────────────

    [Test]
    public async Task ClearMessagesAsync_Removes_All()
    {
        await _client.SendMessageAsync("a");
        await _client.SendMessageAsync("b");

        await _client.ClearMessagesAsync();

        var peeked = (await _client.PeekMessagesAsync(10)).Value;
        Assert.That(peeked.Length, Is.EqualTo(0));
    }

    // ── SendMessage_With_VisibilityTimeout_Makes_Invisible ──────────────

    [Test]
    public void SendMessage_With_VisibilityTimeout_Makes_Invisible()
    {
        _client.SendMessage("delayed", TimeSpan.FromSeconds(60));

        // Should not be visible for peek
        var peeked = _client.PeekMessages(1).Value;
        Assert.That(peeked.Length, Is.EqualTo(0));
    }

    // ── SendMessage_Default_TTL_Is_7_Days ───────────────────────────────

    [Test]
    public void SendMessage_Default_TTL_Is_7_Days()
    {
        var receipt = _client.SendMessage("ttl test").Value;
        var diff = receipt.ExpirationTime - receipt.InsertionTime;
        Assert.That(diff.TotalDays, Is.EqualTo(7).Within(0.01));
    }
}
