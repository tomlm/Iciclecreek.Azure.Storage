using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Tables;

[TestFixture]
public abstract class TableTransactionTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void Setup() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── SubmitTransaction_All_Succeed ───────────────────────────────────

    [Test]
    public void SubmitTransaction_All_Succeed()
    {
        var client = _fixture.CreateTableClient("tx-ok");
        client.CreateIfNotExists();

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "r1") { ["V"] = "1" }),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "r2") { ["V"] = "2" }),
        };

        var result = client.SubmitTransaction(actions);
        Assert.That(result.Value, Has.Count.EqualTo(2));

        Assert.That(client.GetEntity<TableEntity>("pk", "r1").Value["V"]?.ToString(), Is.EqualTo("1"));
        Assert.That(client.GetEntity<TableEntity>("pk", "r2").Value["V"]?.ToString(), Is.EqualTo("2"));
    }

    [Test]
    public async Task SubmitTransaction_All_Succeed_Async()
    {
        var client = _fixture.CreateTableClient("tx-ok");
        await client.CreateIfNotExistsAsync();

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "r1") { ["V"] = "1" }),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "r2") { ["V"] = "2" }),
        };

        var result = await client.SubmitTransactionAsync(actions);
        Assert.That(result.Value, Has.Count.EqualTo(2));

        Assert.That((await client.GetEntityAsync<TableEntity>("pk", "r1")).Value["V"]?.ToString(), Is.EqualTo("1"));
        Assert.That((await client.GetEntityAsync<TableEntity>("pk", "r2")).Value["V"]?.ToString(), Is.EqualTo("2"));
    }

    // ── SubmitTransaction_Rollback_On_Failure ──────────────────────────

    [Test]
    public void SubmitTransaction_Rollback_On_Failure()
    {
        var client = _fixture.CreateTableClient("tx-fail");
        client.CreateIfNotExists();

        // Pre-add one entity to cause a conflict.
        client.AddEntity(new TableEntity("pk", "existing") { ["V"] = "original" });

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "new1") { ["V"] = "x" }),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "existing")), // will 409
        };

        Assert.Throws<TableTransactionFailedException>(() => client.SubmitTransaction(actions));

        // new1 should have been rolled back.
        var ex = Assert.Throws<RequestFailedException>(() =>
            client.GetEntity<TableEntity>("pk", "new1"));
        Assert.That(ex!.Status, Is.EqualTo(404));

        // existing should be unchanged.
        var existing = client.GetEntity<TableEntity>("pk", "existing").Value;
        Assert.That(existing["V"]?.ToString(), Is.EqualTo("original"));
    }

    [Test]
    public async Task SubmitTransaction_Rollback_On_Failure_Async()
    {
        var client = _fixture.CreateTableClient("tx-fail");
        await client.CreateIfNotExistsAsync();

        // Pre-add one entity to cause a conflict.
        await client.AddEntityAsync(new TableEntity("pk", "existing") { ["V"] = "original" });

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "new1") { ["V"] = "x" }),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "existing")), // will 409
        };

        await Assert.ThrowsAsync<TableTransactionFailedException>(async () => await client.SubmitTransactionAsync(actions));

        // new1 should have been rolled back.
        var ex = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.GetEntityAsync<TableEntity>("pk", "new1"));
        Assert.That(ex!.Status, Is.EqualTo(404));

        // existing should be unchanged.
        var existing = (await client.GetEntityAsync<TableEntity>("pk", "existing")).Value;
        Assert.That(existing["V"]?.ToString(), Is.EqualTo("original"));
    }

    // ── SubmitTransaction_Different_PartitionKey_Throws ─────────────────

    [Test]
    public void SubmitTransaction_Different_PartitionKey_Throws()
    {
        var client = _fixture.CreateTableClient("tx-pk");
        client.CreateIfNotExists();

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk1", "r1")),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk2", "r2")),
        };

        Assert.Throws<RequestFailedException>(() => client.SubmitTransaction(actions));
    }

    [Test]
    public async Task SubmitTransaction_Different_PartitionKey_Throws_Async()
    {
        var client = _fixture.CreateTableClient("tx-pk");
        await client.CreateIfNotExistsAsync();

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk1", "r1")),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk2", "r2")),
        };

        await Assert.ThrowsAsync<RequestFailedException>(async () => await client.SubmitTransactionAsync(actions));
    }
}
