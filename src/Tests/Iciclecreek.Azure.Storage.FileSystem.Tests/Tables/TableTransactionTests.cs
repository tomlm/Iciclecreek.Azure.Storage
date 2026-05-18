using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Tables;

public class TableTransactionTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void SubmitTransaction_All_Succeed()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("tx-ok");
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
    public void SubmitTransaction_Rollback_On_Failure()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("tx-fail");
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
    public void SubmitTransaction_Different_PartitionKey_Throws()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("tx-pk");
        client.CreateIfNotExists();

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk1", "r1")),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk2", "r2")),
        };

        Assert.Throws<RequestFailedException>(() => client.SubmitTransaction(actions));
    }

    // ───────────────────── Async counterparts ─────────────────────

    [Test]
    public async Task SubmitTransaction_All_Succeed_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("tx-ok");
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

    [Test]
    public async Task SubmitTransaction_Rollback_On_Failure_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("tx-fail");
        await client.CreateIfNotExistsAsync();

        // Pre-add one entity to cause a conflict.
        await client.AddEntityAsync(new TableEntity("pk", "existing") { ["V"] = "original" });

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "new1") { ["V"] = "x" }),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk", "existing")), // will 409
        };

        Assert.ThrowsAsync<TableTransactionFailedException>(async () => await client.SubmitTransactionAsync(actions));

        // new1 should have been rolled back.
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.GetEntityAsync<TableEntity>("pk", "new1"));
        Assert.That(ex!.Status, Is.EqualTo(404));

        // existing should be unchanged.
        var existing = (await client.GetEntityAsync<TableEntity>("pk", "existing")).Value;
        Assert.That(existing["V"]?.ToString(), Is.EqualTo("original"));
    }

    [Test]
    public async Task SubmitTransaction_Different_PartitionKey_Throws_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("tx-pk");
        await client.CreateIfNotExistsAsync();

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk1", "r1")),
            new TableTransactionAction(TableTransactionActionType.Add, new TableEntity("pk2", "r2")),
        };

        Assert.ThrowsAsync<RequestFailedException>(async () => await client.SubmitTransactionAsync(actions));
    }
}
