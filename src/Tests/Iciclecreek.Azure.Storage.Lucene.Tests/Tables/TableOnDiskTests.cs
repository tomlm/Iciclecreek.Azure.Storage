using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Tables;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Tables;

// The shared table behaviour again, against indexes in a folder.
public class TableCrudOnDiskTests : TableCrudTestsBase
{
    protected override StorageTestFixture CreateFixture() => new LuceneStorageTestFixture(onDisk: true);
}

public class TableTransactionOnDiskTests : TableTransactionTestsBase
{
    protected override StorageTestFixture CreateFixture() => new LuceneStorageTestFixture(onDisk: true);
}

/// <summary>What only a folder can show: the layout, and that tables outlive the client.</summary>
public class TableOnDiskTests
{
    private LuceneStorageTestFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new LuceneStorageTestFixture(onDisk: true);

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    [Test]
    public void Each_Table_And_Queue_Is_A_Directory_Of_Its_Own()
    {
        _fixture.TableService.GetTableClient("People").CreateIfNotExists();
        _fixture.TableService.GetTableClient("People").AddEntity(new TableEntity("p", "1"));
        _fixture.TableService.GetTableClient("Orders").CreateIfNotExists();
        _fixture.TableService.GetTableClient("Orders").AddEntity(new TableEntity("o", "1"));
        _fixture.QueueService.GetQueueClient("work").CreateIfNotExists();
        _fixture.QueueService.GetQueueClient("work").SendMessage("hello");

        var tables = Directory.GetDirectories(Path.Combine(_fixture.TempPath, "tables")).Select(Path.GetFileName);
        var queues = Directory.GetDirectories(Path.Combine(_fixture.TempPath, "queues")).Select(Path.GetFileName);

        Assert.That(tables, Is.EquivalentTo(new[] { "People", "Orders", "__registry__" }));
        Assert.That(queues, Is.EquivalentTo(new[] { "work", "__registry__" }));
    }

    [Test]
    public void Tables_And_Their_Entities_Survive_Closing_And_Reopening()
    {
        var table = _fixture.TableService.GetTableClient("People");
        table.Create();
        table.AddEntity(new TableEntity("family", "lili") { ["Age"] = 41, ["City"] = "Berlin" });
        var etag = table.GetEntity<TableEntity>("family", "lili").Value.ETag;
        _fixture.TableService.Dispose();

        using var reopened = new LuceneTableServiceClient(_fixture.TempPath);
        var again = reopened.GetTableClient("People");

        Assert.That(reopened.Query().Select(t => t.Name), Is.EqualTo(new[] { "People" }));
        var lili = again.GetEntity<TableEntity>("family", "lili").Value;
        Assert.That((lili.GetInt32("Age"), lili.GetString("City"), lili.ETag), Is.EqualTo((41, "Berlin", etag)));
        // The ETag from before closing still guards an update.
        lili["City"] = "Paris";
        again.UpdateEntity(lili, etag, TableUpdateMode.Replace);
        var stale = Assert.Throws<RequestFailedException>(() => again.UpdateEntity(lili, etag, TableUpdateMode.Replace));
        Assert.That(stale!.Status, Is.EqualTo(412));
    }

    [Test]
    public void Two_Clients_On_One_Folder_See_Each_Others_Changes()
    {
        using var other = new LuceneTableServiceClient(_fixture.TempPath);
        var mine = _fixture.TableService.GetTableClient("Shared");
        var theirs = other.GetTableClient("Shared");

        mine.Create();
        mine.AddEntity(new TableEntity("p", "1") { ["Value"] = "from mine" });

        Assert.That(theirs.GetEntity<TableEntity>("p", "1").Value.GetString("Value"), Is.EqualTo("from mine"));
        theirs.UpsertEntity(new TableEntity("p", "2") { ["Value"] = "from theirs" }, TableUpdateMode.Replace);
        Assert.That(mine.Query<TableEntity>().Select(e => e.RowKey), Is.EquivalentTo(new[] { "1", "2" }));
        Assert.That(Assert.Throws<RequestFailedException>(() => theirs.AddEntity(new TableEntity("p", "1")))!.Status, Is.EqualTo(409));
    }

    [Test]
    public void A_Deleted_Table_Made_Again_Is_Empty()
    {
        var table = _fixture.TableService.GetTableClient("Temp");
        table.Create();
        table.AddEntity(new TableEntity("p", "1"));

        table.Delete();
        Assert.That(Assert.Throws<RequestFailedException>(() => table.GetEntity<TableEntity>("p", "1"))!.Status, Is.EqualTo(404));
        table.Create();

        Assert.That(table.Query<TableEntity>(), Is.Empty);
    }
}
