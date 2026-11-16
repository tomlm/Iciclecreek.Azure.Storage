using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Tables;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using LockObtainFailedException = Lucene.Net.Store.LockObtainFailedException;

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

        // A table's folder is its name in lower case: table names are not case sensitive, and folder names must not depend on it.
        Assert.That(tables, Is.EquivalentTo(new[] { "people", "orders", "__registry__" }));
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
    public void A_Second_Client_On_The_Folder_Gets_Its_Turn_While_The_First_Is_Busy()
    {
        using var other = new LuceneTableServiceClient(_fixture.TempPath);
        var mine = _fixture.TableService.GetTableClient("Shared");
        var theirs = other.GetTableClient("Shared");
        mine.Create();

        // The first client never leaves the table's index alone: four threads upsert without a pause.
        using var stop = new CancellationTokenSource();
        var busy = Enumerable.Range(0, 4).Select(thread => Task.Run(() =>
        {
            for (var n = 0; !stop.IsCancellationRequested; n++)
                mine.UpsertEntity(new TableEntity("busy", $"{thread}-{n % 10}"), TableUpdateMode.Replace);
        })).ToArray();
        try
        {
            for (var n = 0; n < 6; n++)
            {
                var waited = System.Diagnostics.Stopwatch.StartNew();
                theirs.UpsertEntity(new TableEntity("theirs", $"{n}"), TableUpdateMode.Replace);
                // In line behind the four busy threads, not shut out until a ten-second limit.
                Assert.That(waited.Elapsed, Is.LessThan(TimeSpan.FromSeconds(8)));
            }
        }
        finally
        {
            stop.Cancel();
            Task.WaitAll(busy);
        }

        Assert.That(mine.Query<TableEntity>(e => e.PartitionKey == "theirs").Count(), Is.EqualTo(6));
    }

    [Test]
    public void An_Index_Another_Process_Keeps_Locked_Is_Waited_For_And_Then_Refused_As_Busy()
    {
        var table = _fixture.TableService.GetTableClient("locked");
        table.Create();
        table.AddEntity(new TableEntity("p", "before"));
        var queue = _fixture.QueueService.GetQueueClient("locked");
        queue.Create();
        queue.SendMessage("waiting");

        // What another process writing to the table or the queue looks like from here: its write lock is
        // taken, through a directory object that is not the client's own. A new one each time: a
        // Lucene lock that has been released cannot be relied on to hold again.
        IDisposable LockIndex(string kind)
        {
            var folder = FSDirectory.Open(Path.Combine(_fixture.TempPath, kind, "locked"));
            var held = folder.MakeLock(IndexWriter.WRITE_LOCK_NAME);
            Assert.That(held.Obtain(), Is.True);
            return new Release(() => { held.Dispose(); folder.Dispose(); });
        }

        // Held for a moment: the operation waits and then goes through.
        var forAMoment = LockIndex("tables");
        var release = Task.Run(() => { Thread.Sleep(500); forAMoment.Dispose(); });
        var waited = System.Diagnostics.Stopwatch.StartNew();
        table.AddEntity(new TableEntity("p", "after a wait"));
        Assert.That(waited.Elapsed, Is.GreaterThan(TimeSpan.FromMilliseconds(300)).And.LessThan(TimeSpan.FromSeconds(8)));
        release.Wait();

        // Held for good: the operation gives up after ten seconds, with an error an Azure retry policy knows.
        using (LockIndex("tables"))
        using (LockIndex("queues"))
        {
            var adding = Task.Run(() => Assert.Throws<RequestFailedException>(() => table.AddEntity(new TableEntity("p", "never"))));
            var receiving = Task.Run(() => Assert.Throws<RequestFailedException>(() => queue.ReceiveMessages(1)));
            foreach (var refused in new[] { adding.Result!, receiving.Result! })
                Assert.That((refused.Status, refused.ErrorCode), Is.EqualTo((503, "ServerBusy")));
            // A table is refused by its index writer, and says so. A queue has no index writer: it asks for the lock itself.
            Assert.That(adding.Result!.InnerException, Is.InstanceOf<LockObtainFailedException>());
            Assert.That(receiving.Result!.InnerException, Is.Null);
        }

        // Nothing was half done, and both are usable again once the lock is free.
        Assert.That(table.Query<TableEntity>().Select(e => e.RowKey), Is.EquivalentTo(new[] { "before", "after a wait" }));
        Assert.That(queue.ReceiveMessages(1).Value.Single().Body.ToString(), Is.EqualTo("waiting"));
    }

    private sealed class Release(Action release) : IDisposable
    {
        public void Dispose() => release();
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
