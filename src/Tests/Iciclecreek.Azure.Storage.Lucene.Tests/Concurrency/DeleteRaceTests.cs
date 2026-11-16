using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using FilterDirectory = Lucene.Net.Store.FilterDirectory;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using IndexWriterConfig = Lucene.Net.Index.IndexWriterConfig;
using KeywordAnalyzer = Lucene.Net.Analysis.Core.KeywordAnalyzer;
using LuceneDirectory = Lucene.Net.Store.Directory;
using LuceneLock = Lucene.Net.Store.Lock;
using LuceneVersion = Lucene.Net.Util.LuceneVersion;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Concurrency;

/// <summary>
/// A table that is deleted and made again starts empty, whatever was written to it while it was
/// being deleted. And a delete takes only what it was asked to delete: a table made again while
/// the delete runs keeps what is then written to it. <see cref="QueueDeleteRaceTests"/> has the
/// same for queues.
/// </summary>
public class DeleteRaceTests
{
    private const int Rounds = 300;

    [Test]
    public void An_Entity_Added_While_Its_Table_Is_Deleted_Is_Not_There_When_The_Table_Is_Made_Again()
    {
        using var service = new LuceneTableServiceClient();
        var table = service.GetTableClient("raced");

        for (var round = 0; round < Rounds; round++)
        {
            table.Create();
            var adding = Task.Run(() =>
            {
                for (var n = 0; n < 5; n++)
                {
                    try { table.AddEntity(new TableEntity("p", $"{n}")); }
                    catch (RequestFailedException gone) when (gone.Status == 404) { /* the delete got there first */ }
                }
            });
            var deleting = Task.Run(() => table.Delete());
            Task.WaitAll(adding, deleting);

            table.Create();
            Assert.That(table.Query<TableEntity>(), Is.Empty, $"round {round}");
            table.Delete();
        }
    }

    [Test]
    public void Making_A_Table_Clears_What_Was_Left_In_Its_Index_After_It_Was_Deleted()
    {
        var folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneRaces-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var tables = new LuceneTableServiceClient(folder);
            var table = tables.GetTableClient("halfgone");
            table.Create();
            table.AddEntity(new TableEntity("p", "left behind"));

            // A delete empties the index and then takes the name off the list, so a write that gets
            // in between the two is left in the index of something that is no longer listed. Here
            // that is arrived at directly: the list is emptied, behind the client's back, and
            // nothing else.
            using (var registry = FSDirectory.Open(Path.Combine(folder, "tables", "__registry__")))
            using (var writer = new IndexWriter(registry, new IndexWriterConfig(LuceneVersion.LUCENE_48, new KeywordAnalyzer())))
            {
                writer.DeleteAll();
                writer.Commit();
            }
            Assert.That(Assert.Throws<RequestFailedException>(() => table.GetEntity<TableEntity>("p", "left behind"))!.Status, Is.EqualTo(404));

            table.Create();

            Assert.That(table.Query<TableEntity>(), Is.Empty);
        }
        finally
        {
            if (Directory.Exists(folder))
                Directory.Delete(folder, recursive: true);
        }
    }

    [Test]
    public void A_Table_Made_Again_While_It_Is_Being_Deleted_Keeps_What_Is_Added_To_It()
    {
        var folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneRaces-" + Guid.NewGuid().ToString("N"));
        try
        {
            var deleter = new AnotherProcess(folder);
            var maker = new AnotherProcess(folder);
            using var deletersTables = new LuceneTableServiceClient(deleter.Open);
            using var makersTables = new LuceneTableServiceClient(maker.Open);
            var doomed = deletersTables.GetTableClient("remade");
            var remade = makersTables.GetTableClient("remade");
            remade.Create();
            remade.AddEntity(new TableEntity("p", "old"));

            DeleteWhileItIsMadeAgain(deleter, maker, "tables",
                delete: () => doomed.Delete(),
                makeAgain: () =>
                {
                    remade.Create();
                    remade.AddEntity(new TableEntity("p", "new"));
                });

            Assert.That(makersTables.Query().Select(t => t.Name), Is.EqualTo(new[] { "remade" }));
            Assert.That(remade.Query<TableEntity>().Select(e => e.RowKey), Is.EqualTo(new[] { "new" }));
        }
        finally
        {
            if (Directory.Exists(folder))
                Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>
    /// Deletes the table or queue <c>remade</c> while another process makes it again and writes to
    /// it. The delete is stopped where it asks for the write lock of the table's or queue's own
    /// index, which is where it waits whenever another writer has that index, and is let go once
    /// the other process has either done its work or been kept out by the delete.
    /// </summary>
    internal static void DeleteWhileItIsMadeAgain(AnotherProcess deleter, AnotherProcess maker, string kind, Action delete, Action makeAgain)
    {
        using var deleteIsAtTheIndex = new ManualResetEventSlim();
        using var letTheDeleteGo = new ManualResetEventSlim();
        using var makerIsKeptOut = new ManualResetEventSlim();
        deleter.AsksForWriteLock = path =>
        {
            if (path != $"{kind}/remade")
                return;
            deleteIsAtTheIndex.Set();
            letTheDeleteGo.Wait();
        };
        var asksForTheRegistry = 0;
        maker.AsksForWriteLock = path =>
        {
            // Asking a second time means the first try was refused: the delete has the registry.
            if (path == $"{kind}/__registry__" && Interlocked.Increment(ref asksForTheRegistry) == 2)
                makerIsKeptOut.Set();
        };

        var deleting = Task.Run(delete);
        Task makingAgain;
        try
        {
            Assert.That(deleteIsAtTheIndex.Wait(TimeSpan.FromSeconds(30)), Is.True, "the delete got as far as the index");
            makingAgain = Task.Run(makeAgain);
            Assert.That(SpinWait.SpinUntil(() => makingAgain.IsCompleted || makerIsKeptOut.IsSet, TimeSpan.FromSeconds(30)), Is.True,
                "the other process either got in or was kept out");
        }
        finally
        {
            letTheDeleteGo.Set();
        }
        Task.WaitAll(deleting, makingAgain);
    }

    /// <summary>
    /// The directories of a client that stands in for another process. A directory that is not a
    /// plain folder directory has write gates of its own, so its client shares none with any other
    /// client and meets them only at Lucene's write lock, as a second process does.
    /// </summary>
    internal sealed class AnotherProcess(string folder)
    {
        /// <summary>Called with a directory's path each time the write lock of its index is asked for.</summary>
        public Action<string>? AsksForWriteLock { get; set; }

        public LuceneDirectory Open(string path) =>
            new Watched(FSDirectory.Open(Path.Combine(folder, path.Replace('/', Path.DirectorySeparatorChar))), () => AsksForWriteLock?.Invoke(path));

        private sealed class Watched(LuceneDirectory inner, Action asksForWriteLock) : FilterDirectory(inner)
        {
            public override LuceneLock MakeLock(string name)
            {
                if (name == IndexWriter.WRITE_LOCK_NAME)
                    asksForWriteLock();
                return base.MakeLock(name);
            }
        }
    }
}
