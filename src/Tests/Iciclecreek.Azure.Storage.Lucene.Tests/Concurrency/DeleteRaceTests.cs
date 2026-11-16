using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using IndexWriterConfig = Lucene.Net.Index.IndexWriterConfig;
using KeywordAnalyzer = Lucene.Net.Analysis.Core.KeywordAnalyzer;
using LuceneVersion = Lucene.Net.Util.LuceneVersion;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Concurrency;

/// <summary>
/// A table or queue that is deleted and made again starts empty, whatever was going on when it
/// was deleted: a write that was racing the delete, or a delete that never finished.
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
    public void A_Message_Sent_While_Its_Queue_Is_Deleted_Is_Not_There_When_The_Queue_Is_Made_Again()
    {
        using var service = new LuceneQueueServiceClient();
        var queue = service.GetQueueClient("raced");

        for (var round = 0; round < Rounds; round++)
        {
            queue.Create();
            var sending = Task.Run(() =>
            {
                for (var n = 0; n < 5; n++)
                {
                    try { queue.SendMessage($"{n}"); }
                    catch (RequestFailedException gone) when (gone.Status == 404) { /* the delete got there first */ }
                }
            });
            var deleting = Task.Run(() => queue.Delete());
            Task.WaitAll(sending, deleting);

            queue.Create();
            Assert.That(queue.GetProperties().Value.ApproximateMessagesCount, Is.Zero, $"round {round}");
            queue.Delete();
        }
    }

    [Test]
    public void Setting_Metadata_While_The_Queue_Is_Deleted_Does_Not_Bring_The_Queue_Back()
    {
        using var service = new LuceneQueueServiceClient();
        var queue = service.GetQueueClient("raced");

        for (var round = 0; round < Rounds; round++)
        {
            queue.Create();
            var setting = Task.Run(() =>
            {
                for (var n = 0; n < 5; n++)
                {
                    try { queue.SetMetadata(new Dictionary<string, string> { ["n"] = $"{n}" }); }
                    catch (RequestFailedException gone) when (gone.Status == 404) { /* the delete got there first */ }
                }
            });
            var deleting = Task.Run(() => queue.Delete());
            Task.WaitAll(setting, deleting);

            Assert.That(queue.Exists().Value, Is.False, $"round {round}");
        }
    }

    [Test]
    public void Making_A_Table_Or_Queue_Clears_What_A_Delete_That_Never_Finished_Left_Behind()
    {
        var folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneRaces-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var tables = new LuceneTableServiceClient(folder);
            using var queues = new LuceneQueueServiceClient(folder);
            var table = tables.GetTableClient("halfgone");
            var queue = queues.GetQueueClient("halfgone");
            table.Create();
            table.AddEntity(new TableEntity("p", "left behind"));
            queue.Create();
            queue.SendMessage("left behind");

            // A delete takes the name off the list and then empties the index. Here is one that
            // died in between: the lists are emptied, behind the clients' backs, and nothing else.
            foreach (var kind in new[] { "tables", "queues" })
            {
                using var registry = FSDirectory.Open(Path.Combine(folder, kind, "__registry__"));
                using var writer = new IndexWriter(registry, new IndexWriterConfig(LuceneVersion.LUCENE_48, new KeywordAnalyzer()));
                writer.DeleteAll();
                writer.Commit();
            }
            Assert.That(Assert.Throws<RequestFailedException>(() => table.GetEntity<TableEntity>("p", "left behind"))!.Status, Is.EqualTo(404));
            Assert.That(queue.Exists().Value, Is.False);

            table.Create();
            queue.Create();

            Assert.That(table.Query<TableEntity>(), Is.Empty);
            Assert.That(queue.GetProperties().Value.ApproximateMessagesCount, Is.Zero);
            Assert.That(queue.ReceiveMessages(10).Value, Is.Empty);
        }
        finally
        {
            if (Directory.Exists(folder))
                Directory.Delete(folder, recursive: true);
        }
    }
}
