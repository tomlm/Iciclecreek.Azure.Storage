using Azure;
using Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using IndexWriterConfig = Lucene.Net.Index.IndexWriterConfig;
using KeywordAnalyzer = Lucene.Net.Analysis.Core.KeywordAnalyzer;
using LuceneVersion = Lucene.Net.Util.LuceneVersion;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Concurrency;

/// <summary>
/// A queue that is deleted and made again starts empty, whatever was sent to it while it was
/// being deleted. And a delete takes only what it was asked to delete: a queue made again while
/// the delete runs keeps what is then sent to it. The same whether the queues are kept as indexes
/// or as files.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
public class QueueDeleteRaceTests(bool queuesAsFiles)
{
    private const int Rounds = 300;

    [Test]
    public void A_Message_Sent_While_Its_Queue_Is_Deleted_Is_Not_There_When_The_Queue_Is_Made_Again()
    {
        using var service = QueueServices.InMemory(queuesAsFiles);
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
        using var service = QueueServices.InMemory(queuesAsFiles);
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
    public void Making_A_Queue_Clears_What_Was_Left_In_Its_Directory_After_It_Was_Deleted()
    {
        var folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneRaces-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var queues = QueueServices.InFolder(folder, queuesAsFiles);
            var queue = queues.GetQueueClient("halfgone");
            queue.Create();
            queue.SendMessage("left behind");

            // A delete empties the queue's directory and then takes the name off the list, so a
            // message sent in between the two is left in the directory of a queue that is no longer
            // listed. Here that is arrived at directly: the list is emptied, behind the client's
            // back, and nothing else.
            var registry = Path.Combine(folder, "queues", "__registry__");
            if (queuesAsFiles)
            {
                foreach (var listed in Directory.GetFiles(registry, "*.queue"))
                    File.Delete(listed);
            }
            else
            {
                using var index = FSDirectory.Open(registry);
                using var writer = new IndexWriter(index, new IndexWriterConfig(LuceneVersion.LUCENE_48, new KeywordAnalyzer()));
                writer.DeleteAll();
                writer.Commit();
            }
            Assert.That(queue.Exists().Value, Is.False);

            queue.Create();

            Assert.That(queue.GetProperties().Value.ApproximateMessagesCount, Is.Zero);
            Assert.That(queue.ReceiveMessages(10).Value, Is.Empty);
        }
        finally
        {
            if (Directory.Exists(folder))
                Directory.Delete(folder, recursive: true);
        }
    }

    [Test]
    public void A_Queue_Made_Again_While_It_Is_Being_Deleted_Keeps_What_Is_Sent_To_It()
    {
        var folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneRaces-" + Guid.NewGuid().ToString("N"));
        try
        {
            var deleter = new DeleteRaceTests.AnotherProcess(folder);
            var maker = new DeleteRaceTests.AnotherProcess(folder);
            using var deletersQueues = QueueServices.On(deleter.Open, queuesAsFiles);
            using var makersQueues = QueueServices.On(maker.Open, queuesAsFiles);
            var doomed = deletersQueues.GetQueueClient("remade");
            var remade = makersQueues.GetQueueClient("remade");
            remade.Create();
            remade.SendMessage("old");

            DeleteRaceTests.DeleteWhileItIsMadeAgain(deleter, maker, "queues",
                delete: () => doomed.Delete(),
                makeAgain: () =>
                {
                    remade.Create();
                    remade.SendMessage("new");
                });

            Assert.That(remade.Exists().Value, Is.True);
            Assert.That(remade.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "new" }));
        }
        finally
        {
            if (Directory.Exists(folder))
                Directory.Delete(folder, recursive: true);
        }
    }
}
