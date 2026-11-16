using System.Collections.Concurrent;
using Iciclecreek.Azure.Storage.Lucene.Blobs;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using LuceneDirectory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Directories;

/// <summary>
/// What the clients promise about the directory factory they are given: each directory is asked
/// for once, and is disposed with the client.
/// </summary>
public class DirectoryFactoryTests
{
    private readonly ConcurrentBag<(string Path, RAMDirectory Directory)> _made = [];

    [SetUp]
    public void SetUp() => _made.Clear();

    private LuceneDirectory Make(string path)
    {
        var directory = new RAMDirectory();
        _made.Add((path, directory));
        // Slow enough that callers arriving together overlap.
        Thread.Sleep(20);
        return directory;
    }

    private static bool IsDisposed(RAMDirectory directory)
    {
        try
        {
            directory.ListAll();
            return false;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }

    [Test]
    public void Each_Directory_Is_Asked_For_Once_However_Many_Threads_First_Use_It_Together()
    {
        using var tables = new LuceneTableServiceClient(Make);
        using var queues = new LuceneQueueServiceClient(Make);
        using var blobs = new LuceneBlobServiceClient(Make);
        using var start = new ManualResetEventSlim();
        var users = Enumerable.Range(0, 16).Select(n => Task.Run(async () =>
        {
            start.Wait();
            tables.GetTableClient("shared").CreateIfNotExists();
            queues.GetQueueClient("shared").CreateIfNotExists();
            await blobs.GetBlobContainerClient("shared").GetBlobClient($"b{n}").ExistsAsync();
        })).ToArray();
        start.Set();
        Task.WaitAll(users);

        Assert.That(_made.Select(m => m.Path).OrderBy(p => p, StringComparer.Ordinal), Is.EqualTo(new[]
        {
            "blobs/shared", "queues/__registry__", "queues/shared", "tables/__registry__", "tables/shared",
        }));
    }

    [Test]
    public async Task Every_Directory_Handed_Out_Is_Disposed_With_Its_Client()
    {
        var tables = new LuceneTableServiceClient(Make);
        var queues = new LuceneQueueServiceClient(Make);
        var blobs = new LuceneBlobServiceClient(Make);
        tables.GetTableClient("t").Create();
        queues.GetQueueClient("q").Create();
        await blobs.GetBlobContainerClient("c").GetBlobClient("b").UploadAsync(BinaryData.FromString("x"), overwrite: true);
        Assert.That(_made.Select(m => m.Path), Is.EquivalentTo(new[] { "tables/__registry__", "tables/t", "queues/__registry__", "queues/q", "blobs/c" }));
        Assert.That(_made.Select(m => IsDisposed(m.Directory)), Has.All.False);

        tables.Dispose();
        queues.Dispose();
        blobs.Dispose();

        Assert.That(_made.Where(m => !IsDisposed(m.Directory)).Select(m => m.Path), Is.Empty);
        // Disposing twice is harmless.
        Assert.DoesNotThrow(() => { tables.Dispose(); queues.Dispose(); blobs.Dispose(); });
    }

    [Test]
    public void After_Dispose_The_Queue_Client_Asks_Again_And_The_Table_Client_Cannot_Be_Used()
    {
        var tables = new LuceneTableServiceClient(Make);
        using var queues = new LuceneQueueServiceClient(Make);
        tables.GetTableClient("t").CreateIfNotExists();
        queues.GetQueueClient("q").CreateIfNotExists();
        tables.Dispose();
        queues.Dispose();
        string[] askedForBeforeDispose = ["tables/__registry__", "tables/t", "queues/__registry__", "queues/q"];
        Assert.That(_made.Select(m => m.Path), Is.EquivalentTo(askedForBeforeDispose));

        // The table client keeps the directories it was given, closed as they now are, and asks for no others.
        Assert.Catch<ObjectDisposedException>(() => tables.GetTableClient("t").CreateIfNotExists());
        Assert.Catch<ObjectDisposedException>(() => tables.Query().ToList());
        Assert.That(_made.Select(m => m.Path), Is.EquivalentTo(askedForBeforeDispose));

        // The queue client keeps none: it asks for each again and works on what it is then given.
        queues.GetQueueClient("q").CreateIfNotExists();
        Assert.That(queues.GetQueueClient("q").Exists().Value, Is.True);
        Assert.That(_made.Select(m => m.Path), Is.EquivalentTo(askedForBeforeDispose.Concat(["queues/__registry__", "queues/q"])));
    }
}
