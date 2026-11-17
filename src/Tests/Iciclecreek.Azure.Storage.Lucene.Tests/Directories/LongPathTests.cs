using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Lucene.Blobs;
using Iciclecreek.Azure.Storage.Lucene.Internal;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using FSDirectory = Lucene.Net.Store.FSDirectory;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Directories;

/// <summary>
/// A client given a folder works however deep the folder is. On Windows a file past 260
/// characters can be written by .NET, but not synced by Lucene.Net unless its path is in the
/// extended form (<c>\\?\C:\…</c>), and a queued message's file name alone is over 100 characters.
/// <para>
/// The test host is long-path aware, so the sync would succeed here even without the extended
/// form; only an ordinary program shows the failure. So the form of the path is checked as well
/// as the clients working.
/// </para>
/// </summary>
[Platform("Win")]
public class LongPathTests
{
    [TestCase(@"C:\data\storage", @"\\?\C:\data\storage")]
    [TestCase(@"\\server\share\storage", @"\\?\UNC\server\share\storage")]
    [TestCase(@"\\?\C:\data\storage", @"\\?\C:\data\storage")]
    [TestCase(@"\\?\UNC\server\share\storage", @"\\?\UNC\server\share\storage")]
    [TestCase(@"\\.\pipe\storage", @"\\.\pipe\storage")]
    public void A_Full_Path_Is_Given_Its_Extended_Form_Once(string path, string extended)
    {
        Assert.That(FolderPaths.ToExtended(path), Is.EqualTo(extended));
        Assert.That(FolderPaths.WithoutExtended(FolderPaths.ToExtended(path)), Is.EqualTo(path.StartsWith(@"\\?\") ? FolderPaths.WithoutExtended(path) : path));
    }

    [Test]
    public void Every_Folder_Is_Opened_By_Its_Extended_Path()
    {
        var factory = FolderPaths.InFolder(_root);
        using var directory = (FSDirectory)factory("queues/work");
        Assert.That(directory.Directory.FullName, Is.EqualTo(@"\\?\" + Path.Combine(_root, "queues", "work")));
    }

    [Test]
    public void A_Folder_Shares_Its_Write_Gate_Whichever_Form_Of_Its_Path_Opened_It()
    {
        var folder = Path.Combine(_root, "queues", "work");
        using var plain = FSDirectory.Open(folder);
        using var extended = FSDirectory.Open(@"\\?\" + folder);
        Assert.That(IndexWriteLock.GateFor(extended), Is.SameAs(IndexWriteLock.GateFor(plain)));
    }

    private string _top = null!;
    private string _root = null!;

    [SetUp]
    public void SetUp()
    {
        _top = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneLongPaths-" + Guid.NewGuid().ToString("N"));
        _root = _top;
        // Deep enough that a queued message's file is well past 260 characters.
        while (_root.Length < 220)
            _root = Path.Combine(_root, "a-rather-long-folder-name");
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_top))
            Directory.Delete(@"\\?\" + _top, recursive: true);
    }

    [Test]
    public void Queues_Work_In_A_Folder_Past_260_Characters()
    {
        using (var service = new LuceneQueueServiceClient(_root))
        {
            var queue = service.GetQueueClient("work");
            queue.Create();
            queue.SendMessage("first");
            queue.SendMessage("second");

            var received = queue.ReceiveMessage().Value;
            Assert.That(received.MessageText, Is.EqualTo("first"));
            queue.DeleteMessage(received.MessageId, received.PopReceipt);
        }

        var message = Directory.EnumerateFiles(@"\\?\" + _root, "*", SearchOption.AllDirectories)
            .Where(f => f.Contains(Path.Combine("queues", "work"), StringComparison.Ordinal))
            .Select(f => f[@"\\?\".Length..])
            .MaxBy(f => f.Length);
        Assert.That(message, Is.Not.Null);
        Assert.That(message!.Length, Is.GreaterThan(260), "the test folder is not deep enough to show anything");

        using var reopened = new LuceneQueueServiceClient(_root);
        Assert.That(reopened.GetQueueClient("work").ReceiveMessage().Value.MessageText, Is.EqualTo("second"));
    }

    [Test]
    public async Task Blobs_Work_In_A_Folder_Past_260_Characters()
    {
        using (var service = new LuceneBlobServiceClient(_root))
        {
            var container = service.GetBlobContainerClient("files");
            await container.GetBlobClient("notes/today.txt").UploadAsync(BinaryData.FromString("hello"), overwrite: true);
        }

        using var reopened = new LuceneBlobServiceClient(_root);
        var blob = reopened.GetBlobContainerClient("files").GetBlobClient("notes/today.txt");
        Assert.That((await blob.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("hello"));
        Assert.That(reopened.GetBlobContainerClient("files").GetBlobs().Select(b => b.Name), Is.EqualTo(new[] { "notes/today.txt" }));
        Assert.That((await blob.DeleteIfExistsAsync()).Value, Is.True);
    }

    [Test]
    public void Tables_Work_In_A_Folder_Past_260_Characters()
    {
        using (var service = new LuceneTableServiceClient(_root))
        {
            var table = service.GetTableClient("people");
            table.CreateIfNotExists();
            table.AddEntity(new TableEntity("p", "1") { ["Name"] = "Ann" });
        }

        using var reopened = new LuceneTableServiceClient(_root);
        Assert.That(reopened.GetTableClient("people").GetEntity<TableEntity>("p", "1").Value["Name"], Is.EqualTo("Ann"));
        Assert.That(reopened.Query().Select(t => t.Name), Is.EqualTo(new[] { "people" }));
    }

    [Test]
    public void A_Folder_Given_In_Extended_Form_Is_Used_As_It_Is()
    {
        using (var service = new LuceneQueueServiceClient(@"\\?\" + _root))
        {
            var queue = service.GetQueueClient("work");
            queue.Create();
            queue.SendMessage("hello");
        }

        // The same folder, by its plain path.
        using var plain = new LuceneQueueServiceClient(_root);
        Assert.That(plain.GetQueueClient("work").ReceiveMessage().Value.MessageText, Is.EqualTo("hello"));
    }
}
