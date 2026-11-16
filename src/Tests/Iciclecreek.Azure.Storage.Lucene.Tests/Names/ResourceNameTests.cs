using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Lucene.Blobs;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Names;

/// <summary>
/// The names of tables, queues and containers become directory names. Whatever the name and
/// whatever the file system, different names must stay different things, the registry must not
/// be reachable by name, and nothing may be written outside the folder the client was given.
/// </summary>
[TestFixture(false, false)]
[TestFixture(true, false)]
[TestFixture(false, true)]
[TestFixture(true, true)]
public class ResourceNameTests(bool onDisk, bool queuesAsFiles)
{
    private string _outer = null!;
    private string _folder = null!;
    private LuceneTableServiceClient _tables = null!;
    private LuceneQueueServiceClient _queues = null!;
    private LuceneBlobServiceClient _blobs = null!;

    [SetUp]
    public void SetUp()
    {
        // The storage folder sits two levels down, so that a name climbing out of it would land where the test can see.
        _outer = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneNames-" + Guid.NewGuid().ToString("N"));
        _folder = Path.Combine(_outer, "inner", "storage");
        Directory.CreateDirectory(_folder);
        _tables = onDisk ? new LuceneTableServiceClient(_folder) : new LuceneTableServiceClient();
        _queues = onDisk ? QueueServices.InFolder(_folder, queuesAsFiles) : QueueServices.InMemory(queuesAsFiles);
        _blobs = onDisk ? new LuceneBlobServiceClient(_folder) : new LuceneBlobServiceClient();
    }

    [TearDown]
    public void TearDown()
    {
        _tables.Dispose();
        _queues.Dispose();
        _blobs.Dispose();
        Directory.Delete(_outer, recursive: true);
    }

    [Test]
    public void Table_Names_That_Differ_Only_By_Case_Are_One_Table_As_In_Azure()
    {
        var upper = _tables.GetTableClient("Orders");
        var lower = _tables.GetTableClient("orders");
        upper.Create();

        Assert.That(Assert.Throws<RequestFailedException>(() => lower.Create())!.Status, Is.EqualTo(409));
        upper.AddEntity(new TableEntity("p", "1"));
        Assert.That(lower.Query<TableEntity>().Select(e => e.RowKey), Is.EqualTo(new[] { "1" }));
        // Listed once, under the name it was created with.
        Assert.That(_tables.Query().Select(t => t.Name), Is.EqualTo(new[] { "Orders" }));

        lower.Delete();
        Assert.That(_tables.Query(), Is.Empty);
        Assert.That(Assert.Throws<RequestFailedException>(() => upper.AddEntity(new TableEntity("p", "2")))!.Status, Is.EqualTo(404));
    }

    [Test]
    public void Queue_Names_That_Differ_Only_By_Case_Are_Separate_Queues_On_Any_File_System()
    {
        var upper = _queues.GetQueueClient("Work");
        var lower = _queues.GetQueueClient("work");
        upper.Create();
        lower.Create();
        upper.SendMessage("for upper");

        Assert.That(lower.PeekMessages(10).Value, Is.Empty);
        lower.Delete();
        Assert.That(upper.ReceiveMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "for upper" }));
        Assert.That(_queues.GetQueues().Select(q => q.Name), Is.EqualTo(new[] { "Work" }));
    }

    [Test]
    public async Task Container_Names_That_Differ_Only_By_Case_Are_Separate_Containers_On_Any_File_System()
    {
        var upper = _blobs.GetBlobContainerClient("Photos");
        var lower = _blobs.GetBlobContainerClient("photos");
        await upper.GetBlobClient("a.txt").UploadAsync(BinaryData.FromString("in upper"), overwrite: true);
        await lower.GetBlobClient("a.txt").UploadAsync(BinaryData.FromString("in lower"), overwrite: true);

        Assert.That((await upper.GetBlobClient("a.txt").DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("in upper"));
        await lower.DeleteAsync();
        Assert.That(upper.GetBlobs().Select(b => b.Name), Is.EqualTo(new[] { "a.txt" }));
    }

    [Test]
    public void The_Registry_Cannot_Be_Reached_As_A_Table_Or_A_Queue()
    {
        _tables.GetTableClient("real").Create();
        _queues.GetQueueClient("real").Create();

        foreach (var name in new[] { "__registry__", "__REGISTRY__", "" })
        {
            var table = _tables.GetTableClient(name);
            var queue = _queues.GetQueueClient(name);
            var refusals = new[]
            {
                Assert.Throws<RequestFailedException>(() => table.Create())!,
                Assert.Throws<RequestFailedException>(() => table.CreateIfNotExists())!,
                Assert.Throws<RequestFailedException>(() => table.AddEntity(new TableEntity("p", "1")))!,
                Assert.Throws<RequestFailedException>(() => table.Delete())!,
                Assert.Throws<RequestFailedException>(() => queue.Create())!,
                Assert.Throws<RequestFailedException>(() => queue.CreateIfNotExists())!,
                Assert.Throws<RequestFailedException>(() => queue.DeleteMessage("id", "receipt"))!,
            };
            Assert.That(refusals.Select(r => (r.Status, r.ErrorCode)), Has.All.EqualTo((400, "InvalidResourceName")), $"name '{name}'");
            // A queue that cannot be made is not there to be used.
            Assert.That(Assert.Throws<RequestFailedException>(() => queue.SendMessage("x"))!.Status, Is.EqualTo(404));
        }

        // The lists are as they were.
        Assert.That(_tables.Query().Select(t => t.Name), Is.EqualTo(new[] { "real" }));
        Assert.That(_queues.GetQueues().Select(q => q.Name), Is.EqualTo(new[] { "real" }));
    }

    [Test]
    public async Task A_Name_Too_Long_To_Be_A_Folder_Name_Is_Refused()
    {
        // A capital letter is five characters of a folder name, so 52 of them are 260, and a folder name may be 255.
        var capitals = new string('Q', 52);
        var letters = new string('q', 256);
        foreach (var name in new[] { capitals, letters })
        {
            var refusals = new[]
            {
                Assert.Throws<RequestFailedException>(() => _queues.GetQueueClient(name).Create())!,
                Assert.Throws<RequestFailedException>(() => _queues.GetQueueClient(name).CreateIfNotExists())!,
                (await Assert.ThrowsAsync<RequestFailedException>(() => _blobs.GetBlobContainerClient(name).GetBlobClient("b").UploadAsync(BinaryData.FromString("x"), overwrite: true)))!,
            };
            Assert.That(refusals.Select(r => (r.Status, r.ErrorCode)), Has.All.EqualTo((400, "InvalidResourceName")), $"name of {name.Length} characters");
        }
        // A table's folder is its name in lower case, so only the length of the name counts.
        var table = _tables.GetTableClient(new string('T', 256));
        Assert.That(Assert.Throws<RequestFailedException>(() => table.Create())!.ErrorCode, Is.EqualTo("InvalidResourceName"));
        Assert.That(Assert.Throws<RequestFailedException>(() => table.AddEntity(new TableEntity("p", "1")))!.ErrorCode, Is.EqualTo("InvalidResourceName"));
        Assert.That(_tables.Query(), Is.Empty);
        Assert.That(_queues.GetQueues(), Is.Empty);

        // The longest names that fit are still good ones.
        var longestTable = _tables.GetTableClient(new string('T', 255));
        longestTable.Create();
        longestTable.AddEntity(new TableEntity("p", "1"));
        Assert.That(longestTable.Query<TableEntity>().Select(e => e.RowKey), Is.EqualTo(new[] { "1" }));
        var longestQueue = _queues.GetQueueClient(new string('Q', 51));
        longestQueue.Create();
        longestQueue.SendMessage("fits");
        Assert.That(longestQueue.PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { "fits" }));
    }

    [Test]
    public async Task A_Name_That_Looks_Like_A_Path_Or_A_Device_Is_Kept_Inside_The_Folder()
    {
        string[] names = ["../../escaped", @"..\..\escaped", "/escaped", "a/b", "..", "nul", "con", "dots.and spaces "];
        foreach (var name in names)
        {
            var table = _tables.GetTableClient(name);
            table.Create();
            table.AddEntity(new TableEntity("p", name));
            var queue = _queues.GetQueueClient(name);
            queue.Create();
            queue.SendMessage(name);
            await _blobs.GetBlobContainerClient(name).GetBlobClient("b").UploadAsync(BinaryData.FromString(name), overwrite: true);
        }

        // Each is a thing of its own, holding what was put in it.
        Assert.That(_tables.Query().Select(t => t.Name), Is.EquivalentTo(names));
        Assert.That(_queues.GetQueues().Select(q => q.Name), Is.EquivalentTo(names));
        foreach (var name in names)
        {
            Assert.That(_tables.GetTableClient(name).Query<TableEntity>().Select(e => e.RowKey), Is.EqualTo(new[] { name }));
            Assert.That(_queues.GetQueueClient(name).PeekMessages(10).Value.Select(m => m.Body.ToString()), Is.EqualTo(new[] { name }));
            Assert.That((await _blobs.GetBlobContainerClient(name).GetBlobClient("b").DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo(name));
        }

        if (!onDisk)
            return;
        // Nothing was written beside or above the storage folder, and each name is one folder directly under its kind.
        Assert.That(Directory.GetFileSystemEntries(_outer).Select(Path.GetFileName), Is.EqualTo(new[] { "inner" }));
        Assert.That(Directory.GetFileSystemEntries(Path.Combine(_outer, "inner")).Select(Path.GetFileName), Is.EqualTo(new[] { "storage" }));
        Assert.That(Directory.GetFileSystemEntries(_folder).Select(Path.GetFileName), Is.EquivalentTo(new[] { "tables", "queues", "blobs" }));
        Assert.That(Directory.GetDirectories(Path.Combine(_folder, "tables")), Has.Length.EqualTo(names.Length + 1));
        Assert.That(Directory.GetDirectories(Path.Combine(_folder, "queues")), Has.Length.EqualTo(names.Length + 1));
        Assert.That(Directory.GetDirectories(Path.Combine(_folder, "blobs")), Has.Length.EqualTo(names.Length));
        Assert.That(Directory.GetDirectories(Path.Combine(_folder, "queues")).Select(Path.GetFileName), Does.Contain("_002e_002e_002f_002e_002e_002fescaped"));
    }
}
