using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.Lucene.Blobs;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Blobs;

/// <summary>
/// The blob operations the Lucene client supports: upload, download, delete, properties and
/// listing. It has no block, append or page blobs, so the shared blob suites do not apply.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
public class LuceneBlobTests(bool onDisk)
{
    private string _folder = null!;
    private BlobServiceClient _service = null!;
    private BlobContainerClient _container = null!;

    [SetUp]
    public void SetUp()
    {
        _folder = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneBlobs-" + Guid.NewGuid().ToString("N"));
        _service = onDisk ? new LuceneBlobServiceClient(_folder) : new LuceneBlobServiceClient();
        _container = _service.GetBlobContainerClient("files");
        _container.CreateIfNotExists();
    }

    [TearDown]
    public void TearDown()
    {
        _container.DeleteIfExists();
        if (Directory.Exists(_folder))
        {
            try { Directory.Delete(_folder, recursive: true); } catch (IOException) { /* still held by the directory; the temp folder is cleaned by the system */ }
        }
    }

    private static BinaryData Text(string text) => BinaryData.FromString(text);

    [Test]
    public async Task A_Blob_Reads_Back_Exactly_As_Uploaded_With_Its_Properties()
    {
        var blob = _container.GetBlobClient("docs/hello.txt");
        var options = new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = "text/plain" },
            Metadata = new Dictionary<string, string> { ["author"] = "tom" },
        };

        var uploaded = await blob.UploadAsync(Text("héllo wörld"), options);

        Assert.That((await blob.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("héllo wörld"));
        var properties = (await blob.GetPropertiesAsync()).Value;
        Assert.Multiple(() =>
        {
            Assert.That(properties.ContentType, Is.EqualTo("text/plain"));
            Assert.That(properties.ContentLength, Is.EqualTo(Encoding.UTF8.GetByteCount("héllo wörld")));
            Assert.That(properties.Metadata["author"], Is.EqualTo("tom"));
            Assert.That(properties.ETag, Is.EqualTo(uploaded.Value.ETag));
            Assert.That((bool)blob.Exists(), Is.True);
        });
    }

    [Test]
    public async Task A_Large_Blob_Streams_In_And_Out()
    {
        var bytes = new byte[3_000_000];
        new Random(7).NextBytes(bytes);
        var blob = _container.GetBlobClient("big.bin");

        await blob.UploadAsync(new MemoryStream(bytes), overwrite: true);

        using var read = new MemoryStream();
        await blob.DownloadToAsync(read);
        Assert.That(read.ToArray(), Is.EqualTo(bytes));
        await using var streamed = (await blob.DownloadStreamingAsync()).Value.Content;
        using var copy = new MemoryStream();
        await streamed.CopyToAsync(copy);
        Assert.That(copy.Length, Is.EqualTo(bytes.Length));
    }

    [Test]
    public async Task Uploading_Over_A_Blob_Replaces_It_Unless_Told_Not_To()
    {
        var blob = _container.GetBlobClient("note.txt");
        var first = await blob.UploadAsync(Text("one"), overwrite: true);

        // Without overwrite, an existing blob is a conflict and stays as it was.
        var refused = Assert.Throws<RequestFailedException>(() => blob.Upload(Text("two"), overwrite: false));
        Assert.That(refused!.Status, Is.EqualTo(409));
        Assert.That((await blob.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("one"));

        var second = await blob.UploadAsync(Text("three"), overwrite: true);
        Assert.That((await blob.DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("three"));
        Assert.That(second.Value.ETag, Is.Not.EqualTo(first.Value.ETag));
    }

    [Test]
    public async Task A_Missing_Blob_Is_Not_Found_And_Deleting_One_Makes_It_So()
    {
        var blob = _container.GetBlobClient("gone.txt");
        Assert.That((bool)await blob.ExistsAsync(), Is.False);
        Assert.That(Assert.Throws<RequestFailedException>(() => blob.DownloadContent())!.Status, Is.EqualTo(404));
        Assert.That((bool)await blob.DeleteIfExistsAsync(), Is.False);

        await blob.UploadAsync(Text("here"), overwrite: true);
        Assert.That((bool)await blob.DeleteIfExistsAsync(), Is.True);
        Assert.That((bool)await blob.ExistsAsync(), Is.False);
    }

    [Test]
    public async Task Blobs_Are_Listed_Flat_By_Prefix_And_By_Folder()
    {
        foreach (var name in new[] { "a.txt", "docs/b.txt", "docs/c.txt", "docs/deep/d.txt", "pics/e.png" })
            await _container.GetBlobClient(name).UploadAsync(Text(name), overwrite: true);

        var all = _container.GetBlobs().Select(b => b.Name).ToList();
        var docs = _container.GetBlobs(new GetBlobsOptions { Prefix = "docs/" }).Select(b => b.Name).ToList();
        var top = _container.GetBlobsByHierarchy(new GetBlobsByHierarchyOptions { Delimiter = "/" }).ToList();
        var underDocs = _container.GetBlobsByHierarchy(new GetBlobsByHierarchyOptions { Delimiter = "/", Prefix = "docs/" }).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(all, Is.EquivalentTo(new[] { "a.txt", "docs/b.txt", "docs/c.txt", "docs/deep/d.txt", "pics/e.png" }));
            Assert.That(docs, Is.EquivalentTo(new[] { "docs/b.txt", "docs/c.txt", "docs/deep/d.txt" }));
            Assert.That(top.Where(i => i.IsBlob).Select(i => i.Blob.Name), Is.EquivalentTo(new[] { "a.txt" }));
            Assert.That(top.Where(i => i.IsPrefix).Select(i => i.Prefix), Is.EquivalentTo(new[] { "docs/", "pics/" }));
            Assert.That(underDocs.Where(i => i.IsBlob).Select(i => i.Blob.Name), Is.EquivalentTo(new[] { "docs/b.txt", "docs/c.txt" }));
            Assert.That(underDocs.Where(i => i.IsPrefix).Select(i => i.Prefix), Is.EquivalentTo(new[] { "docs/deep/" }));
        });
    }

    [Test]
    public async Task Names_That_Differ_Only_By_Case_Or_Hold_Odd_Characters_Are_Separate_Blobs()
    {
        await _container.GetBlobClient("Report.txt").UploadAsync(Text("upper"), overwrite: true);
        await _container.GetBlobClient("report.txt").UploadAsync(Text("lower"), overwrite: true);
        await _container.GetBlobClient("año/ñ #1?.txt").UploadAsync(Text("odd"), overwrite: true);

        Assert.That((await _container.GetBlobClient("Report.txt").DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("upper"));
        Assert.That((await _container.GetBlobClient("report.txt").DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("lower"));
        Assert.That((await _container.GetBlobClient("año/ñ #1?.txt").DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("odd"));
        Assert.That(_container.GetBlobs().Count(), Is.EqualTo(3));
    }

    [Test]
    public void A_Container_Nothing_Has_Been_Uploaded_To_Lists_Empty_And_Can_Be_Deleted()
    {
        // On disk the container's folder is only made by its first upload.
        var empty = _service.GetBlobContainerClient("empty");
        empty.CreateIfNotExists();

        Assert.That(empty.GetBlobs(), Is.Empty);
        Assert.That(empty.GetBlobsByHierarchy(new GetBlobsByHierarchyOptions { Delimiter = "/" }), Is.Empty);
        Assert.That(empty.DeleteIfExists().Value, Is.False);
        Assert.DoesNotThrow(() => empty.Delete());

        _service.CreateBlobContainer("made");
        Assert.DoesNotThrow(() => _service.DeleteBlobContainer("made"));
    }

    [Test]
    public void A_Container_Is_Only_The_Place_Its_Blobs_Are_In()
    {
        // Containers are not kept anywhere, so one that was never created is there, and creating it again is not a conflict.
        var never = _service.GetBlobContainerClient("never-created");

        Assert.That(never.Exists().Value, Is.True);
        Assert.That(never.Create().GetRawResponse().Status, Is.EqualTo(201));
        Assert.That(never.Create().GetRawResponse().Status, Is.EqualTo(201));
        // And so there is no list of them to give.
        Assert.Throws<NotSupportedException>(() => _service.GetBlobContainers());
        Assert.Throws<NotSupportedException>(() => _service.GetBlobContainersAsync());
        Assert.Throws<NotSupportedException>(() => _service.GetBlobContainers(BlobContainerTraits.Metadata, prefix: "never"));
    }

    [Test]
    public async Task Downloads_The_Client_Does_Not_Support_Say_So()
    {
        var blob = _container.GetBlobClient("x.txt");
        await blob.UploadAsync(Text("x"), overwrite: true);
        var path = Path.Combine(Path.GetTempPath(), "IciclecreekLuceneBlobs-" + Guid.NewGuid().ToString("N") + ".txt");

        Assert.Multiple(() =>
        {
            Assert.Throws<NotSupportedException>(() => blob.OpenRead());
            Assert.Throws<NotSupportedException>(() => blob.OpenRead(new BlobOpenReadOptions(allowModifications: false)));
            Assert.That(() => blob.OpenReadAsync(), Throws.TypeOf<NotSupportedException>());
            Assert.Throws<NotSupportedException>(() => blob.Download());
            Assert.Throws<NotSupportedException>(() => blob.Download(CancellationToken.None));
            Assert.Throws<NotSupportedException>(() => blob.Download(new HttpRange(0, 1)));
            Assert.That(() => blob.DownloadAsync(), Throws.TypeOf<NotSupportedException>());
            Assert.Throws<NotSupportedException>(() => blob.DownloadTo(path));
            Assert.Throws<NotSupportedException>(() => blob.DownloadTo(path, CancellationToken.None));
            Assert.Throws<NotSupportedException>(() => blob.DownloadTo(path, new BlobDownloadToOptions()));
            Assert.Throws<NotSupportedException>(() => blob.DownloadTo(new MemoryStream(), new BlobDownloadToOptions()));
            Assert.That(() => blob.DownloadToAsync(path), Throws.TypeOf<NotSupportedException>());
        });
        Assert.That(File.Exists(path), Is.False);

        // The downloads it does support are beside them.
        var copy = new MemoryStream();
        blob.DownloadTo(copy);
        Assert.That(Encoding.UTF8.GetString(copy.ToArray()), Is.EqualTo("x"));
        Assert.That(blob.DownloadStreaming().Value.Details.ContentLength, Is.EqualTo(1));
    }

    [Test]
    public async Task Containers_Keep_Their_Blobs_Apart()
    {
        var other = _service.GetBlobContainerClient("other");
        await _container.GetBlobClient("same.txt").UploadAsync(Text("in files"), overwrite: true);
        await other.GetBlobClient("same.txt").UploadAsync(Text("in other"), overwrite: true);

        Assert.That((await _container.GetBlobClient("same.txt").DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("in files"));
        Assert.That((await other.GetBlobClient("same.txt").DownloadContentAsync()).Value.Content.ToString(), Is.EqualTo("in other"));
        await other.DeleteIfExistsAsync();
        Assert.That(_container.GetBlobs().Count(), Is.EqualTo(1));
    }
}
