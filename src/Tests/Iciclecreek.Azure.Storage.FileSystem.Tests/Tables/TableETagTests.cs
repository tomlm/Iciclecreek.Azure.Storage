using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Tables;

public class TableETagTests : TableETagTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    [Test]
    public async Task An_Update_Cancelled_Between_Its_Check_And_Its_Write_Leaves_The_Entity_Whole()
    {
        // The token is cancelled at the moment the new entity is being made: after the entity's
        // file has been read and its ETag checked, and before it is written back.
        using var cancellation = new CancellationTokenSource();
        var cancelOnWrite = new CancelWhenATextIsWritten(cancellation);
        var options = new FileStorageOptions();
        options.JsonSerializerOptions.Converters.Add(cancelOnWrite);
        var table = new FileTableServiceClient(_fixture.TempPath, options).GetTableClient("etagcancel");
        table.CreateIfNotExists();
        var added = table.AddEntity(new TableEntity("pk", "rk") { ["Name"] = "before" });
        var tableFolder = Path.Combine(((FileStorageTestFixture)_fixture).TablesPath, "etagcancel");
        var file = Directory.EnumerateFiles(tableFolder, "*.json", SearchOption.AllDirectories).Single();

        // A cancelled update may be answered or refused. Either way the entity is one or the other, whole.
        Response? answered = null;
        cancelOnWrite.Armed = true;
        try
        {
            answered = await table.UpdateEntityAsync(new TableEntity("pk", "rk") { ["Name"] = "after" }, added.Headers.ETag!.Value, TableUpdateMode.Replace, cancellation.Token);
        }
        catch (OperationCanceledException)
        {
        }
        cancelOnWrite.Armed = false;

        Assert.That(cancellation.IsCancellationRequested, Is.True, "the cancellation arrived during the update");
        Assert.That(File.ReadAllText(file), Is.Not.Empty, "the entity file was not left with nothing in it");
        var entity = table.GetEntity<TableEntity>("pk", "rk").Value;
        Assert.That(entity.GetString("Name"), Is.EqualTo(answered == null ? "before" : "after"));
        if (answered != null)
            Assert.That(entity.ETag, Is.EqualTo(answered.Headers.ETag), "the file holds the update that was answered");
    }

    /// <summary>Writes text as usual, but cancels first while it is armed.</summary>
    private sealed class CancelWhenATextIsWritten(CancellationTokenSource cancellation) : System.Text.Json.Serialization.JsonConverter<string>
    {
        public bool Armed { get; set; }

        public override string? Read(ref System.Text.Json.Utf8JsonReader reader, Type typeToConvert, System.Text.Json.JsonSerializerOptions options)
            => reader.GetString();

        public override void Write(System.Text.Json.Utf8JsonWriter writer, string value, System.Text.Json.JsonSerializerOptions options)
        {
            if (Armed)
                cancellation.Cancel();
            writer.WriteStringValue(value);
        }
    }
}
