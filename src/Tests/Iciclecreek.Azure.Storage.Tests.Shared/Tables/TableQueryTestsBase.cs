using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Tables;

[TestFixture]
public abstract class TableQueryTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void Setup() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Helpers ─────────────────────────────────────────────────────────

    protected TableClient SetupPeopleTable()
    {
        var client = _fixture.CreateTableClient("people");
        client.CreateIfNotExists();
        client.AddEntity(new TableEntity("users", "alice") { ["Name"] = "Alice", ["Age"] = 30 });
        client.AddEntity(new TableEntity("users", "bob") { ["Name"] = "Bob", ["Age"] = 25 });
        client.AddEntity(new TableEntity("admins", "carol") { ["Name"] = "Carol", ["Age"] = 40 });
        return client;
    }

    protected async Task<TableClient> SetupPeopleTableAsync()
    {
        var client = _fixture.CreateTableClient("people");
        await client.CreateIfNotExistsAsync();
        await client.AddEntityAsync(new TableEntity("users", "alice") { ["Name"] = "Alice", ["Age"] = 30 });
        await client.AddEntityAsync(new TableEntity("users", "bob") { ["Name"] = "Bob", ["Age"] = 25 });
        await client.AddEntityAsync(new TableEntity("admins", "carol") { ["Name"] = "Carol", ["Age"] = 40 });
        return client;
    }

    // ── Query_All_Returns_All_Entities ──────────────────────────────────

    [Test]
    public void Query_All_Returns_All_Entities()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>().ToList();
        Assert.That(results, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Query_All_Returns_All_Entities_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>()) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(3));
    }

    // ── Query_String_Filter_PartitionKey_Eq ─────────────────────────────

    [Test]
    public void Query_String_Filter_PartitionKey_Eq()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>("PartitionKey eq 'users'").ToList();
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.All(r => r.PartitionKey == "users"), Is.True);
    }

    [Test]
    public async Task Query_String_Filter_PartitionKey_Eq_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("PartitionKey eq 'users'")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.All(r => r.PartitionKey == "users"), Is.True);
    }

    // ── Query_String_Filter_And ─────────────────────────────────────────

    [Test]
    public void Query_String_Filter_And()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>("PartitionKey eq 'users' and RowKey eq 'alice'").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Alice"));
    }

    [Test]
    public async Task Query_String_Filter_And_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("PartitionKey eq 'users' and RowKey eq 'alice'")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Alice"));
    }

    // ── Query_String_Filter_Int_Comparison ──────────────────────────────

    [Test]
    public void Query_String_Filter_Int_Comparison()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>("Age gt 28").ToList();
        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Query_String_Filter_Int_Comparison_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("Age gt 28")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(2));
    }

    // ── Query_Linq_Filter ───────────────────────────────────────────────

    [Test]
    public void Query_Linq_Filter()
    {
        var client = SetupPeopleTable();
        var results = client.Query<TableEntity>(e => e.PartitionKey == "admins").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Carol"));
    }

    [Test]
    public async Task Query_Linq_Filter_Async()
    {
        var client = await SetupPeopleTableAsync();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>(e => e.PartitionKey == "admins")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0]["Name"]?.ToString(), Is.EqualTo("Carol"));
    }

    // ── ServiceClient_Lists_Tables ──────────────────────────────────────

    [Test]
    public void ServiceClient_Lists_Tables()
    {
        var service = _fixture.CreateTableServiceClient();
        service.CreateTable("alpha");
        service.CreateTable("beta");

        var names = service.Query().Select(t => t.Name).OrderBy(n => n).ToArray();
        Assert.That(names, Is.EqualTo(new[] { "alpha", "beta" }));
    }

    [Test]
    public async Task ServiceClient_Lists_Tables_Async()
    {
        var service = _fixture.CreateTableServiceClient();
        service.CreateTable("alpha");
        service.CreateTable("beta");

        var names = new List<string>();
        await foreach (var t in service.QueryAsync()) names.Add(t.Name);
        names.Sort();
        Assert.That(names.ToArray(), Is.EqualTo(new[] { "alpha", "beta" }));
    }

    // ── OData property-type query tests ────────────────────────────────

    private static readonly DateTimeOffset _refDto = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTime _refDt = _refDto.UtcDateTime;
    private static readonly Guid _refGuid = new Guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    private static readonly byte[] _refBytes = new byte[] { 1, 2, 3 };

    protected TableClient SetupTypedTable()
    {
        var client = _fixture.CreateTableClient("typed");
        client.CreateIfNotExists();
        client.AddEntity(new TableEntity("pk", "r1")
        {
            ["Str"]    = "hello",
            ["Int32"]  = (int)10,
            ["Int64"]  = (long)100L,
            ["Dbl"]    = (double)1.5,
            ["Bool"]   = true,
            ["Dt"]     = _refDt,
            ["Dto"]    = _refDto,
            ["Guid"]   = _refGuid,
            ["Bytes"]  = _refBytes,
        });
        client.AddEntity(new TableEntity("pk", "r2")
        {
            ["Str"]    = "world",
            ["Int32"]  = (int)20,
            ["Int64"]  = (long)200L,
            ["Dbl"]    = (double)3.0,
            ["Bool"]   = false,
            ["Dt"]     = _refDt.AddDays(1),
            ["Dto"]    = _refDto.AddDays(1),
            ["Guid"]   = Guid.Empty,
            ["Bytes"]  = new byte[] { 4, 5, 6 },
        });
        return client;
    }

    // string eq / ne

    [Test]
    public void OData_String_Eq()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Str eq 'hello'").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_String_Ne()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Str ne 'hello'").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r2"));
    }

    // int eq / lt / gt / le / ge

    [Test]
    public void OData_Int32_Eq()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Int32 eq 10").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_Int32_Lt()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Int32 lt 20").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_Int32_Gt()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Int32 gt 10").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r2"));
    }

    [Test]
    public void OData_Int32_Le()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Int32 le 10").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_Int32_Ge()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Int32 ge 20").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r2"));
    }

    // long (int64)

    [Test]
    public void OData_Int64_Eq()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Int64 eq 100L").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_Int64_Gt()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Int64 gt 100L").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r2"));
    }

    // double

    [Test]
    public void OData_Double_Eq()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Dbl eq 1.5").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_Double_Lt()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Dbl lt 2.0").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    // bool

    [Test]
    public void OData_Bool_Eq_True()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Bool eq true").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_Bool_Eq_False()
    {
        var client = SetupTypedTable();
        var results = client.Query<TableEntity>("Bool eq false").ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r2"));
    }

    // DateTime

    [Test]
    public void OData_DateTime_Eq()
    {
        var client = SetupTypedTable();
        var filter = $"Dt eq datetime'{_refDt:yyyy-MM-ddTHH:mm:ssZ}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_DateTime_Lt()
    {
        var client = SetupTypedTable();
        var filter = $"Dt lt datetime'{_refDt.AddDays(1):yyyy-MM-ddTHH:mm:ssZ}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_DateTime_Gt()
    {
        var client = SetupTypedTable();
        var filter = $"Dt gt datetime'{_refDt:yyyy-MM-ddTHH:mm:ssZ}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r2"));
    }

    // DateTimeOffset

    [Test]
    public void OData_DateTimeOffset_Eq()
    {
        var client = SetupTypedTable();
        var filter = $"Dto eq datetime'{_refDto:yyyy-MM-ddTHH:mm:ssZ}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_DateTimeOffset_Lt()
    {
        var client = SetupTypedTable();
        var filter = $"Dto lt datetime'{_refDto.AddDays(1):yyyy-MM-ddTHH:mm:ssZ}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    // Guid

    [Test]
    public void OData_Guid_Eq()
    {
        var client = SetupTypedTable();
        var filter = $"Guid eq guid'{_refGuid}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public void OData_Guid_Ne()
    {
        var client = SetupTypedTable();
        var filter = $"Guid ne guid'{_refGuid}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r2"));
    }

    // Binary

    [Test]
    public void OData_Binary_Eq()
    {
        var client = SetupTypedTable();
        var hex = BitConverter.ToString(_refBytes).Replace("-", string.Empty).ToLowerInvariant();
        var filter = $"Bytes eq binary'{hex}'";
        var results = client.Query<TableEntity>(filter).ToList();
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    // Async variants for OData property types

    [Test]
    public async Task OData_String_Eq_Async()
    {
        var client = SetupTypedTable();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("Str eq 'hello'")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public async Task OData_Int32_Eq_Async()
    {
        var client = SetupTypedTable();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("Int32 eq 10")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public async Task OData_Bool_Eq_True_Async()
    {
        var client = SetupTypedTable();
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>("Bool eq true")) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public async Task OData_DateTime_Eq_Async()
    {
        var client = SetupTypedTable();
        var filter = $"Dt eq datetime'{_refDt:yyyy-MM-ddTHH:mm:ssZ}'";
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>(filter)) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }

    [Test]
    public async Task OData_Guid_Eq_Async()
    {
        var client = SetupTypedTable();
        var filter = $"Guid eq guid'{_refGuid}'";
        var results = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>(filter)) results.Add(e);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].RowKey, Is.EqualTo("r1"));
    }
}
