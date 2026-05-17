using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Tables;

[TestFixture]
public abstract class TableETagTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── AddEntity returns ETag ──

    [Test]
    public void AddEntity_Returns_ETag_In_Response()
    {
        var client = _fixture.CreateTableClient("etagadd");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Alice" };
        var response = client.AddEntity(entity);

        var etag = response.Headers.ETag;
        Assert.That(etag, Is.Not.Null, "AddEntity response should contain ETag header");
        Assert.That(etag!.Value.ToString(), Is.Not.Empty, "ETag should not be empty");
    }

    // ── UpsertEntity returns ETag ──

    [Test]
    public async Task UpsertEntity_Returns_ETag_In_Response()
    {
        var client = _fixture.CreateTableClient("etagupsert");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Bob" };
        var response = await client.UpsertEntityAsync(entity, TableUpdateMode.Replace);

        var etag = response.Headers.ETag;
        Assert.That(etag, Is.Not.Null, "UpsertEntity response should contain ETag header");
        Assert.That(etag!.Value.ToString(), Is.Not.Empty);
    }

    // ── UpdateEntity returns ETag ──

    [Test]
    public async Task UpdateEntity_Returns_ETag_In_Response()
    {
        var client = _fixture.CreateTableClient("etagupdate");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Charlie" };
        await client.AddEntityAsync(entity);

        entity["Name"] = "Charlie Updated";
        var response = await client.UpdateEntityAsync(entity, ETag.All, TableUpdateMode.Replace);

        var etag = response.Headers.ETag;
        Assert.That(etag, Is.Not.Null, "UpdateEntity response should contain ETag header");
        Assert.That(etag!.Value.ToString(), Is.Not.Empty);
    }

    // ── ETag changes on every write ──

    [Test]
    public async Task ETag_Changes_On_Every_Write()
    {
        var client = _fixture.CreateTableClient("etagchange");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Value"] = 1 };
        var resp1 = await client.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        var etag1 = resp1.Headers.ETag!.Value.ToString();

        entity["Value"] = 2;
        var resp2 = await client.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        var etag2 = resp2.Headers.ETag!.Value.ToString();

        Assert.That(etag2, Is.Not.EqualTo(etag1), "ETag should change after update");
    }

    // ── GetEntity returns matching ETag ──

    [Test]
    public async Task GetEntity_ETag_Matches_Upsert_Response()
    {
        var client = _fixture.CreateTableClient("etagget");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Diana" };
        var upsertResp = await client.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        var upsertETag = upsertResp.Headers.ETag!.Value.ToString();

        var getResp = await client.GetEntityAsync<TableEntity>("pk", "rk1");
        var getETag = getResp.Value.ETag.ToString();

        Assert.That(getETag, Is.EqualTo(upsertETag),
            "ETag from GetEntity should match the ETag returned by UpsertEntity");
    }

    // ── Conditional update with correct ETag succeeds ──

    [Test]
    public async Task ConditionalUpdate_WithCorrectETag_Succeeds()
    {
        var client = _fixture.CreateTableClient("etagcond");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Eve" };
        var addResp = await client.AddEntityAsync(entity);
        var etag = addResp.Headers.ETag!.Value;

        entity["Name"] = "Eve Updated";
        Assert.DoesNotThrowAsync(async () =>
        {
            await client.UpdateEntityAsync(entity, etag, TableUpdateMode.Replace);
        });
    }

    // ── Conditional update with wrong ETag fails with 412 ──

    [Test]
    public async Task ConditionalUpdate_WithWrongETag_Throws412()
    {
        var client = _fixture.CreateTableClient("etagwrong");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Frank" };
        await client.AddEntityAsync(entity);

        entity["Name"] = "Frank Updated";
        var staleETag = new ETag("0xstale");

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await client.UpdateEntityAsync(entity, staleETag, TableUpdateMode.Replace);
        });
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    // ── Conditional delete with correct ETag succeeds ──

    [Test]
    public async Task ConditionalDelete_WithCorrectETag_Succeeds()
    {
        var client = _fixture.CreateTableClient("etagdel");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Grace" };
        var addResp = await client.AddEntityAsync(entity);
        var etag = addResp.Headers.ETag!.Value;

        Assert.DoesNotThrowAsync(async () =>
        {
            await client.DeleteEntityAsync("pk", "rk1", etag);
        });
    }

    // ── Conditional delete with wrong ETag fails with 412 ──

    [Test]
    public async Task ConditionalDelete_WithWrongETag_Throws412()
    {
        var client = _fixture.CreateTableClient("etagdelwrong");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Hank" };
        await client.AddEntityAsync(entity);

        var staleETag = new ETag("0xstale");
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await client.DeleteEntityAsync("pk", "rk1", staleETag);
        });
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    // ── ETag.All bypasses conditional check ──

    [Test]
    public async Task Update_WithETagAll_AlwaysSucceeds()
    {
        var client = _fixture.CreateTableClient("etagall");
        client.CreateIfNotExists();

        var entity = new TableEntity("pk", "rk1") { ["Name"] = "Ivy" };
        await client.AddEntityAsync(entity);

        entity["Name"] = "Ivy Updated";
        Assert.DoesNotThrowAsync(async () =>
        {
            await client.UpdateEntityAsync(entity, ETag.All, TableUpdateMode.Replace);
        });
    }

    // ── Transaction responses include ETags ──

    [Test]
    public async Task Transaction_Responses_Include_ETags()
    {
        var client = _fixture.CreateTableClient("etagtx");
        client.CreateIfNotExists();

        var actions = new[]
        {
            new TableTransactionAction(TableTransactionActionType.UpsertReplace,
                new TableEntity("pk", "rk1") { ["Name"] = "Tx1" }),
            new TableTransactionAction(TableTransactionActionType.UpsertReplace,
                new TableEntity("pk", "rk2") { ["Name"] = "Tx2" }),
        };

        var responses = await client.SubmitTransactionAsync(actions);

        Assert.That(responses.Value.Count, Is.EqualTo(2));
        foreach (var resp in responses.Value)
        {
            var etag = resp.Headers.ETag;
            Assert.That(etag, Is.Not.Null, "Transaction response should include ETag");
            Assert.That(etag!.Value.ToString(), Is.Not.Empty);
        }
    }

    // ── ETag roundtrip: upsert → get → conditional update ──

    [Test]
    public async Task ETag_Roundtrip_Upsert_Get_ConditionalUpdate()
    {
        var client = _fixture.CreateTableClient("etagrt");
        client.CreateIfNotExists();

        // Upsert
        var entity = new TableEntity("pk", "rk1") { ["Counter"] = 1 };
        await client.UpsertEntityAsync(entity, TableUpdateMode.Replace);

        // Get (captures ETag)
        var getResp = await client.GetEntityAsync<TableEntity>("pk", "rk1");
        var etag = getResp.Value.ETag;
        Assert.That(etag, Is.Not.EqualTo(default(ETag)));

        // Conditional update with captured ETag
        var updated = getResp.Value;
        updated["Counter"] = 2;
        var updateResp = await client.UpdateEntityAsync(updated, etag, TableUpdateMode.Replace);

        // New ETag should differ
        var newETag = updateResp.Headers.ETag!.Value;
        Assert.That(newETag.ToString(), Is.Not.EqualTo(etag.ToString()),
            "ETag should change after conditional update");

        // Verify updated value
        var verify = await client.GetEntityAsync<TableEntity>("pk", "rk1");
        Assert.That(Convert.ToInt32(verify.Value["Counter"]), Is.EqualTo(2));
    }
}
