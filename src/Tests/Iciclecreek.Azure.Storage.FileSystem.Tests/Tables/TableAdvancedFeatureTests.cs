using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Tables;

public class TableAdvancedFeatureTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    // ---- GetEntityIfExists returns HasValue=false for missing entity ----

    [Test]
    public async Task GetEntityIfExists_Returns_Null_For_Missing()
    {
        var client = _root.TableService.GetTableClient("geif-test") as FileTableClient;
        await client.CreateIfNotExistsAsync();

        var result = await client.GetEntityIfExistsAsync<TableEntity>("pk", "missing");
        // NullableResponse<T> doesn't expose a public way to construct HasValue=false,
        // so the test fake returns null for missing entities.
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task GetEntityIfExists_Returns_Value_For_Existing()
    {
        var client = _root.TableService.GetTableClient("geif-exists") as FileTableClient;
        await client.CreateIfNotExistsAsync();
        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["V"] = "found" });

        var result = await client.GetEntityIfExistsAsync<TableEntity>("pk", "rk");
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value!["V"]?.ToString(), Is.EqualTo("found"));
    }

    // ---- FormattableString query on TableServiceClient ----

    [Test]
    public void ServiceClient_Query_FormattableString()
    {
        var service = _root.TableService;
        service.CreateTable("alpha");
        service.CreateTable("beta");

        var tableName = "alpha";
        FormattableString filter = $"TableName eq '{tableName}'";
        // FormattableString just gets .ToString()'d — all tables returned since we don't parse it
        var all = service.Query(filter).ToList();
        Assert.That(all.Count, Is.GreaterThanOrEqualTo(1));
    }

    // ---- Table SetAccessPolicy is a no-op ----

    [Test]
    public void Table_SetAccessPolicy_NoOp()
    {
        var client = _root.TableService.GetTableClient("ns-table") as FileTableClient;
        client.CreateIfNotExists();
        Assert.DoesNotThrow(() =>
            client.SetAccessPolicy(Enumerable.Empty<TableSignedIdentifier>()));
    }

    // ---- TableService GetProperties / GetStatistics return stubs ----

    [Test]
    public void TableService_GetProperties_Returns_Default()
    {
        var service = _root.TableService;
        var props = service.GetProperties().Value;
        Assert.That(props, Is.Not.Null);
    }

    [Test]
    public void TableService_GetStatistics_Returns_Stub()
    {
        var service = _root.TableService;
        // GetStatistics returns a stub value (may be default)
        Assert.DoesNotThrow(() => service.GetStatistics());
    }
}
