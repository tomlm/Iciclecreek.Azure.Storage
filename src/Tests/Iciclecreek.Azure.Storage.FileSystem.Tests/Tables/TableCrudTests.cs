using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Tables;

public class TableCrudTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void AddEntity_And_GetEntity_Roundtrip()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("people");
        client.CreateIfNotExists();

        var entity = new TableEntity("users", "alice")
        {
            ["Name"] = "Alice",
            ["Age"] = 30,
        };
        client.AddEntity(entity);

        var result = client.GetEntity<TableEntity>("users", "alice").Value;
        Assert.That(result["Name"]?.ToString(), Is.EqualTo("Alice"));
        Assert.That(Convert.ToInt32(result["Age"]), Is.EqualTo(30));
    }

    [Test]
    public void AddEntity_Creates_File_On_Disk()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("disk-table");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["X"] = "Y" });

        var path = Path.Combine(_root.TablesPath, "disk-table", "pk", "rk.json");
        Assert.That(File.Exists(path), Is.True);
    }

    [Test]
    public void AddEntity_Duplicate_Throws_409()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("dup-table");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk"));
        var ex = Assert.Throws<RequestFailedException>(() =>
            client.AddEntity(new TableEntity("pk", "rk")));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public void UpsertEntity_Replace_Overwrites()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("upsert");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["A"] = "1", ["B"] = "2" });
        client.UpsertEntity(new TableEntity("pk", "rk") { ["A"] = "replaced" }, TableUpdateMode.Replace);

        var result = client.GetEntity<TableEntity>("pk", "rk").Value;
        Assert.That(result["A"]?.ToString(), Is.EqualTo("replaced"));
        Assert.That(result.ContainsKey("B"), Is.False);
    }

    [Test]
    public void UpsertEntity_Merge_Preserves_Existing_Properties()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("merge");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["A"] = "1", ["B"] = "2" });
        client.UpsertEntity(new TableEntity("pk", "rk") { ["A"] = "updated" }, TableUpdateMode.Merge);

        var result = client.GetEntity<TableEntity>("pk", "rk").Value;
        Assert.That(result["A"]?.ToString(), Is.EqualTo("updated"));
        Assert.That(result["B"]?.ToString(), Is.EqualTo("2"));
    }

    [Test]
    public void DeleteEntity_Removes_Entity()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("del-table");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk"));
        client.DeleteEntity("pk", "rk");

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.GetEntity<TableEntity>("pk", "rk"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public void UpdateEntity_With_Stale_ETag_Throws_412()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-table");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["V"] = "1" });
        var entity1 = client.GetEntity<TableEntity>("pk", "rk").Value;
        var etag1 = entity1.ETag;

        // Change the entity to move the ETag forward.
        client.UpsertEntity(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.UpdateEntity(new TableEntity("pk", "rk") { ["V"] = "3" }, etag1, TableUpdateMode.Replace));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public void UpdateEntity_With_ETag_All_Bypasses_Check()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-all");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["V"] = "1" });
        client.UpsertEntity(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);

        Assert.DoesNotThrow(() =>
            client.UpdateEntity(new TableEntity("pk", "rk") { ["V"] = "3" }, ETag.All, TableUpdateMode.Replace));
    }

    [Test]
    public void ETag_Changes_On_Every_Write()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-track");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["V"] = "1" });
        var etag1 = client.GetEntity<TableEntity>("pk", "rk").Value.ETag;

        client.UpsertEntity(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);
        var etag2 = client.GetEntity<TableEntity>("pk", "rk").Value.ETag;

        client.UpsertEntity(new TableEntity("pk", "rk") { ["V"] = "3" }, TableUpdateMode.Replace);
        var etag3 = client.GetEntity<TableEntity>("pk", "rk").Value.ETag;

        Assert.That(etag1, Is.Not.EqualTo(etag2));
        Assert.That(etag2, Is.Not.EqualTo(etag3));
    }

    [Test]
    public void Optimistic_Concurrency_ReadModifyWrite()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-optimistic");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["Counter"] = 0 });

        // Reader 1 fetches the entity.
        var entity1 = client.GetEntity<TableEntity>("pk", "rk").Value;
        var etag1 = entity1.ETag;

        // Reader 2 fetches the same entity.
        var entity2 = client.GetEntity<TableEntity>("pk", "rk").Value;
        var etag2 = entity2.ETag;

        // Reader 1 updates successfully.
        client.UpdateEntity(
            new TableEntity("pk", "rk") { ["Counter"] = 1 },
            etag1, TableUpdateMode.Replace);

        // Reader 2's update should fail — stale ETag.
        var ex = Assert.Throws<RequestFailedException>(() =>
            client.UpdateEntity(
                new TableEntity("pk", "rk") { ["Counter"] = 1 },
                etag2, TableUpdateMode.Replace));
        Assert.That(ex!.Status, Is.EqualTo(412));

        // Value should reflect Reader 1's write only.
        var final = client.GetEntity<TableEntity>("pk", "rk").Value;
        Assert.That(Convert.ToInt32(final["Counter"]), Is.EqualTo(1));
    }

    [Test]
    public void DeleteEntity_With_Stale_ETag_Throws_412()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-del");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk") { ["V"] = "1" });
        var staleETag = client.GetEntity<TableEntity>("pk", "rk").Value.ETag;

        client.UpsertEntity(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.DeleteEntity("pk", "rk", staleETag));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public void DeleteEntity_With_Correct_ETag_Succeeds()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-del-ok");
        client.CreateIfNotExists();

        client.AddEntity(new TableEntity("pk", "rk"));
        var currentETag = client.GetEntity<TableEntity>("pk", "rk").Value.ETag;

        Assert.DoesNotThrow(() => client.DeleteEntity("pk", "rk", currentETag));

        var ex = Assert.Throws<RequestFailedException>(() =>
            client.GetEntity<TableEntity>("pk", "rk"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    // ───────────────────── Async counterparts ─────────────────────

    [Test]
    public async Task AddEntity_And_GetEntity_Roundtrip_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("people");
        await client.CreateIfNotExistsAsync();

        var entity = new TableEntity("users", "alice")
        {
            ["Name"] = "Alice",
            ["Age"] = 30,
        };
        await client.AddEntityAsync(entity);

        var result = (await client.GetEntityAsync<TableEntity>("users", "alice")).Value;
        Assert.That(result["Name"]?.ToString(), Is.EqualTo("Alice"));
        Assert.That(Convert.ToInt32(result["Age"]), Is.EqualTo(30));
    }

    [Test]
    public async Task AddEntity_Creates_File_On_Disk_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("disk-table");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["X"] = "Y" });

        var path = Path.Combine(_root.TablesPath, "disk-table", "pk", "rk.json");
        Assert.That(File.Exists(path), Is.True);
    }

    [Test]
    public async Task AddEntity_Duplicate_Throws_409_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("dup-table");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk"));
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.AddEntityAsync(new TableEntity("pk", "rk")));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public async Task UpsertEntity_Replace_Overwrites_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("upsert");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["A"] = "1", ["B"] = "2" });
        await client.UpsertEntityAsync(new TableEntity("pk", "rk") { ["A"] = "replaced" }, TableUpdateMode.Replace);

        var result = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value;
        Assert.That(result["A"]?.ToString(), Is.EqualTo("replaced"));
        Assert.That(result.ContainsKey("B"), Is.False);
    }

    [Test]
    public async Task UpsertEntity_Merge_Preserves_Existing_Properties_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("merge");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["A"] = "1", ["B"] = "2" });
        await client.UpsertEntityAsync(new TableEntity("pk", "rk") { ["A"] = "updated" }, TableUpdateMode.Merge);

        var result = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value;
        Assert.That(result["A"]?.ToString(), Is.EqualTo("updated"));
        Assert.That(result["B"]?.ToString(), Is.EqualTo("2"));
    }

    [Test]
    public async Task DeleteEntity_Removes_Entity_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("del-table");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk"));
        await client.DeleteEntityAsync("pk", "rk");

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.GetEntityAsync<TableEntity>("pk", "rk"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }

    [Test]
    public async Task UpdateEntity_With_Stale_ETag_Throws_412_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-table");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["V"] = "1" });
        var entity1 = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value;
        var etag1 = entity1.ETag;

        // Change the entity to move the ETag forward.
        await client.UpsertEntityAsync(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UpdateEntityAsync(new TableEntity("pk", "rk") { ["V"] = "3" }, etag1, TableUpdateMode.Replace));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task UpdateEntity_With_ETag_All_Bypasses_Check_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-all");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["V"] = "1" });
        await client.UpsertEntityAsync(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);

        Assert.DoesNotThrowAsync(async () =>
            await client.UpdateEntityAsync(new TableEntity("pk", "rk") { ["V"] = "3" }, ETag.All, TableUpdateMode.Replace));
    }

    [Test]
    public async Task ETag_Changes_On_Every_Write_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-track");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["V"] = "1" });
        var etag1 = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value.ETag;

        await client.UpsertEntityAsync(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);
        var etag2 = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value.ETag;

        await client.UpsertEntityAsync(new TableEntity("pk", "rk") { ["V"] = "3" }, TableUpdateMode.Replace);
        var etag3 = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value.ETag;

        Assert.That(etag1, Is.Not.EqualTo(etag2));
        Assert.That(etag2, Is.Not.EqualTo(etag3));
    }

    [Test]
    public async Task Optimistic_Concurrency_ReadModifyWrite_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-optimistic");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["Counter"] = 0 });

        // Reader 1 fetches the entity.
        var entity1 = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value;
        var etag1 = entity1.ETag;

        // Reader 2 fetches the same entity.
        var entity2 = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value;
        var etag2 = entity2.ETag;

        // Reader 1 updates successfully.
        await client.UpdateEntityAsync(
            new TableEntity("pk", "rk") { ["Counter"] = 1 },
            etag1, TableUpdateMode.Replace);

        // Reader 2's update should fail — stale ETag.
        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.UpdateEntityAsync(
                new TableEntity("pk", "rk") { ["Counter"] = 1 },
                etag2, TableUpdateMode.Replace));
        Assert.That(ex!.Status, Is.EqualTo(412));

        // Value should reflect Reader 1's write only.
        var final_ = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value;
        Assert.That(Convert.ToInt32(final_["Counter"]), Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteEntity_With_Stale_ETag_Throws_412_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-del");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk") { ["V"] = "1" });
        var staleETag = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value.ETag;

        await client.UpsertEntityAsync(new TableEntity("pk", "rk") { ["V"] = "2" }, TableUpdateMode.Replace);

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.DeleteEntityAsync("pk", "rk", staleETag));
        Assert.That(ex!.Status, Is.EqualTo(412));
    }

    [Test]
    public async Task DeleteEntity_With_Correct_ETag_Succeeds_Async()
    {
        var client = (FileTableClient)_root.TableService.GetTableClient("etag-del-ok");
        await client.CreateIfNotExistsAsync();

        await client.AddEntityAsync(new TableEntity("pk", "rk"));
        var currentETag = (await client.GetEntityAsync<TableEntity>("pk", "rk")).Value.ETag;

        Assert.DoesNotThrowAsync(async () => await client.DeleteEntityAsync("pk", "rk", currentETag));

        var ex = Assert.ThrowsAsync<RequestFailedException>(async () =>
            await client.GetEntityAsync<TableEntity>("pk", "rk"));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }
}
