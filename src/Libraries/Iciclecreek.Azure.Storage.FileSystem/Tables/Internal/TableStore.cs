using System.Security.Cryptography;
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Tables.Internal;

internal sealed class TableStore
{
    private readonly string _rootPath;
    internal readonly FileStorageOptions Options;

    public TableStore(string tablesRootPath, string tableName, FileStorageOptions options)
    {
        _rootPath = tablesRootPath;
        TableName = tableName;
        TablePath = Path.Combine(tablesRootPath, tableName);
        Options = options;
    }

    public string TableName { get; }
    public string TablePath { get; }

    public bool TableExists() => Directory.Exists(TablePath);

    public bool CreateTable()
    {
        if (Directory.Exists(TablePath)) return false;
        Directory.CreateDirectory(TablePath);
        return true;
    }

    public bool DeleteTable()
    {
        if (!Directory.Exists(TablePath)) return false;
        Directory.Delete(TablePath, recursive: true);
        return true;
    }

    public string EntityPath(string pk, string rk)
    {
        var encodedPk = TableKeyEncoder.Encode(pk);
        var encodedRk = TableKeyEncoder.Encode(rk);
        return Path.Combine(TablePath, encodedPk, encodedRk + ".json");
    }

    public string GenerateETag()
    {
        return $"0x{Guid.NewGuid():N}";
    }

    public async Task<string> AddEntityAsync(ITableEntity entity, CancellationToken ct = default)
    {
        var path = EntityPath(entity.PartitionKey, entity.RowKey);
        if (File.Exists(path))
            throw new RequestFailedException(409, "Entity already exists.", "EntityAlreadyExists", null);

        var etag = GenerateETag();
        var json = EntitySerializer.Serialize(entity, etag, Options.JsonSerializerOptions);
        await AtomicFile.WriteAllTextAsync(path, json, ct).ConfigureAwait(false);
        return etag;
    }

    public async Task<TableEntity> GetEntityAsync(string pk, string rk, CancellationToken ct = default)
    {
        var path = EntityPath(pk, rk);
        if (!File.Exists(path))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
        var json = await AtomicFile.ReadAllTextAsync(path, ct).ConfigureAwait(false);
        return EntitySerializer.Deserialize(json, Options.JsonSerializerOptions);
    }

    public bool EntityExists(string pk, string rk) => File.Exists(EntityPath(pk, rk));

    public async Task<string> UpsertEntityAsync(ITableEntity entity, TableUpdateMode mode, CancellationToken ct = default)
    {
        var path = EntityPath(entity.PartitionKey, entity.RowKey);
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        var etag = GenerateETag();
        if (mode == TableUpdateMode.Merge && File.Exists(path))
        {
            var existingJson = await AtomicFile.ReadAllTextAsync(path, ct).ConfigureAwait(false);
            var existing = EntitySerializer.Deserialize(existingJson, Options.JsonSerializerOptions);
            var merged = EntitySerializer.MergeEntities(existing, entity);
            var json = EntitySerializer.Serialize(merged, etag, Options.JsonSerializerOptions);
            await AtomicFile.WriteAllTextAsync(path, json, ct).ConfigureAwait(false);
        }
        else
        {
            var json = EntitySerializer.Serialize(entity, etag, Options.JsonSerializerOptions);
            await AtomicFile.WriteAllTextAsync(path, json, ct).ConfigureAwait(false);
        }
        return etag;
    }

    public async Task<string> UpdateEntityAsync(ITableEntity entity, ETag ifMatch, TableUpdateMode mode, CancellationToken ct = default)
    {
        var path = EntityPath(entity.PartitionKey, entity.RowKey);
        if (!File.Exists(path))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        var etag = GenerateETag();

        // Atomic check-and-write: hold exclusive lock for both ETag check and write
        await AtomicFile.ReadCheckWriteAsync(path, existingText =>
        {
            if (ifMatch != ETag.All)
            {
                var existing = EntitySerializer.Deserialize(existingText, Options.JsonSerializerOptions);
                var existingETag = existing["odata.etag"]?.ToString();
                if (existingETag != ifMatch.ToString())
                    throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);
            }

            if (mode == TableUpdateMode.Merge)
            {
                var existing = EntitySerializer.Deserialize(existingText, Options.JsonSerializerOptions);
                var merged = EntitySerializer.MergeEntities(existing, entity);
                return EntitySerializer.Serialize(merged, etag, Options.JsonSerializerOptions);
            }

            return EntitySerializer.Serialize(entity, etag, Options.JsonSerializerOptions);
        }, ct).ConfigureAwait(false);

        return etag;
    }

    public async Task DeleteEntityAsync(string pk, string rk, ETag ifMatch, CancellationToken ct = default)
    {
        var path = EntityPath(pk, rk);
        if (!File.Exists(path))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        if (ifMatch != ETag.All)
        {
            var json = await AtomicFile.ReadAllTextAsync(path, ct).ConfigureAwait(false);
            var existing = EntitySerializer.Deserialize(json, Options.JsonSerializerOptions);
            var existingETag = existing["odata.etag"]?.ToString();
            if (existingETag != ifMatch.ToString())
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);
        }

        File.Delete(path);
    }

    public async IAsyncEnumerable<TableEntity> EnumerateEntitiesAsync()
    {
        if (!Directory.Exists(TablePath)) yield break;

        foreach (var pkDir in Directory.EnumerateDirectories(TablePath).OrderBy(d => d, StringComparer.Ordinal))
        {
            var dirName = Path.GetFileName(pkDir);

            foreach (var file in Directory.EnumerateFiles(pkDir, "*.json").OrderBy(f => f, StringComparer.Ordinal))
            {
                TableEntity? entity = null;
                try
                {
                    var json = await AtomicFile.ReadAllTextAsync(file).ConfigureAwait(false);
                    entity = EntitySerializer.Deserialize(json, Options.JsonSerializerOptions);
                }
                catch { /* skip corrupted files */ }
                if (entity is not null) yield return entity;
            }
        }
    }
}
