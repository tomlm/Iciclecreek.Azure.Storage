using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory.Tables;

/// <summary>
/// In-memory drop-in replacement for <see cref="Azure.Data.Tables.TableClient"/>.
/// Entities are stored in <see cref="TableStore.Entities"/> as JSON.
/// </summary>
public class MemoryTableClient : TableClient
{
    internal readonly MemoryTableServiceClient _serviceClient;
    internal readonly string _tableName;

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    internal MemoryTableClient(MemoryTableServiceClient serviceClient, string tableName) : base()
    {
        _serviceClient = serviceClient;
        _tableName = tableName;
    }

    /// <inheritdoc/>
    public override string Name => _tableName;
    /// <inheritdoc/>
    public override string AccountName => _serviceClient.AccountName;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_serviceClient.Uri}{_tableName}");

    // ---- Create / Delete ----

    /// <inheritdoc/>
    public override Response<TableItem> Create(CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Tables.TryAdd(_tableName, new TableStore()))
            throw new RequestFailedException(409, "Table already exists.", "TableAlreadyExists", null);
        return Response.FromValue(new TableItem(_tableName), StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
    {
        _serviceClient.Tables.GetOrAdd(_tableName, _ => new TableStore());
        return Response.FromValue(new TableItem(_tableName), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response Delete(CancellationToken cancellationToken = default)
    {
        if (!_serviceClient.Tables.TryRemove(_tableName, out _))
            throw new RequestFailedException(404, "Table not found.", "ResourceNotFound", null);
        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override async Task<Response<TableItem>> CreateAsync(CancellationToken cancellationToken = default)
        => Create(cancellationToken);
    /// <inheritdoc/>
    public override async Task<Response<TableItem>> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        => CreateIfNotExists(cancellationToken);
    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
        => Delete(cancellationToken);

    // ---- Entity CRUD ----

    /// <inheritdoc/>
    public override async Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
    {
        var table = GetTableStore();
        var key = TableStore.EntityKey(entity.PartitionKey, entity.RowKey);
        var entry = new EntityEntry
        {
            PropertiesJson = SerializeProperties(entity)
        };

        if (!table.Entities.TryAdd(key, entry))
            throw new RequestFailedException(409, "Entity already exists.", "EntityAlreadyExists", null);

        return StubResponse.NoContent(entry.ETag);
    }

    /// <inheritdoc/>
    public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
        => AddEntityAsync(entity, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var table = GetTableStore();
        var key = TableStore.EntityKey(partitionKey, rowKey);

        if (!table.Entities.TryGetValue(key, out var entry))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        var te = DeserializeToTableEntity(partitionKey, rowKey, entry.ETag, entry.Timestamp, entry.PropertiesJson);
        return Response.FromValue(ConvertEntity<T>(te), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var table = GetTableStore();
        var key = TableStore.EntityKey(partitionKey, rowKey);

        if (!table.Entities.TryGetValue(key, out var entry))
            return default!;

        var te = DeserializeToTableEntity(partitionKey, rowKey, entry.ETag, entry.Timestamp, entry.PropertiesJson);
        return Response.FromValue<T>(ConvertEntity<T>(te), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityIfExistsAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var table = GetTableStore();
        var key = TableStore.EntityKey(entity.PartitionKey, entity.RowKey);
        string resultETag = null!;

        if (mode == TableUpdateMode.Replace)
        {
            var props = SerializeProperties(entity);
            table.Entities.AddOrUpdate(key,
                _ =>
                {
                    var entry = new EntityEntry { PropertiesJson = props };
                    resultETag = entry.ETag;
                    return entry;
                },
                (_, existing) =>
                {
                    lock (existing.Lock)
                    {
                        existing.PropertiesJson = props;
                        existing.Touch();
                        resultETag = existing.ETag;
                    }
                    return existing;
                });
        }
        else
        {
            // Merge mode
            table.Entities.AddOrUpdate(key,
                _ =>
                {
                    var entry = new EntityEntry { PropertiesJson = SerializeProperties(entity) };
                    resultETag = entry.ETag;
                    return entry;
                },
                (_, existing) =>
                {
                    lock (existing.Lock)
                    {
                        var existingTE = DeserializeToTableEntity(entity.PartitionKey, entity.RowKey, existing.ETag, existing.Timestamp, existing.PropertiesJson);
                        var merged = MergeEntities(existingTE, entity);
                        existing.PropertiesJson = SerializePropertiesFromTableEntity(merged);
                        existing.Touch();
                        resultETag = existing.ETag;
                    }
                    return existing;
                });
        }

        return StubResponse.NoContent(resultETag);
    }

    /// <inheritdoc/>
    public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpsertEntityAsync(entity, mode, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var table = GetTableStore();
        var key = TableStore.EntityKey(entity.PartitionKey, entity.RowKey);

        if (!table.Entities.TryGetValue(key, out var entry))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        string resultETag;
        lock (entry.Lock)
        {
            // ETag check
            if (ifMatch != ETag.All && ifMatch.ToString() != entry.ETag)
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);

            if (mode == TableUpdateMode.Replace)
            {
                entry.PropertiesJson = SerializeProperties(entity);
            }
            else
            {
                var existingTE = DeserializeToTableEntity(entity.PartitionKey, entity.RowKey, entry.ETag, entry.Timestamp, entry.PropertiesJson);
                var merged = MergeEntities(existingTE, entity);
                entry.PropertiesJson = SerializePropertiesFromTableEntity(merged);
            }

            entry.Touch();
            resultETag = entry.ETag;
        }

        return StubResponse.NoContent(resultETag);
    }

    /// <inheritdoc/>
    public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpdateEntityAsync(entity, ifMatch, mode, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        var etag = ifMatch == default ? ETag.All : ifMatch;
        var table = GetTableStore();
        var key = TableStore.EntityKey(partitionKey, rowKey);

        if (!table.Entities.TryGetValue(key, out var entry))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        lock (entry.Lock)
        {
            if (etag != ETag.All && etag.ToString() != entry.ETag)
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);

            if (!table.Entities.TryRemove(key, out _))
                throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
        }

        return StubResponse.NoContent();
    }

    /// <inheritdoc/>
    public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntityAsync(partitionKey, rowKey, ifMatch, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteEntityAsync(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => await DeleteEntityAsync(entity.PartitionKey, entity.RowKey, ifMatch, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response DeleteEntity(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntityAsync(entity, ifMatch, cancellationToken).GetAwaiter().GetResult();

    // ---- Query ----

    /// <inheritdoc/>
    public override AsyncPageable<T> QueryAsync<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var entities = LoadAllEntities();
        var predicate = ODataFilterParser.Parse(filter ?? "");
        var results = entities.Where(e => predicate(e)).Select(ConvertEntity<T>).ToList();
        return new StaticAsyncPageable<T>(new StaticPageable<T>(results));
    }

    /// <inheritdoc/>
    public override Pageable<T> Query<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var entities = LoadAllEntities();
        var predicate = ODataFilterParser.Parse(filter ?? "");
        var results = entities.Where(e => predicate(e)).Select(ConvertEntity<T>).ToList();
        return new StaticPageable<T>(results);
    }

    /// <inheritdoc/>
    public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var compiled = filter.Compile();
        var entities = LoadAllEntities();
        var results = entities.Select(ConvertEntity<T>).Where(e => compiled(e)).ToList();
        return new StaticAsyncPageable<T>(new StaticPageable<T>(results));
    }

    /// <inheritdoc/>
    public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var compiled = filter.Compile();
        var entities = LoadAllEntities();
        var results = entities.Select(ConvertEntity<T>).Where(e => compiled(e)).ToList();
        return new StaticPageable<T>(results);
    }

    // ---- SubmitTransaction ----

    /// <inheritdoc/>
    public override async Task<Response<IReadOnlyList<Response>>> SubmitTransactionAsync(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
    {
        var actions = transactionActions.ToList();
        if (actions.Count == 0)
            throw new ArgumentException("At least one action is required.", nameof(transactionActions));

        var pk = actions[0].Entity.PartitionKey;
        for (var i = 1; i < actions.Count; i++)
        {
            if (actions[i].Entity.PartitionKey != pk)
                throw new RequestFailedException(400, "All entities in a transaction must have the same PartitionKey.", "InvalidInput", null);
        }

        var table = GetTableStore();

        // Snapshot the state of all affected entities so we can rollback on failure.
        var snapshots = new Dictionary<string, EntityEntry?>();
        foreach (var a in actions)
        {
            var key = TableStore.EntityKey(a.Entity.PartitionKey, a.Entity.RowKey);
            if (!snapshots.ContainsKey(key))
            {
                table.Entities.TryGetValue(key, out var existing);
                if (existing != null)
                {
                    // Clone the state
                    snapshots[key] = new EntityEntry
                    {
                        PropertiesJson = existing.PropertiesJson,
                        ETag = existing.ETag,
                        Timestamp = existing.Timestamp
                    };
                }
                else
                {
                    snapshots[key] = null;
                }
            }
        }

        var responses = new List<Response>();
        try
        {
            foreach (var a in actions)
            {
                switch (a.ActionType)
                {
                    case TableTransactionActionType.Add:
                        AddEntityInTransaction(table, a.Entity);
                        break;
                    case TableTransactionActionType.UpdateMerge:
                        UpdateEntityInTransaction(table, a.Entity, a.ETag, TableUpdateMode.Merge);
                        break;
                    case TableTransactionActionType.UpdateReplace:
                        UpdateEntityInTransaction(table, a.Entity, a.ETag, TableUpdateMode.Replace);
                        break;
                    case TableTransactionActionType.UpsertMerge:
                        UpsertEntityInTransaction(table, a.Entity, TableUpdateMode.Merge);
                        break;
                    case TableTransactionActionType.UpsertReplace:
                        UpsertEntityInTransaction(table, a.Entity, TableUpdateMode.Replace);
                        break;
                    case TableTransactionActionType.Delete:
                        DeleteEntityInTransaction(table, a.Entity.PartitionKey, a.Entity.RowKey, a.ETag == default ? ETag.All : a.ETag);
                        break;
                }
                // Include ETag in response for non-delete operations
                var txKey = TableStore.EntityKey(a.Entity.PartitionKey, a.Entity.RowKey);
                if (a.ActionType != TableTransactionActionType.Delete && table.Entities.TryGetValue(txKey, out var txEntry))
                    responses.Add(StubResponse.NoContent(txEntry.ETag));
                else
                    responses.Add(StubResponse.NoContent());
            }
        }
        catch (RequestFailedException ex)
        {
            // Rollback: restore all affected entities to their snapshot state
            Rollback(table, snapshots);
            throw new TableTransactionFailedException(ex);
        }
        catch (Exception ex)
        {
            Rollback(table, snapshots);
            throw new RequestFailedException(500, ex.Message, null, ex);
        }

        IReadOnlyList<Response> list = responses;
        return Response.FromValue(list, StubResponse.Accepted());
    }

    /// <inheritdoc/>
    public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
        => SubmitTransactionAsync(transactionActions, cancellationToken).GetAwaiter().GetResult();

    // ---- Transaction Helpers ----

    private static void Rollback(TableStore table, Dictionary<string, EntityEntry?> snapshots)
    {
        foreach (var kvp in snapshots)
        {
            if (kvp.Value == null)
            {
                // Entity did not exist before — remove it if it was added
                table.Entities.TryRemove(kvp.Key, out _);
            }
            else
            {
                // Restore the entity to its snapshot state
                if (table.Entities.TryGetValue(kvp.Key, out var current))
                {
                    lock (current.Lock)
                    {
                        current.PropertiesJson = kvp.Value.PropertiesJson;
                        current.ETag = kvp.Value.ETag;
                        current.Timestamp = kvp.Value.Timestamp;
                    }
                }
                else
                {
                    // Entity was deleted during transaction — re-add it
                    var restored = new EntityEntry
                    {
                        PropertiesJson = kvp.Value.PropertiesJson,
                        ETag = kvp.Value.ETag,
                        Timestamp = kvp.Value.Timestamp
                    };
                    table.Entities.TryAdd(kvp.Key, restored);
                }
            }
        }
    }

    private void AddEntityInTransaction(TableStore table, ITableEntity entity)
    {
        var key = TableStore.EntityKey(entity.PartitionKey, entity.RowKey);
        var entry = new EntityEntry
        {
            PropertiesJson = SerializeProperties(entity)
        };

        if (!table.Entities.TryAdd(key, entry))
            throw new RequestFailedException(409, "Entity already exists.", "EntityAlreadyExists", null);
    }

    private void UpdateEntityInTransaction(TableStore table, ITableEntity entity, ETag ifMatch, TableUpdateMode mode)
    {
        var key = TableStore.EntityKey(entity.PartitionKey, entity.RowKey);

        if (!table.Entities.TryGetValue(key, out var entry))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        lock (entry.Lock)
        {
            if (ifMatch != ETag.All && ifMatch.ToString() != entry.ETag)
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);

            if (mode == TableUpdateMode.Replace)
            {
                entry.PropertiesJson = SerializeProperties(entity);
            }
            else
            {
                var existingTE = DeserializeToTableEntity(entity.PartitionKey, entity.RowKey, entry.ETag, entry.Timestamp, entry.PropertiesJson);
                var merged = MergeEntities(existingTE, entity);
                entry.PropertiesJson = SerializePropertiesFromTableEntity(merged);
            }

            entry.Touch();
        }
    }

    private void UpsertEntityInTransaction(TableStore table, ITableEntity entity, TableUpdateMode mode)
    {
        var key = TableStore.EntityKey(entity.PartitionKey, entity.RowKey);

        if (mode == TableUpdateMode.Replace)
        {
            var props = SerializeProperties(entity);
            table.Entities.AddOrUpdate(key,
                _ => new EntityEntry { PropertiesJson = props },
                (_, existing) =>
                {
                    lock (existing.Lock)
                    {
                        existing.PropertiesJson = props;
                        existing.Touch();
                    }
                    return existing;
                });
        }
        else
        {
            table.Entities.AddOrUpdate(key,
                _ => new EntityEntry { PropertiesJson = SerializeProperties(entity) },
                (_, existing) =>
                {
                    lock (existing.Lock)
                    {
                        var existingTE = DeserializeToTableEntity(entity.PartitionKey, entity.RowKey, existing.ETag, existing.Timestamp, existing.PropertiesJson);
                        var merged = MergeEntities(existingTE, entity);
                        existing.PropertiesJson = SerializePropertiesFromTableEntity(merged);
                        existing.Touch();
                    }
                    return existing;
                });
        }
    }

    private void DeleteEntityInTransaction(TableStore table, string partitionKey, string rowKey, ETag ifMatch)
    {
        var key = TableStore.EntityKey(partitionKey, rowKey);

        if (!table.Entities.TryGetValue(key, out var entry))
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        lock (entry.Lock)
        {
            if (ifMatch != ETag.All && ifMatch.ToString() != entry.ETag)
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);

            if (!table.Entities.TryRemove(key, out _))
                throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
        }
    }

    // ---- Internal Helpers ----

    private TableStore GetTableStore()
    {
        if (!_serviceClient.Tables.TryGetValue(_tableName, out var table))
            throw new RequestFailedException(404, "Table not found.", "TableNotFound", null);
        return table;
    }

    private List<TableEntity> LoadAllEntities()
    {
        var table = GetTableStore();
        var entities = new List<TableEntity>();

        foreach (var kvp in table.Entities)
        {
            var parts = kvp.Key.Split('\0');
            var pk = parts[0];
            var rk = parts[1];
            var entry = kvp.Value;
            entities.Add(DeserializeToTableEntity(pk, rk, entry.ETag, entry.Timestamp, entry.PropertiesJson));
        }

        return entities;
    }

    private static string SerializeProperties(ITableEntity entity)
    {
        var dict = new Dictionary<string, TypedValue>();
        if (entity is TableEntity te)
        {
            foreach (var kvp in te)
            {
                if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp")
                    continue;
                dict[kvp.Key] = TypedValue.FromObject(kvp.Value);
            }
        }
        else
        {
            // Reflect custom ITableEntity properties
            var type = entity.GetType();
            foreach (var prop in type.GetProperties())
            {
                if (prop.Name is "PartitionKey" or "RowKey" or "ETag" or "Timestamp")
                    continue;
                if (!prop.CanRead) continue;
                var val = prop.GetValue(entity);
                if (val != null)
                    dict[prop.Name] = TypedValue.FromObject(val);
            }
        }
        return JsonSerializer.Serialize(dict, s_jsonOptions);
    }

    private static string SerializePropertiesFromTableEntity(TableEntity entity)
    {
        var dict = new Dictionary<string, TypedValue>();
        foreach (var kvp in entity)
        {
            if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp")
                continue;
            dict[kvp.Key] = TypedValue.FromObject(kvp.Value);
        }
        return JsonSerializer.Serialize(dict, s_jsonOptions);
    }

    private static TableEntity DeserializeToTableEntity(string partitionKey, string rowKey, string etag, DateTimeOffset timestamp, string propsJson)
    {
        var entity = new TableEntity(partitionKey, rowKey)
        {
            Timestamp = timestamp
        };
        entity["odata.etag"] = etag;

        if (!string.IsNullOrWhiteSpace(propsJson))
        {
            var dict = JsonSerializer.Deserialize<Dictionary<string, TypedValue>>(propsJson);
            if (dict != null)
            {
                foreach (var kvp in dict)
                {
                    entity[kvp.Key] = kvp.Value.ToObject();
                }
            }
        }

        return entity;
    }

    private static TableEntity ToTableEntity(ITableEntity entity)
    {
        var te = new TableEntity(entity.PartitionKey, entity.RowKey);
        if (entity is TableEntity source)
        {
            foreach (var kvp in source)
            {
                if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp") continue;
                te[kvp.Key] = kvp.Value;
            }
        }
        else
        {
            var type = entity.GetType();
            foreach (var prop in type.GetProperties())
            {
                if (prop.Name is "PartitionKey" or "RowKey" or "ETag" or "Timestamp") continue;
                if (!prop.CanRead) continue;
                var val = prop.GetValue(entity);
                if (val != null)
                    te[prop.Name] = val;
            }
        }
        return te;
    }

    private static TableEntity MergeEntities(TableEntity existing, ITableEntity incoming)
    {
        var merged = new TableEntity(existing.PartitionKey, existing.RowKey);
        foreach (var kvp in existing)
        {
            if (kvp.Key is "odata.etag" or "Timestamp") continue;
            merged[kvp.Key] = kvp.Value;
        }

        if (incoming is TableEntity te)
        {
            foreach (var kvp in te)
            {
                if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp") continue;
                if (kvp.Value is null)
                    merged.Remove(kvp.Key);
                else
                    merged[kvp.Key] = kvp.Value;
            }
        }
        else
        {
            var type = incoming.GetType();
            foreach (var prop in type.GetProperties())
            {
                if (prop.Name is "PartitionKey" or "RowKey" or "ETag" or "Timestamp") continue;
                if (!prop.CanRead) continue;
                var val = prop.GetValue(incoming);
                if (val != null)
                    merged[prop.Name] = val;
            }
        }
        return merged;
    }

    private static T ConvertEntity<T>(TableEntity entity) where T : class, ITableEntity
    {
        if (typeof(T) == typeof(TableEntity))
            return (entity as T)!;

        var result = (T)Activator.CreateInstance(typeof(T))!;
        result.PartitionKey = entity.PartitionKey;
        result.RowKey = entity.RowKey;
        result.Timestamp = entity.Timestamp;

        if (entity.TryGetValue("odata.etag", out var etagObj) && etagObj is string etagStr)
            result.ETag = new ETag(etagStr);

        var type = typeof(T);
        foreach (var kvp in entity)
        {
            if (kvp.Key is "PartitionKey" or "RowKey" or "Timestamp" or "odata.etag") continue;
            var prop = type.GetProperty(kvp.Key);
            if (prop is not null && prop.CanWrite && kvp.Value is not null)
            {
                try { prop.SetValue(result, Convert.ChangeType(kvp.Value, prop.PropertyType)); }
                catch { }
            }
        }
        return result;
    }

    // ==== Access Policies (stub) ====

    /// <inheritdoc/>
    public override Response<IReadOnlyList<TableSignedIdentifier>> GetAccessPolicies(CancellationToken ct = default)
        => Response.FromValue<IReadOnlyList<TableSignedIdentifier>>(new List<TableSignedIdentifier>(), StubResponse.Ok());
    /// <inheritdoc/>
    public override async Task<Response<IReadOnlyList<TableSignedIdentifier>>> GetAccessPoliciesAsync(CancellationToken ct = default)
        => GetAccessPolicies(ct);
    /// <inheritdoc/>
    public override Response SetAccessPolicy(IEnumerable<TableSignedIdentifier> tableAcl, CancellationToken ct = default)
        => StubResponse.Ok();
    /// <inheritdoc/>
    public override async Task<Response> SetAccessPolicyAsync(IEnumerable<TableSignedIdentifier> tableAcl, CancellationToken ct = default)
        => SetAccessPolicy(tableAcl, ct);

    // ---- Remaining virtual methods ----
    /// <inheritdoc/>
    public override Uri GenerateSasUri(TableSasPermissions permissions, DateTimeOffset expiresOn) => Uri;
    /// <inheritdoc/>
    public override Uri GenerateSasUri(TableSasBuilder builder) => Uri;
    /// <inheritdoc/>
    public override TableSasBuilder GetSasBuilder(TableSasPermissions permissions, DateTimeOffset expiresOn) => new TableSasBuilder(_tableName, permissions, expiresOn);
    /// <inheritdoc/>
    public override TableSasBuilder GetSasBuilder(string rawPermissions, DateTimeOffset expiresOn) => new TableSasBuilder(_tableName, rawPermissions, expiresOn);

    // ---- TypedValue (inline serialization helper matching Sqlite/FileSystem format) ----

    private sealed class TypedValue
    {
        [JsonPropertyName("type")]
        public string Type { get; set; } = "String";

        [JsonPropertyName("value")]
        public string Value { get; set; } = "";

        public static TypedValue FromObject(object? value)
        {
            if (value is null) return new TypedValue { Type = "Null", Value = "" };
            return value switch
            {
                string s => new TypedValue { Type = "String", Value = s },
                int i => new TypedValue { Type = "Int32", Value = i.ToString() },
                long l => new TypedValue { Type = "Int64", Value = l.ToString() },
                double d => new TypedValue { Type = "Double", Value = d.ToString("R") },
                bool b => new TypedValue { Type = "Boolean", Value = b.ToString() },
                DateTimeOffset dto => new TypedValue { Type = "DateTime", Value = dto.UtcDateTime.ToString("O") },
                DateTime dt => new TypedValue { Type = "DateTime", Value = dt.ToUniversalTime().ToString("O") },
                Guid g => new TypedValue { Type = "Guid", Value = g.ToString() },
                byte[] bytes => new TypedValue { Type = "Binary", Value = Convert.ToBase64String(bytes) },
                BinaryData bd => new TypedValue { Type = "Binary", Value = Convert.ToBase64String(bd.ToArray()) },
                _ => new TypedValue { Type = "String", Value = value.ToString() ?? "" },
            };
        }

        public object? ToObject() => Type switch
        {
            "String" => Value,
            "Int32" => int.Parse(Value),
            "Int64" => long.Parse(Value),
            "Double" => double.Parse(Value),
            "Boolean" => bool.Parse(Value),
            "DateTime" => DateTimeOffset.Parse(Value),
            "Guid" => Guid.Parse(Value),
            "Binary" => Convert.FromBase64String(Value),
            "Null" => null,
            _ => Value,
        };
    }
}
