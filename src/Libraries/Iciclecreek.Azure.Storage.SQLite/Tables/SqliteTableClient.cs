using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Iciclecreek.Azure.Storage.SQLite.Internal;
using Microsoft.Data.Sqlite;

namespace Iciclecreek.Azure.Storage.SQLite.Tables;

/// <summary>
/// SQLite-backed drop-in replacement for <see cref="Azure.Data.Tables.TableClient"/>.
/// Each Azure table is a separate SQLite table with dynamic columns for queryable properties.
/// </summary>
public class SqliteTableClient : TableClient
{
    internal readonly SqliteTableServiceClient _serviceClient;
    internal readonly string _tableName;

    // Cache of known columns per table to avoid repeated PRAGMA lookups
    private static readonly ConcurrentDictionary<string, HashSet<string>> s_columnCache = new();

    private static readonly HashSet<string> s_fixedColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "PartitionKey", "RowKey", "_etag", "_timestamp", "_properties"
    };

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    internal SqliteTableClient(SqliteTableServiceClient serviceClient, string tableName) : base()
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
        using var conn = _serviceClient.Db.Open();
        using var tx = conn.BeginTransaction();

        // Register in Tables metadata
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO Tables (Name) VALUES (@name)";
        cmd.Parameters.AddWithValue("@name", _tableName);
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            throw new RequestFailedException(409, "Table already exists.", "TableAlreadyExists", null);
        }

        // Create the per-table SQLite table
        _serviceClient.Db.CreateEntityTable(conn, _tableName, tx);
        tx.Commit();

        return Response.FromValue(new TableItem(_tableName), StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var tx = conn.BeginTransaction();

        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT OR IGNORE INTO Tables (Name) VALUES (@name)";
        cmd.Parameters.AddWithValue("@name", _tableName);
        cmd.ExecuteNonQuery();

        _serviceClient.Db.CreateEntityTable(conn, _tableName, tx);
        tx.Commit();

        return Response.FromValue(new TableItem(_tableName), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response Delete(CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        using var tx = conn.BeginTransaction();

        // Drop the entity table
        _serviceClient.Db.DropEntityTable(conn, _tableName, tx);

        // Remove from Tables metadata
        using var delTable = conn.CreateCommand();
        delTable.Transaction = tx;
        delTable.CommandText = "DELETE FROM Tables WHERE Name = @name";
        delTable.Parameters.AddWithValue("@name", _tableName);
        var rows = delTable.ExecuteNonQuery();

        tx.Commit();

        // Clear column cache
        s_columnCache.TryRemove(ColumnCacheKey, out _);

        if (rows == 0)
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
        var etag = NewETag();
        var timestamp = DateTimeOffset.UtcNow;

        using var conn = _serviceClient.Db.Open();
        var properties = ExtractProperties(entity);
        EnsureColumns(conn, properties);

        var (sql, parameters) = BuildInsertSql(entity.PartitionKey, entity.RowKey, etag, timestamp, properties);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);

        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            throw new RequestFailedException(409, "Entity already exists.", "EntityAlreadyExists", null);
        }

        return StubResponse.NoContent(etag);
    }

    /// <inheritdoc/>
    public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
        => AddEntityAsync(entity, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        var entity = ReadEntity(conn, partitionKey, rowKey);
        if (entity == null)
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
        return Response.FromValue(ConvertEntity<T>(entity), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        using var conn = _serviceClient.Db.Open();
        var entity = ReadEntity(conn, partitionKey, rowKey);
        if (entity == null)
            return default!;
        return Response.FromValue<T>(ConvertEntity<T>(entity), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityIfExistsAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var etag = NewETag();
        var timestamp = DateTimeOffset.UtcNow;

        using var conn = _serviceClient.Db.Open();

        if (mode == TableUpdateMode.Replace)
        {
            var properties = ExtractProperties(entity);
            EnsureColumns(conn, properties);
            var (sql, parameters) = BuildUpsertSql(entity.PartitionKey, entity.RowKey, etag, timestamp, properties);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.ExecuteNonQuery();
        }
        else
        {
            // Merge mode: read existing, merge, then write
            var existing = ReadEntity(conn, entity.PartitionKey, entity.RowKey);
            TableEntity merged = existing != null ? MergeEntities(existing, entity) : ToTableEntity(entity);
            var properties = ExtractPropertiesFromTableEntity(merged);
            EnsureColumns(conn, properties);
            var (sql, parameters) = BuildUpsertSql(entity.PartitionKey, entity.RowKey, etag, timestamp, properties);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            foreach (var p in parameters) cmd.Parameters.Add(p);
            cmd.ExecuteNonQuery();
        }

        return StubResponse.NoContent(etag);
    }

    /// <inheritdoc/>
    public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpsertEntityAsync(entity, mode, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var etag = NewETag();
        var timestamp = DateTimeOffset.UtcNow;

        using var conn = _serviceClient.Db.Open();
        var existing = ReadEntity(conn, entity.PartitionKey, entity.RowKey);
        if (existing == null)
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        var existingETag = existing.TryGetValue("odata.etag", out var eObj) && eObj is string es ? es : "";
        if (ifMatch != ETag.All && ifMatch.ToString() != existingETag)
            throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);

        TableEntity updated = mode == TableUpdateMode.Replace ? ToTableEntity(entity) : MergeEntities(existing, entity);
        var properties = ExtractPropertiesFromTableEntity(updated);
        EnsureColumns(conn, properties);

        var (sql, parameters) = BuildUpdateSql(entity.PartitionKey, entity.RowKey, etag, timestamp, properties);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        cmd.ExecuteNonQuery();

        return StubResponse.NoContent(etag);
    }

    /// <inheritdoc/>
    public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpdateEntityAsync(entity, ifMatch, mode, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        var etag = ifMatch == default ? ETag.All : ifMatch;

        using var conn = _serviceClient.Db.Open();

        if (etag != ETag.All)
        {
            var existing = ReadEntity(conn, partitionKey, rowKey);
            if (existing == null)
                throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
            var existingETag = existing.TryGetValue("odata.etag", out var eObj) && eObj is string es ? es : "";
            if (etag.ToString() != existingETag)
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM [{_tableName}] WHERE PartitionKey = @pk AND RowKey = @rk";
        cmd.Parameters.AddWithValue("@pk", partitionKey);
        cmd.Parameters.AddWithValue("@rk", rowKey);
        var rows = cmd.ExecuteNonQuery();

        if (rows == 0)
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

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
        => new StaticAsyncPageable<T>(new StaticPageable<T>(QueryCore<T>(filter)));

    /// <inheritdoc/>
    public override Pageable<T> Query<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => new StaticPageable<T>(QueryCore<T>(filter));

    /// <inheritdoc/>
    public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var odata = TableClient.CreateQueryFilter(filter);
        return new StaticAsyncPageable<T>(new StaticPageable<T>(QueryCore<T>(odata)));
    }

    /// <inheritdoc/>
    public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var odata = TableClient.CreateQueryFilter(filter);
        return new StaticPageable<T>(QueryCore<T>(odata));
    }

    private List<T> QueryCore<T>(string? filter) where T : class, ITableEntity
    {
        using var conn = _serviceClient.Db.Open();

        // Check if the filter references columns that don't exist
        if (!string.IsNullOrWhiteSpace(filter))
        {
            var referencedColumns = ODataToSqlTranslator.ExtractColumnNames(filter);
            var knownColumns = GetKnownColumns(conn);
            foreach (var col in referencedColumns)
            {
                if (!knownColumns.Contains(col))
                    return new List<T>(); // Column doesn't exist → no matches
            }
        }

        var (sqlFilter, sqlParams) = ODataToSqlTranslator.Translate(filter ?? "");

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT PartitionKey, RowKey, _etag, _timestamp, _properties FROM [{_tableName}] WHERE {sqlFilter}";
        foreach (var p in sqlParams) cmd.Parameters.Add(p);

        var results = new List<T>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var pk = reader.GetString(0);
            var rk = reader.GetString(1);
            var etag = reader.GetString(2);
            var timestamp = DateTimeOffset.Parse(reader.GetString(3));
            var propsJson = reader.GetString(4);
            var entity = DeserializeToTableEntity(pk, rk, etag, timestamp, propsJson);
            results.Add(ConvertEntity<T>(entity));
        }
        return results;
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

        using var conn = _serviceClient.Db.Open();
        using var tx = conn.BeginTransaction();

        var responses = new List<Response>();
        try
        {
            foreach (var a in actions)
            {
                string? txEtag = null;
                switch (a.ActionType)
                {
                    case TableTransactionActionType.Add:
                        txEtag = AddEntityInTransaction(conn, tx, a.Entity);
                        break;
                    case TableTransactionActionType.UpdateMerge:
                        txEtag = UpdateEntityInTransaction(conn, tx, a.Entity, a.ETag, TableUpdateMode.Merge);
                        break;
                    case TableTransactionActionType.UpdateReplace:
                        txEtag = UpdateEntityInTransaction(conn, tx, a.Entity, a.ETag, TableUpdateMode.Replace);
                        break;
                    case TableTransactionActionType.UpsertMerge:
                        txEtag = UpsertEntityInTransaction(conn, tx, a.Entity, TableUpdateMode.Merge);
                        break;
                    case TableTransactionActionType.UpsertReplace:
                        txEtag = UpsertEntityInTransaction(conn, tx, a.Entity, TableUpdateMode.Replace);
                        break;
                    case TableTransactionActionType.Delete:
                        DeleteEntityInTransaction(conn, tx, a.Entity.PartitionKey, a.Entity.RowKey, a.ETag == default ? ETag.All : a.ETag);
                        break;
                }
                responses.Add(txEtag != null ? StubResponse.NoContent(txEtag) : StubResponse.NoContent());
            }

            tx.Commit();
        }
        catch (RequestFailedException ex)
        {
            tx.Rollback();
            throw new TableTransactionFailedException(ex);
        }
        catch (Exception ex)
        {
            tx.Rollback();
            throw new RequestFailedException(500, ex.Message, null, ex);
        }

        IReadOnlyList<Response> list = responses;
        return Response.FromValue(list, StubResponse.Accepted());
    }

    /// <inheritdoc/>
    public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
        => SubmitTransactionAsync(transactionActions, cancellationToken).GetAwaiter().GetResult();

    // ---- Transaction Helpers ----

    private string AddEntityInTransaction(SqliteConnection conn, SqliteTransaction tx, ITableEntity entity)
    {
        var etag = NewETag();
        var timestamp = DateTimeOffset.UtcNow;
        var properties = ExtractProperties(entity);
        EnsureColumns(conn, properties, tx);

        var (sql, parameters) = BuildInsertSql(entity.PartitionKey, entity.RowKey, etag, timestamp, properties);
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);

        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            throw new RequestFailedException(409, "Entity already exists.", "EntityAlreadyExists", null);
        }
        return etag;
    }

    private string UpdateEntityInTransaction(SqliteConnection conn, SqliteTransaction tx, ITableEntity entity, ETag ifMatch, TableUpdateMode mode)
    {
        var existing = ReadEntity(conn, entity.PartitionKey, entity.RowKey, tx);
        if (existing == null)
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);

        var existingETag = existing.TryGetValue("odata.etag", out var eObj) && eObj is string es ? es : "";
        if (ifMatch != ETag.All && ifMatch.ToString() != existingETag)
            throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);

        var etag = NewETag();
        var timestamp = DateTimeOffset.UtcNow;
        TableEntity updated = mode == TableUpdateMode.Replace ? ToTableEntity(entity) : MergeEntities(existing, entity);
        var properties = ExtractPropertiesFromTableEntity(updated);
        EnsureColumns(conn, properties, tx);

        var (sql, parameters) = BuildUpdateSql(entity.PartitionKey, entity.RowKey, etag, timestamp, properties);
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        cmd.ExecuteNonQuery();
        return etag;
    }

    private string UpsertEntityInTransaction(SqliteConnection conn, SqliteTransaction tx, ITableEntity entity, TableUpdateMode mode)
    {
        var etag = NewETag();
        var timestamp = DateTimeOffset.UtcNow;

        Dictionary<string, (TypedValue Typed, object? SqlValue)> properties;
        if (mode == TableUpdateMode.Replace)
        {
            properties = ExtractProperties(entity);
        }
        else
        {
            var existing = ReadEntity(conn, entity.PartitionKey, entity.RowKey, tx);
            TableEntity merged = existing != null ? MergeEntities(existing, entity) : ToTableEntity(entity);
            properties = ExtractPropertiesFromTableEntity(merged);
        }

        EnsureColumns(conn, properties, tx);
        var (sql, parameters) = BuildUpsertSql(entity.PartitionKey, entity.RowKey, etag, timestamp, properties);
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(p);
        cmd.ExecuteNonQuery();
        return etag;
    }

    private void DeleteEntityInTransaction(SqliteConnection conn, SqliteTransaction tx, string partitionKey, string rowKey, ETag ifMatch)
    {
        if (ifMatch != ETag.All)
        {
            var existing = ReadEntity(conn, partitionKey, rowKey, tx);
            if (existing == null)
                throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
            var existingETag = existing.TryGetValue("odata.etag", out var eObj) && eObj is string es ? es : "";
            if (ifMatch.ToString() != existingETag)
                throw new RequestFailedException(412, "ETag mismatch.", "UpdateConditionNotSatisfied", null);
        }

        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"DELETE FROM [{_tableName}] WHERE PartitionKey = @pk AND RowKey = @rk";
        cmd.Parameters.AddWithValue("@pk", partitionKey);
        cmd.Parameters.AddWithValue("@rk", rowKey);
        var rows = cmd.ExecuteNonQuery();

        if (rows == 0)
            throw new RequestFailedException(404, "Entity not found.", "ResourceNotFound", null);
    }

    // ---- SQL Builders ----

    private (string Sql, List<SqliteParameter> Parameters) BuildInsertSql(
        string pk, string rk, string etag, DateTimeOffset timestamp,
        Dictionary<string, (TypedValue Typed, object? SqlValue)> properties)
    {
        var columns = new List<string> { "PartitionKey", "RowKey", "_etag", "_timestamp", "_properties" };
        var paramNames = new List<string> { "@pk", "@rk", "@etag", "@ts", "@props" };
        var parameters = new List<SqliteParameter>
        {
            new("@pk", pk),
            new("@rk", rk),
            new("@etag", etag),
            new("@ts", timestamp.ToString("O")),
            new("@props", SerializeTypedValues(properties)),
        };

        int i = 0;
        foreach (var kvp in properties)
        {
            columns.Add($"[{kvp.Key}]");
            var pName = $"@v{i++}";
            paramNames.Add(pName);
            parameters.Add(new SqliteParameter(pName, kvp.Value.SqlValue ?? DBNull.Value));
        }

        var sql = $"INSERT INTO [{_tableName}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", paramNames)})";
        return (sql, parameters);
    }

    private (string Sql, List<SqliteParameter> Parameters) BuildUpsertSql(
        string pk, string rk, string etag, DateTimeOffset timestamp,
        Dictionary<string, (TypedValue Typed, object? SqlValue)> properties)
    {
        var columns = new List<string> { "PartitionKey", "RowKey", "_etag", "_timestamp", "_properties" };
        var paramNames = new List<string> { "@pk", "@rk", "@etag", "@ts", "@props" };
        var parameters = new List<SqliteParameter>
        {
            new("@pk", pk),
            new("@rk", rk),
            new("@etag", etag),
            new("@ts", timestamp.ToString("O")),
            new("@props", SerializeTypedValues(properties)),
        };

        int i = 0;
        foreach (var kvp in properties)
        {
            columns.Add($"[{kvp.Key}]");
            var pName = $"@v{i++}";
            paramNames.Add(pName);
            parameters.Add(new SqliteParameter(pName, kvp.Value.SqlValue ?? DBNull.Value));
        }

        var sql = $"INSERT OR REPLACE INTO [{_tableName}] ({string.Join(", ", columns)}) VALUES ({string.Join(", ", paramNames)})";
        return (sql, parameters);
    }

    private (string Sql, List<SqliteParameter> Parameters) BuildUpdateSql(
        string pk, string rk, string etag, DateTimeOffset timestamp,
        Dictionary<string, (TypedValue Typed, object? SqlValue)> properties)
    {
        var setClauses = new List<string> { "_etag = @etag", "_timestamp = @ts", "_properties = @props" };
        var parameters = new List<SqliteParameter>
        {
            new("@etag", etag),
            new("@ts", timestamp.ToString("O")),
            new("@props", SerializeTypedValues(properties)),
            new("@pk", pk),
            new("@rk", rk),
        };

        int i = 0;
        foreach (var kvp in properties)
        {
            var pName = $"@v{i++}";
            setClauses.Add($"[{kvp.Key}] = {pName}");
            parameters.Add(new SqliteParameter(pName, kvp.Value.SqlValue ?? DBNull.Value));
        }

        var sql = $"UPDATE [{_tableName}] SET {string.Join(", ", setClauses)} WHERE PartitionKey = @pk AND RowKey = @rk";
        return (sql, parameters);
    }

    // ---- Column Management ----

    private string ColumnCacheKey => $"{_serviceClient.Db.DbPath}:{_tableName}";

    private HashSet<string> GetKnownColumns(SqliteConnection conn)
    {
        var key = ColumnCacheKey;
        if (s_columnCache.TryGetValue(key, out var cached))
            return cached;

        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info([{_tableName}])";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            columns.Add(reader.GetString(1));

        s_columnCache[key] = columns;
        return columns;
    }

    private void EnsureColumns(SqliteConnection conn, Dictionary<string, (TypedValue Typed, object? SqlValue)> properties, SqliteTransaction? tx = null)
    {
        var known = GetKnownColumns(conn);

        foreach (var kvp in properties)
        {
            if (known.Contains(kvp.Key)) continue;
            if (s_fixedColumns.Contains(kvp.Key)) continue;

            var sqlType = GetSqliteType(kvp.Value.Typed.Type);
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = $"ALTER TABLE [{_tableName}] ADD COLUMN [{kvp.Key}] {sqlType}";
            try
            {
                cmd.ExecuteNonQuery();
                known.Add(kvp.Key);
            }
            catch (SqliteException)
            {
                // Column may already exist (race condition with concurrent writers)
                known.Add(kvp.Key);
            }
        }
    }

    private static string GetSqliteType(string typedValueType) => typedValueType switch
    {
        "Int32" or "Int64" or "Boolean" => "INTEGER",
        "Double" => "REAL",
        "Binary" => "BLOB",
        _ => "TEXT"
    };

    // ---- Property Extraction ----

    private Dictionary<string, (TypedValue Typed, object? SqlValue)> ExtractProperties(ITableEntity entity)
    {
        var result = new Dictionary<string, (TypedValue, object?)>();

        if (entity is TableEntity te)
        {
            foreach (var kvp in te)
            {
                if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp") continue;
                var typed = TypedValue.FromObject(kvp.Value);
                result[kvp.Key] = (typed, ToSqlValue(typed));
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
                {
                    var typed = TypedValue.FromObject(val);
                    result[prop.Name] = (typed, ToSqlValue(typed));
                }
            }
        }
        return result;
    }

    private Dictionary<string, (TypedValue Typed, object? SqlValue)> ExtractPropertiesFromTableEntity(TableEntity entity)
    {
        var result = new Dictionary<string, (TypedValue, object?)>();
        foreach (var kvp in entity)
        {
            if (kvp.Key is "PartitionKey" or "RowKey" or "odata.etag" or "Timestamp") continue;
            var typed = TypedValue.FromObject(kvp.Value);
            result[kvp.Key] = (typed, ToSqlValue(typed));
        }
        return result;
    }

    private static object? ToSqlValue(TypedValue tv) => tv.Type switch
    {
        "Null" => null,
        "Int32" => int.TryParse(tv.Value, out var i) ? (long)i : null,
        "Int64" => long.TryParse(tv.Value, out var l) ? l : null,
        "Double" => double.TryParse(tv.Value, out var d) ? d : null,
        "Boolean" => bool.TryParse(tv.Value, out var b) ? (b ? 1L : 0L) : null,
        "Binary" => Convert.FromBase64String(tv.Value),
        _ => tv.Value // String, DateTime, Guid stored as TEXT
    };

    private static string SerializeTypedValues(Dictionary<string, (TypedValue Typed, object? SqlValue)> properties)
    {
        var dict = new Dictionary<string, TypedValue>();
        foreach (var kvp in properties)
            dict[kvp.Key] = kvp.Value.Typed;
        return JsonSerializer.Serialize(dict, s_jsonOptions);
    }

    // ---- Read Helpers ----

    private TableEntity? ReadEntity(SqliteConnection conn, string partitionKey, string rowKey, SqliteTransaction? tx = null)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"SELECT _etag, _timestamp, _properties FROM [{_tableName}] WHERE PartitionKey = @pk AND RowKey = @rk";
        cmd.Parameters.AddWithValue("@pk", partitionKey);
        cmd.Parameters.AddWithValue("@rk", rowKey);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            return null;

        var etag = reader.GetString(0);
        var timestamp = DateTimeOffset.Parse(reader.GetString(1));
        var propsJson = reader.GetString(2);

        return DeserializeToTableEntity(partitionKey, rowKey, etag, timestamp, propsJson);
    }

    private static string NewETag() => $"0x{Guid.NewGuid():N}";

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
                    entity[kvp.Key] = kvp.Value.ToObject();
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

    // ---- TypedValue ----

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
