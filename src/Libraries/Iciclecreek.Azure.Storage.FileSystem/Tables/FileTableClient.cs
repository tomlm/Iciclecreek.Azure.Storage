using System.Linq.Expressions;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Tables.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Tables;

/// <summary>Filesystem-backed drop-in replacement for <see cref="Azure.Data.Tables.TableClient"/>. Each entity is stored as a JSON file at <c>&lt;table&gt;/&lt;partitionKey&gt;/&lt;rowKey&gt;.json</c>.</summary>
public class FileTableClient : TableClient
{
    internal readonly TableStore _store;
    internal readonly FileStorageAccount _account;

    /// <summary>Initializes a new <see cref="FileTableClient"/> from a connection string, table name, and storage provider.</summary>
    /// <param name="connectionString">The connection string identifying the storage account.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="provider">The filesystem storage provider that manages account root paths.</param>
    public FileTableClient(string connectionString, string tableName, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
        _store = new TableStore(_account, tableName);
    }

    /// <summary>Initializes a new <see cref="FileTableClient"/> from a table URI and storage provider.</summary>
    /// <param name="tableUri">The URI of the table, including the account name and table name.</param>
    /// <param name="provider">The filesystem storage provider that manages account root paths.</param>
    public FileTableClient(Uri tableUri, FileStorageProvider provider) : base()
    {
        var (acctName, table) = StorageUriParser.ParseTableUri(tableUri, provider.HostnameSuffix);
        _account = provider.GetAccount(acctName);
        _store = new TableStore(_account, table);
    }

    internal FileTableClient(FileStorageAccount account, string tableName) : base()
    {
        _account = account;
        _store = new TableStore(account, tableName);
    }

    /// <summary>Creates a new <see cref="FileTableClient"/> directly from a <see cref="FileStorageAccount"/> and table name.</summary>
    /// <param name="account">The filesystem storage account.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <returns>A new <see cref="FileTableClient"/> instance.</returns>
    public static FileTableClient FromAccount(FileStorageAccount account, string tableName) => new(account, tableName);

    /// <inheritdoc/>
    public override string Name => _store.TableName;
    /// <inheritdoc/>
    public override string AccountName => _account.Name;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_account.TableServiceUri}{_store.TableName}");

    // ---- Create / Delete (no file I/O, just directory ops) ----

    /// <inheritdoc/>
    public override Response<TableItem> Create(CancellationToken cancellationToken = default)
    {
        if (!_store.CreateTable())
            throw new RequestFailedException(409, "Table already exists.", "TableAlreadyExists", null);
        return Response.FromValue(new TableItem(_store.TableName), StubResponse.Created());
    }

    /// <inheritdoc/>
    public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
    {
        _store.CreateTable();
        return Response.FromValue(new TableItem(_store.TableName), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response Delete(CancellationToken cancellationToken = default)
    {
        if (!_store.DeleteTable())
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

    // ---- Entity CRUD (async = primary) ----

    /// <inheritdoc/>
    public override async Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
    {
        var etag = await _store.AddEntityAsync(entity, cancellationToken).ConfigureAwait(false);
        return StubResponse.NoContent(etag);
    }

    /// <inheritdoc/>
    public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
        => AddEntityAsync(entity, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var entity = await _store.GetEntityAsync(partitionKey, rowKey, cancellationToken).ConfigureAwait(false);
        return Response.FromValue(ConvertEntity<T>(entity), StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        if (!_store.EntityExists(partitionKey, rowKey))
            return default!;

        var result = await GetEntityAsync<T>(partitionKey, rowKey, select, cancellationToken).ConfigureAwait(false);
        return Response.FromValue<T>(result.Value, StubResponse.Ok());
    }

    /// <inheritdoc/>
    public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => GetEntityIfExistsAsync<T>(partitionKey, rowKey, select, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var etag = await _store.UpsertEntityAsync(entity, mode, cancellationToken).ConfigureAwait(false);
        return StubResponse.NoContent(etag);
    }

    /// <inheritdoc/>
    public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpsertEntityAsync(entity, mode, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
    {
        var etag = await _store.UpdateEntityAsync(entity, ifMatch, mode, cancellationToken).ConfigureAwait(false);
        return StubResponse.NoContent(etag);
    }

    /// <inheritdoc/>
    public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpdateEntityAsync(entity, ifMatch, mode, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        var etag = ifMatch == default ? ETag.All : ifMatch;
        await _store.DeleteEntityAsync(partitionKey, rowKey, etag, cancellationToken).ConfigureAwait(false);
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

    // ---- Query (async = primary for entity enumeration) ----

    /// <inheritdoc/>
    public override AsyncPageable<T> QueryAsync<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var predicate = ODataFilterParser.Parse(filter ?? "");
        return new AsyncEnumerablePageable<T>(_store.EnumerateEntitiesAsync(), e => predicate(e) ? ConvertEntity<T>(e) : default);
    }

    /// <inheritdoc/>
    public override Pageable<T> Query<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var predicate = ODataFilterParser.Parse(filter ?? "");
        var results = new List<T>();
        var enumerator = _store.EnumerateEntitiesAsync().GetAsyncEnumerator(cancellationToken);
        try { while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()) { if (predicate(enumerator.Current)) results.Add(ConvertEntity<T>(enumerator.Current)); } }
        finally { enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
        return new StaticPageable<T>(results);
    }

    /// <inheritdoc/>
    public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var compiled = filter.Compile();
        return new AsyncEnumerablePageable<T>(_store.EnumerateEntitiesAsync(), e => { var c = ConvertEntity<T>(e); return compiled(c) ? c : default; });
    }

    /// <inheritdoc/>
    public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var compiled = filter.Compile();
        var results = new List<T>();
        var enumerator = _store.EnumerateEntitiesAsync().GetAsyncEnumerator(cancellationToken);
        try { while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()) { var c = ConvertEntity<T>(enumerator.Current); if (compiled(c)) results.Add(c); } }
        finally { enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
        return new StaticPageable<T>(results);
    }

    // ---- SubmitTransaction (async = primary) ----

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

        var snapshots = new Dictionary<string, string?>();
        foreach (var action in actions)
        {
            var path = _store.EntityPath(action.Entity.PartitionKey, action.Entity.RowKey);
            if (!snapshots.ContainsKey(path))
                snapshots[path] = File.Exists(path) ? await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false) : null;
        }

        var responses = new List<Response>();
        try
        {
            for (var i = 0; i < actions.Count; i++)
            {
                var a = actions[i];
                try
                {
                    string? txEtag = null;
                    switch (a.ActionType)
                    {
                        case TableTransactionActionType.Add:
                            txEtag = await _store.AddEntityAsync(a.Entity, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpdateMerge:
                            txEtag = await _store.UpdateEntityAsync(a.Entity, a.ETag, TableUpdateMode.Merge, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpdateReplace:
                            txEtag = await _store.UpdateEntityAsync(a.Entity, a.ETag, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpsertMerge:
                            txEtag = await _store.UpsertEntityAsync(a.Entity, TableUpdateMode.Merge, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.UpsertReplace:
                            txEtag = await _store.UpsertEntityAsync(a.Entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
                            break;
                        case TableTransactionActionType.Delete:
                            await _store.DeleteEntityAsync(a.Entity.PartitionKey, a.Entity.RowKey, a.ETag == default ? ETag.All : a.ETag, cancellationToken).ConfigureAwait(false);
                            break;
                    }
                    responses.Add(txEtag != null ? StubResponse.NoContent(txEtag) : StubResponse.NoContent());
                }
                catch (RequestFailedException ex)
                {
                    await RollbackAsync(snapshots, cancellationToken).ConfigureAwait(false);
                    throw new TableTransactionFailedException(ex);
                }
            }
        }
        catch (TableTransactionFailedException) { throw; }
        catch (Exception ex)
        {
            await RollbackAsync(snapshots, cancellationToken).ConfigureAwait(false);
            throw new RequestFailedException(500, ex.Message, null, ex);
        }

        IReadOnlyList<Response> list = responses;
        return Response.FromValue(list, StubResponse.Accepted());
    }

    /// <inheritdoc/>
    public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
        => SubmitTransactionAsync(transactionActions, cancellationToken).GetAwaiter().GetResult();

    // ---- Helpers ----

    private static async Task RollbackAsync(Dictionary<string, string?> snapshots, CancellationToken ct)
    {
        foreach (var kvp in snapshots)
        {
            if (kvp.Value is null)
            {
                if (File.Exists(kvp.Key)) File.Delete(kvp.Key);
            }
            else
            {
                await AtomicFile.WriteAllTextAsync(kvp.Key, kvp.Value, ct).ConfigureAwait(false);
            }
        }
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

    // ==== Access Policies (stub — no real ACL enforcement) ====

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
    public override TableSasBuilder GetSasBuilder(TableSasPermissions permissions, DateTimeOffset expiresOn) => new TableSasBuilder(_store.TableName, permissions, expiresOn);
    /// <inheritdoc/>
    public override TableSasBuilder GetSasBuilder(string rawPermissions, DateTimeOffset expiresOn) => new TableSasBuilder(_store.TableName, rawPermissions, expiresOn);
}
