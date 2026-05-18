using System.Linq.Expressions;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite.Tables;

/// <summary>
/// SQLite-backed drop-in replacement for <see cref="Azure.Data.Tables.TableServiceClient"/>.
/// Tables are rows in the Tables table of the SQLite database.
/// </summary>
public class SqliteTableServiceClient : TableServiceClient
{
    internal readonly SqliteDb Db;
    private readonly string _accountName;
    private readonly Uri _tableServiceUri;

    public SqliteTableServiceClient(string dbPath) : base()
    {
        Db = new SqliteDb(dbPath);
        _accountName = string.Empty;
        _tableServiceUri = new Uri("sqlite://table/");
    }

    /// <inheritdoc/>
    public override string AccountName => _accountName;
    /// <inheritdoc/>
    public override Uri Uri => _tableServiceUri;

    // ---- GetTableClient ----

    /// <inheritdoc/>
    public override TableClient GetTableClient(string tableName) => new SqliteTableClient(this, tableName);

    // ---- CreateTable ----

    /// <inheritdoc/>
    public override Response<TableItem> CreateTable(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new SqliteTableClient(this, tableName);
        return client.Create(cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<TableItem> CreateTableIfNotExists(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new SqliteTableClient(this, tableName);
        return client.CreateIfNotExists(cancellationToken);
    }

    /// <inheritdoc/>
    public override Response DeleteTable(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new SqliteTableClient(this, tableName);
        return client.Delete(cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response<TableItem>> CreateTableAsync(string tableName, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateTable(tableName, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response<TableItem>> CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateTableIfNotExists(tableName, cancellationToken); }

    /// <inheritdoc/>
    public override async Task<Response> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteTable(tableName, cancellationToken); }

    // ---- Query ----

    /// <inheritdoc/>
    public override Pageable<TableItem> Query(string? filter = null, int? maxPerPage = null, CancellationToken cancellationToken = default)
    {
        var items = new List<TableItem>();
        using var conn = Db.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM Tables ORDER BY Name";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(new TableItem(reader.GetString(0)));
        }
        return new StaticPageable<TableItem>(items);
    }

    /// <inheritdoc/>
    public override Pageable<TableItem> Query(Expression<Func<TableItem, bool>> filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
    {
        var all = Query((string?)null, maxPerPage, cancellationToken);
        var compiled = filter.Compile();
        return new StaticPageable<TableItem>(all.Where(compiled).ToList());
    }

    /// <inheritdoc/>
    public override AsyncPageable<TableItem> QueryAsync(string? filter = null, int? maxPerPage = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<TableItem>(Query(filter, maxPerPage, cancellationToken));

    /// <inheritdoc/>
    public override AsyncPageable<TableItem> QueryAsync(Expression<Func<TableItem, bool>> filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<TableItem>(Query(filter, maxPerPage, cancellationToken));

    // ---- FormattableString query overloads ----

    /// <inheritdoc/>
    public override Pageable<TableItem> Query(FormattableString filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
        => Query(filter?.ToString(), maxPerPage, cancellationToken);

    /// <inheritdoc/>
    public override AsyncPageable<TableItem> QueryAsync(FormattableString filter, int? maxPerPage = null, CancellationToken cancellationToken = default)
        => QueryAsync(filter?.ToString(), maxPerPage, cancellationToken);

    // ---- Service properties / statistics (stub) ----

    /// <inheritdoc/>
    public override Response<TableServiceProperties> GetProperties(CancellationToken ct = default)
        => Response.FromValue(new TableServiceProperties(), StubResponse.Ok());
    /// <inheritdoc/>
    public override async Task<Response<TableServiceProperties>> GetPropertiesAsync(CancellationToken ct = default)
        => GetProperties(ct);
    /// <inheritdoc/>
    public override Response SetProperties(TableServiceProperties properties, CancellationToken ct = default)
        => StubResponse.Ok();
    /// <inheritdoc/>
    public override async Task<Response> SetPropertiesAsync(TableServiceProperties properties, CancellationToken ct = default)
        => SetProperties(properties, ct);
    /// <inheritdoc/>
    public override Response<TableServiceStatistics> GetStatistics(CancellationToken ct = default)
        => Response.FromValue(default(TableServiceStatistics)!, StubResponse.Ok());
    /// <inheritdoc/>
    public override async Task<Response<TableServiceStatistics>> GetStatisticsAsync(CancellationToken ct = default)
        => GetStatistics(ct);

    // ---- Remaining virtual methods ----
    /// <inheritdoc/>
    public override Uri GenerateSasUri(TableAccountSasPermissions permissions, TableAccountSasResourceTypes resourceTypes, DateTimeOffset expiresOn) => _tableServiceUri;
    /// <inheritdoc/>
    public override Uri GenerateSasUri(TableAccountSasBuilder builder) => _tableServiceUri;
    /// <inheritdoc/>
    public override TableAccountSasBuilder GetSasBuilder(TableAccountSasPermissions permissions, TableAccountSasResourceTypes resourceTypes, DateTimeOffset expiresOn) => new TableAccountSasBuilder(permissions, resourceTypes, expiresOn);
    /// <inheritdoc/>
    public override TableAccountSasBuilder GetSasBuilder(string rawPermissions, TableAccountSasResourceTypes resourceTypes, DateTimeOffset expiresOn) => new TableAccountSasBuilder(rawPermissions, resourceTypes, expiresOn);
}
