using Lucene.Net.Store;
using LuceneDirectory = Lucene.Net.Store.Directory;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Iciclecreek.Azure.Storage.Lucene.Internal;

namespace Iciclecreek.Azure.Storage.Lucene.Tables;

/// <summary>
/// Drop-in replacement for <see cref="Azure.Data.Tables.TableServiceClient"/> that keeps tables in
/// Lucene indexes: each table is a directory of its own, <c>tables/{table}</c>, with its own index
/// writer and a document per entity.
/// <para>
/// As in Azure, table names are not case sensitive: <c>Orders</c> and <c>orders</c> are one table.
/// The directory name is the table's name in lower case, with any character other than a to z,
/// a digit or a hyphen written as <c>_</c> and its four-digit hexadecimal code, so that every
/// table is a folder of its own inside <c>tables</c> on any file system. The name
/// <c>__registry__</c> is refused with <c>400 InvalidResourceName</c>.
/// </para>
/// </summary>
public class LuceneTableServiceClient : TableServiceClient, IDisposable
{
    internal LuceneTableRegistry Tables { get; }
    private readonly string _accountName = string.Empty;
    private readonly Uri _tableServiceUri = new("lucene://table/");

    /// <summary>Tables in memory: gone when this client is.</summary>
    public LuceneTableServiceClient() : this(InMemory())
    {
    }

    /// <summary>Tables in folders under <paramref name="rootPath"/>: <c>tables/{table}</c> for each.</summary>
    public LuceneTableServiceClient(string rootPath) : this(InFolder(rootPath))
    {
    }

    /// <summary>
    /// Tables in the directories the factory gives. It is asked for <c>tables/{table}</c> for each
    /// table and <c>tables/__registry__</c> for the list of tables; each is disposed with this client.
    /// </summary>
    public LuceneTableServiceClient(Func<string, LuceneDirectory> directoryFactory) : base()
    {
        Tables = new LuceneTableRegistry(directoryFactory ?? throw new ArgumentNullException(nameof(directoryFactory)));
    }

    private static Func<string, LuceneDirectory> InMemory()
    {
        var directories = new ConcurrentDictionary<string, LuceneDirectory>(StringComparer.Ordinal);
        return path => directories.GetOrAdd(path, _ => new RAMDirectory());
    }

    private static Func<string, LuceneDirectory> InFolder(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
            throw new ArgumentException("A folder is needed.", nameof(rootPath));
        var root = Path.GetFullPath(rootPath);
        return path => FSDirectory.Open(Path.Combine(root, path.Replace('/', Path.DirectorySeparatorChar)));
    }

    /// <summary>Closes the directories the tables are kept in.</summary>
    public void Dispose()
    {
        Tables.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc/>
    public override string AccountName => _accountName;
    /// <inheritdoc/>
    public override Uri Uri => _tableServiceUri;

    // ---- GetTableClient ----

    /// <inheritdoc/>
    public override TableClient GetTableClient(string tableName) => new LuceneTableClient(this, tableName);

    // ---- CreateTable ----

    /// <inheritdoc/>
    public override Response<TableItem> CreateTable(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new LuceneTableClient(this, tableName);
        return client.Create(cancellationToken);
    }

    /// <inheritdoc/>
    public override Response<TableItem> CreateTableIfNotExists(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new LuceneTableClient(this, tableName);
        return client.CreateIfNotExists(cancellationToken);
    }

    /// <inheritdoc/>
    public override Response DeleteTable(string tableName, CancellationToken cancellationToken = default)
    {
        var client = new LuceneTableClient(this, tableName);
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
        var items = Tables.Keys
            .OrderBy(n => n)
            .Select(n => new TableItem(n))
            .ToList();
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
