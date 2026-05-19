using Microsoft.Data.Sqlite;

namespace Iciclecreek.Azure.Storage.SQLite.Internal;

/// <summary>
/// Manages SQLite database connections and schema initialization.
/// One database file per storage account.
/// </summary>
internal sealed class SqliteDb
{
    private readonly string _connectionString;

    public string DbPath { get; }

    public SqliteDb(string dbPath)
    {
        DbPath = dbPath;
        var dir = Path.GetDirectoryName(dbPath);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();

        InitializeSchema();
    }

    public SqliteConnection Open()
    {
        var conn = new SqliteConnection(_connectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
        cmd.ExecuteNonQuery();
        return conn;
    }

    /// <summary>
    /// Creates a per-table SQLite table for storing entities.
    /// </summary>
    public void CreateEntityTable(SqliteConnection conn, string tableName, SqliteTransaction? tx = null)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"""
            CREATE TABLE IF NOT EXISTS [{tableName}] (
                PartitionKey TEXT NOT NULL,
                RowKey TEXT NOT NULL,
                _etag TEXT NOT NULL,
                _timestamp TEXT NOT NULL,
                _properties TEXT NOT NULL,
                PRIMARY KEY (PartitionKey, RowKey)
            )
            """;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Drops a per-table SQLite table.
    /// </summary>
    public void DropEntityTable(SqliteConnection conn, string tableName, SqliteTransaction? tx = null)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"DROP TABLE IF EXISTS [{tableName}]";
        cmd.ExecuteNonQuery();
    }

    private void InitializeSchema()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = Schema;
        cmd.ExecuteNonQuery();
    }

    private const string Schema = """
        CREATE TABLE IF NOT EXISTS Containers (
            Name TEXT PRIMARY KEY,
            Metadata TEXT,
            LeaseId TEXT,
            LeaseExpiresOn TEXT,
            LeaseDurationSeconds INTEGER DEFAULT 0,
            CreatedOn TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS Blobs (
            ContainerName TEXT NOT NULL,
            BlobName TEXT NOT NULL,
            BlobType TEXT NOT NULL DEFAULT 'Block',
            Content BLOB,
            ContentType TEXT,
            ContentEncoding TEXT,
            ContentLanguage TEXT,
            ContentDisposition TEXT,
            CacheControl TEXT,
            ContentHash TEXT,
            ETag TEXT NOT NULL,
            CreatedOn TEXT NOT NULL,
            LastModified TEXT NOT NULL,
            Length INTEGER NOT NULL DEFAULT 0,
            AccessTier TEXT,
            Metadata TEXT,
            Tags TEXT,
            LeaseId TEXT,
            LeaseExpiresOn TEXT,
            LeaseDurationSeconds INTEGER DEFAULT 0,
            SequenceNumber INTEGER DEFAULT 0,
            IsSealed INTEGER DEFAULT 0,
            VersionId TEXT,
            PRIMARY KEY (ContainerName, BlobName)
        );

        CREATE TABLE IF NOT EXISTS StagedBlocks (
            ContainerName TEXT NOT NULL,
            BlobName TEXT NOT NULL,
            BlockId TEXT NOT NULL,
            Content BLOB NOT NULL,
            Size INTEGER NOT NULL,
            PRIMARY KEY (ContainerName, BlobName, BlockId)
        );

        CREATE TABLE IF NOT EXISTS CommittedBlocks (
            ContainerName TEXT NOT NULL,
            BlobName TEXT NOT NULL,
            BlockId TEXT NOT NULL,
            Size INTEGER NOT NULL,
            Ordinal INTEGER NOT NULL,
            PRIMARY KEY (ContainerName, BlobName, Ordinal)
        );

        CREATE TABLE IF NOT EXISTS PageRanges (
            ContainerName TEXT NOT NULL,
            BlobName TEXT NOT NULL,
            Offset INTEGER NOT NULL,
            Length INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Snapshots (
            ContainerName TEXT NOT NULL,
            BlobName TEXT NOT NULL,
            SnapshotId TEXT NOT NULL,
            Content BLOB,
            Sidecar TEXT,
            PRIMARY KEY (ContainerName, BlobName, SnapshotId)
        );

        CREATE TABLE IF NOT EXISTS Versions (
            ContainerName TEXT NOT NULL,
            BlobName TEXT NOT NULL,
            VersionId TEXT NOT NULL,
            Content BLOB,
            Sidecar TEXT,
            PRIMARY KEY (ContainerName, BlobName, VersionId)
        );

        CREATE TABLE IF NOT EXISTS Tables (
            Name TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS Queues (
            Name TEXT PRIMARY KEY,
            Metadata TEXT
        );

        CREATE TABLE IF NOT EXISTS Messages (
            QueueName TEXT NOT NULL,
            MessageId TEXT NOT NULL,
            PopReceipt TEXT NOT NULL,
            MessageText TEXT NOT NULL,
            InsertedOn TEXT NOT NULL,
            ExpiresOn TEXT NOT NULL,
            NextVisibleOn TEXT NOT NULL,
            DequeueCount INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (QueueName, MessageId)
        );

        CREATE INDEX IF NOT EXISTS IX_Messages_Visible ON Messages (QueueName, NextVisibleOn);
        """;
}
