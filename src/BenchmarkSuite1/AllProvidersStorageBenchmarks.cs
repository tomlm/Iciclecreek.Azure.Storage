using System;
using System.IO;
using System.Linq;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using BenchmarkDotNet.Attributes;
using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tables;
using Microsoft.VSDiagnostics;

[SimpleJob]
[CPUUsageDiagnoser]
public class AllProvidersStorageBenchmarks
{
    // Memory
    private TableClient _memTableClient = null !;
    private BlobContainerClient _memContainerClient = null !;
    // FileSystem
    private TableClient _fsTableClient = null !;
    private BlobContainerClient _fsContainerClient = null !;
    // SQLite
    private TableClient _sqlTableClient = null !;
    private BlobContainerClient _sqlContainerClient = null !;
    private byte[] _blobData = null !;
    private const string Filter = "PartitionKey eq 'pk1' and Age gt 25";
    private static readonly string _tempRoot = @"c:\temp\benchmarks";
    [GlobalSetup]
    public void Setup()
    {
        _blobData = new byte[4096];
        new Random(42).NextBytes(_blobData);
        // ---- Memory ----
        var memTableService = new MemoryTableServiceClient();
        _memTableClient = memTableService.GetTableClient("benchmarks");
        _memTableClient.CreateIfNotExists();
        var memBlobService = new MemoryBlobServiceClient();
        _memContainerClient = memBlobService.GetBlobContainerClient("benchmarks");
        _memContainerClient.CreateIfNotExists();
        // ---- FileSystem ----
        var fsRoot = Path.Combine(_tempRoot, "fs");
        if (Directory.Exists(fsRoot))
            Directory.Delete(fsRoot, true);
        Directory.CreateDirectory(fsRoot);
        var fsTableService = new FileTableServiceClient(fsRoot);
        _fsTableClient = fsTableService.GetTableClient("benchmarks");
        _fsTableClient.CreateIfNotExists();
        var fsBlobService = new FileBlobServiceClient(fsRoot);
        _fsContainerClient = fsBlobService.GetBlobContainerClient("benchmarks");
        _fsContainerClient.CreateIfNotExists();
        // ---- SQLite ----
        var sqlitePath = Path.Combine(_tempRoot, "sqlite", "bench.db");
        var sqliteDir = Path.GetDirectoryName(sqlitePath)!;
        if (Directory.Exists(sqliteDir))
            Directory.Delete(sqliteDir, true);
        Directory.CreateDirectory(sqliteDir);
        var sqlTableService = new SqliteTableServiceClient(sqlitePath);
        _sqlTableClient = sqlTableService.GetTableClient("benchmarks");
        _sqlTableClient.CreateIfNotExists();
        var sqlBlobService = new SqliteBlobServiceClient(sqlitePath);
        _sqlContainerClient = sqlBlobService.GetBlobContainerClient("benchmarks");
        _sqlContainerClient.CreateIfNotExists();
        // Seed entities in all providers
        for (var i = 0; i < 500; i++)
        {
            var entity = new TableEntity($"pk{i % 5}", $"rk{i}")
            {
                ["Name"] = $"Entity{i}",
                ["Age"] = i,
                ["Score"] = (double)i * 1.5,
                ["Active"] = i % 2 == 0
            };
            _memTableClient.AddEntity(entity);
            _fsTableClient.AddEntity(entity);
            _sqlTableClient.AddEntity(entity);
        }

        // Pre-upload blobs
        using (var ms = new MemoryStream(_blobData))
            _memContainerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
        using (var ms = new MemoryStream(_blobData))
            _fsContainerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
        using (var ms = new MemoryStream(_blobData))
            _sqlContainerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
    }

    // ---- Memory benchmarks ----
    [Benchmark]
    public void Memory_TableQuery()
    {
        var results = _memTableClient.Query<TableEntity>(Filter).ToList();
    }

    [Benchmark]
    public void Memory_TableUpsert()
    {
        var entity = new TableEntity("pk1", "rk_bench")
        {
            ["Name"] = "BenchmarkEntity",
            ["Age"] = 42,
            ["Score"] = 3.14,
            ["Active"] = true
        };
        _memTableClient.UpsertEntity(entity, TableUpdateMode.Replace);
    }

    [Benchmark]
    public void Memory_BlobUpload()
    {
        using var ms = new MemoryStream(_blobData);
        _memContainerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
    }

    [Benchmark]
    public void Memory_BlobDownload()
    {
        _memContainerClient.GetBlobClient("bench-blob").DownloadContent();
    }

    // ---- FileSystem benchmarks ----
    [Benchmark]
    public void FileSystem_TableQuery()
    {
        var results = _fsTableClient.Query<TableEntity>(Filter).ToList();
    }

    [Benchmark]
    public void FileSystem_TableUpsert()
    {
        var entity = new TableEntity("pk1", "rk_bench")
        {
            ["Name"] = "BenchmarkEntity",
            ["Age"] = 42,
            ["Score"] = 3.14,
            ["Active"] = true
        };
        _fsTableClient.UpsertEntity(entity, TableUpdateMode.Replace);
    }

    [Benchmark]
    public void FileSystem_BlobUpload()
    {
        using var ms = new MemoryStream(_blobData);
        _fsContainerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
    }

    [Benchmark]
    public void FileSystem_BlobDownload()
    {
        _fsContainerClient.GetBlobClient("bench-blob").DownloadContent();
    }

    // ---- SQLite benchmarks ----
    [Benchmark]
    public void SQLite_TableQuery()
    {
        var results = _sqlTableClient.Query<TableEntity>(Filter).ToList();
    }

    [Benchmark]
    public void SQLite_TableUpsert()
    {
        var entity = new TableEntity("pk1", "rk_bench")
        {
            ["Name"] = "BenchmarkEntity",
            ["Age"] = 42,
            ["Score"] = 3.14,
            ["Active"] = true
        };
        _sqlTableClient.UpsertEntity(entity, TableUpdateMode.Replace);
    }

    [Benchmark]
    public void SQLite_BlobUpload()
    {
        using var ms = new MemoryStream(_blobData);
        _sqlContainerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
    }

    [Benchmark]
    public void SQLite_BlobDownload()
    {
        _sqlContainerClient.GetBlobClient("bench-blob").DownloadContent();
    }
}