using System;
using System.IO;
using System.Linq;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using BenchmarkDotNet.Attributes;
using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;
using Microsoft.VSDiagnostics;

[MemoryDiagnoser]
public class MemoryStorageBenchmarks
{
    private MemoryTableClient _tableClient = null !;
    private MemoryBlobServiceClient _blobServiceClient = null !;
    private BlobContainerClient _containerClient = null !;
    private byte[] _blobData = null !;
    private const string Filter = "PartitionKey eq 'pk1' and Age gt 25";
    [GlobalSetup]
    public void Setup()
    {
        // Table setup
        var tableService = new MemoryTableServiceClient();
        _tableClient = (MemoryTableClient)tableService.GetTableClient("benchmarks");
        _tableClient.CreateIfNotExists();
        for (var i = 0; i < 500; i++)
        {
            var entity = new TableEntity($"pk{i % 5}", $"rk{i}")
            {
                ["Name"] = $"Entity{i}",
                ["Age"] = i,
                ["Score"] = (double)i * 1.5,
                ["Active"] = i % 2 == 0
            };
            _tableClient.AddEntity(entity);
        }

        // Blob setup
        _blobServiceClient = new MemoryBlobServiceClient();
        _containerClient = _blobServiceClient.GetBlobContainerClient("benchmarks");
        _containerClient.CreateIfNotExists();
        _blobData = new byte[4096];
        new Random(42).NextBytes(_blobData);

        // Pre-upload a blob so BlobDownload has data to read
        using var ms = new MemoryStream(_blobData);
        _containerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
    }

    [Benchmark]
    public void TableQuery_WithODataFilter()
    {
        var results = _tableClient.Query<TableEntity>(Filter).ToList();
    }

    [Benchmark]
    public void TableUpsert_Replace()
    {
        var entity = new TableEntity("pk1", "rk_bench")
        {
            ["Name"] = "BenchmarkEntity",
            ["Age"] = 42,
            ["Score"] = 3.14,
            ["Active"] = true
        };
        _tableClient.UpsertEntity(entity, TableUpdateMode.Replace);
    }

    [Benchmark]
    public void BlobUpload()
    {
        using var ms = new System.IO.MemoryStream(_blobData);
        _containerClient.GetBlobClient("bench-blob").Upload(ms, overwrite: true);
    }

    [Benchmark]
    public void BlobDownload()
    {
        _containerClient.GetBlobClient("bench-blob").DownloadContent();
    }
}