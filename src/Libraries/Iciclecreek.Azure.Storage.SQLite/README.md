![Logo](https://raw.githubusercontent.com/tomlm/Iciclecreek.Azure.Storage/refs/heads/main/icon.png)

[![Build](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml) [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.SQLite.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite)

# Iciclecreek.Azure.Storage.SQLite
A **SQLite-backed drop-in replacement** for `Azure.Storage.Blobs`, `Azure.Data.Tables`, and `Azure.Storage.Queues` clients. Use the same Azure SDK types in tests and local development without Azurite or a live Azure account.

All data is stored in a single `.db` file -- portable, atomic, and easy to manage.

## Installation

```
dotnet add package Iciclecreek.Azure.Storage.SQLite
```

## Usage

### Blobs

```csharp
using Iciclecreek.Azure.Storage.SQLite.Blobs;

var blobService = new SqliteBlobServiceClient(@"C:\temp\storage.db");
BlobContainerClient container = blobService.GetBlobContainerClient("my-container");
await container.CreateIfNotExistsAsync();

BlobClient blob = container.GetBlobClient("hello.txt");
await blob.UploadAsync(BinaryData.FromString("Hello, World!"));

var result = (await blob.DownloadContentAsync()).Value;
Console.WriteLine(result.Content.ToString()); // "Hello, World!"
```

### Tables

```csharp
using Iciclecreek.Azure.Storage.SQLite.Tables;

var tableService = new SqliteTableServiceClient(@"C:\temp\storage.db");
TableClient table = tableService.GetTableClient("people");
await table.CreateIfNotExistsAsync();

await table.AddEntityAsync(new TableEntity("users", "alice") { ["Name"] = "Alice" });
var entity = (await table.GetEntityAsync<TableEntity>("users", "alice")).Value;
```

### Queues

```csharp
using Iciclecreek.Azure.Storage.SQLite.Queues;

var queueService = new SqliteQueueServiceClient(@"C:\temp\storage.db");
QueueClient queue = queueService.GetQueueClient("tasks");
queue.Create();

queue.SendMessage("do the thing");
var msg = queue.ReceiveMessage().Value;
Console.WriteLine(msg.Body.ToString()); // "do the thing"
```

### Swap in via dependency injection

Every `Sqlite*` client inherits from its Azure SDK base type:

```csharp
// Production
services.AddSingleton<BlobServiceClient>(
    new BlobServiceClient(connectionString));

// Test
services.AddSingleton<BlobServiceClient>(
    new SqliteBlobServiceClient("test.db"));
```

## Related Packages

| Package | Description |
|---------|-------------|
| [Iciclecreek.Azure.Storage.Memory](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory) | Thread-safe in-memory implementation (fastest, no I/O) |
| [Iciclecreek.Azure.Storage.FileSystem](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed implementation (files on disk) |
| [Iciclecreek.Azure.Storage.Server](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server) | ASP.NET Core REST API server on top of any provider |

## License

MIT
