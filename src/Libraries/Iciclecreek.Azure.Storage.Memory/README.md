![Logo](https://raw.githubusercontent.com/tomlm/Iciclecreek.Azure.Storage/refs/heads/main/icon.png)

[![Build](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml) [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Memory.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory)

# Iciclecreek.Azure.Storage.Memory
A **thread-safe, in-memory drop-in replacement** for `Azure.Storage.Blobs`, `Azure.Data.Tables`, and `Azure.Storage.Queues` clients. Use the same Azure SDK types in tests without any disk I/O, databases, or external services.

Objects returned from reads are deep-cloned to properly simulate network storage semantics -- no shared references, no accidental cache hits.

## Installation

```
dotnet add package Iciclecreek.Azure.Storage.Memory
```

## Usage

### Blobs

```csharp
using Iciclecreek.Azure.Storage.Memory.Blobs;

var blobService = new MemoryBlobServiceClient();
BlobContainerClient container = blobService.GetBlobContainerClient("my-container");
await container.CreateIfNotExistsAsync();

BlobClient blob = container.GetBlobClient("hello.txt");
await blob.UploadAsync(BinaryData.FromString("Hello, World!"));

var result = (await blob.DownloadContentAsync()).Value;
Console.WriteLine(result.Content.ToString()); // "Hello, World!"
```

### Tables

```csharp
using Iciclecreek.Azure.Storage.Memory.Tables;

var tableService = new MemoryTableServiceClient();
TableClient table = tableService.GetTableClient("people");
await table.CreateIfNotExistsAsync();

await table.AddEntityAsync(new TableEntity("users", "alice") { ["Name"] = "Alice" });
var entity = (await table.GetEntityAsync<TableEntity>("users", "alice")).Value;
```

### Queues

```csharp
using Iciclecreek.Azure.Storage.Memory.Queues;

var queueService = new MemoryQueueServiceClient();
QueueClient queue = queueService.GetQueueClient("tasks");
queue.Create();

queue.SendMessage("do the thing");
var msg = queue.ReceiveMessage().Value;
Console.WriteLine(msg.Body.ToString()); // "do the thing"
```

### Swap in via dependency injection

Every `Memory*` client inherits from its Azure SDK base type:

```csharp
// Production
services.AddSingleton<BlobServiceClient>(
    new BlobServiceClient(connectionString));

// Test
services.AddSingleton<BlobServiceClient>(
    new MemoryBlobServiceClient());
```

## Related Packages

| Package | Description |
|---------|-------------|
| [Iciclecreek.Azure.Storage.FileSystem](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed implementation (files on disk) |
| [Iciclecreek.Azure.Storage.SQLite](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed implementation (single .db file) |
| [Iciclecreek.Azure.Storage.Server](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server) | ASP.NET Core REST API server on top of any provider |

## License

MIT
