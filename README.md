![icon](icon.png)
# Iciclecreek.Azure.Storage

**Drop-in replacements** for Azure Storage SDK clients. Use the same `BlobContainerClient`, `TableClient`, and `QueueClient` types in tests and local development without Azurite or a live Azure account.

Choose the backend that fits your scenario: in-memory for fast unit tests, filesystem for inspectable persistent state, SQLite for single-file portable storage, or the REST server for integration testing.

## Packages

| Package | Description | NuGet |
|---------|-------------|-------|
| [Iciclecreek.Azure.Storage.Memory](src/Libraries/Iciclecreek.Azure.Storage.Memory) | Thread-safe in-memory blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Memory.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory) |
| [Iciclecreek.Azure.Storage.FileSystem](src/Libraries/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.FileSystem.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) |
| [Iciclecreek.Azure.Storage.SQLite](src/Libraries/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed blobs, tables, and queues | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.SQLite.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) |
| [Iciclecreek.Azure.Storage.Server](src/Libraries/Iciclecreek.Azure.Storage.Server) | ASP.NET Core controllers exposing the Azure Storage REST API | [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Server.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server) |

## Supported Features

| Area | Memory | FileSystem | SQLite |
|------|:------:|:----------:|:------:|
| Block blobs (Upload, Download, Stage, Commit) | ✅ | ✅ | ✅ |
| Append blobs (Create, Append) | ✅ | ✅ | ✅ |
| Page blobs (Create, UploadPages, ClearPages) | ✅ | ✅ | ✅ |
| Blob containers (CRUD, Listing, Hierarchy) | ✅ | ✅ | ✅ |
| Blob metadata, HTTP headers, tags | ✅ | ✅ | ✅ |
| ETag concurrency (IfMatch / IfNoneMatch) | ✅ | ✅ | ✅ |
| Blob leases and container leases | ✅ | ✅ | ✅ |
| Blob copy (StartCopyFromUri, SyncCopyFromUri) | ✅ | ✅ | ✅ |
| Blob OpenRead / OpenWrite streaming | ✅ | ✅ | ✅ |
| Blob snapshots and versions | ✅ | ✅ | ✅ |
| Tables (CRUD, Upsert, Merge, Replace) | ✅ | ✅ | ✅ |
| Table OData queries and LINQ expressions | ✅ | ✅ | ✅ |
| Table transactions with rollback | ✅ | ✅ | ✅ |
| Queues (CRUD, Send, Receive, Peek, Delete) | ✅ | ✅ | ✅ |
| Queue visibility timeout and TTL | ✅ | ✅ | ✅ |

## Quick Start

```csharp
// Memory -- fastest, no I/O, ideal for unit tests
var blobService = new MemoryBlobServiceClient();
var tableService = new MemoryTableServiceClient();

// FileSystem -- stores data as files on disk
var blobService = new FileBlobServiceClient(@"C:\temp\my-storage");
var tableService = new FileTableServiceClient(@"C:\temp\my-storage");

// SQLite -- stores everything in a single .db file
var blobService = new SqliteBlobServiceClient(@"C:\temp\storage.db");
var tableService = new SqliteTableServiceClient(@"C:\temp\storage.db");

// Then use standard Azure SDK types
BlobContainerClient container = blobService.GetBlobContainerClient("my-container");
await container.CreateIfNotExistsAsync();
```

See each package's README for detailed usage.

## License

MIT
