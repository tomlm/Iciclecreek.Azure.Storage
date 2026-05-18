![Logo](https://raw.githubusercontent.com/tomlm/Iciclecreek.Azure.Storage/refs/heads/main/icon.png)

[![Build](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml) [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Server.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server)

# Iciclecreek.Azure.Storage.Server
ASP.NET Core controllers that expose the **Azure Storage REST API** on top of any `BlobServiceClient`, `TableServiceClient`, and `QueueServiceClient` implementation. Pair with the FileSystem or SQLite provider to run a local Azure Storage-compatible server.

## Installation

```
dotnet add package Iciclecreek.Azure.Storage.Server
```

## Usage

### Register the controllers

```csharp
using Iciclecreek.Azure.Storage.Server;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Queues;

var builder = WebApplication.CreateBuilder(args);

var storagePath = @"C:\temp\my-storage";

// Register Azure SDK clients in DI
builder.Services.AddSingleton<BlobServiceClient>(new FileBlobServiceClient(storagePath));
builder.Services.AddSingleton<TableServiceClient>(new FileTableServiceClient(storagePath));
builder.Services.AddSingleton<QueueServiceClient>(new FileQueueServiceClient(storagePath));

// Add the storage server controllers
builder.Services.AddStorageServer();

var app = builder.Build();
app.MapStorageServer();
app.Run();
```

### Use with any provider

The server works with any Azure SDK client implementation -- FileSystem, SQLite, or even the real Azure clients:

```csharp
// SQLite provider
var dbPath = @"C:\temp\storage.db";
builder.Services.AddSingleton<BlobServiceClient>(new SqliteBlobServiceClient(dbPath));
builder.Services.AddSingleton<TableServiceClient>(new SqliteTableServiceClient(dbPath));
builder.Services.AddSingleton<QueueServiceClient>(new SqliteQueueServiceClient(dbPath));
```

## Related Packages

| Package | Description |
|---------|-------------|
| [Iciclecreek.Azure.Storage.Memory](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory) | Thread-safe in-memory blobs, tables, and queues |
| [Iciclecreek.Azure.Storage.FileSystem](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed blobs, tables, and queues |
| [Iciclecreek.Azure.Storage.SQLite](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed blobs, tables, and queues |

## License

MIT
