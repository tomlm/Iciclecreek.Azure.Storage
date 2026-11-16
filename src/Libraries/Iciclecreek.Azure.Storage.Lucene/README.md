![Logo](https://raw.githubusercontent.com/tomlm/Iciclecreek.Azure.Storage/refs/heads/main/icon.png)

[![Build](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml) [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Lucene.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Lucene)

# Iciclecreek.Azure.Storage.Lucene
A **Lucene.NET-backed drop-in replacement** for `Azure.Data.Tables` and `Azure.Storage.Queues` clients, and for the core of `Azure.Storage.Blobs`. Use the same Azure SDK types without Azurite or a live Azure account.

Data is kept in [Lucene.NET](https://lucenenet.apache.org/) directories: in memory, in a folder, or in any `Directory` you supply. Each table, container and queue is a directory of its own.

## Installation

```
dotnet add package Iciclecreek.Azure.Storage.Lucene
```

## Usage

### Blobs

```csharp
using Iciclecreek.Azure.Storage.Lucene.Blobs;

var blobService = new LuceneBlobServiceClient(@"C:\temp\storage");
BlobContainerClient container = blobService.GetBlobContainerClient("my-container");
await container.CreateIfNotExistsAsync();

BlobClient blob = container.GetBlobClient("hello.txt");
await blob.UploadAsync(BinaryData.FromString("Hello, World!"), overwrite: true);

var result = (await blob.DownloadContentAsync()).Value;
Console.WriteLine(result.Content.ToString()); // "Hello, World!"
```

### Tables

```csharp
using Iciclecreek.Azure.Storage.Lucene.Tables;

var tableService = new LuceneTableServiceClient(@"C:\temp\storage");
TableClient table = tableService.GetTableClient("people");
await table.CreateIfNotExistsAsync();

await table.AddEntityAsync(new TableEntity("users", "alice") { ["Name"] = "Alice" });
var entity = (await table.GetEntityAsync<TableEntity>("users", "alice")).Value;
```

### Queues

```csharp
using Iciclecreek.Azure.Storage.Lucene.Queues;

var queueService = new LuceneQueueServiceClient(@"C:\temp\storage");
QueueClient queue = queueService.GetQueueClient("tasks");
queue.Create();

queue.SendMessage("do the thing");
var msg = queue.ReceiveMessage().Value;
Console.WriteLine(msg.Body.ToString()); // "do the thing"
```

### In memory, or in any Lucene directory

```csharp
// In memory: gone when the client is
var queueService = new LuceneQueueServiceClient();

// Anywhere: the factory is given a path such as "queues/tasks" and returns the directory for it
var tableService = new LuceneTableServiceClient(path => OpenDirectoryFor(path));
```

### Swap in via dependency injection

Every `Lucene*` client inherits from its Azure SDK base type:

```csharp
// Production
services.AddSingleton<QueueServiceClient>(
    new QueueServiceClient(connectionString));

// Test
services.AddSingleton<QueueServiceClient>(
    new LuceneQueueServiceClient());
```

## Layout

Each table, container and queue is a directory of its own under the one supplied. The three clients can share a folder.

```
{directory}
  tables
    People
    Orders
    __registry__      the list of tables
  blobs
    photos
    documents
  queues
    work
    __registry__      the list of queues and their metadata
```

## Behaviour

### Tables

- **One index per table,** with its own index writer and a document per entity.
- **The whole table API:** add, get, upsert, update and delete with ETag checks, merge and replace, queries by OData filter or expression, and transactions.
- **A change is one step.** The index's write lock is held from the first read to the commit, so an ETag check and the write it guards cannot be separated. A transaction that fails leaves the table as it was.
- **A query reads every entity** of the table and then filters. That suits tables of modest size; it is not an index lookup.

### Blobs

- **One directory per container,** with a file per blob.
- **Supported:** upload (with overwrite, If-Match or If-None-Match), download to memory or as a stream, delete, exists, properties with content type and metadata, and listing flat or by hierarchy. Download and delete honour If-Match.
- **Not supported:** block, append and page blob operations, leases, tags, snapshots and copies.

### Queues

- **One index per queue,** with its own index writer and a document per message.
- **First in, first out,** across processes. A message whose visibility timeout runs out goes back to the place it had.
- **Receiving hides a message** from every other receiver until its visibility timeout runs out. Two receivers are never given the same message.
- **Pop receipts** are checked on delete and update, and change on every receive and update. The receipt returned by a send deletes the message for as long as nobody has received it.
- **Time to live** defaults to seven days; `TimeSpan.FromSeconds(-1)` means never.

### Durability and sharing

Every table and queue operation opens its index's writer, does its work, commits and closes the writer. Nothing is lost if the process dies between operations, and several clients and processes can share the same folder.

- **Clients in one process take turns.** All clients on the same folder write to a table or queue one at a time, in the order they asked, so a busy client cannot shut another out.
- **Processes do not queue; they retry.** An index can only be written by one process at a time, and Lucene's write lock cannot be waited on, only tried. An operation that finds the index held by another process tries again every few tens of milliseconds for up to ten seconds. That works well when processes write now and then. Under sustained writing from one process, another can lose every try; its call then fails with `RequestFailedException`, status `503`, error code `ServerBusy` (the `LockObtainFailedException` is the inner exception), and can be retried like the same error from Azure.
- **Receiving from an empty queue does not write,** so idle receivers do not hold up senders.
- **Blobs are not coordinated between clients:** two clients or processes writing the same blob at the same moment are not serialised.

### Differences from Azure Storage

- No size limits are enforced on entities, blobs or messages, and `maxMessages` is not capped at 32.
- For queues, a wrong pop receipt and a missing message both give `404 MessageNotFound`.
- Message text is stored as given; `QueueClientOptions.MessageEncoding` is not applied.

## Related Packages

| Package | Description |
|---------|-------------|
| [Iciclecreek.Azure.Storage.Memory](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Memory) | Thread-safe in-memory implementation (fastest, no I/O) |
| [Iciclecreek.Azure.Storage.FileSystem](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.FileSystem) | Filesystem-backed implementation (files on disk) |
| [Iciclecreek.Azure.Storage.SQLite](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.SQLite) | SQLite-backed implementation (one portable `.db` file) |
| [Iciclecreek.Azure.Storage.Server](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Server) | ASP.NET Core REST API server on top of any provider |

## License

MIT
