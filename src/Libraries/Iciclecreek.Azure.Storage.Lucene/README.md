![Logo](https://raw.githubusercontent.com/tomlm/Iciclecreek.Azure.Storage/refs/heads/main/icon.png)

[![Build](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/Iciclecreek.Azure.Storage/actions/workflows/BuildAndRunTests.yml) [![NuGet](https://img.shields.io/nuget/v/Iciclecreek.Azure.Storage.Lucene.svg)](https://www.nuget.org/packages/Iciclecreek.Azure.Storage.Lucene)

# Iciclecreek.Azure.Storage.Lucene
A **Lucene.NET-backed drop-in replacement** for `Azure.Data.Tables` and `Azure.Storage.Queues` clients, and for the core of `Azure.Storage.Blobs`. Use the same Azure SDK types without Azurite or a live Azure account.

Data is kept through the [Lucene.NET](https://lucenenet.apache.org/) `Directory` abstraction (`Lucene.Net.Store.Directory`): in memory (`RAMDirectory`), in a folder (`FSDirectory`), or in any Lucene `Directory` you supply. Each table, container and queue is a Lucene `Directory` of its own.

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

### In memory, or in any Lucene `Directory`

```csharp
// In memory: gone when the client is
var queueService = new LuceneQueueServiceClient();

// Anywhere: the factory is given a path such as "queues/tasks" and returns the Lucene Directory for it
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

Each table, container and queue is a Lucene `Directory` of its own. Given a folder, each is an `FSDirectory` on a folder of its own inside it, and the three clients can share that folder.

```
{directory}
  tables
    people
    orders
    __registry__      the list of tables
  blobs
    photos
    documents
  queues
    work
    __registry__      the list of queues and their metadata
```

### Names

A name made of lowercase letters, digits and hyphens is its own directory name. Anything else is made safe first, so that two names never share a directory and no name can point outside the folder:

- **Table names are not case sensitive,** as in Azure: `People` and `people` are one table, listed under the name it was created with, in the directory `people`.
- **Queue and container names are case sensitive.** `Work` and `work` are two queues, on any file system.
- **Any other character** is written in the directory name as `_` and its four-digit hexadecimal code: the queue `Work` is the directory `_0057ork`, and a name such as `../x` is the single directory `_002e_002e_002fx`. The Azure naming rules are not otherwise enforced.
- **`__registry__` and the empty name are refused** with `400 InvalidResourceName`. So is a name whose directory name would be longer than 255 characters, the most a folder name can be.

## Behaviour

### Tables

- **One Lucene index per table,** in a Lucene `Directory` of its own, with its own index writer and a document per entity.
- **The whole table API:** add, get, upsert, update and delete with ETag checks, merge and replace, queries by OData filter or expression, and transactions.
- **A change is one step.** The index's write lock is held from the first read to the commit, so an ETag check and the write it guards cannot be separated. A transaction that fails leaves the table as it was.
- **A query reads every entity** of the table and then filters. That suits tables of modest size; it is not an index lookup.

### Blobs

- **One Lucene `Directory` per container,** with a file per blob, written and read through the `Directory`'s own file API. There is no index.
- **Supported:** upload (with overwrite, If-Match or If-None-Match), download to memory or as a stream, delete, exists, properties with content type and metadata, and listing flat or by hierarchy. Download and delete honour If-Match.
- **Containers are not stored.** A container is only the place its blobs are in: every container exists, creating one always succeeds, and deleting one deletes its blobs. There is no list of containers, so `GetBlobContainers` throws `NotSupportedException`.
- **Not supported:** block, append and page blob operations, leases, tags, snapshots and copies. `OpenRead`, `Download` and `DownloadTo` to a file path or with options throw `NotSupportedException`; use `DownloadContent`, `DownloadStreaming` or `DownloadTo(Stream)`.
- **Dispose the service client** to close the directories it opened, as with the table and queue service clients.

### Queues

- **One Lucene `Directory` per queue,** with a file per message and no index. A queue uses only what the `Directory` abstraction gives (list, create, read, delete and sync a file, and its lock), so it works on any Lucene `Directory`, not only a folder. Everything about a message that changes is in the name of its file, so the messages to receive are found by listing the `Directory`, with no file opened, and a pop receipt names its message's file outright.
- **First in, first out,** in the order the messages were sent by the clock. A message whose visibility timeout runs out goes back to the place it had.
- **Receiving lists the queue's Lucene `Directory`,** so one receive costs more the more messages are waiting. Receiving several messages at a time spreads that cost.
- **Receiving hides a message** from every other receiver until its visibility timeout runs out. Two receivers are never given the same message.
- **Pop receipts** are checked on delete and update, and change on every receive and update. The receipt returned by a send deletes the message for as long as nobody has received it.
- **Time to live** defaults to seven days; `TimeSpan.FromSeconds(-1)` means never.

### Durability and sharing

Every table operation opens its index's writer, does its work, commits and closes the writer. Every queue operation that changes a queue takes the write lock of the queue's Lucene `Directory`, writes the files it changes through to disk, and lets the lock go. Nothing is lost if the process dies between operations, and several clients and processes can share the same folder.

- **A process that dies while changing a message** leaves the message as it was, or as it was to become. What it left unfinished is cleared away by the next receive.
- **A process that dies while making a queue** can leave it half made: it exists and takes messages, but is not in `GetQueues`. The next `Create`, `CreateIfNotExists` or `SetMetadata` for it finishes making it, and keeps its messages.
- **Clients in one process take turns.** All clients on the same folder write to a table or queue one at a time, in the order they asked, so a busy client cannot shut another out.
- **Processes do not queue; they retry.** A table or queue can only be written by one process at a time, and Lucene's write lock cannot be waited on, only tried. An operation that finds the table or queue held by another process tries again every few tens of milliseconds for up to ten seconds. That works well when processes write now and then. Under sustained writing from one process, another can lose every try; its call then fails with `RequestFailedException`, status `503`, error code `ServerBusy` (for a table, the `LockObtainFailedException` is the inner exception), and can be retried like the same error from Azure.
- **Receiving from an empty queue takes no lock,** so idle receivers do not hold up senders.
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
