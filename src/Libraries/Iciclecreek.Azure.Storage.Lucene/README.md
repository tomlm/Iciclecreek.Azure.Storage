# Iciclecreek.Azure.Storage.Lucene

Drop-in replacements for the Azure Storage SDK clients that keep their data in
[Lucene.NET](https://lucenenet.apache.org/) directories. Code written against
`TableServiceClient`, `BlobServiceClient` and `QueueServiceClient` runs unchanged.

```csharp
using Iciclecreek.Azure.Storage.Lucene.Tables;
using Iciclecreek.Azure.Storage.Lucene.Blobs;
using Iciclecreek.Azure.Storage.Lucene.Queues;

// In a folder
TableServiceClient tables = new LuceneTableServiceClient(@"C:\data");
BlobServiceClient blobs = new LuceneBlobServiceClient(@"C:\data");
QueueServiceClient queues = new LuceneQueueServiceClient(@"C:\data");

// In memory
var queues = new LuceneQueueServiceClient();

// In any Lucene Directory: the factory is given a path and returns the directory for it
var tables = new LuceneTableServiceClient(path => OpenDirectoryFor(path));
```

## Layout

Each table, container and queue is a directory of its own under the one supplied. The three
clients can share a folder.

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

## Tables

- **One index per table,** with its own index writer and a document per entity. An entity's
  properties are kept as JSON beside its ETag and timestamp.
- **The whole table API:** add, get, upsert, update and delete with ETag checks, merge and
  replace, queries by OData filter or expression, and transactions.
- **A change is one step.** The index's write lock is held from the first read to the commit,
  so an ETag check and the write it guards cannot be separated, in this process or another. A
  transaction that fails leaves the table as it was.
- **A query reads every entity** of the table and then filters. That suits tables of modest
  size; it is not an index lookup.

## Blobs

- **One directory per container,** with a file per blob.
- **Supported:** upload (with overwrite or If-None-Match), download to memory or as a stream,
  delete, exists, properties with content type and metadata, and listing flat or by hierarchy.
- **Not supported:** block, append and page blob operations, leases, tags, snapshots and copies.

## Queues

- **One index per queue,** with its own index writer and a document per message.
- **First in, first out.** Each message is given a place in line that is kept with the index, so
  the order holds across processes. A message whose visibility timeout runs out goes back to the
  place it had.
- **Receiving hides a message** from every other receiver until its visibility timeout runs
  out. Two receivers are never given the same message.
- **Pop receipts** are checked on delete and update, and change on every receive and update.
  The receipt returned by a send deletes the message for as long as nobody has received it.
- **Time to live** defaults to seven days; `TimeSpan.FromSeconds(-1)` means never.

## Durability and sharing

Every table and queue operation opens its index's writer, does its work, commits and closes
the writer. Nothing is lost if the process dies between operations, and several processes can
share the same folder: each waits up to ten seconds for another to finish.

## Differences from Azure Storage

- No size limits are enforced on entities, blobs or messages, and `maxMessages` is not capped at 32.
- For queues, a wrong pop receipt and a missing message both give `404 MessageNotFound`.
- Message text is stored as given; `QueueClientOptions.MessageEncoding` is not applied.
