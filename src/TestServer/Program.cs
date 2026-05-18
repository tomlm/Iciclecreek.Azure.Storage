using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.Server;

var builder = WebApplication.CreateBuilder(args);

// ── Storage root ────────────────────────────────────────────────────────
var storagePath = builder.Configuration["StoragePath"]
    ?? Path.Combine(Path.GetTempPath(), "__storage_data");

// ── Register Azure SDK clients backed by the file system ────────────────
builder.Services.AddSingleton<BlobServiceClient>(new FileBlobServiceClient(Path.Combine(storagePath, "blobs")));
builder.Services.AddSingleton<TableServiceClient>(new FileTableServiceClient(Path.Combine(storagePath, "tables")));
builder.Services.AddSingleton<QueueServiceClient>(new FileQueueServiceClient(Path.Combine(storagePath, "queues")));

// ── Add the Storage REST API controllers ────────────────────────────────
builder.Services.AddStorageServer();

// ── Kestrel: 3 ports (Azurite-compatible) ───────────────────────────────
var blobPort = builder.Configuration.GetValue("BlobPort", 10000);
var queuePort = builder.Configuration.GetValue("QueuePort", 10001);
var tablePort = builder.Configuration.GetValue("TablePort", 10002);

builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenAnyIP(blobPort);
    k.ListenAnyIP(queuePort);
    k.ListenAnyIP(tablePort);
});

var app = builder.Build();

app.MapStorageServer();
app.Run();
