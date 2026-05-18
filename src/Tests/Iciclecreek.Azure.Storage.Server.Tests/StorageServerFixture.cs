using System.Net;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Iciclecreek.Azure.Storage.Server.Tests;

/// <summary>
/// Starts a real Kestrel-based storage server backed by the FileSystem provider.
/// Uses port 0 (OS-assigned) to avoid conflicts when tests run in parallel.
/// Shared across all tests in the assembly via [SetUpFixture].
/// </summary>
[SetUpFixture]
public class StorageServerFixture
{
    public const string AccountName = "testaccount1";

    private static WebApplication? _app;
    private static string? _storagePath;
    public static HttpClient BlobHttp { get; private set; } = null!;
    public static HttpClient QueueHttp { get; private set; } = null!;
    public static HttpClient TableHttp { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task GlobalSetup()
    {
        _storagePath = Path.Combine(Path.GetTempPath(), $"StorageServerTests_{Guid.NewGuid():N}");

        var builder = WebApplication.CreateBuilder();

        builder.Services.AddSingleton<BlobServiceClient>(
            new FileBlobServiceClient(_storagePath));
        builder.Services.AddSingleton<QueueServiceClient>(
            new FileQueueServiceClient(_storagePath));
        builder.Services.AddSingleton<TableServiceClient>(
            new FileTableServiceClient(_storagePath));

        builder.Services.AddStorageServer();

        // Use port 0 so the OS assigns free ports — avoids conflicts in parallel test runs
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.Listen(IPAddress.Loopback, 0);
            k.Listen(IPAddress.Loopback, 0);
            k.Listen(IPAddress.Loopback, 0);
        });

        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Error);

        _app = builder.Build();
        _app.MapStorageServer();

        await _app.StartAsync();

        // Read back the OS-assigned ports
        var server = _app.Services.GetRequiredService<IServer>();
        var addresses = server.Features.Get<IServerAddressesFeature>()!
            .Addresses.Select(a => new Uri(a)).OrderBy(u => u.Port).ToArray();

        var blobPort = addresses[0].Port;
        var queuePort = addresses[1].Port;
        var tablePort = addresses[2].Port;

        // Tell the server which port maps to which service (read at request time by ServicePortConstraint)
        _app.Configuration["BlobPort"] = blobPort.ToString();
        _app.Configuration["QueuePort"] = queuePort.ToString();
        _app.Configuration["TablePort"] = tablePort.ToString();

        BlobHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{blobPort}") };
        QueueHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{queuePort}") };
        TableHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{tablePort}") };
    }

    [OneTimeTearDown]
    public async Task GlobalTeardown()
    {
        BlobHttp?.Dispose();
        QueueHttp?.Dispose();
        TableHttp?.Dispose();
        if (_app != null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
        if (_storagePath != null && Directory.Exists(_storagePath))
        {
            try { Directory.Delete(_storagePath, true); } catch { }
        }
    }
}
