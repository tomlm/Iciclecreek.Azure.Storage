using System.Text.Json;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;

internal sealed class BlockStagingStore
{
    private readonly string _stagingDir;
    private readonly JsonSerializerOptions _jsonOpts;

    public BlockStagingStore(BlobStore store, string blobName)
    {
        _stagingDir = Path.Combine(store.ContainerPath, ".blocks", BlobPathEncoder.EncodeBlockId(blobName));
        _jsonOpts = store.Options.JsonSerializerOptions;
    }

    public string StagingDir => _stagingDir;

    public async Task StageAsync(string base64BlockId, Stream content, CancellationToken ct = default)
    {
        Directory.CreateDirectory(_stagingDir);
        var encoded = BlobPathEncoder.EncodeBlockId(base64BlockId);
        var blockPath = Path.Combine(_stagingDir, encoded + ".block");

        await using (var fs = new FileStream(blockPath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true))
        {
            await content.CopyToAsync(fs, ct).ConfigureAwait(false);
            await fs.FlushAsync(ct).ConfigureAwait(false);
        }

        var index = await ReadIndexAsync(ct).ConfigureAwait(false);
        index[base64BlockId] = new StagedBlockInfo { Size = new FileInfo(blockPath).Length, StagedAtUtc = DateTimeOffset.UtcNow };
        await WriteIndexAsync(index, ct).ConfigureAwait(false);
    }

    public async Task<Stream?> OpenBlockAsync(string base64BlockId)
    {
        var encoded = BlobPathEncoder.EncodeBlockId(base64BlockId);
        var blockPath = Path.Combine(_stagingDir, encoded + ".block");
        if (!File.Exists(blockPath)) return null;
        return new FileStream(blockPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
    }

    public async Task<Dictionary<string, StagedBlockInfo>> ReadIndexAsync(CancellationToken ct = default)
    {
        var indexPath = Path.Combine(_stagingDir, "staging.json");
        if (!File.Exists(indexPath)) return new();
        await using var fs = new FileStream(indexPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await JsonSerializer.DeserializeAsync<Dictionary<string, StagedBlockInfo>>(fs, _jsonOpts, ct).ConfigureAwait(false) ?? new();
    }

    public async Task WriteIndexAsync(Dictionary<string, StagedBlockInfo> index, CancellationToken ct = default)
    {
        Directory.CreateDirectory(_stagingDir);
        var indexPath = Path.Combine(_stagingDir, "staging.json");
        await using var fs = new FileStream(indexPath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, useAsync: true);
        await JsonSerializer.SerializeAsync(fs, index, _jsonOpts, ct).ConfigureAwait(false);
    }

    public void Cleanup()
    {
        if (Directory.Exists(_stagingDir))
            Directory.Delete(_stagingDir, recursive: true);
    }
}

internal sealed class StagedBlockInfo
{
    public long Size { get; set; }
    public DateTimeOffset StagedAtUtc { get; set; }
}
