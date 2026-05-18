namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class AtomicFile
{
    private const int MaxRetries = 50;
    private const int RetryDelayMs = 10;

    public static async Task WriteAllBytesAsync(string path, byte[] bytes, CancellationToken ct = default)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        await RetryOnLockAsync(async () =>
        {
            await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
            await fs.WriteAsync(bytes, ct).ConfigureAwait(false);
            await fs.FlushAsync(ct).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public static async Task WriteAllTextAsync(string path, string text, CancellationToken ct = default)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        await RetryOnLockAsync(async () =>
        {
            await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
            await using var writer = new StreamWriter(fs);
            await writer.WriteAsync(text.AsMemory(), ct).ConfigureAwait(false);
            await writer.FlushAsync().ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public static async Task WriteStreamAsync(string path, Stream content, CancellationToken ct = default)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        await RetryOnLockAsync(async () =>
        {
            await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true);
            await content.CopyToAsync(fs, ct).ConfigureAwait(false);
            await fs.FlushAsync(ct).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Atomically read a file, apply a transform, and write back — all under exclusive lock.
    /// The transform receives the current file content and returns the new content.
    /// If the transform throws, the file is not modified.
    /// </summary>
    public static async Task ReadCheckWriteAsync(string path, Func<string, string> transform, CancellationToken ct = default)
    {
        await RetryOnLockAsync(async () =>
        {
            await using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using var reader = new StreamReader(fs, System.Text.Encoding.UTF8, true, 4096, leaveOpen: true);
            var existingText = await reader.ReadToEndAsync().ConfigureAwait(false);

            var newText = transform(existingText);

            fs.SetLength(0);
            fs.Seek(0, SeekOrigin.Begin);
            await using var writer = new StreamWriter(fs, System.Text.Encoding.UTF8, 4096, leaveOpen: true);
            await writer.WriteAsync(newText.AsMemory(), ct).ConfigureAwait(false);
            await writer.FlushAsync().ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Read a file with retry if another thread holds an exclusive lock.
    /// </summary>
    public static async Task<string> ReadAllTextAsync(string path, CancellationToken ct = default)
    {
        return await RetryOnLockAsync(async () =>
        {
            return await File.ReadAllTextAsync(path, ct).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    private static async Task<T> RetryOnLockAsync<T>(Func<Task<T>> action, CancellationToken ct)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                return await action().ConfigureAwait(false);
            }
            catch (IOException) when (attempt < MaxRetries - 1)
            {
                await Task.Delay(RetryDelayMs, ct).ConfigureAwait(false);
            }
        }
        throw new InvalidOperationException("Should not reach here");
    }

    private static async Task RetryOnLockAsync(Func<Task> action, CancellationToken ct)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                await action().ConfigureAwait(false);
                return;
            }
            catch (IOException) when (attempt < MaxRetries - 1)
            {
                await Task.Delay(RetryDelayMs, ct).ConfigureAwait(false);
            }
        }
    }
}
