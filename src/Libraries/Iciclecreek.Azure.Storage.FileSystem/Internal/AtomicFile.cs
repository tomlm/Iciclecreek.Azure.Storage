using System.Runtime.InteropServices;

namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class AtomicFile
{
    private const int MaxRetries = 50;
    private const int RetryDelayMs = 10;

    private static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    /// <summary>
    /// The sharing that makes an open in <see cref="ReadModifyWriteAsync{T}"/> exclusive. On Windows
    /// that is <see cref="FileShare.Delete"/>: it shuts out every other reader and writer and still
    /// lets the holder delete the file while it is open. Elsewhere .NET takes an exclusive lock only
    /// for <see cref="FileShare.None"/> (any other value is a shared lock, which a second caller is
    /// given too), and an open file can be deleted whatever the sharing.
    /// </summary>
    private static readonly FileShare ExclusiveShare = IsWindows ? FileShare.Delete : FileShare.None;

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
            // Not cancellable: the file is empty at this point, so stopping here would
            // destroy the content rather than leave it unchanged.
            await writer.WriteAsync(newText.AsMemory(), CancellationToken.None).ConfigureAwait(false);
            await writer.FlushAsync().ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    /// <summary>What <see cref="ReadModifyWriteAsync{T}"/> does with the file after reading it.</summary>
    public enum Change
    {
        /// <summary>Leave the file as it is.</summary>
        Keep,
        /// <summary>Replace the file's content.</summary>
        Write,
        /// <summary>Delete the file.</summary>
        Delete,
    }

    /// <summary>
    /// Read a file, decide, and then keep, rewrite or delete it — all while holding the file
    /// exclusively, so two callers (threads or processes) can never both act on the same content.
    /// The hold is a share mode on Windows and an advisory lock elsewhere (see
    /// <see cref="ExclusiveShare"/>), so it excludes other callers of this class and other .NET
    /// code, not every program. Returns <paramref name="missing"/> when the file does not exist.
    /// </summary>
    /// <param name="decide">Given the file's text, says what to do with it and what to return.</param>
    public static async Task<T> ReadModifyWriteAsync<T>(
        string path, Func<string, (Change Change, string Text, T Result)> decide, T missing, CancellationToken ct = default)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                // The holder may delete the file while it is open; no other caller can read or write it meanwhile.
                using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, ExclusiveShare))
                {
                    // Off Windows the file is opened first and locked second, so the previous holder may have
                    // deleted it in between: what is open then is a file that no longer has a name.
                    if (!IsWindows && !File.Exists(path))
                        return missing;

                    string text;
                    using (var reader = new StreamReader(fs, System.Text.Encoding.UTF8, true, 4096, leaveOpen: true))
                        text = await reader.ReadToEndAsync().ConfigureAwait(false);

                    var (change, newText, result) = decide(text);
                    if (change == Change.Delete)
                    {
                        File.Delete(path);
                    }
                    else if (change == Change.Write)
                    {
                        fs.SetLength(0);
                        fs.Seek(0, SeekOrigin.Begin);
                        // No byte-order mark: the file must read back exactly as the text written.
                        using (var writer = new StreamWriter(fs, new System.Text.UTF8Encoding(false), 4096, leaveOpen: true))
                        {
                            // Not cancellable: the file is empty at this point, so stopping here would
                            // destroy the content rather than leave it unchanged.
                            await writer.WriteAsync(newText.AsMemory(), CancellationToken.None).ConfigureAwait(false);
                            await writer.FlushAsync().ConfigureAwait(false);
                        }
                    }
                    return result;
                }
            }
            catch (FileNotFoundException)
            {
                return missing;
            }
            catch (DirectoryNotFoundException)
            {
                return missing;
            }
            catch (Exception ex) when ((ex is IOException || ex is UnauthorizedAccessException) && attempt < MaxRetries - 1)
            {
                // Held by someone else; wait for them. On Windows a file that its holder has deleted but not
                // yet closed refuses to open with access denied, not a sharing violation; by the next
                // attempt it is gone and the caller gets `missing`.
                await Task.Delay(RetryDelayMs, ct).ConfigureAwait(false);
            }
        }
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
