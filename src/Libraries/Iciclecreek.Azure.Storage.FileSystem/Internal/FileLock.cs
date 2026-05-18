namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class FileLock
{
    public static async Task<FileStream> AcquireAsync(
        string path,
        FileMode mode,
        FileAccess access,
        int retryCount = 50,
        TimeSpan retryDelay = default)
    {
        if (retryDelay == default) retryDelay = TimeSpan.FromMilliseconds(20);

        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        Exception? last = null;
        for (var i = 0; i <= retryCount; i++)
        {
            try
            {
                return new FileStream(path, mode, access, FileShare.None, 4096, useAsync: true);
            }
            catch (IOException ex)
            {
                last = ex;
                await Task.Delay(retryDelay).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException ex) when (mode == FileMode.OpenOrCreate || mode == FileMode.Create)
            {
                last = ex;
                await Task.Delay(retryDelay).ConfigureAwait(false);
            }
        }
        throw new IOException($"Unable to acquire exclusive lock on '{path}' after {retryCount} attempts.", last);
    }

    public static Task<FileStream> AcquireAsync(string path, FileMode mode, FileAccess access, FileStorageOptions options)
        => AcquireAsync(path, mode, access, options.LockRetryCount, options.LockRetryDelay);
}
