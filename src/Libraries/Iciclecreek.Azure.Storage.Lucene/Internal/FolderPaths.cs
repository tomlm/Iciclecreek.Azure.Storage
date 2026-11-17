using Lucene.Net.Store;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>
/// The Lucene <c>Directory</c> objects for a client given a folder, and the paths they are opened on.
/// <para>
/// On Windows each folder is opened by its extended path (<c>\\?\C:\…</c>, or <c>\\?\UNC\server\share\…</c>).
/// .NET itself reads and writes files past 260 characters without that, but Lucene.Net syncs a
/// file through its own call to Windows with the path as given. That call fails past 260
/// characters unless the program's manifest declares it long-path aware, which test hosts do and
/// an ordinary .NET program does not; and a queued message's file name alone is over 100.
/// </para>
/// </summary>
internal static class FolderPaths
{
    private const string Extended = @"\\?\";
    private const string ExtendedUnc = @"\\?\UNC\";

    /// <summary>A factory giving an <see cref="FSDirectory"/> on <c>{rootPath}/{path}</c> for each path it is asked for.</summary>
    /// <exception cref="ArgumentException"><paramref name="rootPath"/> is empty.</exception>
    public static Func<string, LuceneDirectory> InFolder(string rootPath)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
            throw new ArgumentException("A folder is needed.", nameof(rootPath));
        var root = ToExtended(Path.GetFullPath(rootPath));
        return path => FSDirectory.Open(Path.Combine(root, path.Replace('/', Path.DirectorySeparatorChar)));
    }

    /// <summary>
    /// The extended form of a full path on Windows; the path as it is anywhere else, or when it is
    /// already extended or a device path.
    /// </summary>
    public static string ToExtended(string fullPath)
    {
        if (!OperatingSystem.IsWindows() || fullPath.StartsWith(Extended, StringComparison.Ordinal) || fullPath.StartsWith(@"\\.\", StringComparison.Ordinal))
            return fullPath;
        return fullPath.StartsWith(@"\\", StringComparison.Ordinal)
            ? ExtendedUnc + fullPath[2..]
            : Extended + fullPath;
    }

    /// <summary>A path without its extended prefix, so that both forms of one folder compare equal.</summary>
    public static string WithoutExtended(string path)
    {
        if (path.StartsWith(ExtendedUnc, StringComparison.OrdinalIgnoreCase))
            return @"\\" + path[ExtendedUnc.Length..];
        return path.StartsWith(Extended, StringComparison.Ordinal) ? path[Extended.Length..] : path;
    }
}
