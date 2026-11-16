using System.Text;
using Azure;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>
/// Turns the name of a table, queue or container into the name of the directory it is kept in.
/// <para>
/// A name is the caller's, and a directory name ends up as a folder, so it is never used as it
/// stands: two names must never mean the same folder (as <c>Orders</c> and <c>orders</c> do on
/// Windows and macOS), a name must not be the registry's own folder, and a name must not be able
/// to point outside the folder the client was given.
/// </para>
/// </summary>
internal static class StorageNames
{
    /// <summary>The directory that lists the tables or queues; nothing else may be called this.</summary>
    public const string Registry = "__registry__";

    // Folder names that Windows gives to devices, whatever folder they are in.
    private static readonly HashSet<string> s_deviceNames = new(StringComparer.Ordinal)
    {
        "con", "prn", "aux", "nul",
        "com0", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
        "lpt0", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
    };

    /// <summary>
    /// The directory name for a name. Lowercase letters, digits and hyphens stand for themselves,
    /// so a name that follows the Azure rules for queues and containers is its own directory name.
    /// Every other character is written as an underscore and its four-digit hexadecimal code, so
    /// that different names always give different directory names, on any file system, and the
    /// result holds nothing a file system reads as a path.
    /// </summary>
    /// <exception cref="RequestFailedException">400 InvalidResourceName: the name is empty or reserved.</exception>
    public static string Directory(string name)
    {
        Check(name);

        var escaped = new StringBuilder(name.Length);
        foreach (var c in name)
        {
            if (c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-')
                escaped.Append(c);
            else
                escaped.Append(Code(c));
        }

        var directory = escaped.ToString();
        // A device name is told apart by writing its first letter as a code, which is done to no other letter.
        return s_deviceNames.Contains(directory) ? Code(directory[0]) + directory[1..] : directory;
    }

    /// <summary>Refuses a name that is empty or is the registry's.</summary>
    /// <exception cref="RequestFailedException">400 InvalidResourceName.</exception>
    public static string Check(string name)
    {
        if (string.IsNullOrEmpty(name) || string.Equals(name, Registry, StringComparison.OrdinalIgnoreCase))
            throw new RequestFailedException(400, $"The resource name '{name}' is empty or reserved.", "InvalidResourceName", null);
        return name;
    }

    private static string Code(char c) => "_" + ((int)c).ToString("x4");
}
