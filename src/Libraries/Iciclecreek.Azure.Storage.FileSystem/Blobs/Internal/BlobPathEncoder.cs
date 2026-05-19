using System.Text;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;

internal static class BlobPathEncoder
{
    private static readonly char[] _illegal = { '<', '>', ':', '"', '|', '?', '*' };

    private static readonly HashSet<string> _reserved = new(StringComparer.OrdinalIgnoreCase)
    {
        "CON", "PRN", "AUX", "NUL",
        "COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
        "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9",
    };

    public static string ToRelativePath(string blobName)
    {
        if (string.IsNullOrEmpty(blobName))
            throw new ArgumentException("Blob name cannot be empty.", nameof(blobName));

        var segments = blobName.Split('/');
        var encoded = new string[segments.Length];
        for (var i = 0; i < segments.Length; i++)
            encoded[i] = EncodeSegment(segments[i]);
        return string.Join(Path.DirectorySeparatorChar, encoded);
    }

    public static string SidecarPath(string fullBlobPath) => fullBlobPath + ".meta.json";

    public static bool IsInternalFile(string fileName)
        => fileName.EndsWith(".meta.json", StringComparison.OrdinalIgnoreCase)
        || fileName.EndsWith("._pending", StringComparison.OrdinalIgnoreCase)
        || fileName.Contains(".version.", StringComparison.OrdinalIgnoreCase)
        || fileName.Contains(".snapshot.", StringComparison.OrdinalIgnoreCase);

    public static string FromRelativePath(string relativePath)
    {
        var segments = relativePath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var sb = new StringBuilder();
        for (var i = 0; i < segments.Length; i++)
        {
            if (i > 0) sb.Append('/');
            sb.Append(DecodeSegment(segments[i]));
        }
        return sb.ToString();
    }

    public static string EncodeBlockId(string blockId) => Uri.EscapeDataString(blockId);

    public static string DecodeBlockId(string encoded) => Uri.UnescapeDataString(encoded);

    private static string EncodeSegment(string segment)
    {
        if (string.IsNullOrEmpty(segment)) return "_";

        var needsEscape = false;
        foreach (var c in segment)
        {
            if (c < 32 || Array.IndexOf(_illegal, c) >= 0)
            {
                needsEscape = true;
                break;
            }
        }
        if (!needsEscape && segment[^1] != '.' && segment[^1] != ' ' && !_reserved.Contains(segment))
            return segment;

        var sb = new StringBuilder(segment.Length);
        foreach (var c in segment)
        {
            if (c < 32 || Array.IndexOf(_illegal, c) >= 0)
                sb.Append('%').Append(((int)c).ToString("X2"));
            else
                sb.Append(c);
        }
        if (_reserved.Contains(sb.ToString()))
            sb.Insert(0, "_");
        if (sb[^1] == '.' || sb[^1] == ' ')
            sb.Append('_');
        return sb.ToString();
    }

    private static string DecodeSegment(string segment)
    {
        if (segment.Length > 0 && segment[0] == '_' && _reserved.Contains(segment[1..].TrimEnd('_')))
            segment = segment[1..];
        if (segment.Length > 0 && segment[^1] == '_')
            segment = segment[..^1];

        var sb = new StringBuilder(segment.Length);
        for (var i = 0; i < segment.Length; i++)
        {
            if (segment[i] == '%' && i + 2 < segment.Length &&
                int.TryParse(segment.AsSpan(i + 1, 2), System.Globalization.NumberStyles.HexNumber, null, out var v))
            {
                sb.Append((char)v);
                i += 2;
            }
            else
            {
                sb.Append(segment[i]);
            }
        }
        return sb.ToString();
    }
}
