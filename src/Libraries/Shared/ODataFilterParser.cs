using Azure.Data.Tables;

namespace Iciclecreek.Azure.Storage.Internal;

internal static class ODataFilterParser
{
    public static Func<TableEntity, bool> Parse(string filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
            return _ => true;
        var tokens = Tokenize(filter);
        var pos = 0;
        var expr = ParseOr(tokens, ref pos);
        return expr;
    }

    private static Func<TableEntity, bool> ParseOr(List<Token> tokens, ref int pos)
    {
        var left = ParseAnd(tokens, ref pos);
        while (pos < tokens.Count && tokens[pos].Value == "or")
        {
            pos++;
            var right = ParseAnd(tokens, ref pos);
            var l = left; var r = right;
            left = e => l(e) || r(e);
        }
        return left;
    }

    private static Func<TableEntity, bool> ParseAnd(List<Token> tokens, ref int pos)
    {
        var left = ParseNot(tokens, ref pos);
        while (pos < tokens.Count && tokens[pos].Value == "and")
        {
            pos++;
            var right = ParseNot(tokens, ref pos);
            var l = left; var r = right;
            left = e => l(e) && r(e);
        }
        return left;
    }

    private static Func<TableEntity, bool> ParseNot(List<Token> tokens, ref int pos)
    {
        if (pos < tokens.Count && tokens[pos].Value == "not")
        {
            pos++;
            var inner = ParsePrimary(tokens, ref pos);
            return e => !inner(e);
        }
        return ParsePrimary(tokens, ref pos);
    }

    private static Func<TableEntity, bool> ParsePrimary(List<Token> tokens, ref int pos)
    {
        if (pos < tokens.Count && tokens[pos].Value == "(")
        {
            pos++; // skip '('
            var expr = ParseOr(tokens, ref pos);
            if (pos < tokens.Count && tokens[pos].Value == ")")
                pos++; // skip ')'
            return expr;
        }

        // Comparison: property op literal
        if (pos + 2 >= tokens.Count)
            throw new NotSupportedException($"Unexpected end of filter expression.");

        var propToken = tokens[pos++];
        var opToken = tokens[pos++];
        var valueToken = tokens[pos++];

        var propName = propToken.Value;
        var op = opToken.Value;
        var literal = ParseLiteral(valueToken);

        return entity =>
        {
            object? entityValue;
            if (propName == "PartitionKey") entityValue = entity.PartitionKey;
            else if (propName == "RowKey") entityValue = entity.RowKey;
            else if (propName == "Timestamp") entityValue = entity.Timestamp;
            else entityValue = entity.ContainsKey(propName) ? entity[propName] : null;

            return CompareValues(entityValue, literal, op);
        };
    }

    private static object? ParseLiteral(Token token)
    {
        var v = token.Value;

        // String literal: 'value'
        if (v.StartsWith('\'') && v.EndsWith('\''))
            return v[1..^1].Replace("''", "'");

        // Boolean
        if (v == "true") return true;
        if (v == "false") return false;

        // datetime literal: datetime'...'
        if (v.StartsWith("datetime'", StringComparison.OrdinalIgnoreCase) && v.EndsWith('\''))
            return DateTimeOffset.Parse(v[9..^1]);

        // guid literal: guid'...'
        if (v.StartsWith("guid'", StringComparison.OrdinalIgnoreCase) && v.EndsWith('\''))
            return Guid.Parse(v[5..^1]);

        // binary literal: binary'<hex>' (Azure SDK OData format)
        if (v.StartsWith("binary'", StringComparison.OrdinalIgnoreCase) && v.EndsWith('\''))
        {
            var hex = v[7..^1];
            var bytes = new byte[hex.Length / 2];
            for (var idx = 0; idx < bytes.Length; idx++)
                bytes[idx] = Convert.ToByte(hex.Substring(idx * 2, 2), 16);
            return bytes;
        }

        // binary literal: X'<hex>' (Azure Table Storage OData format)
        if (v.StartsWith("X'", StringComparison.OrdinalIgnoreCase) && v.EndsWith('\''))
        {
            var hex = v[2..^1];
            var bytes = new byte[hex.Length / 2];
            for (var idx = 0; idx < bytes.Length; idx++)
                bytes[idx] = Convert.ToByte(hex.Substring(idx * 2, 2), 16);
            return bytes;
        }

        // long (contains L suffix)
        if (v.EndsWith('L') && long.TryParse(v[..^1], out var l))
            return l;

        // double (contains dot)
        if (v.Contains('.') && double.TryParse(v, out var d))
            return d;

        // int
        if (int.TryParse(v, out var i))
            return i;

        // long fallback
        if (long.TryParse(v, out var l2))
            return l2;

        throw new NotSupportedException($"Unsupported literal: '{v}'");
    }

    private static bool CompareValues(object? left, object? right, string op)
    {
        if (left is null && right is null) return op is "eq";
        if (left is null || right is null) return op is "ne";

        var cmp = CompareOrdered(left, right);

        return op switch
        {
            "eq" => cmp == 0,
            "ne" => cmp != 0,
            "gt" => cmp > 0,
            "ge" => cmp >= 0,
            "lt" => cmp < 0,
            "le" => cmp <= 0,
            _ => throw new NotSupportedException($"Unsupported operator: '{op}'"),
        };
    }

    private static int CompareOrdered(object left, object right)
    {
        // Normalize types for comparison.
        if (left is DateTimeOffset leftDto && right is DateTimeOffset rightDto)
            return leftDto.CompareTo(rightDto);
        if (left is DateTime leftDt && right is DateTimeOffset rightDto2)
            return new DateTimeOffset(leftDt).CompareTo(rightDto2);
        if (left is DateTimeOffset leftDto2 && right is DateTime rightDt)
            return leftDto2.CompareTo(new DateTimeOffset(rightDt));

        if (left is byte[] leftBytes && right is byte[] rightBytes)
            return leftBytes.SequenceEqual(rightBytes) ? 0 : -1;

        if (left is IComparable c)
        {
            try
            {
                return c.CompareTo(Convert.ChangeType(right, left.GetType()));
            }
            catch
            {
                return string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
            }
        }

        return string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
    }

    private static List<Token> Tokenize(string filter)
    {
        var tokens = new List<Token>();
        var i = 0;
        while (i < filter.Length)
        {
            if (char.IsWhiteSpace(filter[i])) { i++; continue; }
            if (filter[i] == '(' || filter[i] == ')')
            {
                tokens.Add(new Token(filter[i].ToString()));
                i++;
                continue;
            }
            // String literal
            if (filter[i] == '\'')
            {
                var start = i; i++;
                while (i < filter.Length)
                {
                    if (filter[i] == '\'' && i + 1 < filter.Length && filter[i + 1] == '\'')
                        i += 2; // escaped quote
                    else if (filter[i] == '\'')
                    { i++; break; }
                    else i++;
                }
                tokens.Add(new Token(filter[start..i]));
                continue;
            }
            // datetime'...' or guid'...' or binary'...' or X'...' (binary)
            if (i + 2 < filter.Length && (filter[i..].StartsWith("datetime'", StringComparison.OrdinalIgnoreCase) || filter[i..].StartsWith("guid'", StringComparison.OrdinalIgnoreCase) || filter[i..].StartsWith("binary'", StringComparison.OrdinalIgnoreCase) || filter[i..].StartsWith("X'", StringComparison.OrdinalIgnoreCase)))
            {
                var start = i;
                i = filter.IndexOf('\'', i) + 1; // skip to first '
                while (i < filter.Length && filter[i] != '\'') i++;
                if (i < filter.Length) i++; // skip closing '
                tokens.Add(new Token(filter[start..i]));
                continue;
            }
            // Word or number
            {
                var start = i;
                while (i < filter.Length && !char.IsWhiteSpace(filter[i]) && filter[i] != '(' && filter[i] != ')' && filter[i] != '\'')
                    i++;
                if (i > start)
                    tokens.Add(new Token(filter[start..i]));
            }
        }
        return tokens;
    }

    private struct Token
    {
        public string Value { get; }
        public Token(string value) => Value = value;
    }
}
