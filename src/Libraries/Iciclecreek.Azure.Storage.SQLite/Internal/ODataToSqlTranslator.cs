using Microsoft.Data.Sqlite;

namespace Iciclecreek.Azure.Storage.SQLite.Internal;

/// <summary>
/// Translates OData filter strings to SQL WHERE clauses with parameterized values.
/// </summary>
internal static class ODataToSqlTranslator
{
    public static (string Sql, List<SqliteParameter> Parameters) Translate(string filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
            return ("1=1", new List<SqliteParameter>());

        var tokens = Tokenize(filter);
        var pos = 0;
        var parameters = new List<SqliteParameter>();
        var sql = ParseOr(tokens, ref pos, parameters);
        return (sql, parameters);
    }

    private static string ParseOr(List<Token> tokens, ref int pos, List<SqliteParameter> parameters)
    {
        var left = ParseAnd(tokens, ref pos, parameters);
        while (pos < tokens.Count && tokens[pos].Value == "or")
        {
            pos++;
            var right = ParseAnd(tokens, ref pos, parameters);
            left = $"({left} OR {right})";
        }
        return left;
    }

    private static string ParseAnd(List<Token> tokens, ref int pos, List<SqliteParameter> parameters)
    {
        var left = ParseNot(tokens, ref pos, parameters);
        while (pos < tokens.Count && tokens[pos].Value == "and")
        {
            pos++;
            var right = ParseNot(tokens, ref pos, parameters);
            left = $"({left} AND {right})";
        }
        return left;
    }

    private static string ParseNot(List<Token> tokens, ref int pos, List<SqliteParameter> parameters)
    {
        if (pos < tokens.Count && tokens[pos].Value == "not")
        {
            pos++;
            var inner = ParsePrimary(tokens, ref pos, parameters);
            return $"NOT ({inner})";
        }
        return ParsePrimary(tokens, ref pos, parameters);
    }

    private static string ParsePrimary(List<Token> tokens, ref int pos, List<SqliteParameter> parameters)
    {
        if (pos < tokens.Count && tokens[pos].Value == "(")
        {
            pos++; // skip '('
            var expr = ParseOr(tokens, ref pos, parameters);
            if (pos < tokens.Count && tokens[pos].Value == ")")
                pos++; // skip ')'
            return $"({expr})";
        }

        // Comparison: property op literal
        if (pos + 2 >= tokens.Count)
            throw new NotSupportedException("Unexpected end of filter expression.");

        var propToken = tokens[pos++];
        var opToken = tokens[pos++];
        var valueToken = tokens[pos++];

        var propName = propToken.Value;
        var sqlOp = opToken.Value switch
        {
            "eq" => "=",
            "ne" => "!=",
            "gt" => ">",
            "ge" => ">=",
            "lt" => "<",
            "le" => "<=",
            _ => throw new NotSupportedException($"Unsupported operator: '{opToken.Value}'"),
        };

        var paramName = $"@p{parameters.Count}";
        var literal = ParseLiteral(valueToken);
        parameters.Add(new SqliteParameter(paramName, literal ?? DBNull.Value));

        return $"[{propName}] {sqlOp} {paramName}";
    }

    private static object? ParseLiteral(Token token)
    {
        var v = token.Value;

        // String literal: 'value'
        if (v.StartsWith('\'') && v.EndsWith('\''))
            return v[1..^1].Replace("''", "'");

        // Boolean
        if (v == "true") return 1L; // SQLite stores bools as INTEGER
        if (v == "false") return 0L;

        // datetime literal: datetime'...'
        if (v.StartsWith("datetime'", StringComparison.OrdinalIgnoreCase) && v.EndsWith('\''))
            return v[9..^1]; // Store as text for comparison

        // guid literal: guid'...'
        if (v.StartsWith("guid'", StringComparison.OrdinalIgnoreCase) && v.EndsWith('\''))
            return v[5..^1];

        // long (contains L suffix)
        if (v.EndsWith('L') && long.TryParse(v[..^1], out var l))
            return l;

        // double (contains dot)
        if (v.Contains('.') && double.TryParse(v, out var d))
            return d;

        // int
        if (int.TryParse(v, out var i))
            return (long)i; // Use long for SQLite INTEGER affinity

        // long fallback
        if (long.TryParse(v, out var l2))
            return l2;

        throw new NotSupportedException($"Unsupported literal: '{v}'");
    }

    /// <summary>
    /// Extracts column names referenced in the filter expression.
    /// </summary>
    public static HashSet<string> ExtractColumnNames(string filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
            return new HashSet<string>();

        var columns = new HashSet<string>();
        var tokens = Tokenize(filter);
        var pos = 0;
        ExtractColumnsFromOr(tokens, ref pos, columns);
        return columns;
    }

    private static void ExtractColumnsFromOr(List<Token> tokens, ref int pos, HashSet<string> columns)
    {
        ExtractColumnsFromAnd(tokens, ref pos, columns);
        while (pos < tokens.Count && tokens[pos].Value == "or")
        {
            pos++;
            ExtractColumnsFromAnd(tokens, ref pos, columns);
        }
    }

    private static void ExtractColumnsFromAnd(List<Token> tokens, ref int pos, HashSet<string> columns)
    {
        ExtractColumnsFromNot(tokens, ref pos, columns);
        while (pos < tokens.Count && tokens[pos].Value == "and")
        {
            pos++;
            ExtractColumnsFromNot(tokens, ref pos, columns);
        }
    }

    private static void ExtractColumnsFromNot(List<Token> tokens, ref int pos, HashSet<string> columns)
    {
        if (pos < tokens.Count && tokens[pos].Value == "not")
        {
            pos++;
            ExtractColumnsFromPrimary(tokens, ref pos, columns);
            return;
        }
        ExtractColumnsFromPrimary(tokens, ref pos, columns);
    }

    private static void ExtractColumnsFromPrimary(List<Token> tokens, ref int pos, HashSet<string> columns)
    {
        if (pos < tokens.Count && tokens[pos].Value == "(")
        {
            pos++;
            ExtractColumnsFromOr(tokens, ref pos, columns);
            if (pos < tokens.Count && tokens[pos].Value == ")")
                pos++;
            return;
        }

        if (pos + 2 >= tokens.Count) return;

        var propName = tokens[pos++].Value;
        pos++; // skip operator
        pos++; // skip value
        columns.Add(propName);
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
            // datetime'...' or guid'...'
            if (i + 5 < filter.Length && (filter[i..].StartsWith("datetime'", StringComparison.OrdinalIgnoreCase) || filter[i..].StartsWith("guid'", StringComparison.OrdinalIgnoreCase)))
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
