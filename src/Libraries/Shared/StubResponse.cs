using System.Diagnostics.CodeAnalysis;
using Azure;
using Azure.Core;

namespace Iciclecreek.Azure.Storage.Internal;

internal sealed class StubResponse : Response
{
    private readonly List<HttpHeader> _headers = new();

    public StubResponse(int status = 200, string reasonPhrase = "OK")
    {
        Status = status;
        ReasonPhrase = reasonPhrase;
    }

    public override int Status { get; }

    public override string ReasonPhrase { get; }

    public override Stream? ContentStream { get; set; }

    public override string ClientRequestId { get; set; } = Guid.NewGuid().ToString();

    public override void Dispose() => ContentStream?.Dispose();

    protected override bool ContainsHeader(string name)
        => _headers.Any(h => string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase));

    protected override IEnumerable<HttpHeader> EnumerateHeaders() => _headers;

    protected override bool TryGetHeader(string name, [NotNullWhen(true)] out string? value)
    {
        foreach (var h in _headers)
        {
            if (string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = h.Value;
                return true;
            }
        }
        value = null;
        return false;
    }

    protected override bool TryGetHeaderValues(string name, [NotNullWhen(true)] out IEnumerable<string>? values)
    {
        var list = _headers.Where(h => string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase))
                           .Select(h => h.Value)
                           .ToArray();
        if (list.Length > 0)
        {
            values = list;
            return true;
        }
        values = null;
        return false;
    }

    public void AddHeader(string name, string value) => _headers.Add(new HttpHeader(name, value));

    public static Response Ok() => new StubResponse(200, "OK");
    public static Response Created() => new StubResponse(201, "Created");
    public static Response NoContent() => new StubResponse(204, "No Content");
    public static Response Accepted() => new StubResponse(202, "Accepted");

    public static Response NoContent(string etag)
    {
        var response = new StubResponse(204, "No Content");
        // ETags in HTTP headers must be quoted per RFC 7232
        var quoted = etag.StartsWith('"') ? etag : $"\"{etag}\"";
        response.AddHeader("ETag", quoted);
        return response;
    }
}
