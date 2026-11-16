using Azure;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

internal sealed class StaticPageable<T> : Pageable<T> where T : notnull
{
    private readonly IReadOnlyList<T> _items;
    public StaticPageable(IReadOnlyList<T> items) => _items = items;
    public StaticPageable(IEnumerable<T> items) => _items = items.ToList();

    public override IEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        yield return Page<T>.FromValues(_items.ToList(), null, StubResponse.Ok());
    }
}

internal sealed class StaticAsyncPageable<T> : AsyncPageable<T> where T : notnull
{
    private readonly Pageable<T> _inner;
    public StaticAsyncPageable(Pageable<T> inner) => _inner = inner;

    public override async IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        foreach (var page in _inner.AsPages(continuationToken, pageSizeHint))
            yield return page;
    }
}
