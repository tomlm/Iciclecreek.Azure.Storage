namespace Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;

public sealed class TempDb : IDisposable
{
    public TempDb()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            "sqlite-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
        DbPath = System.IO.Path.Combine(Path, "testacct.db");
    }

    public string Path { get; }
    public string DbPath { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, true); } catch { }
    }
}
