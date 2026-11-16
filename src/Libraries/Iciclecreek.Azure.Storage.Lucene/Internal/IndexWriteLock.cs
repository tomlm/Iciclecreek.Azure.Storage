using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Azure;
using Lucene.Net.Analysis.Core;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Util;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Internal;

/// <summary>
/// The turn to write to one index within this process. Callers are let in one at a time in the
/// order they asked, so a busy group of threads cannot keep another caller waiting for ever. The
/// thread that holds the gate may enter it again.
/// </summary>
internal sealed class WriteGate
{
    private readonly object _sync = new();
    private readonly LinkedList<int> _waiting = new();
    private int _owner;
    private int _depth;

    public void Enter()
    {
        var me = Environment.CurrentManagedThreadId;
        lock (_sync)
        {
            if (_owner == me)
            {
                _depth++;
                return;
            }

            var place = _waiting.AddLast(me);
            try
            {
                while (_owner != 0 || _waiting.First != place)
                    Monitor.Wait(_sync);
                _owner = me;
                _depth = 1;
            }
            finally
            {
                // Also when the wait was interrupted: whoever was behind this caller must not wait on it.
                _waiting.Remove(place);
                if (_owner != me)
                    Monitor.PulseAll(_sync);
            }
        }
    }

    public void Exit()
    {
        lock (_sync)
        {
            if (_owner != Environment.CurrentManagedThreadId)
                throw new SynchronizationLockException("The index is not held by this thread.");
            if (--_depth > 0)
                return;
            _owner = 0;
            Monitor.PulseAll(_sync);
        }
    }

    public bool IsHeldByCurrentThread
    {
        get
        {
            lock (_sync)
                return _owner == Environment.CurrentManagedThreadId;
        }
    }
}

/// <summary>
/// Opens the writer of an index, one caller at a time.
/// <para>
/// Within this process every client that opens the same index shares one <see cref="WriteGate"/>:
/// for an index in a folder that is every client on that folder, whichever directory object it
/// holds. Between processes there is only Lucene's write lock, which cannot be waited on, only
/// tried: it is tried again every few tens of milliseconds for up to <see cref="LockWait"/>.
/// That is a retry, not a queue, so a process can lose every try to a busier one; it is then
/// refused with <c>503 ServerBusy</c>, as Azure refuses a request it cannot serve in time.
/// </para>
/// </summary>
internal static class IndexWriteLock
{
    /// <summary>How long an operation keeps trying for an index that another process is writing to.</summary>
    internal static readonly TimeSpan LockWait = TimeSpan.FromSeconds(10);

    // Lucene's own wait polls once a second, so between two writers that hand over without a gap
    // it almost never gets in; these tries are close enough together to catch the gap.
    private const int MinRetryDelayMs = 10;
    private const int MaxRetryDelayMs = 50;

    // One gate per index folder for the life of the process. An entry is a path and an object, and
    // a process only ever opens so many indexes, so they are not removed.
    private static readonly ConcurrentDictionary<string, WriteGate> s_gatesByFolder = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConditionalWeakTable<LuceneDirectory, WriteGate> s_gatesByDirectory = new();

    /// <summary>
    /// The gate for the index in a directory. Directories on the same folder share one; any other
    /// kind of directory has its own, since nothing says whether two of them hold the same index.
    /// </summary>
    public static WriteGate GateFor(LuceneDirectory directory) =>
        directory is FSDirectory inFolder
            // Compared without case, as most file systems do; where case does matter, two folders
            // sharing a gate only costs them some waiting.
            ? s_gatesByFolder.GetOrAdd(inFolder.Directory.FullName, _ => new WriteGate())
            : s_gatesByDirectory.GetValue(directory, _ => new WriteGate());

    /// <summary>
    /// Opens the index for writing, creating it if need be. The caller holds the directory's gate,
    /// so the write lock can only be held by another process, or by a directory this class cannot
    /// tell is the same index.
    /// </summary>
    public static IndexWriter OpenWriter(LuceneDirectory directory)
    {
        var waited = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                // A configuration belongs to the one writer it is given to, so each try has its own.
                var config = new IndexWriterConfig(LuceneVersion.LUCENE_48, new KeywordAnalyzer())
                {
                    OpenMode = OpenMode.CREATE_OR_APPEND,
                    // Try once and come back here, rather than sleep inside Lucene a second at a time.
                    WriteLockTimeout = 0,
                };
                return new IndexWriter(directory, config);
            }
            catch (LockObtainFailedException ex)
            {
                WaitToTryAgain(waited, ex);
            }
        }
    }

    /// <summary>
    /// Takes the write lock of a directory that is written to as plain files, with no index
    /// writer. It is the lock an index writer would take, tried the same way and for as long;
    /// disposing it lets the directory go. The caller holds the directory's gate.
    /// </summary>
    public static global::Lucene.Net.Store.Lock ObtainLock(LuceneDirectory directory)
    {
        var waited = Stopwatch.StartNew();
        while (true)
        {
            var writeLock = directory.MakeLock(IndexWriter.WRITE_LOCK_NAME);
            if (writeLock.Obtain())
                return writeLock;
            // Not disposed: a lock that was refused may be the very object its holder has (a folder
            // directory hands out one lock per path), and disposing it would let the directory go.
            WaitToTryAgain(waited, null);
        }
    }

    /// <summary>Sleeps until the next try, or refuses with <c>503 ServerBusy</c> once <see cref="LockWait"/> has gone by.</summary>
    private static void WaitToTryAgain(Stopwatch waited, Exception? refusal)
    {
        if (waited.Elapsed >= LockWait)
            throw new RequestFailedException(503,
                $"The directory is being written to by another process and did not become free within {LockWait.TotalSeconds:0} seconds. Try again.",
                "ServerBusy", refusal);
        // Uneven delays, so two waiting processes do not keep trying at the same instants.
        Thread.Sleep(Random.Shared.Next(MinRetryDelayMs, MaxRetryDelayMs + 1));
    }
}
