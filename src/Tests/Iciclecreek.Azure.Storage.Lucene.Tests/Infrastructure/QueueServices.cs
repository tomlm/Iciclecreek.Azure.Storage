using Iciclecreek.Azure.Storage.Lucene.Internal;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;

/// <summary>
/// Queue service clients that keep their queues either way: as files, which is what the public
/// constructors give, or as indexes, which has no public constructor.
/// </summary>
internal static class QueueServices
{
    public static LuceneQueueServiceClient InMemory(bool asFiles) => On(LuceneQueueServiceClient.InMemory(), asFiles);

    public static LuceneQueueServiceClient InFolder(string folder, bool asFiles) => On(LuceneQueueServiceClient.InFolder(folder), asFiles);

    public static LuceneQueueServiceClient On(Func<string, LuceneDirectory> directoryFactory, bool asFiles) =>
        new(directoryFactory, asFiles ? QueueStorage.Files : QueueStorage.Index);
}
