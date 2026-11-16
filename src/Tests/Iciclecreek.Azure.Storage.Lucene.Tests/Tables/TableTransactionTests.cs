using Iciclecreek.Azure.Storage.Lucene.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Lucene.Tests.Tables;

public class TableTransactionTests : TableTransactionTestsBase
{
    protected override StorageTestFixture CreateFixture() => new LuceneStorageTestFixture();
}
