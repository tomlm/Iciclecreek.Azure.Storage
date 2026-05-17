using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Tables;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Tables;

public class TableETagTests : TableETagTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();
}
