namespace Iciclecreek.Azure.Storage.Lucene.Tests;

/// <summary>
/// NUnit's metadata-based discovery mode skips assemblies that have no [Test] methods
/// defined locally. This ensures the adapter scans this assembly and discovers
/// all inherited tests from the shared base classes.
/// </summary>
[TestFixture]
public class AssemblyTestDiscoveryBootstrap
{
    [Test]
    public void Assembly_Is_Discoverable() => Assert.Pass();
}
