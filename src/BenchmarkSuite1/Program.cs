using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;


namespace BenchmarkSuite1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var config = DefaultConfig.Instance
                .WithArtifactsPath(@"c:\temp\BenchmarkDotNet.Artifacts");
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        }
    }
}
