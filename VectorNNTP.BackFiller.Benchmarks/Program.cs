using BenchmarkDotNet.Running;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length > 0 && string.Equals(args[0], "stress", StringComparison.OrdinalIgnoreCase))
        {
            await AsyncSinkStressRunner.RunAllAsync().ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-stress", StringComparison.OrdinalIgnoreCase))
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse(args.Skip(1).ToArray());
            int durationSeconds = options.DurationSeconds ?? 120;
            await TransitServerStressRunner.RunAsync(TimeSpan.FromSeconds(durationSeconds), options).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-validate", StringComparison.OrdinalIgnoreCase))
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse(args.Skip(1).ToArray());
            await TransitServerStressRunner.RunValidationAsync(options).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-saturate", StringComparison.OrdinalIgnoreCase))
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse(args.Skip(1).ToArray());
            int durationSeconds = options.DurationSeconds ?? 120;
            await TransitServerStressRunner.RunSaturationAsync(TimeSpan.FromSeconds(durationSeconds), options).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-single-trace", StringComparison.OrdinalIgnoreCase))
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse(args.Skip(1).ToArray());
            await TransitServerStressRunner.RunSingleTraceAsync(options).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-generator-baseline", StringComparison.OrdinalIgnoreCase))
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse(args.Skip(1).ToArray());
            await TransitServerStressRunner.RunGeneratorBaselineAsync(options).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-diagnostic-suite", StringComparison.OrdinalIgnoreCase))
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse(args.Skip(1).ToArray());
            await TransitDiagnosticSuiteRunner.RunAsync(options).ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-32worker-experiments", StringComparison.OrdinalIgnoreCase))
        {
            await Transit32WorkerExperimentRunner.RunAsync().ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-generator-worker-sweep", StringComparison.OrdinalIgnoreCase))
        {
            await TransitServerStressRunner.RunGeneratorWorkerSweepAsync().ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "transit-forensic-32worker", StringComparison.OrdinalIgnoreCase))
        {
            await TransitServerStressRunner.RunForensic32WorkerAsync().ConfigureAwait(false);
            return;
        }

        if (args.Length > 0 && string.Equals(args[0], "dotstuff-bench", StringComparison.OrdinalIgnoreCase))
        {
            _ = BenchmarkRunner.Run<TransitDotStuffingBenchmarks>();
            return;
        }

        _ = BenchmarkRunner.Run<LoggingApiBenchmarks>();
    }
}
