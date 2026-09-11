// <copyright file="Program.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Benchmarks
// Benchmark host entry point that dispatches benchmark and stress runner command modes.

using BenchmarkDotNet.Running;

namespace VectorNNTP.BackFiller.Benchmarks
{
    /// <summary>
    /// Identifies the benchmark entrypoint workflow selected from command-line arguments.
    /// </summary>
    internal enum TransitEntrypointKind
    {
        DefaultLoggingBenchmark,
        AsyncSinkStress,
        TransitStress,
        TransitValidate,
        TransitBenchmarkFakeServer,
        TransitSaturate,
        TransitSingleTrace,
        TransitGeneratorBaseline,
        TransitDiagnosticSuite,
        Transit32WorkerExperiments,
        TransitGeneratorWorkerSweep,
        TransitForensic32Worker,
        DotStuffBenchmark,
        ArticleParseBenchmark,
        ArticleAcquisitionBenchmark,
        YEncValidatorBenchmark,
    }

    /// <summary>
    /// Represents an immutable benchmark entrypoint execution plan produced from CLI inputs.
    /// </summary>
    /// <param name="Kind">The selected entrypoint workflow.</param>
    /// <param name="CliOptions">The parsed transit benchmark options for workflows that consume transit CLI options.</param>
    /// <param name="EffectiveDuration">The effective duration selected by entrypoint planning for duration-driven workflows.</param>
    internal readonly record struct TransitEntrypointPlan(
        TransitEntrypointKind Kind,
        TransitBenchmarkCliOptions CliOptions,
        TimeSpan? EffectiveDuration);

    /// <summary>
    /// Provides command-line dispatch for benchmark and stress execution modes in the benchmark host process.
    /// </summary>
    internal static class Program
    {
        /// <summary>
        /// Gets or sets the default duration in seconds for transit stress and saturation benchmark modes.
        /// </summary>
        private const int DefaultTransitDurationSeconds = 120;

        /// <summary>
        /// Gets or sets the default duration in seconds for validation-oriented transit benchmark modes.
        /// </summary>
        private const int DefaultValidationDurationSeconds = 10;

        /// <summary>
        /// Parses benchmark host command-line arguments and executes the selected benchmark or stress workflow.
        /// </summary>
        /// <param name="args">Command-line arguments passed to the benchmark host process.</param>
        /// <returns>A task that completes when the selected workflow exits.</returns>
        public static async Task Main(string[] args)
        {
            TransitEntrypointPlan plan = BuildEntrypointPlan(args);

            switch (plan.Kind)
            {
                case TransitEntrypointKind.AsyncSinkStress:
                    await AsyncSinkStressRunner.RunAllAsync().ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitStress:
                    await TransitServerStressRunner.RunAsync(RequireDuration(plan), plan.CliOptions).ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitValidate:
                    await TransitServerStressRunner.RunValidationAsync(RequireDuration(plan), plan.CliOptions).ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitBenchmarkFakeServer:
                    await TransitServerStressRunner.RunFakeServerValidationAsync(RequireDuration(plan), plan.CliOptions).ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitSaturate:
                    await TransitServerStressRunner.RunSaturationAsync(RequireDuration(plan), plan.CliOptions).ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitSingleTrace:
                    await TransitServerStressRunner.RunSingleTraceAsync(RequireDuration(plan), plan.CliOptions).ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitGeneratorBaseline:
                    await TransitServerStressRunner.RunGeneratorBaselineAsync(plan.CliOptions).ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitDiagnosticSuite:
                    await TransitDiagnosticSuiteRunner.RunAsync(plan.CliOptions).ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.Transit32WorkerExperiments:
                    await Transit32WorkerExperimentRunner.RunAsync().ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitGeneratorWorkerSweep:
                    await TransitServerStressRunner.RunGeneratorWorkerSweepAsync().ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.TransitForensic32Worker:
                    await TransitServerStressRunner.RunForensic32WorkerAsync().ConfigureAwait(false);
                    return;

                case TransitEntrypointKind.DotStuffBenchmark:
                    _ = BenchmarkRunner.Run<TransitDotStuffingBenchmarks>();
                    return;

                case TransitEntrypointKind.ArticleParseBenchmark:
                    _ = BenchmarkRunner.Run<NntpArticleParserBenchmarks>();
                    return;

                case TransitEntrypointKind.ArticleAcquisitionBenchmark:
                    _ = BenchmarkRunner.Run<NntpArticleAcquisitionBenchmarks>();
                    return;

                case TransitEntrypointKind.YEncValidatorBenchmark:
                    _ = BenchmarkRunner.Run<YEncArticleValidatorBenchmarks>();
                    return;

                default:
                    _ = BenchmarkRunner.Run<LoggingApiBenchmarks>();
                    return;
            }
        }

        /// <summary>
        /// Builds a deterministic entrypoint plan from command-line arguments without performing I/O or benchmark execution.
        /// </summary>
        /// <param name="args">Command-line arguments passed to the benchmark host.</param>
        /// <returns>The immutable entrypoint plan describing selected mode, parsed options, and effective duration when applicable.</returns>
        internal static TransitEntrypointPlan BuildEntrypointPlan(string[] args)
        {
            ArgumentNullException.ThrowIfNull(args);

            if (args.Length == 0)
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.DefaultLoggingBenchmark, default, EffectiveDuration: null);
            }

            string mode = args[0];

            if (string.Equals(mode, "stress", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.AsyncSinkStress, default, EffectiveDuration: null);
            }

            if (string.Equals(mode, "transit-stress", StringComparison.OrdinalIgnoreCase))
            {
                TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([.. args.Skip(1)]);
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitStress, options, TimeSpan.FromSeconds(options.DurationSeconds ?? DefaultTransitDurationSeconds));
            }

            if (string.Equals(mode, "transit-validate", StringComparison.OrdinalIgnoreCase))
            {
                TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([.. args.Skip(1)]);
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitValidate, options, TimeSpan.FromSeconds(options.DurationSeconds ?? DefaultValidationDurationSeconds));
            }

            if (string.Equals(mode, "transit-benchmark-fakeserver", StringComparison.OrdinalIgnoreCase))
            {
                TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([.. args.Skip(1)]);
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitBenchmarkFakeServer, options, TimeSpan.FromSeconds(options.DurationSeconds ?? DefaultValidationDurationSeconds));
            }

            if (string.Equals(mode, "transit-saturate", StringComparison.OrdinalIgnoreCase))
            {
                TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([.. args.Skip(1)]);
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitSaturate, options, TimeSpan.FromSeconds(options.DurationSeconds ?? DefaultTransitDurationSeconds));
            }

            if (string.Equals(mode, "transit-single-trace", StringComparison.OrdinalIgnoreCase))
            {
                TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([.. args.Skip(1)]);
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitSingleTrace, options, TimeSpan.FromSeconds(options.DurationSeconds ?? DefaultValidationDurationSeconds));
            }

            if (string.Equals(mode, "transit-generator-baseline", StringComparison.OrdinalIgnoreCase))
            {
                TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([.. args.Skip(1)]);
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitGeneratorBaseline, options, EffectiveDuration: null);
            }

            if (string.Equals(mode, "transit-diagnostic-suite", StringComparison.OrdinalIgnoreCase))
            {
                TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([.. args.Skip(1)]);
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitDiagnosticSuite, options, EffectiveDuration: null);
            }

            if (string.Equals(mode, "transit-32worker-experiments", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.Transit32WorkerExperiments, default, EffectiveDuration: null);
            }

            if (string.Equals(mode, "transit-generator-worker-sweep", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitGeneratorWorkerSweep, default, EffectiveDuration: null);
            }

            if (string.Equals(mode, "transit-forensic-32worker", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.TransitForensic32Worker, default, EffectiveDuration: null);
            }

            if (string.Equals(mode, "dotstuff-bench", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.DotStuffBenchmark, default, EffectiveDuration: null);
            }

            if (string.Equals(mode, "article-parse-bench", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.ArticleParseBenchmark, default, EffectiveDuration: null);
            }

            if (string.Equals(mode, "article-acquisition-bench", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.ArticleAcquisitionBenchmark, default, EffectiveDuration: null);
            }

            if (string.Equals(mode, "yenc-validator-bench", StringComparison.OrdinalIgnoreCase))
            {
                return new TransitEntrypointPlan(TransitEntrypointKind.YEncValidatorBenchmark, default, EffectiveDuration: null);
            }

            return new TransitEntrypointPlan(TransitEntrypointKind.DefaultLoggingBenchmark, default, EffectiveDuration: null);
        }

        /// <summary>
        /// Resolves the required duration from a duration-driven entrypoint plan.
        /// </summary>
        /// <param name="plan">The entrypoint plan.</param>
        /// <returns>The effective duration declared by planning.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a duration-driven workflow was planned without an effective duration value.</exception>
        private static TimeSpan RequireDuration(TransitEntrypointPlan plan)
        {
            return plan.EffectiveDuration ?? throw new InvalidOperationException($"Entrypoint plan '{plan.Kind}' did not include a required effective duration.");
        }
    }
}
