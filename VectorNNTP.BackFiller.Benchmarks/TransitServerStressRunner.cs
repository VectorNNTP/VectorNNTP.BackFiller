using System.Buffers;
using System.Globalization;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks.Execution;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class TransitServerStressRunner
{
    private const int DefaultArticleTargetBytes = 1 * 1024 * 1024;
    private const int DefaultWarmupSeconds = 10;
    private const int ValidationSeconds = 10;
    private const int DefaultGeneratorMeasurementSeconds = 30;

    private static readonly RuntimeExecutionIdentity RuntimeIdentity = RuntimeExecutionIdentityCapture.Capture(typeof(TransitServerStressRunner).Assembly);
    private static readonly string BenchmarkBuildVersion = RuntimeIdentity.AssemblyFileVersion ?? RuntimeIdentity.RuntimeAssemblyVersion;

    internal static async Task RunAsync(TimeSpan stressDuration, TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(stressDuration, BenchmarkMode.Full, cliOptions);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task RunValidationAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(ValidationSeconds), BenchmarkMode.Validation, cliOptions);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task RunSaturationAsync(TimeSpan stressDuration, TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(stressDuration, BenchmarkMode.Saturation, cliOptions);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task RunGeneratorWorkerSweepAsync(CancellationToken cancellationToken = default)
    {
        int[] workerCounts = [1, 2, 4, 8, 16, 32];

        foreach (int generatorWorkers in workerCounts)
        {
            Console.WriteLine();
            Console.WriteLine($"=== Generator worker sweep run: workers={generatorWorkers} ===");

            TransitBenchmarkCliOptions options = new(
                DurationSeconds: 30,
                WarmupSeconds: 10,
                ConnectionPoolSize: 64,
                PipelineDepth: 16,
                DispatchWorkers: 512,
                QueueMegabytes: 2048,
                QueueArticles: 2048,
                ArticleKilobytes: 1024,
                GeneratorWorkers: generatorWorkers,
                WriteBatchCoalesceMicroseconds: 250);

            TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(30), BenchmarkMode.Forensic, options);
            await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
        }
    }

    internal static async Task RunForensic32WorkerAsync(CancellationToken cancellationToken = default)
    {
        TransitBenchmarkCliOptions options = new(
            DurationSeconds: 30,
            WarmupSeconds: 10,
            ConnectionPoolSize: 64,
            PipelineDepth: 16,
            DispatchWorkers: 512,
            QueueMegabytes: 2048,
            QueueArticles: 2048,
            ArticleKilobytes: 1024,
            GeneratorWorkers: 32,
            WriteBatchCoalesceMicroseconds: 250);

        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(30), BenchmarkMode.Forensic, options);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task RunGeneratorBaselineAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        await GeneratorBaselineRunner.RunAsync(cliOptions, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task RunSingleTraceAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        await TransitSingleTraceRunner.RunAsync(
            cliOptions,
            ValidationSeconds,
            RuntimeIdentity,
            CreateTransitPublisherLogger,
            cancellationToken).ConfigureAwait(false);
    }

    private static Task RunCoreAsync(TransitBenchmarkConfig config, CancellationToken cancellationToken)
    {
        return TransitBenchmarkOrchestrator.RunCoreAsync(
            config,
            RuntimeIdentity,
            BenchmarkBuildVersion,
            CreateTransitPublisherLogger,
            RunMeasurementAsync,
            WriteStructuredResultArtifacts,
            cancellationToken);
    }

    private static async Task<BenchmarkResult> RunMeasurementAsync(
        TransitPublisher publisher,
        TransitBenchmarkConfig config,
        PreparedBenchmarkWorkload workload,
        CancellationToken cancellationToken,
        bool enableForensicDiagnostics)
    {
        return await MeasurementRunCoordinator.RunAsync(
            publisher,
            config,
            workload,
            RuntimeIdentity,
            BenchmarkBuildVersion,
            cancellationToken,
            enableForensicDiagnostics).ConfigureAwait(false);
    }

    private static ILogger<TransitPublisher> CreateTransitPublisherLogger(ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);

        ILogger baseLogger = loggerFactory.CreateLogger<TransitPublisher>();
        return new TransitPublisherBenchmarkLogger(baseLogger);
    }

    private static void WriteStructuredResultArtifacts(BenchmarkResult result, TransitBenchmarkConfig config)
    {
        BenchmarkArtifactWriter.WriteStructuredResultArtifacts(
            result,
            config,
            Environment.ProcessorCount,
            static (benchmarkResult, benchmarkConfig, processorCount) => BenchmarkResultArtifact.From(benchmarkResult, benchmarkConfig, processorCount),
            static artifact => artifact.ToCsv());
    }

    }
