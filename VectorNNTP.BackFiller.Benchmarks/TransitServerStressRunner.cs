using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Transit;

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
        using BoundedArticleQueue queue = new(config.MaxQueuedArticles, config.MaxResidentBytes);
        MeasurementMetrics metrics = new(config.ArticleTargetBytes);
        RuntimeMetrics runtime = new();

        using CancellationTokenSource producerStopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        DateTimeOffset measurementStartUtc = DateTimeOffset.UtcNow;
        Console.WriteLine($"Measurement start UTC: {measurementStartUtc:O}");

        Process process = Process.GetCurrentProcess();
        long allocatedStartBytes = GC.GetTotalAllocatedBytes(precise: false);

        int producerQueueTargetArticles = Math.Clamp(config.ProducerQueueTargetArticles, 1, config.MaxQueuedArticles);

        Task[] producerTasks = new Task[config.GeneratorWorkerCount];
        for (int producerWorkerId = 0; producerWorkerId < producerTasks.Length; producerWorkerId++)
        {
            int capturedWorkerId = producerWorkerId;
            producerTasks[producerWorkerId] = Task.Run(() => MeasurementExecutionEngine.ProducerLoopAsync(
                queue,
                metrics,
                workload,
                producerQueueTargetArticles,
                capturedWorkerId,
                producerStopCts.Token), CancellationToken.None);
        }

        Task telemetryTask = Task.Run(() => MeasurementExecutionEngine.TelemetryLoopAsync(
            queue,
            metrics,
            runtime,
            process,
            allocatedStartBytes,
            publisher,
            producerQueueTargetArticles,
            enableForensicDiagnostics,
            producerStopCts.Token), CancellationToken.None);

        Task[] dispatchers = new Task[config.DispatchWorkerCount];
        for (int i = 0; i < dispatchers.Length; i++)
        {
            dispatchers[i] = Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, publisher, metrics, workload, cancellationToken, enableForensicDiagnostics), CancellationToken.None);
        }

        await Task.Delay(config.MeasurementDuration, cancellationToken).ConfigureAwait(false);

        return await MeasurementExecutionEngine.DrainAndShutdownAsync(
            queue,
            metrics,
            runtime,
            process,
            workload,
            publisher,
            config,
            producerTasks,
            telemetryTask,
            dispatchers,
            producerStopCts,
            measurementStartUtc,
            allocatedStartBytes,
            enableForensicDiagnostics,
            (drainConfig, snapshot, drainMetrics, drainRuntime, drainProcess, workloadPreparation, startUtc, endUtc, drainTime, outstandingAtEnd, drainedAfterEnd, allocatedAtStart, forensicEnabled) =>
                BenchmarkResultFactory.Create(
                    drainConfig,
                    RuntimeIdentity,
                    BenchmarkBuildVersion,
                    snapshot,
                    drainMetrics,
                    drainRuntime,
                    drainProcess,
                    workloadPreparation,
                    startUtc,
                    endUtc,
                    drainTime,
                    outstandingAtEnd,
                    drainedAfterEnd,
                    allocatedAtStart,
                    forensicEnabled)).ConfigureAwait(false);
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
