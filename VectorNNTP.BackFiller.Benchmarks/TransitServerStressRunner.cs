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
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(ValidationSeconds), BenchmarkMode.Validation, cliOptions);

        RuntimeIdentityGuard.EnsureMatches(config.ExpectedRuntimeIdentity, RuntimeIdentity);

        Console.WriteLine("=== Transit Publisher Single Transaction Trace ===");
        Console.WriteLine("Benchmark execution policy: NEVER use --no-build. ALWAYS run clean -> build -> verify output identity -> execute.");
        Console.WriteLine($"Config path: {config.AppSettingsPath}");
        Console.WriteLine($"Logical Transit endpoint host (TLS/SNI/cert): {config.EndpointHost}");
        Console.WriteLine($"Transit port: {config.EndpointPort}");
        Console.WriteLine($"Transit UseSsl config: {config.EndpointUseSsl}");
        Console.WriteLine($"Connection pool size: {config.ConnectionPoolSize}");
        Console.WriteLine($"Per-connection pipeline depth: {config.PerConnectionPipelineDepth}");
        Console.WriteLine($"Dispatch worker count: {config.DispatchWorkerCount}");
        Console.WriteLine($"Target article bytes: {config.ArticleTargetBytes}");

        IPAddress[] resolved = await Dns.GetHostAddressesAsync(config.EndpointHost, cancellationToken).ConfigureAwait(false);
        Console.WriteLine($"Resolved addresses for {config.EndpointHost}: {string.Join(", ", resolved.Select(static x => x.ToString()))}");

        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "HH:mm:ss.fff ";
                });
        });

        ILogger<TransitPublisher> transitPublisherLogger = CreateTransitPublisherLogger(loggerFactory);

        await using TransitPublisher publisher = new(
            TransitBenchmarkOrchestrator.BuildRuntimeOptions(config),
            TimeProvider.System,
            transitPublisherLogger,
            connectionPoolSize: config.ConnectionPoolSize,
            perConnectionPipelineDepth: config.PerConnectionPipelineDepth);

        Console.WriteLine("Phase 1: Initialize publisher/connection stack");
        await publisher.InitializeAsync(cancellationToken).ConfigureAwait(false);

        string messageId = $"<single-trace-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}@benchmark.usenet.ninja>";
        Console.WriteLine($"TRACE_MESSAGE_ID: {messageId}");

        TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(messageId, config.ArticleTargetBytes);

        TransitPublishResult? publishResult = null;
        DateTimeOffset submitStartUtc = DateTimeOffset.UtcNow;
        Console.WriteLine($"TRACE_SUBMIT_START_UTC: {submitStartUtc:O}");

        try
        {
            using CancellationTokenSource timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(90));

            publishResult = await publisher.PublishAsync(messageId, payload.AsMemory(), timeoutCts.Token).ConfigureAwait(false);

            DateTimeOffset submitEndUtc = DateTimeOffset.UtcNow;
            Console.WriteLine($"TRACE_SUBMIT_END_UTC: {submitEndUtc:O}");
            Console.WriteLine($"TRACE_PUBLISH_RESULT: Status={publishResult.Status}, Code={publishResult.ResponseCode}, Text={publishResult.ResponseText}");
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            DateTimeOffset timeoutUtc = DateTimeOffset.UtcNow;
            Console.WriteLine($"TRACE_TIMEOUT_UTC: {timeoutUtc:O}");
            Console.WriteLine("TRACE_PUBLISH_RESULT: TIMED_OUT");
        }
        finally
        {
            payload.Dispose();
        }

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
        TopologyReporter.PrintConnectionTopologyDiagnostics(diagnostics);

        long totalStarted = diagnostics.Connections.Sum(static entry => entry.Snapshot.SubmissionsStarted);
        long totalAccepted = diagnostics.Connections.Sum(static entry => entry.Snapshot.SubmissionsAccepted);
        long totalRejected = diagnostics.Connections.Sum(static entry => entry.Snapshot.SubmissionsRejected);
        long totalAmbiguous = diagnostics.Connections.Sum(static entry => entry.Snapshot.SubmissionsAmbiguous);
        long totalFailed = diagnostics.Connections.Sum(static entry => entry.Snapshot.SubmissionsFailed);
        long totalUnavailable = diagnostics.Connections.Sum(static entry => entry.Snapshot.SubmissionsUnavailable);
        long totalCurrentOutstanding = diagnostics.Connections.Sum(static entry => entry.Snapshot.CurrentConcurrentSubmissions);
        int peakOutstandingPerConnection = diagnostics.Connections.Length == 0 ? 0 : diagnostics.Connections.Max(static entry => entry.Snapshot.MaxConcurrentSubmissions);

        Console.WriteLine("TRACE_BACKFILLER_SUMMARY:");
        Console.WriteLine($"  MessageId={messageId}");
        Console.WriteLine($"  PublishCompleted={(publishResult is not null)}");
        Console.WriteLine($"  PublishStatus={(publishResult?.Status.ToString() ?? "(none)")}");
        Console.WriteLine($"  PublishCode={(publishResult?.ResponseCode?.ToString() ?? "(none)")}");
        Console.WriteLine($"  Totals: Started={totalStarted}, Accepted={totalAccepted}, Rejected={totalRejected}, Ambiguous={totalAmbiguous}, Failed={totalFailed}, Unavailable={totalUnavailable}");
        Console.WriteLine($"  CurrentOutstanding={totalCurrentOutstanding}");
        Console.WriteLine($"  PeakOutstandingPerConnection={peakOutstandingPerConnection}");
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
            CreateBenchmarkResult).ConfigureAwait(false);
    }

    private static BenchmarkResult CreateBenchmarkResult(
        TransitBenchmarkConfig config,
        MeasurementSnapshot snapshot,
        MeasurementMetrics metrics,
        RuntimeMetrics runtime,
        Process process,
        WorkloadPreparationSummary workloadPreparation,
        DateTimeOffset measurementStartUtc,
        DateTimeOffset measurementEndUtc,
        TimeSpan drainDuration,
        long outstandingAtMeasurementEnd,
        long drainedAfterMeasurement,
        long allocatedStartBytes,
        bool enableForensicDiagnostics)
    {
        RuntimeSnapshot runtimeSnapshot = runtime.Snapshot();
        ForensicSnapshot forensic = metrics.CaptureForensicSnapshot();
        double measurementSeconds = config.MeasurementDuration.TotalSeconds;

        long producerObservedTicks = snapshot.ActiveTicks + snapshot.BlockedTicks;
        double blockedPercent = producerObservedTicks <= 0
            ? 0
            : snapshot.BlockedTicks * 100d / producerObservedTicks;

        double activePercent = producerObservedTicks <= 0
            ? 0
            : snapshot.ActiveTicks * 100d / producerObservedTicks;

        double activeMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(snapshot.ActiveTicks);
        double blockedMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(snapshot.BlockedTicks);
        double queueWaitMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(snapshot.ProducerQueueWaitTicks);

        long fallbackAllocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocatedStartBytes;
        double workingSetMb = runtimeSnapshot.LastWorkingSetBytes > 0
            ? runtimeSnapshot.LastWorkingSetBytes / 1024d / 1024d
            : process.WorkingSet64 / 1024d / 1024d;

        double heapMb = runtimeSnapshot.LastGcHeapBytes > 0
            ? runtimeSnapshot.LastGcHeapBytes / 1024d / 1024d
            : GC.GetTotalMemory(forceFullCollection: false) / 1024d / 1024d;

        double allocatedMb = runtimeSnapshot.LastAllocatedBytes > 0
            ? runtimeSnapshot.LastAllocatedBytes / 1024d / 1024d
            : fallbackAllocatedBytes / 1024d / 1024d;

        long effectiveQueueCapacityFromBytes = snapshot.ArticleBytes <= 0 ? 0 : config.MaxResidentBytes / snapshot.ArticleBytes;

        return new BenchmarkResult(
            BenchmarkBuildVersion: TransitServerStressRunner.BenchmarkBuildVersion,
            RuntimeIdentity: TransitServerStressRunner.RuntimeIdentity,
            WorkloadPreparation: workloadPreparation,
            MeasurementStartUtc: measurementStartUtc,
            MeasurementEndUtc: measurementEndUtc,
            DrainDuration: drainDuration,
            OutstandingAtMeasurementEnd: outstandingAtMeasurementEnd,
            DrainedAfterMeasurement: drainedAfterMeasurement,
            GeneratedArticles: snapshot.GeneratedCount,
            GeneratedBytes: snapshot.GeneratedBytes,
            GeneratedGbps: snapshot.GeneratedBytes * 8d / 1_000_000_000d / measurementSeconds,
            AdmittedArticles: snapshot.AdmittedCount,
            AdmittedBytes: snapshot.AdmittedBytes,
            AdmittedGbps: snapshot.AdmittedBytes * 8d / 1_000_000_000d / measurementSeconds,
            AcceptedArticles: snapshot.AcceptedCount,
            AcceptedBytes: snapshot.AcceptedBytes,
            AcceptedGbps: snapshot.AcceptedBytes * 8d / 1_000_000_000d / measurementSeconds,
            RejectedArticles: snapshot.RejectedCount,
            AmbiguousArticles: snapshot.AmbiguousCount,
            MinQueueDepth: snapshot.MinQueueDepth,
            AverageQueueDepth: snapshot.AverageQueueDepth,
            AverageQueuedBytes: snapshot.AverageQueueBytes,
            PeakQueueDepth: snapshot.PeakQueueDepth,
            PeakQueuedBytes: snapshot.PeakQueueBytes,
            PeakInFlight: snapshot.PeakInFlight,
            PeakActualPending: snapshot.PeakActualPending,
            ProducerActivePercent: activePercent,
            ProducerBlockedPercent: blockedPercent,
            ProducerActiveMilliseconds: activeMilliseconds,
            ProducerBlockedMilliseconds: blockedMilliseconds,
            ProducerQueueWaitMilliseconds: queueWaitMilliseconds,
            AverageCpuPercent: runtimeSnapshot.AverageCpuPercent,
            AverageHostCpuPercent: runtimeSnapshot.AverageHostCpuPercent,
            AverageTransitServerCpuPercent: runtimeSnapshot.AverageTransitServerCpuPercent,
            PeakHostCpuPercent: runtimeSnapshot.PeakHostCpuPercent,
            PeakTransitServerCpuPercent: runtimeSnapshot.PeakTransitServerCpuPercent,
            WorkingSetMb: workingSetMb,
            GcHeapMb: heapMb,
            AllocatedMb: allocatedMb,
            Gen0Collections: GC.CollectionCount(0),
            Gen1Collections: GC.CollectionCount(1),
            Gen2Collections: GC.CollectionCount(2),
            AverageDispatchQueueWaitUs: forensic.AverageDispatchQueueWaitUs,
            P50DispatchQueueWaitUs: forensic.P50DispatchQueueWaitUs,
            P95DispatchQueueWaitUs: forensic.P95DispatchQueueWaitUs,
            P99DispatchQueueWaitUs: forensic.P99DispatchQueueWaitUs,
            MaxDispatchQueueWaitUs: forensic.MaxDispatchQueueWaitUs,
            DispatchQueueWaitSampleCount: forensic.DispatchQueueWaitSampleCount,
            AverageSocketWriteUs: forensic.AverageSocketWriteUs,
            P50SocketWriteUs: forensic.P50SocketWriteUs,
            P95SocketWriteUs: forensic.P95SocketWriteUs,
            P99SocketWriteUs: forensic.P99SocketWriteUs,
            MaxSocketWriteUs: forensic.MaxSocketWriteUs,
            SocketWriteSampleCount: forensic.SocketWriteSampleCount,
            AverageResponseWaitUs: forensic.AverageResponseWaitUs,
            P50ResponseWaitUs: forensic.P50ResponseWaitUs,
            P95ResponseWaitUs: forensic.P95ResponseWaitUs,
            P99ResponseWaitUs: forensic.P99ResponseWaitUs,
            MaxResponseWaitUs: forensic.MaxResponseWaitUs,
            ResponseWaitSampleCount: forensic.ResponseWaitSampleCount,
            AverageParseCorrelationUs: forensic.AverageParseCorrelationUs,
            P50ParseCorrelationUs: forensic.P50ParseCorrelationUs,
            P95ParseCorrelationUs: forensic.P95ParseCorrelationUs,
            P99ParseCorrelationUs: forensic.P99ParseCorrelationUs,
            MaxParseCorrelationUs: forensic.MaxParseCorrelationUs,
            ParseCorrelationSampleCount: forensic.ParseCorrelationSampleCount,
            AverageTotalPublishLatencyUs: forensic.AverageTotalPublishLatencyUs,
            P50TotalPublishLatencyUs: forensic.P50TotalPublishLatencyUs,
            P95TotalPublishLatencyUs: forensic.P95TotalPublishLatencyUs,
            P99TotalPublishLatencyUs: forensic.P99TotalPublishLatencyUs,
            MaxTotalPublishLatencyUs: forensic.MaxTotalPublishLatencyUs,
            TotalPublishLatencySampleCount: forensic.TotalPublishLatencySampleCount,
            AveragePublishLatencyUs: forensic.AveragePublishLatencyUs,
            MinPublishLatencyUs: forensic.MinPublishLatencyUs,
            P50PublishLatencyUs: forensic.P50PublishLatencyUs,
            P95PublishLatencyUs: forensic.P95PublishLatencyUs,
            P99PublishLatencyUs: forensic.P99PublishLatencyUs,
            MaxPublishLatencyUs: forensic.MaxPublishLatencyUs,
            AverageLifecycleLatencyUs: forensic.AverageLifecycleLatencyUs,
            PendingDepthLatencyBuckets: forensic.PendingDepthLatencyBuckets,
            ForensicSampleCount: forensic.ForensicSampleCount,
            ConnectionTimeSeriesSummary: forensic.ConnectionTimeSeriesSummary,
            DispatcherTimeSeriesSummary: forensic.DispatcherTimeSeriesSummary,
            ObservabilityNotes: forensic.ObservabilityNotes,
            EffectiveQueueArticleCapacityFromBytes: effectiveQueueCapacityFromBytes);
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
