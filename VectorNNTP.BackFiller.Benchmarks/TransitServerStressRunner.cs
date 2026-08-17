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
        int warmupSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.WarmupSeconds ?? DefaultWarmupSeconds,
            min: 1,
            max: 600,
            optionName: "warmup-seconds");

        int measurementSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.DurationSeconds ?? DefaultGeneratorMeasurementSeconds,
            min: 1,
            max: 3600,
            optionName: "duration-seconds");

        int articleTargetBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.ArticleKilobytes is null ? DefaultArticleTargetBytes : checked(cliOptions.ArticleKilobytes.Value * 1024),
            min: 128 * 1024,
            max: 4 * 1024 * 1024,
            optionName: "article-kib");

        Console.WriteLine("=== Transit Generator Baseline (no network I/O) ===");
        Console.WriteLine("Generator path: TransitBenchmarkCore.BuildMessageId + TransitBenchmarkCore.ArticlePayload.Create + Dispose");
        Console.WriteLine($"Warmup seconds: {warmupSeconds}");
        Console.WriteLine($"Measurement seconds: {measurementSeconds}");
        Console.WriteLine($"Article target bytes: {articleTargetBytes}");

        using CancellationTokenSource warmupCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        warmupCts.CancelAfter(TimeSpan.FromSeconds(warmupSeconds));

        long warmupSequence = 0;
        while (!warmupCts.IsCancellationRequested)
        {
            string warmupMessageId = TransitBenchmarkCore.BuildMessageId(
                benchmarkInstanceId: 0,
                workerId: 0,
                sequence: Interlocked.Increment(ref warmupSequence),
                phase: "gen-warmup");

            TransitBenchmarkCore.ArticlePayload warmupPayload = TransitBenchmarkCore.ArticlePayload.Create(warmupMessageId, articleTargetBytes);
            warmupPayload.Dispose();
        }

        Process process = Process.GetCurrentProcess();
        TimeSpan cpuStart = process.TotalProcessorTime;
        long workingSetPeakBytes = process.WorkingSet64;
        long allocatedStartBytes = GC.GetTotalAllocatedBytes(precise: false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        using CancellationTokenSource measurementCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        measurementCts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        long benchmarkInstanceId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long sequence = 0;
        long generatedArticles = 0;
        long generatedBytes = 0;
        long latencyTicksTotal = 0;
        List<long> latencySamples = new(capacity: 262_144);

        Stopwatch wallClock = Stopwatch.StartNew();

        while (!measurementCts.IsCancellationRequested)
        {
            long sampleStart = Stopwatch.GetTimestamp();

            string messageId = TransitBenchmarkCore.BuildMessageId(
                benchmarkInstanceId,
                workerId: 0,
                sequence: Interlocked.Increment(ref sequence),
                phase: "gen-measure");

            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(messageId, articleTargetBytes);
            int payloadLength = payload.Length;
            payload.Dispose();

            long sampleEnd = Stopwatch.GetTimestamp();
            long sampleTicks = Math.Max(0, sampleEnd - sampleStart);

            generatedArticles++;
            generatedBytes += payloadLength;
            latencyTicksTotal += sampleTicks;
            latencySamples.Add(sampleTicks);

            if ((generatedArticles & 0x3FF) == 0)
            {
                long workingSet = process.WorkingSet64;
                if (workingSet > workingSetPeakBytes)
                {
                    workingSetPeakBytes = workingSet;
                }
            }
        }

        wallClock.Stop();

        TimeSpan cpuEnd = process.TotalProcessorTime;
        long allocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocatedStartBytes;
        int gen0Collections = GC.CollectionCount(0) - gen0Start;
        int gen1Collections = GC.CollectionCount(1) - gen1Start;
        int gen2Collections = GC.CollectionCount(2) - gen2Start;

        double elapsedSeconds = Math.Max(0.000001d, wallClock.Elapsed.TotalSeconds);
        double articlesPerSecond = generatedArticles / elapsedSeconds;
        double mibPerSecond = generatedBytes / 1024d / 1024d / elapsedSeconds;
        double gibPerSecond = generatedBytes / 1024d / 1024d / 1024d / elapsedSeconds;
        double gbps = generatedBytes * 8d / 1_000_000_000d / elapsedSeconds;
        double cpuTimeSeconds = Math.Max(0d, (cpuEnd - cpuStart).TotalSeconds);
        double cpuUtilizationPercent = cpuTimeSeconds / (Environment.ProcessorCount * elapsedSeconds) * 100d;
        double allocatedBytesPerArticle = generatedArticles == 0 ? 0 : (double)allocatedBytes / generatedArticles;

        double avgLatencyUs = generatedArticles == 0
            ? 0
            : latencyTicksTotal * 1_000_000d / (Stopwatch.Frequency * generatedArticles);

        latencySamples.Sort();

        double p50LatencyUs = MetricMathHelpers.ComputePercentileMicroseconds(latencySamples, 0.50);
        double p95LatencyUs = MetricMathHelpers.ComputePercentileMicroseconds(latencySamples, 0.95);
        double p99LatencyUs = MetricMathHelpers.ComputePercentileMicroseconds(latencySamples, 0.99);

        Console.WriteLine();
        Console.WriteLine($"Total articles generated: {generatedArticles}");
        Console.WriteLine($"Total bytes generated: {generatedBytes}");
        Console.WriteLine($"Elapsed time: {wallClock.Elapsed.TotalSeconds:F3}s");
        Console.WriteLine($"Articles/sec: {articlesPerSecond:F4}");
        Console.WriteLine($"MiB/sec: {mibPerSecond:F4}");
        Console.WriteLine($"GiB/sec: {gibPerSecond:F4}");
        Console.WriteLine($"Gbps equivalent: {gbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"CPU time seconds: {cpuTimeSeconds:F4}");
        Console.WriteLine($"CPU utilization %: {cpuUtilizationPercent:F2}");
        Console.WriteLine($"Peak working set MB: {workingSetPeakBytes / 1024d / 1024d:F2}");

        Console.WriteLine();
        Console.WriteLine($"Allocated bytes: {allocatedBytes}");
        Console.WriteLine($"Allocated MB: {allocatedBytes / 1024d / 1024d:F2}");
        Console.WriteLine($"Allocated bytes/article: {allocatedBytesPerArticle:F2}");
        Console.WriteLine($"Allocated KiB/article: {allocatedBytesPerArticle / 1024d:F2}");

        Console.WriteLine();
        Console.WriteLine($"GC Gen0 collections: {gen0Collections}");
        Console.WriteLine($"GC Gen1 collections: {gen1Collections}");
        Console.WriteLine($"GC Gen2 collections: {gen2Collections}");

        Console.WriteLine();
        Console.WriteLine($"Average generation time/article (us): {avgLatencyUs:F3}");
        Console.WriteLine($"P50 generation time/article (us): {p50LatencyUs:F3}");
        Console.WriteLine($"P95 generation time/article (us): {p95LatencyUs:F3}");
        Console.WriteLine($"P99 generation time/article (us): {p99LatencyUs:F3}");

        Console.WriteLine();
        FormatHelpers.PrintRequiredRateComparison(articlesPerSecond, articleTargetBytes);
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

    private sealed class TransitPublisherBenchmarkLogger : ILogger<TransitPublisher>
    {
        private readonly ILogger _inner;

        internal TransitPublisherBenchmarkLogger(ILogger inner)
        {
            _inner = inner;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return _inner.BeginScope(state);
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return _inner.IsEnabled(logLevel);
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!_inner.IsEnabled(logLevel))
            {
                return;
            }

            string message = formatter(state, exception);
            if (ShouldSuppressAccepted239Spam(eventId, logLevel, message))
            {
                return;
            }

            _inner.Log(logLevel, eventId, state, exception, formatter);
        }

        private static bool ShouldSuppressAccepted239Spam(EventId eventId, LogLevel level, string message)
        {
            if (message.Contains("[INIT-TRACE]", StringComparison.Ordinal))
            {
                return true;
            }

            if (level != LogLevel.Information)
            {
                return false;
            }

            if (eventId.Id == 2203)
            {
                return true;
            }

            if (eventId.Id != 2204)
            {
                return false;
            }

            if (!message.Contains("Status=Accepted", StringComparison.Ordinal))
            {
                return false;
            }

            return message.Contains("ResponseCode=239", StringComparison.Ordinal);
        }
    }

    }
