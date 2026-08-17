using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
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

        EnsureRuntimeIdentityMatches(config.ExpectedRuntimeIdentity);

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
            BuildRuntimeOptions(config),
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

    private static async Task RunCoreAsync(TransitBenchmarkConfig config, CancellationToken cancellationToken)
    {
        EnsureRuntimeIdentityMatches(config.ExpectedRuntimeIdentity);

        Console.WriteLine("=== Transit Publisher Production-Path Benchmark ===");
        Console.WriteLine("Benchmark execution policy: NEVER use --no-build. ALWAYS run clean -> build -> verify output identity -> execute.");
        Console.WriteLine($"Benchmark Build Version: {BenchmarkBuildVersion}");
        Console.WriteLine($"RuntimeAssemblyPath: {RuntimeIdentity.RuntimeAssemblyPath}");
        Console.WriteLine($"RuntimeAssemblyVersion: {RuntimeIdentity.RuntimeAssemblyVersion}");
        Console.WriteLine($"AssemblyFileVersion: {RuntimeIdentity.AssemblyFileVersion ?? "(unknown)"}");
        Console.WriteLine($"ProcessPath: {RuntimeIdentity.ProcessPath}");
        Console.WriteLine($"WorkingDirectory: {RuntimeIdentity.WorkingDirectory}");
        Console.WriteLine($"Configuration: {RuntimeIdentity.Configuration ?? "(unknown)"}");
        Console.WriteLine($"Platform: {RuntimeIdentity.Platform ?? "(unknown)"}");
        Console.WriteLine($"TargetFramework: {RuntimeIdentity.TargetFramework ?? "(unknown)"}");
        Console.WriteLine($"RuntimeIdentifier: {RuntimeIdentity.RuntimeIdentifier ?? "(unknown)"}");
        Console.WriteLine($"Architecture: {RuntimeIdentity.Architecture}");
        Console.WriteLine($"SourceRevision: {RuntimeIdentity.SourceRevision ?? "(unknown)"}");
        Console.WriteLine($"BuildTimestampUtc: {(RuntimeIdentity.BuildTimestampUtc.HasValue ? RuntimeIdentity.BuildTimestampUtc.Value.ToString("O", CultureInfo.InvariantCulture) : "(unknown)")}");
        Console.WriteLine($"Mode: {config.Mode}");
        Console.WriteLine($"Experiment profile: {(config.Mode == BenchmarkMode.Saturation ? "Saturation discovery" : "Fixed-duration")}");
        Console.WriteLine($"Config path: {config.AppSettingsPath}");
        Console.WriteLine($"Logical Transit endpoint host (TLS/SNI/cert): {config.EndpointHost}");
        Console.WriteLine($"Transit port: {config.EndpointPort}");
        Console.WriteLine($"Transit UseSsl config: {config.EndpointUseSsl}");
        Console.WriteLine($"Connection pool size: {config.ConnectionPoolSize}");
        Console.WriteLine($"Per-connection pipeline depth: {config.PerConnectionPipelineDepth}");
        Console.WriteLine($"Dispatch worker count: {config.DispatchWorkerCount}");
        Console.WriteLine($"Generator worker count: {config.GeneratorWorkerCount}");
        Console.WriteLine($"Target article bytes: {config.ArticleTargetBytes}");
        Console.WriteLine($"Queue max articles: {config.MaxQueuedArticles}");
        Console.WriteLine($"Queue max resident bytes: {config.MaxResidentBytes}");
        Console.WriteLine($"Producer queue target articles: {config.ProducerQueueTargetArticles}");

        IPAddress[] resolved = await Dns.GetHostAddressesAsync(config.EndpointHost, cancellationToken).ConfigureAwait(false);
        Console.WriteLine($"Resolved addresses for {config.EndpointHost}: {string.Join(", ", resolved.Select(static x => x.ToString()))}");

        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .SetMinimumLevel(LogLevel.Information)
                .AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "HH:mm:ss ";
                });
        });

        ILogger<TransitPublisher> transitPublisherLogger = CreateTransitPublisherLogger(loggerFactory);

        await using TransitPublisher publisher = new(
            BuildRuntimeOptions(config),
            TimeProvider.System,
            transitPublisherLogger,
            connectionPoolSize: config.ConnectionPoolSize,
            perConnectionPipelineDepth: config.PerConnectionPipelineDepth);

        Console.WriteLine();
        Console.WriteLine("=== Phase 1: Initialization ===");
        Console.WriteLine("=== Phase 2: TLS / TransitPublisher startup ===");
        await publisher.InitializeAsync(cancellationToken).ConfigureAwait(false);

        Console.WriteLine();
        Console.WriteLine("=== Phase 3: Smoke test (REAL publisher, realistic ~1MiB articles) ===");
        await RunSmokeAsync(publisher, config, cancellationToken).ConfigureAwait(false);

        Console.WriteLine();
        Console.WriteLine("=== Phase 3.5: Workload preparation ===");
        using PreparedBenchmarkWorkload workload = BenchmarkWorkloadFactory.PrepareBenchmarkWorkload(config);

        Console.WriteLine();
        Console.WriteLine("=== Phase 4: Warmup ===");
        await RunWarmupAsync(publisher, config, workload, cancellationToken).ConfigureAwait(false);

        Console.WriteLine();
        Console.WriteLine("=== Phase 5: EXACT measurement window ===");
        BenchmarkResult result = await RunMeasurementAsync(
            publisher,
            config,
            workload,
            cancellationToken,
            enableForensicDiagnostics: false).ConfigureAwait(false);

        Console.WriteLine();
        Console.WriteLine("=== Phase 7: Connection topology diagnostics ===");
        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot connectionDiagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
        TopologyReporter.PrintConnectionTopologyDiagnostics(connectionDiagnostics);

        Console.WriteLine();
        Console.WriteLine("=== Phase 8: Final results ===");
        BenchmarkConsoleReporter.PrintFinalReport(result, config);
        WriteStructuredResultArtifacts(result, config);
    }

    private static async Task RunSmokeAsync(TransitPublisher publisher, TransitBenchmarkConfig config, CancellationToken cancellationToken)
    {
        const int smokeArticles = 5;

        for (int i = 0; i < smokeArticles; i++)
        {
            string messageId = TransitBenchmarkCore.BuildMessageId(config.BenchmarkInstanceId, workerId: 0, sequence: i + 1, phase: "smoke");
            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(messageId, config.ArticleTargetBytes);

            try
            {
                TransitPublishResult result = await publisher.PublishAsync(messageId, payload.AsMemory(), cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"Smoke article {i + 1}/{smokeArticles}: Status={result.Status}, Code={result.ResponseCode}, Bytes={payload.Length}");

                if (result.Status != TransitPublishStatus.Accepted)
                {
                    throw new InvalidOperationException($"Smoke test requires definitive success. Got {result.Status} ({result.ResponseCode}) for {messageId}.");
                }
            }
            finally
            {
                payload.Dispose();
            }
        }
    }

    private static async Task RunWarmupAsync(TransitPublisher publisher, TransitBenchmarkConfig config, PreparedBenchmarkWorkload workload, CancellationToken cancellationToken)
    {
        using CancellationTokenSource warmupCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        warmupCts.CancelAfter(config.WarmupDuration);

        while (!warmupCts.IsCancellationRequested)
        {
            if (!workload.TryTakeNextMessageId(out string? messageId))
            {
                throw new InvalidOperationException("Pre-generated Message-ID pool exhausted during warmup.");
            }

            try
            {
                _ = await publisher.PublishAsync(messageId, workload.ReusableArticlePayload, warmupCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (warmupCts.IsCancellationRequested)
            {
                break;
            }
        }

        Console.WriteLine($"Warmup complete ({config.WarmupDuration.TotalSeconds:F0}s).");
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

        DateTimeOffset measurementEndUtc = DateTimeOffset.UtcNow;
        Console.WriteLine($"Measurement end UTC:   {measurementEndUtc:O}");

        producerStopCts.Cancel();

        try
        {
            await Task.WhenAll(producerTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        queue.StopAdmission();

        long outstandingAtMeasurementEnd = metrics.GetAdmittedCount() - metrics.GetCompletedCount();
        long completedAtMeasurementEnd = metrics.GetCompletedCount();

        try
        {
            await telemetryTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnosticsAtMeasurementEnd = publisher.CaptureConnectionDiagnosticsSnapshot();
        int pendingMessageIdsAtMeasurementEnd = diagnosticsAtMeasurementEnd.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
        long queuedWriteIntentsAtMeasurementEnd = diagnosticsAtMeasurementEnd.Connections.Sum(static entry => entry.Snapshot.CurrentWriteIntentQueueDepth);

        Console.WriteLine("[SHUTDOWN-DIAG] Measurement window expired: outstandingSubmissions={OutstandingSubmissions} queuedSubmissions={QueuedSubmissions} pendingMessageIds={PendingMessageIds} queuedWriteIntents={QueuedWriteIntents}",
            outstandingAtMeasurementEnd,
            diagnosticsAtMeasurementEnd.QueuedSubmissionCount,
            pendingMessageIdsAtMeasurementEnd,
            queuedWriteIntentsAtMeasurementEnd);

        foreach (TransitPublisher.ConnectionDiagnosticsEntry entry in diagnosticsAtMeasurementEnd.Connections
                     .OrderBy(static x => x.SlotIndex)
                     .ThenBy(static x => x.Snapshot.ConnectionId, StringComparer.Ordinal))
        {
            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = entry.Snapshot;
            Console.WriteLine("[SHUTDOWN-DIAG] Measurement-end connection snapshot: slot={SlotIndex} connectionId={ConnectionId} state={State} inFlight={InFlight} writeQueueDepth={WriteQueueDepth} pendingMessageIds={PendingMessageIds}",
                entry.SlotIndex,
                snapshot.ConnectionId,
                snapshot.CurrentState,
                snapshot.CurrentConcurrentSubmissions,
                snapshot.CurrentWriteIntentQueueDepth,
                snapshot.OutstandingOperations.Length);

            foreach (TransitConnection.OutstandingPublishOperationSnapshot operation in snapshot.OutstandingOperations)
            {
                Console.WriteLine("[SHUTDOWN-DIAG] Outstanding operation: connectionId={ConnectionId} messageId={MessageId} writeIntentEnqueued={WriteIntentEnqueued} takethisStagedForWrite={TakethisStagedForWrite} flushCompleted={FlushCompleted} waitingFor239Response={WaitingFor239Response} completionTaskStatus={CompletionTaskStatus} completionStatus={CompletionStatus} likelyAwaitingPath={LikelyAwaitingPath} t2Enqueued={T2WriteIntentEnqueuedTick} t6Staged={T6FrameStageEndTick} t8Flush={T8BatchFlushEndTick} t9Correlated={T9ResponseCorrelatedTick}",
                    snapshot.ConnectionId,
                    operation.MessageId,
                    operation.WriteIntentEnqueued,
                    operation.TakethisStagedForWrite,
                    operation.FlushCompleted,
                    operation.WaitingFor239Response,
                    operation.CompletionTaskStatus,
                    operation.CompletionStatus?.ToString() ?? "(null)",
                    operation.LikelyAwaitingPath,
                    operation.T2WriteIntentEnqueuedTick,
                    operation.T6FrameStageEndTick,
                    operation.T8BatchFlushEndTick,
                    operation.T9ResponseCorrelatedTick);
            }
        }

        Console.WriteLine();
        Console.WriteLine("=== Phase 6: Drain ===");
        Stopwatch drainStopwatch = Stopwatch.StartNew();
        await Task.WhenAll(dispatchers).ConfigureAwait(false);
        drainStopwatch.Stop();

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnosticsAfterDrain = publisher.CaptureConnectionDiagnosticsSnapshot();
        int pendingMessageIdsAfterDrain = diagnosticsAfterDrain.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
        long queuedWriteIntentsAfterDrain = diagnosticsAfterDrain.Connections.Sum(static entry => entry.Snapshot.CurrentWriteIntentQueueDepth);

        Console.WriteLine("[SHUTDOWN-DIAG] Drain completed: outstandingSubmissions={OutstandingSubmissions} queuedSubmissions={QueuedSubmissions} pendingMessageIds={PendingMessageIds} queuedWriteIntents={QueuedWriteIntents}",
            metrics.GetAdmittedCount() - metrics.GetCompletedCount(),
            diagnosticsAfterDrain.QueuedSubmissionCount,
            pendingMessageIdsAfterDrain,
            queuedWriteIntentsAfterDrain);

        long drainedAfterMeasurement = Math.Max(0, metrics.GetCompletedCount() - completedAtMeasurementEnd);

        return CreateBenchmarkResult(
            config,
            metrics.Snapshot(),
            metrics,
            runtime,
            process,
            workload.PreparationSummary,
            measurementStartUtc,
            measurementEndUtc,
            drainStopwatch.Elapsed,
            outstandingAtMeasurementEnd,
            drainedAfterMeasurement,
            allocatedStartBytes,
            enableForensicDiagnostics);
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

    private static void EnsureRuntimeIdentityMatches(RuntimeIdentityExpectation expected)
    {
        RuntimeIdentityGuard.EnsureMatches(expected, RuntimeIdentity);
    }

    private static BackFillerRuntimeOptions BuildRuntimeOptions(TransitBenchmarkConfig config)
    {
        return new BackFillerRuntimeOptions(
            CanonicalBackFillerFqdn: "benchmark.backfiller.usenet.ninja",
            BackFillerId: 1,
            CanonicalDnsSuffix: "usenet.ninja",
            ValidatedLogDirectory: Path.GetTempPath(),
            ValidatedCertificateDirectory: Path.GetTempPath(),
            RabbitMqHosts: [],
            RabbitMqPort: 5672,
            RabbitMqEnableSsl: false,
            TransitServerHost: config.EndpointHost,
            TransitServerPort: config.EndpointPort,
            TransitServerUseSsl: config.EndpointUseSsl,
            ShutdownGracePeriodSeconds: 120,
            ShutdownDrainQueuedWork: true,
            ShutdownFinishActiveArticles: true,
            RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
            WriteBatchCoalesceMicroseconds: config.WriteBatchCoalesceMicroseconds);
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

    private readonly record struct BenchmarkResultArtifact(
        string BenchmarkBuildVersion,
        string RuntimeAssemblyVersion,
        string RuntimeAssemblyPath,
        string AssemblyFileVersion,
        string ProcessPath,
        string WorkingDirectory,
        string Configuration,
        string Platform,
        string TargetFramework,
        string RuntimeIdentifier,
        string Architecture,
        string SourceRevision,
        string BuildTimestampUtc,
        double WorkloadPreGenerationMs,
        double PayloadPreparationMs,
        int MessageIdPoolSize,
        int UniqueMessageIdCount,
        int DuplicateMessageIdCount,
        int ReusablePayloadBytes,
        string Mode,
        double WarmupSeconds,
        double MeasurementSeconds,
        int ConnectionPoolSize,
        int PipelineDepth,
        int DispatchWorkers,
        int GeneratorWorkers,
        int QueueMaxArticles,
        long QueueMaxBytes,
        int ArticleTargetBytes,
        int ProducerQueueTargetArticles,
        long GeneratedArticles,
        double GeneratedGbps,
        long AdmittedArticles,
        double AdmittedGbps,
        long AcceptedArticles,
        double AcceptedGbps,
        long RejectedArticles,
        long AmbiguousArticles,
        long PeakQueueDepth,
        long PeakQueueBytes,
        long PeakDispatcherInFlight,
        long PeakActualPending,
        double ProducerBlockedPercent,
        double ProducerQueueWaitMs,
        double AverageCpuPercent,
        double AverageHostCpuPercent,
        double AverageTransitServerCpuPercent,
        double PeakHostCpuPercent,
        double PeakTransitServerCpuPercent,
        double EquivalentBusyCores,
        double WorkingSetMb,
        double GcHeapMb,
        double AllocatedMb,
        int Gen0,
        int Gen1,
        int Gen2,
        double AverageDispatchQueueWaitUs,
        double P50DispatchQueueWaitUs,
        double P95DispatchQueueWaitUs,
        double P99DispatchQueueWaitUs,
        double MaxDispatchQueueWaitUs,
        long DispatchQueueWaitSampleCount,
        double AverageSocketWriteUs,
        double P50SocketWriteUs,
        double P95SocketWriteUs,
        double P99SocketWriteUs,
        double MaxSocketWriteUs,
        long SocketWriteSampleCount,
        double AverageResponseWaitUs,
        double P50ResponseWaitUs,
        double P95ResponseWaitUs,
        double P99ResponseWaitUs,
        double MaxResponseWaitUs,
        long ResponseWaitSampleCount,
        double AverageParseCorrelationUs,
        double P50ParseCorrelationUs,
        double P95ParseCorrelationUs,
        double P99ParseCorrelationUs,
        double MaxParseCorrelationUs,
        long ParseCorrelationSampleCount,
        double AverageTotalPublishLatencyUs,
        double P50TotalPublishLatencyUs,
        double P95TotalPublishLatencyUs,
        double P99TotalPublishLatencyUs,
        double MaxTotalPublishLatencyUs,
        long TotalPublishLatencySampleCount,
        double AveragePublishLatencyUs,
        double P50PublishLatencyUs,
        double P95PublishLatencyUs,
        double P99PublishLatencyUs,
        double MaxPublishLatencyUs,
        double AverageLifecycleLatencyUs,
        string PendingDepthLatencyBuckets,
        long EffectiveQueueArticleCapacityFromBytes,
        string ObservabilityNotes)
    {
        internal static BenchmarkResultArtifact From(BenchmarkResult result, TransitBenchmarkConfig config, int processorCount)
        {
            return new BenchmarkResultArtifact(
                BenchmarkBuildVersion: result.BenchmarkBuildVersion,
                RuntimeAssemblyVersion: result.RuntimeIdentity.RuntimeAssemblyVersion,
                RuntimeAssemblyPath: result.RuntimeIdentity.RuntimeAssemblyPath,
                AssemblyFileVersion: result.RuntimeIdentity.AssemblyFileVersion ?? "(unknown)",
                ProcessPath: result.RuntimeIdentity.ProcessPath,
                WorkingDirectory: result.RuntimeIdentity.WorkingDirectory,
                Configuration: result.RuntimeIdentity.Configuration ?? "(unknown)",
                Platform: result.RuntimeIdentity.Platform ?? "(unknown)",
                TargetFramework: result.RuntimeIdentity.TargetFramework ?? "(unknown)",
                RuntimeIdentifier: result.RuntimeIdentity.RuntimeIdentifier ?? "(unknown)",
                Architecture: result.RuntimeIdentity.Architecture,
                SourceRevision: result.RuntimeIdentity.SourceRevision ?? "(unknown)",
                BuildTimestampUtc: result.RuntimeIdentity.BuildTimestampUtc?.ToString("O", CultureInfo.InvariantCulture) ?? "(unknown)",
                WorkloadPreGenerationMs: result.WorkloadPreparation.PreGenerationDurationMilliseconds,
                PayloadPreparationMs: result.WorkloadPreparation.PayloadPreparationDurationMilliseconds,
                MessageIdPoolSize: result.WorkloadPreparation.MessageIdPoolSize,
                UniqueMessageIdCount: result.WorkloadPreparation.UniqueMessageIdCount,
                DuplicateMessageIdCount: result.WorkloadPreparation.DuplicateMessageIdCount,
                ReusablePayloadBytes: result.WorkloadPreparation.ReusablePayloadBytes,
                Mode: config.Mode.ToString(),
                WarmupSeconds: config.WarmupDuration.TotalSeconds,
                MeasurementSeconds: config.MeasurementDuration.TotalSeconds,
                ConnectionPoolSize: config.ConnectionPoolSize,
                PipelineDepth: config.PerConnectionPipelineDepth,
                DispatchWorkers: config.DispatchWorkerCount,
                GeneratorWorkers: config.GeneratorWorkerCount,
                QueueMaxArticles: config.MaxQueuedArticles,
                QueueMaxBytes: config.MaxResidentBytes,
                ArticleTargetBytes: config.ArticleTargetBytes,
                ProducerQueueTargetArticles: config.ProducerQueueTargetArticles,
                GeneratedArticles: result.GeneratedArticles,
                GeneratedGbps: result.GeneratedGbps,
                AdmittedArticles: result.AdmittedArticles,
                AdmittedGbps: result.AdmittedGbps,
                AcceptedArticles: result.AcceptedArticles,
                AcceptedGbps: result.AcceptedGbps,
                RejectedArticles: result.RejectedArticles,
                AmbiguousArticles: result.AmbiguousArticles,
                PeakQueueDepth: result.PeakQueueDepth,
                PeakQueueBytes: result.PeakQueuedBytes,
                PeakDispatcherInFlight: result.PeakInFlight,
                PeakActualPending: result.PeakActualPending,
                ProducerBlockedPercent: result.ProducerBlockedPercent,
                ProducerQueueWaitMs: result.ProducerQueueWaitMilliseconds,
                AverageCpuPercent: result.AverageCpuPercent,
                AverageHostCpuPercent: result.AverageHostCpuPercent,
                AverageTransitServerCpuPercent: result.AverageTransitServerCpuPercent,
                PeakHostCpuPercent: result.PeakHostCpuPercent,
                PeakTransitServerCpuPercent: result.PeakTransitServerCpuPercent,
                EquivalentBusyCores: result.AverageCpuPercent / 100d * processorCount,
                WorkingSetMb: result.WorkingSetMb,
                GcHeapMb: result.GcHeapMb,
                AllocatedMb: result.AllocatedMb,
                Gen0: result.Gen0Collections,
                Gen1: result.Gen1Collections,
                Gen2: result.Gen2Collections,
                AverageDispatchQueueWaitUs: result.AverageDispatchQueueWaitUs,
                P50DispatchQueueWaitUs: result.P50DispatchQueueWaitUs,
                P95DispatchQueueWaitUs: result.P95DispatchQueueWaitUs,
                P99DispatchQueueWaitUs: result.P99DispatchQueueWaitUs,
                MaxDispatchQueueWaitUs: result.MaxDispatchQueueWaitUs,
                DispatchQueueWaitSampleCount: result.DispatchQueueWaitSampleCount,
                AverageSocketWriteUs: result.AverageSocketWriteUs,
                P50SocketWriteUs: result.P50SocketWriteUs,
                P95SocketWriteUs: result.P95SocketWriteUs,
                P99SocketWriteUs: result.P99SocketWriteUs,
                MaxSocketWriteUs: result.MaxSocketWriteUs,
                SocketWriteSampleCount: result.SocketWriteSampleCount,
                AverageResponseWaitUs: result.AverageResponseWaitUs,
                P50ResponseWaitUs: result.P50ResponseWaitUs,
                P95ResponseWaitUs: result.P95ResponseWaitUs,
                P99ResponseWaitUs: result.P99ResponseWaitUs,
                MaxResponseWaitUs: result.MaxResponseWaitUs,
                ResponseWaitSampleCount: result.ResponseWaitSampleCount,
                AverageParseCorrelationUs: result.AverageParseCorrelationUs,
                P50ParseCorrelationUs: result.P50ParseCorrelationUs,
                P95ParseCorrelationUs: result.P95ParseCorrelationUs,
                P99ParseCorrelationUs: result.P99ParseCorrelationUs,
                MaxParseCorrelationUs: result.MaxParseCorrelationUs,
                ParseCorrelationSampleCount: result.ParseCorrelationSampleCount,
                AverageTotalPublishLatencyUs: result.AverageTotalPublishLatencyUs,
                P50TotalPublishLatencyUs: result.P50TotalPublishLatencyUs,
                P95TotalPublishLatencyUs: result.P95TotalPublishLatencyUs,
                P99TotalPublishLatencyUs: result.P99TotalPublishLatencyUs,
                MaxTotalPublishLatencyUs: result.MaxTotalPublishLatencyUs,
                TotalPublishLatencySampleCount: result.TotalPublishLatencySampleCount,
                AveragePublishLatencyUs: result.AveragePublishLatencyUs,
                P50PublishLatencyUs: result.P50PublishLatencyUs,
                P95PublishLatencyUs: result.P95PublishLatencyUs,
                P99PublishLatencyUs: result.P99PublishLatencyUs,
                MaxPublishLatencyUs: result.MaxPublishLatencyUs,
                AverageLifecycleLatencyUs: result.AverageLifecycleLatencyUs,
                PendingDepthLatencyBuckets: result.PendingDepthLatencyBuckets,
                EffectiveQueueArticleCapacityFromBytes: result.EffectiveQueueArticleCapacityFromBytes,
                ObservabilityNotes: result.ObservabilityNotes);
        }

        internal string ToCsv()
        {
            string[] headers =
            [
                "benchmark_build_version","runtime_assembly_version","runtime_assembly_path","assembly_file_version","process_path","working_directory","configuration","platform","target_framework","runtime_identifier","architecture","source_revision","build_timestamp_utc","workload_pregeneration_ms","payload_preparation_ms","message_id_pool_size","message_id_unique_count","message_id_duplicate_count","reusable_payload_bytes","mode","warmup_seconds","measurement_seconds","connections","pipeline_depth","dispatch_workers","generator_workers",
                "queue_max_articles","queue_max_bytes","article_target_bytes","producer_queue_target_articles",
                "generated_articles","generated_gbps","admitted_articles","admitted_gbps","accepted_articles","accepted_gbps",
                "rejected_articles","ambiguous_articles","peak_queue_depth","peak_queue_bytes","peak_dispatcher_in_flight","peak_actual_pending",
                "producer_blocked_percent","producer_queue_wait_ms","bf_cpu_avg_percent","host_cpu_avg_percent","transit_cpu_avg_percent",
                "host_cpu_peak_percent","transit_cpu_peak_percent","equivalent_busy_cores","working_set_mb","gc_heap_mb","allocated_mb",
                "gen0","gen1","gen2","dispatch_wait_us_avg","dispatch_wait_us_p50","dispatch_wait_us_p95","dispatch_wait_us_p99","dispatch_wait_us_max","dispatch_wait_samples",
                "socket_write_us_avg","socket_write_us_p50","socket_write_us_p95","socket_write_us_p99","socket_write_us_max","socket_write_samples",
                "response_wait_us_avg","response_wait_us_p50","response_wait_us_p95","response_wait_us_p99","response_wait_us_max","response_wait_samples",
                "parse_correlation_us_avg","parse_correlation_us_p50","parse_correlation_us_p95","parse_correlation_us_p99","parse_correlation_us_max","parse_correlation_samples",
                "publish_total_us_avg","publish_total_us_p50","publish_total_us_p95","publish_total_us_p99","publish_total_us_max","publish_total_samples",
                "publish_latency_us_avg","publish_latency_us_p50","publish_latency_us_p95","publish_latency_us_p99",
                "publish_latency_us_max","lifecycle_latency_us_avg","effective_queue_article_capacity_from_bytes",
                "pending_depth_latency_buckets","observability_notes"
            ];

            string[] values =
            [
                Escape(BenchmarkBuildVersion),
                Escape(RuntimeAssemblyVersion),
                Escape(RuntimeAssemblyPath),
                Escape(AssemblyFileVersion),
                Escape(ProcessPath),
                Escape(WorkingDirectory),
                Escape(Configuration),
                Escape(Platform),
                Escape(TargetFramework),
                Escape(RuntimeIdentifier),
                Escape(Architecture),
                Escape(SourceRevision),
                Escape(BuildTimestampUtc),
                WorkloadPreGenerationMs.ToString("F2", CultureInfo.InvariantCulture),
                PayloadPreparationMs.ToString("F2", CultureInfo.InvariantCulture),
                MessageIdPoolSize.ToString(CultureInfo.InvariantCulture),
                UniqueMessageIdCount.ToString(CultureInfo.InvariantCulture),
                DuplicateMessageIdCount.ToString(CultureInfo.InvariantCulture),
                ReusablePayloadBytes.ToString(CultureInfo.InvariantCulture),
                Escape(Mode),
                WarmupSeconds.ToString("F0", CultureInfo.InvariantCulture),
                MeasurementSeconds.ToString("F0", CultureInfo.InvariantCulture),
                ConnectionPoolSize.ToString(CultureInfo.InvariantCulture),
                PipelineDepth.ToString(CultureInfo.InvariantCulture),
                DispatchWorkers.ToString(CultureInfo.InvariantCulture),
                GeneratorWorkers.ToString(CultureInfo.InvariantCulture),
                QueueMaxArticles.ToString(CultureInfo.InvariantCulture),
                QueueMaxBytes.ToString(CultureInfo.InvariantCulture),
                ArticleTargetBytes.ToString(CultureInfo.InvariantCulture),
                ProducerQueueTargetArticles.ToString(CultureInfo.InvariantCulture),
                GeneratedArticles.ToString(CultureInfo.InvariantCulture),
                GeneratedGbps.ToString("F4", CultureInfo.InvariantCulture),
                AdmittedArticles.ToString(CultureInfo.InvariantCulture),
                AdmittedGbps.ToString("F4", CultureInfo.InvariantCulture),
                AcceptedArticles.ToString(CultureInfo.InvariantCulture),
                AcceptedGbps.ToString("F4", CultureInfo.InvariantCulture),
                RejectedArticles.ToString(CultureInfo.InvariantCulture),
                AmbiguousArticles.ToString(CultureInfo.InvariantCulture),
                PeakQueueDepth.ToString(CultureInfo.InvariantCulture),
                PeakQueueBytes.ToString(CultureInfo.InvariantCulture),
                PeakDispatcherInFlight.ToString(CultureInfo.InvariantCulture),
                PeakActualPending.ToString(CultureInfo.InvariantCulture),
                ProducerBlockedPercent.ToString("F2", CultureInfo.InvariantCulture),
                ProducerQueueWaitMs.ToString("F2", CultureInfo.InvariantCulture),
                AverageCpuPercent.ToString("F2", CultureInfo.InvariantCulture),
                AverageHostCpuPercent.ToString("F2", CultureInfo.InvariantCulture),
                AverageTransitServerCpuPercent.ToString("F2", CultureInfo.InvariantCulture),
                PeakHostCpuPercent.ToString("F2", CultureInfo.InvariantCulture),
                PeakTransitServerCpuPercent.ToString("F2", CultureInfo.InvariantCulture),
                EquivalentBusyCores.ToString("F3", CultureInfo.InvariantCulture),
                WorkingSetMb.ToString("F2", CultureInfo.InvariantCulture),
                GcHeapMb.ToString("F2", CultureInfo.InvariantCulture),
                AllocatedMb.ToString("F2", CultureInfo.InvariantCulture),
                Gen0.ToString(CultureInfo.InvariantCulture),
                Gen1.ToString(CultureInfo.InvariantCulture),
                Gen2.ToString(CultureInfo.InvariantCulture),
                AverageDispatchQueueWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                P50DispatchQueueWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                P95DispatchQueueWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                P99DispatchQueueWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                MaxDispatchQueueWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                DispatchQueueWaitSampleCount.ToString(CultureInfo.InvariantCulture),
                AverageSocketWriteUs.ToString("F3", CultureInfo.InvariantCulture),
                P50SocketWriteUs.ToString("F3", CultureInfo.InvariantCulture),
                P95SocketWriteUs.ToString("F3", CultureInfo.InvariantCulture),
                P99SocketWriteUs.ToString("F3", CultureInfo.InvariantCulture),
                MaxSocketWriteUs.ToString("F3", CultureInfo.InvariantCulture),
                SocketWriteSampleCount.ToString(CultureInfo.InvariantCulture),
                AverageResponseWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                P50ResponseWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                P95ResponseWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                P99ResponseWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                MaxResponseWaitUs.ToString("F3", CultureInfo.InvariantCulture),
                ResponseWaitSampleCount.ToString(CultureInfo.InvariantCulture),
                AverageParseCorrelationUs.ToString("F3", CultureInfo.InvariantCulture),
                P50ParseCorrelationUs.ToString("F3", CultureInfo.InvariantCulture),
                P95ParseCorrelationUs.ToString("F3", CultureInfo.InvariantCulture),
                P99ParseCorrelationUs.ToString("F3", CultureInfo.InvariantCulture),
                MaxParseCorrelationUs.ToString("F3", CultureInfo.InvariantCulture),
                ParseCorrelationSampleCount.ToString(CultureInfo.InvariantCulture),
                AverageTotalPublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                P50TotalPublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                P95TotalPublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                P99TotalPublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                MaxTotalPublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                TotalPublishLatencySampleCount.ToString(CultureInfo.InvariantCulture),
                AveragePublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                P50PublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                P95PublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                P99PublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                MaxPublishLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                AverageLifecycleLatencyUs.ToString("F3", CultureInfo.InvariantCulture),
                EffectiveQueueArticleCapacityFromBytes.ToString(CultureInfo.InvariantCulture),
                Escape(PendingDepthLatencyBuckets),
                Escape(ObservabilityNotes)
            ];

            return string.Join(',', headers) + Environment.NewLine + string.Join(',', values) + Environment.NewLine;

            static string Escape(string value)
            {
                string escaped = value.Replace("\"", "\"\"");
                return $"\"{escaped}\"";
            }
        }
    }

    }
