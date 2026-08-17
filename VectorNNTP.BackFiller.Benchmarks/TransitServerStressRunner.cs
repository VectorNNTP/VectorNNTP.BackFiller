using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class TransitServerStressRunner
{
    private const string RequiredTransitHostname = "incoming.usenet.ninja";
    private const int DefaultArticleTargetBytes = 1 * 1024 * 1024;
    private const int DefaultWarmupSeconds = 10;
    private const int ValidationSeconds = 10;
    private const int DefaultGeneratorMeasurementSeconds = 30;
    private const int PreGeneratedMessageIdPoolSize = 2_000_000;

    private static readonly object HostCpuGate = new();
    private static DateTime _hostCpuLastSampleUtc;
    private static long _hostCpuLastTotalProcessTicks;

    private static readonly object TransitServerCpuGate = new();
    private static DateTime _transitServerCpuLastSampleUtc;
    private static long _transitServerCpuLastTotalTicks;
    private static readonly RuntimeExecutionIdentity RuntimeIdentity = CaptureRuntimeExecutionIdentity();
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

        double p50LatencyUs = ComputePercentileMicroseconds(latencySamples, 0.50);
        double p95LatencyUs = ComputePercentileMicroseconds(latencySamples, 0.95);
        double p99LatencyUs = ComputePercentileMicroseconds(latencySamples, 0.99);

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
        PrintRequiredRateComparison(articlesPerSecond, articleTargetBytes);
    }

    private static double ComputePercentileMicroseconds(List<long> sortedLatencyTicks, double percentile)
    {
        ArgumentNullException.ThrowIfNull(sortedLatencyTicks);

        if (sortedLatencyTicks.Count == 0)
        {
            return 0;
        }

        percentile = Math.Clamp(percentile, 0d, 1d);
        int index = (int)Math.Clamp(Math.Ceiling(percentile * sortedLatencyTicks.Count) - 1, 0, sortedLatencyTicks.Count - 1);
        long ticks = sortedLatencyTicks[index];
        return TransitBenchmarkCore.StopwatchTicksToMilliseconds(ticks) * 1000d;
    }

    private static void PrintRequiredRateComparison(double currentArticlesPerSecond, int articleBytes)
    {
        double bitsPerArticle = articleBytes * 8d;

        double articlesPerSecond10Gbps = 10_000_000_000d / bitsPerArticle;
        double articlesPerSecond20Gbps = 20_000_000_000d / bitsPerArticle;
        double articlesPerSecond30Gbps = 30_000_000_000d / bitsPerArticle;
        double articlesPerSecond40Gbps = 40_000_000_000d / bitsPerArticle;

        double requiredImprovementFor10Gbps = currentArticlesPerSecond <= 0
            ? double.PositiveInfinity
            : articlesPerSecond10Gbps / currentArticlesPerSecond;

        double currentTo10GbpsRatio = articlesPerSecond10Gbps <= 0
            ? 0
            : currentArticlesPerSecond / articlesPerSecond10Gbps;

        Console.WriteLine($"Required rate for 10 Gbps: {articlesPerSecond10Gbps:F4} articles/sec");
        Console.WriteLine($"Required rate for 20 Gbps: {articlesPerSecond20Gbps:F4} articles/sec");
        Console.WriteLine($"Required rate for 30 Gbps: {articlesPerSecond30Gbps:F4} articles/sec");
        Console.WriteLine($"Required rate for 40 Gbps: {articlesPerSecond40Gbps:F4} articles/sec");
        Console.WriteLine($"Current rate: {currentArticlesPerSecond:F4} articles/sec");
        Console.WriteLine($"Required improvement for 10 Gbps: {requiredImprovementFor10Gbps:F4}x");
        Console.WriteLine($"Current rate / 10 Gbps target ratio: {currentTo10GbpsRatio:F4}");
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
        PrintConnectionTopologyDiagnostics(diagnostics);

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
        using PreparedBenchmarkWorkload workload = PrepareBenchmarkWorkload(config);

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
        PrintConnectionTopologyDiagnostics(connectionDiagnostics);

        Console.WriteLine();
        Console.WriteLine("=== Phase 8: Final results ===");
        PrintFinalReport(result, config);
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

    private static PreparedBenchmarkWorkload PrepareBenchmarkWorkload(TransitBenchmarkConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        Stopwatch idGenerationStopwatch = Stopwatch.StartNew();
        string[] messageIds = new string[PreGeneratedMessageIdPoolSize];
        HashSet<string> uniqueness = new(PreGeneratedMessageIdPoolSize, StringComparer.Ordinal);

        for (int i = 0; i < messageIds.Length; i++)
        {
            string messageId = TransitBenchmarkCore.BuildMessageId(config.BenchmarkInstanceId, workerId: 0, sequence: i + 1, phase: "pre");
            messageIds[i] = messageId;
            uniqueness.Add(messageId);
        }

        idGenerationStopwatch.Stop();

        Stopwatch payloadPreparationStopwatch = Stopwatch.StartNew();
        byte[] reusablePayloadTemplate = CreateReusablePayloadTemplate(config.ArticleTargetBytes);
        payloadPreparationStopwatch.Stop();

        int duplicateCount = messageIds.Length - uniqueness.Count;

        WorkloadPreparationSummary summary = new(
            PreGenerationDurationMilliseconds: idGenerationStopwatch.Elapsed.TotalMilliseconds,
            PayloadPreparationDurationMilliseconds: payloadPreparationStopwatch.Elapsed.TotalMilliseconds,
            MessageIdPoolSize: messageIds.Length,
            UniqueMessageIdCount: uniqueness.Count,
            DuplicateMessageIdCount: duplicateCount,
            ReusablePayloadBytes: reusablePayloadTemplate.Length);

        Console.WriteLine($"Pre-generation duration ms: {summary.PreGenerationDurationMilliseconds:F2}");
        Console.WriteLine($"Pre-generated Message-IDs: {summary.MessageIdPoolSize:N0}");
        Console.WriteLine($"Unique Message-IDs: {summary.UniqueMessageIdCount:N0}");
        Console.WriteLine($"Duplicate Message-IDs: {summary.DuplicateMessageIdCount:N0}");
        Console.WriteLine($"Payload preparation duration ms: {summary.PayloadPreparationDurationMilliseconds:F2}");
        Console.WriteLine($"Reusable article payload bytes: {summary.ReusablePayloadBytes:N0}");

        return new PreparedBenchmarkWorkload(messageIds, reusablePayloadTemplate, summary);
    }

    private static byte[] CreateReusablePayloadTemplate(int targetBytes)
    {
        string headers = "Message-ID: <benchmark-static@usenet.ninja>\r\n" +
                         "Date: Thu, 01 Jan 1970 00:00:00 GMT\r\n" +
                         "From: benchmark@usenet.ninja\r\n" +
                         "Newsgroups: alt.test\r\n" +
                         "Subject: BackFiller TransitPublisher benchmark workload\r\n" +
                         "Path: benchmark.usenet.ninja\r\n" +
                         "\r\n";

        byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
        int minimumTrailerBytes = 2;
        int bodyBytes = Math.Max(1, targetBytes - headerBytes.Length - minimumTrailerBytes);

        byte[] reusable = new byte[headerBytes.Length + bodyBytes + minimumTrailerBytes];
        int offset = 0;

        Buffer.BlockCopy(headerBytes, 0, reusable, offset, headerBytes.Length);
        offset += headerBytes.Length;

        for (int i = 0; i < bodyBytes; i++)
        {
            reusable[offset++] = (byte)('a' + (i % 26));
        }

        reusable[offset++] = (byte)'\r';
        reusable[offset++] = (byte)'\n';

        return reusable;
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
            producerTasks[producerWorkerId] = Task.Run(() => ProducerLoopAsync(
                queue,
                metrics,
                workload,
                producerQueueTargetArticles,
                capturedWorkerId,
                producerStopCts.Token), CancellationToken.None);
        }

        Task telemetryTask = Task.Run(() => TelemetryLoopAsync(
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
            dispatchers[i] = Task.Run(() => DispatchLoopAsync(queue, publisher, metrics, workload, cancellationToken, enableForensicDiagnostics), CancellationToken.None);
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

        return BenchmarkResult.Create(
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

    private static async Task ProducerLoopAsync(
        BoundedArticleQueue queue,
        MeasurementMetrics metrics,
        PreparedBenchmarkWorkload workload,
        int targetQueuedArticles,
        int workerId,
        CancellationToken cancellationToken)
    {
        _ = targetQueuedArticles;
        _ = workerId;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!workload.TryTakeNextMessageId(out string? messageId))
            {
                return;
            }

            long loopStart = Stopwatch.GetTimestamp();
            long generationEnd = Stopwatch.GetTimestamp();
            Console.WriteLine("[SUBMIT-PATH] stage=generator-created messageId={0} tick={1}", messageId, generationEnd);

            long queueWaitStart = Stopwatch.GetTimestamp();
            bool admittedToQueue = await queue.TryWriteAsync(new QueuedArticle(messageId, workload.PayloadLength), cancellationToken).ConfigureAwait(false);
            long queueWaitEnd = Stopwatch.GetTimestamp();

            if (!admittedToQueue)
            {
                return;
            }

            long loopEnd = Stopwatch.GetTimestamp();
            long loopTicks = Math.Max(0, loopEnd - loopStart);
            long generationTicks = Math.Max(0, generationEnd - loopStart);
            long queueWaitTicks = Math.Max(0, queueWaitEnd - queueWaitStart);
            long activeTicks = Math.Max(0, loopTicks - queueWaitTicks);
            long otherActiveTicks = Math.Max(0, activeTicks - generationTicks);

            TransitBenchmarkCore.ProducerTiming producerTiming = TransitBenchmarkCore.ProducerTiming.FromRaw(
                loopTicks: loopTicks,
                generationTicks: generationTicks,
                blockedTicks: queueWaitTicks,
                otherActiveTicks: otherActiveTicks);

            metrics.OnGenerated(workload.PayloadLength, producerTiming, queueWaitTicks);
        }
    }

    private static async Task DispatchLoopAsync(
        BoundedArticleQueue queue,
        TransitPublisher publisher,
        MeasurementMetrics metrics,
        PreparedBenchmarkWorkload workload,
        CancellationToken cancellationToken,
        bool enableForensicDiagnostics)
    {
        while (await queue.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (queue.TryRead(out QueuedArticle queuedArticle))
            {
                long dequeuedTick = Stopwatch.GetTimestamp();
                metrics.OnDequeued(dequeuedTick);
                Interlocked.Increment(ref metrics.InFlightSubmissions);

                try
                {
                    int pendingAtSubmit = 0;
                    if (enableForensicDiagnostics)
                    {
                        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot beforeSubmit = publisher.CaptureConnectionDiagnosticsSnapshot();
                        pendingAtSubmit = beforeSubmit.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
                    }

                    metrics.OnAdmitted(queuedArticle.PayloadLength, dequeuedTick);
                    long publishStartTick = Stopwatch.GetTimestamp();
                    TransitPublishResult result = await publisher.PublishAsync(queuedArticle.MessageId, workload.ReusableArticlePayload, cancellationToken).ConfigureAwait(false);
                    long publishEndTick = Stopwatch.GetTimestamp();

                    int pendingAtComplete = 0;
                    if (enableForensicDiagnostics)
                    {
                        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot afterSubmit = publisher.CaptureConnectionDiagnosticsSnapshot();
                        pendingAtComplete = afterSubmit.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
                    }

                    metrics.OnPublishResult(result, queuedArticle.PayloadLength, dequeuedTick, publishStartTick, publishEndTick, pendingAtSubmit, pendingAtComplete);
                }
                finally
                {
                    Interlocked.Decrement(ref metrics.InFlightSubmissions);
                    queue.ReleaseReservation(queuedArticle.PayloadLength);
                }
            }
        }

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot dispatcherExitDiagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
        int dispatcherExitPendingMessageIds = dispatcherExitDiagnostics.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
        long dispatcherExitQueuedWriteIntents = dispatcherExitDiagnostics.Connections.Sum(static entry => entry.Snapshot.CurrentWriteIntentQueueDepth);
        Console.WriteLine("[SHUTDOWN-DIAG] DispatchLoop exit: queuedSubmissions={QueuedSubmissions} pendingMessageIds={PendingMessageIds} queuedWriteIntents={QueuedWriteIntents}",
            dispatcherExitDiagnostics.QueuedSubmissionCount,
            dispatcherExitPendingMessageIds,
            dispatcherExitQueuedWriteIntents);
    }

    private static async Task TelemetryLoopAsync(
        BoundedArticleQueue queue,
        MeasurementMetrics metrics,
        RuntimeMetrics runtime,
        Process process,
        long allocatedStartBytes,
        TransitPublisher publisher,
        int queueTargetArticles,
        bool enableForensicDiagnostics,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("elapsed_s gen_art_s gen_MB_s gen_Gbps adm_art_s adm_MB_s acc_art_s acc_MB_s acc_Gbps rej_art_s amb_art_s q_depth q_bytes inflight dispatch_pending actual_pending peak_conn_inflight conn_ready active_slots host_cpu_pct transit_cpu_pct cpu_pct ws_mb heap_mb alloc_mb gen0 gen1 gen2 prod_active_pct prod_blocked_pct prod_active_ms prod_blocked_ms queue_wait_ms");
        Console.WriteLine("NOTE: generated/admitted/accepted are distinct throughput classes; accepted is based on definitive TransitServer success responses.");
        Console.WriteLine($"Queue target depth (articles): {queueTargetArticles}");

        Stopwatch elapsed = Stopwatch.StartNew();
        MeasurementSnapshot previous = metrics.Snapshot();
        TimeSpan previousElapsed = TimeSpan.Zero;
        TimeSpan previousCpu = process.TotalProcessorTime;

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);

            TimeSpan now = elapsed.Elapsed;
            double seconds = (now - previousElapsed).TotalSeconds;
            if (seconds <= 0)
            {
                continue;
            }

            MeasurementSnapshot current = metrics.Snapshot();

            long generatedCountDelta = current.GeneratedCount - previous.GeneratedCount;
            long generatedBytesDelta = current.GeneratedBytes - previous.GeneratedBytes;
            long admittedCountDelta = current.AdmittedCount - previous.AdmittedCount;
            long admittedBytesDelta = current.AdmittedBytes - previous.AdmittedBytes;
            long acceptedCountDelta = current.AcceptedCount - previous.AcceptedCount;
            long acceptedBytesDelta = current.AcceptedBytes - previous.AcceptedBytes;
            long rejectedCountDelta = current.RejectedCount - previous.RejectedCount;
            long ambiguousCountDelta = current.AmbiguousCount - previous.AmbiguousCount;

            int queueDepth = queue.CurrentQueuedCount;
            long queueBytes = queue.CurrentQueuedBytes;
            int inFlight = Volatile.Read(ref metrics.InFlightSubmissions);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
            int actualPending = diagnostics.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
            int peakConnectionInFlight = diagnostics.Connections.Length == 0 ? 0 : diagnostics.Connections.Max(static x => x.Snapshot.MaxConcurrentSubmissions);
            int readyConnections = diagnostics.Connections.Count(static x => x.Snapshot.ReadyTransitionCount > 0);
            int activeSlots = diagnostics.Slots.Count(static x => x.TotalSubmissionsRouted > 0);

            metrics.ObservePeaks(queueDepth, queueBytes, inFlight);
            metrics.ObserveActualPending(actualPending);

            TimeSpan cpuNow = process.TotalProcessorTime;
            double cpuPercent = (cpuNow - previousCpu).TotalSeconds / (Environment.ProcessorCount * seconds) * 100d;
            previousCpu = cpuNow;

            double hostCpuPercent = ReadHostCpuPercent();
            double transitServerCpuPercent = ReadTransitServerCpuPercent();

            long workingSet = process.WorkingSet64;
            long gcHeapBytes = GC.GetTotalMemory(forceFullCollection: false);
            long allocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocatedStartBytes;

            runtime.Sample(cpuPercent, hostCpuPercent, transitServerCpuPercent, workingSet, gcHeapBytes, allocatedBytes);

            if (enableForensicDiagnostics)
            {
                metrics.RecordConnectionSample(diagnostics, now);
                metrics.RecordDispatcherSample(now, inFlight, current.AdmittedCount - current.CompletedCount, actualPending, queueDepth, queueBytes);
            }

            long activeTicksDelta = current.ActiveTicks - previous.ActiveTicks;
            long blockedTicksDelta = current.BlockedTicks - previous.BlockedTicks;
            long producerObservedTicksDelta = activeTicksDelta + blockedTicksDelta;

            double blockedPercent = producerObservedTicksDelta <= 0
                ? 0
                : blockedTicksDelta * 100d / producerObservedTicksDelta;

            double activePercent = producerObservedTicksDelta <= 0
                ? 0
                : activeTicksDelta * 100d / producerObservedTicksDelta;

            double activeMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(activeTicksDelta);
            double blockedMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(blockedTicksDelta);
            long queueWaitTicksDelta = current.ProducerQueueWaitTicks - previous.ProducerQueueWaitTicks;
            double queueWaitMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(queueWaitTicksDelta);

            Console.WriteLine($"{now.TotalSeconds:F1} {generatedCountDelta / seconds:F2} {generatedBytesDelta / 1024d / 1024d / seconds:F2} {generatedBytesDelta * 8d / 1_000_000_000d / seconds:F4} {admittedCountDelta / seconds:F2} {admittedBytesDelta / 1024d / 1024d / seconds:F2} {acceptedCountDelta / seconds:F2} {acceptedBytesDelta / 1024d / 1024d / seconds:F2} {acceptedBytesDelta * 8d / 1_000_000_000d / seconds:F4} {rejectedCountDelta / seconds:F2} {ambiguousCountDelta / seconds:F2} {queueDepth} {queueBytes} {inFlight} {current.AdmittedCount - current.CompletedCount} {actualPending} {peakConnectionInFlight} {readyConnections} {activeSlots} {hostCpuPercent:F2} {transitServerCpuPercent:F2} {cpuPercent:F2} {workingSet / 1024d / 1024d:F2} {gcHeapBytes / 1024d / 1024d:F2} {allocatedBytes / 1024d / 1024d:F2} {GC.CollectionCount(0)} {GC.CollectionCount(1)} {GC.CollectionCount(2)} {activePercent:F2} {blockedPercent:F2} {activeMilliseconds:F2} {blockedMilliseconds:F2} {queueWaitMilliseconds:F2}");

            previous = current;
            previousElapsed = now;
        }
    }

    private static void PrintConnectionTopologyDiagnostics(TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics)
    {
        Console.WriteLine($"Configured pool size: {diagnostics.ConfiguredConnectionPoolSize}");
        Console.WriteLine($"Configured per-connection pipeline depth: {diagnostics.ConfiguredPerConnectionPipelineDepth}");
        Console.WriteLine($"Global reconnect count: {diagnostics.TotalReconnects}");

        int uniqueConnectionCount = diagnostics.Connections.Select(static x => x.Snapshot.ConnectionId).Distinct(StringComparer.Ordinal).Count();
        long totalSocketOpens = diagnostics.Connections.Sum(static x => x.Snapshot.SocketOpenCount);
        int readyConnectionCount = diagnostics.Connections.Count(static x => x.Snapshot.ReadyTransitionCount > 0);
        int activeConnectionCount = diagnostics.Slots.Count(static x => x.TotalSubmissionsRouted > 0);

        Console.WriteLine($"Unique TransitConnection instances observed: {uniqueConnectionCount}");
        Console.WriteLine($"Total physical socket opens observed: {totalSocketOpens}");
        Console.WriteLine($"Connections reaching READY at least once: {readyConnectionCount}");
        Console.WriteLine($"Pool slots that carried submissions: {activeConnectionCount}/{diagnostics.ConfiguredConnectionPoolSize}");

        Console.WriteLine();
        Console.WriteLine("Per-slot participation:");
        foreach (TransitPublisher.ConnectionSlotSnapshot slot in diagnostics.Slots.OrderBy(static x => x.SlotIndex))
        {
            Console.WriteLine($"  Slot {slot.SlotIndex}: HasCurrent={slot.HasCurrentConnection}, CurrentConnectionId={slot.CurrentConnectionId ?? "(none)"}, RoutedSubmissions={slot.TotalSubmissionsRouted}, Reconnects={slot.Reconnects}, CreatedConnections={slot.CreatedConnections}, MaxObservedInFlightDepth={slot.MaxObservedInFlightDepth}, WaitedForChannelReadability={slot.WaitedForChannelReadabilityCount}, WaitedForCompletionWhileFull={slot.WaitedForCompletionWhilePipelineFullCount}, FirstReachedConfiguredDepthTick={slot.FirstReachedConfiguredDepthTick}");
        }

        Console.WriteLine();
        Console.WriteLine("Per-connection diagnostics:");
        foreach (TransitPublisher.ConnectionDiagnosticsEntry entry in diagnostics.Connections
                     .OrderBy(static x => x.SlotIndex)
                     .ThenBy(static x => x.Snapshot.ConnectionId, StringComparer.Ordinal))
        {
            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = entry.Snapshot;
            Console.WriteLine($"  Slot {entry.SlotIndex}, ConnectionId={snapshot.ConnectionId}, Host={snapshot.Host}:{snapshot.Port}, State={snapshot.CurrentState}, TLS={snapshot.IsTlsActive}, Compression={snapshot.IsCompressionActive}");
            Console.WriteLine($"    Endpoints: Local={snapshot.LocalEndpoint ?? "(unavailable)"}, Remote={snapshot.RemoteEndpoint ?? "(unavailable)"}");
            Console.WriteLine($"    SocketOpens={snapshot.SocketOpenCount}, ReadyTransitions={snapshot.ReadyTransitionCount}, MaxInFlight={snapshot.MaxConcurrentSubmissions}, CurrentInFlight={snapshot.CurrentConcurrentSubmissions}");
            Console.WriteLine($"    Submissions: Started={snapshot.SubmissionsStarted}, Accepted={snapshot.SubmissionsAccepted}, Rejected={snapshot.SubmissionsRejected}, Ambiguous={snapshot.SubmissionsAmbiguous}, Unavailable={snapshot.SubmissionsUnavailable}, Failed={snapshot.SubmissionsFailed}");

            TransitConnection.PipeliningDiagnosticSummary diagnosticSummary = snapshot.DiagnosticsSummary;
            Console.WriteLine($"    Pipelining diagnostics: MaxPendingDepth={diagnosticSummary.MaxPendingDepth}, MaxWriteQueueDepth={diagnosticSummary.MaxWriteQueueDepth}, MaxWriterBatchSize={diagnosticSummary.MaxWriterBatchSize}, AvgBatchSize={diagnosticSummary.AverageWriterBatchSize:F2}, P50={diagnosticSummary.P50WriterBatchSize:F0}, P95={diagnosticSummary.P95WriterBatchSize:F0}, P99={diagnosticSummary.P99WriterBatchSize:F0}, NumberOfBatches={diagnosticSummary.NumberOfBatches}");
            Console.WriteLine($"    Batch histogram: {diagnosticSummary.BatchSizeHistogram}");
            Console.WriteLine($"    Coalescing wait (us): Avg={diagnosticSummary.AverageCoalescingWaitMicroseconds:F2}, P50={diagnosticSummary.P50CoalescingWaitMicroseconds:F2}, P95={diagnosticSummary.P95CoalescingWaitMicroseconds:F2}, P99={diagnosticSummary.P99CoalescingWaitMicroseconds:F2}");
            Console.WriteLine($"    MaxLaterTakethisBeforeResponse={diagnosticSummary.MaxLogicalOutstandingAheadAtResponse}, CapturedOperationCount={diagnosticSummary.CapturedOperationCount}, SampledOperationCount={diagnosticSummary.SampledOperationCount}");

            int sampleCount = Math.Min(snapshot.DiagnosticSampleRecords.Length, 1000);
            if (sampleCount > 0)
            {
                Console.WriteLine($"    Diagnostic sample records shown: {sampleCount}");
                for (int i = 0; i < sampleCount; i++)
                {
                    TransitConnection.DiagnosticOperationRecord sample = snapshot.DiagnosticSampleRecords[i];
                    Console.WriteLine($"      MessageId={sample.MessageId}; T0={sample.T0SubmitEnterTick}; T0SubmitTakethis={sample.T0SubmitTakethisEnterTick}; T1={sample.T1PendingRegisteredTick}; T2EnqueueStart={sample.T2WriteIntentEnqueueStartTick}; T2={sample.T2WriteIntentEnqueuedTick}; T2BeforeAwait={sample.T2BeforeCompletionAwaitTick}; T3={sample.T3WriterDequeuedTick}; T4={sample.T4AssignedToBatchTick}; T5={sample.T5FrameStageBeginTick}; T6={sample.T6FrameStageEndTick}; T7={sample.T7BatchFlushBeginTick}; T8={sample.T8BatchFlushEndTick}; T9={sample.T9ResponseCorrelatedTick}; T10={sample.T10SubmitCompletionTick}; PendingT1={sample.PendingDepthAtT1}; PendingT2={sample.PendingDepthAtT2}; PendingT3={sample.PendingDepthAtT3}; PendingT4={sample.PendingDepthAtT4}; PendingT9={sample.PendingDepthAtT9}; QueueT2={sample.QueueDepthAtT2}; QueueT3={sample.QueueDepthAtT3}; QueueBatchStart={sample.QueueDepthAtBatchStart}; BatchDequeued={sample.BatchDequeuedCount}; QueueT9={sample.QueueDepthAtT9}; BatchId={sample.BatchId}; BatchPosition={sample.BatchPosition}; BatchSize={sample.BatchSize}; SendSequence={sample.SendSequence}; LaterTakethisBefore239={sample.LogicalOutstandingAheadAtResponse}");
                }
            }
        }

        int submissionTraceCount = diagnostics.SubmissionTraceRecords.Length;
        Console.WriteLine();
        Console.WriteLine($"Submission pump trace records: {submissionTraceCount}");
        if (submissionTraceCount > 0)
        {
            int submissionTraceSampleCount = Math.Min(submissionTraceCount, 1000);
            Console.WriteLine($"Submission pump trace sample shown: {submissionTraceSampleCount}");
            for (int i = 0; i < submissionTraceSampleCount; i++)
            {
                TransitPublisher.SubmissionTraceRecord trace = diagnostics.SubmissionTraceRecords[i];
                Console.WriteLine($"  MessageId={trace.MessageId}; PumpReadTick={trace.RemovedFromSubmissionChannelTick}; PublishInvokeTick={trace.PublishToConnectionInvokedTick}; InFlightBeforeAdd={trace.InFlightCountBeforeAdd}; InFlightAfterAdd={trace.InFlightCountAfterAdd}; WriteIntentQueueDepthAtRead={trace.WriteIntentQueueDepthAtPumpRead}");
            }
        }

        int publishTraceCount = diagnostics.PublishToConnectionTraceRecords.Length;
        Console.WriteLine();
        Console.WriteLine($"PublishToConnectionWithReconnect trace records: {publishTraceCount}");
        if (publishTraceCount > 0)
        {
            int publishTraceSampleCount = Math.Min(publishTraceCount, 1000);
            Console.WriteLine($"PublishToConnectionWithReconnect trace sample shown: {publishTraceSampleCount}");
            for (int i = 0; i < publishTraceSampleCount; i++)
            {
                TransitPublisher.PublishToConnectionTraceRecord trace = diagnostics.PublishToConnectionTraceRecords[i];
                Console.WriteLine($"  MessageId={trace.MessageId}; Slot={trace.SlotIndex}; MethodEntryTick={trace.MethodEntryTick}; ConnectionId={trace.SelectedConnectionId ?? "(null)"}; BeforeSubmitTakethisTick={trace.BeforeSubmitTakethisTick}; AfterSubmitTakethisTick={trace.AfterSubmitTakethisTick}");
            }
        }
    }

    private static void PrintFinalReport(BenchmarkResult result, TransitBenchmarkConfig config)
    {
        Console.WriteLine($"Benchmark Build Version: {result.BenchmarkBuildVersion}");
        Console.WriteLine($"Production path exercised: Benchmark -> REAL TransitPublisher -> REAL TransitConnection -> TLS -> MODE STREAM -> TransitServer");
        Console.WriteLine("Generated throughput = bytes generated by the article generator.");
        Console.WriteLine("Admitted throughput = bytes accepted by TransitPublisher PublishAsync contract.");
        Console.WriteLine("Accepted throughput = bytes mapped to definitive TransitServer success responses (e.g., 239).");
        Console.WriteLine("Generated/admitted throughput must not be interpreted as socket-wire throughput.");
        Console.WriteLine("Exact socket-wire throughput is not directly observable from TransitPublisher API surface.");

        Console.WriteLine();
        Console.WriteLine("Preparation summary:");
        Console.WriteLine($"Pre-generation duration ms: {result.WorkloadPreparation.PreGenerationDurationMilliseconds:F2}");
        Console.WriteLine($"Payload preparation duration ms: {result.WorkloadPreparation.PayloadPreparationDurationMilliseconds:F2}");
        Console.WriteLine($"Pre-generated Message-ID pool size: {result.WorkloadPreparation.MessageIdPoolSize:N0}");
        Console.WriteLine($"Unique Message-ID count: {result.WorkloadPreparation.UniqueMessageIdCount:N0}");
        Console.WriteLine($"Duplicate Message-ID count: {result.WorkloadPreparation.DuplicateMessageIdCount:N0}");
        Console.WriteLine($"Reusable article payload bytes: {result.WorkloadPreparation.ReusablePayloadBytes:N0}");

        Console.WriteLine();
        Console.WriteLine($"Warmup duration: {config.WarmupDuration.TotalSeconds:F0}s");
        Console.WriteLine($"Measurement duration: {config.MeasurementDuration.TotalSeconds:F0}s");
        Console.WriteLine($"Drain duration: {result.DrainDuration.TotalSeconds:F3}s");
        Console.WriteLine($"Outstanding at measurement end: {result.OutstandingAtMeasurementEnd}");
        Console.WriteLine($"Drained after measurement: {result.DrainedAfterMeasurement}");

        Console.WriteLine();
        Console.WriteLine($"Measurement start UTC: {result.MeasurementStartUtc:O}");
        Console.WriteLine($"Measurement end UTC:   {result.MeasurementEndUtc:O}");

        Console.WriteLine();
        Console.WriteLine($"Generated articles: {result.GeneratedArticles}");
        Console.WriteLine($"Generated bytes: {result.GeneratedBytes}");
        Console.WriteLine($"Generated Gbps: {result.GeneratedGbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"Admitted articles: {result.AdmittedArticles}");
        Console.WriteLine($"Admitted bytes: {result.AdmittedBytes}");
        Console.WriteLine($"Admitted Gbps: {result.AdmittedGbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"Accepted articles: {result.AcceptedArticles}");
        Console.WriteLine($"Accepted bytes: {result.AcceptedBytes}");
        Console.WriteLine($"Accepted Gbps: {result.AcceptedGbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"Rejected articles: {result.RejectedArticles}");
        Console.WriteLine($"Ambiguous articles: {result.AmbiguousArticles}");

        Console.WriteLine();
        Console.WriteLine($"Queue target depth (articles): {config.ProducerQueueTargetArticles}");
        Console.WriteLine($"Queue minimum depth: {result.MinQueueDepth}");
        Console.WriteLine($"Queue average depth: {result.AverageQueueDepth:F2}");
        Console.WriteLine($"Queue peak depth: {result.PeakQueueDepth}");
        Console.WriteLine($"Queue average bytes: {result.AverageQueuedBytes:F0}");
        Console.WriteLine($"Queue peak bytes: {result.PeakQueuedBytes}");
        Console.WriteLine($"Queue configured article cap: {config.MaxQueuedArticles}");
        Console.WriteLine($"Queue configured byte cap: {config.MaxResidentBytes}");
        Console.WriteLine($"Peak dispatcher in-flight (PublishAsync waits): {result.PeakInFlight}");
        Console.WriteLine($"Peak actual pending submissions (sum connection CurrentInFlight): {result.PeakActualPending}");
        Console.WriteLine($"Producer queue starvation: {(result.PeakQueueDepth <= 1 ? "Yes" : "No")}");
        Console.WriteLine($"Producer active %: {result.ProducerActivePercent:F2}");
        Console.WriteLine($"Producer blocked/backpressured %: {result.ProducerBlockedPercent:F2}");
        Console.WriteLine($"Producer active ms: {result.ProducerActiveMilliseconds:F2}");
        Console.WriteLine($"Producer blocked ms: {result.ProducerBlockedMilliseconds:F2}");
        Console.WriteLine($"Producer queue-capacity wait ms: {result.ProducerQueueWaitMilliseconds:F2}");

        Console.WriteLine();
        Console.WriteLine($"CPU % (avg sampled): {result.AverageCpuPercent:F2}");
        Console.WriteLine($"Host CPU % (avg/peak sampled): {result.AverageHostCpuPercent:F2}/{result.PeakHostCpuPercent:F2}");
        Console.WriteLine($"TransitServer CPU % (avg/peak sampled): {result.AverageTransitServerCpuPercent:F2}/{result.PeakTransitServerCpuPercent:F2}");
        Console.WriteLine($"Working Set MB: {result.WorkingSetMb:F2}");
        Console.WriteLine($"GC Heap MB: {result.GcHeapMb:F2}");
        Console.WriteLine($"Allocated MB: {result.AllocatedMb:F2}");
        Console.WriteLine($"Gen0: {result.Gen0Collections}, Gen1: {result.Gen1Collections}, Gen2: {result.Gen2Collections}");

        Console.WriteLine();
        Console.WriteLine("Forensic timing and time-series:");
        Console.WriteLine($"Forensic samples captured: {result.ForensicSampleCount}");
        Console.WriteLine($"Dispatch wait (T0->T1) us [samples={result.DispatchQueueWaitSampleCount}]: avg={result.AverageDispatchQueueWaitUs:F3}, p50={result.P50DispatchQueueWaitUs:F3}, p95={result.P95DispatchQueueWaitUs:F3}, p99={result.P99DispatchQueueWaitUs:F3}, max={result.MaxDispatchQueueWaitUs:F3}");
        Console.WriteLine($"Socket write (T2->T3) us [samples={result.SocketWriteSampleCount}]: avg={result.AverageSocketWriteUs:F3}, p50={result.P50SocketWriteUs:F3}, p95={result.P95SocketWriteUs:F3}, p99={result.P99SocketWriteUs:F3}, max={result.MaxSocketWriteUs:F3}");
        Console.WriteLine($"Response wait (T3->T4) us [samples={result.ResponseWaitSampleCount}]: avg={result.AverageResponseWaitUs:F3}, p50={result.P50ResponseWaitUs:F3}, p95={result.P95ResponseWaitUs:F3}, p99={result.P99ResponseWaitUs:F3}, max={result.MaxResponseWaitUs:F3}");
        Console.WriteLine($"Parse/correlation (T4->T6) us [samples={result.ParseCorrelationSampleCount}]: avg={result.AverageParseCorrelationUs:F3}, p50={result.P50ParseCorrelationUs:F3}, p95={result.P95ParseCorrelationUs:F3}, p99={result.P99ParseCorrelationUs:F3}, max={result.MaxParseCorrelationUs:F3}");
        Console.WriteLine($"Total PublishAsync (T0->T7) us [samples={result.TotalPublishLatencySampleCount}]: avg={result.AverageTotalPublishLatencyUs:F3}, p50={result.P50TotalPublishLatencyUs:F3}, p95={result.P95TotalPublishLatencyUs:F3}, p99={result.P99TotalPublishLatencyUs:F3}, max={result.MaxTotalPublishLatencyUs:F3}");

        if (result.AverageTotalPublishLatencyUs > 0)
        {
            double dispatchPct = result.AverageDispatchQueueWaitUs * 100d / result.AverageTotalPublishLatencyUs;
            double writePct = result.AverageSocketWriteUs * 100d / result.AverageTotalPublishLatencyUs;
            double responsePct = result.AverageResponseWaitUs * 100d / result.AverageTotalPublishLatencyUs;
            double parseCorrelationPct = result.AverageParseCorrelationUs * 100d / result.AverageTotalPublishLatencyUs;
            double accountedPct = dispatchPct + writePct + responsePct + parseCorrelationPct;
            double remainderPct = Math.Max(0, 100d - accountedPct);
            Console.WriteLine($"Average T0->T7 contribution (%): dispatch={dispatchPct:F2}, write={writePct:F2}, responseWait={responsePct:F2}, parse/correlation={parseCorrelationPct:F2}, remainder={remainderPct:F2}");
        }

        Console.WriteLine($"Legacy publish latency us: avg={result.AveragePublishLatencyUs:F3}, min={result.MinPublishLatencyUs:F3}, p50={result.P50PublishLatencyUs:F3}, p95={result.P95PublishLatencyUs:F3}, p99={result.P99PublishLatencyUs:F3}, max={result.MaxPublishLatencyUs:F3}");
        Console.WriteLine($"Lifecycle latency avg (us): {result.AverageLifecycleLatencyUs:F3}");
        Console.WriteLine("Latency buckets by pending depth:");
        Console.WriteLine(result.PendingDepthLatencyBuckets);
        Console.WriteLine($"Dispatcher series summary: {result.DispatcherTimeSeriesSummary}");
        Console.WriteLine("Connection series summary:");
        Console.WriteLine(result.ConnectionTimeSeriesSummary);
        Console.WriteLine($"Observability boundaries: {result.ObservabilityNotes}");
        Console.WriteLine($"Queue capacity estimate by byte budget (maxResidentBytes/articleBytes): {result.EffectiveQueueArticleCapacityFromBytes}");

        Console.WriteLine();
        Console.WriteLine("TransitServer reconciliation:");
        Console.WriteLine($"Benchmark measurement start UTC: {result.MeasurementStartUtc:O}");
        Console.WriteLine($"Benchmark measurement end UTC:   {result.MeasurementEndUtc:O}");
        Console.WriteLine($"Benchmark accepted articles: {result.AcceptedArticles}");
        Console.WriteLine($"Benchmark rejected articles: {result.RejectedArticles}");
        Console.WriteLine($"Benchmark ambiguous articles: {result.AmbiguousArticles}");
        Console.WriteLine($"Benchmark elapsed seconds: {config.MeasurementDuration.TotalSeconds:F0}");
        Console.WriteLine($"Benchmark accepted articles/sec: {result.AcceptedArticles / config.MeasurementDuration.TotalSeconds:F4}");
        Console.WriteLine("TransitServer spool/throughput counters may use different reporting windows and aggregation cadence (for example rolling 60-second windows). Compare by timestamps, not by assuming identical window boundaries.");
    }

    private static ILogger<TransitPublisher> CreateTransitPublisherLogger(ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);

        ILogger baseLogger = loggerFactory.CreateLogger<TransitPublisher>();
        return new TransitPublisherBenchmarkLogger(baseLogger);
    }

    private static void WriteStructuredResultArtifacts(BenchmarkResult result, TransitBenchmarkConfig config)
    {
        try
        {
            BenchmarkResultArtifact artifact = BenchmarkResultArtifact.From(result, config, Environment.ProcessorCount);
            string json = JsonSerializer.Serialize(artifact, new JsonSerializerOptions { WriteIndented = true });
            string csv = artifact.ToCsv();

            string stamp = DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
            string baseDir = AppContext.BaseDirectory;
            string jsonPath = Path.Combine(baseDir, $"transit-benchmark-result-{stamp}.json");
            string csvPath = Path.Combine(baseDir, $"transit-benchmark-result-{stamp}.csv");

            File.WriteAllText(jsonPath, json);
            File.WriteAllText(csvPath, csv);

            Console.WriteLine();
            Console.WriteLine("Structured benchmark artifacts written:");
            Console.WriteLine($"JSON: {jsonPath}");
            Console.WriteLine($"CSV:  {csvPath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"WARNING: Failed to write structured benchmark artifacts: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private static RuntimeExecutionIdentity CaptureRuntimeExecutionIdentity()
    {
        Assembly assembly = Assembly.GetEntryAssembly() ?? typeof(TransitServerStressRunner).Assembly;
        string runtimeAssemblyPath = Path.GetFullPath(assembly.Location);
        string runtimeAssemblyVersion = assembly.GetName().Version?.ToString() ?? "(unknown)";
        string? assemblyFileVersion = assembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version;

        string processPath = Environment.ProcessPath ?? Process.GetCurrentProcess().MainModule?.FileName ?? "(unknown)";
        string workingDirectory = Environment.CurrentDirectory;

        string? sourceRevision = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        DateTimeOffset? buildTimestampUtc = null;
        if (File.Exists(runtimeAssemblyPath))
        {
            DateTime lastWriteUtc = File.GetLastWriteTimeUtc(runtimeAssemblyPath);
            if (lastWriteUtc != DateTime.MinValue)
            {
                buildTimestampUtc = new DateTimeOffset(lastWriteUtc, TimeSpan.Zero);
            }
        }

        return new RuntimeExecutionIdentity(
            RuntimeAssemblyPath: runtimeAssemblyPath,
            RuntimeAssemblyVersion: runtimeAssemblyVersion,
            AssemblyFileVersion: assemblyFileVersion,
            ProcessPath: processPath,
            WorkingDirectory: workingDirectory,
            Configuration: AppBuildConfiguration.Value,
            Platform: AppBuildPlatform.Value,
            TargetFramework: AppTargetFramework.Value,
            RuntimeIdentifier: AppRuntimeIdentifier.Value,
            Architecture: RuntimeInformation.ProcessArchitecture.ToString(),
            SourceRevision: sourceRevision,
            BuildTimestampUtc: buildTimestampUtc);
    }

    private static void EnsureRuntimeIdentityMatches(RuntimeIdentityExpectation expected)
    {
        if (string.IsNullOrWhiteSpace(expected.ExpectedAssemblyPath) ||
            string.IsNullOrWhiteSpace(expected.ExpectedAssemblyVersion) ||
            string.IsNullOrWhiteSpace(expected.ExpectedFileVersion) ||
            string.IsNullOrWhiteSpace(expected.ExpectedTargetFramework) ||
            string.IsNullOrWhiteSpace(expected.ExpectedArchitecture))
        {
            throw new InvalidOperationException(
                "Runtime identity guard requires expected hard-identity options: --expected-assembly-path, --expected-assembly-version, --expected-file-version, --expected-target-framework, and --expected-architecture.");
        }

        List<string> mismatches = [];
        List<string> provenanceNotes = [];

        string expectedPath = Path.GetFullPath(expected.ExpectedAssemblyPath);
        string actualPath = Path.GetFullPath(RuntimeIdentity.RuntimeAssemblyPath);
        if (!string.Equals(expectedPath, actualPath, StringComparison.OrdinalIgnoreCase))
        {
            mismatches.Add($"AssemblyPath expected='{expectedPath}' actual='{actualPath}'");
        }

        if (!string.Equals(expected.ExpectedAssemblyVersion, RuntimeIdentity.RuntimeAssemblyVersion, StringComparison.Ordinal))
        {
            mismatches.Add($"AssemblyVersion expected='{expected.ExpectedAssemblyVersion}' actual='{RuntimeIdentity.RuntimeAssemblyVersion}'");
        }

        string actualFileVersion = RuntimeIdentity.AssemblyFileVersion ?? "(unknown)";
        if (!string.Equals(expected.ExpectedFileVersion, actualFileVersion, StringComparison.Ordinal))
        {
            mismatches.Add($"FileVersion expected='{expected.ExpectedFileVersion}' actual='{actualFileVersion}'");
        }

        string actualConfiguration = RuntimeIdentity.Configuration ?? "(unknown)";
        if (!string.IsNullOrWhiteSpace(expected.ExpectedConfiguration))
        {
            if (IsUnknownIdentityValue(actualConfiguration))
            {
                provenanceNotes.Add($"Configuration expected='{expected.ExpectedConfiguration}' actual='(unknown)' (treated as build provenance)");
            }
            else if (!string.Equals(expected.ExpectedConfiguration, actualConfiguration, StringComparison.OrdinalIgnoreCase))
            {
                provenanceNotes.Add($"Configuration expected='{expected.ExpectedConfiguration}' actual='{actualConfiguration}' (treated as build provenance)");
            }
        }

        string actualPlatform = RuntimeIdentity.Platform ?? "(unknown)";
        if (!string.IsNullOrWhiteSpace(expected.ExpectedPlatform))
        {
            if (IsUnknownIdentityValue(actualPlatform))
            {
                provenanceNotes.Add($"Platform expected='{expected.ExpectedPlatform}' actual='(unknown)' (treated as build provenance)");
            }
            else if (!string.Equals(expected.ExpectedPlatform, actualPlatform, StringComparison.OrdinalIgnoreCase))
            {
                provenanceNotes.Add($"Platform expected='{expected.ExpectedPlatform}' actual='{actualPlatform}' (treated as build provenance)");
            }
        }

        string actualTargetFramework = RuntimeIdentity.TargetFramework ?? "(unknown)";
        string normalizedExpectedTargetFramework = NormalizeTargetFramework(expected.ExpectedTargetFramework);
        string normalizedActualTargetFramework = NormalizeTargetFramework(actualTargetFramework);
        if (!string.Equals(normalizedExpectedTargetFramework, normalizedActualTargetFramework, StringComparison.OrdinalIgnoreCase))
        {
            mismatches.Add($"TargetFramework expected='{expected.ExpectedTargetFramework}' (normalized='{normalizedExpectedTargetFramework}') actual='{actualTargetFramework}' (normalized='{normalizedActualTargetFramework}')");
        }

        string actualRuntimeIdentifier = RuntimeIdentity.RuntimeIdentifier ?? "(unknown)";
        if (!string.IsNullOrWhiteSpace(expected.ExpectedRuntimeIdentifier))
        {
            if (IsUnknownIdentityValue(actualRuntimeIdentifier))
            {
                provenanceNotes.Add($"RuntimeIdentifier expected='{expected.ExpectedRuntimeIdentifier}' actual='(unknown)' (treated as build provenance)");
            }
            else if (!string.Equals(expected.ExpectedRuntimeIdentifier, actualRuntimeIdentifier, StringComparison.OrdinalIgnoreCase))
            {
                provenanceNotes.Add($"RuntimeIdentifier expected='{expected.ExpectedRuntimeIdentifier}' actual='{actualRuntimeIdentifier}' (treated as build provenance)");
            }
        }

        if (!string.Equals(expected.ExpectedArchitecture, RuntimeIdentity.Architecture, StringComparison.OrdinalIgnoreCase))
        {
            mismatches.Add($"Architecture expected='{expected.ExpectedArchitecture}' actual='{RuntimeIdentity.Architecture}'");
        }

        if (mismatches.Count == 0)
        {
            if (provenanceNotes.Count > 0)
            {
                Console.WriteLine("Runtime identity provenance notes:");
                foreach (string note in provenanceNotes)
                {
                    Console.WriteLine($"  {note}");
                }
            }

            return;
        }

        StringBuilder message = new();
        message.AppendLine("Runtime identity mismatch detected. ABORTING before warmup/measurement.");
        message.AppendLine("EXPECTED:");
        message.AppendLine($"  path={expected.ExpectedAssemblyPath}");
        message.AppendLine($"  assemblyVersion={expected.ExpectedAssemblyVersion}");
        message.AppendLine($"  fileVersion={expected.ExpectedFileVersion}");
        message.AppendLine($"  configuration={expected.ExpectedConfiguration ?? "(unspecified)"}");
        message.AppendLine($"  platform={expected.ExpectedPlatform ?? "(unspecified)"}");
        message.AppendLine($"  targetFramework={expected.ExpectedTargetFramework}");
        message.AppendLine($"  runtimeIdentifier={expected.ExpectedRuntimeIdentifier ?? "(unspecified)"}");
        message.AppendLine($"  architecture={expected.ExpectedArchitecture}");
        message.AppendLine("ACTUAL:");
        message.AppendLine($"  path={RuntimeIdentity.RuntimeAssemblyPath}");
        message.AppendLine($"  assemblyVersion={RuntimeIdentity.RuntimeAssemblyVersion}");
        message.AppendLine($"  fileVersion={actualFileVersion}");
        message.AppendLine($"  configuration={actualConfiguration}");
        message.AppendLine($"  platform={actualPlatform}");
        message.AppendLine($"  targetFramework={actualTargetFramework} (normalized={normalizedActualTargetFramework})");
        message.AppendLine($"  runtimeIdentifier={actualRuntimeIdentifier}");
        message.AppendLine($"  architecture={RuntimeIdentity.Architecture}");
        message.AppendLine($"  processPath={RuntimeIdentity.ProcessPath}");
        message.AppendLine($"  workingDirectory={RuntimeIdentity.WorkingDirectory}");
        message.AppendLine("MISMATCH DETAILS:");
        foreach (string mismatch in mismatches)
        {
            message.AppendLine($"  - {mismatch}");
        }

        if (provenanceNotes.Count > 0)
        {
            message.AppendLine("PROVENANCE NOTES:");
            foreach (string note in provenanceNotes)
            {
                message.AppendLine($"  - {note}");
            }
        }

        throw new InvalidOperationException(message.ToString());
    }

    private static bool IsUnknownIdentityValue(string? value)
    {
        return string.IsNullOrWhiteSpace(value) || string.Equals(value, "(unknown)", StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeTargetFramework(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "(unknown)";
        }

        string candidate = value.Trim();
        if (candidate.StartsWith("net", StringComparison.OrdinalIgnoreCase))
        {
            return candidate.ToLowerInvariant();
        }

        const string prefix = ".NETCoreApp,Version=v";
        if (candidate.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            string version = candidate[prefix.Length..];
            if (version.Length > 0)
            {
                return "net" + version;
            }
        }

        return candidate.ToLowerInvariant();
    }

    private static double ReadHostCpuPercent()
    {
        lock (HostCpuGate)
        {
            DateTime nowUtc = DateTime.UtcNow;
            long totalTicks = 0;

            Process[] processes = Process.GetProcesses();
            foreach (Process candidate in processes)
            {
                try
                {
                    totalTicks += candidate.TotalProcessorTime.Ticks;
                }
                catch
                {
                }
                finally
                {
                    candidate.Dispose();
                }
            }

            if (_hostCpuLastSampleUtc == default || _hostCpuLastTotalProcessTicks <= 0)
            {
                _hostCpuLastSampleUtc = nowUtc;
                _hostCpuLastTotalProcessTicks = totalTicks;
                return 0;
            }

            double elapsedTicks = Math.Max(1, (nowUtc - _hostCpuLastSampleUtc).Ticks);
            long deltaCpuTicks = Math.Max(0, totalTicks - _hostCpuLastTotalProcessTicks);

            _hostCpuLastSampleUtc = nowUtc;
            _hostCpuLastTotalProcessTicks = totalTicks;

            double percent = deltaCpuTicks * 100d / (elapsedTicks * Environment.ProcessorCount);
            return double.IsFinite(percent) ? Math.Clamp(percent, 0, 100d) : 0;
        }
    }

    private static double ReadTransitServerCpuPercent()
    {
        lock (TransitServerCpuGate)
        {
            DateTime nowUtc = DateTime.UtcNow;
            long totalTicks = 0;

            foreach (Process process in Process.GetProcessesByName("Vector.NNTP.NNTPD"))
            {
                try
                {
                    totalTicks += process.TotalProcessorTime.Ticks;
                }
                catch
                {
                }
                finally
                {
                    process.Dispose();
                }
            }

            if (_transitServerCpuLastSampleUtc == default || _transitServerCpuLastTotalTicks <= 0)
            {
                _transitServerCpuLastSampleUtc = nowUtc;
                _transitServerCpuLastTotalTicks = totalTicks;
                return 0;
            }

            double elapsedTicks = Math.Max(1, (nowUtc - _transitServerCpuLastSampleUtc).Ticks);
            long deltaCpuTicks = Math.Max(0, totalTicks - _transitServerCpuLastTotalTicks);

            _transitServerCpuLastSampleUtc = nowUtc;
            _transitServerCpuLastTotalTicks = totalTicks;

            double percent = deltaCpuTicks * 100d / elapsedTicks;
            return double.IsFinite(percent) ? Math.Max(0, percent) : 0;
        }
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

    private static string FindBackFillerAppSettingsPath()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);

        while (current is not null)
        {
            string candidate = Path.Combine(current.FullName, "VectorNNTP.BackFiller", "appsettings.json");
            if (File.Exists(candidate))
            {
                return candidate;
            }

            current = current.Parent;
        }

        throw new FileNotFoundException("Unable to locate existing BackFiller appsettings.json from benchmark runner base directory.");
    }

    private readonly record struct TransitBenchmarkConfig(
        BenchmarkMode Mode,
        long BenchmarkInstanceId,
        string EndpointHost,
        int EndpointPort,
        bool EndpointUseSsl,
        string AppSettingsPath,
        TimeSpan WarmupDuration,
        TimeSpan MeasurementDuration,
        int ConnectionPoolSize,
        int PerConnectionPipelineDepth,
        int DispatchWorkerCount,
        int GeneratorWorkerCount,
        int WriteBatchCoalesceMicroseconds,
        int MaxQueuedArticles,
        long MaxResidentBytes,
        int ArticleTargetBytes,
        int ProducerQueueTargetArticles,
        RuntimeIdentityExpectation ExpectedRuntimeIdentity)
    {
        internal static TransitBenchmarkConfig Load(TimeSpan measurementDuration, BenchmarkMode mode, TransitBenchmarkCliOptions cliOptions)
        {
            string appSettingsPath = FindBackFillerAppSettingsPath();

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(appSettingsPath, optional: false, reloadOnChange: false)
                .Build();

            string host = configuration["BackFiller:TransitServer:Host"]
                ?? throw new InvalidOperationException("BackFiller:TransitServer:Host is missing in existing application configuration.");

            string normalizedHost = host.Trim();
            if (!normalizedHost.Equals(RequiredTransitHostname, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Configured TransitServer host must be '{RequiredTransitHostname}', but was '{normalizedHost}'.");
            }

            string? portRaw = configuration["BackFiller:TransitServer:Port"];
            if (!int.TryParse(portRaw, out int port) || port is <= 0 or > 65535)
            {
                throw new InvalidOperationException("BackFiller:TransitServer:Port is missing or invalid in existing application configuration.");
            }

            bool useSsl = bool.TryParse(configuration["BackFiller:TransitServer:UseSsl"], out bool parsedUseSsl) && parsedUseSsl;

            int connectionPoolSize = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.ConnectionPoolSize ?? 4, min: 1, max: 64, "connections");
            int perConnectionPipelineDepth = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.PipelineDepth ?? 8, min: 1, max: 64, "pipeline-depth");

            int defaultDispatchWorkers = checked(connectionPoolSize * perConnectionPipelineDepth);
            int dispatchWorkerCount = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
                cliOptions.DispatchWorkers ?? defaultDispatchWorkers,
                min: 1,
                max: 512,
                optionName: "dispatch-workers");

            int articleTargetBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
                cliOptions.ArticleKilobytes is null ? DefaultArticleTargetBytes : checked(cliOptions.ArticleKilobytes.Value * 1024),
                min: 128 * 1024,
                max: 4 * 1024 * 1024,
                optionName: "article-kib");

            int generatorWorkerCount = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
                cliOptions.GeneratorWorkers ?? 1,
                min: 1,
                max: 512,
                optionName: "generator-workers");

            int writeBatchCoalesceMicroseconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
                cliOptions.WriteBatchCoalesceMicroseconds ?? 250,
                min: 1,
                max: 50_000,
                optionName: "write-batch-coalesce-us");

            long maxResidentBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateLongRange(
                cliOptions.QueueMegabytes is null ? 256L * 1024L * 1024L : checked((long)cliOptions.QueueMegabytes.Value * 1024L * 1024L),
                min: 64L * 1024L * 1024L,
                max: 2L * 1024L * 1024L * 1024L,
                optionName: "queue-mib");

            int computedArticlesFromBytes = (int)Math.Max(1, maxResidentBytes / articleTargetBytes);
            int maxQueuedArticles = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
                cliOptions.QueueArticles ?? Math.Max(64, computedArticlesFromBytes),
                min: 1,
                max: 200_000,
                optionName: "queue-articles");

            if (maxResidentBytes < articleTargetBytes)
            {
                throw new InvalidOperationException("Queue byte budget must be at least one article target size.");
            }

            int warmupSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.WarmupSeconds ?? DefaultWarmupSeconds, min: 1, max: 600, optionName: "warmup-seconds");

            int producerQueueTargetArticles = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
                Math.Min(maxQueuedArticles, 2048),
                min: 1,
                max: maxQueuedArticles,
                optionName: "producer-queue-target-articles");

            RuntimeIdentityExpectation expectedRuntimeIdentity = new(
                ExpectedAssemblyPath: cliOptions.ExpectedAssemblyPath,
                ExpectedAssemblyVersion: cliOptions.ExpectedAssemblyVersion,
                ExpectedFileVersion: cliOptions.ExpectedFileVersion,
                ExpectedConfiguration: cliOptions.ExpectedConfiguration,
                ExpectedPlatform: cliOptions.ExpectedPlatform,
                ExpectedTargetFramework: cliOptions.ExpectedTargetFramework,
                ExpectedRuntimeIdentifier: cliOptions.ExpectedRuntimeIdentifier,
                ExpectedArchitecture: cliOptions.ExpectedArchitecture);

            return new TransitBenchmarkConfig(
                Mode: mode,
                BenchmarkInstanceId: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                EndpointHost: normalizedHost,
                EndpointPort: port,
                EndpointUseSsl: useSsl,
                AppSettingsPath: appSettingsPath,
                WarmupDuration: TimeSpan.FromSeconds(warmupSeconds),
                MeasurementDuration: measurementDuration,
                ConnectionPoolSize: connectionPoolSize,
                PerConnectionPipelineDepth: perConnectionPipelineDepth,
                DispatchWorkerCount: dispatchWorkerCount,
                GeneratorWorkerCount: generatorWorkerCount,
                WriteBatchCoalesceMicroseconds: writeBatchCoalesceMicroseconds,
                MaxQueuedArticles: maxQueuedArticles,
                MaxResidentBytes: maxResidentBytes,
                ArticleTargetBytes: articleTargetBytes,
                ProducerQueueTargetArticles: producerQueueTargetArticles,
                ExpectedRuntimeIdentity: expectedRuntimeIdentity);
        }

    }

    private sealed class BoundedArticleQueue : IDisposable
    {
        private readonly Channel<QueuedArticle> _channel;
        private readonly ByteBudget _byteBudget;
        private long _queuedBytes;
        private int _queuedCount;
        private volatile bool _admissionStopped;

        internal BoundedArticleQueue(int maxArticles, long maxResidentBytes)
        {
            _channel = Channel.CreateBounded<QueuedArticle>(new BoundedChannelOptions(maxArticles)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait,
            });

            _byteBudget = new ByteBudget(maxResidentBytes);
        }

        internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
        internal long CurrentQueuedBytes => Volatile.Read(ref _queuedBytes);

        internal async ValueTask<bool> TryWriteAsync(QueuedArticle article, CancellationToken cancellationToken)
        {
            if (_admissionStopped)
            {
                return false;
            }

            await _byteBudget.AcquireAsync(article.PayloadLength, cancellationToken).ConfigureAwait(false);

            try
            {
                await _channel.Writer.WriteAsync(article, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _queuedCount);
                Interlocked.Add(ref _queuedBytes, article.PayloadLength);
                return true;
            }
            catch
            {
                _byteBudget.Release(article.PayloadLength);
                throw;
            }
        }

        internal bool TryRead(out QueuedArticle article)
        {
            bool success = _channel.Reader.TryRead(out article);
            if (success)
            {
                Interlocked.Decrement(ref _queuedCount);
                Interlocked.Add(ref _queuedBytes, -article.PayloadLength);
            }

            return success;
        }

        internal ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
        {
            return _channel.Reader.WaitToReadAsync(cancellationToken);
        }

        internal void ReleaseReservation(int bytes)
        {
            _byteBudget.Release(bytes);
        }

        internal void StopAdmission()
        {
            _admissionStopped = true;
            _channel.Writer.TryComplete();
        }

        public void Dispose()
        {
            StopAdmission();
            _byteBudget.Dispose();
        }
    }

    private sealed class ByteBudget : IDisposable
    {
        private readonly object _gate = new();
        private readonly Queue<BudgetWaiter> _waiters = new();
        private long _availableBytes;
        private bool _disposed;

        internal ByteBudget(long maxBytes)
        {
            if (maxBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxBytes), maxBytes, "Max bytes must be greater than zero.");
            }

            _availableBytes = maxBytes;
        }

        internal ValueTask AcquireAsync(int bytes, CancellationToken cancellationToken)
        {
            if (bytes <= 0)
            {
                return ValueTask.CompletedTask;
            }

            lock (_gate)
            {
                ThrowIfDisposed();

                if (_waiters.Count == 0 && _availableBytes >= bytes)
                {
                    _availableBytes -= bytes;
                    return ValueTask.CompletedTask;
                }

                BudgetWaiter waiter = new(bytes);
                _waiters.Enqueue(waiter);

                if (cancellationToken.CanBeCanceled)
                {
                    waiter.RegisterCancellation(cancellationToken, this);
                }

                return new ValueTask(waiter.Task);
            }
        }

        internal void Release(int bytes)
        {
            if (bytes <= 0)
            {
                return;
            }

            List<BudgetWaiter>? completed = null;

            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _availableBytes += bytes;

                while (_waiters.Count > 0)
                {
                    BudgetWaiter next = _waiters.Peek();
                    if (next.IsCanceled)
                    {
                        _waiters.Dequeue();
                        continue;
                    }

                    if (_availableBytes < next.RequestedBytes)
                    {
                        break;
                    }

                    _waiters.Dequeue();
                    _availableBytes -= next.RequestedBytes;
                    completed ??= [];
                    completed.Add(next);
                }
            }

            if (completed is null)
            {
                return;
            }

            foreach (BudgetWaiter waiter in completed)
            {
                waiter.TrySetAcquired();
            }
        }

        private void CancelWaiter(BudgetWaiter waiter)
        {
            lock (_gate)
            {
                waiter.MarkCanceled();
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ByteBudget));
            }
        }

        public void Dispose()
        {
            List<BudgetWaiter>? waitersToCancel = null;

            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;

                if (_waiters.Count > 0)
                {
                    waitersToCancel = _waiters.ToList();
                    _waiters.Clear();
                }
            }

            if (waitersToCancel is null)
            {
                return;
            }

            foreach (BudgetWaiter waiter in waitersToCancel)
            {
                waiter.TrySetCanceled();
            }
        }

        private sealed class BudgetWaiter
        {
            private readonly TaskCompletionSource _completion;
            private CancellationTokenRegistration _registration;

            internal BudgetWaiter(int requestedBytes)
            {
                RequestedBytes = requestedBytes;
                _completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            internal int RequestedBytes { get; }
            internal Task Task => _completion.Task;
            internal bool IsCanceled { get; private set; }

            internal void RegisterCancellation(CancellationToken cancellationToken, ByteBudget budget)
            {
                _registration = cancellationToken.Register(static state =>
                {
                    CancellationState data = (CancellationState)state!;
                    data.Waiter.TrySetCanceled();
                    data.Budget.CancelWaiter(data.Waiter);
                }, new CancellationState(budget, this));
            }

            internal void MarkCanceled()
            {
                IsCanceled = true;
            }

            internal void TrySetAcquired()
            {
                _registration.Dispose();
                _completion.TrySetResult();
            }

            internal void TrySetCanceled()
            {
                IsCanceled = true;
                _registration.Dispose();
                _completion.TrySetCanceled();
            }

            private readonly record struct CancellationState(ByteBudget Budget, BudgetWaiter Waiter);
        }
    }

    private readonly record struct QueuedArticle(string MessageId, int PayloadLength);

    private readonly record struct WorkloadPreparationSummary(
        double PreGenerationDurationMilliseconds,
        double PayloadPreparationDurationMilliseconds,
        int MessageIdPoolSize,
        int UniqueMessageIdCount,
        int DuplicateMessageIdCount,
        int ReusablePayloadBytes);

    private sealed class PreparedBenchmarkWorkload : IDisposable
    {
        private readonly string[] _messageIds;
        private int _nextMessageIndex;

        internal PreparedBenchmarkWorkload(string[] messageIds, byte[] reusablePayloadBytes, WorkloadPreparationSummary summary)
        {
            _messageIds = messageIds;
            ReusableArticlePayload = reusablePayloadBytes;
            PreparationSummary = summary;
        }

        internal ReadOnlyMemory<byte> ReusableArticlePayload { get; }

        internal int PayloadLength => ReusableArticlePayload.Length;

        internal WorkloadPreparationSummary PreparationSummary { get; }

        internal bool TryTakeNextMessageId(out string? messageId)
        {
            int index = Interlocked.Increment(ref _nextMessageIndex) - 1;
            if ((uint)index >= (uint)_messageIds.Length)
            {
                messageId = null;
                return false;
            }

            messageId = _messageIds[index];
            return true;
        }

        public void Dispose()
        {
        }
    }

    private sealed class MeasurementMetrics
    {
        private long _generatedCount;
        private long _generatedBytes;
        private long _admittedCount;
        private long _admittedBytes;
        private long _acceptedCount;
        private long _acceptedBytes;
        private long _rejectedCount;
        private long _ambiguousCount;
        private long _completedCount;

        private long _blockedTicks;
        private long _generationTicks;
        private long _otherActiveTicks;
        private long _activeTicks;
        private long _loopTicks;

        private long _peakQueueDepth;
        private long _peakQueueBytes;
        private long _peakInFlight;
        private long _peakActualPending;
        private long _minQueueDepth = long.MaxValue;
        private long _minQueueBytes = long.MaxValue;
        private long _queueDepthSampleCount;
        private long _queueDepthSampleSum;
        private long _queueBytesSampleSum;
        private long _producerQueueWaitTicks;

        private long _dispatchQueueWaitTicksTotal;
        private long _dispatchQueueWaitTicksMax;
        private long _dispatchQueueWaitSampleCount;
        private long _publishTicksTotal;
        private long _lifecycleTicksTotal;
        private long _publishSampleCount;
        private long _publishTicksMin = long.MaxValue;
        private long _publishTicksMax;

        private long _socketWriteTicksTotal;
        private long _socketWriteTicksMax;
        private long _socketWriteSampleCount;
        private long _responseWaitTicksTotal;
        private long _responseWaitTicksMax;
        private long _responseWaitSampleCount;
        private long _parseCorrelationTicksTotal;
        private long _parseCorrelationTicksMax;
        private long _parseCorrelationSampleCount;
        private long _totalPublishTicksTotal;
        private long _totalPublishTicksMax;
        private long _totalPublishSampleCount;

        private readonly object _forensicGate = new();
        private readonly List<long> _publishTicksSamples = [];
        private readonly List<long> _dispatchWaitTicksSamples = [];
        private readonly List<long> _socketWriteTicksSamples = [];
        private readonly List<long> _responseWaitTicksSamples = [];
        private readonly List<long> _parseCorrelationTicksSamples = [];
        private readonly List<long> _totalPublishTicksSamples = [];
        private readonly List<long>[] _publishBySubmitDepthBucket = [[], [], [], [], []];
        private readonly List<long>[] _publishByCompleteDepthBucket = [[], [], [], [], []];
        private readonly Dictionary<int, ConnectionSeriesAggregate> _connectionSeries = [];
        private readonly Dictionary<int, ConnectionCounterState> _connectionPrevious = [];
        private readonly List<DispatcherSeriesPoint> _dispatcherSeries = [];
        private int _forensicSampleCount;

        private readonly int _articleBytes;

        internal MeasurementMetrics(int articleBytes)
        {
            _articleBytes = articleBytes;
        }

        internal int InFlightSubmissions;

        internal void OnGenerated(int bytes, TransitBenchmarkCore.ProducerTiming producerTiming, long queueWaitTicks)
        {
            Interlocked.Increment(ref _generatedCount);
            Interlocked.Add(ref _generatedBytes, bytes);
            Interlocked.Add(ref _blockedTicks, producerTiming.BlockedTicks);
            Interlocked.Add(ref _generationTicks, producerTiming.GenerationTicks);
            Interlocked.Add(ref _otherActiveTicks, producerTiming.OtherActiveTicks);
            Interlocked.Add(ref _activeTicks, producerTiming.ActiveTicks);
            Interlocked.Add(ref _loopTicks, producerTiming.LoopTicks);
            Interlocked.Add(ref _producerQueueWaitTicks, queueWaitTicks);
        }

        internal void OnDequeued(long dequeuedTick)
        {
        }

        internal void OnAdmitted(int bytes, long dequeuedTick)
        {
            Interlocked.Increment(ref _admittedCount);
            Interlocked.Add(ref _admittedBytes, bytes);
        }

        internal void OnPublishResult(
            TransitPublishResult publishResult,
            int bytes,
            long dequeuedTick,
            long publishStartTick,
            long publishEndTick,
            int pendingAtSubmit,
            int pendingAtComplete)
        {
            if (publishResult.Status == TransitPublishStatus.Accepted)
            {
                Interlocked.Increment(ref _acceptedCount);
                Interlocked.Add(ref _acceptedBytes, bytes);
            }
            else if (publishResult.Status == TransitPublishStatus.Rejected)
            {
                Interlocked.Increment(ref _rejectedCount);
            }
            else if (publishResult.Status is TransitPublishStatus.Ambiguous or TransitPublishStatus.Unavailable or TransitPublishStatus.Failed or TransitPublishStatus.Canceled)
            {
                Interlocked.Increment(ref _ambiguousCount);
            }

            Interlocked.Increment(ref _completedCount);

            long dispatchQueueWaitTicks = Math.Max(0, publishStartTick - dequeuedTick);
            long publishTicks = Math.Max(0, publishEndTick - publishStartTick);
            long lifecycleTicks = dispatchQueueWaitTicks + publishTicks;

            Interlocked.Add(ref _publishTicksTotal, publishTicks);
            Interlocked.Add(ref _lifecycleTicksTotal, lifecycleTicks);
            Interlocked.Increment(ref _publishSampleCount);
            UpdatePeak(ref _publishTicksMax, publishTicks);
            UpdateMin(ref _publishTicksMin, publishTicks);

            long t0 = publishResult.T0PublishAsyncEnterTick;
            long t1 = publishResult.T1DispatcherAssignedTick;
            long t2 = publishResult.T2SocketWriteBeginTick;
            long t3 = publishResult.T3SocketWriteEndTick;
            long t4 = publishResult.T4ResponseAvailableTick;
            long t6 = publishResult.T6ResponseCorrelatedTick;
            long t7 = publishResult.T7PublishAsyncCompleteTick;

            bool hasDispatchWait = t0 > 0 && t1 >= t0;
            bool hasSocketWrite = t2 > 0 && t3 >= t2;
            bool hasResponseWait = t3 > 0 && t4 >= t3;
            bool hasParseCorrelation = t4 > 0 && t6 >= t4;
            bool hasTotal = t0 > 0 && t7 >= t0;

            long dispatchWaitTicks = hasDispatchWait ? t1 - t0 : 0;
            long socketWriteTicks = hasSocketWrite ? t3 - t2 : 0;
            long responseWaitTicks = hasResponseWait ? t4 - t3 : 0;
            long parseCorrelationTicks = hasParseCorrelation ? t6 - t4 : 0;
            long totalPublishTicks = hasTotal ? t7 - t0 : 0;

            if (hasDispatchWait)
            {
                Interlocked.Add(ref _dispatchQueueWaitTicksTotal, dispatchWaitTicks);
                Interlocked.Increment(ref _dispatchQueueWaitSampleCount);
                UpdatePeak(ref _dispatchQueueWaitTicksMax, dispatchWaitTicks);
            }

            if (hasSocketWrite)
            {
                Interlocked.Add(ref _socketWriteTicksTotal, socketWriteTicks);
                Interlocked.Increment(ref _socketWriteSampleCount);
                UpdatePeak(ref _socketWriteTicksMax, socketWriteTicks);
            }

            if (hasResponseWait)
            {
                Interlocked.Add(ref _responseWaitTicksTotal, responseWaitTicks);
                Interlocked.Increment(ref _responseWaitSampleCount);
                UpdatePeak(ref _responseWaitTicksMax, responseWaitTicks);
            }

            if (hasParseCorrelation)
            {
                Interlocked.Add(ref _parseCorrelationTicksTotal, parseCorrelationTicks);
                Interlocked.Increment(ref _parseCorrelationSampleCount);
                UpdatePeak(ref _parseCorrelationTicksMax, parseCorrelationTicks);
            }

            if (hasTotal)
            {
                Interlocked.Add(ref _totalPublishTicksTotal, totalPublishTicks);
                Interlocked.Increment(ref _totalPublishSampleCount);
                UpdatePeak(ref _totalPublishTicksMax, totalPublishTicks);
            }

            int submitBucket = ClassifyDepthBucket(pendingAtSubmit);
            int completeBucket = ClassifyDepthBucket(pendingAtComplete);

            lock (_forensicGate)
            {
                _publishTicksSamples.Add(publishTicks);
                _publishBySubmitDepthBucket[submitBucket].Add(publishTicks);
                _publishByCompleteDepthBucket[completeBucket].Add(publishTicks);

                if (hasDispatchWait)
                {
                    _dispatchWaitTicksSamples.Add(dispatchWaitTicks);
                }

                if (hasSocketWrite)
                {
                    _socketWriteTicksSamples.Add(socketWriteTicks);
                }

                if (hasResponseWait)
                {
                    _responseWaitTicksSamples.Add(responseWaitTicks);
                }

                if (hasParseCorrelation)
                {
                    _parseCorrelationTicksSamples.Add(parseCorrelationTicks);
                }

                if (hasTotal)
                {
                    _totalPublishTicksSamples.Add(totalPublishTicks);
                }
            }
        }

        internal void RecordConnectionSample(TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics, TimeSpan elapsed)
        {
            lock (_forensicGate)
            {
                _forensicSampleCount++;

                foreach (TransitPublisher.ConnectionDiagnosticsEntry entry in diagnostics.Connections)
                {
                    int slot = entry.SlotIndex;
                    TransitConnection.TransitConnectionDiagnosticsSnapshot s = entry.Snapshot;
                    long completed = s.SubmissionsAccepted + s.SubmissionsRejected + s.SubmissionsAmbiguous + s.SubmissionsUnavailable + s.SubmissionsFailed;

                    if (!_connectionSeries.TryGetValue(slot, out ConnectionSeriesAggregate? aggregate))
                    {
                        aggregate = new ConnectionSeriesAggregate(slot);
                        _connectionSeries[slot] = aggregate;
                    }

                    double submitRate = 0;
                    double completeRate = 0;
                    double responseRate = 0;

                    if (_connectionPrevious.TryGetValue(slot, out ConnectionCounterState prev))
                    {
                        double dt = Math.Max(0.000001d, (elapsed - prev.Elapsed).TotalSeconds);
                        submitRate = Math.Max(0, s.SubmissionsStarted - prev.SubmissionsStarted) / dt;
                        completeRate = Math.Max(0, completed - prev.Completed) / dt;
                        responseRate = completeRate;
                    }

                    _connectionPrevious[slot] = new ConnectionCounterState(s.ConnectionId, elapsed, s.SubmissionsStarted, completed);
                    TransitPublisher.ConnectionSlotSnapshot? slotSnapshot = diagnostics.Slots.FirstOrDefault(x => x.SlotIndex == slot);
                    long reconnects = slotSnapshot?.Reconnects ?? 0;
                    aggregate.Observe(s, submitRate, completeRate, responseRate, reconnects);
                }
            }
        }

        internal void RecordDispatcherSample(TimeSpan elapsed, int inFlight, long dispatchPending, int actualPending, int queueDepth, long queueBytes)
        {
            lock (_forensicGate)
            {
                _dispatcherSeries.Add(new DispatcherSeriesPoint(elapsed, inFlight, dispatchPending, actualPending, queueDepth, queueBytes));
            }
        }

        internal long GetAdmittedCount() => Interlocked.Read(ref _admittedCount);
        internal long GetCompletedCount() => Interlocked.Read(ref _completedCount);

        internal void ObservePeaks(int queueDepth, long queueBytes, int inFlight)
        {
            UpdatePeak(ref _peakQueueDepth, queueDepth);
            UpdatePeak(ref _peakQueueBytes, queueBytes);
            UpdatePeak(ref _peakInFlight, inFlight);
            UpdateMin(ref _minQueueDepth, queueDepth);
            UpdateMin(ref _minQueueBytes, queueBytes);
            Interlocked.Increment(ref _queueDepthSampleCount);
            Interlocked.Add(ref _queueDepthSampleSum, queueDepth);
            Interlocked.Add(ref _queueBytesSampleSum, queueBytes);
        }

        internal void ObserveActualPending(int actualPending)
        {
            UpdatePeak(ref _peakActualPending, actualPending);
        }

        internal ForensicSnapshot CaptureForensicSnapshot()
        {
            long publishCount = Math.Max(1, Interlocked.Read(ref _publishSampleCount));
            long publishMinTicks = Interlocked.Read(ref _publishTicksMin);
            if (publishMinTicks == long.MaxValue)
            {
                publishMinTicks = 0;
            }

            lock (_forensicGate)
            {
                long dispatchWaitSampleCount = Math.Max(1, Interlocked.Read(ref _dispatchQueueWaitSampleCount));
                long socketWriteSampleCount = Math.Max(1, Interlocked.Read(ref _socketWriteSampleCount));
                long responseWaitSampleCount = Math.Max(1, Interlocked.Read(ref _responseWaitSampleCount));
                long parseCorrelationSampleCount = Math.Max(1, Interlocked.Read(ref _parseCorrelationSampleCount));
                long totalPublishSampleCount = Math.Max(1, Interlocked.Read(ref _totalPublishSampleCount));

                return new ForensicSnapshot(
                    AverageDispatchQueueWaitUs: TicksToUs(Interlocked.Read(ref _dispatchQueueWaitTicksTotal) / (double)dispatchWaitSampleCount),
                    P50DispatchQueueWaitUs: PercentileUs(_dispatchWaitTicksSamples, 0.50),
                    P95DispatchQueueWaitUs: PercentileUs(_dispatchWaitTicksSamples, 0.95),
                    P99DispatchQueueWaitUs: PercentileUs(_dispatchWaitTicksSamples, 0.99),
                    MaxDispatchQueueWaitUs: TicksToUs(Interlocked.Read(ref _dispatchQueueWaitTicksMax)),
                    DispatchQueueWaitSampleCount: Interlocked.Read(ref _dispatchQueueWaitSampleCount),
                    AverageSocketWriteUs: TicksToUs(Interlocked.Read(ref _socketWriteTicksTotal) / (double)socketWriteSampleCount),
                    P50SocketWriteUs: PercentileUs(_socketWriteTicksSamples, 0.50),
                    P95SocketWriteUs: PercentileUs(_socketWriteTicksSamples, 0.95),
                    P99SocketWriteUs: PercentileUs(_socketWriteTicksSamples, 0.99),
                    MaxSocketWriteUs: TicksToUs(Interlocked.Read(ref _socketWriteTicksMax)),
                    SocketWriteSampleCount: Interlocked.Read(ref _socketWriteSampleCount),
                    AverageResponseWaitUs: TicksToUs(Interlocked.Read(ref _responseWaitTicksTotal) / (double)responseWaitSampleCount),
                    P50ResponseWaitUs: PercentileUs(_responseWaitTicksSamples, 0.50),
                    P95ResponseWaitUs: PercentileUs(_responseWaitTicksSamples, 0.95),
                    P99ResponseWaitUs: PercentileUs(_responseWaitTicksSamples, 0.99),
                    MaxResponseWaitUs: TicksToUs(Interlocked.Read(ref _responseWaitTicksMax)),
                    ResponseWaitSampleCount: Interlocked.Read(ref _responseWaitSampleCount),
                    AverageParseCorrelationUs: TicksToUs(Interlocked.Read(ref _parseCorrelationTicksTotal) / (double)parseCorrelationSampleCount),
                    P50ParseCorrelationUs: PercentileUs(_parseCorrelationTicksSamples, 0.50),
                    P95ParseCorrelationUs: PercentileUs(_parseCorrelationTicksSamples, 0.95),
                    P99ParseCorrelationUs: PercentileUs(_parseCorrelationTicksSamples, 0.99),
                    MaxParseCorrelationUs: TicksToUs(Interlocked.Read(ref _parseCorrelationTicksMax)),
                    ParseCorrelationSampleCount: Interlocked.Read(ref _parseCorrelationSampleCount),
                    AverageTotalPublishLatencyUs: TicksToUs(Interlocked.Read(ref _totalPublishTicksTotal) / (double)totalPublishSampleCount),
                    P50TotalPublishLatencyUs: PercentileUs(_totalPublishTicksSamples, 0.50),
                    P95TotalPublishLatencyUs: PercentileUs(_totalPublishTicksSamples, 0.95),
                    P99TotalPublishLatencyUs: PercentileUs(_totalPublishTicksSamples, 0.99),
                    MaxTotalPublishLatencyUs: TicksToUs(Interlocked.Read(ref _totalPublishTicksMax)),
                    TotalPublishLatencySampleCount: Interlocked.Read(ref _totalPublishSampleCount),
                    AveragePublishLatencyUs: TicksToUs(Interlocked.Read(ref _publishTicksTotal) / (double)publishCount),
                    MinPublishLatencyUs: TicksToUs(publishMinTicks),
                    P50PublishLatencyUs: PercentileUs(_publishTicksSamples, 0.50),
                    P95PublishLatencyUs: PercentileUs(_publishTicksSamples, 0.95),
                    P99PublishLatencyUs: PercentileUs(_publishTicksSamples, 0.99),
                    MaxPublishLatencyUs: TicksToUs(Interlocked.Read(ref _publishTicksMax)),
                    AverageLifecycleLatencyUs: TicksToUs(Interlocked.Read(ref _lifecycleTicksTotal) / (double)publishCount),
                    PendingDepthLatencyBuckets: BuildDepthBucketSummary(_publishBySubmitDepthBucket, _publishByCompleteDepthBucket),
                    ForensicSampleCount: _forensicSampleCount,
                    ConnectionTimeSeriesSummary: BuildConnectionSeriesSummary(_connectionSeries),
                    DispatcherTimeSeriesSummary: BuildDispatcherSeriesSummary(_dispatcherSeries),
                    ObservabilityNotes: "Lifecycle timing captures T0..T7 with Stopwatch ticks, enabling separation of dispatch wait, socket write, response wait, parse/correlation, and total PublishAsync latency without per-article logs.");
            }
        }

        internal MeasurementSnapshot Snapshot()
        {
            return new MeasurementSnapshot(
                GeneratedCount: Interlocked.Read(ref _generatedCount),
                GeneratedBytes: Interlocked.Read(ref _generatedBytes),
                AdmittedCount: Interlocked.Read(ref _admittedCount),
                AdmittedBytes: Interlocked.Read(ref _admittedBytes),
                AcceptedCount: Interlocked.Read(ref _acceptedCount),
                AcceptedBytes: Interlocked.Read(ref _acceptedBytes),
                RejectedCount: Interlocked.Read(ref _rejectedCount),
                AmbiguousCount: Interlocked.Read(ref _ambiguousCount),
                CompletedCount: Interlocked.Read(ref _completedCount),
                BlockedTicks: Interlocked.Read(ref _blockedTicks),
                GenerationTicks: Interlocked.Read(ref _generationTicks),
                OtherActiveTicks: Interlocked.Read(ref _otherActiveTicks),
                ActiveTicks: Interlocked.Read(ref _activeTicks),
                LoopTicks: Interlocked.Read(ref _loopTicks),
                PeakQueueDepth: Interlocked.Read(ref _peakQueueDepth),
                PeakQueueBytes: Interlocked.Read(ref _peakQueueBytes),
                PeakInFlight: Interlocked.Read(ref _peakInFlight),
                PeakActualPending: Interlocked.Read(ref _peakActualPending),
                MinQueueDepth: NormalizeMin(_minQueueDepth),
                MinQueueBytes: NormalizeMin(_minQueueBytes),
                AverageQueueDepth: ComputeAverage(_queueDepthSampleSum, _queueDepthSampleCount),
                AverageQueueBytes: ComputeAverage(_queueBytesSampleSum, _queueDepthSampleCount),
                ProducerQueueWaitTicks: Interlocked.Read(ref _producerQueueWaitTicks),
                ArticleBytes: _articleBytes);
        }

        private static int ClassifyDepthBucket(int pending)
        {
            if (pending <= 4) return 0;
            if (pending <= 8) return 1;
            if (pending <= 12) return 2;
            if (pending <= 16) return 3;
            return 4;
        }

        private static string BuildDepthBucketSummary(List<long>[] submitBuckets, List<long>[] completeBuckets)
        {
            string[] labels = ["1-4", "5-8", "9-12", "13-16", ">16"];
            List<string> parts = new(capacity: labels.Length);
            for (int i = 0; i < labels.Length; i++)
            {
                double submitAvg = submitBuckets[i].Count == 0 ? 0 : TicksToUs(submitBuckets[i].Average());
                double submitP95 = PercentileUs(submitBuckets[i], 0.95);
                double completeAvg = completeBuckets[i].Count == 0 ? 0 : TicksToUs(completeBuckets[i].Average());
                double completeP95 = PercentileUs(completeBuckets[i], 0.95);
                parts.Add($"Depth {labels[i]}: submit avg={submitAvg:F2}us p95={submitP95:F2}us | complete avg={completeAvg:F2}us p95={completeP95:F2}us");
            }

            return string.Join("; ", parts);
        }

        private static string BuildConnectionSeriesSummary(Dictionary<int, ConnectionSeriesAggregate> series)
        {
            if (series.Count == 0)
            {
                return "(no connection time-series samples)";
            }

            IEnumerable<string> lines = series
                .OrderBy(static x => x.Key)
                .Select(static kv => kv.Value.FormatLine());

            return string.Join(Environment.NewLine, lines);
        }

        private static string BuildDispatcherSeriesSummary(List<DispatcherSeriesPoint> series)
        {
            if (series.Count == 0)
            {
                return "(no dispatcher time-series samples)";
            }

            double avgInFlight = series.Average(static x => x.InFlight);
            double avgDispatchPending = series.Average(static x => x.DispatchPending);
            double avgActualPending = series.Average(static x => x.ActualPending);
            int maxInFlight = series.Max(static x => x.InFlight);
            long maxDispatchPending = series.Max(static x => x.DispatchPending);
            int maxActualPending = series.Max(static x => x.ActualPending);

            return $"samples={series.Count}, inFlight avg/max={avgInFlight:F2}/{maxInFlight}, dispatchPending avg/max={avgDispatchPending:F2}/{maxDispatchPending}, actualPending avg/max={avgActualPending:F2}/{maxActualPending}";
        }

        private static double PercentileUs(List<long> samples, double percentile)
        {
            if (samples.Count == 0)
            {
                return 0;
            }

            List<long> sorted = [.. samples];
            sorted.Sort();
            int index = (int)Math.Clamp(Math.Ceiling(percentile * sorted.Count) - 1, 0, sorted.Count - 1);
            return TicksToUs(sorted[index]);
        }

        private static double TicksToUs(double ticks)
        {
            if (ticks <= 0)
            {
                return 0;
            }

            return ticks * 1_000_000d / Stopwatch.Frequency;
        }

        private static long NormalizeMin(long value)
        {
            return value == long.MaxValue ? 0 : value;
        }

        private static double ComputeAverage(long sum, long count)
        {
            return count <= 0 ? 0 : (double)sum / count;
        }

        private static void UpdatePeak(ref long location, long candidate)
        {
            while (true)
            {
                long current = Interlocked.Read(ref location);
                if (candidate <= current)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref location, candidate, current) == current)
                {
                    return;
                }
            }
        }

        private static void UpdateMin(ref long location, long candidate)
        {
            while (true)
            {
                long current = Interlocked.Read(ref location);
                if (candidate >= current)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref location, candidate, current) == current)
                {
                    return;
                }
            }
        }

        private sealed class ConnectionSeriesAggregate
        {
            private readonly int _slot;
            private double _pendingSum;
            private int _samples;
            private int _pendingMin = int.MaxValue;
            private int _pendingMax;
            private int _maxInFlight;
            private long _failures;
            private long _reconnects;
            private double _submitRateSum;
            private double _completeRateSum;
            private double _responseRateSum;

            internal ConnectionSeriesAggregate(int slot)
            {
                _slot = slot;
            }

            internal void Observe(TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot, double submitRate, double completeRate, double responseRate, long reconnects)
            {
                _samples++;
                _pendingSum += snapshot.CurrentConcurrentSubmissions;
                _pendingMin = Math.Min(_pendingMin, snapshot.CurrentConcurrentSubmissions);
                _pendingMax = Math.Max(_pendingMax, snapshot.CurrentConcurrentSubmissions);
                _maxInFlight = Math.Max(_maxInFlight, snapshot.MaxConcurrentSubmissions);
                _submitRateSum += submitRate;
                _completeRateSum += completeRate;
                _responseRateSum += responseRate;
                _failures = snapshot.SubmissionsFailed + snapshot.SubmissionsUnavailable;
                _reconnects = reconnects;
            }

            internal string FormatLine()
            {
                double avgPending = _samples == 0 ? 0 : _pendingSum / _samples;
                double avgSubmitRate = _samples == 0 ? 0 : _submitRateSum / _samples;
                double avgCompleteRate = _samples == 0 ? 0 : _completeRateSum / _samples;
                double avgResponseRate = _samples == 0 ? 0 : _responseRateSum / _samples;
                int pendingMin = _pendingMin == int.MaxValue ? 0 : _pendingMin;
                return $"slot={_slot}, pending min/avg/max={pendingMin}/{avgPending:F2}/{_pendingMax}, maxInFlight={_maxInFlight}, submitRate={avgSubmitRate:F2}/s, completionRate={avgCompleteRate:F2}/s, responseRate={avgResponseRate:F2}/s, failures={_failures}, reconnects={_reconnects}";
            }
        }

        private readonly record struct ConnectionCounterState(
            string ConnectionId,
            TimeSpan Elapsed,
            long SubmissionsStarted,
            long Completed);

        private readonly record struct DispatcherSeriesPoint(
            TimeSpan Elapsed,
            int InFlight,
            long DispatchPending,
            int ActualPending,
            int QueueDepth,
            long QueueBytes);
    }

    private sealed class RuntimeMetrics
    {
        private readonly object _gate = new();
        private double _cpuPercentSum;
        private double _hostCpuPercentSum;
        private double _transitServerCpuPercentSum;
        private long _cpuSampleCount;
        private long _lastWorkingSet;
        private long _lastGcHeap;
        private long _lastAllocated;
        private double _peakHostCpuPercent;
        private double _peakTransitServerCpuPercent;

        internal void Sample(double cpuPercent, double hostCpuPercent, double transitServerCpuPercent, long workingSet, long gcHeap, long allocated)
        {
            lock (_gate)
            {
                _cpuPercentSum += cpuPercent;
                _hostCpuPercentSum += hostCpuPercent;
                _transitServerCpuPercentSum += transitServerCpuPercent;
                _cpuSampleCount++;
                _lastWorkingSet = workingSet;
                _lastGcHeap = gcHeap;
                _lastAllocated = allocated;
                _peakHostCpuPercent = Math.Max(_peakHostCpuPercent, hostCpuPercent);
                _peakTransitServerCpuPercent = Math.Max(_peakTransitServerCpuPercent, transitServerCpuPercent);
            }
        }

        internal RuntimeSnapshot Snapshot()
        {
            lock (_gate)
            {
                double avgCpu = _cpuSampleCount == 0 ? 0 : _cpuPercentSum / _cpuSampleCount;
                double avgHostCpu = _cpuSampleCount == 0 ? 0 : _hostCpuPercentSum / _cpuSampleCount;
                double avgTransitCpu = _cpuSampleCount == 0 ? 0 : _transitServerCpuPercentSum / _cpuSampleCount;
                return new RuntimeSnapshot(avgCpu, avgHostCpu, avgTransitCpu, _peakHostCpuPercent, _peakTransitServerCpuPercent, _lastWorkingSet, _lastGcHeap, _lastAllocated);
            }
        }
    }

    private readonly record struct MeasurementSnapshot(
        long GeneratedCount,
        long GeneratedBytes,
        long AdmittedCount,
        long AdmittedBytes,
        long AcceptedCount,
        long AcceptedBytes,
        long RejectedCount,
        long AmbiguousCount,
        long CompletedCount,
        long BlockedTicks,
        long GenerationTicks,
        long OtherActiveTicks,
        long ActiveTicks,
        long LoopTicks,
        long PeakQueueDepth,
        long PeakQueueBytes,
        long PeakInFlight,
        long PeakActualPending,
        long MinQueueDepth,
        long MinQueueBytes,
        double AverageQueueDepth,
        double AverageQueueBytes,
        long ProducerQueueWaitTicks,
        int ArticleBytes);

    private readonly record struct RuntimeSnapshot(
        double AverageCpuPercent,
        double AverageHostCpuPercent,
        double AverageTransitServerCpuPercent,
        double PeakHostCpuPercent,
        double PeakTransitServerCpuPercent,
        long LastWorkingSetBytes,
        long LastGcHeapBytes,
        long LastAllocatedBytes);

    private readonly record struct BenchmarkResult(
        string BenchmarkBuildVersion,
        RuntimeExecutionIdentity RuntimeIdentity,
        WorkloadPreparationSummary WorkloadPreparation,
        DateTimeOffset MeasurementStartUtc,
        DateTimeOffset MeasurementEndUtc,
        TimeSpan DrainDuration,
        long OutstandingAtMeasurementEnd,
        long DrainedAfterMeasurement,
        long GeneratedArticles,
        long GeneratedBytes,
        double GeneratedGbps,
        long AdmittedArticles,
        long AdmittedBytes,
        double AdmittedGbps,
        long AcceptedArticles,
        long AcceptedBytes,
        double AcceptedGbps,
        long RejectedArticles,
        long AmbiguousArticles,
        long MinQueueDepth,
        double AverageQueueDepth,
        double AverageQueuedBytes,
        long PeakQueueDepth,
        long PeakQueuedBytes,
        long PeakInFlight,
        long PeakActualPending,
        double ProducerActivePercent,
        double ProducerBlockedPercent,
        double ProducerActiveMilliseconds,
        double ProducerBlockedMilliseconds,
        double ProducerQueueWaitMilliseconds,
        double AverageCpuPercent,
        double AverageHostCpuPercent,
        double AverageTransitServerCpuPercent,
        double PeakHostCpuPercent,
        double PeakTransitServerCpuPercent,
        double WorkingSetMb,
        double GcHeapMb,
        double AllocatedMb,
        int Gen0Collections,
        int Gen1Collections,
        int Gen2Collections,
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
        double MinPublishLatencyUs,
        double P50PublishLatencyUs,
        double P95PublishLatencyUs,
        double P99PublishLatencyUs,
        double MaxPublishLatencyUs,
        double AverageLifecycleLatencyUs,
        string PendingDepthLatencyBuckets,
        int ForensicSampleCount,
        string ConnectionTimeSeriesSummary,
        string DispatcherTimeSeriesSummary,
        string ObservabilityNotes,
        long EffectiveQueueArticleCapacityFromBytes)
    {
        internal static BenchmarkResult Create(
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
    }

    private readonly record struct ForensicSnapshot(
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
        double MinPublishLatencyUs,
        double P50PublishLatencyUs,
        double P95PublishLatencyUs,
        double P99PublishLatencyUs,
        double MaxPublishLatencyUs,
        double AverageLifecycleLatencyUs,
        string PendingDepthLatencyBuckets,
        int ForensicSampleCount,
        string ConnectionTimeSeriesSummary,
        string DispatcherTimeSeriesSummary,
        string ObservabilityNotes)
    {
        internal static ForensicSnapshot Empty => new(
            AverageDispatchQueueWaitUs: 0,
            P50DispatchQueueWaitUs: 0,
            P95DispatchQueueWaitUs: 0,
            P99DispatchQueueWaitUs: 0,
            MaxDispatchQueueWaitUs: 0,
            DispatchQueueWaitSampleCount: 0,
            AverageSocketWriteUs: 0,
            P50SocketWriteUs: 0,
            P95SocketWriteUs: 0,
            P99SocketWriteUs: 0,
            MaxSocketWriteUs: 0,
            SocketWriteSampleCount: 0,
            AverageResponseWaitUs: 0,
            P50ResponseWaitUs: 0,
            P95ResponseWaitUs: 0,
            P99ResponseWaitUs: 0,
            MaxResponseWaitUs: 0,
            ResponseWaitSampleCount: 0,
            AverageParseCorrelationUs: 0,
            P50ParseCorrelationUs: 0,
            P95ParseCorrelationUs: 0,
            P99ParseCorrelationUs: 0,
            MaxParseCorrelationUs: 0,
            ParseCorrelationSampleCount: 0,
            AverageTotalPublishLatencyUs: 0,
            P50TotalPublishLatencyUs: 0,
            P95TotalPublishLatencyUs: 0,
            P99TotalPublishLatencyUs: 0,
            MaxTotalPublishLatencyUs: 0,
            TotalPublishLatencySampleCount: 0,
            AveragePublishLatencyUs: 0,
            MinPublishLatencyUs: 0,
            P50PublishLatencyUs: 0,
            P95PublishLatencyUs: 0,
            P99PublishLatencyUs: 0,
            MaxPublishLatencyUs: 0,
            AverageLifecycleLatencyUs: 0,
            PendingDepthLatencyBuckets: "(forensic diagnostics disabled)",
            ForensicSampleCount: 0,
            ConnectionTimeSeriesSummary: "(forensic diagnostics disabled)",
            DispatcherTimeSeriesSummary: "(forensic diagnostics disabled)",
            ObservabilityNotes: "Forensic diagnostics disabled for this run.");
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

    private readonly record struct RuntimeIdentityExpectation(
        string? ExpectedAssemblyPath,
        string? ExpectedAssemblyVersion,
        string? ExpectedFileVersion,
        string? ExpectedConfiguration,
        string? ExpectedPlatform,
        string? ExpectedTargetFramework,
        string? ExpectedRuntimeIdentifier,
        string? ExpectedArchitecture);

    private readonly record struct RuntimeExecutionIdentity(
        string RuntimeAssemblyPath,
        string RuntimeAssemblyVersion,
        string? AssemblyFileVersion,
        string ProcessPath,
        string WorkingDirectory,
        string? Configuration,
        string? Platform,
        string? TargetFramework,
        string? RuntimeIdentifier,
        string Architecture,
        string? SourceRevision,
        DateTimeOffset? BuildTimestampUtc);

    private static class AppBuildConfiguration
    {
        public static readonly string? Value = AppAssemblyMetadata.GetValue("Configuration")
            ?? typeof(AppBuildConfiguration).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration;
    }

    private static class AppBuildPlatform
    {
        public static readonly string? Value = AppAssemblyMetadata.GetValue("Platform");
    }

    private static class AppTargetFramework
    {
        public static readonly string? Value = AppAssemblyMetadata.GetValue("TargetFramework")
            ?? typeof(AppTargetFramework).Assembly.GetCustomAttribute<System.Runtime.Versioning.TargetFrameworkAttribute>()?.FrameworkName;
    }

    private static class AppRuntimeIdentifier
    {
        public static readonly string? Value = AppAssemblyMetadata.GetValue("RuntimeIdentifier");
    }

    private static class AppAssemblyMetadata
    {
        public static string? GetValue(string key)
        {
            return typeof(AppAssemblyMetadata).Assembly
                .GetCustomAttributes<AssemblyMetadataAttribute>()
                .FirstOrDefault(metadata => string.Equals(metadata.Key, key, StringComparison.OrdinalIgnoreCase))
                ?.Value;
        }
    }

    private enum BenchmarkMode
    {
        Validation,
        Full,
        Saturation,
        Forensic,
    }
}
