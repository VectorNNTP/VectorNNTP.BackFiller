using System.Globalization;
using System.Net;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class TransitBenchmarkOrchestrator
{
    internal static async Task RunCoreAsync(
        TransitBenchmarkConfig config,
        RuntimeExecutionIdentity runtimeIdentity,
        string benchmarkBuildVersion,
        Func<ILoggerFactory, ILogger<TransitPublisher>> createTransitPublisherLogger,
        Func<TransitPublisher, TransitBenchmarkConfig, PreparedBenchmarkWorkload, CancellationToken, bool, Task<BenchmarkResult>> runMeasurementAsync,
        Action<BenchmarkResult, TransitBenchmarkConfig> writeStructuredResultArtifacts,
        CancellationToken cancellationToken)
    {
        RuntimeIdentityGuard.EnsureMatches(config.ExpectedRuntimeIdentity, runtimeIdentity);

        Console.WriteLine("=== Transit Publisher Production-Path Benchmark ===");
        Console.WriteLine("Benchmark execution policy: NEVER use --no-build. ALWAYS run clean -> build -> verify output identity -> execute.");
        Console.WriteLine($"Benchmark Build Version: {benchmarkBuildVersion}");
        Console.WriteLine($"RuntimeAssemblyPath: {runtimeIdentity.RuntimeAssemblyPath}");
        Console.WriteLine($"RuntimeAssemblyVersion: {runtimeIdentity.RuntimeAssemblyVersion}");
        Console.WriteLine($"AssemblyFileVersion: {runtimeIdentity.AssemblyFileVersion ?? "(unknown)"}");
        Console.WriteLine($"ProcessPath: {runtimeIdentity.ProcessPath}");
        Console.WriteLine($"WorkingDirectory: {runtimeIdentity.WorkingDirectory}");
        Console.WriteLine($"Configuration: {runtimeIdentity.Configuration ?? "(unknown)"}");
        Console.WriteLine($"Platform: {runtimeIdentity.Platform ?? "(unknown)"}");
        Console.WriteLine($"TargetFramework: {runtimeIdentity.TargetFramework ?? "(unknown)"}");
        Console.WriteLine($"RuntimeIdentifier: {runtimeIdentity.RuntimeIdentifier ?? "(unknown)"}");
        Console.WriteLine($"Architecture: {runtimeIdentity.Architecture}");
        Console.WriteLine($"SourceRevision: {runtimeIdentity.SourceRevision ?? "(unknown)"}");
        Console.WriteLine($"BuildTimestampUtc: {(runtimeIdentity.BuildTimestampUtc.HasValue ? runtimeIdentity.BuildTimestampUtc.Value.ToString("O", CultureInfo.InvariantCulture) : "(unknown)")}");
        Console.WriteLine($"ProductionDependencyPath: {runtimeIdentity.ProductionDependencyPath ?? "(unknown)"}");
        Console.WriteLine($"ProductionDependencyAssemblyVersion: {runtimeIdentity.ProductionDependencyAssemblyVersion ?? "(unknown)"}");
        Console.WriteLine($"ProductionDependencyFileVersion: {runtimeIdentity.ProductionDependencyFileVersion ?? "(unknown)"}");
        Console.WriteLine($"Mode: {config.Mode}");
        string executionProfile = config.MeasurementArticleCount is int articleCount
            ? $"Fixed-article-count ({articleCount})"
            : (config.Mode == BenchmarkMode.Saturation ? "Saturation discovery" : "Fixed-duration");
        Console.WriteLine($"Experiment profile: {executionProfile}");
        Console.WriteLine($"Config path: {config.AppSettingsPath}");
        Console.WriteLine($"Endpoint type: {config.EndpointType}");
        Console.WriteLine($"Endpoint identity: {config.EndpointIdentity}");
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
        Console.WriteLine($"Measurement article count: {(config.MeasurementArticleCount?.ToString() ?? "(duration-driven)")}");

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

        ILogger<TransitPublisher> transitPublisherLogger = createTransitPublisherLogger(loggerFactory);

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
        if (config.MeasurementArticleCount is null)
        {
            await RunWarmupAsync(publisher, config, workload, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            Console.WriteLine("Warmup skipped for fixed article-count mode to preserve exact measurement article accounting.");
        }

        Console.WriteLine();
        Console.WriteLine("=== Phase 5: EXACT measurement window ===");
        BenchmarkResult result = await runMeasurementAsync(
            publisher,
            config,
            workload,
            cancellationToken,
            false).ConfigureAwait(false);

        Console.WriteLine();
        Console.WriteLine("=== Phase 7: Connection topology diagnostics ===");
        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot connectionDiagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
        TopologyReporter.PrintConnectionTopologyDiagnostics(connectionDiagnostics);

        Console.WriteLine();
        Console.WriteLine("=== Phase 8: Final results ===");
        BenchmarkConsoleReporter.PrintFinalReport(result, config);
        writeStructuredResultArtifacts(result, config);
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

    internal static BackFillerRuntimeOptions BuildRuntimeOptions(TransitBenchmarkConfig config)
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
}
