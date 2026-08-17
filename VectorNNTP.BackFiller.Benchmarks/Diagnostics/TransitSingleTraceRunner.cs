using System.Net;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class TransitSingleTraceRunner
{
    internal static async Task RunAsync(
        TransitBenchmarkCliOptions cliOptions,
        int validationSeconds,
        RuntimeExecutionIdentity runtimeIdentity,
        Func<ILoggerFactory, ILogger<TransitPublisher>> createTransitPublisherLogger,
        CancellationToken cancellationToken = default)
    {
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(validationSeconds), BenchmarkMode.Validation, cliOptions);

        RuntimeIdentityGuard.EnsureMatches(config.ExpectedRuntimeIdentity, runtimeIdentity);

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

        ILogger<TransitPublisher> transitPublisherLogger = createTransitPublisherLogger(loggerFactory);

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
}
