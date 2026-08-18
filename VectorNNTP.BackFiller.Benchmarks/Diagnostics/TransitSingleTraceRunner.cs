using System.Net;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class TransitSingleTraceRunner
{
    internal interface ITransitSingleTracePublishExecutor
    {
        ValueTask<TransitPublishResult> PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken);
    }

    internal sealed record SingleTracePublishBatchResult(
        IReadOnlyList<string> MessageIds,
        IReadOnlyList<TransitPublishResult> PublishResults,
        int TimeoutCount);

    private sealed class TransitPublisherSingleTracePublishExecutor : ITransitSingleTracePublishExecutor
    {
        private readonly TransitPublisher _publisher;

        internal TransitPublisherSingleTracePublishExecutor(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);
            _publisher = publisher;
        }

        public ValueTask<TransitPublishResult> PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)
        {
            return _publisher.PublishAsync(messageId, articlePayload, cancellationToken);
        }
    }

    internal static int ResolveRequestedArticleCount(int? measurementArticleCount)
    {
        return measurementArticleCount ?? 1;
    }

    internal static async Task<SingleTracePublishBatchResult> PublishSequentiallyAsync(
        ITransitSingleTracePublishExecutor publishExecutor,
        int requestedArticleCount,
        int articleTargetBytes,
        Action<string> writeLine,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(publishExecutor);
        ArgumentNullException.ThrowIfNull(writeLine);

        List<string> publishedMessageIds = new(requestedArticleCount);
        List<TransitPublishResult> publishResults = new(requestedArticleCount);
        int timeoutCount = 0;

        for (int articleIndex = 1; articleIndex <= requestedArticleCount; articleIndex++)
        {
            string messageId = $"<single-trace-{articleIndex:D4}-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}@benchmark.usenet.ninja>";
            publishedMessageIds.Add(messageId);
            writeLine($"TRACE_MESSAGE_ID[{articleIndex}/{requestedArticleCount}]: {messageId}");

            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(messageId, articleTargetBytes);
            DateTimeOffset submitStartUtc = DateTimeOffset.UtcNow;
            writeLine($"TRACE_SUBMIT_START_UTC[{articleIndex}/{requestedArticleCount}]: {submitStartUtc:O}");

            try
            {
                using CancellationTokenSource timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(90));

                TransitPublishResult publishResult = await publishExecutor.PublishAsync(messageId, payload.AsMemory(), timeoutCts.Token).ConfigureAwait(false);
                publishResults.Add(publishResult);

                DateTimeOffset submitEndUtc = DateTimeOffset.UtcNow;
                writeLine($"TRACE_SUBMIT_END_UTC[{articleIndex}/{requestedArticleCount}]: {submitEndUtc:O}");
                writeLine($"TRACE_PUBLISH_RESULT[{articleIndex}/{requestedArticleCount}]: MessageId={messageId}, Status={publishResult.Status}, Code={publishResult.ResponseCode}, Text={publishResult.ResponseText}");
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                timeoutCount++;
                DateTimeOffset timeoutUtc = DateTimeOffset.UtcNow;
                writeLine($"TRACE_TIMEOUT_UTC[{articleIndex}/{requestedArticleCount}]: {timeoutUtc:O}");
                writeLine($"TRACE_PUBLISH_RESULT[{articleIndex}/{requestedArticleCount}]: MessageId={messageId}, TIMED_OUT");
            }
            finally
            {
                payload.Dispose();
            }
        }

        return new SingleTracePublishBatchResult(publishedMessageIds, publishResults, timeoutCount);
    }

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
        int requestedArticleCount = ResolveRequestedArticleCount(config.MeasurementArticleCount);
        Console.WriteLine($"Requested article count: {requestedArticleCount}");
        Console.WriteLine($"RuntimeAssemblyPath: {runtimeIdentity.RuntimeAssemblyPath}");
        Console.WriteLine($"RuntimeAssemblyVersion: {runtimeIdentity.RuntimeAssemblyVersion}");
        Console.WriteLine($"AssemblyFileVersion: {runtimeIdentity.AssemblyFileVersion ?? "(unknown)"}");
        Console.WriteLine($"ProcessPath: {runtimeIdentity.ProcessPath}");
        Console.WriteLine($"TargetFramework: {runtimeIdentity.TargetFramework ?? "(unknown)"}");
        Console.WriteLine($"RuntimeIdentifier: {runtimeIdentity.RuntimeIdentifier ?? "(unknown)"}");
        Console.WriteLine($"Architecture: {runtimeIdentity.Architecture}");
        Console.WriteLine($"ProductionDependencyPath: {runtimeIdentity.ProductionDependencyPath ?? "(unknown)"}");
        Console.WriteLine($"ProductionDependencyAssemblyVersion: {runtimeIdentity.ProductionDependencyAssemblyVersion ?? "(unknown)"}");
        Console.WriteLine($"ProductionDependencyFileVersion: {runtimeIdentity.ProductionDependencyFileVersion ?? "(unknown)"}");

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

        SingleTracePublishBatchResult publishBatch = await PublishSequentiallyAsync(
            new TransitPublisherSingleTracePublishExecutor(publisher),
            requestedArticleCount,
            config.ArticleTargetBytes,
            Console.WriteLine,
            cancellationToken).ConfigureAwait(false);

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

        long publishCompletedCount = publishBatch.PublishResults.Count;
        long publishAcceptedCount = publishBatch.PublishResults.LongCount(static result => result.Status == TransitPublishStatus.Accepted);
        long publishRejectedCount = publishBatch.PublishResults.LongCount(static result => result.Status == TransitPublishStatus.Rejected);
        long publishFailedCount = publishBatch.PublishResults.LongCount(static result => result.Status == TransitPublishStatus.Failed);
        long publishAmbiguousCount = publishBatch.PublishResults.LongCount(static result => result.Status == TransitPublishStatus.Ambiguous);
        long publishUnavailableCount = publishBatch.PublishResults.LongCount(static result => result.Status == TransitPublishStatus.Unavailable);
        string publishResponseCodes = publishBatch.PublishResults.Count == 0
            ? "(none)"
            : string.Join(",", publishBatch.PublishResults.Select(static result => result.ResponseCode?.ToString() ?? "(none)"));

        Console.WriteLine("TRACE_BACKFILLER_SUMMARY:");
        Console.WriteLine($"  RequestedArticleCount={requestedArticleCount}");
        Console.WriteLine($"  SubmittedArticleCount={publishBatch.MessageIds.Count}");
        Console.WriteLine($"  PublishCompletedCount={publishCompletedCount}");
        Console.WriteLine($"  PublishAcceptedCount={publishAcceptedCount}");
        Console.WriteLine($"  PublishRejectedCount={publishRejectedCount}");
        Console.WriteLine($"  PublishFailedCount={publishFailedCount}");
        Console.WriteLine($"  PublishAmbiguousCount={publishAmbiguousCount}");
        Console.WriteLine($"  PublishUnavailableCount={publishUnavailableCount}");
        Console.WriteLine($"  PublishTimeoutCount={publishBatch.TimeoutCount}");
        Console.WriteLine($"  PublishResponseCodes={publishResponseCodes}");
        Console.WriteLine($"  LastMessageId={(publishBatch.MessageIds.Count == 0 ? "(none)" : publishBatch.MessageIds[^1])}");
        Console.WriteLine($"  Totals: Started={totalStarted}, Accepted={totalAccepted}, Rejected={totalRejected}, Ambiguous={totalAmbiguous}, Failed={totalFailed}, Unavailable={totalUnavailable}");
        Console.WriteLine($"  CurrentOutstanding={totalCurrentOutstanding}");
        Console.WriteLine($"  PeakOutstandingPerConnection={peakOutstandingPerConnection}");
    }
}
