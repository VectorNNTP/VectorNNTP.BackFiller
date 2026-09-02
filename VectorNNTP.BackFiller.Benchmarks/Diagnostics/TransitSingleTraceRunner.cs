// <copyright file="TransitSingleTraceRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Diagnostics/TransitSingleTraceRunner: provides focused diagnostic execution and logging for transit benchmarks.

using System.Net;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the transit SingleTraceRunner class for benchmark or isolated-regression execution.
/// </summary>
internal static class TransitSingleTraceRunner
{
    /// <summary>
    /// Gets or sets the small ArticleMinBytes value.
    /// </summary>
    private const int SmallArticleMinBytes = 64;
    /// <summary>
    /// Gets or sets the small ArticleMaxBytes value.
    /// </summary>
    private const int SmallArticleMaxBytes = 1023;
    /// <summary>
    /// Gets or sets the large ArticleMinBytes value.
    /// </summary>
    private const int LargeArticleMinBytes = 1_048_577;
    /// <summary>
    /// Gets or sets the large ArticleMaxBytes value.
    /// </summary>
    private const int LargeArticleMaxBytes = 2_097_151;

    /// <summary>
    /// Defines the i TransitSingleTracePublishExecutor interface for benchmark or isolated-regression execution.
    /// </summary>
    internal interface ITransitSingleTracePublishExecutor
    {
        ValueTask<TransitPublishResult> PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Defines the single TracePublishBatchResult record for benchmark or isolated-regression execution.
    /// </summary>
    internal sealed record SingleTracePublishBatchResult(
        IReadOnlyList<string> MessageIds,
        IReadOnlyList<TransitPublishResult> PublishResults,
        int TimeoutCount,
        IReadOnlyList<SingleTraceArticleDescriptor> Articles);

    /// <summary>
    /// Defines the single TraceArticleDescriptor record for benchmark or isolated-regression execution.
    /// </summary>
    internal sealed record SingleTraceArticleDescriptor(
        int ArticleIndex,
        string MessageId,
        int ArticleSizeBytes,
        string SizeClass);

    /// <summary>
    /// Defines the transit PublisherSingleTracePublishExecutor class for benchmark or isolated-regression execution.
    /// </summary>
    private sealed class TransitPublisherSingleTracePublishExecutor : ITransitSingleTracePublishExecutor
    {
        /// <summary>
        /// Gets or sets the _publisher value.
        /// </summary>
        private readonly TransitPublisher _publisher;

        /// <summary>
        /// Performs the transit PublisherSingleTracePublishExecutor operation.
        /// </summary>
        internal TransitPublisherSingleTracePublishExecutor(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);
            _publisher = publisher;
        }

        /// <summary>
        /// Performs the publish Async operation.
        /// </summary>
        public ValueTask<TransitPublishResult> PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)
        {
            return _publisher.PublishAsync(messageId, articlePayload, cancellationToken);
        }
    }

    /// <summary>
    /// Performs the resolve RequestedArticleCount operation.
    /// </summary>
    internal static int ResolveRequestedArticleCount(int? measurementArticleCount)
    {
        return measurementArticleCount ?? 1;
    }

    /// <summary>
    /// Performs the publish SequentiallyAsync operation.
    /// </summary>
    internal static Task<SingleTracePublishBatchResult> PublishSequentiallyAsync(
        ITransitSingleTracePublishExecutor publishExecutor,
        int requestedArticleCount,
        int articleTargetBytes,
        Action<string> writeLine,
        CancellationToken cancellationToken)
    {
        return PublishWithPipelineDepthAsync(
            publishExecutor,
            requestedArticleCount,
            articleTargetBytes,
            effectivePipelineDepth: 1,
            writeLine,
            cancellationToken);
    }

    /// <summary>
    /// Performs the publish WithPipelineDepthAsync operation.
    /// </summary>
    internal static async Task<SingleTracePublishBatchResult> PublishWithPipelineDepthAsync(
        ITransitSingleTracePublishExecutor publishExecutor,
        int requestedArticleCount,
        int articleTargetBytes,
        int effectivePipelineDepth,
        Action<string> writeLine,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(publishExecutor);
        ArgumentNullException.ThrowIfNull(writeLine);

        if (requestedArticleCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(requestedArticleCount), requestedArticleCount, "Requested article count must be zero or greater.");
        }

        if (effectivePipelineDepth <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(effectivePipelineDepth), effectivePipelineDepth, "Pipeline depth must be greater than zero.");
        }

        List<string> publishedMessageIds = new(requestedArticleCount);
        List<SingleTraceArticleDescriptor> articleDescriptors = new(requestedArticleCount);
        TransitPublishResult?[] publishResultsByArticle = new TransitPublishResult[requestedArticleCount];
        int timeoutCount = 0;
        Random random = Random.Shared;

        int boundedDepth = Math.Min(effectivePipelineDepth, Math.Max(1, requestedArticleCount));
        List<OutstandingPublishOperation> outstanding = new(boundedDepth);
        int nextArticleIndex = 1;

        while (nextArticleIndex <= requestedArticleCount || outstanding.Count > 0)
        {
            while (nextArticleIndex <= requestedArticleCount && outstanding.Count < boundedDepth)
            {
                int articleIndex = nextArticleIndex;
                nextArticleIndex++;

                string messageId = $"<single-trace-{articleIndex:D4}-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}@benchmark.usenet.ninja>";
                publishedMessageIds.Add(messageId);
                writeLine($"TRACE_MESSAGE_ID[{articleIndex}/{requestedArticleCount}]: {messageId}");

                (int articleSizeBytes, string sizeClass) = GenerateRandomArticleSize(random);
                articleDescriptors.Add(new SingleTraceArticleDescriptor(articleIndex, messageId, articleSizeBytes, sizeClass));
                writeLine($"TRACE_ARTICLE_SIZE[{articleIndex}/{requestedArticleCount}]: MessageId={messageId}, SizeBytes={articleSizeBytes}, SizeClass={sizeClass}");

                TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(messageId, articleSizeBytes);
                DateTimeOffset submitStartUtc = DateTimeOffset.UtcNow;
                writeLine($"TRACE_SUBMIT_START_UTC[{articleIndex}/{requestedArticleCount}]: {submitStartUtc:O}");

                CancellationTokenSource timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(90));

                Task<PublishCompletion> publishTask = ExecutePublishAsync(
                    publishExecutor,
                    articleIndex,
                    requestedArticleCount,
                    messageId,
                    payload,
                    timeoutCts,
                    writeLine,
                    cancellationToken);

                outstanding.Add(new OutstandingPublishOperation(articleIndex, publishTask));
            }

            if (outstanding.Count == 0)
            {
                break;
            }

            int completedIndex = -1;
            for (int i = 0; i < outstanding.Count; i++)
            {
                if (outstanding[i].Task.IsCompleted)
                {
                    completedIndex = i;
                    break;
                }
            }

            if (completedIndex < 0)
            {
                Task<PublishCompletion>[] pending = new Task<PublishCompletion>[outstanding.Count];
                for (int i = 0; i < outstanding.Count; i++)
                {
                    pending[i] = outstanding[i].Task;
                }

                Task<PublishCompletion> completedTask = await Task.WhenAny(pending).ConfigureAwait(false);
                for (int i = 0; i < outstanding.Count; i++)
                {
                    if (ReferenceEquals(outstanding[i].Task, completedTask))
                    {
                        completedIndex = i;
                        break;
                    }
                }

                if (completedIndex < 0)
                {
                    throw new InvalidOperationException("Unable to resolve completed single-trace publish task.");
                }
            }

            OutstandingPublishOperation completedOperation = outstanding[completedIndex];
            int lastIndex = outstanding.Count - 1;
            outstanding[completedIndex] = outstanding[lastIndex];
            outstanding.RemoveAt(lastIndex);

            PublishCompletion completion = await completedOperation.Task.ConfigureAwait(false);
            if (completion.TimedOut)
            {
                timeoutCount++;
            }
            else
            {
                publishResultsByArticle[completion.ArticleIndex - 1] = completion.Result;
            }
        }

        List<TransitPublishResult> publishResults = new(requestedArticleCount - timeoutCount);
        for (int i = 0; i < publishResultsByArticle.Length; i++)
        {
            TransitPublishResult? result = publishResultsByArticle[i];
            if (result is not null)
            {
                publishResults.Add(result);
            }
        }

        return new SingleTracePublishBatchResult(publishedMessageIds, publishResults, timeoutCount, articleDescriptors);
    }

    /// <summary>
    /// Performs the generate RandomArticleSize operation.
    /// </summary>
    private static (int ArticleSizeBytes, string SizeClass) GenerateRandomArticleSize(Random random)
    {
        ArgumentNullException.ThrowIfNull(random);

        bool useSmall = random.Next(2) == 0;
        if (useSmall)
        {
            int smallBytes = random.Next(SmallArticleMinBytes, SmallArticleMaxBytes + 1);
            return (smallBytes, "SMALL");
        }

        int largeBytes = random.Next(LargeArticleMinBytes, LargeArticleMaxBytes + 1);
        return (largeBytes, "LARGE");
    }

    /// <summary>
    /// Performs the execute PublishAsync operation.
    /// </summary>
    private static async Task<PublishCompletion> ExecutePublishAsync(
        ITransitSingleTracePublishExecutor publishExecutor,
        int articleIndex,
        int requestedArticleCount,
        string messageId,
        TransitBenchmarkCore.ArticlePayload payload,
        CancellationTokenSource timeoutCts,
        Action<string> writeLine,
        CancellationToken benchmarkCancellationToken)
    {
        try
        {
            TransitPublishResult publishResult = await publishExecutor.PublishAsync(messageId, payload.AsMemory(), timeoutCts.Token).ConfigureAwait(false);
            DateTimeOffset submitEndUtc = DateTimeOffset.UtcNow;
            writeLine($"TRACE_SUBMIT_END_UTC[{articleIndex}/{requestedArticleCount}]: {submitEndUtc:O}");
            writeLine($"TRACE_PUBLISH_RESULT[{articleIndex}/{requestedArticleCount}]: MessageId={messageId}, Status={publishResult.Status}, Code={publishResult.ResponseCode}, Text={publishResult.ResponseText}");
            return new PublishCompletion(articleIndex, publishResult, TimedOut: false);
        }
        catch (OperationCanceledException) when (!benchmarkCancellationToken.IsCancellationRequested)
        {
            DateTimeOffset timeoutUtc = DateTimeOffset.UtcNow;
            writeLine($"TRACE_TIMEOUT_UTC[{articleIndex}/{requestedArticleCount}]: {timeoutUtc:O}");
            writeLine($"TRACE_PUBLISH_RESULT[{articleIndex}/{requestedArticleCount}]: MessageId={messageId}, TIMED_OUT");
            return new PublishCompletion(articleIndex, Result: null, TimedOut: true);
        }
        finally
        {
            timeoutCts.Dispose();
            payload.Dispose();
        }
    }

    /// <summary>
    /// Defines the outstanding PublishOperation record struct for benchmark or isolated-regression execution.
    /// </summary>
    private readonly record struct OutstandingPublishOperation(int ArticleIndex, Task<PublishCompletion> Task);

    /// <summary>
    /// Defines the publish Completion record struct for benchmark or isolated-regression execution.
    /// </summary>
    private readonly record struct PublishCompletion(int ArticleIndex, TransitPublishResult? Result, bool TimedOut);

    /// <summary>
    /// Performs the run Async operation.
    /// </summary>
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
        Console.WriteLine($"Target article bytes: variable-per-article (SMALL: 64-1023, LARGE: 1048577-2097151)");
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

        SingleTracePublishBatchResult publishBatch = await PublishWithPipelineDepthAsync(
            /// <summary>
            /// Performs the transit PublisherSingleTracePublishExecutor operation.
            /// </summary>
            new TransitPublisherSingleTracePublishExecutor(publisher),
            requestedArticleCount,
            config.ArticleTargetBytes,
            config.PerConnectionPipelineDepth,
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

        Console.WriteLine("TRACE_ARTICLE_SIZE_TABLE:");
        foreach (SingleTraceArticleDescriptor article in publishBatch.Articles.OrderBy(static entry => entry.ArticleIndex))
        {
            Console.WriteLine($"  Index={article.ArticleIndex}, MessageId={article.MessageId}, SizeBytes={article.ArticleSizeBytes}, SizeClass={article.SizeClass}");
        }

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
