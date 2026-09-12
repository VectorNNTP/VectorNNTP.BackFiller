// <copyright file="BenchmarkConsoleReporter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Reporting/BenchmarkConsoleReporter: formats benchmark measurements and topology details for operators and artifacts.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the benchmark ConsoleReporter class used by the benchmark or regression gate.
/// </summary>
internal static class BenchmarkConsoleReporter
{
    /// <summary>
    /// Implements the print FinalReport contract.
    /// </summary>
    internal static void PrintFinalReport(BenchmarkResult result, TransitBenchmarkConfig config)
    {
        Console.WriteLine($"Benchmark Build Version: {result.BenchmarkBuildVersion}");
        Console.WriteLine($"Endpoint type: {config.EndpointType}");
        Console.WriteLine($"Endpoint identity: {config.EndpointIdentity}");
        Console.WriteLine($"Endpoint host: {config.EndpointHost}");
        Console.WriteLine($"Endpoint port: {config.EndpointPort}");
        Console.WriteLine($"Production path exercised: Benchmark -> REAL TransitPublisher -> REAL TransitConnection -> TLS -> MODE STREAM -> TransitServer");
        Console.WriteLine("Offered throughput = bytes successfully offered to the benchmark queue during the measurement window.");
        Console.WriteLine("Admitted throughput = bytes admitted by dispatcher workers into TransitPublisher PublishAsync submissions.");
        Console.WriteLine("Accepted throughput = within-window bytes mapped to definitive TransitServer success responses (e.g., 239).");
        Console.WriteLine("Offered/admitted throughput must not be interpreted as socket-wire throughput.");
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
        Console.WriteLine($"Configured measurement duration: {config.MeasurementDuration.TotalSeconds:F0}s");
        Console.WriteLine($"Measured window duration: {result.Boundary.MeasurementWindowDuration.TotalSeconds:F6}s");
        Console.WriteLine($"Measurement article count: {(config.MeasurementArticleCount?.ToString() ?? "(duration-driven)")}");
        Console.WriteLine($"Drain duration: {result.DrainDuration.TotalSeconds:F3}s");
        Console.WriteLine($"Outstanding at measurement end: {result.OutstandingAtMeasurementEnd}");
        Console.WriteLine($"Drained after measurement: {result.DrainedAfterMeasurement}");

        Console.WriteLine();
        Console.WriteLine($"Measurement start UTC: {result.Boundary.MeasurementStartUtc:O}");
        Console.WriteLine($"Measurement end UTC:   {result.Boundary.MeasurementEndUtc:O}");

        Console.WriteLine();
        Console.WriteLine($"Offered articles: {result.OfferedArticles}");
        Console.WriteLine($"Offered bytes: {result.OfferedBytes}");
        Console.WriteLine($"Offered Gbps: {result.OfferedGbps:F4}");
        Console.WriteLine($"Offered within-window articles: {result.OfferedWithinWindowArticles}");
        Console.WriteLine($"Offered within-window Gbps: {result.OfferedWithinWindowGbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"Admitted articles: {result.AdmittedArticles}");
        Console.WriteLine($"Admitted bytes: {result.AdmittedBytes}");
        Console.WriteLine($"Admitted Gbps: {result.AdmittedGbps:F4}");
        Console.WriteLine($"Admitted within-window articles: {result.AdmittedWithinWindowArticles}");
        Console.WriteLine($"Admitted within-window Gbps: {result.AdmittedWithinWindowGbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"Accepted articles: {result.AcceptedArticles}");
        Console.WriteLine($"Accepted bytes: {result.AcceptedBytes}");
        Console.WriteLine($"Accepted Gbps (within-window accepted / measurement window): {result.AcceptedGbps:F4}");
        Console.WriteLine($"Accepted within-window articles: {result.AcceptedWithinWindowArticles}");
        Console.WriteLine($"Accepted within-window bytes: {result.AcceptedWithinWindowBytes}");
        Console.WriteLine($"Accepted within-window Gbps: {result.AcceptedWithinWindowGbps:F4}");
        Console.WriteLine($"Accepted post-measurement articles: {result.AcceptedPostMeasurementArticles}");
        Console.WriteLine($"Accepted post-measurement bytes: {result.AcceptedPostMeasurementBytes}");
        Console.WriteLine($"Accepted Gbps (drain-inclusive): {result.DrainInclusiveAcceptedGbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"Rejected articles: {result.RejectedArticles}");
        Console.WriteLine($"Rejected within-window articles: {result.RejectedWithinWindowArticles}");
        Console.WriteLine($"Ambiguous articles: {result.AmbiguousArticles}");
        Console.WriteLine($"Ambiguous within-window articles: {result.AmbiguousWithinWindowArticles}");
        Console.WriteLine($"Failed articles: {result.FailedArticles}");
        Console.WriteLine($"Failed within-window articles: {result.FailedWithinWindowArticles}");
        Console.WriteLine($"Unavailable articles: {result.UnavailableArticles}");
        Console.WriteLine($"Unavailable within-window articles: {result.UnavailableWithinWindowArticles}");
        Console.WriteLine($"Canceled articles: {result.CanceledArticles}");
        Console.WriteLine($"Canceled within-window articles: {result.CanceledWithinWindowArticles}");
        Console.WriteLine($"Completed within-window articles: {result.CompletedWithinWindowArticles}");
        Console.WriteLine($"Completed post-measurement articles: {result.CompletedPostMeasurementArticles}");

        Console.WriteLine();
        Console.WriteLine($"Queue target depth (articles): {config.ProducerQueueTargetArticles}");
        Console.WriteLine($"Queue depth samples: {result.QueueDepthSampleCount}");
        if (result.QueueDepthSampleCount == 0)
        {
            Console.WriteLine("Queue minimum depth: (no samples)");
            Console.WriteLine("Queue average depth: (no samples)");
            Console.WriteLine("Queue average bytes: (no samples)");
        }
        else
        {
            Console.WriteLine($"Queue minimum depth: {result.MinQueueDepth}");
            Console.WriteLine($"Queue average depth: {result.AverageQueueDepth:F2}");
            Console.WriteLine($"Queue average bytes: {result.AverageQueuedBytes:F0}");
        }

        Console.WriteLine($"Queue peak depth: {result.PeakQueueDepth}");
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
        Console.WriteLine($"Benchmark measurement start UTC: {result.Boundary.MeasurementStartUtc:O}");
        Console.WriteLine($"Benchmark measurement end UTC:   {result.Boundary.MeasurementEndUtc:O}");
        Console.WriteLine($"Benchmark accepted articles (all terminalized): {result.AcceptedArticles}");
        Console.WriteLine($"Benchmark accepted articles (within-window): {result.AcceptedWithinWindowArticles}");
        Console.WriteLine($"Benchmark rejected articles: {result.RejectedArticles}");
        Console.WriteLine($"Benchmark ambiguous articles: {result.AmbiguousArticles}");
        Console.WriteLine($"Benchmark elapsed seconds (monotonic): {result.Boundary.MeasurementWindowDuration.TotalSeconds:F4}");
        Console.WriteLine($"Benchmark accepted articles/sec (within-window): {result.AcceptedWithinWindowArticles / Math.Max(0.000001d, result.Boundary.MeasurementWindowDuration.TotalSeconds):F4}");
        Console.WriteLine("TransitServer spool/throughput counters may use different reporting windows and aggregation cadence (for example rolling 60-second windows). Compare by timestamps, not by assuming identical window boundaries.");
    }
}
