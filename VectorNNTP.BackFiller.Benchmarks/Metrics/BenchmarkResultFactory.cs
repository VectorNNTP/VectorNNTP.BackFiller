using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class BenchmarkResultFactory
{
    internal static BenchmarkResult Create(
        TransitBenchmarkConfig config,
        RuntimeExecutionIdentity runtimeIdentity,
        string benchmarkBuildVersion,
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
            BenchmarkBuildVersion: benchmarkBuildVersion,
            RuntimeIdentity: runtimeIdentity,
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
