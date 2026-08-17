using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal sealed class MeasurementMetrics
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
        LatencyAggregators.UpdatePeak(ref _publishTicksMax, publishTicks);
        LatencyAggregators.UpdateMin(ref _publishTicksMin, publishTicks);

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
            LatencyAggregators.UpdatePeak(ref _dispatchQueueWaitTicksMax, dispatchWaitTicks);
        }

        if (hasSocketWrite)
        {
            Interlocked.Add(ref _socketWriteTicksTotal, socketWriteTicks);
            Interlocked.Increment(ref _socketWriteSampleCount);
            LatencyAggregators.UpdatePeak(ref _socketWriteTicksMax, socketWriteTicks);
        }

        if (hasResponseWait)
        {
            Interlocked.Add(ref _responseWaitTicksTotal, responseWaitTicks);
            Interlocked.Increment(ref _responseWaitSampleCount);
            LatencyAggregators.UpdatePeak(ref _responseWaitTicksMax, responseWaitTicks);
        }

        if (hasParseCorrelation)
        {
            Interlocked.Add(ref _parseCorrelationTicksTotal, parseCorrelationTicks);
            Interlocked.Increment(ref _parseCorrelationSampleCount);
            LatencyAggregators.UpdatePeak(ref _parseCorrelationTicksMax, parseCorrelationTicks);
        }

        if (hasTotal)
        {
            Interlocked.Add(ref _totalPublishTicksTotal, totalPublishTicks);
            Interlocked.Increment(ref _totalPublishSampleCount);
            LatencyAggregators.UpdatePeak(ref _totalPublishTicksMax, totalPublishTicks);
        }

        int submitBucket = MetricMathHelpers.ClassifyDepthBucket(pendingAtSubmit);
        int completeBucket = MetricMathHelpers.ClassifyDepthBucket(pendingAtComplete);

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
        LatencyAggregators.UpdatePeak(ref _peakQueueDepth, queueDepth);
        LatencyAggregators.UpdatePeak(ref _peakQueueBytes, queueBytes);
        LatencyAggregators.UpdatePeak(ref _peakInFlight, inFlight);
        LatencyAggregators.UpdateMin(ref _minQueueDepth, queueDepth);
        LatencyAggregators.UpdateMin(ref _minQueueBytes, queueBytes);
        Interlocked.Increment(ref _queueDepthSampleCount);
        Interlocked.Add(ref _queueDepthSampleSum, queueDepth);
        Interlocked.Add(ref _queueBytesSampleSum, queueBytes);
    }

    internal void ObserveActualPending(int actualPending)
    {
        LatencyAggregators.UpdatePeak(ref _peakActualPending, actualPending);
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
                AverageDispatchQueueWaitUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _dispatchQueueWaitTicksTotal) / (double)dispatchWaitSampleCount),
                P50DispatchQueueWaitUs: MetricMathHelpers.PercentileUs(_dispatchWaitTicksSamples, 0.50),
                P95DispatchQueueWaitUs: MetricMathHelpers.PercentileUs(_dispatchWaitTicksSamples, 0.95),
                P99DispatchQueueWaitUs: MetricMathHelpers.PercentileUs(_dispatchWaitTicksSamples, 0.99),
                MaxDispatchQueueWaitUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _dispatchQueueWaitTicksMax)),
                DispatchQueueWaitSampleCount: Interlocked.Read(ref _dispatchQueueWaitSampleCount),
                AverageSocketWriteUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _socketWriteTicksTotal) / (double)socketWriteSampleCount),
                P50SocketWriteUs: MetricMathHelpers.PercentileUs(_socketWriteTicksSamples, 0.50),
                P95SocketWriteUs: MetricMathHelpers.PercentileUs(_socketWriteTicksSamples, 0.95),
                P99SocketWriteUs: MetricMathHelpers.PercentileUs(_socketWriteTicksSamples, 0.99),
                MaxSocketWriteUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _socketWriteTicksMax)),
                SocketWriteSampleCount: Interlocked.Read(ref _socketWriteSampleCount),
                AverageResponseWaitUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _responseWaitTicksTotal) / (double)responseWaitSampleCount),
                P50ResponseWaitUs: MetricMathHelpers.PercentileUs(_responseWaitTicksSamples, 0.50),
                P95ResponseWaitUs: MetricMathHelpers.PercentileUs(_responseWaitTicksSamples, 0.95),
                P99ResponseWaitUs: MetricMathHelpers.PercentileUs(_responseWaitTicksSamples, 0.99),
                MaxResponseWaitUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _responseWaitTicksMax)),
                ResponseWaitSampleCount: Interlocked.Read(ref _responseWaitSampleCount),
                AverageParseCorrelationUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _parseCorrelationTicksTotal) / (double)parseCorrelationSampleCount),
                P50ParseCorrelationUs: MetricMathHelpers.PercentileUs(_parseCorrelationTicksSamples, 0.50),
                P95ParseCorrelationUs: MetricMathHelpers.PercentileUs(_parseCorrelationTicksSamples, 0.95),
                P99ParseCorrelationUs: MetricMathHelpers.PercentileUs(_parseCorrelationTicksSamples, 0.99),
                MaxParseCorrelationUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _parseCorrelationTicksMax)),
                ParseCorrelationSampleCount: Interlocked.Read(ref _parseCorrelationSampleCount),
                AverageTotalPublishLatencyUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _totalPublishTicksTotal) / (double)totalPublishSampleCount),
                P50TotalPublishLatencyUs: MetricMathHelpers.PercentileUs(_totalPublishTicksSamples, 0.50),
                P95TotalPublishLatencyUs: MetricMathHelpers.PercentileUs(_totalPublishTicksSamples, 0.95),
                P99TotalPublishLatencyUs: MetricMathHelpers.PercentileUs(_totalPublishTicksSamples, 0.99),
                MaxTotalPublishLatencyUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _totalPublishTicksMax)),
                TotalPublishLatencySampleCount: Interlocked.Read(ref _totalPublishSampleCount),
                AveragePublishLatencyUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _publishTicksTotal) / (double)publishCount),
                MinPublishLatencyUs: MetricMathHelpers.TicksToUs(publishMinTicks),
                P50PublishLatencyUs: MetricMathHelpers.PercentileUs(_publishTicksSamples, 0.50),
                P95PublishLatencyUs: MetricMathHelpers.PercentileUs(_publishTicksSamples, 0.95),
                P99PublishLatencyUs: MetricMathHelpers.PercentileUs(_publishTicksSamples, 0.99),
                MaxPublishLatencyUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _publishTicksMax)),
                AverageLifecycleLatencyUs: MetricMathHelpers.TicksToUs(Interlocked.Read(ref _lifecycleTicksTotal) / (double)publishCount),
                PendingDepthLatencyBuckets: FormatHelpers.BuildDepthBucketSummary(_publishBySubmitDepthBucket, _publishByCompleteDepthBucket),
                ForensicSampleCount: _forensicSampleCount,
                ConnectionTimeSeriesSummary: LatencyAggregators.BuildConnectionSeriesSummary(_connectionSeries),
                DispatcherTimeSeriesSummary: LatencyAggregators.BuildDispatcherSeriesSummary(_dispatcherSeries),
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
            MinQueueDepth: MetricMathHelpers.NormalizeMin(_minQueueDepth),
            MinQueueBytes: MetricMathHelpers.NormalizeMin(_minQueueBytes),
            AverageQueueDepth: MetricMathHelpers.ComputeAverage(_queueDepthSampleSum, _queueDepthSampleCount),
            AverageQueueBytes: MetricMathHelpers.ComputeAverage(_queueBytesSampleSum, _queueDepthSampleCount),
            ProducerQueueWaitTicks: Interlocked.Read(ref _producerQueueWaitTicks),
            ArticleBytes: _articleBytes);
    }
}
