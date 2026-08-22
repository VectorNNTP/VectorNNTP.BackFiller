using System.Collections.Concurrent;
using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Low-overhead, opt-in forensic recorder for the dispatch consumer queue-read path.
/// </summary>
/// <remarks>
/// The recorder is instrumentation only: it never changes queue semantics, consumer count, batching,
/// scheduling or socket behaviour. Full managed stacks are captured for a small, bounded, representative
/// sample of wait episodes so that the measurement is not materially distorted.
/// </remarks>
internal sealed class QueueConsumerForensics
{
    internal const int DefaultLongWaitThresholdMilliseconds = 10;
    private const int EnqueueRingCapacity = 4096;
    private const int MaxStackSamplesPerBucket = 4;
    private const int MaxExportedLongWaits = 20;
    private const int MaxRecordedTryReadFailures = 4096;
    private const int MaxIntervalSamples = 50_000;
    private const int MaxStateCensuses = 64;

    private readonly QueueConsumerProbe[] _probes;
    private readonly int[] _waiterBuckets;
    private readonly int[] _bucketSampleCounts;
    private readonly EnqueueRecord[] _enqueueRing = new EnqueueRecord[EnqueueRingCapacity];
    private readonly ConcurrentQueue<QueueConsumerStackSample> _stackSamples = new();
    private readonly ConcurrentQueue<LongWaitRecord> _longWaits = new();
    private readonly ConcurrentQueue<TryReadFailureRecord> _tryReadFailures = new();
    private readonly ConcurrentQueue<ConsumerStateCensus> _stateCensuses = new();
    private readonly object _intervalGate = new();
    private readonly List<long> _intervalATicks = [];
    private readonly List<long> _intervalBTicks = [];
    private readonly List<long> _intervalC0Ticks = [];
    private readonly List<long> _intervalCTicks = [];
    private readonly List<long> _intervalDTicks = [];
    private readonly List<long> _intervalETicks = [];
    private readonly List<long> _totalWaitTicks = [];
    private readonly long _startTicks;
    private readonly DateTimeOffset _startUtc;
    private readonly long _longWaitThresholdTicks;

    private long _enqueueSequence;
    private long _waitEpisodeCount;
    private long _waitEpisodesCompletedSynchronously;
    private long _longWaitCount;
    private long _recordedTryReadFailureCount;
    private long _tryReadAttempts;
    private long _tryReadSuccesses;
    private long _tryReadFailuresClassA;
    private long _tryReadFailuresClassB;
    private long _tryReadFailuresClassC;
    private long _tryReadFailuresClassD;
    private long _threadHopsAcrossWait;
    private long _consumerOwnedArticles;
    private int _currentWaiters;
    private int _maxConcurrentWaiters;
    private int _currentTryReads;
    private int _maxConcurrentTryReads;
    private int _stateCensusCount;

    /// <summary>
    /// Initializes a new instance of the <see cref="QueueConsumerForensics"/> class.
    /// </summary>
    /// <param name="consumerCount">Number of dispatch consumers that will be instrumented.</param>
    /// <param name="longWaitThresholdMilliseconds">Wait duration above which an episode is recorded in full detail.</param>
    internal QueueConsumerForensics(int consumerCount, int longWaitThresholdMilliseconds = DefaultLongWaitThresholdMilliseconds)
    {
        if (consumerCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(consumerCount), consumerCount, "Consumer count must be greater than zero.");
        }

        if (longWaitThresholdMilliseconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(longWaitThresholdMilliseconds), longWaitThresholdMilliseconds, "Long wait threshold must be greater than zero.");
        }

        ConsumerCount = consumerCount;
        LongWaitThresholdMilliseconds = longWaitThresholdMilliseconds;
        _longWaitThresholdTicks = (long)(Stopwatch.Frequency * (longWaitThresholdMilliseconds / 1000d));
        _waiterBuckets = BuildWaiterBuckets(consumerCount);
        _bucketSampleCounts = new int[_waiterBuckets.Length];
        _probes = new QueueConsumerProbe[consumerCount];
        for (int consumerId = 0; consumerId < consumerCount; consumerId++)
        {
            _probes[consumerId] = new QueueConsumerProbe(this, consumerId);
        }

        _startTicks = Stopwatch.GetTimestamp();
        _startUtc = DateTimeOffset.UtcNow;
    }

    /// <summary>Gets the number of instrumented dispatch consumers.</summary>
    internal int ConsumerCount { get; }

    /// <summary>Gets the wait duration threshold above which full episode detail is recorded.</summary>
    internal int LongWaitThresholdMilliseconds { get; }

    /// <summary>Gets the waiter-count buckets at which representative managed stacks are sampled.</summary>
    internal IReadOnlyList<int> WaiterBuckets => _waiterBuckets;

    /// <summary>Gets the current number of consumers parked inside <c>WaitToReadAsync</c>.</summary>
    internal int CurrentWaiters => Volatile.Read(ref _currentWaiters);

    /// <summary>Gets the highest observed number of simultaneous <c>WaitToReadAsync</c> waiters.</summary>
    internal int MaxConcurrentWaiters => Volatile.Read(ref _maxConcurrentWaiters);

    /// <summary>Gets the highest observed number of simultaneous <c>TryRead</c> calls.</summary>
    internal int MaxConcurrentTryReads => Volatile.Read(ref _maxConcurrentTryReads);

    /// <summary>
    /// Gets the probe used by a single dispatch consumer.
    /// </summary>
    /// <param name="consumerId">Logical consumer identifier.</param>
    /// <returns>The probe owned by the consumer.</returns>
    internal QueueConsumerProbe GetProbe(int consumerId)
    {
        if ((uint)consumerId >= (uint)_probes.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(consumerId), consumerId, "Consumer id is outside the instrumented consumer range.");
        }

        return _probes[consumerId];
    }

    /// <summary>
    /// Records a producer enqueue, capturing the write-start (T0), write-complete (T1), accounting-visible, and
    /// producer-side ThreadPool state.
    /// </summary>
    /// <param name="channelWriteStartTicks">
    /// Stopwatch tick immediately before <c>ChannelWriter.WriteAsync</c> was called (T0).
    /// Captures any byte-budget wait that preceded the write.  The difference between this value and
    /// <paramref name="channelWriteCompletedTicks"/> (interval C0) reveals bounded-channel backpressure.
    /// </param>
    /// <param name="channelWriteCompletedTicks">Stopwatch tick at which the channel write completed — the item is now readable (T1).</param>
    /// <param name="threadPoolPendingAtWrite">
    /// <see cref="ThreadPool.PendingWorkItemCount"/> captured immediately after the write completed (at T1).
    /// This is the number of ThreadPool work items ahead of the woken consumer continuations at the moment of wake.
    /// High values here directly predict long C intervals.
    /// </param>
    /// <param name="accountingVisibleTicks">Stopwatch tick at which the queue depth accounting was updated (T1+ε).</param>
    internal void RecordEnqueue(long channelWriteStartTicks, long channelWriteCompletedTicks, long threadPoolPendingAtWrite, long accountingVisibleTicks)
    {
        long sequence = Interlocked.Increment(ref _enqueueSequence) - 1;
        int index = (int)(sequence % EnqueueRingCapacity);
        ref EnqueueRecord slot = ref _enqueueRing[index];
        Volatile.Write(ref slot.Sequence, -1);
        slot.ChannelWriteStartTicks = channelWriteStartTicks;
        slot.ChannelWriteCompletedTicks = channelWriteCompletedTicks;
        slot.ThreadPoolPendingAtWrite = threadPoolPendingAtWrite;
        slot.ConsumersWaitingAtWrite = Volatile.Read(ref _currentWaiters);
        slot.AccountingVisibleTicks = accountingVisibleTicks;
        Volatile.Write(ref slot.Sequence, sequence);
    }

    /// <summary>
    /// Captures the current distribution of consumer states.
    /// </summary>
    /// <returns>The state census.</returns>
    internal ConsumerStateCensus CaptureStateCensus()
    {
        int created = 0;
        int waiting = 0;
        int waitReturned = 0;
        int tryReading = 0;
        int processing = 0;
        int exited = 0;

        foreach (QueueConsumerProbe probe in _probes)
        {
            switch (probe.State)
            {
                case QueueConsumerState.Created:
                    created++;
                    break;
                case QueueConsumerState.WaitingToRead:
                    waiting++;
                    break;
                case QueueConsumerState.WaitReturned:
                    waitReturned++;
                    break;
                case QueueConsumerState.TryReading:
                    tryReading++;
                    break;
                case QueueConsumerState.ProcessingArticle:
                    processing++;
                    break;
                default:
                    exited++;
                    break;
            }
        }

        ConsumerStateCensus census = new(
            TimestampUtc: DateTimeOffset.UtcNow,
            ElapsedMillisecondsSinceStart: ElapsedMilliseconds(Stopwatch.GetTimestamp()),
            Created: created,
            WaitingToRead: waiting,
            WaitReturned: waitReturned,
            TryReading: tryReading,
            ProcessingArticle: processing,
            Exited: exited);

        if (Interlocked.Increment(ref _stateCensusCount) <= MaxStateCensuses)
        {
            _stateCensuses.Enqueue(census);
        }

        return census;
    }

    /// <summary>
    /// Builds the complete forensic report.
    /// </summary>
    /// <param name="channelQueuedByAccounting">Queue depth reported by <c>CurrentQueuedCount</c> at report time.</param>
    /// <param name="channelQueuedBytesByAccounting">Queue bytes reported by <c>CurrentQueuedBytes</c> at report time.</param>
    /// <param name="transportInFlightArticles">Articles owned by the transport at report time, when observable.</param>
    /// <returns>The forensic report.</returns>
    internal QueueConsumerForensicsReport BuildReport(
        int channelQueuedByAccounting,
        long channelQueuedBytesByAccounting,
        long transportInFlightArticles)
    {
        ConsumerStateCensus finalCensus = CaptureStateCensus();
        long consumerOwned = Volatile.Read(ref _consumerOwnedArticles);
        long tryReadFailures = Volatile.Read(ref _tryReadAttempts) - Volatile.Read(ref _tryReadSuccesses);
        long classB = Volatile.Read(ref _tryReadFailuresClassB);
        long classC = Volatile.Read(ref _tryReadFailuresClassC);

        IntervalStatistics[] intervals;
        lock (_intervalGate)
        {
            intervals =
            [
                BuildIntervalStatistics("A", "WAIT_START -> first producer enqueue (channel write completed)", _intervalATicks),
                BuildIntervalStatistics("B", "first enqueue -> batch eligibility (queue depth accounting updated)", _intervalBTicks),
                BuildIntervalStatistics("C0", "T0 (before WriteAsync) -> T1 (WriteAsync returned): channel write duration including any backpressure wait", _intervalC0Ticks),
                BuildIntervalStatistics("C", "batch eligibility -> WAIT_RETURN (channel wake + continuation scheduling)", _intervalCTicks),
                BuildIntervalStatistics("D", "WAIT_RETURN -> TRYREAD_START", _intervalDTicks),
                BuildIntervalStatistics("E", "TRYREAD_START -> TRYREAD_END", _intervalETicks),
                BuildIntervalStatistics("TOTAL", "WAIT_START -> WAIT_RETURN (total observed wait)", _totalWaitTicks),
            ];
        }

        LongWaitRecord[] firstLongWaits = [.. _longWaits.OrderBy(static record => record.Ordinal).Take(MaxExportedLongWaits)];

        return new QueueConsumerForensicsReport(
            GeneratedUtc: DateTimeOffset.UtcNow,
            ObservedWindowMilliseconds: ElapsedMilliseconds(Stopwatch.GetTimestamp()),
            ConsumerCount: ConsumerCount,
            LongWaitThresholdMilliseconds: LongWaitThresholdMilliseconds,
            WaitEpisodeCount: Volatile.Read(ref _waitEpisodeCount),
            WaitEpisodesCompletedSynchronously: Volatile.Read(ref _waitEpisodesCompletedSynchronously),
            WaitEpisodesParked: Math.Max(0, Volatile.Read(ref _waitEpisodeCount) - Volatile.Read(ref _waitEpisodesCompletedSynchronously)),
            LongWaitEpisodeCount: Volatile.Read(ref _longWaitCount),
            MaxConcurrentWaiters: MaxConcurrentWaiters,
            MaxConcurrentTryReads: MaxConcurrentTryReads,
            TryReadAttemptCount: Volatile.Read(ref _tryReadAttempts),
            TryReadSuccessCount: Volatile.Read(ref _tryReadSuccesses),
            TryReadFailureCount: tryReadFailures,
            TryReadFailuresClassA: Volatile.Read(ref _tryReadFailuresClassA),
            TryReadFailuresClassB: classB,
            TryReadFailuresClassC: classC,
            TryReadFailuresClassD: Volatile.Read(ref _tryReadFailuresClassD),
            AnyTryReadFailureWithPositiveDepth: classB > 0,
            EnqueueCount: Volatile.Read(ref _enqueueSequence),
            ThreadHopsAcrossWait: Volatile.Read(ref _threadHopsAcrossWait),
            Intervals: intervals,
            StackSamples: [.. _stackSamples],
            FirstLongWaits: firstLongWaits,
            TryReadFailures: [.. _tryReadFailures],
            StateCensuses: [.. _stateCensuses.Append(finalCensus)],
            Ownership: new OwnershipAccounting(
                ChannelQueuedByAccounting: channelQueuedByAccounting,
                ChannelQueuedBytesByAccounting: channelQueuedBytesByAccounting,
                ConsumerOwnedArticles: consumerOwned,
                TransportInFlightArticles: transportInFlightArticles,
                TotalOutstandingWork: channelQueuedByAccounting + consumerOwned + transportInFlightArticles,
                Note: "Consumer-owned articles left the Channel via TryRead and are owned by a dispatch consumer until PublishAsync completes; transport in-flight is reported by TransitPublisher connection diagnostics."),
            ObservabilityNotes: BuildObservabilityNotes(classB));
    }

    /// <summary>
    /// Converts a stopwatch tick into milliseconds relative to the instrumentation start.
    /// </summary>
    /// <param name="ticks">Stopwatch tick.</param>
    /// <returns>Milliseconds since instrumentation start.</returns>
    internal double ElapsedMilliseconds(long ticks)
    {
        return (ticks - _startTicks) * 1000d / Stopwatch.Frequency;
    }

    /// <summary>
    /// Converts a stopwatch tick into a wall-clock timestamp relative to the instrumentation start.
    /// </summary>
    /// <param name="ticks">Stopwatch tick.</param>
    /// <returns>The wall-clock timestamp.</returns>
    internal DateTimeOffset ToUtcTimestamp(long ticks)
    {
        return _startUtc.AddMilliseconds(ElapsedMilliseconds(ticks));
    }

    /// <summary>
    /// Registers entry into <c>WaitToReadAsync</c>.
    /// </summary>
    /// <param name="completedSynchronously">Whether the returned <see cref="ValueTask{TResult}"/> was already completed.</param>
    /// <returns>The number of simultaneous parked waiters including the caller, or zero when the wait completed synchronously.</returns>
    /// <remarks>Only waits that actually park are counted as waiters; a synchronously completed wait never parks.</remarks>
    internal int EnterWait(bool completedSynchronously)
    {
        Interlocked.Increment(ref _waitEpisodeCount);
        if (completedSynchronously)
        {
            Interlocked.Increment(ref _waitEpisodesCompletedSynchronously);
            return 0;
        }

        int waiters = Interlocked.Increment(ref _currentWaiters);
        UpdateMaximum(ref _maxConcurrentWaiters, waiters);
        return waiters;
    }

    /// <summary>
    /// Registers return from <c>WaitToReadAsync</c>.
    /// </summary>
    /// <param name="parked">Whether the wait was counted as a parked waiter at WAIT_START.</param>
    /// <returns>The number of simultaneous parked waiters after the caller resumed.</returns>
    internal int ExitWait(bool parked)
    {
        return parked ? Interlocked.Decrement(ref _currentWaiters) : Volatile.Read(ref _currentWaiters);
    }

    /// <summary>
    /// Registers entry into <c>TryRead</c>.
    /// </summary>
    /// <returns>The number of simultaneous <c>TryRead</c> calls including the caller.</returns>
    internal int EnterTryRead()
    {
        Interlocked.Increment(ref _tryReadAttempts);
        int concurrent = Interlocked.Increment(ref _currentTryReads);
        UpdateMaximum(ref _maxConcurrentTryReads, concurrent);
        return concurrent;
    }

    /// <summary>
    /// Registers return from <c>TryRead</c>.
    /// </summary>
    /// <param name="success">Whether the read returned an article.</param>
    internal void ExitTryRead(bool success)
    {
        Interlocked.Decrement(ref _currentTryReads);
        if (success)
        {
            Interlocked.Increment(ref _tryReadSuccesses);
            Interlocked.Increment(ref _consumerOwnedArticles);
        }
    }

    /// <summary>
    /// Registers completion of article processing by a consumer.
    /// </summary>
    internal void ReleaseConsumerOwnership()
    {
        Interlocked.Decrement(ref _consumerOwnedArticles);
    }

    /// <summary>
    /// Registers that a consumer resumed on a different managed thread than it parked on.
    /// </summary>
    internal void RecordThreadHop()
    {
        Interlocked.Increment(ref _threadHopsAcrossWait);
    }

    /// <summary>
    /// Selects a stack-sampling bucket for the supplied waiter count, if that bucket still has sampling budget.
    /// </summary>
    /// <param name="waiterCount">Number of simultaneous waiters observed at WAIT_START.</param>
    /// <returns>The waiter bucket value, or <c>-1</c> when no sample should be captured.</returns>
    internal int TrySelectStackBucket(int waiterCount)
    {
        int bucketIndex = -1;
        for (int i = 0; i < _waiterBuckets.Length; i++)
        {
            if (waiterCount >= _waiterBuckets[i])
            {
                bucketIndex = i;
            }
        }

        if (bucketIndex < 0)
        {
            return -1;
        }

        if (Interlocked.Increment(ref _bucketSampleCounts[bucketIndex]) > MaxStackSamplesPerBucket)
        {
            Interlocked.Decrement(ref _bucketSampleCounts[bucketIndex]);
            return -1;
        }

        return _waiterBuckets[bucketIndex];
    }

    /// <summary>
    /// Stores a captured managed stack sample.
    /// </summary>
    /// <param name="sample">The sample to store.</param>
    internal void AddStackSample(QueueConsumerStackSample sample)
    {
        ArgumentNullException.ThrowIfNull(sample);
        _stackSamples.Enqueue(sample);
    }

    /// <summary>
    /// Resolves the first producer enqueue that happened at or after the supplied enqueue sequence.
    /// </summary>
    /// <param name="sequenceAtWaitStart">Enqueue sequence observed at WAIT_START.</param>
    /// <param name="channelWriteStartTicks">Tick immediately before <c>WriteAsync</c> was called (T0); zero when unresolved.</param>
    /// <param name="channelWriteCompletedTicks">Tick at which the enqueued item became readable (T1); zero when unresolved.</param>
    /// <param name="threadPoolPendingAtWrite">ThreadPool pending work-item count captured at T1; zero when unresolved.</param>
    /// <param name="consumersWaitingAtWrite">Number of consumers parked in <c>WaitToReadAsync</c> at the moment of write; zero when unresolved.</param>
    /// <param name="accountingVisibleTicks">Tick at which the depth accounting reflected the enqueue (T1+ε); zero when unresolved.</param>
    /// <returns>The correlation outcome.</returns>
    internal EnqueueCorrelation TryResolveFirstEnqueueAfter(
        long sequenceAtWaitStart,
        out long channelWriteStartTicks,
        out long channelWriteCompletedTicks,
        out long threadPoolPendingAtWrite,
        out int consumersWaitingAtWrite,
        out long accountingVisibleTicks)
    {
        channelWriteStartTicks = 0;
        channelWriteCompletedTicks = 0;
        threadPoolPendingAtWrite = 0;
        consumersWaitingAtWrite = 0;
        accountingVisibleTicks = 0;

        long currentSequence = Volatile.Read(ref _enqueueSequence);
        if (currentSequence <= sequenceAtWaitStart)
        {
            return EnqueueCorrelation.NoEnqueueObserved;
        }

        if (currentSequence - sequenceAtWaitStart > EnqueueRingCapacity)
        {
            return EnqueueCorrelation.Undeterminable;
        }

        int index = (int)(sequenceAtWaitStart % EnqueueRingCapacity);
        ref EnqueueRecord slot = ref _enqueueRing[index];
        long observedSequence = Volatile.Read(ref slot.Sequence);
        long writeStartTicks = slot.ChannelWriteStartTicks;
        long writeTicks = slot.ChannelWriteCompletedTicks;
        long tpPending = slot.ThreadPoolPendingAtWrite;
        int waitersAtWrite = slot.ConsumersWaitingAtWrite;
        long accountingTicks = slot.AccountingVisibleTicks;
        if (observedSequence != sequenceAtWaitStart || Volatile.Read(ref slot.Sequence) != sequenceAtWaitStart)
        {
            return EnqueueCorrelation.Undeterminable;
        }

        channelWriteStartTicks = writeStartTicks;
        channelWriteCompletedTicks = writeTicks;
        threadPoolPendingAtWrite = tpPending;
        consumersWaitingAtWrite = waitersAtWrite;
        accountingVisibleTicks = accountingTicks;
        return EnqueueCorrelation.Resolved;
    }

    /// <summary>
    /// Gets the current producer enqueue sequence.
    /// </summary>
    /// <returns>The number of enqueues observed so far.</returns>
    internal long ReadEnqueueSequence()
    {
        return Volatile.Read(ref _enqueueSequence);
    }

    /// <summary>
    /// Records a failed <c>TryRead</c> reconciled against queue depth accounting.
    /// </summary>
    /// <param name="record">The failure observation.</param>
    /// <param name="classification">The reconciliation class.</param>
    internal void RecordTryReadFailure(TryReadFailureRecord record, TryReadFailureClass classification)
    {
        ArgumentNullException.ThrowIfNull(record);

        switch (classification)
        {
            case TryReadFailureClass.CountZeroBefore:
                Interlocked.Increment(ref _tryReadFailuresClassA);
                break;
            case TryReadFailureClass.CountPositiveBefore:
                Interlocked.Increment(ref _tryReadFailuresClassB);
                break;
            case TryReadFailureClass.CountChangedDuringObservation:
                Interlocked.Increment(ref _tryReadFailuresClassC);
                break;
            default:
                Interlocked.Increment(ref _tryReadFailuresClassD);
                break;
        }

        if (Interlocked.Increment(ref _recordedTryReadFailureCount) <= MaxRecordedTryReadFailures)
        {
            _tryReadFailures.Enqueue(record);
        }
    }

    /// <summary>
    /// Records a wait episode that exceeded the long-wait threshold.
    /// </summary>
    /// <param name="factory">Factory producing the record once an ordinal has been assigned.</param>
    /// <param name="intervalATicks">Interval A duration in stopwatch ticks, or a negative value when unresolved.</param>
    /// <param name="intervalBTicks">Interval B duration in stopwatch ticks, or a negative value when unresolved.</param>
    /// <param name="intervalC0Ticks">Interval C0 duration (T0→T1, WriteAsync duration) in stopwatch ticks, or a negative value when unresolved.</param>
    /// <param name="intervalCTicks">Interval C duration in stopwatch ticks, or a negative value when unresolved.</param>
    /// <param name="intervalDTicks">Interval D duration in stopwatch ticks.</param>
    /// <param name="intervalETicks">Interval E duration in stopwatch ticks.</param>
    /// <param name="totalWaitTicks">Total wait duration in stopwatch ticks.</param>
    internal void RecordLongWait(
        Func<int, LongWaitRecord> factory,
        long intervalATicks,
        long intervalBTicks,
        long intervalC0Ticks,
        long intervalCTicks,
        long intervalDTicks,
        long intervalETicks,
        long totalWaitTicks)
    {
        ArgumentNullException.ThrowIfNull(factory);

        long ordinal = Interlocked.Increment(ref _longWaitCount);

        lock (_intervalGate)
        {
            AddIntervalSample(_intervalATicks, intervalATicks);
            AddIntervalSample(_intervalBTicks, intervalBTicks);
            AddIntervalSample(_intervalC0Ticks, intervalC0Ticks);
            AddIntervalSample(_intervalCTicks, intervalCTicks);
            AddIntervalSample(_intervalDTicks, intervalDTicks);
            AddIntervalSample(_intervalETicks, intervalETicks);
            AddIntervalSample(_totalWaitTicks, totalWaitTicks);
        }

        if (ordinal <= MaxExportedLongWaits)
        {
            _longWaits.Enqueue(factory((int)ordinal));
        }
    }

    private static void AddIntervalSample(List<long> samples, long ticks)
    {
        if (ticks < 0 || samples.Count >= MaxIntervalSamples)
        {
            return;
        }

        samples.Add(ticks);
    }

    private static IntervalStatistics BuildIntervalStatistics(string interval, string description, List<long> samples)
    {
        return new IntervalStatistics(
            Interval: interval,
            Description: description,
            SampleCount: samples.Count,
            P50Microseconds: MetricMathHelpers.PercentileUs(samples, 0.50d),
            P95Microseconds: MetricMathHelpers.PercentileUs(samples, 0.95d),
            P99Microseconds: MetricMathHelpers.PercentileUs(samples, 0.99d),
            MaxMicroseconds: samples.Count == 0 ? 0d : MetricMathHelpers.TicksToUs(samples.Max()));
    }

    private static int[] BuildWaiterBuckets(int consumerCount)
    {
        int nearMax = Math.Max(1, consumerCount * 9 / 10);
        int[] candidates = [1, 10, 100, nearMax];
        return [.. candidates.Where(candidate => candidate <= consumerCount).Distinct().Order()];
    }

    private static void UpdateMaximum(ref int target, int candidate)
    {
        int observed = Volatile.Read(ref target);
        while (candidate > observed)
        {
            int previous = Interlocked.CompareExchange(ref target, candidate, observed);
            if (previous == observed)
            {
                return;
            }

            observed = previous;
        }
    }

    private IReadOnlyList<string> BuildObservabilityNotes(long classBFailures)
    {
        List<string> notes =
        [
            "Managed stacks are captured by the consumer itself immediately before parking, immediately after resuming, and immediately before TryRead. The .NET runtime exposes no supported in-process API to capture the stack of an async-parked state machine from another thread, so the WAIT_START stack is the exact source statement the consumer executed when it parked.",
            "Inline versus asynchronous continuation execution cannot be determined reliably from runtime APIs. What is determined here is (a) whether WaitToReadAsync completed synchronously (ValueTask.IsCompleted observed before the await) and (b) whether the consumer resumed on a different managed thread than it parked on.",
            "Interval E measures only the TryRead call itself; the forensic stack capture happens before the TryRead start timestamp, so it is accounted to interval D. Interval D is therefore an upper bound for instrumented episodes.",
            "TryRead failures are classified against CurrentQueuedCount, which is an Interlocked counter maintained by BoundedArticleQueue and is not the Channel's readable item count; both counters are updated after the corresponding channel operation.",
            "Interval C0 (T0→T1) measures the duration of ChannelWriter.WriteAsync itself, capturing any bounded-channel backpressure wait. When the channel has capacity C0 is in the low-microsecond range; a large C0 indicates the channel was full and the producer was blocked behind a consumer drain.",
            "Interval C (T1+ε→T2) measures from immediately after the channel write completed (item is readable) until the consumer continuation actually executed on the ThreadPool. This is the Channel wake-up and ThreadPool scheduling latency. A large C with a small C0 indicates the write itself was instant but the consumer continuation was delayed in the ThreadPool queue.",
            "ThreadPoolPendingWorkItemsAtChannelWrite is captured at T1 (immediately after WriteAsync) and represents the number of ThreadPool work items already queued at the exact moment the consumer continuations were enqueued by the Channel. High values here directly predict large C intervals because the consumer continuations must queue behind that many items.",
            "ConsumersWaitingAtChannelWrite is the number of consumers parked in WaitToReadAsync at the moment of the channel write. When this is zero the write found no waiting consumers, meaning C does not measure wake-up latency for that episode — it measures how long before any consumer called WaitToReadAsync and found the item.",
        ];

        notes.Add(classBFailures > 0
            ? $"{classBFailures} failed TryRead observation(s) occurred while CurrentQueuedCount was greater than zero and unchanged across the observation; this is an accounting/visibility discrepancy, not necessarily a lost item."
            : "No failed TryRead was observed while CurrentQueuedCount was greater than zero and unchanged across the observation.");

        return notes;
    }

    private struct EnqueueRecord
    {
        internal long Sequence;
        internal long ChannelWriteStartTicks;
        internal long ChannelWriteCompletedTicks;
        internal long ThreadPoolPendingAtWrite;
        internal int ConsumersWaitingAtWrite;
        internal long AccountingVisibleTicks;
    }
}
