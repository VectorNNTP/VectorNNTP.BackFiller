namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Lifecycle state of a dispatch consumer as observed by the queue-read forensic instrumentation.
/// </summary>
internal enum QueueConsumerState
{
    /// <summary>Consumer task has not entered the dispatch loop yet.</summary>
    Created,

    /// <summary>Consumer is inside <c>await WaitToReadAsync(...)</c> and has not resumed yet.</summary>
    WaitingToRead,

    /// <summary>Consumer resumed from <c>WaitToReadAsync</c> but has not called <c>TryRead</c> yet.</summary>
    WaitReturned,

    /// <summary>Consumer is executing <c>ChannelReader.TryRead</c>.</summary>
    TryReading,

    /// <summary>Consumer owns an article and is publishing it (socket write plus response wait).</summary>
    ProcessingArticle,

    /// <summary>Consumer left the dispatch loop.</summary>
    Exited,
}

/// <summary>
/// Classification of a failed <c>TryRead</c> relative to the queue's own depth accounting.
/// </summary>
internal enum TryReadFailureClass
{
    /// <summary>Class A: <c>CurrentQueuedCount</c> was zero immediately before the failed read.</summary>
    CountZeroBefore,

    /// <summary>Class B: <c>CurrentQueuedCount</c> was greater than zero immediately before the failed read and unchanged after it.</summary>
    CountPositiveBefore,

    /// <summary>Class C: <c>CurrentQueuedCount</c> changed between the before and after observation.</summary>
    CountChangedDuringObservation,

    /// <summary>Class D: the observation could not be reconciled.</summary>
    Undeterminable,
}

/// <summary>
/// Interval-resolution outcome for the WAIT_START to first-enqueue correlation.
/// </summary>
internal enum EnqueueCorrelation
{
    /// <summary>The first enqueue after WAIT_START was located in the enqueue ring buffer.</summary>
    Resolved,

    /// <summary>No enqueue was observed between WAIT_START and WAIT_RETURN.</summary>
    NoEnqueueObserved,

    /// <summary>The enqueue record was overwritten or torn before it could be read.</summary>
    Undeterminable,
}

/// <summary>
/// A representative managed stack sample of a dispatch consumer at a well-defined point of the queue-read path.
/// </summary>
internal sealed record QueueConsumerStackSample(
    string Phase,
    int WaiterBucket,
    int ConsumerId,
    int ManagedThreadId,
    int? TaskId,
    DateTimeOffset TimestampUtc,
    double ElapsedMillisecondsSinceStart,
    int QueueDepth,
    long QueueBytes,
    string ConsumerState,
    int ConcurrentWaiters,
    int ConcurrentTryReads,
    string SynchronizationContext,
    string TaskScheduler,
    int ThreadPoolThreadCount,
    int ThreadPoolAvailableWorkerThreads,
    int ThreadPoolAvailableCompletionPortThreads,
    long ThreadPoolPendingWorkItemCount,
    string StackTrace);

/// <summary>
/// A single failed <c>TryRead</c> reconciled against the queue depth accounting immediately before and after the call.
/// </summary>
internal sealed record TryReadFailureRecord(
    int ConsumerId,
    int ManagedThreadId,
    int? TaskId,
    DateTimeOffset TimestampUtc,
    double ElapsedMillisecondsSinceStart,
    int QueueDepthBefore,
    long QueueBytesBefore,
    int QueueDepthAfter,
    long QueueBytesAfter,
    double TryReadMicroseconds,
    string Classification);

/// <summary>
/// A wait episode longer than the configured long-wait threshold, decomposed into the A-E intervals plus the
/// new C0 sub-interval (T0→T1: channel <c>WriteAsync</c> duration) and the producer-side ThreadPool snapshot.
/// </summary>
/// <remarks>
/// <para>
/// Timestamp decomposition reference for this record:
/// <list type="bullet">
///   <item><description>T0 (<c>ChannelWriteStartMs</c>): immediately before <c>_channel.Writer.WriteAsync</c> was called — after the byte-budget was acquired.</description></item>
///   <item><description>T1 (<c>FirstEnqueueMs</c>): immediately after <c>WriteAsync</c> returned — the item is readable by consumers.</description></item>
///   <item><description>T1+ε (<c>BatchEligibleMs</c>): after the two post-write <c>Interlocked</c> accounting updates — the application depth counter caught up.</description></item>
///   <item><description>T2 (<c>WaitReturnMs</c>): the instant the consumer's <c>await WaitToReadAsync</c> resumed on the ThreadPool — the continuation actually executed.</description></item>
/// </list>
/// </para>
/// <para>
/// Interval C0 (<c>IntervalC0ChannelWriteAsyncDurationUs</c>) = T1 − T0: how long <c>WriteAsync</c> itself took, including any
/// channel-capacity backpressure wait.  When the channel was empty or had space this will be ≈ 0–5 µs.  A large C0 indicates
/// the bounded channel was full and the producer was blocked waiting for consumers to drain it.
/// </para>
/// <para>
/// Interval C (<c>IntervalCBatchEligibleToWaitReturnUs</c>) ≈ T2 − T1: the Channel wake-up and continuation-scheduling latency.
/// This is the primary interval of interest for the 150–200 ms production anomaly.  When C is large while C0 is small, the
/// write itself was instant but the consumer continuation sat in the ThreadPool queue for the full C duration.
/// <c>ThreadPoolPendingWorkItemsAtChannelWrite</c> (captured at T1) quantifies the ThreadPool backlog that the consumer
/// continuation had to queue behind.
/// </para>
/// </remarks>
internal sealed record LongWaitRecord(
    int Ordinal,
    int ConsumerId,
    int WaitStartThreadId,
    int WaitReturnThreadId,
    int? WaitStartTaskId,
    int? WaitReturnTaskId,
    DateTimeOffset WaitStartUtc,
    double WaitStartMs,
    double ChannelWriteStartMs,
    double FirstEnqueueMs,
    double BatchEligibleMs,
    double WaitReturnMs,
    double TryReadStartMs,
    double TryReadEndMs,
    string EnqueueCorrelation,
    bool WaitCompletedSynchronously,
    bool WaitResult,
    bool TryReadResult,
    int QueueDepthAtWaitStart,
    long QueueBytesAtWaitStart,
    int QueueDepthAtWaitReturn,
    long QueueBytesAtWaitReturn,
    int QueueDepthBeforeTryRead,
    int QueueDepthAfterTryRead,
    int ConcurrentWaitersAtWaitStart,
    int ConcurrentWaitersAtWaitReturn,
    double IntervalAWaitStartToFirstEnqueueUs,
    double IntervalBFirstEnqueueToBatchEligibleUs,
    double IntervalC0ChannelWriteAsyncDurationUs,
    double IntervalCBatchEligibleToWaitReturnUs,
    double IntervalDWaitReturnToTryReadStartUs,
    double IntervalETryReadDurationUs,
    double TotalWaitUs,
    int ThreadPoolThreadCountAtWaitReturn,
    int ThreadPoolAvailableWorkerThreadsAtWaitReturn,
    int ThreadPoolAvailableCompletionPortThreadsAtWaitReturn,
    long ThreadPoolPendingWorkItemsAtWaitReturn,
    long ThreadPoolPendingWorkItemsAtChannelWrite,
    int ConsumersWaitingAtChannelWrite,
    string SynchronizationContextAtWaitReturn,
    string TaskSchedulerAtWaitReturn,
    string? WaitStartStack,
    string? WaitReturnStack,
    string? TryReadStartStack);

/// <summary>
/// P50/P95/P99/MAX summary for one measured interval, expressed in microseconds.
/// </summary>
internal sealed record IntervalStatistics(
    string Interval,
    string Description,
    long SampleCount,
    double P50Microseconds,
    double P95Microseconds,
    double P99Microseconds,
    double MaxMicroseconds);

/// <summary>
/// Census of consumer states captured at a single instant.
/// </summary>
internal sealed record ConsumerStateCensus(
    DateTimeOffset TimestampUtc,
    double ElapsedMillisecondsSinceStart,
    int Created,
    int WaitingToRead,
    int WaitReturned,
    int TryReading,
    int ProcessingArticle,
    int Exited);

/// <summary>
/// Ownership accounting reconciling channel-resident, consumer-owned and transport in-flight articles.
/// </summary>
internal sealed record OwnershipAccounting(
    int ChannelQueuedByAccounting,
    long ChannelQueuedBytesByAccounting,
    long ConsumerOwnedArticles,
    long TransportInFlightArticles,
    long TotalOutstandingWork,
    string Note);

/// <summary>
/// Complete forensic report for the dispatch consumer queue-read path.
/// </summary>
internal sealed record QueueConsumerForensicsReport(
    DateTimeOffset GeneratedUtc,
    double ObservedWindowMilliseconds,
    int ConsumerCount,
    int LongWaitThresholdMilliseconds,
    long WaitEpisodeCount,
    long WaitEpisodesCompletedSynchronously,
    long WaitEpisodesParked,
    long LongWaitEpisodeCount,
    int MaxConcurrentWaiters,
    int MaxConcurrentTryReads,
    long TryReadAttemptCount,
    long TryReadSuccessCount,
    long TryReadFailureCount,
    long TryReadFailuresClassA,
    long TryReadFailuresClassB,
    long TryReadFailuresClassC,
    long TryReadFailuresClassD,
    bool AnyTryReadFailureWithPositiveDepth,
    long EnqueueCount,
    long ThreadHopsAcrossWait,
    IReadOnlyList<IntervalStatistics> Intervals,
    IReadOnlyList<QueueConsumerStackSample> StackSamples,
    IReadOnlyList<LongWaitRecord> FirstLongWaits,
    IReadOnlyList<TryReadFailureRecord> TryReadFailures,
    IReadOnlyList<ConsumerStateCensus> StateCensuses,
    OwnershipAccounting Ownership,
    IReadOnlyList<string> ObservabilityNotes);
