using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Per-consumer forensic probe recording the WAIT -> WAKE -> TRYREAD -> PROCESS path of one dispatch consumer.
/// </summary>
/// <remarks>
/// All probe methods are called by the owning consumer only, in strict sequence, so episode fields do not
/// require synchronization between consumers. Cross-consumer aggregation happens in <see cref="QueueConsumerForensics"/>.
/// </remarks>
internal sealed class QueueConsumerProbe
{
    private readonly QueueConsumerForensics _owner;

    private int _state;

    private long _waitStartTicks;
    private long _waitReturnTicks;
    private long _tryReadStartTicks;
    private long _enqueueSequenceAtWaitStart;
    private int _waitStartThreadId;
    private int _waitReturnThreadId;
    private int? _waitStartTaskId;
    private int? _waitReturnTaskId;
    private int _queueDepthAtWaitStart;
    private long _queueBytesAtWaitStart;
    private int _queueDepthAtWaitReturn;
    private long _queueBytesAtWaitReturn;
    private int _queueDepthBeforeTryRead;
    private long _queueBytesBeforeTryRead;
    private int _waitersAtWaitStart;
    private int _waitersAtWaitReturn;
    private int _sampleBucket = -1;
    private bool _waitCompletedSynchronously;
    private bool _waitResult;
    private bool _longWaitPending;
    private long _channelWriteStartTicks;
    private long _firstEnqueueTicks;
    private long _batchEligibleTicks;
    private long _threadPoolPendingAtWrite;
    private int _consumersWaitingAtWrite;
    private EnqueueCorrelation _enqueueCorrelation;
    private int _threadPoolThreadCountAtWaitReturn;
    private int _threadPoolAvailableWorkersAtWaitReturn;
    private int _threadPoolAvailableCompletionPortsAtWaitReturn;
    private long _threadPoolPendingWorkItemsAtWaitReturn;
    private string _syncContextAtWaitReturn = "(none)";
    private string _taskSchedulerAtWaitReturn = "(unknown)";
    private string? _waitStartStack;
    private string? _waitReturnStack;
    private string? _tryReadStartStack;

    /// <summary>
    /// Initializes a new instance of the <see cref="QueueConsumerProbe"/> class.
    /// </summary>
    /// <param name="owner">Owning forensic recorder.</param>
    /// <param name="consumerId">Logical consumer identifier.</param>
    internal QueueConsumerProbe(QueueConsumerForensics owner, int consumerId)
    {
        _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        ConsumerId = consumerId;
    }

    /// <summary>Gets the logical consumer identifier.</summary>
    internal int ConsumerId { get; }

    /// <summary>Gets the current consumer state.</summary>
    internal QueueConsumerState State => (QueueConsumerState)Volatile.Read(ref _state);

    /// <summary>
    /// Records that the consumer is about to await <c>WaitToReadAsync</c>.
    /// </summary>
    /// <param name="queueDepth">Queue depth observed at WAIT_START.</param>
    /// <param name="queueBytes">Queue bytes observed at WAIT_START.</param>
    /// <param name="waitCompletedSynchronously">Whether the returned <see cref="ValueTask{TResult}"/> was already completed.</param>
    internal void RecordWaitStart(int queueDepth, long queueBytes, bool waitCompletedSynchronously)
    {
        _waitStartTicks = Stopwatch.GetTimestamp();
        _enqueueSequenceAtWaitStart = _owner.ReadEnqueueSequence();
        _waitStartThreadId = Environment.CurrentManagedThreadId;
        _waitStartTaskId = Task.CurrentId;
        _queueDepthAtWaitStart = queueDepth;
        _queueBytesAtWaitStart = queueBytes;
        _waitCompletedSynchronously = waitCompletedSynchronously;
        _waitStartStack = null;
        _waitReturnStack = null;
        _tryReadStartStack = null;
        _longWaitPending = false;
        Volatile.Write(ref _state, (int)QueueConsumerState.WaitingToRead);

        _waitersAtWaitStart = _owner.EnterWait(waitCompletedSynchronously);
        _sampleBucket = waitCompletedSynchronously ? -1 : _owner.TrySelectStackBucket(_waitersAtWaitStart);
        if (_sampleBucket < 0)
        {
            return;
        }

        _waitStartStack = CaptureStack();
        _owner.AddStackSample(BuildSample(
            phase: "WAIT_START",
            ticks: _waitStartTicks,
            queueDepth: queueDepth,
            queueBytes: queueBytes,
            concurrentWaiters: _waitersAtWaitStart,
            concurrentTryReads: 0,
            stack: _waitStartStack));
    }

    /// <summary>
    /// Records that <c>WaitToReadAsync</c> returned.
    /// </summary>
    /// <param name="waitResult">The value returned by <c>WaitToReadAsync</c>.</param>
    /// <param name="queueDepth">Queue depth observed at WAIT_RETURN.</param>
    /// <param name="queueBytes">Queue bytes observed at WAIT_RETURN.</param>
    internal void RecordWaitReturn(bool waitResult, int queueDepth, long queueBytes)
    {
        _waitReturnTicks = Stopwatch.GetTimestamp();
        _waitersAtWaitReturn = _owner.ExitWait(!_waitCompletedSynchronously);
        _waitReturnThreadId = Environment.CurrentManagedThreadId;
        _waitReturnTaskId = Task.CurrentId;
        _queueDepthAtWaitReturn = queueDepth;
        _queueBytesAtWaitReturn = queueBytes;
        _waitResult = waitResult;
        Volatile.Write(ref _state, (int)QueueConsumerState.WaitReturned);

        if (_waitReturnThreadId != _waitStartThreadId)
        {
            _owner.RecordThreadHop();
        }

        long totalWaitTicks = Math.Max(0, _waitReturnTicks - _waitStartTicks);
        _longWaitPending = !_waitCompletedSynchronously && totalWaitTicks >= LongWaitThresholdTicks;

        if (_longWaitPending)
        {
            _enqueueCorrelation = _owner.TryResolveFirstEnqueueAfter(
                _enqueueSequenceAtWaitStart,
                out _channelWriteStartTicks,
                out _firstEnqueueTicks,
                out _threadPoolPendingAtWrite,
                out _consumersWaitingAtWrite,
                out _batchEligibleTicks);
            CaptureThreadPoolState();
            _syncContextAtWaitReturn = SynchronizationContext.Current?.GetType().FullName ?? "(none)";
            _taskSchedulerAtWaitReturn = TaskScheduler.Current.GetType().FullName ?? "(unknown)";
        }

        if (_sampleBucket >= 0)
        {
            _waitReturnStack = CaptureStack();
            _owner.AddStackSample(BuildSample(
                phase: "WAIT_RETURN",
                ticks: _waitReturnTicks,
                queueDepth: queueDepth,
                queueBytes: queueBytes,
                concurrentWaiters: Math.Max(0, _waitersAtWaitReturn),
                concurrentTryReads: 0,
                stack: _waitReturnStack));
        }
    }

    /// <summary>
    /// Records that the consumer is about to call <c>TryRead</c>.
    /// </summary>
    /// <param name="queueDepth">Queue depth observed immediately before <c>TryRead</c>.</param>
    /// <param name="queueBytes">Queue bytes observed immediately before <c>TryRead</c>.</param>
    internal void RecordTryReadStart(int queueDepth, long queueBytes)
    {
        _queueDepthBeforeTryRead = queueDepth;
        _queueBytesBeforeTryRead = queueBytes;
        Volatile.Write(ref _state, (int)QueueConsumerState.TryReading);
        int concurrentTryReads = _owner.EnterTryRead();

        if (_sampleBucket >= 0 || _longWaitPending)
        {
            long stackTicks = Stopwatch.GetTimestamp();
            _tryReadStartStack = CaptureStack();
            if (_sampleBucket >= 0)
            {
                _owner.AddStackSample(BuildSample(
                    phase: "TRYREAD_START",
                    ticks: stackTicks,
                    queueDepth: queueDepth,
                    queueBytes: queueBytes,
                    concurrentWaiters: _owner.CurrentWaiters,
                    concurrentTryReads: concurrentTryReads,
                    stack: _tryReadStartStack));
            }
        }

        _tryReadStartTicks = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Records the outcome of <c>TryRead</c> and, when applicable, finalizes a long wait episode.
    /// </summary>
    /// <param name="success">Whether an article was returned.</param>
    /// <param name="queueDepthAfter">Queue depth observed immediately after <c>TryRead</c>.</param>
    /// <param name="queueBytesAfter">Queue bytes observed immediately after <c>TryRead</c>.</param>
    internal void RecordTryReadEnd(bool success, int queueDepthAfter, long queueBytesAfter)
    {
        long tryReadEndTicks = Stopwatch.GetTimestamp();
        _owner.ExitTryRead(success);
        Volatile.Write(ref _state, (int)(success ? QueueConsumerState.ProcessingArticle : QueueConsumerState.WaitReturned));

        long tryReadTicks = Math.Max(0, tryReadEndTicks - _tryReadStartTicks);

        if (!success)
        {
            TryReadFailureClass classification = ClassifyFailure(_queueDepthBeforeTryRead, queueDepthAfter);
            _owner.RecordTryReadFailure(
                new TryReadFailureRecord(
                    ConsumerId: ConsumerId,
                    ManagedThreadId: Environment.CurrentManagedThreadId,
                    TaskId: Task.CurrentId,
                    TimestampUtc: _owner.ToUtcTimestamp(_tryReadStartTicks),
                    ElapsedMillisecondsSinceStart: _owner.ElapsedMilliseconds(_tryReadStartTicks),
                    QueueDepthBefore: _queueDepthBeforeTryRead,
                    QueueBytesBefore: _queueBytesBeforeTryRead,
                    QueueDepthAfter: queueDepthAfter,
                    QueueBytesAfter: queueBytesAfter,
                    TryReadMicroseconds: MetricMathHelpers.TicksToUs(tryReadTicks),
                    Classification: classification.ToString()),
                classification);
        }

        if (!_longWaitPending)
        {
            return;
        }

        _longWaitPending = false;
        FinalizeLongWait(success, queueDepthAfter, tryReadTicks, tryReadEndTicks);
    }

    /// <summary>
    /// Records that the consumer finished publishing the article it owned.
    /// </summary>
    internal void RecordProcessingComplete()
    {
        _owner.ReleaseConsumerOwnership();
        Volatile.Write(ref _state, (int)QueueConsumerState.WaitReturned);
    }

    /// <summary>
    /// Records that the consumer left the dispatch loop.
    /// </summary>
    internal void RecordExit()
    {
        Volatile.Write(ref _state, (int)QueueConsumerState.Exited);
    }

    private long LongWaitThresholdTicks => (long)(Stopwatch.Frequency * (_owner.LongWaitThresholdMilliseconds / 1000d));

    private static TryReadFailureClass ClassifyFailure(int depthBefore, int depthAfter)
    {
        if (depthBefore < 0 || depthAfter < 0)
        {
            return TryReadFailureClass.Undeterminable;
        }

        if (depthBefore != depthAfter)
        {
            return TryReadFailureClass.CountChangedDuringObservation;
        }

        return depthBefore == 0
            ? TryReadFailureClass.CountZeroBefore
            : TryReadFailureClass.CountPositiveBefore;
    }

    private static string CaptureStack()
    {
        return new StackTrace(skipFrames: 1, fNeedFileInfo: true).ToString();
    }

    private void CaptureThreadPoolState()
    {
        ThreadPool.GetAvailableThreads(out int availableWorkers, out int availableCompletionPorts);
        _threadPoolThreadCountAtWaitReturn = ThreadPool.ThreadCount;
        _threadPoolAvailableCompletionPortsAtWaitReturn = availableCompletionPorts;
        _threadPoolAvailableWorkersAtWaitReturn = availableWorkers;
        _threadPoolPendingWorkItemsAtWaitReturn = ThreadPool.PendingWorkItemCount;
    }

    private void FinalizeLongWait(bool tryReadResult, int queueDepthAfter, long tryReadTicks, long tryReadEndTicks)
    {
        long totalWaitTicks = Math.Max(0, _waitReturnTicks - _waitStartTicks);
        long intervalATicks = _enqueueCorrelation == EnqueueCorrelation.Resolved ? Math.Max(0, _firstEnqueueTicks - _waitStartTicks) : -1;
        long intervalBTicks = _enqueueCorrelation == EnqueueCorrelation.Resolved ? Math.Max(0, _batchEligibleTicks - _firstEnqueueTicks) : -1;
        long intervalC0Ticks = _enqueueCorrelation == EnqueueCorrelation.Resolved ? Math.Max(0, _firstEnqueueTicks - _channelWriteStartTicks) : -1;
        long intervalCTicks = _enqueueCorrelation == EnqueueCorrelation.Resolved ? Math.Max(0, _waitReturnTicks - _batchEligibleTicks) : -1;
        long intervalDTicks = Math.Max(0, _tryReadStartTicks - _waitReturnTicks);

        int queueDepthBeforeTryRead = _queueDepthBeforeTryRead;
        long channelWriteStartTicks = _channelWriteStartTicks;
        long firstEnqueueTicks = _firstEnqueueTicks;
        long batchEligibleTicks = _batchEligibleTicks;
        long threadPoolPendingAtWrite = _threadPoolPendingAtWrite;
        int consumersWaitingAtWrite = _consumersWaitingAtWrite;
        EnqueueCorrelation correlation = _enqueueCorrelation;
        string? waitStartStack = _waitStartStack;
        string? waitReturnStack = _waitReturnStack;
        string? tryReadStartStack = _tryReadStartStack;

        _owner.RecordLongWait(
            ordinal => new LongWaitRecord(
                Ordinal: ordinal,
                ConsumerId: ConsumerId,
                WaitStartThreadId: _waitStartThreadId,
                WaitReturnThreadId: _waitReturnThreadId,
                WaitStartTaskId: _waitStartTaskId,
                WaitReturnTaskId: _waitReturnTaskId,
                WaitStartUtc: _owner.ToUtcTimestamp(_waitStartTicks),
                WaitStartMs: _owner.ElapsedMilliseconds(_waitStartTicks),
                ChannelWriteStartMs: correlation == EnqueueCorrelation.Resolved ? _owner.ElapsedMilliseconds(channelWriteStartTicks) : double.NaN,
                FirstEnqueueMs: correlation == EnqueueCorrelation.Resolved ? _owner.ElapsedMilliseconds(firstEnqueueTicks) : double.NaN,
                BatchEligibleMs: correlation == EnqueueCorrelation.Resolved ? _owner.ElapsedMilliseconds(batchEligibleTicks) : double.NaN,
                WaitReturnMs: _owner.ElapsedMilliseconds(_waitReturnTicks),
                TryReadStartMs: _owner.ElapsedMilliseconds(_tryReadStartTicks),
                TryReadEndMs: _owner.ElapsedMilliseconds(tryReadEndTicks),
                EnqueueCorrelation: correlation.ToString(),
                WaitCompletedSynchronously: _waitCompletedSynchronously,
                WaitResult: _waitResult,
                TryReadResult: tryReadResult,
                QueueDepthAtWaitStart: _queueDepthAtWaitStart,
                QueueBytesAtWaitStart: _queueBytesAtWaitStart,
                QueueDepthAtWaitReturn: _queueDepthAtWaitReturn,
                QueueBytesAtWaitReturn: _queueBytesAtWaitReturn,
                QueueDepthBeforeTryRead: queueDepthBeforeTryRead,
                QueueDepthAfterTryRead: queueDepthAfter,
                ConcurrentWaitersAtWaitStart: _waitersAtWaitStart,
                ConcurrentWaitersAtWaitReturn: Math.Max(0, _waitersAtWaitReturn),
                IntervalAWaitStartToFirstEnqueueUs: intervalATicks < 0 ? double.NaN : MetricMathHelpers.TicksToUs(intervalATicks),
                IntervalBFirstEnqueueToBatchEligibleUs: intervalBTicks < 0 ? double.NaN : MetricMathHelpers.TicksToUs(intervalBTicks),
                IntervalC0ChannelWriteAsyncDurationUs: intervalC0Ticks < 0 ? double.NaN : MetricMathHelpers.TicksToUs(intervalC0Ticks),
                IntervalCBatchEligibleToWaitReturnUs: intervalCTicks < 0 ? double.NaN : MetricMathHelpers.TicksToUs(intervalCTicks),
                IntervalDWaitReturnToTryReadStartUs: MetricMathHelpers.TicksToUs(intervalDTicks),
                IntervalETryReadDurationUs: MetricMathHelpers.TicksToUs(tryReadTicks),
                TotalWaitUs: MetricMathHelpers.TicksToUs(totalWaitTicks),
                ThreadPoolThreadCountAtWaitReturn: _threadPoolThreadCountAtWaitReturn,
                ThreadPoolAvailableWorkerThreadsAtWaitReturn: _threadPoolAvailableWorkersAtWaitReturn,
                ThreadPoolAvailableCompletionPortThreadsAtWaitReturn: _threadPoolAvailableCompletionPortsAtWaitReturn,
                ThreadPoolPendingWorkItemsAtWaitReturn: _threadPoolPendingWorkItemsAtWaitReturn,
                ThreadPoolPendingWorkItemsAtChannelWrite: threadPoolPendingAtWrite,
                ConsumersWaitingAtChannelWrite: consumersWaitingAtWrite,
                SynchronizationContextAtWaitReturn: _syncContextAtWaitReturn,
                TaskSchedulerAtWaitReturn: _taskSchedulerAtWaitReturn,
                WaitStartStack: waitStartStack,
                WaitReturnStack: waitReturnStack,
                TryReadStartStack: tryReadStartStack),
            intervalATicks,
            intervalBTicks,
            intervalC0Ticks,
            intervalCTicks,
            intervalDTicks,
            tryReadTicks,
            totalWaitTicks);
    }

    private QueueConsumerStackSample BuildSample(
        string phase,
        long ticks,
        int queueDepth,
        long queueBytes,
        int concurrentWaiters,
        int concurrentTryReads,
        string stack)
    {
        ThreadPool.GetAvailableThreads(out int availableWorkers, out int availableCompletionPorts);

        return new QueueConsumerStackSample(
            Phase: phase,
            WaiterBucket: _sampleBucket,
            ConsumerId: ConsumerId,
            ManagedThreadId: Environment.CurrentManagedThreadId,
            TaskId: Task.CurrentId,
            TimestampUtc: _owner.ToUtcTimestamp(ticks),
            ElapsedMillisecondsSinceStart: _owner.ElapsedMilliseconds(ticks),
            QueueDepth: queueDepth,
            QueueBytes: queueBytes,
            ConsumerState: State.ToString(),
            ConcurrentWaiters: concurrentWaiters,
            ConcurrentTryReads: concurrentTryReads,
            SynchronizationContext: SynchronizationContext.Current?.GetType().FullName ?? "(none)",
            TaskScheduler: TaskScheduler.Current.GetType().FullName ?? "(unknown)",
            ThreadPoolThreadCount: ThreadPool.ThreadCount,
            ThreadPoolAvailableWorkerThreads: availableWorkers,
            ThreadPoolAvailableCompletionPortThreads: availableCompletionPorts,
            ThreadPoolPendingWorkItemCount: ThreadPool.PendingWorkItemCount,
            StackTrace: stack);
    }
}
