using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Runs three synthetic channel experiments to measure the effect of <see cref="UnboundedChannelOptions.AllowSynchronousContinuations"/>,
/// a single-reader control, and a single-reader batched-drain control.
/// </summary>
/// <remarks>
/// <para>
/// Experiment 1 — AllowSynchronousContinuations A/B: identical consumer/producer sweep (1–512 consumers, 1 and 4 producers)
/// with <c>AllowSynchronousContinuations=false</c> (control) and <c>AllowSynchronousContinuations=true</c> (experiment).
/// Tracks per-item thread identity for both the producer side and the consumer side to detect inline execution of
/// consumer continuations on the producer thread.
/// </para>
/// <para>
/// Experiment 2 — Single-reader control: exactly one consumer, 1/4/32 producers.
/// </para>
/// <para>
/// Experiment 3 — Single-reader batched-drain: exactly one consumer that drains all available items after each
/// <c>WaitToReadAsync</c>, matching the <c>while(TryRead) consume</c> production pattern.
/// </para>
/// </remarks>
internal static class SyntheticChannelContinuationAbRunner
{
    private static readonly int[] AbConsumerCounts = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512];
    private static readonly int[] AbProducerCounts = [1, 4];
    private static readonly int[] SingleReaderProducerCounts = [1, 4, 32];

    /// <summary>
    /// Executes the full A/B experiment matrix and emits a JSON evidence artifact.
    /// </summary>
    /// <param name="options">The experiment configuration.</param>
    /// <returns>A task that represents the asynchronous experiment.</returns>
    internal static async Task RunAsync(SyntheticChannelContinuationAbOptions options)
    {
        List<AbTrial> abTrials = [];

        // ---------------------------------------------------------------
        // Experiment 1 — AllowSynchronousContinuations A/B
        // ---------------------------------------------------------------
        foreach (bool allowSync in new[] { false, true })
        {
            foreach (int consumers in AbConsumerCounts)
            {
                foreach (int producers in AbProducerCounts)
                {
                    string tag = $"Exp1 allowSync={allowSync} consumers={consumers} producers={producers}";
                    Console.WriteLine($"[{tag}] warm-up...");
                    _ = await RunAbTrialAsync(consumers, producers, allowSync, options.WarmupWaves, collectMeasurements: false, batched: false).ConfigureAwait(false);
                    for (int t = 1; t <= options.Trials; t++)
                    {
                        Console.WriteLine($"[{tag}] trial {t}/{options.Trials}...");
                        abTrials.Add(await RunAbTrialAsync(consumers, producers, allowSync, options.MeasuredWaves, collectMeasurements: true, batched: false).ConfigureAwait(false));
                    }
                }
            }
        }

        // ---------------------------------------------------------------
        // Experiment 2 — Single-reader control
        // ---------------------------------------------------------------
        List<AbTrial> singleReaderTrials = [];
        foreach (int producers in SingleReaderProducerCounts)
        {
            string tag = $"Exp2 single-reader producers={producers}";
            Console.WriteLine($"[{tag}] warm-up...");
            _ = await RunAbTrialAsync(1, producers, allowSynchronousContinuations: false, options.WarmupWaves, collectMeasurements: false, batched: false).ConfigureAwait(false);
            for (int t = 1; t <= options.Trials; t++)
            {
                Console.WriteLine($"[{tag}] trial {t}/{options.Trials}...");
                singleReaderTrials.Add(await RunAbTrialAsync(1, producers, allowSynchronousContinuations: false, options.MeasuredWaves, collectMeasurements: true, batched: false).ConfigureAwait(false));
            }
        }

        // ---------------------------------------------------------------
        // Experiment 3 — Single-reader batched drain
        // ---------------------------------------------------------------
        List<AbTrial> batchedDrainTrials = [];
        foreach (int producers in SingleReaderProducerCounts)
        {
            string tag = $"Exp3 batched-drain producers={producers}";
            Console.WriteLine($"[{tag}] warm-up...");
            _ = await RunAbTrialAsync(1, producers, allowSynchronousContinuations: false, options.WarmupWaves, collectMeasurements: false, batched: true).ConfigureAwait(false);
            for (int t = 1; t <= options.Trials; t++)
            {
                Console.WriteLine($"[{tag}] trial {t}/{options.Trials}...");
                batchedDrainTrials.Add(await RunAbTrialAsync(1, producers, allowSynchronousContinuations: false, options.MeasuredWaves, collectMeasurements: true, batched: true).ConfigureAwait(false));
            }
        }

        // ---------------------------------------------------------------
        // Emit artifact
        // ---------------------------------------------------------------
        var report = new AbReport(
            Stopwatch.Frequency,
            options.WarmupWaves,
            options.MeasuredWaves,
            options.Trials,
            abTrials,
            singleReaderTrials,
            batchedDrainTrials);

        string path = Path.Combine(AppContext.BaseDirectory, "synthetic-channel-continuation-ab.json");
        File.WriteAllText(path, JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"\nEvidence artifact: {path}");

        // ---------------------------------------------------------------
        // Console summary
        // ---------------------------------------------------------------
        Console.WriteLine("\n=== Experiment 1 — AllowSynchronousContinuations A/B ===");
        PrintAbSummary(abTrials);
        Console.WriteLine("\n=== Experiment 2 — Single-reader control ===");
        PrintAbSummary(singleReaderTrials);
        Console.WriteLine("\n=== Experiment 3 — Single-reader batched drain ===");
        PrintAbSummary(batchedDrainTrials);
    }

    /// <summary>
    /// Prints a compact summary row for each trial.
    /// </summary>
    /// <param name="trials">The trials to summarize.</param>
    private static void PrintAbSummary(IEnumerable<AbTrial> trials)
    {
        Console.WriteLine($"{"sync",5} {"cons",4} {"prod",4} {"reads",7} {"failed",6} {"maxW",4} {"syncW",5} {"asyncW",6} {"mig",4} " +
                          $"{"C p50 µs",9} {"C p95 µs",9} {"C p99 µs",9} {"C max µs",9} {"thr/s",10} {"prodOnC",7} {"pendW",6}");
        foreach (AbTrial t in trials)
        {
            Console.WriteLine(
                $"{t.AllowSynchronousContinuations,5} {t.ConsumerCount,4} {t.ProducerCount,4} {t.SuccessfulReads,7} {t.FailedReads,6} " +
                $"{t.MaxSimultaneousWaiters,4} {t.SynchronousWaits,5} {t.AsynchronousWaits,6} {t.ConsumerThreadMigrations,4} " +
                $"{t.WaitToResumeMicroseconds.P50,9:F2} {t.WaitToResumeMicroseconds.P95,9:F2} " +
                $"{t.WaitToResumeMicroseconds.P99,9:F2} {t.WaitToResumeMicroseconds.Max,9:F2} " +
                $"{t.ItemsPerSecond,10:F0} {t.ProducerInlineContinuations,7} {t.MaxPendingWorkItems,6}");
        }
    }

    // ===================================================================
    //  Trial runner
    // ===================================================================

    /// <summary>
    /// Executes one wave-based trial, optionally with synchronous continuations and optional batched drain.
    /// </summary>
    /// <param name="consumerCount">The number of concurrent consumers (must be exactly 1 for batched).</param>
    /// <param name="producerCount">The number of concurrent producer tasks per wave.</param>
    /// <param name="allowSynchronousContinuations">The channel continuation mode under test.</param>
    /// <param name="waves">The number of independent waves.</param>
    /// <param name="collectMeasurements">Whether to retain per-item timing samples.</param>
    /// <param name="batched">Whether the single consumer drains all available items per wake-up.</param>
    /// <returns>The completed trial evidence.</returns>
    private static async Task<AbTrial> RunAbTrialAsync(
        int consumerCount,
        int producerCount,
        bool allowSynchronousContinuations,
        int waves,
        bool collectMeasurements,
        bool batched)
    {
        // Items written per wave = consumerCount (or 1 for single-reader batched, where producers write consumerCount=1 item each wave).
        int itemsPerWave = consumerCount;
        int totalItems = waves * itemsPerWave;

        Channel<AbItem> channel = Channel.CreateUnbounded<AbItem>(new UnboundedChannelOptions
        {
            SingleWriter = producerCount == 1,
            SingleReader = consumerCount == 1,
            AllowSynchronousContinuations = allowSynchronousContinuations,
        });

        AbTrialState state = new(consumerCount, totalItems, collectMeasurements, allowSynchronousContinuations);
        Task waiterQuorum = state.WaitForWaiterQuorumAsync();

        Task[] consumers = new Task[consumerCount];
        for (int id = 0; id < consumerCount; id++)
        {
            int capturedId = id;
            consumers[id] = Task.Run(() => batched
                ? ConsumeBatchedAsync(channel.Reader, state, capturedId)
                : ConsumeOneAsync(channel.Reader, state, capturedId));
        }

        Process process = Process.GetCurrentProcess();
        TimeSpan cpuStart = process.TotalProcessorTime;
        long startTs = Stopwatch.GetTimestamp();

        for (int wave = 0; wave < waves; wave++)
        {
            await waiterQuorum.ConfigureAwait(false);
            waiterQuorum = state.WaitForWaiterQuorumAsync();

            Task[] producers = new Task[producerCount];
            for (int pid = 0; pid < producerCount; pid++)
            {
                int first = pid * itemsPerWave / producerCount;
                int lastExcl = (pid + 1) * itemsPerWave / producerCount;
                int capturedPid = pid;
                producers[pid] = Task.Run(() => ProduceRange(channel.Writer, state, first, lastExcl, capturedPid));
            }

            await Task.WhenAll(producers).ConfigureAwait(false);
        }

        channel.Writer.TryComplete();
        await Task.WhenAll(consumers).ConfigureAwait(false);

        long endTs = Stopwatch.GetTimestamp();
        TimeSpan cpuElapsed = process.TotalProcessorTime - cpuStart;
        return state.BuildTrial(consumerCount, producerCount, Stopwatch.GetElapsedTime(startTs, endTs), cpuElapsed);
    }

    // ===================================================================
    //  Producer
    // ===================================================================

    /// <summary>
    /// Writes the producer's assigned item range synchronously, recording the producer thread for each item.
    /// </summary>
    /// <param name="writer">The channel writer.</param>
    /// <param name="state">The trial state.</param>
    /// <param name="first">The inclusive item slot.</param>
    /// <param name="lastExclusive">The exclusive item slot.</param>
    /// <param name="producerId">The logical producer identifier.</param>
    private static void ProduceRange(ChannelWriter<AbItem> writer, AbTrialState state, int first, int lastExclusive, int producerId)
    {
        int producerThreadId = Environment.CurrentManagedThreadId;
        for (int slot = first; slot < lastExclusive; slot++)
        {
            int sequence = state.NextSequence();
            long ts = Stopwatch.GetTimestamp();
            if (!writer.TryWrite(new AbItem(sequence, ts, producerThreadId, producerId)))
            {
                throw new InvalidOperationException("Unbounded channel rejected write.");
            }
        }
    }

    // ===================================================================
    //  Consumer — one-item-per-wake
    // ===================================================================

    /// <summary>
    /// Awaits <c>WaitToReadAsync</c> then reads exactly one item per wake, matching the previous experiment's consumer.
    /// </summary>
    /// <param name="reader">The channel reader.</param>
    /// <param name="state">The trial state.</param>
    /// <param name="consumerId">The logical consumer identifier.</param>
    /// <returns>A task that represents the consumer.</returns>
    private static async Task ConsumeOneAsync(ChannelReader<AbItem> reader, AbTrialState state, int consumerId)
    {
        while (true)
        {
            int waitThreadId = Environment.CurrentManagedThreadId;
            ValueTask<bool> wait = reader.WaitToReadAsync();
            bool completedSynchronously = wait.IsCompletedSuccessfully;
            if (!completedSynchronously)
            {
                state.EnterWaiter();
            }

            bool readable = await wait.ConfigureAwait(false);
            long resumeTs = Stopwatch.GetTimestamp();
            int resumeThreadId = Environment.CurrentManagedThreadId;

            if (!completedSynchronously)
            {
                state.ExitWaiter();
            }

            if (!readable)
            {
                return;
            }

            state.EnterTryRead();
            long tryReadStart = Stopwatch.GetTimestamp();
            bool read = reader.TryRead(out AbItem item);
            long tryReadEnd = Stopwatch.GetTimestamp();
            state.ExitTryRead();

            if (!read)
            {
                state.RecordFailedRead();
                continue;
            }

            state.RecordRead(item, resumeTs, tryReadStart, tryReadEnd, waitThreadId, resumeThreadId, consumerId, completedSynchronously, drainCount: 1);
        }
    }

    // ===================================================================
    //  Consumer — batched drain
    // ===================================================================

    /// <summary>
    /// Awaits <c>WaitToReadAsync</c> then drains all available items, matching the production WaitToRead/while(TryRead) pattern.
    /// </summary>
    /// <param name="reader">The channel reader.</param>
    /// <param name="state">The trial state.</param>
    /// <param name="consumerId">The logical consumer identifier.</param>
    /// <returns>A task that represents the consumer.</returns>
    private static async Task ConsumeBatchedAsync(ChannelReader<AbItem> reader, AbTrialState state, int consumerId)
    {
        while (true)
        {
            int waitThreadId = Environment.CurrentManagedThreadId;
            ValueTask<bool> wait = reader.WaitToReadAsync();
            bool completedSynchronously = wait.IsCompletedSuccessfully;
            if (!completedSynchronously)
            {
                state.EnterWaiter();
            }

            bool readable = await wait.ConfigureAwait(false);
            long resumeTs = Stopwatch.GetTimestamp();
            int resumeThreadId = Environment.CurrentManagedThreadId;

            if (!completedSynchronously)
            {
                state.ExitWaiter();
            }

            if (!readable)
            {
                return;
            }

            int drainCount = 0;
            state.EnterTryRead();
            long tryReadStart = Stopwatch.GetTimestamp();
            while (reader.TryRead(out AbItem item))
            {
                long tryReadEnd = Stopwatch.GetTimestamp();
                state.ExitTryRead();
                drainCount++;
                state.RecordRead(item, resumeTs, tryReadStart, tryReadEnd, waitThreadId, resumeThreadId, consumerId, completedSynchronously, drainCount);
                state.EnterTryRead();
                tryReadStart = Stopwatch.GetTimestamp();
            }

            // Exited because TryRead returned false — decrement the final phantom EnterTryRead.
            state.ExitTryRead();
            if (drainCount == 0)
            {
                state.RecordFailedRead();
            }
        }
    }
}

// ===================================================================
//  Options
// ===================================================================

/// <summary>
/// Parses the command surface for the continuation A/B experiment.
/// </summary>
internal readonly record struct SyntheticChannelContinuationAbOptions(int WarmupWaves, int MeasuredWaves, int Trials)
{
    /// <summary>
    /// Parses command-line options after <c>channel-continuation-ab</c>.
    /// </summary>
    /// <param name="args">The command-line tokens.</param>
    /// <returns>The validated experiment options.</returns>
    internal static SyntheticChannelContinuationAbOptions Parse(string[] args)
    {
        int warmup = 10;
        int measured = 100;
        int trials = 3;
        for (int i = 0; i < args.Length; i += 2)
        {
            if (i + 1 >= args.Length)
            {
                throw new ArgumentException($"Missing value for option '{args[i]}'.");
            }

            if (!int.TryParse(args[i + 1], out int v) || v <= 0)
            {
                throw new ArgumentException($"Option '{args[i]}' requires a positive integer.");
            }

            switch (args[i])
            {
                case "--warmup-waves": warmup = v; break;
                case "--measured-waves": measured = v; break;
                case "--trials": trials = v; break;
                default: throw new ArgumentException($"Unknown option '{args[i]}'.");
            }
        }

        return new SyntheticChannelContinuationAbOptions(warmup, measured, trials);
    }
}

// ===================================================================
//  Trial state
// ===================================================================

/// <summary>
/// Thread-safe counters and fixed-size sample store for one isolated trial.
/// </summary>
internal sealed class AbTrialState
{
    private readonly int _consumerCount;
    private readonly AbSample[] _samples;
    private readonly bool _collectMeasurements;
    private readonly bool _allowSynchronousContinuations;
    private TaskCompletionSource _waiterQuorum = NewQuorum();
    private int _waiters;
    private int _maxWaiters;
    private int _tryReads;
    private int _maxTryReads;
    private int _sequence;
    private long _synchronousWaits;
    private long _asynchronousWaits;
    private long _successfulReads;
    private long _failedReads;
    private int _maxThreadCount;
    private long _maxPendingWorkItems;
    private int _minimumAvailableWorkers = int.MaxValue;
    private long _producerInlineContinuations;
    private long _totalDrained;

    /// <summary>
    /// Initializes fixed-capacity state for one trial.
    /// </summary>
    /// <param name="consumerCount">The fixed consumer count.</param>
    /// <param name="totalItems">The fixed produced-item count.</param>
    /// <param name="collectMeasurements">Whether sample records should be populated.</param>
    /// <param name="allowSynchronousContinuations">The channel option under test.</param>
    internal AbTrialState(int consumerCount, int totalItems, bool collectMeasurements, bool allowSynchronousContinuations)
    {
        _consumerCount = consumerCount;
        _samples = new AbSample[totalItems];
        _collectMeasurements = collectMeasurements;
        _allowSynchronousContinuations = allowSynchronousContinuations;
    }

    /// <summary>Returns a task that completes once all consumers are parked in the current wave.</summary>
    /// <returns>The waiter-quorum task.</returns>
    internal Task WaitForWaiterQuorumAsync() => _waiterQuorum.Task;

    /// <summary>Allocates the next unique item sequence number.</summary>
    /// <returns>The zero-based sequence.</returns>
    internal int NextSequence() => Interlocked.Increment(ref _sequence) - 1;

    /// <summary>Records entry to an asynchronous channel wait.</summary>
    internal void EnterWaiter()
    {
        Interlocked.Increment(ref _asynchronousWaits);
        int current = Interlocked.Increment(ref _waiters);
        UpdateMax(ref _maxWaiters, current);
        if (current == _consumerCount)
        {
            TaskCompletionSource q = Interlocked.Exchange(ref _waiterQuorum, NewQuorum());
            q.TrySetResult();
        }
    }

    /// <summary>Records return from an asynchronous channel wait.</summary>
    internal void ExitWaiter() => Interlocked.Decrement(ref _waiters);

    /// <summary>Records entry into the TryRead window.</summary>
    internal void EnterTryRead()
    {
        int current = Interlocked.Increment(ref _tryReads);
        UpdateMax(ref _maxTryReads, current);
    }

    /// <summary>Records exit from the TryRead window.</summary>
    internal void ExitTryRead() => Interlocked.Decrement(ref _tryReads);

    /// <summary>Records an unsuccessful TryRead after a readable signal.</summary>
    internal void RecordFailedRead() => Interlocked.Increment(ref _failedReads);

    /// <summary>
    /// Records one consumed item along with continuation thread-identity and timing.
    /// </summary>
    /// <param name="item">The item carrying producer metadata.</param>
    /// <param name="resumeTs">Timestamp after the await resumed.</param>
    /// <param name="tryReadStart">Timestamp before TryRead.</param>
    /// <param name="tryReadEnd">Timestamp after TryRead.</param>
    /// <param name="waitThreadId">Thread at wait registration.</param>
    /// <param name="resumeThreadId">Thread at continuation resumption.</param>
    /// <param name="consumerId">Logical consumer identifier.</param>
    /// <param name="completedSynchronously">Whether WaitToReadAsync returned synchronously.</param>
    /// <param name="drainCount">Position within a drain batch (1 = first/only item in batch).</param>
    internal void RecordRead(
        AbItem item,
        long resumeTs,
        long tryReadStart,
        long tryReadEnd,
        int waitThreadId,
        int resumeThreadId,
        int consumerId,
        bool completedSynchronously,
        int drainCount)
    {
        Interlocked.Increment(ref _successfulReads);
        Interlocked.Add(ref _totalDrained, drainCount);
        if (completedSynchronously)
        {
            Interlocked.Increment(ref _synchronousWaits);
        }

        // Detect whether the consumer continuation ran inline on the producer thread.
        // This is only possible when AllowSynchronousContinuations=true.
        bool producerInline = resumeThreadId == item.ProducerThreadId;
        if (producerInline)
        {
            Interlocked.Increment(ref _producerInlineContinuations);
        }

        ThreadPool.GetAvailableThreads(out int available, out _);
        UpdateMax(ref _maxThreadCount, ThreadPool.ThreadCount);
        UpdateMax(ref _maxPendingWorkItems, ThreadPool.PendingWorkItemCount);
        UpdateMin(ref _minimumAvailableWorkers, available);

        if (_collectMeasurements && item.Sequence < _samples.Length)
        {
            _samples[item.Sequence] = new AbSample(
                item.AvailableTimestamp,
                resumeTs,
                tryReadStart,
                tryReadEnd,
                waitThreadId,
                resumeThreadId,
                item.ProducerThreadId,
                item.ProducerId,
                consumerId,
                completedSynchronously,
                producerInline,
                drainCount);
        }
    }

    /// <summary>
    /// Builds the immutable trial evidence record after all consumers have completed.
    /// </summary>
    /// <param name="consumerCount">The trial consumer count.</param>
    /// <param name="producerCount">The trial producer count.</param>
    /// <param name="elapsed">Wall-clock trial duration.</param>
    /// <param name="cpuElapsed">Process CPU time for the trial.</param>
    /// <returns>The aggregate evidence record.</returns>
    internal AbTrial BuildTrial(int consumerCount, int producerCount, TimeSpan elapsed, TimeSpan cpuElapsed)
    {
        List<double> waitToResume = [];
        List<double> resumeToTryRead = [];
        List<double> tryRead = [];
        int migrations = 0;
        foreach (AbSample s in _samples)
        {
            if (s.AvailableTimestamp == 0)
            {
                continue;
            }

            waitToResume.Add(ToUs(s.ResumeTimestamp - s.AvailableTimestamp));
            resumeToTryRead.Add(ToUs(s.TryReadStart - s.ResumeTimestamp));
            tryRead.Add(ToUs(s.TryReadEnd - s.TryReadStart));
            if (s.WaitThreadId != s.ResumeThreadId)
            {
                migrations++;
            }
        }

        double throughput = elapsed.TotalSeconds > 0
            ? Interlocked.Read(ref _successfulReads) / elapsed.TotalSeconds
            : 0;
        double cpu = elapsed.TotalMilliseconds > 0
            ? cpuElapsed.TotalMilliseconds / (elapsed.TotalMilliseconds * Environment.ProcessorCount) * 100
            : 0;

        return new AbTrial(
            _allowSynchronousContinuations,
            consumerCount,
            producerCount,
            Interlocked.Read(ref _successfulReads),
            Interlocked.Read(ref _failedReads),
            Interlocked.Read(ref _synchronousWaits),
            Interlocked.Read(ref _asynchronousWaits),
            _maxWaiters,
            _maxTryReads,
            _maxThreadCount,
            _minimumAvailableWorkers == int.MaxValue ? 0 : _minimumAvailableWorkers,
            _maxPendingWorkItems,
            migrations,
            Interlocked.Read(ref _producerInlineContinuations),
            Interlocked.Read(ref _totalDrained),
            throughput,
            cpu,
            CalcDist(waitToResume),
            CalcDist(resumeToTryRead),
            CalcDist(tryRead));
    }

    private static AbLatencyDist CalcDist(List<double> v)
    {
        if (v.Count == 0)
        {
            return default;
        }

        v.Sort();
        return new AbLatencyDist(
            v.Count,
            Pct(v, 0.50),
            Pct(v, 0.95),
            Pct(v, 0.99),
            Pct(v, 0.999),
            v[^1]);
    }

    private static double Pct(IReadOnlyList<double> v, double p)
    {
        int idx = Math.Clamp((int)Math.Ceiling(v.Count * p) - 1, 0, v.Count - 1);
        return v[idx];
    }

    private static double ToUs(long ticks) => ticks * 1_000_000d / Stopwatch.Frequency;

    private static void UpdateMax(ref int loc, int candidate)
    {
        while (true)
        {
            int cur = Volatile.Read(ref loc);
            if (candidate <= cur || Interlocked.CompareExchange(ref loc, candidate, cur) == cur)
            {
                return;
            }
        }
    }

    private static void UpdateMax(ref long loc, long candidate)
    {
        while (true)
        {
            long cur = Interlocked.Read(ref loc);
            if (candidate <= cur || Interlocked.CompareExchange(ref loc, candidate, cur) == cur)
            {
                return;
            }
        }
    }

    private static void UpdateMin(ref int loc, int candidate)
    {
        while (true)
        {
            int cur = Volatile.Read(ref loc);
            if (candidate >= cur || Interlocked.CompareExchange(ref loc, candidate, cur) == cur)
            {
                return;
            }
        }
    }

    private static TaskCompletionSource NewQuorum() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

// ===================================================================
//  Data types
// ===================================================================

/// <summary>
/// Carries producer-side identity and availability timestamp through the isolated channel.
/// </summary>
/// <param name="Sequence">Unique sample slot.</param>
/// <param name="AvailableTimestamp">Timestamp immediately before the successful TryWrite.</param>
/// <param name="ProducerThreadId">Managed thread that called TryWrite.</param>
/// <param name="ProducerId">Logical producer identifier.</param>
internal readonly record struct AbItem(int Sequence, long AvailableTimestamp, int ProducerThreadId, int ProducerId);

/// <summary>
/// Holds per-item timing, thread-identity, and continuation-mode observations.
/// </summary>
internal readonly record struct AbSample(
    long AvailableTimestamp,
    long ResumeTimestamp,
    long TryReadStart,
    long TryReadEnd,
    int WaitThreadId,
    int ResumeThreadId,
    int ProducerThreadId,
    int ProducerId,
    int ConsumerId,
    bool CompletedSynchronously,
    bool ProducerInline,
    int DrainCount);

/// <summary>
/// Percentile latency distribution in microseconds with P99.9.
/// </summary>
internal readonly record struct AbLatencyDist(int Count, double P50, double P95, double P99, double P999, double Max);

/// <summary>
/// Aggregate evidence for one isolated channel trial.
/// </summary>
internal readonly record struct AbTrial(
    bool AllowSynchronousContinuations,
    int ConsumerCount,
    int ProducerCount,
    long SuccessfulReads,
    long FailedReads,
    long SynchronousWaits,
    long AsynchronousWaits,
    int MaxSimultaneousWaiters,
    int MaxSimultaneousTryReads,
    int MaxThreadPoolThreadCount,
    int MinimumAvailableWorkerThreads,
    long MaxPendingWorkItems,
    int ConsumerThreadMigrations,
    long ProducerInlineContinuations,
    long TotalDrained,
    double ItemsPerSecond,
    double CpuUtilizationPercent,
    AbLatencyDist WaitToResumeMicroseconds,
    AbLatencyDist ResumeToTryReadMicroseconds,
    AbLatencyDist TryReadMicroseconds);

/// <summary>
/// Complete evidence artifact for the A/B continuation experiment.
/// </summary>
internal readonly record struct AbReport(
    long StopwatchFrequency,
    int WarmupWaves,
    int MeasuredWaves,
    int TrialsPerTopology,
    IReadOnlyList<AbTrial> AbTrials,
    IReadOnlyList<AbTrial> SingleReaderTrials,
    IReadOnlyList<AbTrial> BatchedDrainTrials);
