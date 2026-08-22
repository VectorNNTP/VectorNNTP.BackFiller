using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Runs an isolated <see cref="Channel{T}"/> waiter wake-up experiment without application pipeline components.
/// </summary>
internal static class SyntheticChannelWakeupRunner
{
    private static readonly int[] ConsumerCounts = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512];
    private static readonly int[] ProducerCounts = [1, 4];

    /// <summary>
    /// Executes warm-up and independent measured trials, then writes a machine-readable evidence artifact.
    /// </summary>
    /// <param name="options">The bounded experiment configuration.</param>
    /// <returns>A task that represents the asynchronous experiment.</returns>
    internal static async Task RunAsync(SyntheticChannelWakeupOptions options)
    {
        List<SyntheticChannelWakeupTrial> trials = [];
        foreach (int consumerCount in ConsumerCounts)
        {
            foreach (int producerCount in ProducerCounts)
            {
                Console.WriteLine($"Starting consumers={consumerCount} producers={producerCount} warmup.");
                _ = await RunTrialAsync(consumerCount, producerCount, options.WarmupWaves, collectMeasurements: false).ConfigureAwait(false);
                for (int trial = 1; trial <= options.Trials; trial++)
                {
                    Console.WriteLine($"Starting consumers={consumerCount} producers={producerCount} trial={trial}.");
                    trials.Add(await RunTrialAsync(consumerCount, producerCount, options.MeasuredWaves, collectMeasurements: true).ConfigureAwait(false));
                }
            }
        }

        SyntheticChannelWakeupReport report = new(
            Stopwatch.Frequency,
            options.WarmupWaves,
            options.MeasuredWaves,
            options.Trials,
            trials);
        string path = Path.Combine(AppContext.BaseDirectory, "synthetic-channel-wakeup-forensics.json");
        File.WriteAllText(path, JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"Synthetic channel wake-up evidence: {path}");
        foreach (SyntheticChannelWakeupTrial trial in trials)
        {
            Console.WriteLine(
                $"consumers={trial.ConsumerCount,3} producers={trial.ProducerCount} items={trial.SuccessfulReads,6} " +
                $"C us p50/p95/p99/max={trial.WaitToResumeMicroseconds.P50:F2}/{trial.WaitToResumeMicroseconds.P95:F2}/" +
                $"{trial.WaitToResumeMicroseconds.P99:F2}/{trial.WaitToResumeMicroseconds.Max:F2} throughput={trial.ItemsPerSecond:F0}/s");
        }
    }

    /// <summary>
    /// Runs a wave-based trial where every consumer is asynchronously parked before producers make work available.
    /// </summary>
    /// <param name="consumerCount">The number of channel consumers.</param>
    /// <param name="producerCount">The number of concurrent producer tasks per wave.</param>
    /// <param name="waves">The number of independent waiter-population waves.</param>
    /// <param name="collectMeasurements">Whether to retain per-item timing samples.</param>
    /// <returns>The completed trial evidence.</returns>
    private static async Task<SyntheticChannelWakeupTrial> RunTrialAsync(int consumerCount, int producerCount, int waves, bool collectMeasurements)
    {
        Channel<SyntheticItem> channel = Channel.CreateUnbounded<SyntheticItem>(
            new UnboundedChannelOptions { SingleWriter = producerCount == 1, SingleReader = consumerCount == 1, AllowSynchronousContinuations = false });
        TrialState state = new(consumerCount, waves * consumerCount, collectMeasurements);
        Task waiterQuorum = state.WaitForWaiterQuorumAsync();
        Task[] consumers = new Task[consumerCount];
        for (int consumerId = 0; consumerId < consumerCount; consumerId++)
        {
            int capturedConsumerId = consumerId;
            consumers[consumerId] = Task.Run(() => ConsumeAsync(channel.Reader, state, capturedConsumerId));
        }

        Process process = Process.GetCurrentProcess();
        TimeSpan cpuStart = process.TotalProcessorTime;
        long start = Stopwatch.GetTimestamp();
        for (int wave = 0; wave < waves; wave++)
        {
            await waiterQuorum.ConfigureAwait(false);
            waiterQuorum = state.WaitForWaiterQuorumAsync();
            Task[] producers = new Task[producerCount];
            for (int producerId = 0; producerId < producerCount; producerId++)
            {
                int first = producerId * consumerCount / producerCount;
                int lastExclusive = (producerId + 1) * consumerCount / producerCount;
                producers[producerId] = Task.Run(() => ProduceRange(channel.Writer, state, first, lastExclusive));
            }

            await Task.WhenAll(producers).ConfigureAwait(false);
        }

        channel.Writer.TryComplete();
        await Task.WhenAll(consumers).ConfigureAwait(false);
        long end = Stopwatch.GetTimestamp();
        TimeSpan cpuElapsed = process.TotalProcessorTime - cpuStart;
        return state.BuildTrial(consumerCount, producerCount, Stopwatch.GetElapsedTime(start, end), cpuElapsed);
    }

    /// <summary>
    /// Makes a producer's assigned range of a wave available using only synchronous channel writes.
    /// </summary>
    /// <param name="writer">The channel writer.</param>
    /// <param name="state">The trial state.</param>
    /// <param name="first">The inclusive consumer slot.</param>
    /// <param name="lastExclusive">The exclusive consumer slot.</param>
    private static void ProduceRange(ChannelWriter<SyntheticItem> writer, TrialState state, int first, int lastExclusive)
    {
        for (int slot = first; slot < lastExclusive; slot++)
        {
            int sequence = state.NextSequence();
            long availableTimestamp = Stopwatch.GetTimestamp();
            if (!writer.TryWrite(new SyntheticItem(sequence, availableTimestamp)))
            {
                throw new InvalidOperationException("Unbounded channel rejected a write before completion.");
            }
        }
    }

    /// <summary>
    /// Repeatedly waits for an item, records the continuation boundary, and consumes exactly one item per successful wait.
    /// </summary>
    /// <param name="reader">The channel reader.</param>
    /// <param name="state">The trial state.</param>
    /// <param name="consumerId">The logical consumer identifier.</param>
    /// <returns>A task that represents the consumer.</returns>
    private static async Task ConsumeAsync(ChannelReader<SyntheticItem> reader, TrialState state, int consumerId)
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
            long resumeTimestamp = Stopwatch.GetTimestamp();
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
            bool read = reader.TryRead(out SyntheticItem item);
            long tryReadEnd = Stopwatch.GetTimestamp();
            state.ExitTryRead();
            if (!read)
            {
                state.RecordFailedRead();
                continue;
            }

            state.RecordRead(item, resumeTimestamp, tryReadStart, tryReadEnd, waitThreadId, resumeThreadId, consumerId, completedSynchronously);
        }
    }
}

/// <summary>
/// Parses the intentionally small command surface of the isolated channel experiment.
/// </summary>
internal readonly record struct SyntheticChannelWakeupOptions(int WarmupWaves, int MeasuredWaves, int Trials)
{
    /// <summary>
    /// Parses command-line options without importing application benchmark configuration.
    /// </summary>
    /// <param name="args">The command-line tokens after <c>channel-wakeup-forensic</c>.</param>
    /// <returns>The validated experiment options.</returns>
    internal static SyntheticChannelWakeupOptions Parse(string[] args)
    {
        int warmupWaves = 10;
        int measuredWaves = 100;
        int trials = 3;
        for (int index = 0; index < args.Length; index += 2)
        {
            if (index + 1 >= args.Length)
            {
                throw new ArgumentException($"Missing value for option '{args[index]}'.");
            }

            int value = ParsePositive(args[index], args[index + 1]);
            switch (args[index])
            {
                case "--warmup-waves":
                    warmupWaves = value;
                    break;
                case "--measured-waves":
                    measuredWaves = value;
                    break;
                case "--trials":
                    trials = value;
                    break;
                default:
                    throw new ArgumentException($"Unknown option '{args[index]}'.");
            }
        }

        return new SyntheticChannelWakeupOptions(warmupWaves, measuredWaves, trials);
    }

    /// <summary>
    /// Parses a positive integer option value.
    /// </summary>
    /// <param name="name">The option name.</param>
    /// <param name="value">The raw option value.</param>
    /// <returns>The parsed positive value.</returns>
    private static int ParsePositive(string name, string value)
    {
        if (!int.TryParse(value, out int parsed) || parsed <= 0)
        {
            throw new ArgumentException($"Option '{name}' requires a positive integer.");
        }

        return parsed;
    }
}

/// <summary>
/// Holds the thread-safe counters and fixed-size timing sample store for one isolated trial.
/// </summary>
internal sealed class TrialState
{
    private readonly int _consumerCount;
    private readonly SyntheticChannelWakeupSample[] _samples;
    private readonly bool _collectMeasurements;
    private TaskCompletionSource _waiterQuorum = NewWaiterQuorum();
    private int _waiters;
    private int _maxWaiters;
    private int _tryReads;
    private int _maxTryReads;
    private int _sequence;
    private long _waitCount;
    private long _synchronousWaits;
    private long _asynchronousWaits;
    private long _successfulReads;
    private long _failedReads;
    private int _maxThreadCount;
    private long _maxPendingWorkItems;
    private int _minimumAvailableWorkers = int.MaxValue;

    /// <summary>
    /// Initializes fixed-capacity state for one trial.
    /// </summary>
    /// <param name="consumerCount">The fixed consumer count.</param>
    /// <param name="totalItems">The fixed produced-item count.</param>
    /// <param name="collectMeasurements">Whether sample records should be populated.</param>
    internal TrialState(int consumerCount, int totalItems, bool collectMeasurements)
    {
        _consumerCount = consumerCount;
        _samples = new SyntheticChannelWakeupSample[totalItems];
        _collectMeasurements = collectMeasurements;
    }

    /// <summary>
    /// Returns a task that completes once all consumers are parked in the current wave.
    /// </summary>
    /// <returns>The waiter-quorum task.</returns>
    internal Task WaitForWaiterQuorumAsync() => _waiterQuorum.Task;

    /// <summary>
    /// Allocates the next unique item sequence.
    /// </summary>
    /// <returns>The zero-based item sequence.</returns>
    internal int NextSequence() => Interlocked.Increment(ref _sequence) - 1;

    /// <summary>
    /// Records entry to an asynchronous channel wait.
    /// </summary>
    internal void EnterWaiter()
    {
        Interlocked.Increment(ref _waitCount);
        Interlocked.Increment(ref _asynchronousWaits);
        int current = Interlocked.Increment(ref _waiters);
        UpdateMaximum(ref _maxWaiters, current);
        if (current == _consumerCount)
        {
            TaskCompletionSource quorum = Interlocked.Exchange(ref _waiterQuorum, NewWaiterQuorum());
            quorum.TrySetResult();
        }
    }

    /// <summary>
    /// Records return from an asynchronous channel wait.
    /// </summary>
    internal void ExitWaiter()
    {
        Interlocked.Decrement(ref _waiters);
    }

    /// <summary>
    /// Records entry into the narrow TryRead timing interval.
    /// </summary>
    internal void EnterTryRead()
    {
        int current = Interlocked.Increment(ref _tryReads);
        UpdateMaximum(ref _maxTryReads, current);
    }

    /// <summary>
    /// Records exit from the narrow TryRead timing interval.
    /// </summary>
    internal void ExitTryRead()
    {
        Interlocked.Decrement(ref _tryReads);
    }

    /// <summary>
    /// Records an unsuccessful TryRead after a readable signal.
    /// </summary>
    internal void RecordFailedRead()
    {
        Interlocked.Increment(ref _failedReads);
    }

    /// <summary>
    /// Records one consumed item and its surrounding continuation measurements.
    /// </summary>
    /// <param name="item">The consumed item carrying its availability timestamp.</param>
    /// <param name="resumeTimestamp">The timestamp immediately after the await resumes.</param>
    /// <param name="tryReadStart">The timestamp immediately before TryRead.</param>
    /// <param name="tryReadEnd">The timestamp immediately after TryRead.</param>
    /// <param name="waitThreadId">The managed thread at wait registration.</param>
    /// <param name="resumeThreadId">The managed thread at await resumption.</param>
    /// <param name="consumerId">The logical consumer identifier.</param>
    /// <param name="completedSynchronously">Whether the wait was already complete.</param>
    internal void RecordRead(
        SyntheticItem item,
        long resumeTimestamp,
        long tryReadStart,
        long tryReadEnd,
        int waitThreadId,
        int resumeThreadId,
        int consumerId,
        bool completedSynchronously)
    {
        Interlocked.Increment(ref _successfulReads);
        Interlocked.Increment(ref _waitCount);
        if (completedSynchronously)
        {
            Interlocked.Increment(ref _synchronousWaits);
        }

        ThreadPool.GetAvailableThreads(out int availableWorkers, out _);
        UpdateMaximum(ref _maxThreadCount, ThreadPool.ThreadCount);
        UpdateMaximum(ref _maxPendingWorkItems, ThreadPool.PendingWorkItemCount);
        UpdateMinimum(ref _minimumAvailableWorkers, availableWorkers);
        if (_collectMeasurements)
        {
            _samples[item.Sequence] = new SyntheticChannelWakeupSample(
                item.AvailableTimestamp,
                resumeTimestamp,
                tryReadStart,
                tryReadEnd,
                waitThreadId,
                resumeThreadId,
                consumerId,
                completedSynchronously);
        }
    }

    /// <summary>
    /// Builds immutable aggregate evidence after all consumers have completed.
    /// </summary>
    /// <param name="consumerCount">The trial consumer count.</param>
    /// <param name="producerCount">The trial producer count.</param>
    /// <param name="elapsed">The wall-clock trial duration.</param>
    /// <param name="cpuElapsed">The process CPU time consumed by the trial.</param>
    /// <returns>The aggregate evidence record.</returns>
    internal SyntheticChannelWakeupTrial BuildTrial(int consumerCount, int producerCount, TimeSpan elapsed, TimeSpan cpuElapsed)
    {
        List<double> waitToResume = [];
        List<double> resumeToTryRead = [];
        List<double> tryRead = [];
        int migrations = 0;
        foreach (SyntheticChannelWakeupSample sample in _samples)
        {
            if (sample.AvailableTimestamp == 0)
            {
                continue;
            }

            waitToResume.Add(ToMicroseconds(sample.ResumeTimestamp - sample.AvailableTimestamp));
            resumeToTryRead.Add(ToMicroseconds(sample.TryReadStart - sample.ResumeTimestamp));
            tryRead.Add(ToMicroseconds(sample.TryReadEnd - sample.TryReadStart));
            if (sample.WaitThreadId != sample.ResumeThreadId)
            {
                migrations++;
            }
        }

        return new SyntheticChannelWakeupTrial(
            consumerCount,
            producerCount,
            Interlocked.Read(ref _successfulReads),
            Interlocked.Read(ref _failedReads),
            Interlocked.Read(ref _waitCount),
            Interlocked.Read(ref _synchronousWaits),
            Interlocked.Read(ref _asynchronousWaits),
            _maxWaiters,
            _maxTryReads,
            _maxThreadCount,
            _minimumAvailableWorkers == int.MaxValue ? 0 : _minimumAvailableWorkers,
            _maxPendingWorkItems,
            migrations,
            elapsed.TotalSeconds <= 0 ? 0 : Interlocked.Read(ref _successfulReads) / elapsed.TotalSeconds,
            elapsed.TotalMilliseconds <= 0 ? 0 : cpuElapsed.TotalMilliseconds / (elapsed.TotalMilliseconds * Environment.ProcessorCount) * 100,
            CalculateDistribution(waitToResume),
            CalculateDistribution(resumeToTryRead),
            CalculateDistribution(tryRead));
    }

    /// <summary>
    /// Calculates a percentile distribution from lightweight trial samples.
    /// </summary>
    /// <param name="values">The samples in microseconds.</param>
    /// <returns>The distribution summary.</returns>
    private static SyntheticLatencyDistribution CalculateDistribution(List<double> values)
    {
        if (values.Count == 0)
        {
            return default;
        }

        values.Sort();
        return new SyntheticLatencyDistribution(
            values.Count,
            Percentile(values, 0.50),
            Percentile(values, 0.95),
            Percentile(values, 0.99),
            values[^1]);
    }

    /// <summary>
    /// Selects a nearest-rank percentile from a sorted sample collection.
    /// </summary>
    /// <param name="values">The sorted samples.</param>
    /// <param name="percentile">The requested percentile in [0, 1].</param>
    /// <returns>The selected sample.</returns>
    private static double Percentile(IReadOnlyList<double> values, double percentile)
    {
        int index = Math.Clamp((int)Math.Ceiling(values.Count * percentile) - 1, 0, values.Count - 1);
        return values[index];
    }

    /// <summary>
    /// Converts stopwatch ticks to microseconds.
    /// </summary>
    /// <param name="ticks">The elapsed stopwatch ticks.</param>
    /// <returns>The elapsed duration in microseconds.</returns>
    private static double ToMicroseconds(long ticks) => ticks * 1_000_000d / Stopwatch.Frequency;

    /// <summary>
    /// Updates an integer maximum using atomic compare-exchange.
    /// </summary>
    /// <param name="location">The maximum location.</param>
    /// <param name="candidate">The candidate value.</param>
    private static void UpdateMaximum(ref int location, int candidate)
    {
        while (true)
        {
            int current = Volatile.Read(ref location);
            if (candidate <= current || Interlocked.CompareExchange(ref location, candidate, current) == current)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Updates a long maximum using atomic compare-exchange.
    /// </summary>
    /// <param name="location">The maximum location.</param>
    /// <param name="candidate">The candidate value.</param>
    private static void UpdateMaximum(ref long location, long candidate)
    {
        while (true)
        {
            long current = Interlocked.Read(ref location);
            if (candidate <= current || Interlocked.CompareExchange(ref location, candidate, current) == current)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Updates an integer minimum using atomic compare-exchange.
    /// </summary>
    /// <param name="location">The minimum location.</param>
    /// <param name="candidate">The candidate value.</param>
    private static void UpdateMinimum(ref int location, int candidate)
    {
        while (true)
        {
            int current = Volatile.Read(ref location);
            if (candidate >= current || Interlocked.CompareExchange(ref location, candidate, current) == current)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Creates a continuation-asynchronous waiter quorum completion source.
    /// </summary>
    /// <returns>The completion source.</returns>
    private static TaskCompletionSource NewWaiterQuorum() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>
/// Carries the producer-side availability timestamp through the isolated channel.
/// </summary>
/// <param name="Sequence">The unique sample slot.</param>
/// <param name="AvailableTimestamp">The timestamp immediately before the successful channel write.</param>
internal readonly record struct SyntheticItem(int Sequence, long AvailableTimestamp);

/// <summary>
/// Holds one item's lightweight timing and thread-identity observations.
/// </summary>
internal readonly record struct SyntheticChannelWakeupSample(
    long AvailableTimestamp,
    long ResumeTimestamp,
    long TryReadStart,
    long TryReadEnd,
    int WaitThreadId,
    int ResumeThreadId,
    int ConsumerId,
    bool CompletedSynchronously);

/// <summary>
/// Represents a percentile distribution in microseconds.
/// </summary>
internal readonly record struct SyntheticLatencyDistribution(int Count, double P50, double P95, double P99, double Max);

/// <summary>
/// Represents one measured isolated-channel trial.
/// </summary>
internal readonly record struct SyntheticChannelWakeupTrial(
    int ConsumerCount,
    int ProducerCount,
    long SuccessfulReads,
    long FailedReads,
    long WaitCount,
    long SynchronousWaits,
    long AsynchronousWaits,
    int MaxSimultaneousWaiters,
    int MaxSimultaneousTryReads,
    int MaxThreadPoolThreadCount,
    int MinimumAvailableWorkerThreads,
    long MaxPendingWorkItems,
    int ConsumerThreadMigrations,
    double ItemsPerSecond,
    double CpuUtilizationPercent,
    SyntheticLatencyDistribution WaitToResumeMicroseconds,
    SyntheticLatencyDistribution ResumeToTryReadMicroseconds,
    SyntheticLatencyDistribution TryReadMicroseconds);

/// <summary>
/// Represents the complete isolated-channel evidence artifact.
/// </summary>
internal readonly record struct SyntheticChannelWakeupReport(
    long StopwatchFrequency,
    int WarmupWaves,
    int MeasuredWaves,
    int TrialsPerTopology,
    IReadOnlyList<SyntheticChannelWakeupTrial> Trials);
