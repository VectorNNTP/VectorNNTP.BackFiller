// <copyright file="Transit32WorkerExperimentRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Transit32WorkerExperimentRunner: compares generator, queue, and full-pipeline scaling at thirty-two workers.

using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the transit 32 WorkerExperimentRunner class used by the benchmark or regression gate.
/// </summary>
internal static class Transit32WorkerExperimentRunner
{
    /// <summary>
    /// Gets or sets the article Bytes.
    /// </summary>
    private const int ArticleBytes = 1024 * 1024;
    /// <summary>
    /// Gets or sets the producer Workers.
    /// </summary>
    private const int ProducerWorkers = 32;
    /// <summary>
    /// Gets or sets the dispatch Workers.
    /// </summary>
    private const int DispatchWorkers = 512;
    /// <summary>
    /// Gets or sets the queue Articles.
    /// </summary>
    private const int QueueArticles = 1024;
    /// <summary>
    /// Gets or sets the queue Bytes.
    /// </summary>
    private const long QueueBytes = 1024L * 1024L * 1024L;
    /// <summary>
    /// Gets or sets the queue Target.
    /// </summary>
    private const int QueueTarget = 512;
    /// <summary>
    /// Gets or sets the connections.
    /// </summary>
    private const int Connections = 64;
    /// <summary>
    /// Gets or sets the pipeline Depth.
    /// </summary>
    private const int PipelineDepth = 16;

    /// <summary>
    /// Gets or sets the required TransitHostname.
    /// </summary>
    private const string RequiredTransitHostname = "incoming.usenet.ninja";

    /// <summary>
    /// Runs Async.

    /// </summary>
    internal static async Task RunAsync(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("=== 32-Worker Benchmark Experiments ===");
        Console.WriteLine($"Article bytes={ArticleBytes}, ProducerWorkers={ProducerWorkers}, DispatchWorkers={DispatchWorkers}");
        Console.WriteLine($"Queue hard bounds: articles={QueueArticles}, bytes={QueueBytes}, target={QueueTarget}");

        GeneratorResult exp1 = await RunGeneratorOnlyAsync("EXPERIMENT 1 - 32-worker generator", warmupSeconds: 10, measurementSeconds: 30, ProducerWorkers, cancellationToken).ConfigureAwait(false);

        QueueResult exp2 = await RunQueueNoOpAsync("EXPERIMENT 2 - 32-worker generator + bounded queue", warmupSeconds: 10, measurementSeconds: 30, cancellationToken).ConfigureAwait(false);

        RealPipelineResult exp3 = await RunRealPipelineAsync("EXPERIMENT 3 - 32-worker real TransitPublisher pipeline", warmupSeconds: 10, measurementSeconds: 120, cancellationToken).ConfigureAwait(false);

        PrintFinalSummary(exp1, exp2, exp3);
    }

    /// <summary>
    /// Runs GeneratorOnlyAsync.

    /// </summary>
    private static async Task<GeneratorResult> RunGeneratorOnlyAsync(string label, int warmupSeconds, int measurementSeconds, int workers, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");

        await WarmupGeneratorAsync(warmupSeconds, workers, cancellationToken).ConfigureAwait(false);

        Process process = Process.GetCurrentProcess();
        CpuSampler cpu = new(process);

        long allocStart = GC.GetTotalAllocatedBytes(false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        long totalArticles = 0;
        long totalBytes = 0;
        long totalLatencyTicks = 0;
        ConcurrentBag<long> latencyTicks = [];

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        cpu.Start();
        Stopwatch wall = Stopwatch.StartNew();

        Task[] producerTasks = new Task[workers];
        for (int i = 0; i < workers; i++)
        {
            int workerId = i;
            producerTasks[i] = Task.Run(() =>
            {
                long seq = 0;
                while (!cts.IsCancellationRequested)
                {
                    long start = Stopwatch.GetTimestamp();
                    string id = TransitBenchmarkCore.BuildMessageId(10, workerId, Interlocked.Increment(ref seq), "exp1");
                    TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, ArticleBytes);
                    int len = payload.Length;
                    payload.Dispose();
                    long elapsed = Math.Max(0, Stopwatch.GetTimestamp() - start);

                    Interlocked.Increment(ref totalArticles);
                    Interlocked.Add(ref totalBytes, len);
                    Interlocked.Add(ref totalLatencyTicks, elapsed);
                    latencyTicks.Add(elapsed);
                }
            }, CancellationToken.None);
        }

        try
        {
            await Task.WhenAll(producerTasks).ConfigureAwait(false);
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
        }

        wall.Stop();
        cpu.Stop();

        GeneratorResult result = new(
            Label: label,
            Workers: workers,
            Articles: totalArticles,
            Bytes: totalBytes,
            Elapsed: wall.Elapsed,
            CpuPercent: cpu.AverageCpuPercent,
            BusyCores: cpu.EquivalentBusyCores,
            PeakWorkingSetBytes: cpu.PeakWorkingSetBytes,
            AllocatedBytes: GC.GetTotalAllocatedBytes(false) - allocStart,
            Gen0: GC.CollectionCount(0) - gen0Start,
            Gen1: GC.CollectionCount(1) - gen1Start,
            Gen2: GC.CollectionCount(2) - gen2Start,
            AvgLatencyUs: totalArticles == 0 ? 0 : totalLatencyTicks * 1_000_000d / (Stopwatch.Frequency * totalArticles),
            P50Us: PercentileUs(latencyTicks, 0.50),
            P95Us: PercentileUs(latencyTicks, 0.95),
            P99Us: PercentileUs(latencyTicks, 0.99));

        PrintGeneratorResult(result);
        PrintScalingEfficiency(result.ArticlesPerSecond);
        return result;
    }

    /// <summary>
    /// Runs QueueNoOpAsync.

    /// </summary>
    private static async Task<QueueResult> RunQueueNoOpAsync(string label, int warmupSeconds, int measurementSeconds, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");

        await WarmupGeneratorAsync(warmupSeconds, ProducerWorkers, cancellationToken).ConfigureAwait(false);

        using TransitBenchmarkCore.BoundedArticleQueue queue = new(QueueArticles, QueueBytes);
        QueueSampler sampler = new(queue);
        Process process = Process.GetCurrentProcess();
        CpuSampler cpu = new(process);

        long allocStart = GC.GetTotalAllocatedBytes(false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        long generated = 0;
        long generatedBytes = 0;
        long dispatched = 0;
        long completed = 0;
        long producerLoopTicks = 0;
        long producerBlockedTicks = 0;
        int dispatchInFlight = 0;
        int peakDispatchInFlight = 0;

        Task[] dispatchers = new Task[DispatchWorkers];
        for (int i = 0; i < DispatchWorkers; i++)
        {
            dispatchers[i] = Task.Run(async () =>
            {
                while (await queue.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
                {
                    while (queue.TryRead(out TransitBenchmarkCore.QueuedArticle item))
                    {
                        Interlocked.Increment(ref dispatched);
                        int now = Interlocked.Increment(ref dispatchInFlight);
                        peakDispatchInFlight = Math.Max(peakDispatchInFlight, now);

                        queue.ReleaseReservation(item.Payload.Length);
                        item.Payload.Dispose();

                        Interlocked.Increment(ref completed);
                        Interlocked.Decrement(ref dispatchInFlight);
                    }
                }
            }, CancellationToken.None);
        }

        Task[] producers = new Task[ProducerWorkers];
        for (int i = 0; i < ProducerWorkers; i++)
        {
            int workerId = i;
            producers[i] = Task.Run(async () =>
            {
                long seq = 0;
                while (!cts.IsCancellationRequested)
                {
                    long loopStart = Stopwatch.GetTimestamp();

                    if (queue.CurrentQueuedCount >= QueueTarget)
                    {
                        await Task.Yield();
                        continue;
                    }

                    string id = TransitBenchmarkCore.BuildMessageId(20, workerId, Interlocked.Increment(ref seq), "exp2");
                    TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, ArticleBytes);

                    long waitStart = Stopwatch.GetTimestamp();
                    bool admitted;
                    try
                    {
                        admitted = await queue.TryWriteAsync(new TransitBenchmarkCore.QueuedArticle(id, payload), cts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cts.IsCancellationRequested)
                    {
                        payload.Dispose();
                        break;
                    }

                    long loopEnd = Stopwatch.GetTimestamp();

                    if (!admitted)
                    {
                        payload.Dispose();
                        break;
                    }

                    Interlocked.Increment(ref generated);
                    Interlocked.Add(ref generatedBytes, payload.Length);
                    Interlocked.Add(ref producerBlockedTicks, Math.Max(0, loopEnd - waitStart));
                    Interlocked.Add(ref producerLoopTicks, Math.Max(0, loopEnd - loopStart));
                }
            }, CancellationToken.None);
        }

        sampler.Start();
        cpu.Start();
        Stopwatch wall = Stopwatch.StartNew();

        try
        {
            await Task.WhenAll(producers).ConfigureAwait(false);
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
        }

        queue.StopAdmission();
        await Task.WhenAll(dispatchers).ConfigureAwait(false);

        wall.Stop();
        sampler.Stop();
        cpu.Stop();

        double blockedPercent = producerLoopTicks <= 0 ? 0 : producerBlockedTicks * 100d / producerLoopTicks;

        QueueResult result = new(
            Label: label,
            Generated: generated,
            Dispatched: dispatched,
            Completed: completed,
            Bytes: generatedBytes,
            Elapsed: wall.Elapsed,
            CpuPercent: cpu.AverageCpuPercent,
            BusyCores: cpu.EquivalentBusyCores,
            PeakWorkingSetBytes: cpu.PeakWorkingSetBytes,
            AllocatedBytes: GC.GetTotalAllocatedBytes(false) - allocStart,
            Gen0: GC.CollectionCount(0) - gen0Start,
            Gen1: GC.CollectionCount(1) - gen1Start,
            Gen2: GC.CollectionCount(2) - gen2Start,
            QueueMin: sampler.MinDepth,
            QueueAvg: sampler.AverageDepth,
            QueueMax: sampler.MaxDepth,
            QueueAvgBytes: sampler.AverageBytes,
            QueueMaxBytes: sampler.MaxBytes,
            ProducerBlockedPercent: blockedPercent,
            ProducerQueueWaitMs: producerBlockedTicks * 1000d / Stopwatch.Frequency,
            PeakDispatcherInFlight: peakDispatchInFlight,
            PeakActualPending: 0);

        PrintQueueResult(result);
        return result;
    }

    /// <summary>
    /// Runs RealPipelineAsync.

    /// </summary>
    private static async Task<RealPipelineResult> RunRealPipelineAsync(string label, int warmupSeconds, int measurementSeconds, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");

        RuntimeConfig runtime = LoadRuntimeConfig();

        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Warning);
            builder.AddSimpleConsole(options =>
            {
                options.SingleLine = true;
                options.TimestampFormat = "HH:mm:ss ";
            });
        });

        await using TransitPublisher publisher = new(
            BuildRuntimeOptions(runtime),
            TimeProvider.System,
            loggerFactory.CreateLogger<TransitPublisher>(),
            Connections,
            PipelineDepth);

        await publisher.InitializeAsync(cancellationToken).ConfigureAwait(false);

        using TransitBenchmarkCore.BoundedArticleQueue queue = new(QueueArticles, QueueBytes);
        QueueSampler sampler = new(queue);
        Process process = Process.GetCurrentProcess();
        CpuSampler cpu = new(process);

        long allocStart = GC.GetTotalAllocatedBytes(false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        using CancellationTokenSource warmupCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        warmupCts.CancelAfter(TimeSpan.FromSeconds(warmupSeconds));

        long warmSeq = 0;
        while (!warmupCts.IsCancellationRequested)
        {
            string id = TransitBenchmarkCore.BuildMessageId(30, 0, Interlocked.Increment(ref warmSeq), "exp3-warm");
            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, ArticleBytes);
            try
            {
                _ = await publisher.PublishAsync(id, payload.AsMemory(), warmupCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (warmupCts.IsCancellationRequested)
            {
                payload.Dispose();
                break;
            }

            payload.Dispose();
        }

        ConcurrentDictionary<string, ArticleStageStamp> stamps = new(StringComparer.Ordinal);
        ConcurrentBag<long> publishTicks = [];

        long generated = 0;
        long accepted = 0;
        long completed = 0;
        long bytes = 0;
        long generationTicksTotal = 0;
        long queueWaitTicksTotal = 0;
        long dispatchWaitTicksTotal = 0;
        long publishTicksTotal = 0;
        long lifecycleTicksTotal = 0;
        long counted = 0;

        int dispatcherInFlight = 0;
        int peakDispatcherInFlight = 0;
        long producerLoopTicks = 0;
        long producerBlockedTicks = 0;

        int peakActualPending = 0;
        int readyConnections = 0;
        int activeConnections = 0;
        Dictionary<int, int> maxInFlightDistribution = [];

        Task monitor = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diag = publisher.CaptureConnectionDiagnosticsSnapshot();
                int currentPending = diag.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
                peakActualPending = Math.Max(peakActualPending, currentPending);
                readyConnections = diag.Connections.Count(static x => x.Snapshot.ReadyTransitionCount > 0);
                activeConnections = diag.Slots.Count(static x => x.TotalSubmissionsRouted > 0);

                maxInFlightDistribution = diag.Connections
                    .GroupBy(static c => c.Snapshot.MaxConcurrentSubmissions)
                    .OrderBy(static g => g.Key)
                    .ToDictionary(static g => g.Key, static g => g.Count());

                await Task.Delay(250, cts.Token).ConfigureAwait(false);
            }
        }, CancellationToken.None);

        Task[] dispatchers = new Task[DispatchWorkers];
        for (int i = 0; i < DispatchWorkers; i++)
        {
            dispatchers[i] = Task.Run(async () =>
            {
                while (await queue.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
                {
                    while (queue.TryRead(out TransitBenchmarkCore.QueuedArticle item))
                    {
                        long dequeueTick = Stopwatch.GetTimestamp();
                        if (stamps.TryGetValue(item.MessageId, out ArticleStageStamp stampAtDequeue))
                        {
                            stamps[item.MessageId] = stampAtDequeue with { DequeueTick = dequeueTick };
                        }

                        int now = Interlocked.Increment(ref dispatcherInFlight);
                        peakDispatcherInFlight = Math.Max(peakDispatcherInFlight, now);

                        long publishStart = Stopwatch.GetTimestamp();
                        TransitPublishResult result = await publisher.PublishAsync(item.MessageId, item.Payload.AsMemory(), CancellationToken.None).ConfigureAwait(false);
                        long publishEnd = Stopwatch.GetTimestamp();

                        if (result.Status == TransitPublishStatus.Accepted)
                        {
                            Interlocked.Increment(ref accepted);
                        }

                        if (stamps.TryRemove(item.MessageId, out ArticleStageStamp stamp))
                        {
                            long queueWait = stamp.DequeueTick > 0 ? Math.Max(0, stamp.DequeueTick - stamp.EnqueueEndTick) : 0;
                            long dispatchWait = stamp.DequeueTick > 0 ? Math.Max(0, publishStart - stamp.DequeueTick) : 0;
                            long publishTicksValue = Math.Max(0, publishEnd - publishStart);
                            long lifecycle = stamp.GenerationTicks + queueWait + dispatchWait + publishTicksValue;

                            Interlocked.Add(ref generationTicksTotal, stamp.GenerationTicks);
                            Interlocked.Add(ref queueWaitTicksTotal, queueWait);
                            Interlocked.Add(ref dispatchWaitTicksTotal, dispatchWait);
                            Interlocked.Add(ref publishTicksTotal, publishTicksValue);
                            Interlocked.Add(ref lifecycleTicksTotal, lifecycle);
                            Interlocked.Increment(ref counted);
                            publishTicks.Add(publishTicksValue);
                        }

                        Interlocked.Increment(ref completed);
                        Interlocked.Decrement(ref dispatcherInFlight);
                        queue.ReleaseReservation(item.Payload.Length);
                        item.Payload.Dispose();
                    }
                }
            }, CancellationToken.None);
        }

        Task[] producers = new Task[ProducerWorkers];
        for (int i = 0; i < ProducerWorkers; i++)
        {
            int workerId = i;
            producers[i] = Task.Run(async () =>
            {
                long seq = 0;
                while (!cts.IsCancellationRequested)
                {
                    long loopStart = Stopwatch.GetTimestamp();
                    if (queue.CurrentQueuedCount >= QueueTarget)
                    {
                        await Task.Yield();
                        continue;
                    }

                    long genStart = Stopwatch.GetTimestamp();
                    string id = TransitBenchmarkCore.BuildMessageId(31, workerId, Interlocked.Increment(ref seq), "exp3");
                    TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, ArticleBytes);
                    long genEnd = Stopwatch.GetTimestamp();
                    long genTicks = Math.Max(0, genEnd - genStart);

                    long waitStart = Stopwatch.GetTimestamp();
                    bool admitted;
                    try
                    {
                        admitted = await queue.TryWriteAsync(new TransitBenchmarkCore.QueuedArticle(id, payload), cts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cts.IsCancellationRequested)
                    {
                        payload.Dispose();
                        break;
                    }

                    long loopEnd = Stopwatch.GetTimestamp();

                    if (!admitted)
                    {
                        payload.Dispose();
                        break;
                    }

                    stamps[id] = new ArticleStageStamp(genTicks, loopEnd, 0);
                    Interlocked.Increment(ref generated);
                    Interlocked.Add(ref bytes, payload.Length);
                    Interlocked.Add(ref producerBlockedTicks, Math.Max(0, loopEnd - waitStart));
                    Interlocked.Add(ref producerLoopTicks, Math.Max(0, loopEnd - loopStart));
                }
            }, CancellationToken.None);
        }

        sampler.Start();
        cpu.Start();
        Stopwatch wall = Stopwatch.StartNew();

        try
        {
            await Task.WhenAll(producers).ConfigureAwait(false);
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
        }

        queue.StopAdmission();
        await Task.WhenAll(dispatchers).ConfigureAwait(false);

        try
        {
            await monitor.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        wall.Stop();
        sampler.Stop();
        cpu.Stop();

        double blockedPercent = producerLoopTicks <= 0 ? 0 : producerBlockedTicks * 100d / producerLoopTicks;

        RealPipelineResult result = new(
            Label: label,
            Generated: generated,
            Completed: completed,
            Accepted: accepted,
            Bytes: bytes,
            Elapsed: wall.Elapsed,
            CpuPercent: cpu.AverageCpuPercent,
            BusyCores: cpu.EquivalentBusyCores,
            PeakWorkingSetBytes: cpu.PeakWorkingSetBytes,
            AllocatedBytes: GC.GetTotalAllocatedBytes(false) - allocStart,
            Gen0: GC.CollectionCount(0) - gen0Start,
            Gen1: GC.CollectionCount(1) - gen1Start,
            Gen2: GC.CollectionCount(2) - gen2Start,
            QueueMin: sampler.MinDepth,
            QueueAvg: sampler.AverageDepth,
            QueueMax: sampler.MaxDepth,
            QueueAvgBytes: sampler.AverageBytes,
            QueueMaxBytes: sampler.MaxBytes,
            ProducerBlockedPercent: blockedPercent,
            ProducerQueueWaitMs: producerBlockedTicks * 1000d / Stopwatch.Frequency,
            PeakDispatcherInFlight: peakDispatcherInFlight,
            PeakActualPending: peakActualPending,
            AvgGenerationUs: counted == 0 ? 0 : generationTicksTotal * 1_000_000d / (Stopwatch.Frequency * counted),
            AvgQueueWaitUs: counted == 0 ? 0 : queueWaitTicksTotal * 1_000_000d / (Stopwatch.Frequency * counted),
            AvgDispatchWaitUs: counted == 0 ? 0 : dispatchWaitTicksTotal * 1_000_000d / (Stopwatch.Frequency * counted),
            AvgPublishUs: counted == 0 ? 0 : publishTicksTotal * 1_000_000d / (Stopwatch.Frequency * counted),
            AvgLifecycleUs: counted == 0 ? 0 : lifecycleTicksTotal * 1_000_000d / (Stopwatch.Frequency * counted),
            P50PublishUs: PercentileUs(publishTicks, 0.50),
            P95PublishUs: PercentileUs(publishTicks, 0.95),
            P99PublishUs: PercentileUs(publishTicks, 0.99),
            ReadyConnections: readyConnections,
            ActiveConnections: activeConnections,
            MaxInFlightDistribution: maxInFlightDistribution);

        PrintRealPipelineResult(result);
        return result;
    }

    /// <summary>
    /// Implements the warmup GeneratorAsync contract.
    /// </summary>
    private static async Task WarmupGeneratorAsync(int warmupSeconds, int workers, CancellationToken cancellationToken)
    {
        using CancellationTokenSource warmup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        warmup.CancelAfter(TimeSpan.FromSeconds(warmupSeconds));

        Task[] tasks = new Task[workers];
        for (int i = 0; i < workers; i++)
        {
            int workerId = i;
            tasks[i] = Task.Run(() =>
            {
                long seq = 0;
                while (!warmup.IsCancellationRequested)
                {
                    string id = TransitBenchmarkCore.BuildMessageId(1, workerId, Interlocked.Increment(ref seq), "warm");
                    TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, ArticleBytes);
                    payload.Dispose();
                }
            }, CancellationToken.None);
        }

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (Exception) when (warmup.IsCancellationRequested)
        {
        }
    }

    /// <summary>
    /// Builds RuntimeOptions.

    /// </summary>
    private static BackFillerRuntimeOptions BuildRuntimeOptions(RuntimeConfig runtime)
    {
        return new BackFillerRuntimeOptions(
            CanonicalBackFillerFqdn: "benchmark.backfiller.usenet.ninja",
            BackFillerId: 1,
            CanonicalDnsSuffix: "usenet.ninja",
            ValidatedLogDirectory: Path.GetTempPath(),
            ValidatedCertificateDirectory: Path.GetTempPath(),
            RabbitMqHosts: [],
            RabbitMqPort: 5672,
            RabbitMqEnableSsl: false,
            TransitServerHost: runtime.Host,
            TransitServerPort: runtime.Port,
            TransitServerUseSsl: runtime.UseSsl,
            BindPort: 119,
            ConfiguredBindAddressTokens: ["127.0.0.1"],
            ShutdownGracePeriodSeconds: 120,
            ShutdownDrainQueuedWork: true,
            ShutdownFinishActiveArticles: true,
            RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
            WriteBatchCoalesceMicroseconds: 250);
    }

    /// <summary>
    /// Implements the load RuntimeConfig contract.
    /// </summary>
    private static RuntimeConfig LoadRuntimeConfig()
    {
        string appSettingsPath = FindBackFillerAppSettingsPath();
        IConfigurationRoot configuration = new ConfigurationBuilder()
            .AddJsonFile(appSettingsPath, optional: false, reloadOnChange: false)
            .Build();

        string host = configuration["BackFiller:TransitServer:Host"]
            ?? throw new InvalidOperationException("BackFiller:TransitServer:Host is missing in existing application configuration.");

        string normalizedHost = host.Trim();
        if (!normalizedHost.Equals(RequiredTransitHostname, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Configured TransitServer host must be '{RequiredTransitHostname}', but was '{normalizedHost}'.");
        }

        string? portRaw = configuration["BackFiller:TransitServer:Port"];
        if (!int.TryParse(portRaw, out int port) || port is <= 0 or > 65535)
        {
            throw new InvalidOperationException("BackFiller:TransitServer:Port is missing or invalid in existing application configuration.");
        }

        bool useSsl = bool.TryParse(configuration["BackFiller:TransitServer:UseSsl"], out bool parsedUseSsl) && parsedUseSsl;
        return new RuntimeConfig(normalizedHost, port, useSsl);
    }

    /// <summary>
    /// Implements the find BackFillerAppSettingsPath contract.
    /// </summary>
    private static string FindBackFillerAppSettingsPath()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            string candidate = Path.Combine(current.FullName, "VectorNNTP.BackFiller", "appsettings.json");
            if (File.Exists(candidate))
            {
                return candidate;
            }

            current = current.Parent;
        }

        throw new FileNotFoundException("Unable to locate existing BackFiller appsettings.json from benchmark runner base directory.");
    }

    /// <summary>
    /// Implements the percentile Us contract.
    /// </summary>
    private static double PercentileUs(IEnumerable<long> ticks, double percentile)
    {
        long[] ordered = ticks.OrderBy(static x => x).ToArray();
        if (ordered.Length == 0)
        {
            return 0;
        }

        percentile = Math.Clamp(percentile, 0d, 1d);
        int index = (int)Math.Clamp(Math.Ceiling(percentile * ordered.Length) - 1, 0, ordered.Length - 1);
        return ordered[index] * 1_000_000d / Stopwatch.Frequency;
    }

    /// <summary>
    /// Implements the print ScalingEfficiency contract.
    /// </summary>
    private static void PrintScalingEfficiency(double current32Rate)
    {
        const double baseline1 = 772.2691;
        const double baseline4 = 2932.5899;
        const double baseline8 = 4264.0247;
        const double baseline16 = 6666.1003;

        Console.WriteLine("Scaling efficiency of 32-worker run relative to baselines:");
        Console.WriteLine($"vs 1 worker: x{(current32Rate / baseline1):F4}");
        Console.WriteLine($"vs 4 workers: x{(current32Rate / baseline4):F4}");
        Console.WriteLine($"vs 8 workers: x{(current32Rate / baseline8):F4}");
        Console.WriteLine($"vs 16 workers: x{(current32Rate / baseline16):F4}");
    }

    /// <summary>
    /// Implements the print GeneratorResult contract.
    /// </summary>
    private static void PrintGeneratorResult(GeneratorResult result)
    {
        Console.WriteLine($"Articles/sec={result.ArticlesPerSecond:F4}, Gbps={result.Gbps:F4}, CPU%={result.CpuPercent:F2}, EqBusyCores={result.BusyCores:F3}");
        Console.WriteLine($"Latency us: avg={result.AvgLatencyUs:F3}, p50={result.P50Us:F3}, p95={result.P95Us:F3}, p99={result.P99Us:F3}");
        Console.WriteLine($"Alloc/article={result.AllocatedBytesPerArticle:F2} bytes, GC=({result.Gen0},{result.Gen1},{result.Gen2}), WSpeakMB={result.PeakWorkingSetBytes / 1024d / 1024d:F2}");
    }

    /// <summary>
    /// Implements the print QueueResult contract.
    /// </summary>
    private static void PrintQueueResult(QueueResult result)
    {
        Console.WriteLine($"Generated/sec={result.GeneratedPerSecond:F4}, Dispatched/sec={result.DispatchedPerSecond:F4}, Completed/sec={result.CompletedPerSecond:F4}");
        Console.WriteLine($"Generated/Completed Gbps={result.Gbps:F4}");
        Console.WriteLine($"Queue depth min/avg/max={result.QueueMin}/{result.QueueAvg:F2}/{result.QueueMax}, queue bytes avg/max={result.QueueAvgBytes:F0}/{result.QueueMaxBytes}");
        Console.WriteLine($"Producer blocked/backpressured={result.ProducerBlockedPercent:F2}%, queue-capacity wait={result.ProducerQueueWaitMs:F2} ms");
        Console.WriteLine($"Peak dispatcher in-flight={result.PeakDispatcherInFlight}, peak actual pending={result.PeakActualPending}");
        Console.WriteLine($"CPU%={result.CpuPercent:F2}, EqBusyCores={result.BusyCores:F3}, alloc/article={result.AllocatedBytesPerArticle:F2} bytes, GC=({result.Gen0},{result.Gen1},{result.Gen2}), WSpeakMB={result.PeakWorkingSetBytes / 1024d / 1024d:F2}");
    }

    /// <summary>
    /// Implements the print RealPipelineResult contract.
    /// </summary>
    private static void PrintRealPipelineResult(RealPipelineResult result)
    {
        Console.WriteLine($"Generated/sec={result.GeneratedPerSecond:F4}, Completed/sec={result.CompletedPerSecond:F4}, Accepted/sec={result.AcceptedPerSecond:F4}, Accepted Gbps={result.Gbps:F4}");
        Console.WriteLine($"Queue depth min/avg/max={result.QueueMin}/{result.QueueAvg:F2}/{result.QueueMax}, queue bytes avg/max={result.QueueAvgBytes:F0}/{result.QueueMaxBytes}");
        Console.WriteLine($"Producer blocked/backpressured={result.ProducerBlockedPercent:F2}%, queue-capacity wait={result.ProducerQueueWaitMs:F2} ms");
        Console.WriteLine($"Peak dispatcher in-flight={result.PeakDispatcherInFlight}, peak actual pending={result.PeakActualPending}");
        Console.WriteLine($"Connections READY={result.ReadyConnections}, active slots={result.ActiveConnections}");
        Console.WriteLine($"MaxInFlight distribution: {FormatDistribution(result.MaxInFlightDistribution)}");
        Console.WriteLine($"Latency us avg: generation={result.AvgGenerationUs:F3}, queueWait={result.AvgQueueWaitUs:F3}, dispatchWait={result.AvgDispatchWaitUs:F3}, publish={result.AvgPublishUs:F3}, lifecycle={result.AvgLifecycleUs:F3}");
        Console.WriteLine($"Publish latency us: p50={result.P50PublishUs:F3}, p95={result.P95PublishUs:F3}, p99={result.P99PublishUs:F3}");
        Console.WriteLine($"CPU%={result.CpuPercent:F2}, EqBusyCores={result.BusyCores:F3}, alloc/article={result.AllocatedBytesPerArticle:F2} bytes, GC=({result.Gen0},{result.Gen1},{result.Gen2}), WSpeakMB={result.PeakWorkingSetBytes / 1024d / 1024d:F2}");
    }

    /// <summary>
    /// Formats Distribution.

    /// </summary>
    private static string FormatDistribution(Dictionary<int, int> distribution)
    {
        if (distribution.Count == 0)
        {
            return "(none)";
        }

        return string.Join(", ", distribution.OrderBy(static kvp => kvp.Key).Select(static kvp => $"{kvp.Key}:{kvp.Value}"));
    }

    /// <summary>
    /// Implements the print FinalSummary contract.
    /// </summary>
    private static void PrintFinalSummary(GeneratorResult exp1, QueueResult exp2, RealPipelineResult exp3)
    {
        Console.WriteLine();
        Console.WriteLine("=== Final 32-worker experiment summary ===");
        Console.WriteLine($"Exp1 generator: {exp1.ArticlesPerSecond:F4} art/s, {exp1.Gbps:F4} Gbps");
        Console.WriteLine($"Exp2 generator+queue completed: {exp2.CompletedPerSecond:F4} art/s, {exp2.Gbps:F4} Gbps");
        Console.WriteLine($"Exp3 real pipeline accepted: {exp3.AcceptedPerSecond:F4} art/s, {exp3.Gbps:F4} Gbps");
        Console.WriteLine($"Maximum observed Gbps: {Math.Max(exp1.Gbps, Math.Max(exp2.Gbps, exp3.Gbps)):F4}");
        Console.WriteLine($"Maximum observed articles/sec: {Math.Max(exp1.ArticlesPerSecond, Math.Max(exp2.CompletedPerSecond, exp3.AcceptedPerSecond)):F4}");
        Console.WriteLine($"Queue high-water mark (real pipeline): depth={exp3.QueueMax}, bytes={exp3.QueueMaxBytes}");
        Console.WriteLine($"Producer backpressure occurred (real pipeline): {(exp3.ProducerBlockedPercent > 0 ? "Yes" : "No")}");
        Console.WriteLine("Use real-pipeline accepted Gbps vs prior 3.7009 Gbps to determine material improvement.");
    }

    /// <summary>
    /// Represents the cpu Sampler class used by the benchmark or regression gate.
    /// </summary>
    private sealed class CpuSampler
    {
        /// <summary>
        /// Gets or sets the _process.
        /// </summary>
        private readonly Process _process;
        /// <summary>
        /// Runs the _elapsed benchmark scenario.
        /// </summary>
        private readonly Stopwatch _elapsed = Stopwatch.StartNew();
        /// <summary>
        /// Gets or sets the _cts.
        /// </summary>
        private CancellationTokenSource? _cts;
        /// <summary>
        /// Gets or sets the _task.
        /// </summary>
        private Task? _task;
        /// <summary>
        /// Gets or sets the _cpuStart.
        /// </summary>
        private TimeSpan _cpuStart;
        /// <summary>
        /// Gets or sets the _peakWorkingSet.
        /// </summary>
        private long _peakWorkingSet;

        /// <summary>
        /// Implements the cpu Sampler contract.
        /// </summary>
        internal CpuSampler(Process process)
        {
            _process = process;
            _peakWorkingSet = process.WorkingSet64;
        }

        /// <summary>
        /// Gets or sets the average CpuPercent.
        /// </summary>
        internal double AverageCpuPercent { get; private set; }
        /// <summary>
        /// Gets or sets the equivalent BusyCores.
        /// </summary>
        internal double EquivalentBusyCores => AverageCpuPercent / 100d * Environment.ProcessorCount;
        /// <summary>
        /// Gets or sets the peak WorkingSetBytes.
        /// </summary>
        internal long PeakWorkingSetBytes => _peakWorkingSet;
        /// <summary>
        /// Gets or sets the total CpuTime.
        /// </summary>
        internal TimeSpan TotalCpuTime { get; private set; }

        /// <summary>
        /// Runs the start benchmark scenario.
        /// </summary>
        internal void Start()
        {
            _cpuStart = _process.TotalProcessorTime;
            _cts = new CancellationTokenSource();
            _task = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    long ws = _process.WorkingSet64;
                    if (ws > _peakWorkingSet)
                    {
                        _peakWorkingSet = ws;
                    }

                    await Task.Delay(200, _cts.Token).ConfigureAwait(false);
                }
            }, CancellationToken.None);
        }

        /// <summary>
        /// Runs the stop benchmark scenario.
        /// </summary>
        internal void Stop()
        {
            if (_cts is null)
            {
                return;
            }

            _cts.Cancel();
            try
            {
                _task?.GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
            }

            TotalCpuTime = _process.TotalProcessorTime - _cpuStart;
            double seconds = Math.Max(0.000001d, _elapsed.Elapsed.TotalSeconds);
            AverageCpuPercent = TotalCpuTime.TotalSeconds / (Environment.ProcessorCount * seconds) * 100d;
            _cts.Dispose();
            _cts = null;
        }
    }

    /// <summary>
    /// Represents the queue Sampler class used by the benchmark or regression gate.
    /// </summary>
    private sealed class QueueSampler
    {
        /// <summary>
        /// Gets or sets the _queue.
        /// </summary>
        private readonly TransitBenchmarkCore.BoundedArticleQueue _queue;
        /// <summary>
        /// Gets or sets the _cts.
        /// </summary>
        private CancellationTokenSource? _cts;
        /// <summary>
        /// Gets or sets the _task.
        /// </summary>
        private Task? _task;
        /// <summary>
        /// Gets or sets the _depthSum.
        /// </summary>
        private long _depthSum;
        /// <summary>
        /// Gets or sets the _byteSum.
        /// </summary>
        private long _byteSum;
        /// <summary>
        /// Gets or sets the _count.
        /// </summary>
        private long _count;
        /// <summary>
        /// Gets or sets the _minDepth.
        /// </summary>
        private int _minDepth = int.MaxValue;
        /// <summary>
        /// Gets or sets the _maxDepth.
        /// </summary>
        private int _maxDepth;
        /// <summary>
        /// Gets or sets the _maxBytes.
        /// </summary>
        private long _maxBytes;

        /// <summary>
        /// Implements the queue Sampler contract.
        /// </summary>
        internal QueueSampler(TransitBenchmarkCore.BoundedArticleQueue queue)
        {
            _queue = queue;
        }

        /// <summary>
        /// Gets or sets the min Depth.
        /// </summary>
        internal int MinDepth => _minDepth == int.MaxValue ? 0 : _minDepth;
        /// <summary>
        /// Gets or sets the max Depth.
        /// </summary>
        internal int MaxDepth => _maxDepth;
        /// <summary>
        /// Implements the average Depth contract.
        /// </summary>
        internal double AverageDepth => _count == 0 ? 0 : (double)_depthSum / _count;
        /// <summary>
        /// Implements the average Bytes contract.
        /// </summary>
        internal double AverageBytes => _count == 0 ? 0 : (double)_byteSum / _count;
        /// <summary>
        /// Gets or sets the max Bytes.
        /// </summary>
        internal long MaxBytes => _maxBytes;

        /// <summary>
        /// Runs the start benchmark scenario.
        /// </summary>
        internal void Start()
        {
            _cts = new CancellationTokenSource();
            _task = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    int depth = _queue.CurrentQueuedCount;
                    long bytes = _queue.CurrentQueuedBytes;
                    Interlocked.Add(ref _depthSum, depth);
                    Interlocked.Add(ref _byteSum, bytes);
                    Interlocked.Increment(ref _count);
                    _minDepth = Math.Min(_minDepth, depth);
                    _maxDepth = Math.Max(_maxDepth, depth);
                    _maxBytes = Math.Max(_maxBytes, bytes);
                    await Task.Delay(100, _cts.Token).ConfigureAwait(false);
                }
            }, CancellationToken.None);
        }

        /// <summary>
        /// Runs the stop benchmark scenario.
        /// </summary>
        internal void Stop()
        {
            if (_cts is null)
            {
                return;
            }

            _cts.Cancel();
            try
            {
                _task?.GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
            }

            _cts.Dispose();
            _cts = null;
        }
    }

    /// <summary>
    /// Represents the runtime Config record struct used by the benchmark or regression gate.
    /// </summary>
    private readonly record struct RuntimeConfig(string Host, int Port, bool UseSsl);
    /// <summary>
    /// Represents the article StageStamp record struct used by the benchmark or regression gate.
    /// </summary>
    private readonly record struct ArticleStageStamp(long GenerationTicks, long EnqueueEndTick, long DequeueTick);

    /// <summary>
    /// Represents the generator Result record struct used by the benchmark or regression gate.
    /// </summary>
    private readonly record struct GeneratorResult(
        string Label,
        int Workers,
        long Articles,
        long Bytes,
        TimeSpan Elapsed,
        double CpuPercent,
        double BusyCores,
        long PeakWorkingSetBytes,
        long AllocatedBytes,
        int Gen0,
        int Gen1,
        int Gen2,
        double AvgLatencyUs,
        double P50Us,
        double P95Us,
        double P99Us)
    {
        /// <summary>
        /// Gets or sets the articles PerSecond.
        /// </summary>
        internal double ArticlesPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Articles / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the gbps.
        /// </summary>
        internal double Gbps => Elapsed.TotalSeconds <= 0 ? 0 : Bytes * 8d / 1_000_000_000d / Elapsed.TotalSeconds;
        /// <summary>
        /// Implements the allocated BytesPerArticle contract.
        /// </summary>
        internal double AllocatedBytesPerArticle => Articles == 0 ? 0 : (double)AllocatedBytes / Articles;
    }

    /// <summary>
    /// Represents the queue Result record struct used by the benchmark or regression gate.
    /// </summary>
    private readonly record struct QueueResult(
        string Label,
        long Generated,
        long Dispatched,
        long Completed,
        long Bytes,
        TimeSpan Elapsed,
        double CpuPercent,
        double BusyCores,
        long PeakWorkingSetBytes,
        long AllocatedBytes,
        int Gen0,
        int Gen1,
        int Gen2,
        int QueueMin,
        double QueueAvg,
        int QueueMax,
        double QueueAvgBytes,
        long QueueMaxBytes,
        double ProducerBlockedPercent,
        double ProducerQueueWaitMs,
        int PeakDispatcherInFlight,
        int PeakActualPending)
    {
        /// <summary>
        /// Gets or sets the generated PerSecond.
        /// </summary>
        internal double GeneratedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Generated / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the dispatched PerSecond.
        /// </summary>
        internal double DispatchedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Dispatched / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the completed PerSecond.
        /// </summary>
        internal double CompletedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Completed / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the gbps.
        /// </summary>
        internal double Gbps => Elapsed.TotalSeconds <= 0 ? 0 : Bytes * 8d / 1_000_000_000d / Elapsed.TotalSeconds;
        /// <summary>
        /// Implements the allocated BytesPerArticle contract.
        /// </summary>
        internal double AllocatedBytesPerArticle => Generated == 0 ? 0 : (double)AllocatedBytes / Generated;
    }

    /// <summary>
    /// Represents the real PipelineResult record struct used by the benchmark or regression gate.
    /// </summary>
    private readonly record struct RealPipelineResult(
        string Label,
        long Generated,
        long Completed,
        long Accepted,
        long Bytes,
        TimeSpan Elapsed,
        double CpuPercent,
        double BusyCores,
        long PeakWorkingSetBytes,
        long AllocatedBytes,
        int Gen0,
        int Gen1,
        int Gen2,
        int QueueMin,
        double QueueAvg,
        int QueueMax,
        double QueueAvgBytes,
        long QueueMaxBytes,
        double ProducerBlockedPercent,
        double ProducerQueueWaitMs,
        int PeakDispatcherInFlight,
        int PeakActualPending,
        double AvgGenerationUs,
        double AvgQueueWaitUs,
        double AvgDispatchWaitUs,
        double AvgPublishUs,
        double AvgLifecycleUs,
        double P50PublishUs,
        double P95PublishUs,
        double P99PublishUs,
        int ReadyConnections,
        int ActiveConnections,
        Dictionary<int, int> MaxInFlightDistribution)
    {
        /// <summary>
        /// Gets or sets the generated PerSecond.
        /// </summary>
        internal double GeneratedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Generated / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the completed PerSecond.
        /// </summary>
        internal double CompletedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Completed / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the accepted PerSecond.
        /// </summary>
        internal double AcceptedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Accepted / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the gbps.
        /// </summary>
        internal double Gbps => Elapsed.TotalSeconds <= 0 ? 0 : Bytes * 8d / 1_000_000_000d / Elapsed.TotalSeconds;
        /// <summary>
        /// Implements the allocated BytesPerArticle contract.
        /// </summary>
        internal double AllocatedBytesPerArticle => Generated == 0 ? 0 : (double)AllocatedBytes / Generated;
    }
}
