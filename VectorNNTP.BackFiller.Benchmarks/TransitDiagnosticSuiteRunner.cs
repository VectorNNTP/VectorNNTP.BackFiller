// <copyright file="TransitDiagnosticSuiteRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// TransitDiagnosticSuiteRunner: runs focused transit diagnostics and emits evidence for lifecycle and throughput behavior.

using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the transit DiagnosticSuiteRunner class for benchmark or isolated-regression execution.
/// </summary>
internal static class TransitDiagnosticSuiteRunner
{
    /// <summary>
    /// Gets or sets the required TransitHostname value.
    /// </summary>
    private const string RequiredTransitHostname = "incoming.usenet.ninja";
    /// <summary>
    /// Gets or sets the default ArticleTargetBytes value.
    /// </summary>
    private const int DefaultArticleTargetBytes = 1 * 1024 * 1024;

    /// <summary>
    /// Performs the run Async operation.
    /// </summary>
    internal static async Task RunAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        int warmupSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.WarmupSeconds ?? 10,
            min: 1,
            max: 600,
            optionName: "warmup-seconds");

        int measurementSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.DurationSeconds ?? 30,
            min: 1,
            max: 3600,
            optionName: "duration-seconds");

        int articleTargetBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.ArticleKilobytes is null ? DefaultArticleTargetBytes : checked(cliOptions.ArticleKilobytes.Value * 1024),
            min: 128 * 1024,
            max: 4 * 1024 * 1024,
            optionName: "article-kib");

        int queueArticles = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.QueueArticles ?? 2048,
            min: 1,
            max: 200_000,
            optionName: "queue-articles");

        long queueBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateLongRange(
            cliOptions.QueueMegabytes is null ? 2048L * 1024 * 1024 : checked((long)cliOptions.QueueMegabytes.Value * 1024L * 1024L),
            min: 64L * 1024L * 1024L,
            max: 2L * 1024L * 1024L * 1024L,
            optionName: "queue-mib");

        int dispatchWorkers = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.DispatchWorkers ?? 512,
            min: 1,
            max: 512,
            optionName: "dispatch-workers");

        int connections = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.ConnectionPoolSize ?? 64, 1, 64, "connections");
        int pipelineDepth = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(cliOptions.PipelineDepth ?? 16, 1, 64, "pipeline-depth");

        Console.WriteLine("=== Transit Diagnostic Suite (Benchmark-only) ===");
        Console.WriteLine($"Warmup={warmupSeconds}s, Measurement={measurementSeconds}s, ArticleBytes={articleTargetBytes}");
        Console.WriteLine($"QueueArticles={queueArticles}, QueueBytes={queueBytes}, DispatchWorkers={dispatchWorkers}, Connections={connections}, PipelineDepth={pipelineDepth}");

        GenerationOnlyResult test1 = await RunGeneratorOnlyAsync("TEST 1 - Generator only", warmupSeconds, measurementSeconds, articleTargetBytes, workerCount: 1, cancellationToken).ConfigureAwait(false);

        QueuePipelineResult test2 = await RunGeneratorQueueAsync("TEST 2 - Generator + Queue", warmupSeconds, measurementSeconds, articleTargetBytes, queueArticles, queueBytes, cancellationToken).ConfigureAwait(false);

        QueuePipelineResult test3 = await RunGeneratorQueueDispatchNoOpAsync("TEST 3 - Generator + Queue + Dispatcher(no-op)", warmupSeconds, measurementSeconds, articleTargetBytes, queueArticles, queueBytes, dispatchWorkers, cancellationToken).ConfigureAwait(false);

        EndToEndResult test4 = await RunRealPublisherInstrumentedAsync(
            "TEST 4 - Generator + REAL TransitPublisher (instrumented)",
            warmupSeconds,
            measurementSeconds,
            articleTargetBytes,
            queueArticles,
            queueBytes,
            dispatchWorkers,
            connections,
            pipelineDepth,
            cancellationToken).ConfigureAwait(false);

        List<GenerationOnlyResult> test5 = await RunParallelGeneratorSweepAsync(
            "TEST 5 - Generator parallelism",
            warmupSeconds,
            measurementSeconds,
            articleTargetBytes,
            cancellationToken).ConfigureAwait(false);

        PrintSummary(test1, test2, test3, test4, test5, articleTargetBytes);
    }

    /// <summary>
    /// Performs the warmup GeneratorAsync operation.
    /// </summary>
    private static async Task WarmupGeneratorAsync(int warmupSeconds, int articleBytes, int workerCount, CancellationToken cancellationToken)
    {
        using CancellationTokenSource warmupCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        warmupCts.CancelAfter(TimeSpan.FromSeconds(warmupSeconds));

        Task[] workers = new Task[workerCount];
        for (int i = 0; i < workerCount; i++)
        {
            int workerId = i;
            workers[i] = Task.Run(() =>
            {
                long seq = 0;
                while (!warmupCts.IsCancellationRequested)
                {
                    string id = TransitBenchmarkCore.BuildMessageId(0, workerId, Interlocked.Increment(ref seq), "diag-warmup");
                    TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, articleBytes);
                    payload.Dispose();
                }
            }, CancellationToken.None);
        }

        try
        {
            await Task.WhenAll(workers).ConfigureAwait(false);
        }
        catch (Exception) when (warmupCts.IsCancellationRequested)
        {
        }
    }

    /// <summary>
    /// Performs the run GeneratorOnlyAsync operation.
    /// </summary>
    private static async Task<GenerationOnlyResult> RunGeneratorOnlyAsync(string label, int warmupSeconds, int measurementSeconds, int articleBytes, int workerCount, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");

        await WarmupGeneratorAsync(warmupSeconds, articleBytes, workerCount, cancellationToken).ConfigureAwait(false);

        Process process = Process.GetCurrentProcess();
        CpuSampler sampler = new(process);

        long allocatedStart = GC.GetTotalAllocatedBytes(false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        long totalArticles = 0;
        long totalBytes = 0;
        long totalLatencyTicks = 0;
        ConcurrentBag<long> latencies = [];

        Stopwatch wall = Stopwatch.StartNew();
        sampler.Start();

        Task[] workers = new Task[workerCount];
        for (int i = 0; i < workerCount; i++)
        {
            int workerId = i;
            workers[i] = Task.Run(() =>
            {
                long seq = 0;
                while (!cts.IsCancellationRequested)
                {
                    long start = Stopwatch.GetTimestamp();
                    string id = TransitBenchmarkCore.BuildMessageId(1, workerId, Interlocked.Increment(ref seq), "diag-gen");
                    TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, articleBytes);
                    int len = payload.Length;
                    payload.Dispose();
                    long elapsed = Math.Max(0, Stopwatch.GetTimestamp() - start);

                    Interlocked.Increment(ref totalArticles);
                    Interlocked.Add(ref totalBytes, len);
                    Interlocked.Add(ref totalLatencyTicks, elapsed);
                    latencies.Add(elapsed);
                }
            }, CancellationToken.None);
        }

        try
        {
            await Task.WhenAll(workers).ConfigureAwait(false);
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
        }

        sampler.Stop();
        wall.Stop();

        TimeSpan cpuTime = sampler.TotalCpuTime;
        long allocatedBytes = GC.GetTotalAllocatedBytes(false) - allocatedStart;

        GenerationOnlyResult result = new(
            Label: label,
            WorkerCount: workerCount,
            Articles: totalArticles,
            Bytes: totalBytes,
            Elapsed: wall.Elapsed,
            CpuUtilizationPercent: sampler.AverageCpuPercent,
            EquivalentBusyCores: sampler.EquivalentBusyCores,
            PeakWorkingSetBytes: sampler.PeakWorkingSetBytes,
            AllocatedBytes: allocatedBytes,
            Gen0: GC.CollectionCount(0) - gen0Start,
            Gen1: GC.CollectionCount(1) - gen1Start,
            Gen2: GC.CollectionCount(2) - gen2Start,
            AverageLatencyUs: totalArticles == 0 ? 0 : totalLatencyTicks * 1_000_000d / (Stopwatch.Frequency * totalArticles),
            P50LatencyUs: ComputePercentileMicroseconds(latencies, 0.50),
            P95LatencyUs: ComputePercentileMicroseconds(latencies, 0.95),
            P99LatencyUs: ComputePercentileMicroseconds(latencies, 0.99),
            CpuTime: cpuTime);

        PrintGenerationResult(result);
        return result;
    }

    /// <summary>
    /// Performs the run GeneratorQueueAsync operation.
    /// </summary>
    private static async Task<QueuePipelineResult> RunGeneratorQueueAsync(string label, int warmupSeconds, int measurementSeconds, int articleBytes, int queueArticles, long queueBytes, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");

        await WarmupGeneratorAsync(warmupSeconds, articleBytes, 1, cancellationToken).ConfigureAwait(false);

        using TransitBenchmarkCore.BoundedArticleQueue queue = new(queueArticles, queueBytes);
        Process process = Process.GetCurrentProcess();
        CpuSampler sampler = new(process);
        QueueDepthSampler queueSampler = new(queue);

        long allocatedStart = GC.GetTotalAllocatedBytes(false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        long generated = 0;
        long generatedBytes = 0;
        long dequeued = 0;
        long genTicksTotal = 0;
        long enqueueTicksTotal = 0;
        long dequeueTicksTotal = 0;
        ConcurrentBag<long> genLat = [];
        ConcurrentBag<long> enqLat = [];
        ConcurrentBag<long> deqLat = [];

        Task consumer = Task.Run(async () =>
        {
            while (await queue.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
            {
                while (queue.TryRead(out TransitBenchmarkCore.QueuedArticle item))
                {
                    long ds = Stopwatch.GetTimestamp();
                    queue.ReleaseReservation(item.Payload.Length);
                    item.Payload.Dispose();
                    long de = Stopwatch.GetTimestamp();
                    long dt = Math.Max(0, de - ds);

                    Interlocked.Increment(ref dequeued);
                    Interlocked.Add(ref dequeueTicksTotal, dt);
                    deqLat.Add(dt);
                }
            }
        }, CancellationToken.None);

        Stopwatch wall = Stopwatch.StartNew();
        sampler.Start();
        queueSampler.Start();

        long seq = 0;
        while (!cts.IsCancellationRequested)
        {
            long gs = Stopwatch.GetTimestamp();
            string id = TransitBenchmarkCore.BuildMessageId(2, 0, Interlocked.Increment(ref seq), "diag-q");
            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, articleBytes);
            long ge = Stopwatch.GetTimestamp();
            long gt = Math.Max(0, ge - gs);

            long es = Stopwatch.GetTimestamp();
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

            long ee = Stopwatch.GetTimestamp();
            long et = Math.Max(0, ee - es);

            if (!admitted)
            {
                payload.Dispose();
                break;
            }

            Interlocked.Increment(ref generated);
            Interlocked.Add(ref generatedBytes, payload.Length);
            Interlocked.Add(ref genTicksTotal, gt);
            Interlocked.Add(ref enqueueTicksTotal, et);
            genLat.Add(gt);
            enqLat.Add(et);
        }

        queue.StopAdmission();
        await consumer.ConfigureAwait(false);

        queueSampler.Stop();
        sampler.Stop();
        wall.Stop();

        QueuePipelineResult result = new(
            Label: label,
            Generated: generated,
            Dispatched: dequeued,
            Completed: dequeued,
            Bytes: generatedBytes,
            Elapsed: wall.Elapsed,
            CpuUtilizationPercent: sampler.AverageCpuPercent,
            EquivalentBusyCores: sampler.EquivalentBusyCores,
            PeakWorkingSetBytes: sampler.PeakWorkingSetBytes,
            AllocatedBytes: GC.GetTotalAllocatedBytes(false) - allocatedStart,
            Gen0: GC.CollectionCount(0) - gen0Start,
            Gen1: GC.CollectionCount(1) - gen1Start,
            Gen2: GC.CollectionCount(2) - gen2Start,
            QueueMin: queueSampler.MinDepth,
            QueueAvg: queueSampler.AverageDepth,
            QueueMax: queueSampler.MaxDepth,
            AvgGenerationUs: generated == 0 ? 0 : genTicksTotal * 1_000_000d / (Stopwatch.Frequency * generated),
            AvgEnqueueUs: generated == 0 ? 0 : enqueueTicksTotal * 1_000_000d / (Stopwatch.Frequency * generated),
            AvgDequeueUs: dequeued == 0 ? 0 : dequeueTicksTotal * 1_000_000d / (Stopwatch.Frequency * dequeued),
            P50GenerationUs: ComputePercentileMicroseconds(genLat, 0.50),
            P95GenerationUs: ComputePercentileMicroseconds(genLat, 0.95),
            P99GenerationUs: ComputePercentileMicroseconds(genLat, 0.99),
            PeakInFlight: 0,
            AvgQueueWaitUs: 0,
            AvgDispatchWaitUs: 0,
            AvgPublishUs: 0);

        PrintQueuePipelineResult(result);
        return result;
    }

    /// <summary>
    /// Performs the run GeneratorQueueDispatchNoOpAsync operation.
    /// </summary>
    private static async Task<QueuePipelineResult> RunGeneratorQueueDispatchNoOpAsync(string label, int warmupSeconds, int measurementSeconds, int articleBytes, int queueArticles, long queueBytes, int dispatchWorkers, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");

        await WarmupGeneratorAsync(warmupSeconds, articleBytes, 1, cancellationToken).ConfigureAwait(false);

        using TransitBenchmarkCore.BoundedArticleQueue queue = new(queueArticles, queueBytes);
        Process process = Process.GetCurrentProcess();
        CpuSampler sampler = new(process);
        QueueDepthSampler queueSampler = new(queue);

        long allocatedStart = GC.GetTotalAllocatedBytes(false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        long generated = 0;
        long dispatched = 0;
        long completed = 0;
        long bytes = 0;
        int inFlight = 0;
        int peakInFlight = 0;
        long blockedTicks = 0;

        Task[] workers = new Task[dispatchWorkers];
        for (int i = 0; i < workers.Length; i++)
        {
            workers[i] = Task.Run(async () =>
            {
                while (await queue.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
                {
                    while (queue.TryRead(out TransitBenchmarkCore.QueuedArticle item))
                    {
                        Interlocked.Increment(ref dispatched);
                        int now = Interlocked.Increment(ref inFlight);
                        peakInFlight = Math.Max(peakInFlight, now);

                        queue.ReleaseReservation(item.Payload.Length);
                        item.Payload.Dispose();
                        Interlocked.Increment(ref completed);
                        Interlocked.Decrement(ref inFlight);
                    }
                }
            }, CancellationToken.None);
        }

        Stopwatch wall = Stopwatch.StartNew();
        sampler.Start();
        queueSampler.Start();

        long seq = 0;
        while (!cts.IsCancellationRequested)
        {
            long gs = Stopwatch.GetTimestamp();
            string id = TransitBenchmarkCore.BuildMessageId(3, 0, Interlocked.Increment(ref seq), "diag-d");
            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, articleBytes);
            long es = Stopwatch.GetTimestamp();
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

            long ee = Stopwatch.GetTimestamp();

            if (!admitted)
            {
                payload.Dispose();
                break;
            }

            Interlocked.Increment(ref generated);
            Interlocked.Add(ref bytes, payload.Length);
            Interlocked.Add(ref blockedTicks, Math.Max(0, ee - es));
            _ = gs;
        }

        queue.StopAdmission();
        await Task.WhenAll(workers).ConfigureAwait(false);

        queueSampler.Stop();
        sampler.Stop();
        wall.Stop();

        QueuePipelineResult result = new(
            Label: label,
            Generated: generated,
            Dispatched: dispatched,
            Completed: completed,
            Bytes: bytes,
            Elapsed: wall.Elapsed,
            CpuUtilizationPercent: sampler.AverageCpuPercent,
            EquivalentBusyCores: sampler.EquivalentBusyCores,
            PeakWorkingSetBytes: sampler.PeakWorkingSetBytes,
            AllocatedBytes: GC.GetTotalAllocatedBytes(false) - allocatedStart,
            Gen0: GC.CollectionCount(0) - gen0Start,
            Gen1: GC.CollectionCount(1) - gen1Start,
            Gen2: GC.CollectionCount(2) - gen2Start,
            QueueMin: queueSampler.MinDepth,
            QueueAvg: queueSampler.AverageDepth,
            QueueMax: queueSampler.MaxDepth,
            AvgGenerationUs: 0,
            AvgEnqueueUs: generated == 0 ? 0 : blockedTicks * 1_000_000d / (Stopwatch.Frequency * generated),
            AvgDequeueUs: 0,
            P50GenerationUs: 0,
            P95GenerationUs: 0,
            P99GenerationUs: 0,
            PeakInFlight: peakInFlight,
            AvgQueueWaitUs: 0,
            AvgDispatchWaitUs: 0,
            AvgPublishUs: 0);

        PrintQueuePipelineResult(result);
        return result;
    }

    /// <summary>
    /// Performs the run RealPublisherInstrumentedAsync operation.
    /// </summary>
    private static async Task<EndToEndResult> RunRealPublisherInstrumentedAsync(
        string label,
        int warmupSeconds,
        int measurementSeconds,
        int articleBytes,
        int queueArticles,
        long queueBytes,
        int dispatchWorkers,
        int connections,
        int pipelineDepth,
        CancellationToken cancellationToken)
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
            /// <summary>
            /// Performs the back FillerRuntimeOptions operation.
            /// </summary>
            new BackFillerRuntimeOptions(
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
                WriteBatchCoalesceMicroseconds: 250),
            TimeProvider.System,
            loggerFactory.CreateLogger<TransitPublisher>(),
            connections,
            pipelineDepth);

        await publisher.InitializeAsync(cancellationToken).ConfigureAwait(false);

        using TransitBenchmarkCore.BoundedArticleQueue queue = new(queueArticles, queueBytes);
        Process process = Process.GetCurrentProcess();
        CpuSampler sampler = new(process);
        QueueDepthSampler queueSampler = new(queue);
        StageTracker tracker = new();

        long allocatedStart = GC.GetTotalAllocatedBytes(false);
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
            string id = TransitBenchmarkCore.BuildMessageId(4, 0, Interlocked.Increment(ref warmSeq), "diag-warm");
            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, articleBytes);
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

        long generated = 0;
        long completed = 0;
        long accepted = 0;
        long bytes = 0;

        Task[] workers = new Task[dispatchWorkers];
        for (int i = 0; i < workers.Length; i++)
        {
            workers[i] = Task.Run(async () =>
            {
                while (await queue.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
                {
                    while (queue.TryRead(out TransitBenchmarkCore.QueuedArticle item))
                    {
                        long dequeueTick = Stopwatch.GetTimestamp();
                        tracker.RecordDequeued(item.MessageId, dequeueTick);

                        long publishStart = Stopwatch.GetTimestamp();
                        tracker.RecordPublishStart(item.MessageId, publishStart);

                        TransitPublishResult result = await publisher.PublishAsync(item.MessageId, item.Payload.AsMemory(), CancellationToken.None).ConfigureAwait(false);

                        long publishEnd = Stopwatch.GetTimestamp();
                        tracker.RecordPublishEnd(item.MessageId, publishEnd, result.Status == TransitPublishStatus.Accepted);

                        if (result.Status == TransitPublishStatus.Accepted)
                        {
                            Interlocked.Increment(ref accepted);
                        }

                        Interlocked.Increment(ref completed);
                        queue.ReleaseReservation(item.Payload.Length);
                        item.Payload.Dispose();
                    }
                }
            }, CancellationToken.None);
        }

        Stopwatch wall = Stopwatch.StartNew();
        sampler.Start();
        queueSampler.Start();

        long seq = 0;
        while (!cts.IsCancellationRequested)
        {
            long genStart = Stopwatch.GetTimestamp();
            string id = TransitBenchmarkCore.BuildMessageId(5, 0, Interlocked.Increment(ref seq), "diag-real");
            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(id, articleBytes);
            long genEnd = Stopwatch.GetTimestamp();
            long generationTicks = Math.Max(0, genEnd - genStart);

            long enqStart = Stopwatch.GetTimestamp();
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

            long enqEnd = Stopwatch.GetTimestamp();
            long enqueueTicks = Math.Max(0, enqEnd - enqStart);

            if (!admitted)
            {
                payload.Dispose();
                break;
            }

            tracker.RecordProduced(id, generationTicks, enqueueTicks, enqEnd);
            Interlocked.Increment(ref generated);
            Interlocked.Add(ref bytes, payload.Length);
        }

        queue.StopAdmission();
        await Task.WhenAll(workers).ConfigureAwait(false);

        queueSampler.Stop();
        sampler.Stop();
        wall.Stop();

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diag = publisher.CaptureConnectionDiagnosticsSnapshot();
        int peakPerConnection = diag.Connections.Length == 0 ? 0 : diag.Connections.Max(static x => x.Snapshot.MaxConcurrentSubmissions);
        int peakActualPending = tracker.PeakActualPending(diag);

        EndToEndResult result4 = new(
            Label: label,
            Generated: generated,
            Completed: completed,
            Accepted: accepted,
            Bytes: bytes,
            Elapsed: wall.Elapsed,
            CpuUtilizationPercent: sampler.AverageCpuPercent,
            EquivalentBusyCores: sampler.EquivalentBusyCores,
            PeakWorkingSetBytes: sampler.PeakWorkingSetBytes,
            AllocatedBytes: GC.GetTotalAllocatedBytes(false) - allocatedStart,
            Gen0: GC.CollectionCount(0) - gen0Start,
            Gen1: GC.CollectionCount(1) - gen1Start,
            Gen2: GC.CollectionCount(2) - gen2Start,
            QueueMin: queueSampler.MinDepth,
            QueueAvg: queueSampler.AverageDepth,
            QueueMax: queueSampler.MaxDepth,
            AvgGenerationUs: tracker.AverageGenerationUs,
            AvgQueueWaitUs: tracker.AverageQueueWaitUs,
            AvgDispatchWaitUs: tracker.AverageDispatchWaitUs,
            AvgPublishUs: tracker.AveragePublishUs,
            AvgLifecycleUs: tracker.AverageLifecycleUs,
            PeakActualPending: peakActualPending,
            PeakPerConnectionInFlight: peakPerConnection,
            ReadyConnections: diag.Connections.Count(static x => x.Snapshot.ReadyTransitionCount > 0),
            ActiveConnections: diag.Slots.Count(static x => x.TotalSubmissionsRouted > 0));

        PrintEndToEndResult(result4);
        return result4;
    }

    /// <summary>
    /// Performs the run ParallelGeneratorSweepAsync operation.
    /// </summary>
    private static async Task<List<GenerationOnlyResult>> RunParallelGeneratorSweepAsync(string label, int warmupSeconds, int measurementSeconds, int articleBytes, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");

        int[] workers = [1, 2, 4, 8, 16];
        List<GenerationOnlyResult> results = [];

        double previousRate = 0;
        foreach (int workerCount in workers)
        {
            GenerationOnlyResult result = await RunGeneratorOnlyAsync($"{label} (workers={workerCount})", warmupSeconds, measurementSeconds, articleBytes, workerCount, cancellationToken).ConfigureAwait(false);
            results.Add(result);

            if (previousRate > 0 && result.ArticlesPerSecond <= previousRate * 1.02)
            {
                Console.WriteLine($"Scaling plateau detected at workers={workerCount}; stopping sweep.");
                break;
            }

            previousRate = result.ArticlesPerSecond;
        }

        return results;
    }

    /// <summary>
    /// Performs the load RuntimeConfig operation.
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
    /// Performs the find BackFillerAppSettingsPath operation.
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
    /// Performs the compute PercentileMicroseconds operation.
    /// </summary>
    private static double ComputePercentileMicroseconds(IEnumerable<long> ticks, double percentile)
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
    /// Performs the print GenerationResult operation.
    /// </summary>
    private static void PrintGenerationResult(GenerationOnlyResult result)
    {
        Console.WriteLine($"Articles={result.Articles}, Articles/sec={result.ArticlesPerSecond:F4}, Gbps={result.Gbps:F4}, AvgUs={result.AverageLatencyUs:F3}, P50Us={result.P50LatencyUs:F3}, P95Us={result.P95LatencyUs:F3}, P99Us={result.P99LatencyUs:F3}");
        Console.WriteLine($"CPU%={result.CpuUtilizationPercent:F2}, EqBusyCores={result.EquivalentBusyCores:F3}, AllocPerArticle={result.AllocatedBytesPerArticle:F2} bytes, WSpeakMB={result.PeakWorkingSetBytes / 1024d / 1024d:F2}, GC=({result.Gen0},{result.Gen1},{result.Gen2})");
    }

    /// <summary>
    /// Performs the print QueuePipelineResult operation.
    /// </summary>
    private static void PrintQueuePipelineResult(QueuePipelineResult result)
    {
        Console.WriteLine($"Generated/sec={result.GeneratedPerSecond:F4}, Dispatched/sec={result.DispatchedPerSecond:F4}, Completed/sec={result.CompletedPerSecond:F4}, Gbps={result.Gbps:F4}");
        Console.WriteLine($"Queue[min/avg/max]={result.QueueMin}/{result.QueueAvg:F2}/{result.QueueMax}, PeakDispatchInFlight={result.PeakInFlight}");
        Console.WriteLine($"AvgUs: gen={result.AvgGenerationUs:F3}, enqueue={result.AvgEnqueueUs:F3}, dequeue={result.AvgDequeueUs:F3}");
        Console.WriteLine($"CPU%={result.CpuUtilizationPercent:F2}, EqBusyCores={result.EquivalentBusyCores:F3}, AllocPerArticle={result.AllocatedBytesPerArticle:F2} bytes, WSpeakMB={result.PeakWorkingSetBytes / 1024d / 1024d:F2}, GC=({result.Gen0},{result.Gen1},{result.Gen2})");
    }

    /// <summary>
    /// Performs the print EndToEndResult operation.
    /// </summary>
    private static void PrintEndToEndResult(EndToEndResult result)
    {
        Console.WriteLine($"Generated/sec={result.GeneratedPerSecond:F4}, Completed/sec={result.CompletedPerSecond:F4}, Accepted/sec={result.AcceptedPerSecond:F4}, Gbps={result.Gbps:F4}");
        Console.WriteLine($"Connections READY={result.ReadyConnections}, Active={result.ActiveConnections}, PeakActualPending={result.PeakActualPending}, PeakPerConnectionInFlight={result.PeakPerConnectionInFlight}");
        Console.WriteLine($"Queue[min/avg/max]={result.QueueMin}/{result.QueueAvg:F2}/{result.QueueMax}");
        Console.WriteLine($"Stage AvgUs: generation={result.AvgGenerationUs:F3}, queueWait={result.AvgQueueWaitUs:F3}, dispatchWait={result.AvgDispatchWaitUs:F3}, publish={result.AvgPublishUs:F3}, lifecycle={result.AvgLifecycleUs:F3}");
        Console.WriteLine($"CPU%={result.CpuUtilizationPercent:F2}, EqBusyCores={result.EquivalentBusyCores:F3}, AllocPerArticle={result.AllocatedBytesPerArticle:F2} bytes, WSpeakMB={result.PeakWorkingSetBytes / 1024d / 1024d:F2}, GC=({result.Gen0},{result.Gen1},{result.Gen2})");
    }

    /// <summary>
    /// Performs the print Summary operation.
    /// </summary>
    private static void PrintSummary(GenerationOnlyResult test1, QueuePipelineResult test2, QueuePipelineResult test3, EndToEndResult test4, List<GenerationOnlyResult> test5, int articleBytes)
    {
        Console.WriteLine();
        Console.WriteLine("=== Throughput Comparison Table ===");
        Console.WriteLine($"Test1 Generator-only: {test1.ArticlesPerSecond:F4} art/s, {test1.Gbps:F4} Gbps");
        Console.WriteLine($"Test2 Generator+Queue: {test2.CompletedPerSecond:F4} art/s, {test2.Gbps:F4} Gbps");
        Console.WriteLine($"Test3 Generator+Queue+Dispatch(no-op): {test3.CompletedPerSecond:F4} art/s, {test3.Gbps:F4} Gbps");
        Console.WriteLine($"Test4 Real end-to-end: {test4.AcceptedPerSecond:F4} art/s, {test4.Gbps:F4} Gbps");

        Console.WriteLine();
        Console.WriteLine("=== CPU / Core Utilization Comparison ===");
        Console.WriteLine($"Test1 CPU%={test1.CpuUtilizationPercent:F2}, EqBusyCores={test1.EquivalentBusyCores:F3}");
        Console.WriteLine($"Test2 CPU%={test2.CpuUtilizationPercent:F2}, EqBusyCores={test2.EquivalentBusyCores:F3}");
        Console.WriteLine($"Test3 CPU%={test3.CpuUtilizationPercent:F2}, EqBusyCores={test3.EquivalentBusyCores:F3}");
        Console.WriteLine($"Test4 CPU%={test4.CpuUtilizationPercent:F2}, EqBusyCores={test4.EquivalentBusyCores:F3}");
        Console.WriteLine("Per-core max utilization is not directly exposed by this benchmark implementation; EqBusyCores is derived from process CPU%.");

        Console.WriteLine();
        Console.WriteLine("=== Stage Latency Comparison (avg us) ===");
        Console.WriteLine($"Test2: generation={test2.AvgGenerationUs:F3}, enqueue={test2.AvgEnqueueUs:F3}, dequeue={test2.AvgDequeueUs:F3}");
        Console.WriteLine($"Test4: generation={test4.AvgGenerationUs:F3}, queueWait={test4.AvgQueueWaitUs:F3}, dispatchWait={test4.AvgDispatchWaitUs:F3}, publish={test4.AvgPublishUs:F3}, lifecycle={test4.AvgLifecycleUs:F3}");

        double tenGbpsRate = 10_000_000_000d / (articleBytes * 8d);
        Console.WriteLine();
        Console.WriteLine($"10Gbps required rate: {tenGbpsRate:F4} art/s; Test4 current ratio: {(test4.AcceptedPerSecond / tenGbpsRate):F4}");

        Console.WriteLine();
        Console.WriteLine("=== Parallel Generation Scaling ===");
        foreach (GenerationOnlyResult r in test5)
        {
            Console.WriteLine($"Workers={r.WorkerCount}: {r.ArticlesPerSecond:F4} art/s, {r.Gbps:F4} Gbps, CPU%={r.CpuUtilizationPercent:F2}, EqBusyCores={r.EquivalentBusyCores:F3}");
        }

        double dropStage2 = test1.ArticlesPerSecond - test2.CompletedPerSecond;
        double dropStage3 = test2.CompletedPerSecond - test3.CompletedPerSecond;
        double dropStage4 = test3.CompletedPerSecond - test4.AcceptedPerSecond;

        string firstDrop = dropStage2 > 0 ? "Generator -> Queue" : (dropStage3 > 0 ? "Queue -> Dispatcher" : "Dispatcher -> Real Publish");
        Console.WriteLine();
        Console.WriteLine($"First material drop stage: {firstDrop}");
        Console.WriteLine("Most likely bottleneck candidate is the first stage with sustained throughput loss and highest added latency in this table.");
        Console.WriteLine("Next experiment: isolate PublishAsync call-path internals by measuring per-connection outstanding depth over time against publish duration buckets.");
    }

    /// <summary>
    /// Defines the cpu Sampler class for benchmark or isolated-regression execution.
    /// </summary>
    private sealed class CpuSampler
    {
        /// <summary>
        /// Gets or sets the _process value.
        /// </summary>
        private readonly Process _process;
        /// <summary>
        /// Performs the _elapsed operation.
        /// </summary>
        private readonly Stopwatch _elapsed = Stopwatch.StartNew();
        /// <summary>
        /// Gets or sets the _cts value.
        /// </summary>
        private CancellationTokenSource? _cts;
        /// <summary>
        /// Gets or sets the _task value.
        /// </summary>
        private Task? _task;
        /// <summary>
        /// Gets or sets the _cpuStart value.
        /// </summary>
        private TimeSpan _cpuStart;
        /// <summary>
        /// Gets or sets the _peakWorkingSet value.
        /// </summary>
        private long _peakWorkingSet;

        /// <summary>
        /// Performs the cpu Sampler operation.
        /// </summary>
        internal CpuSampler(Process process)
        {
            _process = process;
            _peakWorkingSet = process.WorkingSet64;
        }

        /// <summary>
        /// Gets or sets the average CpuPercent value.
        /// </summary>
        internal double AverageCpuPercent { get; private set; }
        /// <summary>
        /// Gets or sets the equivalent BusyCores value.
        /// </summary>
        internal double EquivalentBusyCores => AverageCpuPercent / 100d * Environment.ProcessorCount;
        /// <summary>
        /// Gets or sets the peak WorkingSetBytes value.
        /// </summary>
        internal long PeakWorkingSetBytes => _peakWorkingSet;
        /// <summary>
        /// Gets or sets the total CpuTime value.
        /// </summary>
        internal TimeSpan TotalCpuTime { get; private set; }

        /// <summary>
        /// Performs the start operation.
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
        /// Performs the stop operation.
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
    /// Defines the queue DepthSampler class for benchmark or isolated-regression execution.
    /// </summary>
    private sealed class QueueDepthSampler
    {
        /// <summary>
        /// Gets or sets the _queue value.
        /// </summary>
        private readonly TransitBenchmarkCore.BoundedArticleQueue _queue;
        /// <summary>
        /// Gets or sets the _cts value.
        /// </summary>
        private CancellationTokenSource? _cts;
        /// <summary>
        /// Gets or sets the _task value.
        /// </summary>
        private Task? _task;
        /// <summary>
        /// Gets or sets the _sum value.
        /// </summary>
        private long _sum;
        /// <summary>
        /// Gets or sets the _count value.
        /// </summary>
        private long _count;
        /// <summary>
        /// Gets or sets the _min value.
        /// </summary>
        private int _min = int.MaxValue;
        /// <summary>
        /// Gets or sets the _max value.
        /// </summary>
        private int _max;

        /// <summary>
        /// Performs the queue DepthSampler operation.
        /// </summary>
        internal QueueDepthSampler(TransitBenchmarkCore.BoundedArticleQueue queue)
        {
            _queue = queue;
        }

        /// <summary>
        /// Gets or sets the min Depth value.
        /// </summary>
        internal int MinDepth => _min == int.MaxValue ? 0 : _min;
        /// <summary>
        /// Gets or sets the max Depth value.
        /// </summary>
        internal int MaxDepth => _max;
        /// <summary>
        /// Performs the average Depth operation.
        /// </summary>
        internal double AverageDepth => _count == 0 ? 0 : (double)_sum / _count;

        /// <summary>
        /// Performs the start operation.
        /// </summary>
        internal void Start()
        {
            _cts = new CancellationTokenSource();
            _task = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    int depth = _queue.CurrentQueuedCount;
                    _sum += depth;
                    _count++;
                    _min = Math.Min(_min, depth);
                    _max = Math.Max(_max, depth);
                    await Task.Delay(100, _cts.Token).ConfigureAwait(false);
                }
            }, CancellationToken.None);
        }

        /// <summary>
        /// Performs the stop operation.
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
    /// Defines the stage Tracker class for benchmark or isolated-regression execution.
    /// </summary>
    private sealed class StageTracker
    {
        /// <summary>
        /// Performs the _stamps operation.
        /// </summary>
        private readonly ConcurrentDictionary<string, StageStamp> _stamps = new(StringComparer.Ordinal);
        /// <summary>
        /// Gets or sets the _generationTicks value.
        /// </summary>
        private long _generationTicks;
        /// <summary>
        /// Gets or sets the _queueWaitTicks value.
        /// </summary>
        private long _queueWaitTicks;
        /// <summary>
        /// Gets or sets the _dispatchWaitTicks value.
        /// </summary>
        private long _dispatchWaitTicks;
        /// <summary>
        /// Gets or sets the _publishTicks value.
        /// </summary>
        private long _publishTicks;
        /// <summary>
        /// Gets or sets the _lifecycleTicks value.
        /// </summary>
        private long _lifecycleTicks;
        /// <summary>
        /// Gets or sets the _count value.
        /// </summary>
        private long _count;

        /// <summary>
        /// Performs the average GenerationUs operation.
        /// </summary>
        internal double AverageGenerationUs => _count == 0 ? 0 : _generationTicks * 1_000_000d / (Stopwatch.Frequency * _count);
        /// <summary>
        /// Performs the average QueueWaitUs operation.
        /// </summary>
        internal double AverageQueueWaitUs => _count == 0 ? 0 : _queueWaitTicks * 1_000_000d / (Stopwatch.Frequency * _count);
        /// <summary>
        /// Performs the average DispatchWaitUs operation.
        /// </summary>
        internal double AverageDispatchWaitUs => _count == 0 ? 0 : _dispatchWaitTicks * 1_000_000d / (Stopwatch.Frequency * _count);
        /// <summary>
        /// Performs the average PublishUs operation.
        /// </summary>
        internal double AveragePublishUs => _count == 0 ? 0 : _publishTicks * 1_000_000d / (Stopwatch.Frequency * _count);
        /// <summary>
        /// Performs the average LifecycleUs operation.
        /// </summary>
        internal double AverageLifecycleUs => _count == 0 ? 0 : _lifecycleTicks * 1_000_000d / (Stopwatch.Frequency * _count);

        /// <summary>
        /// Performs the record Produced operation.
        /// </summary>
        internal void RecordProduced(string messageId, long generationTicks, long enqueueTicks, long enqueueEndTick)
        {
            _stamps[messageId] = new StageStamp(generationTicks, enqueueTicks, enqueueEndTick, 0, 0);
        }

        /// <summary>
        /// Performs the record Dequeued operation.
        /// </summary>
        internal void RecordDequeued(string messageId, long dequeueTick)
        {
            _ = _stamps.AddOrUpdate(
                messageId,
                _ => new StageStamp(0, 0, 0, dequeueTick, 0),
                (_, old) => old with { DequeueTick = dequeueTick });
        }

        /// <summary>
        /// Performs the record PublishStart operation.
        /// </summary>
        internal void RecordPublishStart(string messageId, long publishStart)
        {
            _ = _stamps.AddOrUpdate(
                messageId,
                _ => new StageStamp(0, 0, 0, 0, publishStart),
                (_, old) => old with { PublishStartTick = publishStart });
        }

        /// <summary>
        /// Performs the record PublishEnd operation.
        /// </summary>
        internal void RecordPublishEnd(string messageId, long publishEndTick, bool accepted)
        {
            _ = accepted;
            if (!_stamps.TryRemove(messageId, out StageStamp stamp))
            {
                return;
            }

            long queueWait = stamp.DequeueTick > 0 && stamp.EnqueueEndTick > 0 ? Math.Max(0, stamp.DequeueTick - stamp.EnqueueEndTick) : 0;
            long dispatchWait = stamp.PublishStartTick > 0 && stamp.DequeueTick > 0 ? Math.Max(0, stamp.PublishStartTick - stamp.DequeueTick) : 0;
            long publish = stamp.PublishStartTick > 0 ? Math.Max(0, publishEndTick - stamp.PublishStartTick) : 0;
            long lifecycle = stamp.GenerationTicks + queueWait + dispatchWait + publish;

            Interlocked.Add(ref _generationTicks, stamp.GenerationTicks);
            Interlocked.Add(ref _queueWaitTicks, queueWait);
            Interlocked.Add(ref _dispatchWaitTicks, dispatchWait);
            Interlocked.Add(ref _publishTicks, publish);
            Interlocked.Add(ref _lifecycleTicks, lifecycle);
            Interlocked.Increment(ref _count);
        }

        /// <summary>
        /// Performs the peak ActualPending operation.
        /// </summary>
        internal int PeakActualPending(TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics)
        {
            return diagnostics.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
        }

        /// <summary>
        /// Defines the stage Stamp record struct for benchmark or isolated-regression execution.
        /// </summary>
        private readonly record struct StageStamp(long GenerationTicks, long EnqueueTicks, long EnqueueEndTick, long DequeueTick, long PublishStartTick);
    }

    /// <summary>
    /// Defines the runtime Config record struct for benchmark or isolated-regression execution.
    /// </summary>
    private readonly record struct RuntimeConfig(string Host, int Port, bool UseSsl);

    /// <summary>
    /// Defines the generation OnlyResult record struct for benchmark or isolated-regression execution.
    /// </summary>
    private readonly record struct GenerationOnlyResult(
        string Label,
        int WorkerCount,
        long Articles,
        long Bytes,
        TimeSpan Elapsed,
        double CpuUtilizationPercent,
        double EquivalentBusyCores,
        long PeakWorkingSetBytes,
        long AllocatedBytes,
        int Gen0,
        int Gen1,
        int Gen2,
        double AverageLatencyUs,
        double P50LatencyUs,
        double P95LatencyUs,
        double P99LatencyUs,
        TimeSpan CpuTime)
    {
        /// <summary>
        /// Gets or sets the articles PerSecond value.
        /// </summary>
        internal double ArticlesPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Articles / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the gbps value.
        /// </summary>
        internal double Gbps => Elapsed.TotalSeconds <= 0 ? 0 : Bytes * 8d / 1_000_000_000d / Elapsed.TotalSeconds;
        /// <summary>
        /// Performs the allocated BytesPerArticle operation.
        /// </summary>
        internal double AllocatedBytesPerArticle => Articles == 0 ? 0 : (double)AllocatedBytes / Articles;
    }

    /// <summary>
    /// Defines the queue PipelineResult record struct for benchmark or isolated-regression execution.
    /// </summary>
    private readonly record struct QueuePipelineResult(
        string Label,
        long Generated,
        long Dispatched,
        long Completed,
        long Bytes,
        TimeSpan Elapsed,
        double CpuUtilizationPercent,
        double EquivalentBusyCores,
        long PeakWorkingSetBytes,
        long AllocatedBytes,
        int Gen0,
        int Gen1,
        int Gen2,
        int QueueMin,
        double QueueAvg,
        int QueueMax,
        double AvgGenerationUs,
        double AvgEnqueueUs,
        double AvgDequeueUs,
        double P50GenerationUs,
        double P95GenerationUs,
        double P99GenerationUs,
        int PeakInFlight,
        double AvgQueueWaitUs,
        double AvgDispatchWaitUs,
        double AvgPublishUs)
    {
        /// <summary>
        /// Gets or sets the generated PerSecond value.
        /// </summary>
        internal double GeneratedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Generated / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the dispatched PerSecond value.
        /// </summary>
        internal double DispatchedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Dispatched / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the completed PerSecond value.
        /// </summary>
        internal double CompletedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Completed / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the gbps value.
        /// </summary>
        internal double Gbps => Elapsed.TotalSeconds <= 0 ? 0 : Bytes * 8d / 1_000_000_000d / Elapsed.TotalSeconds;
        /// <summary>
        /// Performs the allocated BytesPerArticle operation.
        /// </summary>
        internal double AllocatedBytesPerArticle => Generated == 0 ? 0 : (double)AllocatedBytes / Generated;
    }

    /// <summary>
    /// Defines the end ToEndResult record struct for benchmark or isolated-regression execution.
    /// </summary>
    private readonly record struct EndToEndResult(
        string Label,
        long Generated,
        long Completed,
        long Accepted,
        long Bytes,
        TimeSpan Elapsed,
        double CpuUtilizationPercent,
        double EquivalentBusyCores,
        long PeakWorkingSetBytes,
        long AllocatedBytes,
        int Gen0,
        int Gen1,
        int Gen2,
        int QueueMin,
        double QueueAvg,
        int QueueMax,
        double AvgGenerationUs,
        double AvgQueueWaitUs,
        double AvgDispatchWaitUs,
        double AvgPublishUs,
        double AvgLifecycleUs,
        int PeakActualPending,
        int PeakPerConnectionInFlight,
        int ReadyConnections,
        int ActiveConnections)
    {
        /// <summary>
        /// Gets or sets the generated PerSecond value.
        /// </summary>
        internal double GeneratedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Generated / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the completed PerSecond value.
        /// </summary>
        internal double CompletedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Completed / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the accepted PerSecond value.
        /// </summary>
        internal double AcceptedPerSecond => Elapsed.TotalSeconds <= 0 ? 0 : Accepted / Elapsed.TotalSeconds;
        /// <summary>
        /// Gets or sets the gbps value.
        /// </summary>
        internal double Gbps => Elapsed.TotalSeconds <= 0 ? 0 : Bytes * 8d / 1_000_000_000d / Elapsed.TotalSeconds;
        /// <summary>
        /// Performs the allocated BytesPerArticle operation.
        /// </summary>
        internal double AllocatedBytesPerArticle => Generated == 0 ? 0 : (double)AllocatedBytes / Generated;
    }
}
