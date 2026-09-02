// <copyright file="GeneratorBaselineRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Workload/GeneratorBaselineRunner: prepares and drives reproducible benchmark input workloads.

using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the generator BaselineRunner class used by the benchmark or regression gate.
/// </summary>
internal static class GeneratorBaselineRunner
{
    /// <summary>
    /// Gets or sets the default ArticleTargetBytes.
    /// </summary>
    private const int DefaultArticleTargetBytes = 1 * 1024 * 1024;
    /// <summary>
    /// Gets or sets the default WarmupSeconds.
    /// </summary>
    private const int DefaultWarmupSeconds = 10;
    /// <summary>
    /// Gets or sets the default GeneratorMeasurementSeconds.
    /// </summary>
    private const int DefaultGeneratorMeasurementSeconds = 30;

    /// <summary>
    /// Runs Async.

    /// </summary>
    internal static async Task RunAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        int warmupSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.WarmupSeconds ?? DefaultWarmupSeconds,
            min: 1,
            max: 600,
            optionName: "warmup-seconds");

        int measurementSeconds = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.DurationSeconds ?? DefaultGeneratorMeasurementSeconds,
            min: 1,
            max: 3600,
            optionName: "duration-seconds");

        int articleTargetBytes = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(
            cliOptions.ArticleKilobytes is null ? DefaultArticleTargetBytes : checked(cliOptions.ArticleKilobytes.Value * 1024),
            min: 128 * 1024,
            max: 4 * 1024 * 1024,
            optionName: "article-kib");

        Console.WriteLine("=== Transit Generator Baseline (no network I/O) ===");
        Console.WriteLine("Generator path: TransitBenchmarkCore.BuildMessageId + TransitBenchmarkCore.ArticlePayload.Create + Dispose");
        Console.WriteLine($"Warmup seconds: {warmupSeconds}");
        Console.WriteLine($"Measurement seconds: {measurementSeconds}");
        Console.WriteLine($"Article target bytes: {articleTargetBytes}");

        using CancellationTokenSource warmupCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        warmupCts.CancelAfter(TimeSpan.FromSeconds(warmupSeconds));

        long warmupSequence = 0;
        while (!warmupCts.IsCancellationRequested)
        {
            string warmupMessageId = TransitBenchmarkCore.BuildMessageId(
                benchmarkInstanceId: 0,
                workerId: 0,
                sequence: Interlocked.Increment(ref warmupSequence),
                phase: "gen-warmup");

            TransitBenchmarkCore.ArticlePayload warmupPayload = TransitBenchmarkCore.ArticlePayload.Create(warmupMessageId, articleTargetBytes);
            warmupPayload.Dispose();
        }

        Process process = Process.GetCurrentProcess();
        TimeSpan cpuStart = process.TotalProcessorTime;
        long workingSetPeakBytes = process.WorkingSet64;
        long allocatedStartBytes = GC.GetTotalAllocatedBytes(precise: false);
        int gen0Start = GC.CollectionCount(0);
        int gen1Start = GC.CollectionCount(1);
        int gen2Start = GC.CollectionCount(2);

        using CancellationTokenSource measurementCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        measurementCts.CancelAfter(TimeSpan.FromSeconds(measurementSeconds));

        long benchmarkInstanceId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long sequence = 0;
        long generatedArticles = 0;
        long generatedBytes = 0;
        long latencyTicksTotal = 0;
        List<long> latencySamples = new(capacity: 262_144);

        Stopwatch wallClock = Stopwatch.StartNew();

        while (!measurementCts.IsCancellationRequested)
        {
            long sampleStart = Stopwatch.GetTimestamp();

            string messageId = TransitBenchmarkCore.BuildMessageId(
                benchmarkInstanceId,
                workerId: 0,
                sequence: Interlocked.Increment(ref sequence),
                phase: "gen-measure");

            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(messageId, articleTargetBytes);
            int payloadLength = payload.Length;
            payload.Dispose();

            long sampleEnd = Stopwatch.GetTimestamp();
            long sampleTicks = Math.Max(0, sampleEnd - sampleStart);

            generatedArticles++;
            generatedBytes += payloadLength;
            latencyTicksTotal += sampleTicks;
            latencySamples.Add(sampleTicks);

            if ((generatedArticles & 0x3FF) == 0)
            {
                long workingSet = process.WorkingSet64;
                if (workingSet > workingSetPeakBytes)
                {
                    workingSetPeakBytes = workingSet;
                }
            }
        }

        wallClock.Stop();

        TimeSpan cpuEnd = process.TotalProcessorTime;
        long allocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocatedStartBytes;
        int gen0Collections = GC.CollectionCount(0) - gen0Start;
        int gen1Collections = GC.CollectionCount(1) - gen1Start;
        int gen2Collections = GC.CollectionCount(2) - gen2Start;

        double elapsedSeconds = Math.Max(0.000001d, wallClock.Elapsed.TotalSeconds);
        double articlesPerSecond = generatedArticles / elapsedSeconds;
        double mibPerSecond = generatedBytes / 1024d / 1024d / elapsedSeconds;
        double gibPerSecond = generatedBytes / 1024d / 1024d / 1024d / elapsedSeconds;
        double gbps = generatedBytes * 8d / 1_000_000_000d / elapsedSeconds;
        double cpuTimeSeconds = Math.Max(0d, (cpuEnd - cpuStart).TotalSeconds);
        double cpuUtilizationPercent = cpuTimeSeconds / (Environment.ProcessorCount * elapsedSeconds) * 100d;
        double allocatedBytesPerArticle = generatedArticles == 0 ? 0 : (double)allocatedBytes / generatedArticles;

        double avgLatencyUs = generatedArticles == 0
            ? 0
            : latencyTicksTotal * 1_000_000d / (Stopwatch.Frequency * generatedArticles);

        latencySamples.Sort();

        double p50LatencyUs = MetricMathHelpers.ComputePercentileMicroseconds(latencySamples, 0.50);
        double p95LatencyUs = MetricMathHelpers.ComputePercentileMicroseconds(latencySamples, 0.95);
        double p99LatencyUs = MetricMathHelpers.ComputePercentileMicroseconds(latencySamples, 0.99);

        Console.WriteLine();
        Console.WriteLine($"Total articles generated: {generatedArticles}");
        Console.WriteLine($"Total bytes generated: {generatedBytes}");
        Console.WriteLine($"Elapsed time: {wallClock.Elapsed.TotalSeconds:F3}s");
        Console.WriteLine($"Articles/sec: {articlesPerSecond:F4}");
        Console.WriteLine($"MiB/sec: {mibPerSecond:F4}");
        Console.WriteLine($"GiB/sec: {gibPerSecond:F4}");
        Console.WriteLine($"Gbps equivalent: {gbps:F4}");

        Console.WriteLine();
        Console.WriteLine($"CPU time seconds: {cpuTimeSeconds:F4}");
        Console.WriteLine($"CPU utilization %: {cpuUtilizationPercent:F2}");
        Console.WriteLine($"Peak working set MB: {workingSetPeakBytes / 1024d / 1024d:F2}");

        Console.WriteLine();
        Console.WriteLine($"Allocated bytes: {allocatedBytes}");
        Console.WriteLine($"Allocated MB: {allocatedBytes / 1024d / 1024d:F2}");
        Console.WriteLine($"Allocated bytes/article: {allocatedBytesPerArticle:F2}");
        Console.WriteLine($"Allocated KiB/article: {allocatedBytesPerArticle / 1024d:F2}");

        Console.WriteLine();
        Console.WriteLine($"GC Gen0 collections: {gen0Collections}");
        Console.WriteLine($"GC Gen1 collections: {gen1Collections}");
        Console.WriteLine($"GC Gen2 collections: {gen2Collections}");

        Console.WriteLine();
        Console.WriteLine($"Average generation time/article (us): {avgLatencyUs:F3}");
        Console.WriteLine($"P50 generation time/article (us): {p50LatencyUs:F3}");
        Console.WriteLine($"P95 generation time/article (us): {p95LatencyUs:F3}");
        Console.WriteLine($"P99 generation time/article (us): {p99LatencyUs:F3}");

        Console.WriteLine();
        FormatHelpers.PrintRequiredRateComparison(articlesPerSecond, articleTargetBytes);
    }
}
