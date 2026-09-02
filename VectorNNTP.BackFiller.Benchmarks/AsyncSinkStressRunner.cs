// <copyright file="AsyncSinkStressRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// AsyncSinkStressRunner: exercises asynchronous logging sinks under burst, sustained-rate, and shutdown workloads.

using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.Async;

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Represents the async SinkStressRunner class used by the benchmark or regression gate.
    /// </summary>
    internal static class AsyncSinkStressRunner
    {
        private const int DefaultBufferSize = 10_000;
        private static readonly int[] ProducerCounts = [1, 2, 4, 8, 16, 32];
        private static readonly int[] SustainedRatesPerSecond = [1_000, 10_000, 50_000, 100_000];
        private static readonly TimeSpan SustainedDuration = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Runs AllAsync.
        /// </summary>
        public static async Task RunAllAsync()
        {
            Console.WriteLine("=== Async Sink Stress Runner ===");
            Console.WriteLine($"OS: {System.Runtime.InteropServices.RuntimeInformation.OSDescription}");
            Console.WriteLine($"Runtime: {Environment.Version}");
            Console.WriteLine($"Process Architecture: {System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}");
            Console.WriteLine($"CPU Count: {Environment.ProcessorCount}");
            Console.WriteLine($"Server GC: {System.Runtime.GCSettings.IsServerGC}");

            await RunBurstMatrixAsync().ConfigureAwait(false);
            await RunSustainedMatrixAsync().ConfigureAwait(false);
            await RunShutdownFlushScenarioAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Runs BurstMatrixAsync.
        /// </summary>
        private static async Task RunBurstMatrixAsync()
        {
            Console.WriteLine();
            Console.WriteLine("=== Burst Matrix ===");
            Console.WriteLine("| Producers | Events/Producer | Submitted | Written | EventsLost | ProduceMs | FlushMs | TotalMs | P50us | P95us | P99us | MaxUs |");
            Console.WriteLine("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

            const int eventsPerProducer = 50_000;

            foreach (int producerCount in ProducerCounts)
            {
                StressScenarioResult result = await RunBurstScenarioAsync(producerCount, eventsPerProducer).ConfigureAwait(false);
                Console.WriteLine($"| {result.ProducerCount} | {result.EventsPerProducer} | {result.Submitted} | {result.Written} | {result.EventsLost} | {result.ProduceMilliseconds:F2} | {result.FlushMilliseconds:F2} | {result.TotalMilliseconds:F2} | {result.ProducerLatency.P50Microseconds:F2} | {result.ProducerLatency.P95Microseconds:F2} | {result.ProducerLatency.P99Microseconds:F2} | {result.ProducerLatency.MaxMicroseconds:F2} |");
            }
        }

        /// <summary>
        /// Runs SustainedMatrixAsync.
        /// </summary>
        private static async Task RunSustainedMatrixAsync()
        {
            Console.WriteLine();
            Console.WriteLine("=== Sustained Matrix ===");
            Console.WriteLine("| Producers | TargetRate/s | DurationSec | Submitted | Written | EventsLost | ActualRate/s | ProduceMs | FlushMs | P50us | P95us | P99us | MaxUs |");
            Console.WriteLine("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

            foreach (int producerCount in ProducerCounts)
            {
                foreach (int targetRate in SustainedRatesPerSecond)
                {
                    StressScenarioResult result = await RunSustainedScenarioAsync(producerCount, targetRate, SustainedDuration).ConfigureAwait(false);
                    Console.WriteLine($"| {result.ProducerCount} | {result.TargetRatePerSecond} | {result.DurationSeconds:F1} | {result.Submitted} | {result.Written} | {result.EventsLost} | {result.ActualRatePerSecond:F2} | {result.ProduceMilliseconds:F2} | {result.FlushMilliseconds:F2} | {result.ProducerLatency.P50Microseconds:F2} | {result.ProducerLatency.P95Microseconds:F2} | {result.ProducerLatency.P99Microseconds:F2} | {result.ProducerLatency.MaxMicroseconds:F2} |");
                }
            }
        }

        /// <summary>
        /// Runs ShutdownFlushScenarioAsync.
        /// </summary>
        private static async Task RunShutdownFlushScenarioAsync()
        {
            Console.WriteLine();
            Console.WriteLine("=== Shutdown Flush Scenario ===");

            const int producerCount = 8;
            const int eventsPerProducer = 75_000;

            StressScenarioResult result = await RunBurstScenarioAsync(producerCount, eventsPerProducer).ConfigureAwait(false);

            Console.WriteLine($"Submitted: {result.Submitted}");
            Console.WriteLine($"Written: {result.Written}");
            Console.WriteLine($"EventsLost: {result.EventsLost}");
            Console.WriteLine($"ProduceMs: {result.ProduceMilliseconds:F2}");
            Console.WriteLine($"FlushMs: {result.FlushMilliseconds:F2}");
            Console.WriteLine($"TotalMs: {result.TotalMilliseconds:F2}");
        }

        /// <summary>
        /// Runs BurstScenarioAsync.
        /// </summary>
        private static async Task<StressScenarioResult> RunBurstScenarioAsync(int producerCount, int eventsPerProducer)
        {
            string outputDirectory = CreateTempOutputDirectory("burst");
            SequenceCountingSink countingSink = new();

            Serilog.ILogger logger = BuildProductionLikeLogger(outputDirectory, countingSink);

            try
            {
                long sequence = 0;
                ConcurrentBag<double> latencyMicros = [];

                Stopwatch produceSw = Stopwatch.StartNew();

                Task[] producers = Enumerable.Range(0, producerCount)
                    .Select(producerId => Task.Run(() =>
                    {
                        for (int i = 0; i < eventsPerProducer; i++)
                        {
                            long eventSequence = Interlocked.Increment(ref sequence);
                            long start = Stopwatch.GetTimestamp();

                            logger.Information(
                                "Stress event {Sequence} producer={ProducerId} index={Index} elapsed={Elapsed} state={State}",
                                eventSequence,
                                producerId,
                                i,
                                TimeSpan.FromMilliseconds(i % 1000),
                                "Burst");

                            long stop = Stopwatch.GetTimestamp();
                            latencyMicros.Add(ToMicroseconds(stop - start));
                        }
                    }))
                    .ToArray();

                await Task.WhenAll(producers).ConfigureAwait(false);
                produceSw.Stop();

                Stopwatch flushSw = Stopwatch.StartNew();
                if (logger is IDisposable disposable)
                {
                    disposable.Dispose();
                }

                flushSw.Stop();

                return BuildResult(
                    producerCount,
                    eventsPerProducer,
                    targetRatePerSecond: null,
                    durationSeconds: null,
                    submitted: sequence,
                    written: countingSink.WrittenCount,
                    produceMilliseconds: produceSw.Elapsed.TotalMilliseconds,
                    flushMilliseconds: flushSw.Elapsed.TotalMilliseconds,
                    producerLatency: PercentileSet.From(latencyMicros));
            }
            finally
            {
                TryDeleteDirectory(outputDirectory);
            }
        }

        /// <summary>
        /// Runs SustainedScenarioAsync.
        /// </summary>
        private static async Task<StressScenarioResult> RunSustainedScenarioAsync(int producerCount, int targetRatePerSecond, TimeSpan duration)
        {
            string outputDirectory = CreateTempOutputDirectory("sustained");
            SequenceCountingSink countingSink = new();

            Serilog.ILogger logger = BuildProductionLikeLogger(outputDirectory, countingSink);

            try
            {
                long sequence = 0;
                ConcurrentBag<double> latencyMicros = [];
                using CancellationTokenSource cts = new(duration);

                int perProducerRate = Math.Max(1, targetRatePerSecond / producerCount);
                TimeSpan delay = TimeSpan.FromSeconds(1.0 / perProducerRate);

                Stopwatch produceSw = Stopwatch.StartNew();

                Task[] producers = Enumerable.Range(0, producerCount)
                    .Select(producerId => Task.Run(async () =>
                    {
                        while (!cts.Token.IsCancellationRequested)
                        {
                            long eventSequence = Interlocked.Increment(ref sequence);
                            long start = Stopwatch.GetTimestamp();

                            logger.Information(
                                "Sustained event {Sequence} producer={ProducerId} elapsed={Elapsed} state={State}",
                                eventSequence,
                                producerId,
                                TimeSpan.FromMilliseconds(eventSequence % 1000),
                                "Sustained");

                            long stop = Stopwatch.GetTimestamp();
                            latencyMicros.Add(ToMicroseconds(stop - start));

                            try
                            {
                                await Task.Delay(delay, cts.Token).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException) when (cts.IsCancellationRequested)
                            {
                                break;
                            }
                        }
                    }))
                    .ToArray();

                await Task.WhenAll(producers).ConfigureAwait(false);
                produceSw.Stop();

                Stopwatch flushSw = Stopwatch.StartNew();
                if (logger is IDisposable disposable)
                {
                    disposable.Dispose();
                }

                flushSw.Stop();

                return BuildResult(
                    producerCount,
                    eventsPerProducer: null,
                    targetRatePerSecond,
                    duration.TotalSeconds,
                    submitted: sequence,
                    written: countingSink.WrittenCount,
                    produceMilliseconds: produceSw.Elapsed.TotalMilliseconds,
                    flushMilliseconds: flushSw.Elapsed.TotalMilliseconds,
                    producerLatency: PercentileSet.From(latencyMicros));
            }
            finally
            {
                TryDeleteDirectory(outputDirectory);
            }
        }

        /// <summary>
        /// Builds ProductionLikeLogger.
        /// </summary>
        private static Serilog.ILogger BuildProductionLikeLogger(string outputDirectory, SequenceCountingSink countingSink)
        {
            string logFilePath = Path.Combine(outputDirectory, "stress-.log");

            return new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Information)
                .MinimumLevel.Override("System", LogEventLevel.Warning)
                .Enrich.FromLogContext()
                .Enrich.WithProperty("Application", "VectorNNTP.BackFiller.Benchmarks")
                .Enrich.WithProperty("ProcessId", Environment.ProcessId)
                .WriteTo.Async(
                    sink => sink.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}"),
                    bufferSize: DefaultBufferSize,
                    blockWhenFull: true)
                .WriteTo.Async(
                    sink => sink.File(
                        path: logFilePath,
                        rollingInterval: RollingInterval.Day,
                        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
                        retainedFileCountLimit: 2,
                        fileSizeLimitBytes: 256 * 1024 * 1024,
                        rollOnFileSizeLimit: true),
                    bufferSize: DefaultBufferSize,
                    blockWhenFull: true)
                .WriteTo.Sink(countingSink)
                .CreateLogger();
        }

        /// <summary>
        /// Builds Result.
        /// </summary>
        private static StressScenarioResult BuildResult(
            int producerCount,
            int? eventsPerProducer,
            int? targetRatePerSecond,
            double? durationSeconds,
            long submitted,
            long written,
            double produceMilliseconds,
            double flushMilliseconds,
            PercentileSet producerLatency)
        {
            double totalSeconds = (produceMilliseconds + flushMilliseconds) / 1000.0;
            double actualRate = totalSeconds <= 0 ? 0 : submitted / totalSeconds;

            return new StressScenarioResult(
                ProducerCount: producerCount,
                EventsPerProducer: eventsPerProducer,
                TargetRatePerSecond: targetRatePerSecond,
                DurationSeconds: durationSeconds,
                Submitted: submitted,
                Written: written,
                EventsLost: submitted - written,
                ProduceMilliseconds: produceMilliseconds,
                FlushMilliseconds: flushMilliseconds,
                TotalMilliseconds: produceMilliseconds + flushMilliseconds,
                ActualRatePerSecond: actualRate,
                ProducerLatency: producerLatency);
        }

        /// <summary>
        /// Creates TempOutputDirectory.
        /// </summary>
        private static string CreateTempOutputDirectory(string scenario)
        {
            string directory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Benchmarks", scenario, Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            return directory;
        }
        private static void TryDeleteDirectory(string path)
        {
            try
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, recursive: true);
                }
            }
            catch
            {
                // best effort cleanup
            }
        }

        /// <summary>
        /// Converts to Microseconds.
        /// </summary>
        private static double ToMicroseconds(long ticks)
        {
            return ticks * 1_000_000.0 / Stopwatch.Frequency;
        }

        /// <summary>
        /// Represents the sequence CountingSink class used by the benchmark or regression gate.
        /// </summary>
        private sealed class SequenceCountingSink : Serilog.Core.ILogEventSink
        {
            private long _writtenCount;
            public long WrittenCount => Interlocked.Read(ref _writtenCount);

            /// <summary>
            /// Runs the emit benchmark scenario.
            /// </summary>
            public void Emit(LogEvent logEvent)
            {
                _ = logEvent;
                _ = Interlocked.Increment(ref _writtenCount);
            }
        }

        /// <summary>
        /// Represents the percentile Set record struct used by the benchmark or regression gate.
        /// </summary>
        private readonly record struct PercentileSet(double P50Microseconds, double P95Microseconds, double P99Microseconds, double MaxMicroseconds)
        {
            /// <summary>
            /// Runs the from benchmark scenario.
            /// </summary>
            public static PercentileSet From(IEnumerable<double> values)
            {
                double[] ordered = values.OrderBy(v => v).ToArray();
                if (ordered.Length == 0)
                {
                    return new PercentileSet(0, 0, 0, 0);
                }

                return new PercentileSet(
                    P50Microseconds: Percentile(ordered, 0.50),
                    P95Microseconds: Percentile(ordered, 0.95),
                    P99Microseconds: Percentile(ordered, 0.99),
                    MaxMicroseconds: ordered[^1]);
            }

            /// <summary>
            /// Runs the percentile benchmark scenario.
            /// </summary>
            private static double Percentile(double[] ordered, double percentile)
            {
                if (ordered.Length == 0)
                {
                    return 0;
                }

                double position = (ordered.Length - 1) * percentile;
                int lower = (int)Math.Floor(position);
                int upper = (int)Math.Ceiling(position);
                if (lower == upper)
                {
                    return ordered[lower];
                }

                double weight = position - lower;
                return ordered[lower] + ((ordered[upper] - ordered[lower]) * weight);
            }
        }

        /// <summary>
        /// Represents the stress ScenarioResult record struct used by the benchmark or regression gate.
        /// </summary>
        private readonly record struct StressScenarioResult(
            int ProducerCount,
            int? EventsPerProducer,
            int? TargetRatePerSecond,
            double? DurationSeconds,
            long Submitted,
            long Written,
            long EventsLost,
            double ProduceMilliseconds,
            double FlushMilliseconds,
            double TotalMilliseconds,
            double ActualRatePerSecond,
            PercentileSet ProducerLatency);
    }
}



