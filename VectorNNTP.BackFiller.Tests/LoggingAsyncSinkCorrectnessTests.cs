// <copyright file="LoggingAsyncSinkCorrectnessTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for logging async sink correctness, covering the covered test contracts.

using Serilog;
using Serilog.Events;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Covers logging async sink correctness behavior and invariants exercised by this test suite.
    /// </summary>
    public sealed class LoggingAsyncSinkCorrectnessTests
    {
        /// <summary>
        /// Exercises async sink  block when full  emits all submitted events behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public Task AsyncSink_BlockWhenFull_EmitsAllSubmittedEvents()
        {
            string outputDirectory = CreateTempOutputDirectory();
            CountingSink countingSink = new();

            ILogger logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Async(
                    configure: sink => sink.Sink(countingSink),
                    bufferSize: 256,
                    blockWhenFull: true)
                .WriteTo.Async(
                    configure: sink => sink.File(
                        path: Path.Combine(outputDirectory, "correctness-.log"),
                        rollingInterval: RollingInterval.Day,
                        retainedFileCountLimit: 1,
                        fileSizeLimitBytes: 64 * 1024 * 1024,
                        rollOnFileSizeLimit: true),
                    bufferSize: 256,
                    blockWhenFull: true)
                .CreateLogger();

            /// <summary>
            /// Supplies total events for the fixture or scenario under test.
            /// </summary>
            const int TotalEvents = 10_000;

            for (int i = 0; i < TotalEvents; i++)
            {
                logger.Information("Correctness event {Sequence}", i);
            }

            if (logger is IDisposable disposable)
            {
                disposable.Dispose();
            }

            Assert.Equal(TotalEvents, countingSink.Count);

            TryDeleteDirectory(outputDirectory);
            return Task.CompletedTask;
        }
        /// <summary>
        /// Exercises async sink  close and flush async  drains queued events behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public Task AsyncSink_CloseAndFlushAsync_DrainsQueuedEvents()
        {
            string outputDirectory = CreateTempOutputDirectory();
            CountingSink countingSink = new();

            ILogger logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Async(
                    configure: sink => sink.Sink(countingSink),
                    bufferSize: 128,
                    blockWhenFull: true)
                .WriteTo.Async(
                    configure: sink => sink.Console(),
                    bufferSize: 128,
                    blockWhenFull: true)
                .WriteTo.Async(
                    configure: sink => sink.File(
                        path: Path.Combine(outputDirectory, "flush-.log"),
                        rollingInterval: RollingInterval.Day,
                        retainedFileCountLimit: 1,
                        fileSizeLimitBytes: 64 * 1024 * 1024,
                        rollOnFileSizeLimit: true),
                    bufferSize: 128,
                    blockWhenFull: true)
                .CreateLogger();

            /// <summary>
            /// Supplies total events for the fixture or scenario under test.
            /// </summary>
            const int TotalEvents = 5_000;

            _ = Parallel.For(
                fromInclusive: 0,
                toExclusive: TotalEvents,
                body: i => logger.Information("Flush event {Sequence}", i));

            if (logger is IDisposable disposable)
            {
                disposable.Dispose();
            }

            Assert.Equal(TotalEvents, countingSink.Count);

            TryDeleteDirectory(outputDirectory);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Exercises create temp output directory behavior, including the expected result and failure semantics.
        /// </summary>
        private static string CreateTempOutputDirectory()
        {
            string directory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", "LoggingAsyncSink", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(directory);
            return directory;
        }

        /// <summary>
        /// Exercises try delete directory behavior, including the expected result and failure semantics.
        /// </summary>
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
                // best-effort cleanup
            }
        }

        /// <summary>
        /// Covers counting sink behavior and invariants exercised by this test suite.
        /// </summary>
        private sealed class CountingSink : Serilog.Core.ILogEventSink
        {
            /// <summary>
            /// Supplies  count for the fixture or scenario under test.
            /// </summary>
            private long _count;

            /// <summary>
            /// Exercises count behavior, including the expected result and failure semantics.
            /// </summary>
            public long Count => Interlocked.Read(ref _count);

            /// <summary>
            /// Exercises emit behavior, including the expected result and failure semantics.
            /// </summary>
            public void Emit(LogEvent logEvent)
            {
                _ = logEvent;
                _ = Interlocked.Increment(ref _count);
            }
        }
    }
}
