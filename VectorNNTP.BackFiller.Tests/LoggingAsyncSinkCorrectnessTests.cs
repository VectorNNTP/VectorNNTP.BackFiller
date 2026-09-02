// <copyright file="LoggingAsyncSinkCorrectnessTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for logging async sink correctness, covering configuration, runtime, and failure-handling contracts exercised by the tests.
// Primary responsibility: documents the executable contracts covered by the logging async sink correctness test suite.

using Serilog;
using Serilog.Events;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
        /// Verifies the logging async sink correctness tests scenario and its documented contract.
    /// </summary>
    public sealed class LoggingAsyncSinkCorrectnessTests
    {
        /// <summary>
        /// Verifies the async sink block when full emits all submitted events scenario and its documented contract.
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
        /// Verifies the async sink close and flush async drains queued events scenario and its documented contract.
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
        /// Verifies the create temp output directory scenario and its documented contract.
        /// </summary>
        /// <returns>The create temp output directory value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create temp output directory scenario and its documented contract.
        /// </summary>
        /// <returns>The create temp output directory value produced for the requested scenario.</returns>
        private static string CreateTempOutputDirectory()
        {
            string directory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", "LoggingAsyncSink", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(directory);
            return directory;
        }

        /// <summary>
        /// Verifies the try delete directory scenario and its documented contract.
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
        /// Verifies the counting sink scenario and its documented contract.
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
        /// Verifies the emit scenario and its documented contract.
            /// </summary>
            public void Emit(LogEvent logEvent)
            {
                _ = logEvent;
                _ = Interlocked.Increment(ref _count);
            }
        }
    }
}
