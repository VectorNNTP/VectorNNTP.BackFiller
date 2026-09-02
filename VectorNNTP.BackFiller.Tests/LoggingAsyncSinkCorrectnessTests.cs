// <copyright file="LoggingAsyncSinkCorrectnessTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for logging async sink correctness.

using Serilog;
using Serilog.Events;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Documents the LoggingAsyncSinkCorrectnessTests test type and its protected contract.
    /// </summary>
    public sealed class LoggingAsyncSinkCorrectnessTests
    {
        /// <summary>
        /// Verifies the AsyncSink_BlockWhenFull_EmitsAllSubmittedEvents scenario and expected contract.
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
            /// Stores the TotalEvents fixture value used by these tests.
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
        /// Verifies the AsyncSink_CloseAndFlushAsync_DrainsQueuedEvents scenario and expected contract.
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
            /// Stores the TotalEvents fixture value used by these tests.
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
        /// Verifies the CreateTempOutputDirectory scenario and expected contract.
        /// </summary>
        private static string CreateTempOutputDirectory()
        {
            string directory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", "LoggingAsyncSink", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(directory);
            return directory;
        }

        /// <summary>
        /// Verifies the TryDeleteDirectory scenario and expected contract.
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
        /// Documents the CountingSink test type and its protected contract.
        /// </summary>
        private sealed class CountingSink : Serilog.Core.ILogEventSink
        {
            /// <summary>
            /// Stores the _count fixture value used by these tests.
            /// </summary>
            private long _count;

            /// <summary>
            /// Stores the Count value used by this test fixture.
            /// </summary>
            public long Count => Interlocked.Read(ref _count);

            /// <summary>
            /// Verifies the Emit scenario and expected contract.
            /// </summary>
            public void Emit(LogEvent logEvent)
            {
                _ = logEvent;
                _ = Interlocked.Increment(ref _count);
            }
        }
    }
}
