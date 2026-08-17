using Serilog;
using Serilog.Events;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

public sealed class LoggingAsyncSinkCorrectnessTests
{
    [Fact]
    public Task AsyncSink_BlockWhenFull_EmitsAllSubmittedEvents()
    {
        string outputDirectory = CreateTempOutputDirectory();
        CountingSink countingSink = new();

        Serilog.ILogger logger = new LoggerConfiguration()
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

        const int totalEvents = 10_000;

        for (int i = 0; i < totalEvents; i++)
        {
            logger.Information("Correctness event {Sequence}", i);
        }

        if (logger is IDisposable disposable)
        {
            disposable.Dispose();
        }

        Assert.Equal(totalEvents, countingSink.Count);

        TryDeleteDirectory(outputDirectory);
        return Task.CompletedTask;
    }

    [Fact]
    public Task AsyncSink_CloseAndFlushAsync_DrainsQueuedEvents()
    {
        string outputDirectory = CreateTempOutputDirectory();
        CountingSink countingSink = new();

        Serilog.ILogger logger = new LoggerConfiguration()
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

        const int totalEvents = 5_000;

        Parallel.For(
            fromInclusive: 0,
            toExclusive: totalEvents,
            body: i => logger.Information("Flush event {Sequence}", i));

        if (logger is IDisposable disposable)
        {
            disposable.Dispose();
        }

        Assert.Equal(totalEvents, countingSink.Count);

        TryDeleteDirectory(outputDirectory);
        return Task.CompletedTask;
    }

    private static string CreateTempOutputDirectory()
    {
        string directory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", "LoggingAsyncSink", Guid.NewGuid().ToString("N"));
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
            // best-effort cleanup
        }
    }

    private sealed class CountingSink : Serilog.Core.ILogEventSink
    {
        private long _count;

        public long Count => Interlocked.Read(ref _count);

        public void Emit(LogEvent logEvent)
        {
            _ = logEvent;
            _ = Interlocked.Increment(ref _count);
        }
    }
}
