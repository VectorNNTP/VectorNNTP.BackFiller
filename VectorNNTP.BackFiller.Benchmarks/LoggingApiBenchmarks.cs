using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using Serilog.Extensions.Logging;

namespace VectorNNTP.BackFiller.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 10)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public partial class LoggingApiBenchmarks
{
    private static readonly DemoState DemoStateValue = DemoState.Ready;
    private static readonly TimeSpan DemoTimeSpan = TimeSpan.FromMilliseconds(1234);
    private const int DemoInt = 42;
    private const long DemoLong = 123456789L;
    private const double DemoDouble = 123.456;
    private const string DemoString = "benchmark-payload";

    private Exception _precreatedException = null!;

    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugEnabled = null!;
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugDisabled = null!;
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedInfoEnabled = null!;

    private Serilog.ILogger _serilogDebugEnabled = null!;
    private Serilog.ILogger _serilogDebugDisabled = null!;
    private Serilog.ILogger _serilogInfoEnabled = null!;

    [GlobalSetup]
    public void Setup()
    {
        _precreatedException = new InvalidOperationException("Precreated benchmark exception");

        _sourceGeneratedDebugEnabled = CreateMelLogger(LogLevel.Debug);
        _sourceGeneratedDebugDisabled = CreateMelLogger(LogLevel.Information);
        _sourceGeneratedInfoEnabled = CreateMelLogger(LogLevel.Information);

        _serilogDebugEnabled = CreateSerilogLogger(LogEventLevel.Debug);
        _serilogDebugDisabled = CreateSerilogLogger(LogEventLevel.Information);
        _serilogInfoEnabled = CreateSerilogLogger(LogEventLevel.Information);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        DisposeLogger(_sourceGeneratedDebugEnabled);
        DisposeLogger(_sourceGeneratedDebugDisabled);
        DisposeLogger(_sourceGeneratedInfoEnabled);

        (_serilogDebugEnabled as IDisposable)?.Dispose();
        (_serilogDebugDisabled as IDisposable)?.Dispose();
        (_serilogInfoEnabled as IDisposable)?.Dispose();
    }

    [BenchmarkCategory("SourceGenerated", "Disabled")]
    [Benchmark(Description = "Debug LoggerMessage disabled")]
    public void SourceGenerated_Debug_Disabled()
    {
        BenchmarkLoggingMessages.LogDemoDebug(
            _sourceGeneratedDebugDisabled,
            DemoStateValue,
            DemoInt,
            DemoLong,
            DemoDouble,
            DemoTimeSpan,
            DemoString);
    }

    [BenchmarkCategory("SourceGenerated", "Enabled")]
    [Benchmark(Description = "Debug LoggerMessage enabled")]
    public void SourceGenerated_Debug_Enabled()
    {
        BenchmarkLoggingMessages.LogDemoDebug(
            _sourceGeneratedDebugEnabled,
            DemoStateValue,
            DemoInt,
            DemoLong,
            DemoDouble,
            DemoTimeSpan,
            DemoString);
    }

    [BenchmarkCategory("SourceGenerated", "Enabled")]
    [Benchmark(Description = "Info LoggerMessage enabled")]
    public void SourceGenerated_Info_Enabled()
    {
        BenchmarkLoggingMessages.LogDemoInformation(
            _sourceGeneratedInfoEnabled,
            DemoStateValue,
            DemoInt,
            DemoLong,
            DemoDouble,
            DemoTimeSpan,
            DemoString);
    }

    [BenchmarkCategory("SerilogStatic", "Disabled")]
    [Benchmark(Description = "Debug Serilog template disabled")]
    public void SerilogStatic_Debug_Disabled()
    {
        _serilogDebugDisabled.Debug(
            "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}",
            DemoStateValue,
            DemoInt,
            DemoLong,
            DemoDouble,
            DemoTimeSpan,
            DemoString);
    }

    [BenchmarkCategory("SerilogStatic", "Enabled")]
    [Benchmark(Description = "Debug Serilog template enabled")]
    public void SerilogStatic_Debug_Enabled()
    {
        _serilogDebugEnabled.Debug(
            "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}",
            DemoStateValue,
            DemoInt,
            DemoLong,
            DemoDouble,
            DemoTimeSpan,
            DemoString);
    }

    [BenchmarkCategory("SerilogStatic", "Enabled")]
    [Benchmark(Description = "Info Serilog template enabled")]
    public void SerilogStatic_Info_Enabled()
    {
        _serilogInfoEnabled.Information(
            "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}",
            DemoStateValue,
            DemoInt,
            DemoLong,
            DemoDouble,
            DemoTimeSpan,
            DemoString);
    }

    [BenchmarkCategory("SourceGenerated", "Exception", "Disabled")]
    [Benchmark(Description = "LoggerMessage exception disabled")]
    public void SourceGenerated_Exception_Disabled()
    {
        BenchmarkLoggingMessages.LogDemoException(
            _sourceGeneratedDebugDisabled,
            _precreatedException,
            "disabled-path");
    }

    [BenchmarkCategory("SourceGenerated", "Exception", "Enabled")]
    [Benchmark(Description = "LoggerMessage exception enabled")]
    public void SourceGenerated_Exception_Enabled()
    {
        BenchmarkLoggingMessages.LogDemoException(
            _sourceGeneratedDebugEnabled,
            _precreatedException,
            "enabled-path");
    }

    [BenchmarkCategory("SerilogStatic", "Exception", "Disabled")]
    [Benchmark(Description = "Serilog exception disabled")]
    public void Serilog_Exception_Disabled()
    {
        _serilogDebugDisabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "disabled-path");
    }

    [BenchmarkCategory("SerilogStatic", "Exception", "Enabled")]
    [Benchmark(Description = "Serilog exception enabled")]
    public void Serilog_Exception_Enabled()
    {
        _serilogDebugEnabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "enabled-path");
    }

    [BenchmarkCategory("Exception", "Construction")]
    [Benchmark(Description = "Exception construction only")]
    public Exception Exception_Construction_Only()
    {
        return new InvalidOperationException("Constructed in benchmark iteration");
    }

    private static Microsoft.Extensions.Logging.ILogger CreateMelLogger(LogLevel minimumLevel)
    {
        Serilog.ILogger serilogLogger = new LoggerConfiguration()
            .MinimumLevel.Is(ToSerilogLevel(minimumLevel))
            .WriteTo.Sink(new NullSerilogSink())
            .CreateLogger();

        ILoggerFactory factory = LoggerFactory.Create(builder =>
        {
            _ = builder.ClearProviders();
            _ = builder.SetMinimumLevel(minimumLevel);
            _ = builder.AddSerilog(serilogLogger, dispose: true);
        });

        return factory.CreateLogger(typeof(LoggingApiBenchmarks).FullName!);
    }

    private static Serilog.ILogger CreateSerilogLogger(LogEventLevel minimumLevel)
    {
        return new LoggerConfiguration()
            .MinimumLevel.Is(minimumLevel)
            .WriteTo.Sink(new NullSerilogSink())
            .CreateLogger();
    }

    private static void DisposeLogger(Microsoft.Extensions.Logging.ILogger logger)
    {
        if (logger is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    private static LogEventLevel ToSerilogLevel(LogLevel level)
    {
        return level switch
        {
            LogLevel.Trace => LogEventLevel.Verbose,
            LogLevel.Debug => LogEventLevel.Debug,
            LogLevel.Information => LogEventLevel.Information,
            LogLevel.Warning => LogEventLevel.Warning,
            LogLevel.Error => LogEventLevel.Error,
            LogLevel.Critical => LogEventLevel.Fatal,
            _ => LogEventLevel.Information,
        };
    }

    private enum DemoState
    {
        Starting,
        Ready,
        Draining,
    }

    private sealed class NullSerilogSink : Serilog.Core.ILogEventSink
    {
        public void Emit(LogEvent logEvent)
        {
            _ = logEvent;
        }
    }

    private static partial class BenchmarkLoggingMessages
    {
        [LoggerMessage(
            EventId = 2000,
            Level = LogLevel.Debug,
            Message = "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}")]
        public static partial void LogDemoDebug(
            Microsoft.Extensions.Logging.ILogger logger,
            DemoState state,
            int intValue,
            long longValue,
            double doubleValue,
            TimeSpan elapsed,
            string payload);

        [LoggerMessage(
            EventId = 2001,
            Level = LogLevel.Information,
            Message = "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}")]
        public static partial void LogDemoInformation(
            Microsoft.Extensions.Logging.ILogger logger,
            DemoState state,
            int intValue,
            long longValue,
            double doubleValue,
            TimeSpan elapsed,
            string payload);

        [LoggerMessage(
            EventId = 2002,
            Level = LogLevel.Debug,
            Message = "Exception in benchmark path {Path}")]
        public static partial void LogDemoException(
            Microsoft.Extensions.Logging.ILogger logger,
            Exception exception,
            string path);
    }
}
