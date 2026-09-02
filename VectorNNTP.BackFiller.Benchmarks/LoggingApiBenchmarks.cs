// <copyright file="LoggingApiBenchmarks.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// LoggingApiBenchmarks: measures the allocation and throughput characteristics of logging API call patterns.

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
/// <summary>
/// Defines the logging ApiBenchmarks class for benchmark or isolated-regression execution.
/// </summary>
public partial class LoggingApiBenchmarks
{
    /// <summary>
    /// Gets or sets the demo StateValue value.
    /// </summary>
    private static readonly DemoState DemoStateValue = DemoState.Ready;
    /// <summary>
    /// Performs the demo TimeSpan operation.
    /// </summary>
    private static readonly TimeSpan DemoTimeSpan = TimeSpan.FromMilliseconds(1234);
    /// <summary>
    /// Gets or sets the demo Int value.
    /// </summary>
    private const int DemoInt = 42;
    /// <summary>
    /// Gets or sets the demo Long value.
    /// </summary>
    private const long DemoLong = 123456789L;
    /// <summary>
    /// Gets or sets the demo Double value.
    /// </summary>
    private const double DemoDouble = 123.456;
    /// <summary>
    /// Gets or sets the demo String value.
    /// </summary>
    private const string DemoString = "benchmark-payload";

    /// <summary>
    /// Gets or sets the _precreatedException value.
    /// </summary>
    private Exception _precreatedException = null!;

    /// <summary>
    /// Gets or sets the _sourceGeneratedDebugEnabled value.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugEnabled = null!;
    /// <summary>
    /// Gets or sets the _sourceGeneratedDebugDisabled value.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugDisabled = null!;
    /// <summary>
    /// Gets or sets the _sourceGeneratedInfoEnabled value.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedInfoEnabled = null!;

    /// <summary>
    /// Gets or sets the _serilogDebugEnabled value.
    /// </summary>
    private Serilog.ILogger _serilogDebugEnabled = null!;
    /// <summary>
    /// Gets or sets the _serilogDebugDisabled value.
    /// </summary>
    private Serilog.ILogger _serilogDebugDisabled = null!;
    /// <summary>
    /// Gets or sets the _serilogInfoEnabled value.
    /// </summary>
    private Serilog.ILogger _serilogInfoEnabled = null!;

    [GlobalSetup]
    /// <summary>
    /// Performs the setup operation.
    /// </summary>
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
    /// <summary>
    /// Performs the cleanup operation.
    /// </summary>
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
    /// <summary>
    /// Performs the source Generated_Debug_Disabled operation.
    /// </summary>
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
    /// <summary>
    /// Performs the source Generated_Debug_Enabled operation.
    /// </summary>
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
    /// <summary>
    /// Performs the source Generated_Info_Enabled operation.
    /// </summary>
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
    /// <summary>
    /// Performs the serilog Static_Debug_Disabled operation.
    /// </summary>
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
    /// <summary>
    /// Performs the serilog Static_Debug_Enabled operation.
    /// </summary>
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
    /// <summary>
    /// Performs the serilog Static_Info_Enabled operation.
    /// </summary>
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
    /// <summary>
    /// Performs the source Generated_Exception_Disabled operation.
    /// </summary>
    public void SourceGenerated_Exception_Disabled()
    {
        BenchmarkLoggingMessages.LogDemoException(
            _sourceGeneratedDebugDisabled,
            _precreatedException,
            "disabled-path");
    }

    [BenchmarkCategory("SourceGenerated", "Exception", "Enabled")]
    [Benchmark(Description = "LoggerMessage exception enabled")]
    /// <summary>
    /// Performs the source Generated_Exception_Enabled operation.
    /// </summary>
    public void SourceGenerated_Exception_Enabled()
    {
        BenchmarkLoggingMessages.LogDemoException(
            _sourceGeneratedDebugEnabled,
            _precreatedException,
            "enabled-path");
    }

    [BenchmarkCategory("SerilogStatic", "Exception", "Disabled")]
    [Benchmark(Description = "Serilog exception disabled")]
    /// <summary>
    /// Performs the serilog _Exception_Disabled operation.
    /// </summary>
    public void Serilog_Exception_Disabled()
    {
        _serilogDebugDisabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "disabled-path");
    }

    [BenchmarkCategory("SerilogStatic", "Exception", "Enabled")]
    [Benchmark(Description = "Serilog exception enabled")]
    /// <summary>
    /// Performs the serilog _Exception_Enabled operation.
    /// </summary>
    public void Serilog_Exception_Enabled()
    {
        _serilogDebugEnabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "enabled-path");
    }

    [BenchmarkCategory("Exception", "Construction")]
    [Benchmark(Description = "Exception construction only")]
    /// <summary>
    /// Performs the exception _Construction_Only operation.
    /// </summary>
    public Exception Exception_Construction_Only()
    {
        return new InvalidOperationException("Constructed in benchmark iteration");
    }

    /// <summary>
    /// Performs the create MelLogger operation.
    /// </summary>
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

    /// <summary>
    /// Performs the create SerilogLogger operation.
    /// </summary>
    private static Serilog.ILogger CreateSerilogLogger(LogEventLevel minimumLevel)
    {
        return new LoggerConfiguration()
            .MinimumLevel.Is(minimumLevel)
            .WriteTo.Sink(new NullSerilogSink())
            .CreateLogger();
    }

    /// <summary>
    /// Performs the dispose Logger operation.
    /// </summary>
    private static void DisposeLogger(Microsoft.Extensions.Logging.ILogger logger)
    {
        if (logger is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    /// <summary>
    /// Performs the to SerilogLevel operation.
    /// </summary>
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

    /// <summary>
    /// Defines the demo State enum for benchmark or isolated-regression execution.
    /// </summary>
    private enum DemoState
    {
        Starting,
        Ready,
        Draining,
    }

    /// <summary>
    /// Defines the null SerilogSink class for benchmark or isolated-regression execution.
    /// </summary>
    private sealed class NullSerilogSink : Serilog.Core.ILogEventSink
    {
        /// <summary>
        /// Performs the emit operation.
        /// </summary>
        public void Emit(LogEvent logEvent)
        {
            _ = logEvent;
        }
    }

    /// <summary>
    /// Defines the benchmark LoggingMessages class for benchmark or isolated-regression execution.
    /// </summary>
    private static partial class BenchmarkLoggingMessages
    {
        [LoggerMessage(
            EventId = 2000,
            Level = LogLevel.Debug,
            Message = "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}")]
        /// <summary>
        /// Performs the log DemoDebug operation.
        /// </summary>
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
        /// <summary>
        /// Performs the log DemoInformation operation.
        /// </summary>
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
        /// <summary>
        /// Performs the log DemoException operation.
        /// </summary>
        public static partial void LogDemoException(
            Microsoft.Extensions.Logging.ILogger logger,
            Exception exception,
            string path);
    }
}
