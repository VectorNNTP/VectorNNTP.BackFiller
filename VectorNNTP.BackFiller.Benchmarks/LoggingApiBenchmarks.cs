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

/// <summary>
/// Compares source-generated and Serilog logging paths with enabled, disabled, and exception scenarios.
/// </summary>
/// <remarks>Benchmark attributes are intentionally kept on the type to compare equivalent call sites.</remarks>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 10)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public partial class LoggingApiBenchmarks
{
    /// <summary>
    /// Gets or sets the demo StateValue.
    /// </summary>
    private static readonly DemoState DemoStateValue = DemoState.Ready;
    /// <summary>
    /// Implements the demo TimeSpan contract.
    /// </summary>
    private static readonly TimeSpan DemoTimeSpan = TimeSpan.FromMilliseconds(1234);
    /// <summary>
    /// Gets or sets the demo Int.
    /// </summary>
    private const int DemoInt = 42;
    /// <summary>
    /// Gets or sets the demo Long.
    /// </summary>
    private const long DemoLong = 123456789L;
    /// <summary>
    /// Gets or sets the demo Double.
    /// </summary>
    private const double DemoDouble = 123.456;
    /// <summary>
    /// Gets or sets the demo String.
    /// </summary>
    private const string DemoString = "benchmark-payload";

    /// <summary>
    /// Gets or sets the _precreatedException.
    /// </summary>
    private Exception _precreatedException = null!;

    /// <summary>
    /// Gets or sets the _sourceGeneratedDebugEnabled.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugEnabled = null!;
    /// <summary>
    /// Gets or sets the _sourceGeneratedDebugDisabled.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugDisabled = null!;
    /// <summary>
    /// Gets or sets the _sourceGeneratedInfoEnabled.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedInfoEnabled = null!;

    /// <summary>
    /// Gets or sets the _serilogDebugEnabled.
    /// </summary>
    private Serilog.ILogger _serilogDebugEnabled = null!;
    /// <summary>
    /// Gets or sets the _serilogDebugDisabled.
    /// </summary>
    private Serilog.ILogger _serilogDebugDisabled = null!;
    /// <summary>
    /// Gets or sets the _serilogInfoEnabled.
    /// </summary>
    private Serilog.ILogger _serilogInfoEnabled = null!;

    /// <summary>
    /// Creates the reusable logger and exception state used by each benchmark iteration.
    /// </summary>
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

    /// <summary>
    /// Disposes logger resources created by <see cref="Setup"/>.
    /// </summary>
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

    /// <summary>
    /// Measures a disabled source-generated debug log call.
    /// </summary>
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

    /// <summary>
    /// Measures an enabled source-generated debug log call.
    /// </summary>
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

    /// <summary>
    /// Measures an enabled source-generated information log call.
    /// </summary>
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

    /// <summary>
    /// Measures a disabled Serilog template-based debug call.
    /// </summary>
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

    /// <summary>
    /// Measures an enabled Serilog template-based debug call.
    /// </summary>
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

    /// <summary>
    /// Measures an enabled Serilog template-based information call.
    /// </summary>
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

    /// <summary>
    /// Measures a disabled source-generated exception log call.
    /// </summary>
    [BenchmarkCategory("SourceGenerated", "Exception", "Disabled")]
    [Benchmark(Description = "LoggerMessage exception disabled")]
    public void SourceGenerated_Exception_Disabled()
    {
        BenchmarkLoggingMessages.LogDemoException(
            _sourceGeneratedDebugDisabled,
            _precreatedException,
            "disabled-path");
    }

    /// <summary>
    /// Measures an enabled source-generated exception log call.
    /// </summary>
    [BenchmarkCategory("SourceGenerated", "Exception", "Enabled")]
    [Benchmark(Description = "LoggerMessage exception enabled")]
    public void SourceGenerated_Exception_Enabled()
    {
        BenchmarkLoggingMessages.LogDemoException(
            _sourceGeneratedDebugEnabled,
            _precreatedException,
            "enabled-path");
    }

    /// <summary>
    /// Measures a disabled Serilog exception log call.
    /// </summary>
    [BenchmarkCategory("SerilogStatic", "Exception", "Disabled")]
    [Benchmark(Description = "Serilog exception disabled")]
    public void Serilog_Exception_Disabled()
    {
        _serilogDebugDisabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "disabled-path");
    }

    /// <summary>
    /// Measures an enabled Serilog exception log call.
    /// </summary>
    [BenchmarkCategory("SerilogStatic", "Exception", "Enabled")]
    [Benchmark(Description = "Serilog exception enabled")]
    public void Serilog_Exception_Enabled()
    {
        _serilogDebugEnabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "enabled-path");
    }

    /// <summary>
    /// Measures exception construction without logging.
    /// <returns>A newly constructed benchmark exception.</returns>
    /// </summary>
    [BenchmarkCategory("Exception", "Construction")]
    [Benchmark(Description = "Exception construction only")]
    public Exception Exception_Construction_Only()
    {
        return new InvalidOperationException("Constructed in benchmark iteration");
    }

    /// <summary>
    /// Creates MelLogger.
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
    /// Creates SerilogLogger.
    /// </summary>
    private static Serilog.ILogger CreateSerilogLogger(LogEventLevel minimumLevel)
    {
        return new LoggerConfiguration()
            .MinimumLevel.Is(minimumLevel)
            .WriteTo.Sink(new NullSerilogSink())
            .CreateLogger();
    }

    /// <summary>
    /// Implements the dispose Logger contract.
    /// </summary>
    private static void DisposeLogger(Microsoft.Extensions.Logging.ILogger logger)
    {
        if (logger is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    /// <summary>
    /// Converts to SerilogLevel.
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
    /// Represents the demo State enum used by the benchmark or regression gate.
    /// </summary>
    private enum DemoState
    {
        Starting,
        Ready,
        Draining,
    }

    /// <summary>
    /// Represents the null SerilogSink class used by the benchmark or regression gate.
    /// </summary>
    private sealed class NullSerilogSink : Serilog.Core.ILogEventSink
    {
        /// <summary>
        /// Runs the emit benchmark scenario.
        /// </summary>
        public void Emit(LogEvent logEvent)
        {
            _ = logEvent;
        }
    }

    /// <summary>
    /// Represents the benchmark LoggingMessages class used by the benchmark or regression gate.
    /// </summary>
    private static partial class BenchmarkLoggingMessages
    {
        [LoggerMessage(
            EventId = 2000,
            Level = LogLevel.Debug,
            Message = "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}")]
        /// <summary>
        /// Emits the benchmark debug log template with representative payload and timing fields.
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
        /// Emits the benchmark information log template with representative payload and timing fields.
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
        /// Emits the benchmark exception-path debug log template.
        /// </summary>
        public static partial void LogDemoException(
            Microsoft.Extensions.Logging.ILogger logger,
            Exception exception,
            string path);
    }
}
