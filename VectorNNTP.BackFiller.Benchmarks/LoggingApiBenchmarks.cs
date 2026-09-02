// <copyright file="LoggingApiBenchmarks.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// LoggingApiBenchmarks: defines the benchmark entry point or scenario for controlled performance validation.

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
/// Represents the logging ApiBenchmarks class used by this benchmark or regression-gate component.
/// </summary>
public partial class LoggingApiBenchmarks
{
    /// <summary>
    /// Gets or sets the demo StateValue value used by this component.
    /// </summary>
    private static readonly DemoState DemoStateValue = DemoState.Ready;
    /// <summary>
    /// Executes the demo TimeSpan operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static readonly TimeSpan DemoTimeSpan = TimeSpan.FromMilliseconds(1234);
    /// <summary>
    /// Gets or sets the demo Int value used by this component.
    /// </summary>
    private const int DemoInt = 42;
    /// <summary>
    /// Gets or sets the demo Long value used by this component.
    /// </summary>
    private const long DemoLong = 123456789L;
    /// <summary>
    /// Gets or sets the demo Double value used by this component.
    /// </summary>
    private const double DemoDouble = 123.456;
    /// <summary>
    /// Gets or sets the demo String value used by this component.
    /// </summary>
    private const string DemoString = "benchmark-payload";

    /// <summary>
    /// Gets or sets the _precreatedException value used by this component.
    /// </summary>
    private Exception _precreatedException = null!;

    /// <summary>
    /// Gets or sets the _sourceGeneratedDebugEnabled value used by this component.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugEnabled = null!;
    /// <summary>
    /// Gets or sets the _sourceGeneratedDebugDisabled value used by this component.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedDebugDisabled = null!;
    /// <summary>
    /// Gets or sets the _sourceGeneratedInfoEnabled value used by this component.
    /// </summary>
    private Microsoft.Extensions.Logging.ILogger _sourceGeneratedInfoEnabled = null!;

    /// <summary>
    /// Gets or sets the _serilogDebugEnabled value used by this component.
    /// </summary>
    private Serilog.ILogger _serilogDebugEnabled = null!;
    /// <summary>
    /// Gets or sets the _serilogDebugDisabled value used by this component.
    /// </summary>
    private Serilog.ILogger _serilogDebugDisabled = null!;
    /// <summary>
    /// Gets or sets the _serilogInfoEnabled value used by this component.
    /// </summary>
    private Serilog.ILogger _serilogInfoEnabled = null!;

    [GlobalSetup]
    /// <summary>
    /// Executes the setup operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the cleanup operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the source Generated_Debug_Disabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the source Generated_Debug_Enabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the source Generated_Info_Enabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the serilog Static_Debug_Disabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the serilog Static_Debug_Enabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the serilog Static_Info_Enabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the source Generated_Exception_Disabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the source Generated_Exception_Enabled operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the serilog _Exception_Disabled operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public void Serilog_Exception_Disabled()
    {
        _serilogDebugDisabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "disabled-path");
    }

    [BenchmarkCategory("SerilogStatic", "Exception", "Enabled")]
    [Benchmark(Description = "Serilog exception enabled")]
    /// <summary>
    /// Executes the serilog _Exception_Enabled operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public void Serilog_Exception_Enabled()
    {
        _serilogDebugEnabled.Debug(_precreatedException, "Exception in benchmark path {Path}", "enabled-path");
    }

    [BenchmarkCategory("Exception", "Construction")]
    [Benchmark(Description = "Exception construction only")]
    /// <summary>
    /// Executes the exception _Construction_Only operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public Exception Exception_Construction_Only()
    {
        return new InvalidOperationException("Constructed in benchmark iteration");
    }

    /// <summary>
    /// Executes the create MelLogger operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the create SerilogLogger operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static Serilog.ILogger CreateSerilogLogger(LogEventLevel minimumLevel)
    {
        return new LoggerConfiguration()
            .MinimumLevel.Is(minimumLevel)
            .WriteTo.Sink(new NullSerilogSink())
            .CreateLogger();
    }

    /// <summary>
    /// Executes the dispose Logger operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static void DisposeLogger(Microsoft.Extensions.Logging.ILogger logger)
    {
        if (logger is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    /// <summary>
    /// Executes the to SerilogLevel operation while preserving the component's benchmark or test-harness contract.
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
    /// Represents the demo State enum used by this benchmark or regression-gate component.
    /// </summary>
    private enum DemoState
    {
        Starting,
        Ready,
        Draining,
    }

    /// <summary>
    /// Represents the null SerilogSink class used by this benchmark or regression-gate component.
    /// </summary>
    private sealed class NullSerilogSink : Serilog.Core.ILogEventSink
    {
        /// <summary>
        /// Executes the emit operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public void Emit(LogEvent logEvent)
        {
            _ = logEvent;
        }
    }

    /// <summary>
    /// Represents the benchmark LoggingMessages class used by this benchmark or regression-gate component.
    /// </summary>
    private static partial class BenchmarkLoggingMessages
    {
        [LoggerMessage(
            EventId = 2000,
            Level = LogLevel.Debug,
            Message = "Demo state={State}; int={IntValue}; long={LongValue}; double={DoubleValue}; elapsed={Elapsed}; payload={Payload}")]
        /// <summary>
        /// Executes the log DemoDebug operation while preserving the component's benchmark or test-harness contract.
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
        /// Executes the log DemoInformation operation while preserving the component's benchmark or test-harness contract.
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
        /// Executes the log DemoException operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public static partial void LogDemoException(
            Microsoft.Extensions.Logging.ILogger logger,
            Exception exception,
            string path);
    }
}
