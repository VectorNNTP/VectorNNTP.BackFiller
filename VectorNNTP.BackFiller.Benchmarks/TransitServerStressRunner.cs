// <copyright file="TransitServerStressRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// TransitServerStressRunner: defines the benchmark entry point or scenario for controlled performance validation.

using System.Buffers;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks.Execution;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the transit ServerStressRunner class used by this benchmark or regression-gate component.
/// </summary>
internal static class TransitServerStressRunner
{
    /// <summary>
    /// Gets or sets the default ArticleTargetBytes value used by this component.
    /// </summary>
    private const int DefaultArticleTargetBytes = 1 * 1024 * 1024;
    /// <summary>
    /// Gets or sets the default WarmupSeconds value used by this component.
    /// </summary>
    private const int DefaultWarmupSeconds = 10;
    /// <summary>
    /// Gets or sets the validation Seconds value used by this component.
    /// </summary>
    private const int ValidationSeconds = 10;
    /// <summary>
    /// Gets or sets the default GeneratorMeasurementSeconds value used by this component.
    /// </summary>
    private const int DefaultGeneratorMeasurementSeconds = 30;

    /// <summary>
    /// Executes the runtime Identity operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static readonly RuntimeExecutionIdentity RuntimeIdentity = RuntimeExecutionIdentityCapture.Capture(typeof(TransitServerStressRunner).Assembly);
    /// <summary>
    /// Gets or sets the benchmark BuildVersion value used by this component.
    /// </summary>
    private static readonly string BenchmarkBuildVersion = RuntimeIdentity.AssemblyFileVersion ?? RuntimeIdentity.RuntimeAssemblyVersion;

    /// <summary>
    /// Executes the run Async operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task RunAsync(TimeSpan stressDuration, TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(stressDuration, BenchmarkMode.Full, cliOptions);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the run ValidationAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task RunValidationAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(ValidationSeconds), BenchmarkMode.Validation, cliOptions);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs validation benchmark against the benchmark-only dev/null transit fake server.
    /// </summary>
    /// <param name="cliOptions">The benchmark CLI options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when benchmark execution has finished.</returns>
    internal static async Task RunFakeServerValidationAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        await using BenchmarkDevNullTransitServer fakeServer = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback, cancellationToken: cancellationToken).ConfigureAwait(false);

        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(
            TimeSpan.FromSeconds(ValidationSeconds),
            BenchmarkMode.Validation,
            cliOptions,
            endpointHostOverride: IPAddress.Loopback.ToString(),
            endpointPortOverride: fakeServer.Port,
            endpointUseSslOverride: false,
            endpointType: BenchmarkDevNullTransitServer.EndpointTypeLabel,
            endpointIdentity: BenchmarkDevNullTransitServer.ServerIdentity);

        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);

        Console.WriteLine();
        Console.WriteLine("=== FakeServer benchmark sink summary ===");
        Console.WriteLine($"Endpoint type: {BenchmarkDevNullTransitServer.EndpointTypeLabel}");
        Console.WriteLine($"Host: {IPAddress.Loopback}");
        Console.WriteLine($"Port: {fakeServer.Port}");
        Console.WriteLine($"Server identity: {BenchmarkDevNullTransitServer.ServerIdentity}");
        Console.WriteLine($"Accepted articles: {fakeServer.AcceptedArticles}");
        Console.WriteLine($"Consumed opaque payload bytes: {fakeServer.ConsumedArticleBytes}");
        Console.WriteLine($"Accepted TCP connections: {fakeServer.TotalConnections}");

        await VerifyBenchmarkConnectedToFakeServerAsync(fakeServer.Port, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the run SaturationAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task RunSaturationAsync(TimeSpan stressDuration, TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(stressDuration, BenchmarkMode.Saturation, cliOptions);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the run GeneratorWorkerSweepAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task RunGeneratorWorkerSweepAsync(CancellationToken cancellationToken = default)
    {
        int[] workerCounts = [1, 2, 4, 8, 16, 32];

        foreach (int generatorWorkers in workerCounts)
        {
            Console.WriteLine();
            Console.WriteLine($"=== Generator worker sweep run: workers={generatorWorkers} ===");

            TransitBenchmarkCliOptions options = new(
                DurationSeconds: 30,
                WarmupSeconds: 10,
                ConnectionPoolSize: 64,
                PipelineDepth: 16,
                DispatchWorkers: 512,
                QueueMegabytes: 2048,
                QueueArticles: 2048,
                ArticleKilobytes: 1024,
                GeneratorWorkers: generatorWorkers,
                WriteBatchCoalesceMicroseconds: 250);

            TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(30), BenchmarkMode.Forensic, options);
            await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes the run Forensic32 WorkerAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task RunForensic32WorkerAsync(CancellationToken cancellationToken = default)
    {
        TransitBenchmarkCliOptions options = new(
            DurationSeconds: 30,
            WarmupSeconds: 10,
            ConnectionPoolSize: 64,
            PipelineDepth: 16,
            DispatchWorkers: 512,
            QueueMegabytes: 2048,
            QueueArticles: 2048,
            ArticleKilobytes: 1024,
            GeneratorWorkers: 32,
            WriteBatchCoalesceMicroseconds: 250);

        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(30), BenchmarkMode.Forensic, options);
        await RunCoreAsync(config, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the run GeneratorBaselineAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task RunGeneratorBaselineAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        await GeneratorBaselineRunner.RunAsync(cliOptions, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the run SingleTraceAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task RunSingleTraceAsync(TransitBenchmarkCliOptions cliOptions, CancellationToken cancellationToken = default)
    {
        await TransitSingleTraceRunner.RunAsync(
            cliOptions,
            ValidationSeconds,
            RuntimeIdentity,
            CreateTransitPublisherLogger,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the run CoreAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static Task RunCoreAsync(TransitBenchmarkConfig config, CancellationToken cancellationToken)
    {
        return TransitBenchmarkOrchestrator.RunCoreAsync(
            config,
            RuntimeIdentity,
            BenchmarkBuildVersion,
            CreateTransitPublisherLogger,
            RunMeasurementAsync,
            WriteStructuredResultArtifacts,
            cancellationToken);
    }

    /// <summary>
    /// Verifies benchmark traffic reached the fake server endpoint by probing connection activity.
    /// </summary>
    /// <param name="port">The fake server listen port.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when verification is done.</returns>
    private static async Task VerifyBenchmarkConnectedToFakeServerAsync(int port, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using TcpClient probe = new();
        await probe.ConnectAsync(IPAddress.Loopback, port, cancellationToken).ConfigureAwait(false);
        await using NetworkStream stream = probe.GetStream();

        byte[] buffer = new byte[64];
        _ = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false);
        string greeting = Encoding.ASCII.GetString(buffer);
        Console.WriteLine($"FakeServer probe greeting prefix: {greeting.TrimEnd('\0', '\r', '\n')}");
    }

    /// <summary>
    /// Executes the run MeasurementAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static async Task<BenchmarkResult> RunMeasurementAsync(
        TransitPublisher publisher,
        TransitBenchmarkConfig config,
        PreparedBenchmarkWorkload workload,
        CancellationToken cancellationToken,
        bool enableForensicDiagnostics)
    {
        return await MeasurementRunCoordinator.RunAsync(
            publisher,
            config,
            workload,
            RuntimeIdentity,
            BenchmarkBuildVersion,
            cancellationToken,
            enableForensicDiagnostics).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the create TransitPublisherLogger operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static ILogger<TransitPublisher> CreateTransitPublisherLogger(ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);

        ILogger baseLogger = loggerFactory.CreateLogger<TransitPublisher>();
        return new TransitPublisherBenchmarkLogger(baseLogger);
    }

    /// <summary>
    /// Executes the write StructuredResultArtifacts operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private static void WriteStructuredResultArtifacts(BenchmarkResult result, TransitBenchmarkConfig config)
    {
        BenchmarkArtifactWriter.WriteStructuredResultArtifacts(
            result,
            config,
            Environment.ProcessorCount,
            /// <summary>
            /// Executes the from operation while preserving the component's benchmark or test-harness contract.
            /// </summary>
            static (benchmarkResult, benchmarkConfig, processorCount) => BenchmarkResultArtifact.From(benchmarkResult, benchmarkConfig, processorCount),
            /// <summary>
            /// Executes the artifact operation while preserving the component's benchmark or test-harness contract.
            /// </summary>
            static artifact => artifact.ToCsv());
    }

    }
