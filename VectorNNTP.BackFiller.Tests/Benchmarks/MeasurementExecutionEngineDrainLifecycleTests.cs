// <copyright file="MeasurementExecutionEngineDrainLifecycleTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for measurement execution engine drain lifecycle, covering service lifecycle and shutdown contracts; benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the measurement execution engine drain lifecycle test suite.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates benchmark drain lifecycle behavior when publisher submission progress stalls.
    /// </summary>
    public sealed class MeasurementExecutionEngineDrainLifecycleTests
    {
        /// <summary>
        /// Ensures drain preemption releases dispatchers and completes without hanging when first publish lane stalls.
        /// </summary>
        [Fact]
        public async Task DrainAndShutdownAsync_WhenFirstPublishLaneStalls_PreemptsPublisherAndCompletesDispatcherDrain()
        {
            byte[] payload = [(byte)'D', (byte)'\n'];
            string[] messageIds = [.. Enumerable.Range(1, 64).Select(static i => $"<drain-{i}@example.com>")];

            TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseServer = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using DrainLifecycleFakeServer server = await DrainLifecycleFakeServer.StartAsync(async (stream, cancellationToken) =>
            {
                await DrainLifecycleFakeServer.WriteLineAsync(stream, "200 transit ready");
                await DrainLifecycleFakeServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await DrainLifecycleFakeServer.WriteLineAsync(stream, "101 Capability list:");
                await DrainLifecycleFakeServer.WriteLineAsync(stream, "STREAMING");
                await DrainLifecycleFakeServer.WriteLineAsync(stream, ".");
                await DrainLifecycleFakeServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await DrainLifecycleFakeServer.WriteLineAsync(stream, "203 Streaming permitted");

                string firstTakethis = await DrainLifecycleFakeServer.ReadLineAsync(stream, cancellationToken);
                Assert.StartsWith("TAKETHIS ", firstTakethis, StringComparison.Ordinal);
                _ = await DrainLifecycleFakeServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                _ = firstTakethisObserved.TrySetResult();

                await releaseServer.Task.WaitAsync(cancellationToken);

                while (!cancellationToken.IsCancellationRequested)
                {
                    string command = await DrainLifecycleFakeServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(command, "QUIT", StringComparison.Ordinal))
                    {
                        await DrainLifecycleFakeServer.WriteLineAsync(stream, "205 closing connection");
                        return;
                    }

                    if (command.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        _ = await DrainLifecycleFakeServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    }
                }
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            using BoundedArticleQueue queue = new(maxArticles: 256, maxResidentBytes: 64L * 1024L * 1024L);
            MeasurementMetrics metrics = new(articleBytes: payload.Length);
            RuntimeMetrics runtime = new();
            PreparedBenchmarkWorkload workload = new(messageIds, payload, new WorkloadPreparationSummary(0, 0, messageIds.Length, messageIds.Length, 0, payload.Length));

            foreach (string messageId in messageIds)
            {
                bool admitted = await queue.TryWriteAsync(new QueuedArticle(messageId, payload.Length), CancellationToken.None);
                Assert.True(admitted);
            }

            Task[] dispatchers = [.. Enumerable.Range(0, 128)
                .Select(_ => Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(
                    queue,
                    publisher,
                    metrics,
                    workload,
                    CancellationToken.None,
                    enableForensicDiagnostics: true)))];

            using CancellationTokenSource firstObserveTimeout = new(TimeSpan.FromSeconds(10));
            await firstTakethisObserved.Task.WaitAsync(firstObserveTimeout.Token);

            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementArticleCount: messageIds.Length) with
            {
                ConnectionPoolSize = 1,
                PerConnectionPipelineDepth = 1,
                DispatchWorkerCount = 128,
                GeneratorWorkerCount = 1,
                MaxQueuedArticles = 256,
                MaxResidentBytes = 64L * 1024L * 1024L,
                ArticleTargetBytes = payload.Length,
                ProducerQueueTargetArticles = 128,
            };

            using CancellationTokenSource producerStopCts = new();
            Task[] producerTasks = [];
            Task telemetryTask = Task.CompletedTask;

            using CancellationTokenSource drainTimeout = new(TimeSpan.FromSeconds(20));
            BenchmarkResult result = await MeasurementExecutionEngine.DrainAndShutdownAsync(
                queue,
                metrics,
                runtime,
                Process.GetCurrentProcess(),
                workload,
                publisher,
                config,
                producerTasks,
                telemetryTask,
                dispatchers,
                producerStopCts,
                DateTimeOffset.UtcNow,
                allocatedStartBytes: GC.GetTotalAllocatedBytes(false),
                enableForensicDiagnostics: true,
                (drainConfig, snapshot, drainMetrics, drainRuntime, drainProcess, workloadPreparation, startUtc, endUtc, drainTime, outstandingAtEnd, drainedAfterEnd, allocatedAtStart, forensicEnabled, fixedCountBoundaryTelemetry) =>
                    BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                        drainConfig,
                        snapshot,
                        drainMetrics,
                        drainRuntime,
                        workloadPreparation,
                        startUtc,
                        endUtc,
                        drainTime,
                        outstandingAtEnd,
                        drainedAfterEnd,
                        allocatedAtStart,
                        forensicEnabled,
                        fixedCountBoundaryTelemetry)).WaitAsync(drainTimeout.Token);

            Assert.True(dispatchers.All(static task => task.IsCompleted), "All dispatcher tasks should complete after publisher preemption.");
            Assert.Equal(metrics.GetAdmittedCount(), metrics.GetCompletedCount());

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot afterDrainDiagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(0, afterDrainDiagnostics.QueuedSubmissionCount);
            Assert.True(result.OutstandingAtMeasurementEnd >= 1);

            _ = releaseServer.TrySetResult();
        }

        /// <summary>
        /// Creates a TransitPublisher configured for loopback fake-server lifecycle tests.
        /// </summary>
        private static TransitPublisher CreatePublisher(int port, int connectionPoolSize, int perConnectionPipelineDepth)
        {
            BackFillerRuntimeOptions options = new(
                CanonicalBackFillerFqdn: "bf.example.com",
                BackFillerId: 42,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: "C:\\logs",
                ValidatedCertificateDirectory: "C:\\certs",
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: IPAddress.Loopback.ToString(),
                TransitServerPort: port,
                TransitServerUseSsl: false,
                ShutdownGracePeriodSeconds: 60,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 120,
                WriteBatchCoalesceMicroseconds: 250);

            return new TransitPublisher(options, TimeProvider.System, NullLogger<TransitPublisher>.Instance, connectionPoolSize, perConnectionPipelineDepth);
        }

        /// <summary>
        /// Minimal fake transit server for benchmark drain lifecycle tests.
        /// </summary>
        private sealed class DrainLifecycleFakeServer : IAsyncDisposable
        {
            /// <summary>
            /// Supplies  listener for the fixture or scenario under test.
            /// </summary>
            private readonly TcpListener _listener;
            /// <summary>
            /// Supplies  session for the fixture or scenario under test.
            /// </summary>
            private readonly Func<NetworkStream, CancellationToken, Task> _session;
            /// <summary>
            /// Exercises  cts behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly CancellationTokenSource _cts = new();
            /// <summary>
            /// Supplies  accept loop for the fixture or scenario under test.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
        /// Verifies the drain lifecycle fake server behavior and expected contract.
            /// </summary>
            private DrainLifecycleFakeServer(TcpListener listener, Func<NetworkStream, CancellationToken, Task> session)
            {
                _listener = listener;
                _session = session;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Starts the fake server on an ephemeral loopback port.
            /// </summary>
            internal static Task<DrainLifecycleFakeServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                return Task.FromResult(new DrainLifecycleFakeServer(listener, session));
            }

            /// <summary>
            /// Gets the bound TCP port.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Reads one CRLF-delimited ASCII line.
            /// </summary>
            internal static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> buffer = [];
                while (true)
                {
                    byte current = await ReadByteAsync(stream, cancellationToken);
                    if (current == (byte)'\n')
                    {
                        break;
                    }

                    buffer.Add(current);
                }

                if (buffer.Count > 0 && buffer[^1] == (byte)'\r')
                {
                    buffer.RemoveAt(buffer.Count - 1);
                }

                return Encoding.ASCII.GetString([.. buffer]);
            }

            /// <summary>
            /// Reads one dot-terminated TAKETHIS payload body.
            /// </summary>
            internal static async Task<byte[]> ReadTakethisPayloadAsync(Stream stream, CancellationToken cancellationToken)
            {
                using MemoryStream payload = new();
                bool atLineStart = true;

                while (true)
                {
                    byte current = await ReadByteAsync(stream, cancellationToken);

                    if (atLineStart && current == (byte)'.')
                    {
                        byte next = await ReadByteAsync(stream, cancellationToken);
                        if (next == (byte)'\r')
                        {
                            byte nextNext = await ReadByteAsync(stream, cancellationToken);
                            if (nextNext == (byte)'\n')
                            {
                                break;
                            }

                            await payload.WriteAsync(new byte[] { current, next, nextNext }, cancellationToken);
                            atLineStart = false;
                            continue;
                        }

                        await payload.WriteAsync(new byte[] { next }, cancellationToken);
                        atLineStart = next == (byte)'\n';
                        continue;
                    }

                    await payload.WriteAsync(new byte[] { current }, cancellationToken);

                    if (current == (byte)'\n')
                    {
                        atLineStart = true;
                    }
                    else if (current != (byte)'\r')
                    {
                        atLineStart = false;
                    }
                }

                return payload.ToArray();
            }

            /// <summary>
            /// Verifies that the next command line matches the expected value.
            /// </summary>
            internal static async Task ExpectCommandAsync(Stream stream, string expected, CancellationToken cancellationToken)
            {
                string line = await ReadLineAsync(stream, cancellationToken);
                Assert.Equal(expected, line);
            }

            /// <summary>
            /// Writes one CRLF-delimited ASCII line.
            /// </summary>
            internal static Task WriteLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                return stream.WriteAsync(bytes).AsTask();
            }

            /// <inheritdoc />
            public async ValueTask DisposeAsync()
            {
                _cts.Cancel();
                _listener.Stop();

                try
                {
                    await _acceptLoop.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                _cts.Dispose();
            }

            /// <summary>
            /// Accepts one or more sessions until cancellation.
            /// </summary>
            private async Task AcceptLoopAsync()
            {
                while (!_cts.IsCancellationRequested)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                    _ = Task.Run(async () =>
                    {
                        await using NetworkStream stream = client.GetStream();
                        await _session(stream, _cts.Token).ConfigureAwait(false);
                    }, CancellationToken.None);
                }
            }

            /// <summary>
            /// Reads exactly one byte from the stream.
            /// </summary>
            private static async ValueTask<byte> ReadByteAsync(Stream stream, CancellationToken cancellationToken)
            {
                byte[] single = new byte[1];
                int read = await stream.ReadAsync(single, cancellationToken).ConfigureAwait(false);
                return read == 0 ? throw new InvalidOperationException("Unexpected EOF while reading stream data.") : single[0];
            }
        }
    }
}


