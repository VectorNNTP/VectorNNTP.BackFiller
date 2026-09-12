// <copyright file="MeasurementAccountingInvariantContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for measurement accounting invariants and mutation guards.

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
    /// Validates accounting invariants and mutation-sensitive benchmark throughput contracts.
    /// </summary>
    public sealed class MeasurementAccountingInvariantContractTests
    {
        [Fact]
        public async Task DispatchLoopAsync_WhenPublishThrowsAfterAdmission_AdmittedArticlesStillTerminalizeExactlyOnceAsync()
        {
            const int articleCount = 7;
            string[] messageIds = [.. Enumerable.Range(1, articleCount).Select(static i => $"<throw-{i}@benchmark.usenet.ninja>")];
            byte[] payload = [(byte)'X'];

            await using TransitPublisher publisher = CreatePublisher(port: 1190, connectionPoolSize: 1, perConnectionPipelineDepth: 1);

            using BoundedArticleQueue queue = new(maxArticles: 64, maxResidentBytes: 16L * 1024L * 1024L);
            MeasurementMetrics metrics = new(articleBytes: payload.Length);
            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());
            PreparedBenchmarkWorkload workload = new(messageIds, payload, new WorkloadPreparationSummary(0, 0, messageIds.Length, messageIds.Length, 0, payload.Length));

            foreach (string messageId in messageIds)
            {
                bool queued = await queue.TryWriteAsync(new QueuedArticle(messageId, payload.Length), CancellationToken.None);
                Assert.True(queued);
            }

            queue.StopAdmission();

            Dictionary<string, int> terminalCounts = new(StringComparer.Ordinal);
            Dictionary<string, TransitPublishStatus> terminalStatusByMessageId = new(StringComparer.Ordinal);
            object gate = new();

            void ObserveTerminal(TransitPublishResult result)
            {
                lock (gate)
                {
                    terminalCounts.TryGetValue(result.MessageId, out int count);
                    terminalCounts[result.MessageId] = count + 1;
                    terminalStatusByMessageId[result.MessageId] = result.Status;
                }
            }

            Task[] dispatchers =
            [
                Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, publisher, metrics, workload, null, CancellationToken.None, false, terminalObserver: ObserveTerminal)),
                Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, publisher, metrics, workload, null, CancellationToken.None, false, terminalObserver: ObserveTerminal))
            ];

            await Task.WhenAll(dispatchers);

            MeasurementSnapshot snapshot = metrics.Snapshot();

            Assert.Equal(articleCount, snapshot.AdmittedCount);
            Assert.Equal(articleCount, snapshot.CompletedCount);
            Assert.Equal(articleCount, snapshot.FailedCount);
            Assert.Equal(articleCount, snapshot.AmbiguousCount);
            Assert.Equal(0, snapshot.AcceptedCount);
            Assert.Equal(0, snapshot.RejectedCount);
            Assert.Equal(0, snapshot.UnavailableCount);
            Assert.Equal(0, snapshot.CanceledCount);
            Assert.True(snapshot.CompletedCount <= snapshot.AdmittedCount);

            Assert.Equal(articleCount, terminalCounts.Count);
            Assert.All(messageIds, id => Assert.True(terminalCounts.TryGetValue(id, out int count) && count == 1));
            Assert.All(messageIds, id => Assert.Equal(TransitPublishStatus.Failed, terminalStatusByMessageId[id]));
        }

        [Fact]
        public async Task DispatchLoopAsync_WhenPublishCancellationOccursAfterAdmission_ArticlesAreCountedAsCanceledExactlyOnceAsync()
        {
            const int articleCount = 5;
            string[] messageIds = [.. Enumerable.Range(1, articleCount).Select(static i => $"<cancel-{i}@benchmark.usenet.ninja>")];
            byte[] payload = Encoding.ASCII.GetBytes("Y\r\n");

            await using CancellationBlockingTransitServer server = await CancellationBlockingTransitServer.StartAsync();
            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            using BoundedArticleQueue queue = new(maxArticles: 64, maxResidentBytes: 16L * 1024L * 1024L);
            MeasurementMetrics metrics = new(articleBytes: payload.Length);
            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());
            PreparedBenchmarkWorkload workload = new(messageIds, payload, new WorkloadPreparationSummary(0, 0, messageIds.Length, messageIds.Length, 0, payload.Length));

            foreach (string messageId in messageIds)
            {
                bool queued = await queue.TryWriteAsync(new QueuedArticle(messageId, payload.Length), CancellationToken.None);
                Assert.True(queued);
            }

            queue.StopAdmission();

            using CancellationTokenSource cts = new();
            Dictionary<string, int> terminalCounts = new(StringComparer.Ordinal);
            Dictionary<string, TransitPublishStatus> terminalStatusByMessageId = new(StringComparer.Ordinal);
            object gate = new();

            void ObserveTerminal(TransitPublishResult result)
            {
                lock (gate)
                {
                    terminalCounts.TryGetValue(result.MessageId, out int count);
                    terminalCounts[result.MessageId] = count + 1;
                    terminalStatusByMessageId[result.MessageId] = result.Status;
                }
            }

            Task[] dispatchers = [.. Enumerable.Range(0, articleCount)
                .Select(_ => Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, publisher, metrics, workload, null, cts.Token, false, terminalObserver: ObserveTerminal)))];

            await metrics.WaitForAdmittedCountAsync(articleCount, CancellationToken.None);
            cts.Cancel();
            try
            {
                await Task.WhenAll(dispatchers);
            }
            catch (OperationCanceledException)
            {
            }

            MeasurementSnapshot snapshot = metrics.Snapshot();

            Assert.Equal(articleCount, snapshot.AdmittedCount);
            Assert.Equal(articleCount, snapshot.CompletedCount);
            Assert.Equal(articleCount, snapshot.CanceledCount);
            Assert.Equal(articleCount, snapshot.AmbiguousCount);
            Assert.True(snapshot.CompletedCount <= snapshot.AdmittedCount);

            Assert.Equal(articleCount, terminalCounts.Count);
            Assert.All(messageIds, id => Assert.True(terminalCounts.TryGetValue(id, out int count) && count == 1));
            Assert.All(messageIds, id => Assert.Equal(TransitPublishStatus.Canceled, terminalStatusByMessageId[id]));
        }

        [Fact]
        public void MutationGuards_AcceptedWindowAndDurationAndClassificationContractsHold()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);

            long startTick = Stopwatch.GetTimestamp();
            long boundaryTick = startTick + 10_000;
            metrics.MarkMeasurementStart(startTick);

            metrics.OnGenerated(1024, startTick + 10, TransitBenchmarkCore.ProducerTiming.FromRaw(10, 3, 7, 0), 7);
            metrics.OnAdmitted(1024, startTick + 11);
            metrics.OnPublishResult(new TransitPublishResult("<pre@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", 1, 1, 1, 1, 1, 1, 1, 1), 1024, startTick + 11, startTick + 12, startTick + 13, 1, 0, completionTickOverride: boundaryTick);

            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            metrics.OnGenerated(1024, boundaryTick + 1, TransitBenchmarkCore.ProducerTiming.FromRaw(10, 3, 7, 0), 7);
            metrics.OnAdmitted(1024, boundaryTick + 2);
            metrics.OnPublishResult(new TransitPublishResult("<post@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", 1, 1, 1, 1, 1, 1, 1, 1), 1024, boundaryTick + 2, boundaryTick + 3, boundaryTick + 4, 1, 0, completionTickOverride: boundaryTick + 1);

            metrics.OnGenerated(1024, boundaryTick + 10, TransitBenchmarkCore.ProducerTiming.FromRaw(10, 3, 7, 0), 7);
            metrics.OnAdmitted(1024, boundaryTick + 11);
            metrics.OnPublishResult(new TransitPublishResult("<reject@benchmark.usenet.ninja>", TransitPublishStatus.Rejected, 439, "reject", 1, 1, 1, 1, 1, 1, 1, 1), 1024, boundaryTick + 11, boundaryTick + 12, boundaryTick + 13, 1, 0, completionTickOverride: boundaryTick + 2);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                metrics.Snapshot(),
                metrics,
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 1, 1, 0, 0, 10, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(5),
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: 0,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: false);

            double wrongAcceptedWithAllBytes = result.AcceptedBytes * 8d / 1_000_000_000d / Math.Max(0.000001d, result.Boundary.MeasurementWindowDuration.TotalSeconds);
            double wrongAcceptedWithDrainDuration = result.AcceptedWithinWindowBytes * 8d / 1_000_000_000d / Math.Max(0.000001d, result.Boundary.EndToEndDuration.TotalSeconds);

            Assert.NotEqual(wrongAcceptedWithAllBytes, result.AcceptedGbps);
            Assert.NotEqual(wrongAcceptedWithDrainDuration, result.AcceptedGbps);
            Assert.Equal(1, result.AcceptedWithinWindowArticles);
            Assert.Equal(1, result.AcceptedPostMeasurementArticles);
            Assert.Equal(1, result.RejectedArticles);
            Assert.Equal(result.CompletedArticles, result.AcceptedArticles + result.RejectedArticles + result.AmbiguousArticles);
            Assert.True(result.AcceptedArticles <= result.CompletedArticles);
            Assert.True(result.RejectedArticles <= result.CompletedArticles);
            Assert.True(result.AmbiguousArticles <= result.CompletedArticles);
            Assert.True(result.CompletedArticles <= result.AdmittedArticles);
        }

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

        private sealed class CancellationBlockingTransitServer : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly CancellationTokenSource _cts = new();
            private readonly Task _sessionTask;

            private CancellationBlockingTransitServer(TcpListener listener)
            {
                _listener = listener;
                _sessionTask = Task.Run(RunSingleSessionAsync);
            }

            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            internal static Task<CancellationBlockingTransitServer> StartAsync()
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                return Task.FromResult(new CancellationBlockingTransitServer(listener));
            }

            public async ValueTask DisposeAsync()
            {
                _cts.Cancel();
                _listener.Stop();
                try
                {
                    await _sessionTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (IOException)
                {
                }
                catch (SocketException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }

            private async Task RunSingleSessionAsync()
            {
                using TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                await using NetworkStream stream = client.GetStream();

                await WriteLineAsync(stream, "200 transit ready", _cts.Token).ConfigureAwait(false);
                await ExpectCommandAsync(stream, "CAPABILITIES", _cts.Token).ConfigureAwait(false);
                await WriteLineAsync(stream, "101 Capability list:", _cts.Token).ConfigureAwait(false);
                await WriteLineAsync(stream, "STREAMING", _cts.Token).ConfigureAwait(false);
                await WriteLineAsync(stream, ".", _cts.Token).ConfigureAwait(false);
                await ExpectCommandAsync(stream, "MODE STREAM", _cts.Token).ConfigureAwait(false);
                await WriteLineAsync(stream, "203 Streaming permitted", _cts.Token).ConfigureAwait(false);

                TaskCompletionSource holdOpen = new(TaskCreationOptions.RunContinuationsAsynchronously);
                await holdOpen.Task.WaitAsync(_cts.Token).ConfigureAwait(false);
            }

            private static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> buffer = [];
                while (true)
                {
                    byte[] one = new byte[1];
                    int read = await stream.ReadAsync(one, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        throw new EndOfStreamException("Stream closed while reading line.");
                    }

                    if (one[0] == (byte)'\n')
                    {
                        break;
                    }

                    buffer.Add(one[0]);
                }

                if (buffer.Count > 0 && buffer[^1] == (byte)'\r')
                {
                    buffer.RemoveAt(buffer.Count - 1);
                }

                return Encoding.ASCII.GetString([.. buffer]);
            }

            private static async Task ExpectCommandAsync(Stream stream, string expected, CancellationToken cancellationToken)
            {
                string line = await ReadLineAsync(stream, cancellationToken).ConfigureAwait(false);
                Assert.Equal(expected, line);
            }

            private static Task WriteLineAsync(Stream stream, string line, CancellationToken cancellationToken)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                return stream.WriteAsync(bytes, cancellationToken).AsTask();
            }
        }
    }
}
