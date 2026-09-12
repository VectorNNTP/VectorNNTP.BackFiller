// <copyright file="MeasurementExecutionEngineFixedCountBehaviorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for fixed-count dispatch behavior across concurrent dispatchers.

using System.Diagnostics;
using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Verifies fixed-count admission closure and boundary behavior under concurrent dispatch.
    /// </summary>
    public sealed class MeasurementExecutionEngineFixedCountBehaviorTests
    {
        [Fact]
        public async Task DispatchLoopAsync_FixedCountConcurrentDispatchers_AdmitsExactlyTargetAndPreservesBoundarySplit()
        {
            const int targetAdmitted = 8;
            const int contenderCount = 4;
            const int totalQueued = 64;
            const int dispatcherCount = 8;

            string[] messageIds = [.. Enumerable.Range(1, totalQueued).Select(static i => $"<fixed-count-{i}@benchmark.usenet.ninja>")];
            byte[] payload = Encoding.ASCII.GetBytes("X\r\nY\r\n");

            await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback, port: 0);
            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            using BoundedArticleQueue queue = new(maxArticles: totalQueued, maxResidentBytes: 16L * 1024L * 1024L);
            MeasurementMetrics metrics = new(articleBytes: payload.Length);
            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());

            PreparedBenchmarkWorkload workload = new(
                messageIds,
                payload,
                new WorkloadPreparationSummary(0, 0, messageIds.Length, messageIds.Length, 0, payload.Length));

            foreach (string messageId in messageIds)
            {
                bool queued = await queue.TryWriteAsync(new QueuedArticle(messageId, payload.Length), CancellationToken.None);
                Assert.True(queued);
            }

            queue.StopAdmission();

            FixedArticleLimiter limiter = new(targetAdmitted);
            TaskCompletionSource finalReservationRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource contendersStaged = new(TaskCreationOptions.RunContinuationsAsynchronously);
            int waitingAtReservationGate = 0;

            ValueTask ReservationGateAsync()
            {
                if (metrics.GetAdmittedCount() < targetAdmitted - 1)
                {
                    return ValueTask.CompletedTask;
                }

                int waiting = Interlocked.Increment(ref waitingAtReservationGate);
                if (waiting >= contenderCount)
                {
                    _ = contendersStaged.TrySetResult();
                }

                return new ValueTask(finalReservationRelease.Task);
            }

            Task[] dispatchers = [.. Enumerable.Range(0, dispatcherCount)
                .Select(_ => Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(
                    queue,
                    publisher,
                    metrics,
                    workload,
                    fixedCountAdmissionLimiter: limiter,
                    cancellationToken: CancellationToken.None,
                    enableForensicDiagnostics: false,
                    reservationGateAsync: ReservationGateAsync)))];

            using CancellationTokenSource waitTimeout = new(TimeSpan.FromSeconds(10));
            await contendersStaged.Task.WaitAsync(waitTimeout.Token);
            _ = finalReservationRelease.TrySetResult();

            await metrics.WaitForAdmittedCountAsync(targetAdmitted, waitTimeout.Token);
            Assert.True(Volatile.Read(ref waitingAtReservationGate) >= contenderCount);

            Assert.Equal(targetAdmitted, metrics.GetAdmittedCount());
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, Stopwatch.GetTimestamp());

            await Task.WhenAll(dispatchers);

            MeasurementSnapshot snapshot = metrics.Snapshot();

            Assert.Equal(targetAdmitted, snapshot.AdmittedCount);
            Assert.Equal(targetAdmitted, snapshot.CompletedCount);
            Assert.Equal(targetAdmitted, snapshot.SubmittedCount);
            Assert.True(snapshot.SubmittedCount <= targetAdmitted);
            Assert.True(server.AcceptedArticles <= targetAdmitted);
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
        }
    }
