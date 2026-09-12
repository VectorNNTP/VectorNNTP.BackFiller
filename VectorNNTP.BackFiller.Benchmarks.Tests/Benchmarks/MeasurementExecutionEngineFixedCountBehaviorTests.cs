// <copyright file="MeasurementExecutionEngineFixedCountBehaviorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for fixed-count dispatch behavior across concurrent dispatchers.

using System.Diagnostics;
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
            byte[] payload = [(byte)'D', (byte)'\n'];

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
            int publishCalls = 0;
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

            ValueTask<TransitPublishResult> PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)
            {
                _ = articlePayload;
                _ = cancellationToken;
                _ = Interlocked.Increment(ref publishCalls);

                long tick = Stopwatch.GetTimestamp();
                return ValueTask.FromResult(new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Accepted,
                    ResponseCode: 239,
                    ResponseText: "ok",
                    T0PublishAsyncEnterTick: tick,
                    T1DispatcherAssignedTick: tick,
                    T2SocketWriteBeginTick: tick,
                    T3SocketWriteEndTick: tick,
                    T4ResponseAvailableTick: tick,
                    T5ResponseParsedTick: tick,
                    T6ResponseCorrelatedTick: tick,
                    T7PublishAsyncCompleteTick: tick,
                    Provenance: TransitPublishProvenance.OtherOrUnknown,
                    ProvenanceTick: tick));
            }

            Task[] dispatchers = [.. Enumerable.Range(0, dispatcherCount)
                .Select(_ => Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(
                    queue,
                    publisher: null,
                    metrics,
                    workload,
                    fixedCountAdmissionLimiter: limiter,
                    cancellationToken: CancellationToken.None,
                    enableForensicDiagnostics: false,
                    publishAsyncOverride: PublishAsync,
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

            Assert.Equal(targetAdmitted, publishCalls);
            Assert.Equal(targetAdmitted, snapshot.AdmittedCount);
            Assert.Equal(targetAdmitted, snapshot.CompletedCount);
            Assert.Equal(targetAdmitted, snapshot.AcceptedCount);
            Assert.True(publishCalls <= targetAdmitted);
        }
    }
}
