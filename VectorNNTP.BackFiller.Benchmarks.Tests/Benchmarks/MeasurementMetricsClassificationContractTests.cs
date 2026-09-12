// <copyright file="MeasurementMetricsClassificationContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for measurement metrics classification contract, covering benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the measurement metrics classification contract test suite.

using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Confirms the measurement metrics classification contract tests behavior.
    /// </summary>
    public sealed class MeasurementMetricsClassificationContractTests
    {
        /// <summary>
        /// Confirms the on publish result classifies statuses into accepted rejected and ambiguous contracts behavior.
        /// </summary>
        [Fact]
        public void OnPublishResult_ClassifiesStatusesIntoAcceptedRejectedAndAmbiguousContracts()
        {
            MeasurementMetrics metrics = new(articleBytes: 1024);

            TransitPublishStatus[] statuses =
            [
                TransitPublishStatus.Accepted,
                TransitPublishStatus.Rejected,
                TransitPublishStatus.Ambiguous,
                TransitPublishStatus.Failed,
                TransitPublishStatus.Unavailable,
                TransitPublishStatus.Canceled,
                TransitPublishStatus.Queued
            ];

            foreach (TransitPublishStatus status in statuses)
            {
                metrics.OnPublishResult(
                    new TransitPublishResult(
                        MessageId: $"<{status}-contract@benchmark.usenet.ninja>",
                        Status: status,
                        ResponseCode: null,
                        ResponseText: null,
                        T0PublishAsyncEnterTick: 10,
                        T1DispatcherAssignedTick: 11,
                        T2SocketWriteBeginTick: 12,
                        T3SocketWriteEndTick: 13,
                        T4ResponseAvailableTick: 14,
                        T5ResponseParsedTick: 15,
                        T6ResponseCorrelatedTick: 16,
                        T7PublishAsyncCompleteTick: 17),
                    bytes: 1024,
                    dequeuedTick: 9,
                    publishStartTick: 10,
                    publishEndTick: 17,
                    pendingAtSubmit: 1,
                    pendingAtComplete: 1);
            }

            MeasurementSnapshot snapshot = metrics.Snapshot();

            Assert.Equal(1, snapshot.AcceptedCount);
            Assert.Equal(1024, snapshot.AcceptedBytes);
            Assert.Equal(1, snapshot.RejectedCount);
            Assert.Equal(4, snapshot.AmbiguousCount);
            Assert.Equal(statuses.Length, snapshot.CompletedCount);
        }

        [Fact]
        public void IsPostMeasurementTick_WhenBoundaryTickIsPublished_IsClassifiedBySingleTickState()
        {
            MeasurementMetrics metrics = new(articleBytes: 1024);
            long start = Stopwatch.GetTimestamp();
            long boundary = start + 100;
            metrics.MarkMeasurementStart(start);
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundary);

            Assert.False(metrics.IsPostMeasurementTick(boundary));
            Assert.True(metrics.IsPostMeasurementTick(boundary + 1));
        }

        [Fact]
        public void OnGenerated_WhenOfferCompletesAfterBoundary_IsClassifiedAsPostMeasurement()
        {
            MeasurementMetrics metrics = new(articleBytes: 1024);
            long start = Stopwatch.GetTimestamp();
            long boundary = start + 100;
            metrics.MarkMeasurementStart(start);
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundary);

            metrics.OnGenerated(1024, boundary + 1, TransitBenchmarkCore.ProducerTiming.FromRaw(loopTicks: 10, generationTicks: 3, blockedTicks: 7, otherActiveTicks: 0), queueWaitTicks: 7);

            MeasurementSnapshot snapshot = metrics.Snapshot();
            Assert.Equal(1, snapshot.OfferedCount);
            Assert.Equal(0, snapshot.OfferedWithinWindowCount);
        }

        [Fact]
        public void OnAdmitted_WhenAdmissionOccursAfterBoundary_IsClassifiedAsPostMeasurement()
        {
            MeasurementMetrics metrics = new(articleBytes: 1024);
            long start = Stopwatch.GetTimestamp();
            long boundary = start + 100;
            metrics.MarkMeasurementStart(start);
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundary);

            metrics.OnAdmitted(1024, boundary + 1);

            MeasurementSnapshot snapshot = metrics.Snapshot();
            Assert.Equal(1, snapshot.AdmittedCount);
            Assert.Equal(0, snapshot.AdmittedWithinWindowCount);
        }

        [Fact]
        public void OnPublishResult_UsesPublishEndTickForCompletionClassificationWithoutOverride()
        {
            MeasurementMetrics metrics = new(articleBytes: 1024);
            long start = Stopwatch.GetTimestamp();
            long boundary = start + 100;
            metrics.MarkMeasurementStart(start);
            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundary);

            metrics.OnPublishResult(
                new TransitPublishResult(
                    MessageId: "<completion-source@benchmark.usenet.ninja>",
                    Status: TransitPublishStatus.Accepted,
                    ResponseCode: 239,
                    ResponseText: "ok",
                    T0PublishAsyncEnterTick: start + 10,
                    T1DispatcherAssignedTick: start + 11,
                    T2SocketWriteBeginTick: start + 12,
                    T3SocketWriteEndTick: start + 13,
                    T4ResponseAvailableTick: start + 14,
                    T5ResponseParsedTick: start + 15,
                    T6ResponseCorrelatedTick: start + 16,
                    T7PublishAsyncCompleteTick: start + 17),
                bytes: 1024,
                dequeuedTick: start + 9,
                publishStartTick: start + 10,
                publishEndTick: boundary,
                pendingAtSubmit: 1,
                pendingAtComplete: 0);

            MeasurementSnapshot snapshot = metrics.Snapshot();
            Assert.Equal(1, snapshot.AcceptedWithinWindowCount);
            Assert.Equal(0, snapshot.AcceptedPostMeasurementCount);
            Assert.Equal(1, snapshot.CompletedWithinWindowCount);
            Assert.Equal(0, snapshot.CompletedPostMeasurementCount);

            metrics.OnPublishResult(
                new TransitPublishResult(
                    MessageId: "<completion-source-post@benchmark.usenet.ninja>",
                    Status: TransitPublishStatus.Accepted,
                    ResponseCode: 239,
                    ResponseText: "ok",
                    T0PublishAsyncEnterTick: start + 20,
                    T1DispatcherAssignedTick: start + 21,
                    T2SocketWriteBeginTick: start + 22,
                    T3SocketWriteEndTick: start + 23,
                    T4ResponseAvailableTick: start + 24,
                    T5ResponseParsedTick: start + 25,
                    T6ResponseCorrelatedTick: start + 26,
                    T7PublishAsyncCompleteTick: start + 27),
                bytes: 1024,
                dequeuedTick: start + 19,
                publishStartTick: start + 20,
                publishEndTick: boundary + 1,
                pendingAtSubmit: 1,
                pendingAtComplete: 0);

            snapshot = metrics.Snapshot();
            Assert.Equal(1, snapshot.AcceptedWithinWindowCount);
            Assert.Equal(1, snapshot.AcceptedPostMeasurementCount);
            Assert.Equal(1, snapshot.CompletedWithinWindowCount);
            Assert.Equal(1, snapshot.CompletedPostMeasurementCount);
        }
    }
}
