// <copyright file="MeasurementMetricsClassificationContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for measurement metrics classification contract, covering benchmark measurement and runtime identity contracts.

using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Covers measurement metrics classification contract behavior and invariants exercised by this test suite.
    /// </summary>
    public sealed class MeasurementMetricsClassificationContractTests
    {
        /// <summary>
        /// Exercises on publish result  classifies statuses into accepted rejected and ambiguous contracts behavior, including the expected result and failure semantics.
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
    }
}
