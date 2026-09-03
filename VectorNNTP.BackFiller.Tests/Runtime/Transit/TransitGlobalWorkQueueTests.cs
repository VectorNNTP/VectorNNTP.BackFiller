// <copyright file="TransitGlobalWorkQueueTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit global work queue, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit global work queue test suite.

using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Transit
{
    /// <summary>
    /// Tests the BackFiller global transit work queue ownership, bounds, retry, and completion semantics.
    /// </summary>
    public sealed class TransitGlobalWorkQueueTests
    {
        /// <summary>
        /// Confirms the enqueue and claim when capacity available updates queue and in flight accounting behavior.
        /// </summary>
        [Fact]
        public async Task EnqueueAndClaim_WhenCapacityAvailable_UpdatesQueueAndInFlightAccounting()
        {
            GlobalTransitWorkQueue queue = new(maxQueuedItemCount: 4, maxQueuedPayloadBytes: 1024);
            TransitWorkItem item = CreateItem(1, "<queue-claim@example.com>", 128);

            await queue.EnqueueAsync(item, CancellationToken.None);

            Assert.Equal(1, queue.QueuedItemCount);
            Assert.Equal(128, queue.QueuedPayloadBytes);
            Assert.Equal(0, queue.InFlightCount);

            bool claimed = queue.TryClaim("conn-1", out TransitWorkItem? claimedItem);

            Assert.True(claimed);
            Assert.NotNull(claimedItem);
            Assert.Equal(item.WorkItemId, claimedItem!.WorkItemId);
            Assert.Equal(0, queue.QueuedItemCount);
            Assert.Equal(0, queue.QueuedPayloadBytes);
            Assert.Equal(1, queue.InFlightCount);
        }
        /// <summary>
        /// Confirms the enqueue async when item capacity reached waits until claim frees capacity behavior.
        /// </summary>
        [Fact]
        public async Task EnqueueAsync_WhenItemCapacityReached_WaitsUntilClaimFreesCapacity()
        {
            GlobalTransitWorkQueue queue = new(maxQueuedItemCount: 1, maxQueuedPayloadBytes: 4096);
            TransitWorkItem first = CreateItem(1, "<wait-item-1@example.com>", 64);
            TransitWorkItem second = CreateItem(2, "<wait-item-2@example.com>", 64);

            await queue.EnqueueAsync(first, CancellationToken.None);

            Task blockedEnqueue = queue.EnqueueAsync(second, CancellationToken.None).AsTask();
            await Task.Delay(50);
            Assert.False(blockedEnqueue.IsCompleted);

            Assert.True(queue.TryClaim("conn-1", out _));

            await blockedEnqueue;
            Assert.Equal(1, queue.QueuedItemCount);
            Assert.Equal(64, queue.QueuedPayloadBytes);
        }
        /// <summary>
        /// Confirms the enqueue async when payload byte capacity reached waits until claim frees bytes behavior.
        /// </summary>
        [Fact]
        public async Task EnqueueAsync_WhenPayloadByteCapacityReached_WaitsUntilClaimFreesBytes()
        {
            GlobalTransitWorkQueue queue = new(maxQueuedItemCount: 10, maxQueuedPayloadBytes: 128);
            TransitWorkItem first = CreateItem(1, "<wait-bytes-1@example.com>", 128);
            TransitWorkItem second = CreateItem(2, "<wait-bytes-2@example.com>", 1);

            await queue.EnqueueAsync(first, CancellationToken.None);

            Task blockedEnqueue = queue.EnqueueAsync(second, CancellationToken.None).AsTask();
            await Task.Delay(50);
            Assert.False(blockedEnqueue.IsCompleted);

            Assert.True(queue.TryClaim("conn-1", out _));

            await blockedEnqueue;
            Assert.Equal(1, queue.QueuedItemCount);
            Assert.Equal(1, queue.QueuedPayloadBytes);
        }
        /// <summary>
        /// Confirms the schedule retry async when attempt budget remaining requeues after delay behavior.
        /// </summary>
        [Fact]
        public async Task ScheduleRetryAsync_WhenAttemptBudgetRemaining_RequeuesAfterDelay()
        {
            GlobalTransitWorkQueue queue = new(maxQueuedItemCount: 4, maxQueuedPayloadBytes: 1024);
            TransitWorkItem item = CreateItem(1, "<retry@example.com>", 64);

            await queue.EnqueueAsync(item, CancellationToken.None);
            Assert.True(queue.TryClaim("conn-1", out TransitWorkItem? claimed));
            Assert.NotNull(claimed);

            _ = await queue.ScheduleRetryAsync(
                claimed!,
                TransitWorkFailureClass.ConnectionReset,
                TransitTransmissionUncertainty.ConnectionFailedDuringSend,
                retryDelay: TimeSpan.FromMilliseconds(40),
                transferOwnershipFromInFlight: true,
                cancellationToken: CancellationToken.None);

            Assert.Equal(1, queue.RetryPendingCount);
            Assert.Equal(0, queue.QueuedItemCount);

            await Task.Delay(60);
            await queue.DrainEligibleRetriesAsync(CancellationToken.None);

            Assert.Equal(0, queue.RetryPendingCount);
            Assert.Equal(1, queue.QueuedItemCount);
            Assert.True(queue.TryClaim("conn-2", out TransitWorkItem? retried));
            Assert.NotNull(retried);
            Assert.Equal(2, retried!.AttemptCount);
        }
        /// <summary>
        /// Confirms the transit work item try complete allows exactly one terminal completion behavior.
        /// </summary>
        [Fact]
        public void TransitWorkItem_TryComplete_AllowsExactlyOneTerminalCompletion()
        {
            TransitWorkItem item = CreateItem(1, "<complete-once@example.com>", 64);

            TransitPublishResult first = new(
                MessageId: item.MessageId,
                Status: TransitPublishStatus.Accepted,
                ResponseCode: 239,
                ResponseText: "ok",
                Provenance: TransitPublishProvenance.OtherOrUnknown);

            TransitPublishResult second = new(
                MessageId: item.MessageId,
                Status: TransitPublishStatus.Failed,
                ResponseCode: null,
                ResponseText: "should not win",
                Provenance: TransitPublishProvenance.Failed);

            bool firstWon = item.TryComplete(first, TransitPublishProvenance.OtherOrUnknown);
            bool secondWon = item.TryComplete(second, TransitPublishProvenance.Failed);

            Assert.True(firstWon);
            Assert.False(secondWon);
            Assert.Equal(TransitWorkItemState.CompletedAccepted, item.State);
        }
        /// <summary>
        /// Confirms the transit work item retry attempt budget is bounded to three transmissions behavior.
        /// </summary>
        [Fact]
        public void TransitWorkItem_RetryAttemptBudget_IsBoundedToThreeTransmissions()
        {
            TransitWorkItem item = CreateItem(1, "<attempt-budget@example.com>", 64);

            item.MarkClaimed("conn-1", DateTimeOffset.UtcNow);
            Assert.Equal(1, item.AttemptCount);
            Assert.True(item.HasAttemptsRemaining());

            Assert.True(item.TryMoveToRetryPending(
                TransitWorkFailureClass.ConnectionReset,
                TransitTransmissionUncertainty.ConnectionFailedDuringSend,
                DateTimeOffset.UtcNow,
                TimeSpan.Zero));

            item.MarkQueued(DateTimeOffset.UtcNow);
            item.MarkClaimed("conn-1", DateTimeOffset.UtcNow);
            Assert.Equal(2, item.AttemptCount);
            Assert.True(item.HasAttemptsRemaining());

            Assert.True(item.TryMoveToRetryPending(
                TransitWorkFailureClass.ConnectionReset,
                TransitTransmissionUncertainty.ConnectionFailedDuringSend,
                DateTimeOffset.UtcNow,
                TimeSpan.Zero));

            item.MarkQueued(DateTimeOffset.UtcNow);
            item.MarkClaimed("conn-1", DateTimeOffset.UtcNow);

            Assert.Equal(3, item.AttemptCount);
            Assert.False(item.HasAttemptsRemaining());
        }
        /// <summary>
        /// Confirms the mark in flight terminal when no in flight ownership throws invariant violation behavior.
        /// </summary>
        [Fact]
        public void MarkInFlightTerminal_WhenNoInFlightOwnership_ThrowsInvariantViolation()
        {
            GlobalTransitWorkQueue queue = new(maxQueuedItemCount: 2, maxQueuedPayloadBytes: 1024);

            InvalidOperationException exception = Assert.Throws<InvalidOperationException>(queue.MarkInFlightTerminal);
            Assert.Contains("in-flight accounting invariant", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Confirms the create item behavior.
        /// </summary>
        /// <returns>The value returned by the create item helper.</returns>
        /// <summary>
        /// Confirms the create item behavior.
        /// </summary>
        /// <param name="id">The id used by this test scenario.</param>
        /// <param name="messageId">The message id used by this test scenario.</param>
        /// <param name="payloadSize">The payload size used by this test scenario.</param>
        /// <returns>The value returned by the create item helper.</returns>
        private static TransitWorkItem CreateItem(long id, string messageId, int payloadSize)
        {
            byte[] payload = new byte[payloadSize];
            if (payloadSize > 0)
            {
                payload[^1] = (byte)'\n';
            }

            return new TransitWorkItem(id, messageId, payload, maxAttempts: 3);
        }
    }
}


