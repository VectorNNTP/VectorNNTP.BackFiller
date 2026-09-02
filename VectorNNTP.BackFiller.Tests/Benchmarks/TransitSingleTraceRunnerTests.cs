// <copyright file="TransitSingleTraceRunnerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Contract and behavior tests for the transit single trace runner benchmark component.

using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Documents the TransitSingleTraceRunnerTests test type and its protected contract.
    /// </summary>
    public sealed class TransitSingleTraceRunnerTests
    {
        /// <summary>
        /// Verifies the ResolveRequestedArticleCount_WhenNull_DefaultsToOne scenario and expected contract.
        /// </summary>
        [Fact]
        public void ResolveRequestedArticleCount_WhenNull_DefaultsToOne()
        {
            int count = TransitSingleTraceRunner.ResolveRequestedArticleCount(null);

            Assert.Equal(1, count);
        }
        /// <summary>
        /// Verifies the ResolveRequestedArticleCount_WhenSpecified_ReturnsSpecifiedValue scenario and expected contract.
        /// </summary>
        [Fact]
        public void ResolveRequestedArticleCount_WhenSpecified_ReturnsSpecifiedValue()
        {
            int count = TransitSingleTraceRunner.ResolveRequestedArticleCount(10);

            Assert.Equal(10, count);
        }
        /// <summary>
        /// Verifies the PublishSequentiallyAsync_WhenArticleCountOne_PublishesExactlyOnce scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task PublishSequentiallyAsync_WhenArticleCountOne_PublishesExactlyOnce()
        {
            RecordingPublishExecutor executor = new();
            List<string> logs = [];

            TransitSingleTraceRunner.SingleTracePublishBatchResult result = await TransitSingleTraceRunner.PublishSequentiallyAsync(
                executor,
                requestedArticleCount: 1,
                articleTargetBytes: 128 * 1024,
                logs.Add,
                CancellationToken.None);

            _ = Assert.Single(executor.CallOrder);
            _ = Assert.Single(result.MessageIds);
            _ = Assert.Single(result.PublishResults);
            Assert.Equal(0, result.TimeoutCount);
        }
        /// <summary>
        /// Verifies the PublishSequentiallyAsync_WhenArticleCountTen_PublishesTenSequentially scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task PublishSequentiallyAsync_WhenArticleCountTen_PublishesTenSequentially()
        {
            ControlledPublishExecutor executor = new();
            List<string> logs = [];

            Task<TransitSingleTraceRunner.SingleTracePublishBatchResult> publishTask = TransitSingleTraceRunner.PublishSequentiallyAsync(
                executor,
                requestedArticleCount: 10,
                articleTargetBytes: 128 * 1024,
                logs.Add,
                CancellationToken.None);

            for (int expected = 1; expected <= 10; expected++)
            {
                await executor.WaitUntilStartedAsync(expected);
                Assert.Equal(expected, executor.StartedCount);
                Assert.Equal(expected - 1, executor.CompletedCount);
                executor.CompleteNext();
            }

            TransitSingleTraceRunner.SingleTracePublishBatchResult result = await publishTask;

            Assert.Equal(10, executor.StartedCount);
            Assert.Equal(10, executor.CompletedCount);
            Assert.Equal(10, result.MessageIds.Count);
            Assert.Equal(10, result.PublishResults.Count);
            Assert.Equal(0, result.TimeoutCount);
            Assert.Equal(Enumerable.Range(1, 10), executor.CallOrder);
            Assert.Equal(1, executor.MaxOutstandingObserved);
        }
        /// <summary>
        /// Verifies the PublishWithPipelineDepthAsync_WhenDepthTwo_CanHaveTwoOutstandingConcurrently scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task PublishWithPipelineDepthAsync_WhenDepthTwo_CanHaveTwoOutstandingConcurrently()
        {
            ControlledPublishExecutor executor = new();
            List<string> logs = [];

            Task<TransitSingleTraceRunner.SingleTracePublishBatchResult> publishTask = TransitSingleTraceRunner.PublishWithPipelineDepthAsync(
                executor,
                requestedArticleCount: 10,
                articleTargetBytes: 128 * 1024,
                effectivePipelineDepth: 2,
                logs.Add,
                CancellationToken.None);

            await executor.WaitUntilStartedAsync(2);
            Assert.Equal(2, executor.StartedCount);
            Assert.Equal(0, executor.CompletedCount);
            Assert.Equal(2, executor.CurrentOutstandingCount);

            executor.CompleteByCallIndex(1);
            await executor.WaitUntilStartedAsync(3);
            Assert.True(executor.MaxOutstandingObserved >= 2);

            executor.CompleteByCallIndex(2);
            for (int expected = 3; expected <= 10; expected++)
            {
                await executor.WaitUntilStartedAsync(expected);
                executor.CompleteByCallIndex(expected);
            }

            TransitSingleTraceRunner.SingleTracePublishBatchResult result = await publishTask;

            Assert.Equal(10, executor.StartedCount);
            Assert.Equal(10, executor.CompletedCount);
            Assert.Equal(10, result.MessageIds.Count);
            Assert.Equal(10, result.PublishResults.Count);
            Assert.Equal(0, result.TimeoutCount);
            Assert.True(executor.MaxOutstandingObserved >= 2);
            Assert.True(executor.MaxOutstandingObserved <= 2);
        }

        /// <summary>
        /// Documents the RecordingPublishExecutor test type and its protected contract.
        /// </summary>
        private sealed class RecordingPublishExecutor : TransitSingleTraceRunner.ITransitSingleTracePublishExecutor
        {
            /// <summary>
            /// Stores the CallOrder value used by this test fixture.
            /// </summary>
            internal List<int> CallOrder { get; } = [];

            /// <summary>
            /// Stores the _startedCount fixture value used by these tests.
            /// </summary>
            private int _startedCount;

            /// <summary>
            /// Verifies the PublishAsync scenario and expected contract.
            /// </summary>
            public ValueTask<TransitPublishResult> PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(messageId);

                int callIndex = Interlocked.Increment(ref _startedCount);
                CallOrder.Add(callIndex);

                TransitPublishResult result = new(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Accepted,
                    ResponseCode: 239,
                    ResponseText: "ok",
                    T0PublishAsyncEnterTick: 0,
                    T1DispatcherAssignedTick: 0,
                    T2SocketWriteBeginTick: 0,
                    T3SocketWriteEndTick: 0,
                    T4ResponseAvailableTick: 0,
                    T5ResponseParsedTick: 0,
                    T6ResponseCorrelatedTick: 0,
                    T7PublishAsyncCompleteTick: 0);

                return new ValueTask<TransitPublishResult>(result);
            }
        }

        /// <summary>
        /// Documents the ControlledPublishExecutor test type and its protected contract.
        /// </summary>
        private sealed class ControlledPublishExecutor : TransitSingleTraceRunner.ITransitSingleTracePublishExecutor
        {
            /// <summary>
            /// Documents the _gatesByCallIndex member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<int, TaskCompletionSource<bool>> _gatesByCallIndex = [];
            /// <summary>
            /// Stores the _callOrderQueue fixture value used by these tests.
            /// </summary>
            private readonly Queue<int> _callOrderQueue = new();
            /// <summary>
            /// Stores the _sync fixture value used by these tests.
            /// </summary>
            private readonly object _sync = new();

            /// <summary>
            /// Stores the CallOrder value used by this test fixture.
            /// </summary>
            internal List<int> CallOrder { get; } = [];

            /// <summary>
            /// Stores the StartedCount value used by this test fixture.
            /// </summary>
            internal int StartedCount { get; private set; }

            /// <summary>
            /// Stores the CompletedCount value used by this test fixture.
            /// </summary>
            internal int CompletedCount { get; private set; }

            /// <summary>
            /// Stores the CurrentOutstandingCount value used by this test fixture.
            /// </summary>
            internal int CurrentOutstandingCount => StartedCount - CompletedCount;

            /// <summary>
            /// Stores the MaxOutstandingObserved value used by this test fixture.
            /// </summary>
            internal int MaxOutstandingObserved { get; private set; }

            /// <summary>
            /// Verifies the WaitUntilStartedAsync scenario and expected contract.
            /// </summary>
            internal async Task WaitUntilStartedAsync(int expectedCount)
            {
                while (true)
                {
                    lock (_sync)
                    {
                        if (StartedCount >= expectedCount)
                        {
                            return;
                        }
                    }

                    await Task.Delay(1).ConfigureAwait(false);
                }
            }

            /// <summary>
            /// Verifies the CompleteNext scenario and expected contract.
            /// </summary>
            internal void CompleteNext()
            {
                int callIndex;
                TaskCompletionSource<bool> gate;
                lock (_sync)
                {
                    callIndex = _callOrderQueue.Dequeue();
                    gate = _gatesByCallIndex[callIndex];
                }

                gate.SetResult(true);
            }

            /// <summary>
            /// Verifies the CompleteByCallIndex scenario and expected contract.
            /// </summary>
            internal void CompleteByCallIndex(int callIndex)
            {
                TaskCompletionSource<bool> gate;
                lock (_sync)
                {
                    gate = _gatesByCallIndex[callIndex];
                }

                gate.SetResult(true);
            }

            /// <summary>
            /// Verifies the PublishAsync scenario and expected contract.
            /// </summary>
            public async ValueTask<TransitPublishResult> PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(messageId);

                TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
                int callIndex;

                lock (_sync)
                {
                    StartedCount++;
                    callIndex = StartedCount;
                    CallOrder.Add(callIndex);
                    _gatesByCallIndex.Add(callIndex, gate);
                    _callOrderQueue.Enqueue(callIndex);
                    int currentOutstanding = StartedCount - CompletedCount;
                    if (currentOutstanding > MaxOutstandingObserved)
                    {
                        MaxOutstandingObserved = currentOutstanding;
                    }
                }

                _ = await gate.Task.ConfigureAwait(false);

                lock (_sync)
                {
                    CompletedCount++;
                    _ = _gatesByCallIndex.Remove(callIndex);
                }

                return new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Accepted,
                    ResponseCode: 239,
                    ResponseText: "ok",
                    T0PublishAsyncEnterTick: 0,
                    T1DispatcherAssignedTick: 0,
                    T2SocketWriteBeginTick: 0,
                    T3SocketWriteEndTick: 0,
                    T4ResponseAvailableTick: 0,
                    T5ResponseParsedTick: 0,
                    T6ResponseCorrelatedTick: 0,
                    T7PublishAsyncCompleteTick: 0);
            }
        }
    }
}


