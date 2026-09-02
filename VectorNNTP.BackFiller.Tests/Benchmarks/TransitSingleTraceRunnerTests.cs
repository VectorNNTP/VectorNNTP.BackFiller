// <copyright file="TransitSingleTraceRunnerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for transit single trace runner, covering NNTP article and transport behavior.

using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Covers transit single trace runner behavior and invariants exercised by this test suite.
    /// </summary>
    public sealed class TransitSingleTraceRunnerTests
    {
        /// <summary>
        /// Exercises resolve requested article count  when null  defaults to one behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ResolveRequestedArticleCount_WhenNull_DefaultsToOne()
        {
            int count = TransitSingleTraceRunner.ResolveRequestedArticleCount(null);

            Assert.Equal(1, count);
        }
        /// <summary>
        /// Exercises resolve requested article count  when specified  returns specified value behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ResolveRequestedArticleCount_WhenSpecified_ReturnsSpecifiedValue()
        {
            int count = TransitSingleTraceRunner.ResolveRequestedArticleCount(10);

            Assert.Equal(10, count);
        }
        /// <summary>
        /// Exercises publish sequentially async  when article count one  publishes exactly once behavior, including the expected result and failure semantics.
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
        /// Exercises publish sequentially async  when article count ten  publishes ten sequentially behavior, including the expected result and failure semantics.
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
        /// Exercises publish with pipeline depth async  when depth two  can have two outstanding concurrently behavior, including the expected result and failure semantics.
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
        /// Covers recording publish executor behavior and invariants exercised by this test suite.
        /// </summary>
        private sealed class RecordingPublishExecutor : TransitSingleTraceRunner.ITransitSingleTracePublishExecutor
        {
            /// <summary>
            /// Supplies call order for the fixture or scenario under test.
            /// </summary>
            internal List<int> CallOrder { get; } = [];

            /// <summary>
            /// Supplies  started count for the fixture or scenario under test.
            /// </summary>
            private int _startedCount;

            /// <summary>
            /// Exercises publish async behavior, including the expected result and failure semantics.
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
        /// Covers controlled publish executor behavior and invariants exercised by this test suite.
        /// </summary>
        private sealed class ControlledPublishExecutor : TransitSingleTraceRunner.ITransitSingleTracePublishExecutor
        {
            /// <summary>
            /// Supplies  gates by call index for the fixture or scenario under test.
            /// </summary>
            private readonly Dictionary<int, TaskCompletionSource<bool>> _gatesByCallIndex = [];
            /// <summary>
            /// Exercises  call order queue behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly Queue<int> _callOrderQueue = new();
            /// <summary>
            /// Exercises  sync behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly object _sync = new();

            /// <summary>
            /// Supplies call order for the fixture or scenario under test.
            /// </summary>
            internal List<int> CallOrder { get; } = [];

            /// <summary>
            /// Supplies started count for the fixture or scenario under test.
            /// </summary>
            internal int StartedCount { get; private set; }

            /// <summary>
            /// Supplies completed count for the fixture or scenario under test.
            /// </summary>
            internal int CompletedCount { get; private set; }

            /// <summary>
            /// Supplies current outstanding count for the fixture or scenario under test.
            /// </summary>
            internal int CurrentOutstandingCount => StartedCount - CompletedCount;

            /// <summary>
            /// Supplies max outstanding observed for the fixture or scenario under test.
            /// </summary>
            internal int MaxOutstandingObserved { get; private set; }

            /// <summary>
            /// Exercises wait until started async behavior, including the expected result and failure semantics.
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
            /// Exercises complete next behavior, including the expected result and failure semantics.
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
            /// Exercises complete by call index behavior, including the expected result and failure semantics.
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
            /// Exercises publish async behavior, including the expected result and failure semantics.
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


