// <copyright file="TransitSingleTraceRunnerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    public sealed class TransitSingleTraceRunnerTests
    {
        [Fact]
        public void ResolveRequestedArticleCount_WhenNull_DefaultsToOne()
        {
            int count = TransitSingleTraceRunner.ResolveRequestedArticleCount(null);

            Assert.Equal(1, count);
        }

        [Fact]
        public void ResolveRequestedArticleCount_WhenSpecified_ReturnsSpecifiedValue()
        {
            int count = TransitSingleTraceRunner.ResolveRequestedArticleCount(10);

            Assert.Equal(10, count);
        }

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

            TransitSingleTraceRunner.SingleTracePublishBatchResult result = await publishTask.ConfigureAwait(false);

            Assert.Equal(10, executor.StartedCount);
            Assert.Equal(10, executor.CompletedCount);
            Assert.Equal(10, result.MessageIds.Count);
            Assert.Equal(10, result.PublishResults.Count);
            Assert.Equal(0, result.TimeoutCount);
            Assert.Equal(Enumerable.Range(1, 10), executor.CallOrder);
            Assert.Equal(1, executor.MaxOutstandingObserved);
        }

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

            TransitSingleTraceRunner.SingleTracePublishBatchResult result = await publishTask.ConfigureAwait(false);

            Assert.Equal(10, executor.StartedCount);
            Assert.Equal(10, executor.CompletedCount);
            Assert.Equal(10, result.MessageIds.Count);
            Assert.Equal(10, result.PublishResults.Count);
            Assert.Equal(0, result.TimeoutCount);
            Assert.True(executor.MaxOutstandingObserved >= 2);
            Assert.True(executor.MaxOutstandingObserved <= 2);
        }

        private sealed class RecordingPublishExecutor : TransitSingleTraceRunner.ITransitSingleTracePublishExecutor
        {
            internal List<int> CallOrder { get; } = [];

            private int _startedCount;

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

        private sealed class ControlledPublishExecutor : TransitSingleTraceRunner.ITransitSingleTracePublishExecutor
        {
            private readonly Dictionary<int, TaskCompletionSource<bool>> _gatesByCallIndex = [];
            private readonly Queue<int> _callOrderQueue = new();
            private readonly object _sync = new();

            internal List<int> CallOrder { get; } = [];

            internal int StartedCount { get; private set; }

            internal int CompletedCount { get; private set; }

            internal int CurrentOutstandingCount => StartedCount - CompletedCount;

            internal int MaxOutstandingObserved { get; private set; }

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

            internal void CompleteByCallIndex(int callIndex)
            {
                TaskCompletionSource<bool> gate;
                lock (_sync)
                {
                    gate = _gatesByCallIndex[callIndex];
                }

                gate.SetResult(true);
            }

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
