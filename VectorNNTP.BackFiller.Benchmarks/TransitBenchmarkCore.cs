// <copyright file="TransitBenchmarkCore.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// TransitBenchmarkCore: owns shared transit benchmark setup, execution, and teardown helpers.

using System.Buffers;
using System.Text;
using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the transit BenchmarkCore class used by the benchmark or regression gate.
/// </summary>
internal static class TransitBenchmarkCore
{
    /// <summary>
    /// Builds MessageId.
    /// </summary>
    internal static string BuildMessageId(long benchmarkInstanceId, int workerId, long sequence, string phase)
    {
        return $"<{phase}-{benchmarkInstanceId:x}-{workerId:x}-{sequence:x}@benchmark.usenet.ninja>";
    }

    /// <summary>
    /// Implements the stopwatch TicksToMilliseconds contract.
    /// </summary>
    internal static double StopwatchTicksToMilliseconds(long ticks)
    {
        if (ticks <= 0)
        {
            return 0;
        }

        return ticks * 1000d / System.Diagnostics.Stopwatch.Frequency;
    }

    /// <summary>
    /// Represents the producer Timing record struct used by the benchmark or regression gate.
    /// </summary>
    internal readonly record struct ProducerTiming(long LoopTicks, long GenerationTicks, long BlockedTicks, long OtherActiveTicks)
    {
        /// <summary>
        /// Creates a value from Raw.
        /// </summary>
        internal static ProducerTiming FromRaw(long loopTicks, long generationTicks, long blockedTicks, long otherActiveTicks)
        {
            long normalizedLoopTicks = Math.Max(0, loopTicks);
            long normalizedBlockedTicks = Math.Clamp(blockedTicks, 0, normalizedLoopTicks);
            long normalizedGenerationTicks = Math.Clamp(generationTicks, 0, normalizedLoopTicks);
            long normalizedOtherActiveTicks = Math.Max(0, otherActiveTicks);
            long normalizedActiveTicks = normalizedGenerationTicks + normalizedOtherActiveTicks;

            if (normalizedActiveTicks > normalizedLoopTicks - normalizedBlockedTicks)
            {
                normalizedActiveTicks = normalizedLoopTicks - normalizedBlockedTicks;
            }

            long normalizedOther = Math.Max(0, normalizedActiveTicks - normalizedGenerationTicks);
            return new ProducerTiming(normalizedLoopTicks, normalizedGenerationTicks, normalizedBlockedTicks, normalizedOther);
        }

        /// <summary>
        /// Gets or sets the active Ticks.
        /// </summary>
        internal long ActiveTicks => GenerationTicks + OtherActiveTicks;
    }

    /// <summary>
    /// Represents the byte Budget class used by the benchmark or regression gate.
    /// </summary>
    internal sealed class ByteBudget : IDisposable
    {
        /// <summary>
        /// Runs the _gate benchmark scenario.
        /// </summary>
        private readonly object _gate = new();
        /// <summary>
        /// Runs the _waiters benchmark scenario.
        /// </summary>
        private readonly Queue<BudgetWaiter> _waiters = new();
        /// <summary>
        /// Gets or sets the _availableBytes.
        /// </summary>
        private long _availableBytes;
        /// <summary>
        /// Gets or sets the _disposed.
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// Implements the byte Budget contract.
        /// </summary>
        internal ByteBudget(long maxBytes)
        {
            if (maxBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxBytes), maxBytes, "Max bytes must be greater than zero.");
            }

            _availableBytes = maxBytes;
        }

        /// <summary>
        /// Implements the acquire Async contract.
        /// </summary>
        internal ValueTask AcquireAsync(int bytes, CancellationToken cancellationToken)
        {
            if (bytes <= 0)
            {
                return ValueTask.CompletedTask;
            }

            lock (_gate)
            {
                ThrowIfDisposed();

                if (_waiters.Count == 0 && _availableBytes >= bytes)
                {
                    _availableBytes -= bytes;
                    return ValueTask.CompletedTask;
                }

                BudgetWaiter waiter = new(bytes);
                _waiters.Enqueue(waiter);

                if (cancellationToken.CanBeCanceled)
                {
                    waiter.RegisterCancellation(cancellationToken, this);
                }

                return new ValueTask(waiter.Task);
            }
        }

        /// <summary>
        /// Runs the release benchmark scenario.
        /// </summary>
        internal void Release(int bytes)
        {
            if (bytes <= 0)
            {
                return;
            }

            List<BudgetWaiter>? completed = null;

            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _availableBytes += bytes;

                while (_waiters.Count > 0)
                {
                    BudgetWaiter next = _waiters.Peek();
                    if (next.IsCanceled)
                    {
                        _waiters.Dequeue();
                        continue;
                    }

                    if (_availableBytes < next.RequestedBytes)
                    {
                        break;
                    }

                    _waiters.Dequeue();
                    _availableBytes -= next.RequestedBytes;
                    completed ??= [];
                    completed.Add(next);
                }
            }

            if (completed is null)
            {
                return;
            }

            foreach (BudgetWaiter waiter in completed)
            {
                waiter.TrySetAcquired();
            }
        }

        /// <summary>
        /// Implements the cancel Waiter contract.
        /// </summary>
        private void CancelWaiter(BudgetWaiter waiter)
        {
            lock (_gate)
            {
                waiter.MarkCanceled();
            }
        }

        /// <summary>
        /// Implements the throw IfDisposed contract.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ByteBudget));
            }
        }

        /// <summary>
        /// Releases resources held by this instance.
        /// </summary>
        public void Dispose()
        {
            List<BudgetWaiter>? waitersToCancel = null;

            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;

                if (_waiters.Count > 0)
                {
                    waitersToCancel = _waiters.ToList();
                    _waiters.Clear();
                }
            }

            if (waitersToCancel is null)
            {
                return;
            }

            foreach (BudgetWaiter waiter in waitersToCancel)
            {
                waiter.TrySetCanceled();
            }
        }

        /// <summary>
        /// Represents the budget Waiter class used by the benchmark or regression gate.
        /// </summary>
        private sealed class BudgetWaiter
        {
            /// <summary>
            /// Gets or sets the _completion.
            /// </summary>
            private readonly TaskCompletionSource _completion;
            /// <summary>
            /// Gets or sets the _registration.
            /// </summary>
            private CancellationTokenRegistration _registration;

            /// <summary>
            /// Implements the budget Waiter contract.
            /// </summary>
            internal BudgetWaiter(int requestedBytes)
            {
                RequestedBytes = requestedBytes;
                _completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            /// <summary>
            /// Gets or sets the requested Bytes.
            /// </summary>
            internal int RequestedBytes { get; }
            /// <summary>
            /// Gets or sets the task.
            /// </summary>
            internal Task Task => _completion.Task;
            /// <summary>
            /// Gets or sets the is Canceled.
            /// </summary>
            internal bool IsCanceled { get; private set; }

            /// <summary>
            /// Implements the register Cancellation contract.
            /// </summary>
            internal void RegisterCancellation(CancellationToken cancellationToken, ByteBudget budget)
            {
                _registration = cancellationToken.Register(static state =>
                {
                    CancellationState data = (CancellationState)state!;
                    data.Waiter.TrySetCanceled();
                    data.Budget.CancelWaiter(data.Waiter);
                }, new CancellationState(budget, this));
            }

            /// <summary>
            /// Implements the mark Canceled contract.
            /// </summary>
            internal void MarkCanceled()
            {
                IsCanceled = true;
            }

            /// <summary>
            /// Implements the try SetAcquired contract.
            /// </summary>
            internal void TrySetAcquired()
            {
                _registration.Dispose();
                _completion.TrySetResult();
            }

            /// <summary>
            /// Implements the try SetCanceled contract.
            /// </summary>
            internal void TrySetCanceled()
            {
                IsCanceled = true;
                _registration.Dispose();
                _completion.TrySetCanceled();
            }

            /// <summary>
            /// Represents the cancellation State record struct used by the benchmark or regression gate.
            /// </summary>
            private readonly record struct CancellationState(ByteBudget Budget, BudgetWaiter Waiter);
        }
    }

    /// <summary>
    /// Represents the bounded ArticleQueue class used by the benchmark or regression gate.
    /// </summary>
    internal sealed class BoundedArticleQueue : IDisposable
    {
        /// <summary>
        /// Gets or sets the _channel.
        /// </summary>
        private readonly Channel<QueuedArticle> _channel;
        /// <summary>
        /// Gets or sets the _byteBudget.
        /// </summary>
        private readonly ByteBudget _byteBudget;
        /// <summary>
        /// Gets or sets the _queuedBytes.
        /// </summary>
        private long _queuedBytes;
        /// <summary>
        /// Gets or sets the _queuedCount.
        /// </summary>
        private int _queuedCount;
        /// <summary>
        /// Gets or sets the _admissionStopped.
        /// </summary>
        private volatile bool _admissionStopped;

        /// <summary>
        /// Implements the bounded ArticleQueue contract.
        /// </summary>
        internal BoundedArticleQueue(int maxArticles, long maxResidentBytes)
        {
            _channel = Channel.CreateBounded<QueuedArticle>(new BoundedChannelOptions(maxArticles)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait,
            });

            _byteBudget = new ByteBudget(maxResidentBytes);
        }

        /// <summary>
        /// Implements the current QueuedCount contract.
        /// </summary>
        internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
        /// <summary>
        /// Implements the current QueuedBytes contract.
        /// </summary>
        internal long CurrentQueuedBytes => Volatile.Read(ref _queuedBytes);

        /// <summary>
        /// Implements the try WriteAsync contract.
        /// </summary>
        internal async ValueTask<bool> TryWriteAsync(QueuedArticle article, CancellationToken cancellationToken)
        {
            if (_admissionStopped)
            {
                return false;
            }

            await _byteBudget.AcquireAsync(article.Payload.Length, cancellationToken).ConfigureAwait(false);

            try
            {
                await _channel.Writer.WriteAsync(article, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _queuedCount);
                Interlocked.Add(ref _queuedBytes, article.Payload.Length);
                return true;
            }
            catch
            {
                _byteBudget.Release(article.Payload.Length);
                throw;
            }
        }

        /// <summary>
        /// Implements the try Read contract.
        /// </summary>
        internal bool TryRead(out QueuedArticle article)
        {
            bool success = _channel.Reader.TryRead(out article);
            if (success)
            {
                Interlocked.Decrement(ref _queuedCount);
                Interlocked.Add(ref _queuedBytes, -article.Payload.Length);
            }

            return success;
        }

        /// <summary>
        /// Implements the wait ToReadAsync contract.
        /// </summary>
        internal ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
        {
            return _channel.Reader.WaitToReadAsync(cancellationToken);
        }

        /// <summary>
        /// Implements the release Reservation contract.
        /// </summary>
        internal void ReleaseReservation(int bytes)
        {
            _byteBudget.Release(bytes);
        }

        /// <summary>
        /// Stops Admission.
        /// </summary>
        internal void StopAdmission()
        {
            _admissionStopped = true;
            _channel.Writer.TryComplete();
        }

        /// <summary>
        /// Releases resources held by this instance.
        /// </summary>
        public void Dispose()
        {
            StopAdmission();
            _byteBudget.Dispose();
        }
    }

    /// <summary>
    /// Represents the queued Article record struct used by the benchmark or regression gate.
    /// </summary>
    internal readonly record struct QueuedArticle(string MessageId, ArticlePayload Payload);

    /// <summary>
    /// Represents the article Payload struct used by the benchmark or regression gate.
    /// </summary>
    internal readonly struct ArticlePayload : IDisposable
    {
        /// <summary>
        /// Gets or sets the _buffer.
        /// </summary>
        private readonly byte[] _buffer;
        /// <summary>
        /// Gets or sets the length.
        /// </summary>
        internal int Length { get; }

        /// <summary>
        /// Implements the article Payload contract.
        /// </summary>
        private ArticlePayload(byte[] buffer, int length)
        {
            _buffer = buffer;
            Length = length;
        }

        /// <summary>
        /// Runs the create benchmark scenario.
        /// </summary>
        internal static ArticlePayload Create(string messageId, int targetBytes)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(targetBytes + 4096);
            int offset = 0;

            offset = WriteAscii(buffer, offset, "Message-ID: ");
            offset = WriteAscii(buffer, offset, messageId);
            offset = WriteAscii(buffer, offset, "\r\n");

            offset = WriteAscii(buffer, offset, "Date: ");
            offset = WriteAscii(buffer, offset, DateTimeOffset.UtcNow.ToString("r"));
            offset = WriteAscii(buffer, offset, "\r\n");

            offset = WriteAscii(buffer, offset, "From: benchmark@usenet.ninja\r\n");
            offset = WriteAscii(buffer, offset, "Newsgroups: alt.test\r\n");
            offset = WriteAscii(buffer, offset, "Subject: BackFiller TransitPublisher benchmark workload\r\n");
            offset = WriteAscii(buffer, offset, "Path: benchmark.usenet.ninja\r\n");
            offset = WriteAscii(buffer, offset, "\r\n");

            int minimumTrailerBytes = 2;
            int bodyBytes = Math.Max(1, targetBytes - offset - minimumTrailerBytes);

            for (int i = 0; i < bodyBytes; i++)
            {
                buffer[offset++] = (byte)('a' + (i % 26));
            }

            if (buffer[offset - 1] != (byte)'\n')
            {
                buffer[offset++] = (byte)'\r';
                buffer[offset++] = (byte)'\n';
            }

            return new ArticlePayload(buffer, offset);
        }

        /// <summary>
        /// Implements the as Memory contract.
        /// </summary>
        internal ReadOnlyMemory<byte> AsMemory() => _buffer.AsMemory(0, Length);

        /// <summary>
        /// Releases resources held by this instance.
        /// </summary>
        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_buffer, clearArray: false);
        }

        /// <summary>
        /// Writes Ascii.
        /// </summary>
        private static int WriteAscii(byte[] destination, int offset, string value)
        {
            int written = Encoding.ASCII.GetBytes(value, destination.AsSpan(offset));
            return offset + written;
        }
    }

    /// <summary>
    /// Represents the transit BenchmarkConfigValidator class used by the benchmark or regression gate.
    /// </summary>
    internal static class TransitBenchmarkConfigValidator
    {
        /// <summary>
        /// Implements the validate IntRange contract.
        /// </summary>
        internal static int ValidateIntRange(int value, int min, int max, string optionName)
        {
            if (value < min || value > max)
            {
                throw new InvalidOperationException($"Option '{optionName}' must be between {min} and {max}. Actual: {value}.");
            }

            return value;
        }

        /// <summary>
        /// Implements the validate LongRange contract.
        /// </summary>
        internal static long ValidateLongRange(long value, long min, long max, string optionName)
        {
            if (value < min || value > max)
            {
                throw new InvalidOperationException($"Option '{optionName}' must be between {min} and {max}. Actual: {value}.");
            }

            return value;
        }
    }
}
