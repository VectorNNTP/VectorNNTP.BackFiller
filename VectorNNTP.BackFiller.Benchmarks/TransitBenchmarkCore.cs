using System.Buffers;
using System.Text;
using System.Threading.Channels;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class TransitBenchmarkCore
{
    internal static string BuildMessageId(long benchmarkInstanceId, int workerId, long sequence, string phase)
    {
        return $"<{phase}-{benchmarkInstanceId:x}-{workerId:x}-{sequence:x}@benchmark.usenet.ninja>";
    }

    internal static double StopwatchTicksToMilliseconds(long ticks)
    {
        if (ticks <= 0)
        {
            return 0;
        }

        return ticks * 1000d / System.Diagnostics.Stopwatch.Frequency;
    }

    internal readonly record struct ProducerTiming(long LoopTicks, long GenerationTicks, long BlockedTicks, long OtherActiveTicks)
    {
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

        internal long ActiveTicks => GenerationTicks + OtherActiveTicks;
    }

    internal sealed class ByteBudget : IDisposable
    {
        private readonly object _gate = new();
        private readonly Queue<BudgetWaiter> _waiters = new();
        private long _availableBytes;
        private bool _disposed;

        internal ByteBudget(long maxBytes)
        {
            if (maxBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxBytes), maxBytes, "Max bytes must be greater than zero.");
            }

            _availableBytes = maxBytes;
        }

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

        private void CancelWaiter(BudgetWaiter waiter)
        {
            lock (_gate)
            {
                waiter.MarkCanceled();
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ByteBudget));
            }
        }

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

        private sealed class BudgetWaiter
        {
            private readonly TaskCompletionSource _completion;
            private CancellationTokenRegistration _registration;

            internal BudgetWaiter(int requestedBytes)
            {
                RequestedBytes = requestedBytes;
                _completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            internal int RequestedBytes { get; }
            internal Task Task => _completion.Task;
            internal bool IsCanceled { get; private set; }

            internal void RegisterCancellation(CancellationToken cancellationToken, ByteBudget budget)
            {
                _registration = cancellationToken.Register(static state =>
                {
                    CancellationState data = (CancellationState)state!;
                    data.Waiter.TrySetCanceled();
                    data.Budget.CancelWaiter(data.Waiter);
                }, new CancellationState(budget, this));
            }

            internal void MarkCanceled()
            {
                IsCanceled = true;
            }

            internal void TrySetAcquired()
            {
                _registration.Dispose();
                _completion.TrySetResult();
            }

            internal void TrySetCanceled()
            {
                IsCanceled = true;
                _registration.Dispose();
                _completion.TrySetCanceled();
            }

            private readonly record struct CancellationState(ByteBudget Budget, BudgetWaiter Waiter);
        }
    }

    internal sealed class BoundedArticleQueue : IDisposable
    {
        private readonly Channel<QueuedArticle> _channel;
        private readonly ByteBudget _byteBudget;
        private long _queuedBytes;
        private int _queuedCount;
        private volatile bool _admissionStopped;

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

        internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
        internal long CurrentQueuedBytes => Volatile.Read(ref _queuedBytes);

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

        internal ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
        {
            return _channel.Reader.WaitToReadAsync(cancellationToken);
        }

        internal void ReleaseReservation(int bytes)
        {
            _byteBudget.Release(bytes);
        }

        internal void StopAdmission()
        {
            _admissionStopped = true;
            _channel.Writer.TryComplete();
        }

        public void Dispose()
        {
            StopAdmission();
            _byteBudget.Dispose();
        }
    }

    internal readonly record struct QueuedArticle(string MessageId, ArticlePayload Payload);

    internal readonly struct ArticlePayload : IDisposable
    {
        private readonly byte[] _buffer;
        internal int Length { get; }

        private ArticlePayload(byte[] buffer, int length)
        {
            _buffer = buffer;
            Length = length;
        }

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

        internal ReadOnlyMemory<byte> AsMemory() => _buffer.AsMemory(0, Length);

        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_buffer, clearArray: false);
        }

        private static int WriteAscii(byte[] destination, int offset, string value)
        {
            int written = Encoding.ASCII.GetBytes(value, destination.AsSpan(offset));
            return offset + written;
        }
    }

    internal static class TransitBenchmarkConfigValidator
    {
        internal static int ValidateIntRange(int value, int min, int max, string optionName)
        {
            if (value < min || value > max)
            {
                throw new InvalidOperationException($"Option '{optionName}' must be between {min} and {max}. Actual: {value}.");
            }

            return value;
        }

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
