// <copyright file="ByteBudget.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/ByteBudget: provides a thread-safe byte-count budget for bounded benchmark queues.

namespace VectorNNTP.BackFiller.Benchmarks
{

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

            /// <summary>
            /// Represents the cancellation State record struct used by the benchmark or regression gate.
            /// </summary>
            private readonly record struct CancellationState(ByteBudget Budget, BudgetWaiter Waiter);
        }
    }
}



