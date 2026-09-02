// <copyright file="ByteBudget.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/ByteBudget: provides a thread-safe byte-count budget for bounded benchmark queues.

namespace VectorNNTP.BackFiller.Benchmarks;

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
