// <copyright file="ByteBudget.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/ByteBudget: provides a thread-safe byte-count budget for bounded benchmark queues.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the byte Budget class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class ByteBudget : IDisposable
{
    /// <summary>
    /// Performs the _gate operation.
    /// </summary>
    private readonly object _gate = new();
    /// <summary>
    /// Performs the _waiters operation.
    /// </summary>
    private readonly Queue<BudgetWaiter> _waiters = new();
    /// <summary>
    /// Gets or sets the _availableBytes value.
    /// </summary>
    private long _availableBytes;
    /// <summary>
    /// Gets or sets the _disposed value.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Performs the byte Budget operation.
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
    /// Performs the acquire Async operation.
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
    /// Performs the release operation.
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
    /// Performs the cancel Waiter operation.
    /// </summary>
    private void CancelWaiter(BudgetWaiter waiter)
    {
        lock (_gate)
        {
            waiter.MarkCanceled();
        }
    }

    /// <summary>
    /// Performs the throw IfDisposed operation.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ByteBudget));
        }
    }

    /// <summary>
    /// Performs the dispose operation.
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
    /// Defines the budget Waiter class for benchmark or isolated-regression execution.
    /// </summary>
    private sealed class BudgetWaiter
    {
        /// <summary>
        /// Gets or sets the _completion value.
        /// </summary>
        private readonly TaskCompletionSource _completion;
        /// <summary>
        /// Gets or sets the _registration value.
        /// </summary>
        private CancellationTokenRegistration _registration;

        /// <summary>
        /// Performs the budget Waiter operation.
        /// </summary>
        internal BudgetWaiter(int requestedBytes)
        {
            RequestedBytes = requestedBytes;
            _completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>
        /// Gets or sets the requested Bytes value.
        /// </summary>
        internal int RequestedBytes { get; }
        /// <summary>
        /// Gets or sets the task value.
        /// </summary>
        internal Task Task => _completion.Task;
        /// <summary>
        /// Gets or sets the is Canceled value.
        /// </summary>
        internal bool IsCanceled { get; private set; }

        /// <summary>
        /// Performs the register Cancellation operation.
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
        /// Performs the mark Canceled operation.
        /// </summary>
        internal void MarkCanceled()
        {
            IsCanceled = true;
        }

        /// <summary>
        /// Performs the try SetAcquired operation.
        /// </summary>
        internal void TrySetAcquired()
        {
            _registration.Dispose();
            _completion.TrySetResult();
        }

        /// <summary>
        /// Performs the try SetCanceled operation.
        /// </summary>
        internal void TrySetCanceled()
        {
            IsCanceled = true;
            _registration.Dispose();
            _completion.TrySetCanceled();
        }

        /// <summary>
        /// Defines the cancellation State record struct for benchmark or isolated-regression execution.
        /// </summary>
        private readonly record struct CancellationState(ByteBudget Budget, BudgetWaiter Waiter);
    }
}
