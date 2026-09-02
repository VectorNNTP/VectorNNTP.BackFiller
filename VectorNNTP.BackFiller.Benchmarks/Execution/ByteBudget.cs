// <copyright file="ByteBudget.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/ByteBudget: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the byte Budget class used by this benchmark or regression-gate component.
/// </summary>
internal sealed class ByteBudget : IDisposable
{
    /// <summary>
    /// Executes the _gate operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private readonly object _gate = new();
    /// <summary>
    /// Executes the _waiters operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private readonly Queue<BudgetWaiter> _waiters = new();
    /// <summary>
    /// Gets or sets the _availableBytes value used by this component.
    /// </summary>
    private long _availableBytes;
    /// <summary>
    /// Gets or sets the _disposed value used by this component.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Executes the byte Budget operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the acquire Async operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the release operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the cancel Waiter operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private void CancelWaiter(BudgetWaiter waiter)
    {
        lock (_gate)
        {
            waiter.MarkCanceled();
        }
    }

    /// <summary>
    /// Executes the throw IfDisposed operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ByteBudget));
        }
    }

    /// <summary>
    /// Executes the dispose operation while preserving the component's benchmark or test-harness contract.
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
    /// Represents the budget Waiter class used by this benchmark or regression-gate component.
    /// </summary>
    private sealed class BudgetWaiter
    {
        /// <summary>
        /// Gets or sets the _completion value used by this component.
        /// </summary>
        private readonly TaskCompletionSource _completion;
        /// <summary>
        /// Gets or sets the _registration value used by this component.
        /// </summary>
        private CancellationTokenRegistration _registration;

        /// <summary>
        /// Executes the budget Waiter operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        internal BudgetWaiter(int requestedBytes)
        {
            RequestedBytes = requestedBytes;
            _completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>
        /// Gets or sets the requested Bytes value used by this component.
        /// </summary>
        internal int RequestedBytes { get; }
        /// <summary>
        /// Gets or sets the task value used by this component.
        /// </summary>
        internal Task Task => _completion.Task;
        /// <summary>
        /// Gets or sets the is Canceled value used by this component.
        /// </summary>
        internal bool IsCanceled { get; private set; }

        /// <summary>
        /// Executes the register Cancellation operation while preserving the component's benchmark or test-harness contract.
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
        /// Executes the mark Canceled operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        internal void MarkCanceled()
        {
            IsCanceled = true;
        }

        /// <summary>
        /// Executes the try SetAcquired operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        internal void TrySetAcquired()
        {
            _registration.Dispose();
            _completion.TrySetResult();
        }

        /// <summary>
        /// Executes the try SetCanceled operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        internal void TrySetCanceled()
        {
            IsCanceled = true;
            _registration.Dispose();
            _completion.TrySetCanceled();
        }

        /// <summary>
        /// Represents the cancellation State record struct used by this benchmark or regression-gate component.
        /// </summary>
        private readonly record struct CancellationState(ByteBudget Budget, BudgetWaiter Waiter);
    }
}
