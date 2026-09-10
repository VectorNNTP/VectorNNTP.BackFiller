// <copyright file="TestAcmeAccountKeyPathLockCoordinator.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Path-scoped synchronization coordinator for ACME account-key fixture materialization.

using System.Collections.Concurrent;
using System.Threading;

namespace VectorNNTP.BackFiller.Tests.TestInfrastructure.Certificates
{
    /// <summary>
    /// Coordinates path-scoped synchronization for ACME account-key fixture materialization.
    /// </summary>
    internal static class TestAcmeAccountKeyPathLockCoordinator
    {
        private static readonly ConcurrentDictionary<string, object> PathGates = new(StringComparer.Ordinal);
        private static readonly AsyncLocal<IAcquireObserver?> CurrentObserver = new();

        /// <summary>
        /// Computes the canonical destination account-key file path for a certificate directory.
        /// </summary>
        /// <param name="certificateDirectory">The target certificate directory.</param>
        /// <returns>The full canonical destination path for <c>account.key</c>.</returns>
        internal static string GetAccountKeyPath(string certificateDirectory)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(certificateDirectory);
            return Path.GetFullPath(Path.Combine(Path.GetFullPath(certificateDirectory), "account.key"));
        }

        /// <summary>
        /// Enters the synchronization gate for a destination account-key path.
        /// </summary>
        /// <param name="keyFilePath">The destination account-key path.</param>
        /// <returns>A disposable that exits the synchronization gate.</returns>
        internal static IDisposable Acquire(string keyFilePath)
        {
            string lockKey = GetPathLockKey(keyFilePath);
            object gate = PathGates.GetOrAdd(lockKey, static _ => new object());
            IAcquireObserver? observer = CurrentObserver.Value;

            observer?.OnAcquireAttempt(lockKey);
            Monitor.Enter(gate);
            observer?.OnEntered(lockKey);
            return new Releaser(gate, lockKey, observer);
        }

        /// <summary>
        /// Attempts to enter the synchronization gate for a destination account-key path without blocking.
        /// </summary>
        /// <param name="keyFilePath">The destination account-key path.</param>
        /// <param name="releaser">The releaser when acquisition succeeds; otherwise <c>null</c>.</param>
        /// <returns><c>true</c> when the gate was acquired; otherwise <c>false</c>.</returns>
        internal static bool TryAcquire(string keyFilePath, out IDisposable? releaser)
        {
            string lockKey = GetPathLockKey(keyFilePath);
            object gate = PathGates.GetOrAdd(lockKey, static _ => new object());
            IAcquireObserver? observer = CurrentObserver.Value;

            observer?.OnAcquireAttempt(lockKey);
            if (!Monitor.TryEnter(gate))
            {
                releaser = null;
                return false;
            }

            observer?.OnEntered(lockKey);
            releaser = new Releaser(gate, lockKey, observer);
            return true;
        }

        /// <summary>
        /// Begins an observation scope for lock acquisition events on the current async flow.
        /// </summary>
        /// <param name="observer">The observer to receive acquisition events.</param>
        /// <returns>A disposable scope that restores the previous observer.</returns>
        internal static IDisposable BeginObservation(IAcquireObserver observer)
        {
            ArgumentNullException.ThrowIfNull(observer);
            IAcquireObserver? previous = CurrentObserver.Value;
            CurrentObserver.Value = observer;
            return new ObservationScope(previous);
        }

        internal interface IAcquireObserver
        {
            void OnAcquireAttempt(string lockKey);

            void OnEntered(string lockKey);

            void OnExited(string lockKey);
        }

        private static string GetPathLockKey(string keyFilePath)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(keyFilePath);
            string fullPath = Path.GetFullPath(keyFilePath);
            return OperatingSystem.IsWindows()
                ? fullPath.ToUpperInvariant()
                : fullPath;
        }

        private sealed class Releaser(object gate, string lockKey, IAcquireObserver? observer) : IDisposable
        {
            private object? _gate = gate;

            public void Dispose()
            {
                object? localGate = Interlocked.Exchange(ref _gate, null);
                if (localGate is null)
                {
                    return;
                }

                try
                {
                    observer?.OnExited(lockKey);
                }
                finally
                {
                    Monitor.Exit(localGate);
                }
            }
        }

        private sealed class ObservationScope(IAcquireObserver? previous) : IDisposable
        {
            private IAcquireObserver? _previous = previous;

            public void Dispose()
            {
                IAcquireObserver? previousObserver = Interlocked.Exchange(ref _previous, null);
                CurrentObserver.Value = previousObserver;
            }
        }
    }
}
