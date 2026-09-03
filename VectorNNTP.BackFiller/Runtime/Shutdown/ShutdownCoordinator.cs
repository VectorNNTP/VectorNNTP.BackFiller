// <copyright file="ShutdownCoordinator.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: shutdown coordinator in the runtime shutdown subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// ShutdownCoordinator.cs -- Graceful/forced shutdown coordination state machine.
//
// Owns domain-specific shutdown escalation across worker components using two cancellation tokens:
//   - GracefulShutdownStartedToken: indicates cooperative shutdown should begin
//   - ForcedShutdownToken: indicates graceful budget expired or immediate forced shutdown
//
// Observability:
//   - Captures UTC and monotonic Stopwatch timestamps for graceful/forced transitions
//   - Captures shutdown reasons for both graceful initiation and forced escalation
//
// Architectural scope:
//   - This coordinator is an application-level shutdown state machine that operates inside the Generic Host lifecycle.
//   - It does not replace HostOptions.ShutdownTimeout or external supervisor deadlines.
//   - In this service, the coordinator grace-period timer intentionally shares the same configured duration as
//     HostOptions.ShutdownTimeout, but both timers remain independent.
//   - Generic Host controls StopAsync cancellation deadlines; systemd controls final process termination.
//
// Lifecycle model:
//   Running -> GracefulShutdown -> ForcedShutdown -> Completed
//
// Thread safety:
//   - State transitions are synchronized via _gate.
//   - Cancellation is published outside locks to avoid callback reentrancy deadlocks.
//   - Disposal races with late shutdown signals are treated as benign.

using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;

namespace VectorNNTP.Backfiller.Runtime.Shutdown
{
    /// <summary>
    /// Coordinates application-level graceful and forced shutdown signaling with explicit state transitions and cancellation tokens.
    /// </summary>
    /// <remarks>
    /// <para>State transitions are monotonic.</para>
    /// <para>Normal path:</para>
    /// <code>
    /// Running
    ///   |
    ///   +-- SignalGracefulShutdown()
    ///   v
    /// GracefulShutdown
    ///   |
    ///   +-- grace period expires
    ///   +-- SignalForcedShutdown()
    ///   v
    /// ForcedShutdown
    ///   |
    ///   +-- Dispose()
    ///   v
    /// Completed
    /// </code>
    /// <para>Immediate forced path:</para>
    /// <code>
    /// Running
    ///   |
    ///   +-- SignalForcedShutdown()
    ///   v
    /// ForcedShutdown
    ///   |
    ///   +-- Dispose()
    ///   v
    /// Completed
    /// </code>
    /// </remarks>
    internal sealed partial class ShutdownCoordinator : IDisposable
    {
        /// <summary>
        /// Represents the lifecycle state for shutdown coordination.
        /// </summary>
        internal enum ShutdownState
        {
            /// <summary>The service is running and has not started shutdown.</summary>
            Running,

            /// <summary>Graceful shutdown has started and forced escalation timer is active.</summary>
            GracefulShutdown,

            /// <summary>Forced shutdown has been signaled.</summary>
            ForcedShutdown,

            /// <summary>Coordinator resources have been released and subsequent shutdown signals are ignored.</summary>
            Completed,
        }

        /// <summary>
        /// Represents the cause for graceful or forced shutdown signaling.
        /// </summary>
        internal enum ShutdownReason
        {
            /// <summary>No explicit reason was provided.</summary>
            Unknown,

            /// <summary>Generic Host lifecycle is stopping.</summary>
            HostStopping,

            /// <summary>Operating system shutdown signal (for example SIGTERM or Ctrl+C) was received.</summary>
            OperatingSystemSignal,

            /// <summary>Shutdown was explicitly requested by an operator action.</summary>
            OperatorRequest,

            /// <summary>A mandatory dependency failed and required shutdown.</summary>
            DependencyFailure,

            /// <summary>A fatal application error triggered shutdown.</summary>
            FatalError,

            /// <summary>Graceful shutdown budget expired and forced escalation was triggered.</summary>
            GracePeriodExpired,
        }

        // Technical upper bound for CancellationTokenSource.CancelAfter(TimeSpan) on current runtime.
        // This is distinct from operational shutdown limits, which are enforced by ShutdownOptions validation.
        /// <summary>
        /// Largest delay that can be scheduled safely through <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/>.
        /// </summary>
        private static readonly TimeSpan MaximumGracePeriod = TimeSpan.FromMilliseconds(int.MaxValue);

        /// <summary>
        /// Serializes shutdown state transitions and associated timestamp/reason bookkeeping.
        /// </summary>
        private readonly object _gate = new();
        /// <summary>
        /// Cancellation source that notifies consumers when cooperative shutdown should begin.
        /// </summary>
        private readonly CancellationTokenSource _gracefulShutdownStartedCts = new();
        /// <summary>
        /// Cancellation source that notifies consumers when forced shutdown escalation has been reached.
        /// </summary>
        private readonly CancellationTokenSource _forcedShutdownCts = new();
        /// <summary>
        /// Logger receiving callback and disposal race diagnostics during shutdown signaling.
        /// </summary>
        private readonly ILogger<ShutdownCoordinator> _logger;

        /// <summary>
        /// Timer-backed cancellation source that escalates graceful shutdown to forced shutdown when it fires.
        /// </summary>
        private CancellationTokenSource? _gracePeriodCts;
        /// <summary>
        /// Current shutdown coordination state guarded by <see cref="_gate"/>.
        /// </summary>
        private ShutdownState _state = ShutdownState.Running;
        /// <summary>
        /// UTC timestamp recorded when graceful shutdown first began.
        /// </summary>
        private DateTimeOffset? _gracefulShutdownStartedAtUtc;
        /// <summary>
        /// UTC timestamp recorded when forced shutdown was signaled.
        /// </summary>
        private DateTimeOffset? _forcedShutdownAtUtc;
        /// <summary>
        /// Monotonic timestamp recorded when graceful shutdown first began.
        /// </summary>
        private long? _gracefulShutdownStartedTimestamp;
        /// <summary>
        /// Monotonic timestamp recorded when forced shutdown was signaled.
        /// </summary>
        private long? _forcedShutdownTimestamp;
        /// <summary>
        /// Reason captured for the transition that first initiated graceful shutdown.
        /// </summary>
        private ShutdownReason _gracefulShutdownReason = ShutdownReason.Unknown;
        /// <summary>
        /// Reason captured for the transition that placed the coordinator into forced shutdown.
        /// </summary>
        private ShutdownReason _forcedShutdownReason = ShutdownReason.Unknown;

        /// <summary>
        /// Initializes a new shutdown coordinator.
        /// </summary>
        internal ShutdownCoordinator()
            : this(NullLogger<ShutdownCoordinator>.Instance)
        {
        }

        /// <summary>
        /// Initializes a new shutdown coordinator with logger injection.
        /// </summary>
        /// <param name="logger">Logger for shutdown diagnostics.</param>
        public ShutdownCoordinator(ILogger<ShutdownCoordinator> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);
            _logger = logger;
        }

        /// <summary>
        /// Gets a token canceled when graceful shutdown starts.
        /// </summary>
        internal CancellationToken GracefulShutdownStartedToken => _gracefulShutdownStartedCts.Token;

        /// <summary>
        /// Gets a token canceled when forced shutdown escalation occurs.
        /// </summary>
        internal CancellationToken ForcedShutdownToken => _forcedShutdownCts.Token;

        /// <summary>
        /// Returns the current shutdown coordinator state.
        /// </summary>
        internal ShutdownState State
        {
            get
            {
                lock (_gate)
                {
                    return _state;
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the grace-period escalation timer has been canceled.
        /// </summary>
        internal bool IsGracePeriodEscalationCanceled
        {
            get
            {
                lock (_gate)
                {
                    return _gracePeriodCts?.IsCancellationRequested ?? false;
                }
            }
        }

        /// <summary>
        /// Returns the UTC timestamp when graceful shutdown started.
        /// </summary>
        internal DateTimeOffset? GracefulShutdownStartedAtUtc
        {
            get
            {
                lock (_gate)
                {
                    return _gracefulShutdownStartedAtUtc;
                }
            }
        }

        /// <summary>
        /// Returns the UTC timestamp when forced shutdown was signaled.
        /// </summary>
        internal DateTimeOffset? ForcedShutdownAtUtc
        {
            get
            {
                lock (_gate)
                {
                    return _forcedShutdownAtUtc;
                }
            }
        }

        /// <summary>
        /// Returns the monotonic timestamp when graceful shutdown started.
        /// </summary>
        internal long? GracefulShutdownStartedTimestamp
        {
            get
            {
                lock (_gate)
                {
                    return _gracefulShutdownStartedTimestamp;
                }
            }
        }

        /// <summary>
        /// Returns the monotonic timestamp when forced shutdown was signaled.
        /// </summary>
        internal long? ForcedShutdownTimestamp
        {
            get
            {
                lock (_gate)
                {
                    return _forcedShutdownTimestamp;
                }
            }
        }

        /// <summary>
        /// Returns the reason that initiated graceful shutdown.
        /// </summary>
        internal ShutdownReason GracefulShutdownReason
        {
            get
            {
                lock (_gate)
                {
                    return _gracefulShutdownReason;
                }
            }
        }

        /// <summary>
        /// Returns the reason that initiated forced shutdown.
        /// </summary>
        internal ShutdownReason ForcedShutdownReason
        {
            get
            {
                lock (_gate)
                {
                    return _forcedShutdownReason;
                }
            }
        }

        /// <summary>
        /// Returns the elapsed graceful-shutdown duration when both monotonic timestamps are available.
        /// </summary>
        internal TimeSpan? GracefulShutdownElapsed
        {
            get
            {
                lock (_gate)
                {
                    return _gracefulShutdownStartedTimestamp.HasValue && _forcedShutdownTimestamp.HasValue
                        ? Stopwatch.GetElapsedTime(_gracefulShutdownStartedTimestamp.Value, _forcedShutdownTimestamp.Value)
                        : null;
                }
            }
        }

        /// <summary>
        /// Signals graceful shutdown and schedules forced cancellation after the grace period.
        /// </summary>
        /// <param name="gracePeriod">Time to wait before forced escalation.</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="gracePeriod"/> is negative or exceeds the supported scheduler range.
        /// </exception>
        /// <exception cref="ObjectDisposedException">Thrown when coordinator has already completed disposal.</exception>
        internal void SignalGracefulShutdown(TimeSpan gracePeriod)
        {
            SignalGracefulShutdown(gracePeriod, ShutdownReason.Unknown);
        }

        /// <summary>
        /// Signals graceful shutdown with an explicit shutdown reason and schedules forced escalation.
        /// </summary>
        /// <param name="gracePeriod">Time to wait before forced escalation.</param>
        /// <param name="reason">Reason that initiated graceful shutdown signaling.</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="gracePeriod"/> is negative or exceeds the supported scheduler range.
        /// </exception>
        /// <exception cref="ObjectDisposedException">Thrown when coordinator has already completed disposal.</exception>
        internal void SignalGracefulShutdown(TimeSpan gracePeriod, ShutdownReason reason)
        {
            if (gracePeriod < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(gracePeriod), "Grace period cannot be negative.");
            }

            if (gracePeriod > MaximumGracePeriod)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(gracePeriod),
                    gracePeriod,
                    $"Grace period must be less than or equal to {MaximumGracePeriod}.");
            }

            lock (_gate)
            {
                if (_state != ShutdownState.Running)
                {
                    return;
                }

                ThrowIfDisposed();

                _state = ShutdownState.GracefulShutdown;
                _gracefulShutdownReason = reason;
                _gracefulShutdownStartedAtUtc = DateTimeOffset.UtcNow;
                _gracefulShutdownStartedTimestamp = Stopwatch.GetTimestamp();

                CancellationTokenSource timeoutCts = new();
                _gracePeriodCts = timeoutCts;

                _ = timeoutCts.Token.Register(
                    // Escalate to forced shutdown when the grace-period timer fires.
                    static state =>
                    {
                        ((ShutdownCoordinator)state!).SignalForcedShutdownFromGracePeriodTimer();
                    },
                    this,
                    useSynchronizationContext: false);

                timeoutCts.CancelAfter(gracePeriod);
            }

            // State and forced-escalation timer are established before publishing
            // shutdown-started cancellation to consumers.
            // Cancellation callbacks may execute synchronously.
            // Never invoke CancellationTokenSource.Cancel() while holding _gate.
            CancelTokenSourceSafely(_logger, _gracefulShutdownStartedCts, "graceful-shutdown-started", ShutdownState.GracefulShutdown);
        }

        /// <summary>
        /// Signals immediate forced shutdown cancellation.
        /// </summary>
        /// <remarks>
        /// Forced shutdown also implies shutdown has started. If forced shutdown is signaled directly from
        /// <see cref="ShutdownState.Running"/>, this method first cancels <see cref="GracefulShutdownStartedToken"/>
        /// and then cancels <see cref="ForcedShutdownToken"/>.
        ///
        /// Unlike <see cref="SignalGracefulShutdown(TimeSpan)"/>, this method does not throw after disposal.
        /// Late forced-shutdown signals can race with disposal from the grace-period timer callback; those
        /// races are benign and are intentionally ignored when state is <see cref="ShutdownState.Completed"/>.
        /// </remarks>
        internal void SignalForcedShutdown()
        {
            SignalForcedShutdown(ShutdownReason.Unknown);
        }

        /// <summary>
        /// Signals immediate forced shutdown cancellation with an explicit reason.
        /// </summary>
        /// <param name="reason">Reason that initiated forced shutdown signaling.</param>
        internal void SignalForcedShutdown(ShutdownReason reason)
        {
            SignalForcedShutdownCore(cancelGracePeriodEscalation: true, reason);
        }

        /// <summary>
        /// Escalates shutdown to forced mode from the grace-period timer callback.
        /// </summary>
        /// <remarks>
        /// This path preserves the timer signal semantics by not canceling the grace-period timer source again.
        /// </remarks>
        private void SignalForcedShutdownFromGracePeriodTimer()
        {
            SignalForcedShutdownCore(cancelGracePeriodEscalation: false, ShutdownReason.GracePeriodExpired);
        }

        /// <summary>
        /// Transitions to forced shutdown and emits cancellation signals in a race-safe order.
        /// </summary>
        /// <param name="cancelGracePeriodEscalation">
        /// <see langword="true"/> when forced shutdown is user-initiated and the grace-period escalation timer
        /// should be canceled; otherwise <see langword="false"/> for timer-driven escalation.
        /// </param>
        /// <param name="reason">Reason that initiated forced shutdown signaling.</param>
        private void SignalForcedShutdownCore(bool cancelGracePeriodEscalation, ShutdownReason reason)
        {
            bool shouldCancelGraceful;
            CancellationTokenSource? gracePeriodCtsToCancel;

            lock (_gate)
            {
                if (_state is ShutdownState.ForcedShutdown or ShutdownState.Completed)
                {
                    return;
                }

                shouldCancelGraceful = _state == ShutdownState.Running;
                gracePeriodCtsToCancel = cancelGracePeriodEscalation ? _gracePeriodCts : null;

                if (cancelGracePeriodEscalation)
                {
                    _gracePeriodCts = null;
                }

                _state = ShutdownState.ForcedShutdown;
                _forcedShutdownReason = reason;
                _forcedShutdownAtUtc = DateTimeOffset.UtcNow;
                _forcedShutdownTimestamp = Stopwatch.GetTimestamp();

                _gracefulShutdownStartedAtUtc ??= _forcedShutdownAtUtc;

                if (!_gracefulShutdownStartedTimestamp.HasValue)
                {
                    _gracefulShutdownStartedTimestamp = _forcedShutdownTimestamp;
                }
            }

            if (gracePeriodCtsToCancel is not null)
            {
                CancelGracePeriodTimerSafely(gracePeriodCtsToCancel);
                DisposeCancellationTokenSourceSafely(_logger, gracePeriodCtsToCancel, "grace-period");
            }

            if (shouldCancelGraceful)
            {
                CancelTokenSourceSafely(_logger, _gracefulShutdownStartedCts, "graceful-shutdown-started", ShutdownState.ForcedShutdown);
            }

            CancelTokenSourceSafely(_logger, _forcedShutdownCts, "forced-shutdown", ShutdownState.ForcedShutdown);
        }

        /// <summary>
        /// Releases resources and marks the coordinator completed.
        /// </summary>
        public void Dispose()
        {
            CancellationTokenSource? gracePeriodCtsToDispose;
            bool shouldDisposeGracefulShutdownStarted;
            bool shouldDisposeForcedShutdown;

            lock (_gate)
            {
                if (_state == ShutdownState.Completed)
                {
                    return;
                }

                gracePeriodCtsToDispose = _gracePeriodCts;
                _gracePeriodCts = null;
                shouldDisposeGracefulShutdownStarted = _state == ShutdownState.Running;
                shouldDisposeForcedShutdown = _state is ShutdownState.Running or ShutdownState.GracefulShutdown;
                _state = ShutdownState.Completed;
            }

            if (gracePeriodCtsToDispose is not null)
            {
                CancelGracePeriodTimerSafely(gracePeriodCtsToDispose);
                DisposeCancellationTokenSourceSafely(_logger, gracePeriodCtsToDispose, "grace-period");
            }

            if (shouldDisposeGracefulShutdownStarted)
            {
                CancelTokenSourceSafely(_logger, _gracefulShutdownStartedCts, "graceful-shutdown-started", ShutdownState.Completed);
            }

            if (shouldDisposeForcedShutdown)
            {
                CancelTokenSourceSafely(_logger, _forcedShutdownCts, "forced-shutdown", ShutdownState.Completed);
            }

            DisposeCancellationTokenSourceSafely(_logger, _gracefulShutdownStartedCts, "graceful-shutdown-started");
            DisposeCancellationTokenSourceSafely(_logger, _forcedShutdownCts, "forced-shutdown");
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Throws when the coordinator has already completed disposal and can no longer accept graceful-shutdown requests.
        /// </summary>
        private void ThrowIfDisposed()
        {
            ObjectDisposedException.ThrowIf(_state == ShutdownState.Completed, this);
        }

        /// <summary>
        /// Cancels the grace-period escalation timer while tolerating timer and disposal races.
        /// </summary>
        /// <param name="gracePeriodCts">Timer cancellation source to cancel.</param>
        private static void CancelGracePeriodTimerSafely(CancellationTokenSource gracePeriodCts)
        {
            try
            {
                gracePeriodCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Benign race: timer callback/disposal may have already completed.
            }
        }

        /// <summary>
        /// Cancels one coordinator-owned token source and records callback or disposal races for diagnostics.
        /// </summary>
        /// <param name="logger">Logger receiving callback or disposal race diagnostics.</param>
        /// <param name="cancellationTokenSource">Cancellation source to cancel.</param>
        /// <param name="name">Logical name of the source used in diagnostics.</param>
        /// <param name="state">Coordinator state associated with the cancellation attempt.</param>
        private static void CancelTokenSourceSafely(ILogger logger, CancellationTokenSource cancellationTokenSource, string name, ShutdownState state)
        {
            try
            {
                cancellationTokenSource.Cancel();
            }
            catch (AggregateException ex)
            {
                LogCancellationCallbackFailed(logger, ex, name, state);
            }
            catch (ObjectDisposedException ex)
            {
                LogCancellationSkippedAlreadyDisposed(logger, ex, name, state);
            }
        }

        /// <summary>
        /// Disposes one coordinator-owned token source while tolerating duplicate-disposal races.
        /// </summary>
        /// <param name="logger">Logger receiving disposal-race diagnostics.</param>
        /// <param name="cancellationTokenSource">Cancellation source to dispose.</param>
        /// <param name="name">Logical name of the source used in diagnostics.</param>
        private static void DisposeCancellationTokenSourceSafely(ILogger logger, CancellationTokenSource cancellationTokenSource, string name)
        {
            try
            {
                cancellationTokenSource.Dispose();
            }
            catch (ObjectDisposedException ex)
            {
                LogDisposalSkippedAlreadyDisposed(logger, ex, name);
            }
        }

        /// <summary>
        /// Logs that one cancellation callback threw while a coordinator token source was being canceled.
        /// </summary>
        /// <param name="logger">Logger receiving the callback-failure event.</param>
        /// <param name="exception">Exception recorded from the callback aggregate.</param>
        /// <param name="cancellationTokenSourceName">Logical name of the token source being canceled.</param>
        /// <param name="shutdownState">Coordinator state associated with the cancellation attempt.</param>
        [LoggerMessage(
            EventId = 1200,
            Level = LogLevel.Debug,
            Message = "{CancellationTokenSourceName} cancellation callback threw during shutdown signaling (state={ShutdownState}).")]
        private static partial void LogCancellationCallbackFailed(
            ILogger logger,
            Exception exception,
            string cancellationTokenSourceName,
            ShutdownState shutdownState);

        /// <summary>
        /// Logs that cancellation raced with disposal and was skipped because the target token source was already disposed.
        /// </summary>
        /// <param name="logger">Logger receiving the skipped-cancellation event.</param>
        /// <param name="exception">Disposed-object exception recorded for diagnostics.</param>
        /// <param name="cancellationTokenSourceName">Logical name of the token source that had already been disposed.</param>
        /// <param name="shutdownState">Coordinator state associated with the skipped cancellation.</param>
        [LoggerMessage(
            EventId = 1201,
            Level = LogLevel.Debug,
            Message = "{CancellationTokenSourceName} cancellation skipped because the coordinator is already disposed (state={ShutdownState}).")]
        private static partial void LogCancellationSkippedAlreadyDisposed(
            ILogger logger,
            Exception exception,
            string cancellationTokenSourceName,
            ShutdownState shutdownState);

        /// <summary>
        /// Logs that token-source disposal was attempted after another path had already disposed the same instance.
        /// </summary>
        /// <param name="logger">Logger receiving the duplicate-disposal event.</param>
        /// <param name="exception">Disposed-object exception recorded for diagnostics.</param>
        /// <param name="cancellationTokenSourceName">Logical name of the token source involved in the race.</param>
        [LoggerMessage(
            EventId = 1202,
            Level = LogLevel.Debug,
            Message = "{CancellationTokenSourceName} disposal skipped because it was already disposed.")]
        private static partial void LogDisposalSkippedAlreadyDisposed(
            ILogger logger,
            Exception exception,
            string cancellationTokenSourceName);
    }
}
