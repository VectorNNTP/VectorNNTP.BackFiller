// <copyright file="ServiceLifecycle.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: service lifecycle in the runtime lifecycle subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// ServiceLifecycle.cs -- Explicit application readiness state machine.
//
// Tracks the application lifecycle through distinct phases, enabling systemd, load balancers,
// and health checks to distinguish "process running" from "service ready to accept work".
//
// State machine:
//   Starting → Validating → Initializing → Ready ↔ Draining → Stopped
//        ↓          ↓             ↓                      ↓
//                    Faulted ←─────────────────────────┘
//
// Key properties:
//   - Strict transition validation: only explicitly defined transitions are permitted
//   - Startup progression: Starting → Validating → Initializing → Ready (monotonic, no reversals)
//   - Bidirectional draining: Ready ↔ Draining (supports graceful shutdown cancellation)
//   - Failure isolation: Faulted is terminal; distinguishes "could not operate" from "orderly shutdown"
//   - Transition events: subscribers notified on every state change (with exception isolation)
//   - Timing metadata: tracks time in each state and transition history
//   - Thread-safe: lock protects all state access (transitions are rare; performance irrelevant)
//   - Centralized: single entry point (TransitionTo) for state changes
//   - Authoritative: state transitions cannot be broken by observer failures
//   - Monotonic timing: durations use TimeProvider for resilience to NTP/VM clock adjustments
//
// Observer Resilience:
// Subscribers are invoked individually with exception isolation. If one subscriber throws,
// the transition completes successfully, the failure is logged, and remaining subscribers
// still execute. State transitions are logged before notifying observers, ensuring the
// authoritative transition record exists even if all subscribers fail.
//
// Observer Contract (CRITICAL):
// Subscribers MUST NOT call TransitionTo() from within StateTransitioned event handlers.
// This creates reentrancy and state confusion:
//   - Outer transition logs "Ready" and starts notifying subscribers
//   - Subscriber calls TransitionTo(Draining), state changes mid-notification
//   - Remaining subscribers observe "Ready" transition but state is already "Draining"
//
// TransitionTo() is internal; only application infrastructure controls lifecycle:
//   - Startup validation (Starting → Validating → Initializing → Ready or Faulted)
//   - Shutdown coordinator (Ready → Draining → Stopped or Faulted)
//   - Configuration reload (Draining → Ready if cancellation requested)
//
// Observers (systemd notifier, health checks, metrics) can only READ state, not CONTROL it.
//
// Timing Model:
// Each transition records both:
//   - Wall-clock UTC timestamp (for human-readable event history)
//   - Monotonic elapsed duration (immune to NTP corrections, VM clock adjustments, system time changes)
//
// Durations are calculated using TimeProvider.GetTimestamp() for monotonic accuracy.
// This prevents LogSlowPhaseWarning from reporting negative or wildly incorrect durations
// when the system clock changes.
//
// Example Transitions:
//
// Normal startup:
//   Starting → Validating → Initializing → Ready
//
// Startup failure (validation error):
//   Starting → Validating → Faulted
//
// Initialization failure (database unreachable):
//   Starting → Validating → Initializing → Faulted
//
// Graceful shutdown:
//   Ready → Draining → Stopped
//
// Shutdown cancellation (application-controlled drain reversal):
//   Ready → Draining → Ready
//     ↑                  ↑
//  drain initiated   drain cancelled
//  (maintenance mode, orchestration layer, or application-level signal)
//
// Drain failure (inflight work timeout):
//   Ready → Draining → Faulted
//
// Terminal State Semantics:
//   Stopped = Orderly shutdown (SIGTERM, graceful drain completed)
//   Faulted = Service could not operate (config invalid, DB unreachable, drain timeout)
//
// This distinction is critical for:
//   - systemd restart policies (Restart=on-failure should trigger for Faulted, not Stopped)
//   - Health check endpoints (Faulted → HTTP 503, Stopped → HTTP 503 but different semantics)
//   - Operational telemetry (Faulted preserves failure reason in transition history)
//
// This bidirectional Ready ↔ Draining transition supports scenarios where an application-controlled
// drain is initiated but then cancelled before work completes (e.g., maintenance mode toggled off,
// blue/green deployment rolled back, or operator intervention). This is distinct from standard
// systemd SIGTERM shutdown, which is typically one-way and leads to process termination.

using Microsoft.Extensions.Logging.Abstractions;

namespace VectorNNTP.Backfiller.Runtime.Lifecycle
{
    /// <summary>
    /// Application lifecycle state machine for readiness tracking.
    /// </summary>
    /// <remarks>
    /// <para><b>Purpose:</b> Provide explicit, observable application readiness state
    /// consumed by systemd (Type=notify), health endpoints, and load balancers to distinguish
    /// "process exists" from "application ready to process work".</para>
    ///
    /// <para><b>State flow:</b> Starting → Validating → Initializing → Ready (↔ Draining) → Stopped</para>
    ///
    /// <para><b>Failure path:</b> Starting/Validating/Initializing/Draining may transition to Faulted
    /// when startup validation, initialization, or graceful drain fails. Faulted is a terminal state
    /// that preserves diagnostic information (vs. Stopped which indicates orderly shutdown).</para>
    ///
    /// <para><b>Strict transitions:</b> Only explicitly defined transitions are permitted. Startup progresses
    /// monotonically (Starting → Validating → Initializing → Ready, no reversals). Ready and Draining may
    /// transition bidirectionally (Ready ↔ Draining) to support application-controlled drain cancellation
    /// (e.g., maintenance mode toggle, orchestration rollback). All transitions are validated and logged.</para>
    ///
    /// <para><b>Events:</b> Subscribers can hook StateTransitioned to monitor state changes for
    /// systemd notifications, health checks, logging, etc.</para>
    ///
    /// <para><b>CRITICAL - Observer contract:</b> Event subscribers MUST NOT call TransitionTo() from
    /// within StateTransitioned handlers. This creates reentrancy, state confusion, and potential deadlocks.
    /// TransitionTo() is internal; only application infrastructure (startup validation, shutdown coordinator)
    /// controls lifecycle state. Observers can only READ state via CurrentState/IsTerminal/IsFaulted.</para>
    ///
    /// <para><b>Thread-safety:</b> All state access is protected by a lock. Health checks, systemd notifications,
    /// shutdown handling, and metrics can safely read state concurrently with transitions. Lock contention is
    /// irrelevant because transitions are extremely rare (startup + shutdown only).</para>
    ///
    /// <para><b>Monotonic timing:</b> Durations use TimeProvider.GetTimestamp() for monotonic measurements
    /// immune to NTP corrections, VM clock adjustments, and system time changes. UTC wall-clock timestamps
    /// are still recorded for human-readable event history.</para>
    /// </remarks>
    public partial class ServiceLifecycle
    {
        /// <summary>
        /// Application lifecycle states.
        /// </summary>
        public enum LifecycleState
        {
            /// <summary>Process starting, bootstrap logger and exception handlers configured.</summary>
            Starting,

            /// <summary>Validating configuration and dependencies.</summary>
            Validating,

            /// <summary>Initializing DI container and hosted services.</summary>
            Initializing,

            /// <summary>Ready to accept work.</summary>
            Ready,

            /// <summary>Draining in-flight work; no new work accepted.</summary>
            Draining,

            /// <summary>Stopped; orderly shutdown (SIGTERM, graceful drain completed).</summary>
            Stopped,

            /// <summary>Faulted; service could not operate (config invalid, dependency unreachable, drain timeout). Terminal state.</summary>
            Faulted
        }

        /// <summary>
        /// Represents a state transition event.
        /// </summary>
        /// <param name="FromState">State transitioning from.</param>
        /// <param name="ToState">State transitioning to.</param>
        /// <param name="Timestamp">Wall-clock UTC timestamp (human-readable).</param>
        /// <param name="Reason">Human-readable reason for transition.</param>
        /// <param name="DurationInPreviousState">Monotonic elapsed time in previous state (immune to clock adjustments).</param>
        public record StateTransition(
            LifecycleState FromState,
            LifecycleState ToState,
            DateTime Timestamp,
            string Reason,
            TimeSpan DurationInPreviousState);

        /// <summary>
        /// Fired when state transitions.
        /// </summary>
        public event Action<StateTransition>? StateTransitioned;

        /// <summary>
        /// Current lifecycle state (thread-safe).
        /// </summary>
        public LifecycleState CurrentState
        {
            get
            {
                lock (_sync)
                {
                    return _currentState;
                }
            }
        }

        /// <summary>
        /// Whether the service is in a terminal state (Stopped or Faulted).
        /// </summary>
        /// <remarks>
        /// Useful for health checks and shutdown coordination. Terminal states cannot transition further.
        /// </remarks>
        public bool IsTerminal
        {
            get
            {
                lock (_sync)
                {
                    return _currentState is LifecycleState.Stopped or LifecycleState.Faulted;
                }
            }
        }

        /// <summary>
        /// Whether the service faulted (could not operate correctly).
        /// </summary>
        /// <remarks>
        /// Faulted indicates startup validation failure, initialization failure, or drain timeout.
        /// Distinguishes failure from orderly shutdown (Stopped).
        /// </remarks>
        public bool IsFaulted
        {
            get
            {
                lock (_sync)
                {
                    return _currentState == LifecycleState.Faulted;
                }
            }
        }

        /// <summary>
        /// Time elapsed in current state (thread-safe snapshot, monotonic).
        /// </summary>
        /// <remarks>
        /// Uses monotonic clock (TimeProvider.GetTimestamp) immune to NTP corrections
        /// and VM clock adjustments.
        /// </remarks>
        public TimeSpan TimeInCurrentState
        {
            get
            {
                lock (_sync)
                {
                    return _timeProvider.GetElapsedTime(_stateEnteredTimestamp);
                }
            }
        }

        /// <summary>
        /// Transition history snapshot (last N transitions, thread-safe).
        /// </summary>
        /// <remarks>
        /// Returns a snapshot array to prevent enumeration races with concurrent transitions.
        /// Allocation is irrelevant (max 50 entries, read rarely).
        /// </remarks>
        public IReadOnlyList<StateTransition> TransitionHistory
        {
            get
            {
                lock (_sync)
                {
                    return [.. _transitionHistory];
                }
            }
        }

        /// <summary>
        /// Stores the sync state used to enforce this component's runtime contract.
        /// </summary>
        private readonly object _sync = new();
        /// <summary>
        /// Stores the time provider state used to enforce this component's runtime contract.
        /// </summary>
        private readonly TimeProvider _timeProvider;
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<ServiceLifecycle> _logger;
        /// <summary>
        /// Stores the current state state used to enforce this component's runtime contract.
        /// </summary>
        private LifecycleState _currentState = LifecycleState.Starting;
        /// <summary>
        /// Stores the state entered timestamp state used to enforce this component's runtime contract.
        /// </summary>
        private long _stateEnteredTimestamp; // Monotonic timestamp from TimeProvider.GetTimestamp()
        /// <summary>
        /// Stores the transition history state used to enforce this component's runtime contract.
        /// </summary>
        private readonly List<StateTransition> _transitionHistory = [];
        /// <summary>
        /// Stores the max history size state used to enforce this component's runtime contract.
        /// </summary>
        private const int MaxHistorySize = 50;
        /// <summary>
        /// Stores the is notifying observers state used to enforce this component's runtime contract.
        /// </summary>
        private bool _isNotifyingObservers; // Prevents transitions during observer notification (reentrancy + concurrency)
        /// <summary>
        /// Stores the slow phase warning logged state used to enforce this component's runtime contract.
        /// </summary>
        private bool _slowPhaseWarningLogged; // One warning per phase

        /// <summary>
        /// Initializes a new ServiceLifecycle with optional TimeProvider for testability.
        /// </summary>
        /// <param name="timeProvider">Time provider for monotonic timestamps (defaults to TimeProvider.System).</param>
        public ServiceLifecycle(TimeProvider? timeProvider = null)
            : this(NullLogger<ServiceLifecycle>.Instance, timeProvider)
        {
        }

        /// <summary>
        /// Initializes a new ServiceLifecycle with logger and optional TimeProvider for testability.
        /// </summary>
        /// <param name="logger">Logger for lifecycle transition diagnostics.</param>
        /// <param name="timeProvider">Time provider for monotonic timestamps (defaults to TimeProvider.System).</param>
        public ServiceLifecycle(ILogger<ServiceLifecycle> logger, TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(logger);

            _logger = logger;
            _timeProvider = timeProvider ?? TimeProvider.System;
            _stateEnteredTimestamp = _timeProvider.GetTimestamp();
        }

        /// <summary>
        /// Transitions to a new state (with strict validation, thread-safe). Internal API.
        /// </summary>
        /// <remarks>
        /// <para>Validates that the transition is explicitly permitted (startup monotonic, Ready ↔ Draining bidirectional).
        /// If valid, updates state, records transition, logs, then notifies observers.</para>
        /// <para>Throws InvalidOperationException if transition is invalid.</para>
        /// <para>Thread-safe: multiple threads can safely call this, though in practice
        /// transitions happen sequentially during startup/shutdown.</para>
        /// <para><b>Observer resilience:</b> State transitions are authoritative and cannot be
        /// broken by subscriber exceptions. Each subscriber is invoked individually with exception
        /// isolation—if one throws, others still execute and the transition completes successfully.</para>
        /// <para><b>WARNING:</b> Do NOT call this from StateTransitioned event handlers. This is internal
        /// API restricted to application infrastructure (startup validation, shutdown coordinator). Observers
        /// can only READ state (CurrentState/IsTerminal/IsFaulted), not CONTROL it. Reentrancy from event
        /// handlers creates state confusion where transition logs/notifications become inconsistent with
        /// actual state.</para>
        /// </remarks>
        internal void TransitionTo(LifecycleState newState, string reason)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(reason);

            StateTransition transition;
            Delegate[]? subscribers;

            lock (_sync)
            {
                // Prevent transitions during observer notification (covers reentrancy and concurrent attempts)
                if (_isNotifyingObservers)
                {
                    string message = $"TransitionTo({newState}) cannot be called while observers are being notified. " +
                                   "Observers MUST NOT call TransitionTo() from StateTransitioned handlers (reentrancy forbidden). " +
                                   "Concurrent transitions from other threads are also blocked during notification.";
                    LogTransitionDuringObserverNotificationRejected(_logger, newState);
                    throw new InvalidOperationException(message);
                }

                _isNotifyingObservers = true;

                // Prevent no-op transitions
                if (newState == _currentState)
                {
                    _isNotifyingObservers = false;
                    LogStateTransitionIgnoredAlreadyInState(_logger, _currentState);
                    return;
                }

                // Validate transition is allowed
                if (!IsValidTransition(_currentState, newState))
                {
                    _isNotifyingObservers = false;
                    string message = $"Invalid state transition: {_currentState} -> {newState} (allowed: {GetValidTransitionsString(_currentState)})";
                    LogInvalidStateTransition(_logger, _currentState, newState, GetValidTransitionsString(_currentState));
                    throw new InvalidOperationException(message);
                }

                // Record transition with monotonic duration and wall-clock timestamp
                long nowTimestamp = _timeProvider.GetTimestamp();
                TimeSpan durationInPreviousState = _timeProvider.GetElapsedTime(_stateEnteredTimestamp, nowTimestamp);
                transition = new(
                    FromState: _currentState,
                    ToState: newState,
                    Timestamp: _timeProvider.GetUtcNow().UtcDateTime, // Wall-clock for human-readable history
                    Reason: reason,
                    DurationInPreviousState: durationInPreviousState);

                // Update state (authoritative; cannot be rolled back by observer failures)
                _currentState = newState;
                _stateEnteredTimestamp = nowTimestamp; // Store monotonic timestamp
                _slowPhaseWarningLogged = false; // Reset warning flag for new state

                // Add to history (with max size limit)
                _transitionHistory.Add(transition);
                if (_transitionHistory.Count > MaxHistorySize)
                {
                    _transitionHistory.RemoveAt(0);
                }

                // Capture subscriber list while locked (GetInvocationList() requires non-null delegate)
                subscribers = StateTransitioned?.GetInvocationList();

                // Keep _isNotifyingObservers = true during subscriber invocation
                // (prevents both reentrancy and concurrent transitions)
            }

            try
            {
                // Log transition immediately after state update (before notifying observers)
                // Ensures transition is always logged even if all subscribers fail
                LogStateTransition(
                    _logger,
                    transition.FromState,
                    transition.ToState,
                    reason,
                    transition.DurationInPreviousState.TotalSeconds);

                // Notify subscribers outside lock with exception isolation
                // State machine is authoritative; observer failures cannot break transitions
                if (subscribers is not null)
                {
                    foreach (Delegate handler in subscribers)
                    {
                        try
                        {
                            ((Action<StateTransition>)handler)(transition);
                        }
                        catch (Exception ex)
                        {
                            LogLifecycleTransitionSubscriberFailed(
                                _logger,
                                ex,
                                transition.FromState,
                                transition.ToState);
                        }
                    }
                }
            }
            finally
            {
                // Reset flag after all subscribers complete (or fail)
                lock (_sync)
                {
                    _isNotifyingObservers = false;
                }
            }
        }

        /// <summary>
        /// Checks if a transition is explicitly permitted by the state machine.
        /// </summary>
        /// <remarks>
        /// State machine is visually represented in switch expression for clarity.
        /// Startup progresses monotonically; Ready ↔ Draining is bidirectional; terminal states reject all transitions.
        /// </remarks>
        private static bool IsValidTransition(LifecycleState from, LifecycleState to)
        {
            return from switch
            {
                LifecycleState.Starting =>
                    to is LifecycleState.Validating or LifecycleState.Faulted,

                LifecycleState.Validating =>
                    to is LifecycleState.Initializing or LifecycleState.Faulted,

                LifecycleState.Initializing =>
                    to is LifecycleState.Ready
                       or LifecycleState.Draining
                       or LifecycleState.Faulted,

                LifecycleState.Ready =>
                    to is LifecycleState.Draining,

                LifecycleState.Draining =>
                    to is LifecycleState.Ready
                       or LifecycleState.Stopped
                       or LifecycleState.Faulted,

                LifecycleState.Stopped => false,  // Terminal
                LifecycleState.Faulted => false,  // Terminal

                _ => false  // Unknown state
            };
        }

        /// <summary>
        /// Gets readable list of valid next states for error messages.
        /// </summary>
        private static string GetValidTransitionsString(LifecycleState state)
        {
            return state switch
            {
                LifecycleState.Starting => "Validating, Faulted",
                LifecycleState.Validating => "Initializing, Faulted",
                LifecycleState.Initializing => "Ready, Draining, Faulted",
                LifecycleState.Ready => "Draining",
                LifecycleState.Draining => "Ready, Stopped, Faulted",
                LifecycleState.Stopped => "none (terminal)",
                LifecycleState.Faulted => "none (terminal)",
                _ => "none (unknown state)"
            };
        }

        /// <summary>
        /// Logs a warning if time in current state exceeds threshold (thread-safe, monotonic, once per phase).
        /// </summary>
        /// <remarks>
        /// <para>Called periodically to detect phases that are taking longer than expected.
        /// Helps identify stalled initialization or validation.</para>
        /// <para>Uses monotonic timing immune to NTP corrections and VM clock adjustments.</para>
        /// <para><b>Once-per-phase:</b> Only logs the first time threshold is exceeded in each state.
        /// Prevents log spam during stuck initialization (e.g., 600 warnings if called every second for 10 minutes).
        /// The eventual state transition log will show final duration.</para>
        /// </remarks>
        public void LogSlowPhaseWarning(TimeSpan threshold, string phaseName)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(threshold, TimeSpan.Zero);
            ArgumentException.ThrowIfNullOrWhiteSpace(phaseName);

            TimeSpan elapsed;
            bool shouldLog;

            lock (_sync)
            {
                elapsed = _timeProvider.GetElapsedTime(_stateEnteredTimestamp);
                shouldLog = elapsed > threshold && !_slowPhaseWarningLogged;

                if (shouldLog)
                {
                    _slowPhaseWarningLogged = true;
                }
            }

            if (shouldLog)
            {
                LogSlowPhaseWarningExceeded(
                    _logger,
                    phaseName,
                    elapsed.TotalSeconds,
                    threshold.TotalSeconds);
            }
        }

        /// <summary>
        /// Gets a summary of current state (thread-safe snapshot, monotonic timing).
        /// </summary>
        public string GetSummary()
        {
            lock (_sync)
            {
                TimeSpan elapsed = _timeProvider.GetElapsedTime(_stateEnteredTimestamp);
                return $"State: {_currentState}; Time: {elapsed.TotalSeconds:F2}s; History: {_transitionHistory.Count} transitions";
            }
        }

        [LoggerMessage(
            EventId = 1100,
            Level = LogLevel.Error,
            Message = "TransitionTo({NewState}) cannot be called while observers are being notified. Observers MUST NOT call TransitionTo() from StateTransitioned handlers (reentrancy forbidden). Concurrent transitions from other threads are also blocked during notification.")]
        /// <summary>
        /// Performs the log transition during observer notification rejected operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogTransitionDuringObserverNotificationRejected(
            ILogger logger,
            LifecycleState newState);

        [LoggerMessage(
            EventId = 1101,
            Level = LogLevel.Warning,
            Message = "State transition ignored: already in {State}")]
        /// <summary>
        /// Performs the log state transition ignored already in state operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogStateTransitionIgnoredAlreadyInState(
            ILogger logger,
            LifecycleState state);

        [LoggerMessage(
            EventId = 1102,
            Level = LogLevel.Error,
            Message = "Invalid state transition: {CurrentState} -> {TargetState} (allowed: {AllowedTransitions})")]
        /// <summary>
        /// Performs the log invalid state transition operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogInvalidStateTransition(
            ILogger logger,
            LifecycleState currentState,
            LifecycleState targetState,
            string allowedTransitions);

        [LoggerMessage(
            EventId = 1103,
            Level = LogLevel.Information,
            Message = "State transition: {From} -> {To} (reason: {Reason}; elapsed={Elapsed:F2}s)")]
        /// <summary>
        /// Performs the log state transition operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogStateTransition(
            ILogger logger,
            LifecycleState from,
            LifecycleState to,
            string reason,
            double elapsed);

        [LoggerMessage(
            EventId = 1104,
            Level = LogLevel.Error,
            Message = "Lifecycle transition subscriber failed for {From} -> {To} (transition completed successfully)")]
        /// <summary>
        /// Performs the log lifecycle transition subscriber failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogLifecycleTransitionSubscriberFailed(
            ILogger logger,
            Exception exception,
            LifecycleState from,
            LifecycleState to);

        [LoggerMessage(
            EventId = 1105,
            Level = LogLevel.Warning,
            Message = "Slow phase warning: {Phase} has taken {Elapsed:F2}s (threshold: {Threshold:F2}s)")]
        /// <summary>
        /// Performs the log slow phase warning exceeded operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogSlowPhaseWarningExceeded(
            ILogger logger,
            string phase,
            double elapsed,
            double threshold);
    }
}
