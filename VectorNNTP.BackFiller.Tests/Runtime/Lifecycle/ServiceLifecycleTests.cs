// <copyright file="ServiceLifecycleTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for service lifecycle, covering service lifecycle and shutdown contracts.
// Primary responsibility: documents the executable contracts covered by the service lifecycle test suite.

using System.Collections.Concurrent;
using VectorNNTP.Backfiller.Runtime.Lifecycle;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Lifecycle
{
    /// <summary>
    /// Tests for ServiceLifecycle state machine invariants.
    /// </summary>
    /// <remarks>
    /// <para>Validates state transition rules, terminal state behavior, and timing/history tracking.</para>
    /// <para>Critical invariants tested:</para>
    /// <list type="bullet">
    /// <item><description>Terminal states (Stopped, Faulted) reject all transitions</description></item>
    /// <item><description>Startup progression is monotonic (no reversals)</description></item>
    /// <item><description>Ready ↔ Draining is bidirectional (application-controlled drain)</description></item>
    /// <item><description>Transition validation enforces explicit state machine rules</description></item>
    /// </list>
    /// </remarks>
    public class ServiceLifecycleTests
    {
        #region Terminal State Invariants

        /// <summary>
        /// Verifies that Stopped (terminal state) rejects all transition attempts.
        /// </summary>
        /// <remarks>
        /// Stopped represents orderly shutdown. Once reached, the service lifecycle is complete.
        /// No state transitions should be permitted from Stopped (including self-transitions).
        /// </remarks>
        /// <summary>
        /// Confirms the stopped rejects all transitions behavior.
        /// </summary>
        /// <param name="targetState">The target state used by this test scenario.</param>
        [Theory]
        [InlineData(ServiceLifecycle.LifecycleState.Starting)]
        [InlineData(ServiceLifecycle.LifecycleState.Validating)]
        [InlineData(ServiceLifecycle.LifecycleState.Initializing)]
        [InlineData(ServiceLifecycle.LifecycleState.Ready)]
        [InlineData(ServiceLifecycle.LifecycleState.Draining)]
        [InlineData(ServiceLifecycle.LifecycleState.Faulted)]
        public void Stopped_RejectsAllTransitions(ServiceLifecycle.LifecycleState targetState)
        {
            // Arrange: Create lifecycle and progress to Stopped state
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Progress through normal shutdown: Starting → Validating → Initializing → Ready → Draining → Stopped
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test: begin validation");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test: begin initialization");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test: service ready");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Draining, "test: begin drain");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Stopped, "test: drain complete");

            Assert.Equal(ServiceLifecycle.LifecycleState.Stopped, lifecycle.CurrentState);
            Assert.True(lifecycle.IsTerminal);

            // Act + Assert: Any transition attempt from Stopped should throw InvalidOperationException
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
                lifecycle.TransitionTo(targetState, "test: invalid transition from terminal state"));

            Assert.Contains("Invalid state transition", ex.Message);
            Assert.Contains("Stopped", ex.Message);

            // Verify state did not change
            Assert.Equal(ServiceLifecycle.LifecycleState.Stopped, lifecycle.CurrentState);
        }

        /// <summary>
        /// Verifies that Faulted (terminal state) rejects all transition attempts.
        /// </summary>
        /// <remarks>
        /// Faulted represents a failure condition (config invalid, DB unreachable, drain timeout).
        /// Once reached, the service lifecycle is complete and diagnostic state is preserved.
        /// No state transitions should be permitted from Faulted (including self-transitions).
        /// </remarks>
        /// <summary>
        /// Confirms the faulted rejects all transitions behavior.
        /// </summary>
        /// <param name="targetState">The target state used by this test scenario.</param>
        [Theory]
        [InlineData(ServiceLifecycle.LifecycleState.Starting)]
        [InlineData(ServiceLifecycle.LifecycleState.Validating)]
        [InlineData(ServiceLifecycle.LifecycleState.Initializing)]
        [InlineData(ServiceLifecycle.LifecycleState.Ready)]
        [InlineData(ServiceLifecycle.LifecycleState.Draining)]
        [InlineData(ServiceLifecycle.LifecycleState.Stopped)]
        public void Faulted_RejectsAllTransitions(ServiceLifecycle.LifecycleState targetState)
        {
            // Arrange: Create lifecycle and progress to Faulted state
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Progress through startup failure: Starting → Validating → Faulted
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test: begin validation");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "test: validation failed (config invalid)");

            Assert.Equal(ServiceLifecycle.LifecycleState.Faulted, lifecycle.CurrentState);
            Assert.True(lifecycle.IsTerminal);
            Assert.True(lifecycle.IsFaulted);

            // Act + Assert: Any transition attempt from Faulted should throw InvalidOperationException
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
                lifecycle.TransitionTo(targetState, "test: invalid transition from terminal state"));

            Assert.Contains("Invalid state transition", ex.Message);
            Assert.Contains("Faulted", ex.Message);

            // Verify state did not change
            Assert.Equal(ServiceLifecycle.LifecycleState.Faulted, lifecycle.CurrentState);
        }

        /// <summary>
        /// Verifies that terminal states correctly report IsTerminal and IsFaulted properties.
        /// </summary>
        [Fact]
        public void TerminalStates_CorrectlyReportProperties()
        {
            // Arrange + Act: Stopped state
            ServiceLifecycle stoppedLifecycle = new(TimeProvider.System);
            stoppedLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");
            stoppedLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");
            stoppedLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test");
            stoppedLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Draining, "test");
            stoppedLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Stopped, "test");

            // Assert: Stopped is terminal but not faulted
            Assert.True(stoppedLifecycle.IsTerminal);
            Assert.False(stoppedLifecycle.IsFaulted);

            // Arrange + Act: Faulted state
            ServiceLifecycle faultedLifecycle = new(TimeProvider.System);
            faultedLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");
            faultedLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "test");

            // Assert: Faulted is both terminal and faulted
            Assert.True(faultedLifecycle.IsTerminal);
            Assert.True(faultedLifecycle.IsFaulted);
        }

        #endregion

        #region Valid Transition Tests

        /// <summary>
        /// Verifies normal startup progression: Starting → Validating → Initializing → Ready.
        /// </summary>
        [Fact]
        public void NormalStartup_ProgressesCorrectly()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            Assert.Equal(ServiceLifecycle.LifecycleState.Starting, lifecycle.CurrentState);

            // Act + Assert: Starting → Validating
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test: begin validation");
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, lifecycle.CurrentState);

            // Act + Assert: Validating → Initializing
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test: begin initialization");
            Assert.Equal(ServiceLifecycle.LifecycleState.Initializing, lifecycle.CurrentState);

            // Act + Assert: Initializing → Ready
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test: service ready");
            Assert.Equal(ServiceLifecycle.LifecycleState.Ready, lifecycle.CurrentState);
            Assert.False(lifecycle.IsTerminal);
        }

        /// <summary>
        /// Verifies graceful shutdown: Ready → Draining → Stopped.
        /// </summary>
        [Fact]
        public void GracefulShutdown_ProgressesCorrectly()
        {
            // Arrange: Progress to Ready
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test");

            // Act + Assert: Ready → Draining
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Draining, "test: begin drain");
            Assert.Equal(ServiceLifecycle.LifecycleState.Draining, lifecycle.CurrentState);

            // Act + Assert: Draining → Stopped
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Stopped, "test: drain complete");
            Assert.Equal(ServiceLifecycle.LifecycleState.Stopped, lifecycle.CurrentState);
            Assert.True(lifecycle.IsTerminal);
        }

        /// <summary>
        /// Verifies application-controlled drain cancellation: Ready → Draining → Ready.
        /// </summary>
        /// <remarks>
        /// This bidirectional transition supports scenarios like maintenance mode toggle,
        /// orchestration rollback, or operator intervention. It is distinct from standard
        /// systemd SIGTERM shutdown, which is one-way.
        /// </remarks>
        /// <summary>
        /// Confirms the drain cancellation allows ready to draining to ready behavior.
        /// </summary>
        [Fact]
        public void DrainCancellation_AllowsReadyToDrainingToReady()
        {
            // Arrange: Progress to Ready
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test");

            // Act + Assert: Ready → Draining
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Draining, "test: maintenance mode initiated");
            Assert.Equal(ServiceLifecycle.LifecycleState.Draining, lifecycle.CurrentState);

            // Act + Assert: Draining → Ready (cancellation)
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test: maintenance mode cancelled");
            Assert.Equal(ServiceLifecycle.LifecycleState.Ready, lifecycle.CurrentState);
            Assert.False(lifecycle.IsTerminal);
        }

        /// <summary>
        /// Verifies drain timeout failure: Ready → Draining → Faulted.
        /// </summary>
        [Fact]
        public void DrainTimeout_TransitionsToFaulted()
        {
            // Arrange: Progress to Draining
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Draining, "test");

            // Act: Draining → Faulted
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Faulted, "test: drain timeout (inflight work did not complete)");

            // Assert
            Assert.Equal(ServiceLifecycle.LifecycleState.Faulted, lifecycle.CurrentState);
            Assert.True(lifecycle.IsTerminal);
            Assert.True(lifecycle.IsFaulted);
        }

        #endregion

        #region Invalid Transition Tests

        /// <summary>
        /// Verifies that startup progression is monotonic (no reversals), excluding the shutdown-authoritative Initializing -> Draining branch.
        /// </summary>
        [Theory]
        [InlineData(ServiceLifecycle.LifecycleState.Validating, ServiceLifecycle.LifecycleState.Starting)]
        [InlineData(ServiceLifecycle.LifecycleState.Initializing, ServiceLifecycle.LifecycleState.Starting)]
        [InlineData(ServiceLifecycle.LifecycleState.Initializing, ServiceLifecycle.LifecycleState.Validating)]
        public void StartupProgression_RejectsReversals(
            ServiceLifecycle.LifecycleState fromState,
            ServiceLifecycle.LifecycleState invalidTargetState)
        {
            // Arrange: Progress to fromState
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            if (fromState >= ServiceLifecycle.LifecycleState.Validating)
                lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");

            if (fromState >= ServiceLifecycle.LifecycleState.Initializing)
                lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");

            Assert.Equal(fromState, lifecycle.CurrentState);

            // Act + Assert: Reversal should throw
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
                lifecycle.TransitionTo(invalidTargetState, "test: invalid reversal"));

            Assert.Contains("Invalid state transition", ex.Message);
            Assert.Equal(fromState, lifecycle.CurrentState); // State unchanged
        }

        /// <summary>
        /// Verifies that Ready can only transition to Draining (not back to startup states).
        /// </summary>
        [Theory]
        [InlineData(ServiceLifecycle.LifecycleState.Starting)]
        [InlineData(ServiceLifecycle.LifecycleState.Validating)]
        [InlineData(ServiceLifecycle.LifecycleState.Initializing)]
        [InlineData(ServiceLifecycle.LifecycleState.Stopped)]
        [InlineData(ServiceLifecycle.LifecycleState.Faulted)]
        public void Ready_RejectsInvalidTransitions(ServiceLifecycle.LifecycleState invalidTargetState)
        {
            // Arrange: Progress to Ready
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test");

            // Act + Assert: Invalid transition from Ready should throw
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
                lifecycle.TransitionTo(invalidTargetState, "test: invalid transition"));

            Assert.Contains("Invalid state transition", ex.Message);
            Assert.Equal(ServiceLifecycle.LifecycleState.Ready, lifecycle.CurrentState); // State unchanged
        }

        #endregion

        #region API Contract Tests

        /// <summary>
        /// Verifies that TransitionTo rejects null reason.
        /// </summary>
        [Fact]
        public void TransitionTo_RejectsNullReason()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Act + Assert: null reason throws ArgumentNullException
            _ = Assert.Throws<ArgumentNullException>(() =>
                lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, null!));
        }

        /// <summary>
        /// Verifies that TransitionTo rejects empty or whitespace reason strings.
        /// </summary>
        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("\t")]
        public void TransitionTo_RejectsEmptyOrWhitespaceReason(string invalidReason)
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Act + Assert: empty/whitespace throws ArgumentException
            _ = Assert.Throws<ArgumentException>(() =>
                lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, invalidReason));
        }

        /// <summary>
        /// Verifies that LogSlowPhaseWarning rejects invalid threshold values.
        /// </summary>
        [Theory]
        [InlineData(0)]      // Zero duration
        [InlineData(-1)]     // Negative duration
        [InlineData(-1000)]  // Large negative duration
        public void LogSlowPhaseWarning_RejectsInvalidThreshold(int milliseconds)
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            TimeSpan invalidThreshold = TimeSpan.FromMilliseconds(milliseconds);

            // Act + Assert
            _ = Assert.Throws<ArgumentOutOfRangeException>(() =>
                lifecycle.LogSlowPhaseWarning(invalidThreshold, "test phase"));
        }

        /// <summary>
        /// Verifies that LogSlowPhaseWarning rejects null phase name.
        /// </summary>
        [Fact]
        public void LogSlowPhaseWarning_RejectsNullPhaseName()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            TimeSpan validThreshold = TimeSpan.FromSeconds(5);

            // Act + Assert: null phaseName throws ArgumentNullException
            _ = Assert.Throws<ArgumentNullException>(() =>
                lifecycle.LogSlowPhaseWarning(validThreshold, null!));
        }

        /// <summary>
        /// Verifies that LogSlowPhaseWarning rejects empty or whitespace phase names.
        /// </summary>
        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("\t")]
        public void LogSlowPhaseWarning_RejectsEmptyOrWhitespacePhaseName(string invalidPhaseName)
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            TimeSpan validThreshold = TimeSpan.FromSeconds(5);

            // Act + Assert: empty/whitespace throws ArgumentException
            _ = Assert.Throws<ArgumentException>(() =>
                lifecycle.LogSlowPhaseWarning(validThreshold, invalidPhaseName));
        }

        #endregion

        #region Concurrency Tests

        /// <summary>
        /// Verifies thread-safe state reads during concurrent transitions.
        /// </summary>
        /// <remarks>
        /// Tests the scenario: 100 concurrent readers + 1 transition writer.
        /// Ensures no exceptions, no corrupted history, and consistent final state.
        /// </remarks>
        /// <summary>
        /// Confirms the concurrent readers with single writer no corruption behavior.
        /// </summary>
        /// <returns>The value returned by the concurrent readers with single writer no corruption helper.</returns>
        [Fact]
        public async Task ConcurrentReaders_WithSingleWriter_NoCorruption()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            ConcurrentBag<Exception> readExceptions = [];
            int readerCount = 100;
            List<Task> readerTasks = [];

            // Act: Start 100 concurrent readers that continuously poll state
            CancellationTokenSource cts = new();
            for (int i = 0; i < readerCount; i++)
            {
                Task task = Task.Run(() =>
                {
                    try
                    {
                        while (!cts.Token.IsCancellationRequested)
                        {
                            // Read all public state properties
                            ServiceLifecycle.LifecycleState _ = lifecycle.CurrentState;
                            bool __ = lifecycle.IsTerminal;
                            bool ___ = lifecycle.IsFaulted;
                            TimeSpan ____ = lifecycle.TimeInCurrentState;
                            IReadOnlyList<ServiceLifecycle.StateTransition> _____ = lifecycle.TransitionHistory;
                            string ______ = lifecycle.GetSummary();
                        }
                    }
                    catch (Exception ex)
                    {
                        readExceptions.Add(ex);
                    }
                }, cts.Token);

                readerTasks.Add(task);
            }

            // Give readers time to start
            Thread.Sleep(50);

            // Act: Perform state transitions while readers are active
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "concurrent test: validation");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "concurrent test: initialization");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "concurrent test: ready");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Draining, "concurrent test: draining");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Stopped, "concurrent test: stopped");

            // Stop readers
            cts.Cancel();
            try
            {
                await Task.WhenAll(readerTasks);
            }
            catch (TaskCanceledException)
            {
                // Expected: tasks were cancelled
            }

            // Assert: No exceptions during concurrent reads
            Assert.Empty(readExceptions);

            // Assert: Final state is correct
            Assert.Equal(ServiceLifecycle.LifecycleState.Stopped, lifecycle.CurrentState);
            Assert.True(lifecycle.IsTerminal);
            Assert.False(lifecycle.IsFaulted);

            // Assert: History is complete and ordered
            IReadOnlyList<ServiceLifecycle.StateTransition> history = lifecycle.TransitionHistory;
            Assert.Equal(5, history.Count); // Starting→Validating, Validating→Initializing, etc.

            Assert.Equal(ServiceLifecycle.LifecycleState.Starting, history[0].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, history[0].ToState);

            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, history[1].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Initializing, history[1].ToState);

            Assert.Equal(ServiceLifecycle.LifecycleState.Initializing, history[2].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Ready, history[2].ToState);

            Assert.Equal(ServiceLifecycle.LifecycleState.Ready, history[3].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Draining, history[3].ToState);

            Assert.Equal(ServiceLifecycle.LifecycleState.Draining, history[4].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Stopped, history[4].ToState);
        }

        /// <summary>
        /// Verifies that TransitionHistory returns a snapshot, not a live collection.
        /// </summary>
        /// <remarks>
        /// Ensures concurrent transitions don't affect previously-retrieved history snapshots.
        /// </remarks>
        /// <summary>
        /// Confirms the transition history returns snapshot not live collection behavior.
        /// </summary>
        [Fact]
        public void TransitionHistory_ReturnsSnapshot_NotLiveCollection()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Act: Get snapshot before transition
            IReadOnlyList<ServiceLifecycle.StateTransition> snapshotBefore = lifecycle.TransitionHistory;
            Assert.Empty(snapshotBefore);

            // Act: Perform transition
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "snapshot test");

            // Act: Get snapshot after transition
            IReadOnlyList<ServiceLifecycle.StateTransition> snapshotAfter = lifecycle.TransitionHistory;

            // Assert: Original snapshot unchanged (proves it's a copy, not a live reference)
            Assert.Empty(snapshotBefore);
            _ = Assert.Single(snapshotAfter);

            // Act: Perform another transition
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "snapshot test 2");

            // Act: Get final snapshot
            IReadOnlyList<ServiceLifecycle.StateTransition> snapshotFinal = lifecycle.TransitionHistory;

            // Assert: Earlier snapshots remain unchanged
            Assert.Empty(snapshotBefore);
            _ = Assert.Single(snapshotAfter);
            Assert.Equal(2, snapshotFinal.Count);
        }

        #endregion

        #region Observer Resilience Tests

        /// <summary>
        /// Verifies that subscriber exceptions are isolated and don't break state transitions.
        /// </summary>
        /// <remarks>
        /// <para>Tests the critical observer contract: if Subscriber A throws, the transition
        /// completes successfully, and Subscribers B and C still execute.</para>
        /// <para>This validates the exception isolation design documented in ServiceLifecycle.</para>
        /// </remarks>
        /// <summary>
        /// Confirms the subscriber exceptions are isolated transition completes behavior.
        /// </summary>
        [Fact]
        public void SubscriberExceptions_AreIsolated_TransitionCompletes()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            bool subscriberAInvoked = false;
            bool subscriberBInvoked = false;
            bool subscriberCInvoked = false;

            // Subscribe three handlers: A throws, B succeeds, C succeeds
            lifecycle.StateTransitioned += (transition) =>
            {
                subscriberAInvoked = true;
                throw new InvalidOperationException("Subscriber A failed");
            };

            lifecycle.StateTransitioned += (transition) =>
            {
                subscriberBInvoked = true;
                // Succeeds normally
            };

            lifecycle.StateTransitioned += (transition) =>
            {
                subscriberCInvoked = true;
                // Succeeds normally
            };

            // Act: Perform transition (should not throw despite Subscriber A failing)
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "observer resilience test");

            // Assert: Transition completed successfully
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, lifecycle.CurrentState);

            // Assert: All subscribers were invoked (even though A threw)
            Assert.True(subscriberAInvoked, "Subscriber A should have been invoked");
            Assert.True(subscriberBInvoked, "Subscriber B should have been invoked despite A throwing");
            Assert.True(subscriberCInvoked, "Subscriber C should have been invoked despite A throwing");

            // Assert: History recorded the transition (proves transition completed)
            IReadOnlyList<ServiceLifecycle.StateTransition> history = lifecycle.TransitionHistory;
            _ = Assert.Single(history);
            Assert.Equal(ServiceLifecycle.LifecycleState.Starting, history[0].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, history[0].ToState);
        }

        /// <summary>
        /// Verifies that multiple subscriber failures don't corrupt state or prevent transition.
        /// </summary>
        [Fact]
        public void MultipleSubscriberFailures_TransitionStillCompletes()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            int invocationCount = 0;

            // Subscribe three handlers that all throw
            lifecycle.StateTransitioned += (transition) =>
            {
                invocationCount++;
                throw new InvalidOperationException("Subscriber 1 failed");
            };

            lifecycle.StateTransitioned += (transition) =>
            {
                invocationCount++;
                throw new ArgumentException("Subscriber 2 failed");
            };

            lifecycle.StateTransitioned += (transition) =>
            {
                invocationCount++;
                throw new NotSupportedException("Subscriber 3 failed");
            };

            // Act: Perform transition (should complete despite all subscribers throwing)
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "multiple failures test");

            // Assert: Transition completed
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, lifecycle.CurrentState);

            // Assert: All subscribers were invoked
            Assert.Equal(3, invocationCount);

            // Assert: State is consistent
            Assert.False(lifecycle.IsTerminal);
            Assert.False(lifecycle.IsFaulted);

            // Assert: Can perform subsequent transitions normally
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "subsequent transition");
            Assert.Equal(ServiceLifecycle.LifecycleState.Initializing, lifecycle.CurrentState);
        }

        /// <summary>
        /// Verifies that subscribers receive correct transition data even when exceptions occur.
        /// </summary>
        [Fact]
        public void Subscribers_ReceiveCorrectTransitionData_EvenWithFailures()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            ServiceLifecycle.StateTransition? capturedTransition = null;

            // Subscribe: first throws, second captures data
            lifecycle.StateTransitioned += (transition) =>
            {
                throw new InvalidOperationException("First subscriber failed");
            };

            lifecycle.StateTransitioned += (transition) =>
            {
                capturedTransition = transition;
            };

            // Act
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "transition data test");

            // Assert: Second subscriber received correct transition data
            Assert.NotNull(capturedTransition);
            Assert.Equal(ServiceLifecycle.LifecycleState.Starting, capturedTransition.FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, capturedTransition.ToState);
            Assert.Equal("transition data test", capturedTransition.Reason);
            Assert.NotEqual(default, capturedTransition.Timestamp);
            Assert.True(capturedTransition.DurationInPreviousState >= TimeSpan.Zero);
        }

        /// <summary>
        /// Verifies that no subscribers / null event handler is handled gracefully.
        /// </summary>
        [Fact]
        public void NoSubscribers_TransitionsWorkNormally()
        {
            // Arrange: Create lifecycle with no subscribers
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Act: Perform transitions without any event handlers
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "no subscribers test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "no subscribers test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "no subscribers test");

            // Assert: Transitions completed normally
            Assert.Equal(ServiceLifecycle.LifecycleState.Ready, lifecycle.CurrentState);
            Assert.Equal(3, lifecycle.TransitionHistory.Count);
        }

        /// <summary>
        /// Verifies that TransitionTo cannot be called from within StateTransitioned handlers (reentrancy forbidden).
        /// </summary>
        /// <remarks>
        /// <para>This test enforces the documented observer contract: subscribers MUST NOT call TransitionTo()
        /// from within StateTransitioned event handlers.</para>
        /// <para><b>Critical invariant:</b> Reentrancy creates state confusion where:</para>
        /// <list type="bullet">
        /// <item><description>Outer transition logs "Ready" and starts notifying subscribers</description></item>
        /// <item><description>Subscriber calls TransitionTo(Draining), state changes mid-notification</description></item>
        /// <item><description>Remaining subscribers observe "Ready" transition but state is already "Draining"</description></item>
        /// </list>
        /// <para>The implementation must detect and reject this scenario with InvalidOperationException.</para>
        /// <para><b>Test design:</b> The reentrant transition (Validating → Initializing) is independently
        /// valid per the state machine rules. This proves the rejection comes from the reentrancy guard,
        /// not from ordinary transition validation.</para>
        /// </remarks>
        /// <summary>
        /// Confirms the reentrant transition from subscriber throws invalid operation exception behavior.
        /// </summary>
        [Fact]
        public void ReentrantTransition_FromSubscriber_ThrowsInvalidOperationException()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            InvalidOperationException? capturedException = null;
            bool subscriberWasInvoked = false;

            // Subscribe a handler that attempts reentrancy (forbidden)
            lifecycle.StateTransitioned += (transition) =>
            {
                subscriberWasInvoked = true;

                // At this point, outer transition (Starting → Validating) has completed state mutation
                // Current state is Validating, and Validating → Initializing is a VALID transition
                // (proves rejection is due to reentrancy guard, not transition validation)
                Assert.Equal(ServiceLifecycle.LifecycleState.Validating, lifecycle.CurrentState);

                try
                {
                    // FORBIDDEN: Attempt to call TransitionTo from within StateTransitioned handler
                    // Transition Validating → Initializing is valid, so rejection must be reentrancy guard
                    lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "reentrant transition attempt");

                    // If we reach here, reentrancy was NOT blocked (test should fail)
                    throw new Exception("TEST FAILURE: Reentrancy was allowed but should have been blocked!");
                }
                catch (InvalidOperationException ex)
                {
                    // Expected: reentrancy detected
                    capturedException = ex;
                }
            };

            // Act: Trigger initial transition Starting → Validating (which will trigger the reentrant attempt)
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "initial transition");

            // Assert: Subscriber was invoked
            Assert.True(subscriberWasInvoked, "Subscriber should have been invoked");

            // Assert: Reentrancy attempt was detected and rejected
            Assert.NotNull(capturedException);
            Assert.Contains("observers are being notified", capturedException.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("TransitionTo", capturedException.Message);
            Assert.Contains("MUST NOT", capturedException.Message);

            // Assert: Original transition completed successfully (reentrancy didn't break state machine)
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, lifecycle.CurrentState);

            // Assert: No corrupted state (only the valid transition was recorded)
            IReadOnlyList<ServiceLifecycle.StateTransition> history = lifecycle.TransitionHistory;
            _ = Assert.Single(history);
            Assert.Equal(ServiceLifecycle.LifecycleState.Starting, history[0].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, history[0].ToState);

            // Assert: Prove the attempted transition would have been valid outside reentrancy context
            // (Create a fresh lifecycle to verify Validating → Initializing is allowed)
            ServiceLifecycle testLifecycle = new(TimeProvider.System);
            testLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");

            // This should succeed (proves the transition is valid when not reentrant)
            testLifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");
            Assert.Equal(ServiceLifecycle.LifecycleState.Initializing, testLifecycle.CurrentState);
        }

        #endregion

        #region Timing and History Tests

        /// <summary>
        /// Verifies that transition history preserves reason strings.
        /// </summary>
        [Fact]
        public void TransitionHistory_PreservesReasons()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Act
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "validation started");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "database connection established");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "all subsystems initialized");

            // Assert
            IReadOnlyList<ServiceLifecycle.StateTransition> history = lifecycle.TransitionHistory;
            Assert.Equal(3, history.Count);
            Assert.Equal("validation started", history[0].Reason);
            Assert.Equal("database connection established", history[1].Reason);
            Assert.Equal("all subsystems initialized", history[2].Reason);
        }

        /// <summary>
        /// Verifies that TimeInCurrentState advances monotonically.
        /// </summary>
        [Fact]
        public void TimeInCurrentState_AdvancesMonotonically()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Act: Sample elapsed time multiple times
            TimeSpan first = lifecycle.TimeInCurrentState;
            Thread.Sleep(10); // Small delay
            TimeSpan second = lifecycle.TimeInCurrentState;
            Thread.Sleep(10); // Small delay
            TimeSpan third = lifecycle.TimeInCurrentState;

            // Assert: Time advances (never goes backward)
            Assert.True(second >= first, "Second sample should be >= first");
            Assert.True(third >= second, "Third sample should be >= second");
            Assert.True(third > first, "Third sample should be > first (some time passed)");
        }

        /// <summary>
        /// Verifies that GetSummary returns valid diagnostic information.
        /// </summary>
        [Fact]
        public void GetSummary_ReturnsValidDiagnostics()
        {
            // Arrange
            ServiceLifecycle lifecycle = new(TimeProvider.System);

            // Act: Get summary in initial state
            string initialSummary = lifecycle.GetSummary();
            Assert.Contains("Starting", initialSummary);
            Assert.Contains("0 transitions", initialSummary); // No history yet

            // Act: Perform some transitions
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test");

            string afterTransitions = lifecycle.GetSummary();

            // Assert: Summary reflects current state and history count
            Assert.Contains("Initializing", afterTransitions);
            Assert.Contains("2 transitions", afterTransitions); // Two transitions recorded
        }

        #endregion

        #region Concurrent Writers Tests

        /// <summary>
        /// Verifies that simultaneous transitions from multiple threads are serialized correctly.
        /// The lock ensures only one transition progresses at a time; concurrent attempts wait.
        /// </summary>
        [Fact]
        public void ConcurrentWriters_OneSucceeds_OtherRejected_StateRemainsCoheren()
        {
            // Arrange: Get to Validating state with a StateTransitioned handler that delays notification
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            TimeSpan slowSubscriberDelay = TimeSpan.FromMilliseconds(100);
            bool threadBStartedWhileNotifying = false;
            bool threadBCompletedNormally = false;

            lifecycle.StateTransitioned += transition =>
            {
                // Slow subscriber - holds the notification phase open
                Thread.Sleep(slowSubscriberDelay);
            };

            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "startup");

            Barrier barrier = new(2); // Synchronize two threads
            Exception? threadBException = null;

            // Act: Thread A transitions to Initializing (will notify slowly)
            //      Thread B attempts transition while A is notifying
            Thread threadA = new(() =>
            {
                barrier.SignalAndWait(); // Wait for both threads ready
                lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "ThreadA normal transition");
            });

            Thread threadB = new(() =>
            {
                barrier.SignalAndWait(); // Wait for both threads ready
                Thread.Sleep(20); // Let Thread A enter TransitionTo first

                try
                {
                    // Thread B attempts to transition while Thread A is notifying observers
                    lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "ThreadB attempt during notification");
                    threadBCompletedNormally = true;
                }
                catch (InvalidOperationException ex)
                {
                    // Expected: Thread B blocked because _isNotifyingObservers is true
                    if (ex.Message.Contains("observers are being notified"))
                    {
                        threadBStartedWhileNotifying = true;
                    }
                    threadBException = ex;
                }
            });

            threadA.Start();
            threadB.Start();
            threadA.Join();
            threadB.Join();

            // Assert: Thread A succeeded
            Assert.Equal(ServiceLifecycle.LifecycleState.Initializing, lifecycle.CurrentState);

            // Assert: Thread B was rejected (attempted transition while observers were being notified)
            Assert.True(threadBStartedWhileNotifying || threadBCompletedNormally,
                "Thread B should have either been rejected during notification or succeeded after");

            // If Thread B was rejected, verify it was due to notification guard
            if (!threadBCompletedNormally)
            {
                Assert.NotNull(threadBException);
                Assert.Contains("observers are being notified", threadBException.Message);
            }

            // Assert: State machine remains coherent (exactly 2 transitions: Starting→Validating, Validating→Initializing)
            // OR 3 if Thread B succeeded after Thread A completed (Initializing→Ready)
            IReadOnlyList<ServiceLifecycle.StateTransition> history = lifecycle.TransitionHistory;
            Assert.True(history.Count is 2 or 3,
                $"Expected 2 or 3 transitions, got {history.Count}");

            Assert.Equal(ServiceLifecycle.LifecycleState.Starting, history[0].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, history[0].ToState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Validating, history[1].FromState);
            Assert.Equal(ServiceLifecycle.LifecycleState.Initializing, history[1].ToState);
            Assert.Equal("ThreadA normal transition", history[1].Reason);
        }

        #endregion
    }
}
