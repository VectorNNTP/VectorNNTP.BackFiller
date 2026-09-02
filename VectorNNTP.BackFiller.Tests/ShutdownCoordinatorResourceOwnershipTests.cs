// <copyright file="ShutdownCoordinatorResourceOwnershipTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for shutdown coordinator resource ownership, covering service lifecycle and shutdown contracts.
// Primary responsibility: documents the executable contracts covered by the shutdown coordinator resource ownership test suite.

using System.Reflection;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests shutdown coordinator resource ownership around graceful-to-forced transitions.
    /// </summary>
    public sealed class ShutdownCoordinatorResourceOwnershipTests
    {
        /// <summary>
        /// Exercises signal forced shutdown  from graceful  disposes grace period escalation source behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void SignalForcedShutdown_FromGraceful_DisposesGracePeriodEscalationSource()
        {
            ShutdownCoordinator coordinator = new();

            coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(5), ShutdownCoordinator.ShutdownReason.HostStopping);

            FieldInfo gracePeriodField = typeof(ShutdownCoordinator).GetField("_gracePeriodCts", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("Expected _gracePeriodCts private field.");

            CancellationTokenSource gracePeriodSource = gracePeriodField.GetValue(coordinator) as CancellationTokenSource
                ?? throw new InvalidOperationException("Expected graceful shutdown to create a grace period CancellationTokenSource.");

            coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest);
            coordinator.Dispose();

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
            _ = Assert.Throws<ObjectDisposedException>(gracePeriodSource.Cancel);
        }
    }
}
