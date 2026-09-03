// <copyright file="ShutdownCoordinatorDisposeSemanticsTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for shutdown coordinator dispose semantics, covering service lifecycle and shutdown contracts.
// Primary responsibility: documents the executable contracts covered by the shutdown coordinator dispose semantics test suite.

using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Shutdown
{
    /// <summary>
    /// Tests disposal semantics to lock in lifecycle-owner responsibilities.
    /// </summary>
    public sealed class ShutdownCoordinatorDisposeSemanticsTests
    {
        /// <summary>
        /// Confirms the dispose during graceful shutdown completes without forced escalation signal behavior.
        /// </summary>
        [Fact]
        public void Dispose_DuringGracefulShutdown_CompletesWithoutForcedEscalationSignal()
        {
            ShutdownCoordinator coordinator = new();

            coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(5), ShutdownCoordinator.ShutdownReason.HostStopping);
            coordinator.Dispose();

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
        }
        /// <summary>
        /// Confirms the dispose during graceful shutdown forced shutdown token becomes unusable behavior.
        /// </summary>
        [Fact]
        public void Dispose_DuringGracefulShutdown_ForcedShutdownTokenBecomesUnusable()
        {
            ShutdownCoordinator coordinator = new();

            coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(5), ShutdownCoordinator.ShutdownReason.HostStopping);
            coordinator.Dispose();

            _ = Assert.Throws<ObjectDisposedException>(() =>
                _ = coordinator.ForcedShutdownToken.IsCancellationRequested);
        }
        /// <summary>
        /// Confirms the dispose during graceful shutdown prevents later timer driven forced escalation behavior.
        /// </summary>
        [Fact]
        public async Task Dispose_DuringGracefulShutdown_PreventsLaterTimerDrivenForcedEscalation()
        {
            TimeSpan gracePeriod = TimeSpan.FromSeconds(1);
            ShutdownCoordinator coordinator = new();

            coordinator.SignalGracefulShutdown(gracePeriod, ShutdownCoordinator.ShutdownReason.HostStopping);
            Assert.Equal(ShutdownCoordinator.ShutdownState.GracefulShutdown, coordinator.State);

            coordinator.Dispose();
            await Task.Delay(gracePeriod + TimeSpan.FromMilliseconds(250));

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
            Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, coordinator.ForcedShutdownReason);
        }
        /// <summary>
        /// Confirms the signal graceful shutdown after dispose is ignored behavior.
        /// </summary>
        [Fact]
        public void SignalGracefulShutdown_AfterDispose_IsIgnored()
        {
            ShutdownCoordinator coordinator = new();
            coordinator.Dispose();

            coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(5), ShutdownCoordinator.ShutdownReason.HostStopping);

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
            Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, coordinator.GracefulShutdownReason);
            Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, coordinator.ForcedShutdownReason);
        }
        /// <summary>
        /// Confirms the signal forced shutdown after dispose is ignored behavior.
        /// </summary>
        [Fact]
        public void SignalForcedShutdown_AfterDispose_IsIgnored()
        {
            ShutdownCoordinator coordinator = new();
            coordinator.Dispose();

            coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.HostStopping);

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
            Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, coordinator.GracefulShutdownReason);
            Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, coordinator.ForcedShutdownReason);
        }
    }
}
