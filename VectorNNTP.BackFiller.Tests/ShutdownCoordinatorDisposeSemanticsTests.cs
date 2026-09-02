// <copyright file="ShutdownCoordinatorDisposeSemanticsTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for shutdown coordinator dispose semantics, covering service lifecycle and shutdown contracts.

using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests disposal semantics to lock in lifecycle-owner responsibilities.
    /// </summary>
    public sealed class ShutdownCoordinatorDisposeSemanticsTests
    {
        /// <summary>
        /// Exercises dispose  during graceful shutdown  completes without forced escalation signal behavior, including the expected result and failure semantics.
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
        /// Exercises dispose  during graceful shutdown  forced shutdown token becomes unusable behavior, including the expected result and failure semantics.
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
        /// Exercises dispose  during graceful shutdown  prevents later timer driven forced escalation behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task Dispose_DuringGracefulShutdown_PreventsLaterTimerDrivenForcedEscalation()
        {
            ShutdownCoordinator coordinator = new();

            coordinator.SignalGracefulShutdown(TimeSpan.FromMilliseconds(50), ShutdownCoordinator.ShutdownReason.HostStopping);
            Assert.Equal(ShutdownCoordinator.ShutdownState.GracefulShutdown, coordinator.State);

            coordinator.Dispose();
            await Task.Delay(TimeSpan.FromMilliseconds(150));

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
            Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, coordinator.ForcedShutdownReason);
        }
        /// <summary>
        /// Exercises signal graceful shutdown  after dispose  is ignored behavior, including the expected result and failure semantics.
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
        /// Exercises signal forced shutdown  after dispose  is ignored behavior, including the expected result and failure semantics.
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
