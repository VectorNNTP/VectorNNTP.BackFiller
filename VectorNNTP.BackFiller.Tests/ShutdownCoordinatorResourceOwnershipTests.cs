using System.Reflection;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests shutdown coordinator resource ownership around graceful-to-forced transitions.
/// </summary>
public sealed class ShutdownCoordinatorResourceOwnershipTests
{
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
        Assert.Throws<ObjectDisposedException>(() => gracePeriodSource.Cancel());
    }
}
