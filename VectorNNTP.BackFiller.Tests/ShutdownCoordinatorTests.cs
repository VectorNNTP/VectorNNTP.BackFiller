using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Concurrency and state-transition tests for <see cref="ShutdownCoordinator"/>.
/// </summary>
public sealed class ShutdownCoordinatorTests
{
    [Fact]
    public void SignalGracefulShutdown_FromRunning_TransitionsToGraceful()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(1));

        Assert.Equal(ShutdownCoordinator.ShutdownState.GracefulShutdown, coordinator.State);
        Assert.True(coordinator.GracefulShutdownStartedToken.IsCancellationRequested);
        Assert.False(coordinator.ForcedShutdownToken.IsCancellationRequested);
        Assert.NotNull(coordinator.GracefulShutdownStartedAtUtc);
        Assert.NotNull(coordinator.GracefulShutdownStartedTimestamp);

        coordinator.Dispose();
    }

    [Fact]
    public void SignalForcedShutdown_FromRunning_TransitionsToForced()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalForcedShutdown();

        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.True(coordinator.GracefulShutdownStartedToken.IsCancellationRequested);
        Assert.True(coordinator.ForcedShutdownToken.IsCancellationRequested);
        Assert.NotNull(coordinator.ForcedShutdownAtUtc);
        Assert.NotNull(coordinator.ForcedShutdownTimestamp);

        coordinator.Dispose();
    }

    [Fact]
    public void SignalForcedShutdown_FromGraceful_TransitionsToForced()
    {
        ShutdownCoordinator coordinator = new();
        coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(5));

        coordinator.SignalForcedShutdown();

        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.True(coordinator.ForcedShutdownToken.IsCancellationRequested);

        coordinator.Dispose();
    }

    [Fact]
    public void Dispose_FromForced_TransitionsToCompleted()
    {
        ShutdownCoordinator coordinator = new();
        coordinator.SignalForcedShutdown();

        coordinator.Dispose();

        Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
    }

    [Fact]
    public void SignalGracefulShutdown_Idempotent_WhenCalledTwice()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(1), ShutdownCoordinator.ShutdownReason.OperatorRequest);
        DateTimeOffset? firstStartedAt = coordinator.GracefulShutdownStartedAtUtc;
        long? firstTimestamp = coordinator.GracefulShutdownStartedTimestamp;
        ShutdownCoordinator.ShutdownReason firstReason = coordinator.GracefulShutdownReason;

        coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(1), ShutdownCoordinator.ShutdownReason.DependencyFailure);

        Assert.Equal(ShutdownCoordinator.ShutdownState.GracefulShutdown, coordinator.State);
        Assert.Equal(firstStartedAt, coordinator.GracefulShutdownStartedAtUtc);
        Assert.Equal(firstTimestamp, coordinator.GracefulShutdownStartedTimestamp);
        Assert.Equal(firstReason, coordinator.GracefulShutdownReason);

        coordinator.Dispose();
    }

    [Fact]
    public void SignalForcedShutdown_Idempotent_WhenCalledTwice()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest);
        DateTimeOffset? firstForcedAt = coordinator.ForcedShutdownAtUtc;
        long? firstForcedTimestamp = coordinator.ForcedShutdownTimestamp;
        ShutdownCoordinator.ShutdownReason firstReason = coordinator.ForcedShutdownReason;

        coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.DependencyFailure);

        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.Equal(firstForcedAt, coordinator.ForcedShutdownAtUtc);
        Assert.Equal(firstForcedTimestamp, coordinator.ForcedShutdownTimestamp);
        Assert.Equal(firstReason, coordinator.ForcedShutdownReason);

        coordinator.Dispose();
    }

    [Fact]
    public async Task GraceEscalation_AfterDeadline_TransitionsToForcedAndCancelsForcedToken()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalGracefulShutdown(TimeSpan.FromMilliseconds(50), ShutdownCoordinator.ShutdownReason.HostStopping);

        Stopwatch wait = Stopwatch.StartNew();
        while (coordinator.State != ShutdownCoordinator.ShutdownState.ForcedShutdown && wait.Elapsed < TimeSpan.FromSeconds(2))
        {
            await Task.Delay(10);
        }

        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.True(coordinator.ForcedShutdownToken.IsCancellationRequested);
        Assert.Equal(ShutdownCoordinator.ShutdownReason.HostStopping, coordinator.GracefulShutdownReason);
        Assert.Equal(ShutdownCoordinator.ShutdownReason.GracePeriodExpired, coordinator.ForcedShutdownReason);

        Assert.NotNull(coordinator.GracefulShutdownStartedAtUtc);
        Assert.NotNull(coordinator.ForcedShutdownAtUtc);
        Assert.True(coordinator.ForcedShutdownAtUtc.Value >= coordinator.GracefulShutdownStartedAtUtc.Value);

        Assert.NotNull(coordinator.GracefulShutdownStartedTimestamp);
        Assert.NotNull(coordinator.ForcedShutdownTimestamp);
        Assert.True(coordinator.ForcedShutdownTimestamp.Value >= coordinator.GracefulShutdownStartedTimestamp.Value);

        Assert.NotNull(coordinator.GracefulShutdownElapsed);
        Assert.True(coordinator.GracefulShutdownElapsed.Value >= TimeSpan.Zero);

        coordinator.Dispose();
    }

    [Fact]
    public void SignalForcedShutdown_Immediately_CancelsGracefulAndForcedTokens()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.FatalError);

        Assert.True(coordinator.GracefulShutdownStartedToken.IsCancellationRequested);
        Assert.True(coordinator.ForcedShutdownToken.IsCancellationRequested);

        coordinator.Dispose();
    }

    [Fact]
    public async Task ShutdownReasons_AutomaticEscalation_PreservesGracefulAndForcedReasons()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalGracefulShutdown(TimeSpan.FromMilliseconds(30), ShutdownCoordinator.ShutdownReason.HostStopping);

        Stopwatch wait = Stopwatch.StartNew();
        while (coordinator.State != ShutdownCoordinator.ShutdownState.ForcedShutdown && wait.Elapsed < TimeSpan.FromSeconds(2))
        {
            await Task.Delay(10);
        }

        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.Equal(ShutdownCoordinator.ShutdownReason.HostStopping, coordinator.GracefulShutdownReason);
        Assert.Equal(ShutdownCoordinator.ShutdownReason.GracePeriodExpired, coordinator.ForcedShutdownReason);

        coordinator.Dispose();
    }

    [Fact]
    public void ShutdownReasons_ImmediateForced_LeavesGracefulUnknownAndSetsForcedReason()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest);

        Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, coordinator.GracefulShutdownReason);
        Assert.Equal(ShutdownCoordinator.ShutdownReason.OperatorRequest, coordinator.ForcedShutdownReason);

        coordinator.Dispose();
    }

    [Fact]
    public void SignalGracefulShutdown_DoesNotCancelForcedTokenInitially()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(1), ShutdownCoordinator.ShutdownReason.HostStopping);

        Assert.True(coordinator.GracefulShutdownStartedToken.IsCancellationRequested);
        Assert.False(coordinator.ForcedShutdownToken.IsCancellationRequested);

        coordinator.Dispose();
    }

    [Fact]
    public async Task SignalGracefulShutdown_CallbackReentrancy_DoesNotDeadlock()
    {
        ShutdownCoordinator coordinator = new();
        using CancellationTokenRegistration registration = coordinator.GracefulShutdownStartedToken.Register(() =>
        {
            _ = coordinator.State;
            coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest);
        });

        Task signalTask = Task.Run(() =>
            coordinator.SignalGracefulShutdown(TimeSpan.FromMilliseconds(100), ShutdownCoordinator.ShutdownReason.HostStopping));

        Task completedTask = await Task.WhenAny(signalTask, Task.Delay(TimeSpan.FromSeconds(2)));

        Assert.Same(signalTask, completedTask);
        await signalTask;
        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.True(coordinator.ForcedShutdownToken.IsCancellationRequested);

        coordinator.Dispose();
    }

    [Fact]
    public void SignalGracefulShutdown_CallbackFailure_DoesNotPreventOtherCallbacks()
    {
        ShutdownCoordinator coordinator = new();
        bool secondCallbackExecuted = false;

        using CancellationTokenRegistration throwingRegistration = coordinator.GracefulShutdownStartedToken.Register(
            static () => throw new InvalidOperationException("test"));

        using CancellationTokenRegistration successfulRegistration = coordinator.GracefulShutdownStartedToken.Register(() =>
            secondCallbackExecuted = true);

        coordinator.SignalGracefulShutdown(TimeSpan.FromMilliseconds(100), ShutdownCoordinator.ShutdownReason.HostStopping);

        Assert.True(secondCallbackExecuted);

        coordinator.Dispose();
    }

    [Fact]
    public async Task ForcedRace_ImmediateForcedWins_PreservesOperatorReason()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalGracefulShutdown(TimeSpan.FromMilliseconds(100), ShutdownCoordinator.ShutdownReason.HostStopping);
        coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest);

        bool forcedSignaled = coordinator.ForcedShutdownToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(2));
        await Task.Delay(150);

        Assert.True(forcedSignaled);
        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.Equal(ShutdownCoordinator.ShutdownReason.OperatorRequest, coordinator.ForcedShutdownReason);

        coordinator.Dispose();
    }

    [Fact]
    public void ForcedRace_TimerWins_PreservesGracePeriodExpiredReason()
    {
        ShutdownCoordinator coordinator = new();

        coordinator.SignalGracefulShutdown(TimeSpan.FromMilliseconds(20), ShutdownCoordinator.ShutdownReason.HostStopping);

        bool forcedSignaled = coordinator.ForcedShutdownToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(2));
        coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest);

        Assert.True(forcedSignaled);
        Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
        Assert.Equal(ShutdownCoordinator.ShutdownReason.GracePeriodExpired, coordinator.ForcedShutdownReason);

        coordinator.Dispose();
    }

    [Fact]
    public async Task ForcedRace_ImmediateAndTimer_ConcurrentFirstWriterWins_NoUnhandledExceptions()
    {
        for (int i = 0; i < 500; i++)
        {
            ShutdownCoordinator coordinator = new();
            TimeSpan gracePeriod = TimeSpan.FromMilliseconds(15);
            coordinator.SignalGracefulShutdown(gracePeriod, ShutdownCoordinator.ShutdownReason.HostStopping);

            Barrier startBarrier = new(participantCount: 2);
            Task immediateForced = Task.Run(async () =>
            {
                startBarrier.SignalAndWait();
                await Task.Delay(gracePeriod);
                coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest);
            });

            startBarrier.SignalAndWait();

            bool forcedSignaled = coordinator.ForcedShutdownToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(2));
            await immediateForced;

            Assert.True(forcedSignaled);
            Assert.Equal(ShutdownCoordinator.ShutdownState.ForcedShutdown, coordinator.State);
            Assert.True(coordinator.ForcedShutdownToken.IsCancellationRequested);
            Assert.True(
                coordinator.ForcedShutdownReason is ShutdownCoordinator.ShutdownReason.OperatorRequest or ShutdownCoordinator.ShutdownReason.GracePeriodExpired);

            coordinator.Dispose();
        }
    }

    [Fact]
    public async Task DisposalRace_TimerEscalationAndDispose_NoUnhandledExceptions()
    {
        for (int i = 0; i < 600; i++)
        {
            ShutdownCoordinator coordinator = new();
            TimeSpan gracePeriod = TimeSpan.FromMilliseconds(8);
            coordinator.SignalGracefulShutdown(gracePeriod, ShutdownCoordinator.ShutdownReason.HostStopping);

            Barrier barrier = new(participantCount: 2);
            Task disposeTask = Task.Run(async () =>
            {
                barrier.SignalAndWait();

                int offset = i % 3;
                TimeSpan disposeDelay = offset switch
                {
                    0 => TimeSpan.FromMilliseconds(6),
                    1 => TimeSpan.FromMilliseconds(8),
                    _ => TimeSpan.FromMilliseconds(10),
                };

                await Task.Delay(disposeDelay);
                coordinator.Dispose();
            });

            barrier.SignalAndWait();

            await Task.WhenAll(disposeTask, Task.Delay(TimeSpan.FromMilliseconds(20)));

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
        }
    }

    [Fact]
    public async Task DisposalRace_ForcedAndDispose_NoUnhandledExceptions()
    {
        for (int i = 0; i < 100; i++)
        {
            ShutdownCoordinator coordinator = new();

            Task forcedTask = Task.Run(() => coordinator.SignalForcedShutdown(ShutdownCoordinator.ShutdownReason.OperatorRequest));
            Task disposeTask = Task.Run(coordinator.Dispose);

            await Task.WhenAll(forcedTask, disposeTask);

            Assert.Equal(ShutdownCoordinator.ShutdownState.Completed, coordinator.State);
        }
    }
}
