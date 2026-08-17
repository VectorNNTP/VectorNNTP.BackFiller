using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests the control-plane startup contract and steady-state service semantics.
/// </summary>
public sealed class ControlPlaneServiceTests
{
    [Fact]
    public async Task StartAsync_CompletesStartupInitialization()
    {
        ControlPlaneService service = new(
            NullLogger<ControlPlaneService>.Instance,
            new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
            new MySqlNntpAccountSnapshotProvider(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                static _ => Task.FromResult<List<NntpAccountSnapshot>>([])));

        Assert.False(service.IsStartupInitializationComplete);

        await service.StartAsync(CancellationToken.None);

        Assert.True(service.IsStartupInitializationComplete);

        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_WhenCanceled_DoesNotCompleteStartupInitialization()
    {
        ControlPlaneService service = new(
            NullLogger<ControlPlaneService>.Instance,
            new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
            new MySqlNntpAccountSnapshotProvider(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                static _ => Task.FromResult<List<NntpAccountSnapshot>>([])));

        using CancellationTokenSource cancellationTokenSource = new();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.StartAsync(cancellationTokenSource.Token));
        Assert.False(service.IsStartupInitializationComplete);
    }

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow()
        {
            return utcNow;
        }
    }
}
