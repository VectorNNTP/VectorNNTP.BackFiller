using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests periodic refresh semantics for the MySQL NNTP account snapshot provider.
/// </summary>
public sealed class MySqlNntpAccountSnapshotProviderRefreshTests
{
    [Fact]
    public async Task RefreshSnapshotAsync_WhenSuccessful_ReplacesSnapshotAtomically()
    {
        NntpAccountSnapshot initial = BuildAccount(Guid.Parse("11111111-1111-1111-1111-111111111111"));
        NntpAccountSnapshot refreshed = BuildAccount(Guid.Parse("22222222-2222-2222-2222-222222222222"));

        int queryCallCount = 0;
        MySqlNntpAccountSnapshotProvider provider = new(
            1,
            NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
            _ =>
            {
                queryCallCount++;
                return Task.FromResult<List<NntpAccountSnapshot>>(queryCallCount == 1 ? [initial] : [refreshed]);
            });

        await provider.LoadInitialSnapshotAsync(CancellationToken.None);
        NntpAccountSnapshotState initialSnapshot = provider.CurrentSnapshot;

        bool refreshedPublished = await provider.RefreshSnapshotAsync(CancellationToken.None);

        Assert.True(refreshedPublished);
        Assert.Single(initialSnapshot.Accounts);
        Assert.Equal(initial.EntryId, initialSnapshot.Accounts[0].EntryId);
        Assert.Single(provider.CurrentSnapshot.Accounts);
        Assert.Equal(refreshed.EntryId, provider.CurrentSnapshot.Accounts[0].EntryId);
    }

    [Fact]
    public async Task RefreshSnapshotAsync_WhenRefreshFails_PreservesPreviousSnapshot()
    {
        NntpAccountSnapshot initial = BuildAccount(Guid.Parse("33333333-3333-3333-3333-333333333333"));

        int queryCallCount = 0;
        MySqlNntpAccountSnapshotProvider provider = new(
            1,
            NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
            _ =>
            {
                queryCallCount++;
                if (queryCallCount == 1)
                {
                    return Task.FromResult<List<NntpAccountSnapshot>>([initial]);
                }

                throw new InvalidOperationException("simulated refresh failure");
            });

        await provider.LoadInitialSnapshotAsync(CancellationToken.None);
        NntpAccountSnapshotState baseline = provider.CurrentSnapshot;

        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.RefreshSnapshotAsync(CancellationToken.None));

        Assert.Same(baseline, provider.CurrentSnapshot);
        Assert.Single(provider.CurrentSnapshot.Accounts);
        Assert.Equal(initial.EntryId, provider.CurrentSnapshot.Accounts[0].EntryId);
    }

    [Fact]
    public async Task RefreshSnapshotAsync_WhenConcurrentCallOccurs_SkipsOverlap()
    {
        NntpAccountSnapshot initial = BuildAccount(Guid.Parse("44444444-4444-4444-4444-444444444444"));
        TaskCompletionSource unblock = new(TaskCreationOptions.RunContinuationsAsynchronously);

        MySqlNntpAccountSnapshotProvider provider = new(
            1,
            NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
            async _ =>
            {
                await unblock.Task;
                return [initial];
            });

        Task<bool> firstRefresh = provider.RefreshSnapshotAsync(CancellationToken.None);
        Task<bool> secondRefresh = provider.RefreshSnapshotAsync(CancellationToken.None);

        bool secondResult = await secondRefresh;
        Assert.False(secondResult);

        unblock.SetResult();
        bool firstResult = await firstRefresh;

        Assert.True(firstResult);
    }

    [Fact]
    public async Task RefreshSnapshotAsync_WhenCanceled_ThrowsOperationCanceledException()
    {
        MySqlNntpAccountSnapshotProvider provider = new(
            1,
            NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
            static cancellationToken =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult<List<NntpAccountSnapshot>>([]);
            });

        using CancellationTokenSource cts = new();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.RefreshSnapshotAsync(cts.Token));
    }

    [Fact]
    public async Task RefreshSnapshotAsync_LogsDoNotContainCredentials()
    {
        NntpAccountSnapshot account = BuildAccount(Guid.Parse("55555555-5555-5555-5555-555555555555"));
        TestLogger<MySqlNntpAccountSnapshotProvider> logger = new();

        MySqlNntpAccountSnapshotProvider provider = new(
            1,
            logger,
            _ => Task.FromResult<List<NntpAccountSnapshot>>([account]));

        _ = await provider.RefreshSnapshotAsync(CancellationToken.None);

        string combined = string.Join("\n", logger.Messages);
        Assert.DoesNotContain("user", combined, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("secret", combined, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("password", combined, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RefreshSnapshotAsync_WhenSuccessful_ReturnsSnapshotUsingConfiguredServerId()
    {
        MySqlNntpAccountSnapshotProvider provider = new(
            9,
            NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
            static _ => Task.FromResult<List<NntpAccountSnapshot>>([]));

        bool refreshed = await provider.RefreshSnapshotAsync(CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal((byte)9, provider.CurrentSnapshot.ServerId);
    }

    private static NntpAccountSnapshot BuildAccount(Guid entryId)
    {
        return new NntpAccountSnapshot(
            EntryId: entryId,
            Backbone: "BackboneA",
            Hostname: "news.example.com",
            KeepAliveSeconds: 30,
            MaxConnections: 10,
            Password: "secret",
            Port: 563,
            ServerId: 1,
            Username: "user",
            UseSsl: true);
    }

    private sealed class TestLogger<T> : ILogger<T>
    {
        internal List<string> Messages { get; } = [];

        IDisposable? ILogger.BeginScope<TState>(TState state)
        {
            return null;
        }

        bool ILogger.IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        void ILogger.Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Messages.Add(formatter(state, exception));
        }
    }
}
