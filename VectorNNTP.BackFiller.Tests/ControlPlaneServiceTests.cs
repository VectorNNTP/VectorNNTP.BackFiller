using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Tests.TestInfrastructure;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests the control-plane startup contract and steady-state service semantics.
    /// </summary>
    public sealed class ControlPlaneServiceTests
    {
        /// <summary>
        /// Confirms startup initialization succeeds when no desired accounts are configured.
        /// </summary>
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

        /// <summary>
        /// Confirms startup cancellation preserves incomplete initialization state.
        /// </summary>
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

            OperationCanceledException canceledException = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.StartAsync(cancellationTokenSource.Token));
            Assert.NotNull(canceledException);
            Assert.False(service.IsStartupInitializationComplete);
        }

        /// <summary>
        /// Verifies shutdown cancellation during startup does not emit account-add failure warnings.
        /// </summary>
        [Fact]
        public async Task StartAsync_WhenShutdownCancellationOccurs_DoesNotLogAccountAddFailedWarningAsync()
        {
            TaskCompletionSource connectionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            FakeNntpServer server = await FakeNntpServer.StartAsync(async (_, cancellationToken) =>
            {
                connectionAccepted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }).ConfigureAwait(false);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port, username: "user", password: "pass"),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            CapturingLoggerProvider loggerProvider = new();
            ControlPlaneService service = new(
                loggerProvider.CreateLogger<ControlPlaneService>(),
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider,
                loggerProvider);

            using CancellationTokenSource startupCancellation = new();
            Task startTask = service.StartAsync(startupCancellation.Token);

            await connectionAccepted.Task.ConfigureAwait(false);
            startupCancellation.Cancel();

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await startTask.ConfigureAwait(false)).ConfigureAwait(false);
            Assert.False(service.IsStartupInitializationComplete);

            Assert.DoesNotContain(
                loggerProvider.Entries,
                static entry => entry.Level == LogLevel.Warning &&
                                entry.Message.Contains("Account add failed", StringComparison.Ordinal));

            Assert.DoesNotContain(
                loggerProvider.Entries,
                static entry => entry.Level == LogLevel.Information &&
                                entry.Message.Contains("Account reconciliation completed", StringComparison.Ordinal));

            Assert.DoesNotContain(
                loggerProvider.Entries,
                static entry => entry.Level == LogLevel.Information &&
                                entry.Message.Contains("Control plane startup initialization completed", StringComparison.Ordinal));

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms startup reconciliation creates one account runtime and converges session capacity to configured max connections.
        /// </summary>
        [Fact]
        public async Task StartAsync_WhenSnapshotContainsAccount_ConvergesToConfiguredPersistentCapacity()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 2).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 2, port: server.Port),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.ManagedAccountCount);
            Assert.Equal(2, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(2, server.AcceptedConnectionCount);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms account removal during refresh removes the managed runtime and retires all owned sessions.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenAccountRemoved_DisposesManagedRuntime()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 1).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.ManagedAccountCount);
            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));

            desiredAccounts.Clear();

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(0, service.ManagedAccountCount);
            Assert.Equal(0, service.GetManagedAccountActiveSessionCount(accountId));
            await WaitForConditionAsync(() => server.ActiveConnectionCount == 0);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms unchanged desired state remains idempotent without account-runtime churn.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenConfigurationUnchanged_RemainsIdempotent()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 2).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 2, port: server.Port),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            int managedCountBefore = service.ManagedAccountCount;
            int activeBefore = service.GetManagedAccountActiveSessionCount(accountId);
            int acceptedConnectionsBefore = server.AcceptedConnectionCount;

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(managedCountBefore, service.ManagedAccountCount);
            Assert.Equal(activeBefore, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(acceptedConnectionsBefore, server.AcceptedConnectionCount);
            Assert.Equal(0, server.AuthInfoUserCommandCount);
            Assert.Equal(0, server.AuthInfoPassCommandCount);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms increasing max-connections establishes only the additional persistent sessions.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenMaxConnectionsIncreases_AddsRequiredSessionsOnly()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 3).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(1, server.AcceptedConnectionCount);

            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 3, port: server.Port);

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(3, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(3, server.AcceptedConnectionCount);
            Assert.Equal(0, server.AuthInfoUserCommandCount);
            Assert.Equal(0, server.AuthInfoPassCommandCount);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms adding a second account creates an independent managed runtime and persistent capacity.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenNewAccountAppears_CreatesIndependentRuntime()
        {
            FakeNntpServer firstServer = await FakeNntpServer.StartAsync(acceptConnectionCount: 1).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable firstServerLease = firstServer.ConfigureAwait(true);
            FakeNntpServer secondServer = await FakeNntpServer.StartAsync(acceptConnectionCount: 2).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable secondServerLease = secondServer.ConfigureAwait(true);

            Guid firstAccountId = Guid.NewGuid();
            Guid secondAccountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(firstAccountId, maxConnections: 1, port: firstServer.Port),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.ManagedAccountCount);
            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(firstAccountId));

            desiredAccounts.Add(CreateAccountSnapshot(secondAccountId, maxConnections: 2, port: secondServer.Port));

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(2, service.ManagedAccountCount);
            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(firstAccountId));
            Assert.Equal(2, service.GetManagedAccountActiveSessionCount(secondAccountId));

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms reducing desired persistent capacity retires only the excess sessions.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenMaxConnectionsDecreases_RetiresExcessSessionsOnly()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 5).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 5, port: server.Port),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            CapturingLoggerProvider loggerProvider = new();
            ControlPlaneService service = new(
                loggerProvider.CreateLogger<ControlPlaneService>(),
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider,
                loggerProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(5, service.GetManagedAccountActiveSessionCount(accountId));

            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 2, port: server.Port);

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(2, service.GetManagedAccountActiveSessionCount(accountId));
            await WaitForConditionAsync(() => server.ActiveConnectionCount == 2);

            Assert.Contains(
                loggerProvider.Entries,
                entry => entry.Level == LogLevel.Information &&
                         entry.Message.Contains("Account reconciled capacity:", StringComparison.Ordinal) &&
                         entry.Message.Contains($"AccountId={accountId}", StringComparison.Ordinal) &&
                         entry.Message.Contains("RetiredSessions=3", StringComparison.Ordinal));

            Assert.DoesNotContain(
                loggerProvider.Entries,
                entry => entry.Level == LogLevel.Information &&
                         entry.Message.Contains("Account reconciled retired sessions:", StringComparison.Ordinal));

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms keepalive-only changes do not recreate existing sessions.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenKeepAliveChanges_DoesNotReconnectOrReauthenticate()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 2).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 2, port: server.Port, keepAliveSeconds: 240),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            int acceptedBefore = server.AcceptedConnectionCount;
            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 2, port: server.Port, keepAliveSeconds: 180);

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(acceptedBefore, server.AcceptedConnectionCount);
            Assert.Equal(2, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(0, server.AuthInfoUserCommandCount);
            Assert.Equal(0, server.AuthInfoPassCommandCount);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms username and password changes force recreation and reauthentication with updated credentials.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenCredentialsChange_RecreatesAndReauthenticates()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 2).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port, username: "user-a", password: "pass-a"),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(1, server.AuthInfoUserCommandCount);
            Assert.Equal(1, server.AuthInfoPassCommandCount);

            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port, username: "user-b", password: "pass-b");

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(2, server.AcceptedConnectionCount);
            Assert.Equal(2, server.AuthInfoUserCommandCount);
            Assert.Equal(2, server.AuthInfoPassCommandCount);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms hostname changes force session replacement to the new endpoint.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenHostnameChanges_ReplacesSessions()
        {
            FakeNntpServer firstServer = await FakeNntpServer.StartAsync(acceptConnectionCount: 1).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable firstServerLease = firstServer.ConfigureAwait(true);
            FakeNntpServer secondServer = await FakeNntpServer.StartAsync(acceptConnectionCount: 1).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable secondServerLease = secondServer.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 1, port: firstServer.Port, hostname: "127.0.0.1"),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));

            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 1, port: secondServer.Port, hostname: "localhost");

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(1, firstServer.AcceptedConnectionCount);
            Assert.Equal(1, secondServer.AcceptedConnectionCount);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Confirms SSL transitions retire old plaintext sessions and converge to one authenticated TLS session.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenUseSslChanges_ReplacesAffectedSessions()
        {
            FakeNntpServer server = await FakeNntpServer.StartWithTransportPlanAsync(
                FakeNntpServer.ConnectionTransport.Plaintext,
                FakeNntpServer.ConnectionTransport.ImplicitTls).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port, hostname: "localhost", useSsl: false, username: "user-a", password: "pass-a"),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider,
                serverCertificateValidationCallback: server.ServerCertificateValidationCallback);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(1, server.PlaintextConnectionCount);
            Assert.Equal(0, server.TlsConnectionCount);
            Assert.Equal(1, server.AuthInfoUserCommandCount);
            Assert.Equal(1, server.AuthInfoPassCommandCount);

            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port, hostname: "localhost", useSsl: true, username: "user-a", password: "pass-a");

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(2, server.AcceptedConnectionCount);
            Assert.Equal(1, server.PlaintextConnectionCount);
            Assert.Equal(1, server.TlsConnectionCount);
            Assert.Equal(2, server.AuthInfoUserCommandCount);
            Assert.Equal(2, server.AuthInfoPassCommandCount);

            await service.StopAsync(CancellationToken.None);
            await WaitForConditionAsync(() => server.ActiveConnectionCount == 0);
        }

        /// <summary>
        /// Confirms partial persistent-session establishment failure preserves healthy capacity and later convergence.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenOneConnectionFails_KeepsHealthySessionsAndConvergesLater()
        {
            FakeNntpServer server = await FakeNntpServer.StartWithTransportPlanAsync(
                FakeNntpServer.ConnectionTransport.Plaintext,
                FakeNntpServer.ConnectionTransport.Plaintext,
                FakeNntpServer.ConnectionTransport.ImmediateClose).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 0, port: 1),
        ];
            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            Assert.Equal(0, service.GetManagedAccountActiveSessionCount(accountId));

            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 3, port: server.Port);

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(2, service.GetManagedAccountActiveSessionCount(accountId));
            Assert.Equal(3, server.AcceptedConnectionCount);

            await service.StopAsync(CancellationToken.None);
            await WaitForConditionAsync(() => server.ActiveConnectionCount == 0);
        }

        /// <summary>
        /// Confirms disabling an account removes managed runtime and closes sessions.
        /// </summary>
        [Fact]
        public async Task RefreshAndReconcileOnceAsync_WhenAccountDisabled_RemovesRuntimeAndDisposesSessions()
        {
            FakeNntpServer server = await FakeNntpServer.StartAsync(acceptConnectionCount: 1).ConfigureAwait(true);
            await using ConfiguredAsyncDisposable serverLease = server.ConfigureAwait(true);

            Guid accountId = Guid.NewGuid();
            List<NntpAccountSnapshot> desiredAccounts =
            [
                CreateAccountSnapshot(accountId, maxConnections: 1, port: server.Port),
        ];

            MySqlNntpAccountSnapshotProvider snapshotProvider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult(desiredAccounts));

            await snapshotProvider.LoadInitialSnapshotAsync(CancellationToken.None);

            ControlPlaneService service = new(
                NullLogger<ControlPlaneService>.Instance,
                new FixedTimeProvider(new DateTimeOffset(2026, 8, 15, 0, 0, 0, TimeSpan.Zero)),
                snapshotProvider);

            await service.StartAsync(CancellationToken.None);

            Assert.Equal(1, service.GetManagedAccountActiveSessionCount(accountId));

            desiredAccounts[0] = CreateAccountSnapshot(accountId, maxConnections: 0, port: server.Port);

            await service.RefreshAndReconcileOnceAsync(CancellationToken.None);

            Assert.Equal(0, service.ManagedAccountCount);
            Assert.Equal(0, service.GetManagedAccountActiveSessionCount(accountId));
            await WaitForConditionAsync(() => server.ActiveConnectionCount == 0);

            await service.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Waits until a condition is true or fails after a deterministic bounded timeout.
        /// </summary>
        /// <param name="condition">Condition to evaluate.</param>
        /// <returns>A task that completes when the condition becomes true.</returns>
        private static async Task WaitForConditionAsync(Func<bool> condition)
        {
            ArgumentNullException.ThrowIfNull(condition);

            const int MaxAttempts = 40;
            for (int attempt = 0; attempt < MaxAttempts; attempt++)
            {
                if (condition())
                {
                    return;
                }

                await Task.Delay(25).ConfigureAwait(false);
            }

            Assert.True(condition(), "Condition did not become true within the expected wait window.");
        }

        /// <summary>
        /// Creates one deterministic account snapshot for control-plane reconciliation tests.
        /// </summary>
        /// <param name="entryId">Stable account entry identifier.</param>
        /// <param name="maxConnections">Desired persistent capacity.</param>
        /// <param name="port">Loopback test server port.</param>
        /// <param name="hostname">NNTP host name.</param>
        /// <param name="keepAliveSeconds">Configured keepalive seconds.</param>
        /// <param name="username">Optional NNTP username.</param>
        /// <param name="password">Optional NNTP password.</param>
        /// <param name="useSsl">Whether SSL/TLS should be used.</param>
        /// <returns>Immutable account snapshot.</returns>
        private static NntpAccountSnapshot CreateAccountSnapshot(
            Guid entryId,
            byte maxConnections,
            ushort port,
            string hostname = "127.0.0.1",
            byte keepAliveSeconds = 30,
            string username = "",
            string password = "",
            bool useSsl = false)
        {
            return new NntpAccountSnapshot(
                EntryId: entryId,
                Backbone: "Giganews",
                Hostname: hostname,
                KeepAliveSeconds: keepAliveSeconds,
                MaxConnections: maxConnections,
                Password: password,
                Port: port,
                ServerId: 1,
                Username: username,
                UseSsl: useSsl);
        }

        /// <summary>
        /// Minimal fake NNTP server that accepts configured persistent connections and performs deterministic protocol handling.
        /// </summary>
        /// <remarks>
        /// Keeps accepted sockets open for the test lifetime so control-plane managed persistent sessions remain connected.
        /// </remarks>
        /// <summary>
        /// Captured control-plane log entry used for cancellation-path log assertions.
        /// </summary>
        /// <param name="Level">Log severity level.</param>
        /// <param name="Message">Rendered log message.</param>
        private sealed record CapturedLogEntry(LogLevel Level, string Message);

        /// <summary>
        /// In-memory logger provider for deterministic control-plane log assertions.
        /// </summary>
        private sealed class CapturingLoggerProvider : ILoggerFactory, ILoggerProvider
        {
            /// <summary>
            /// Synchronization gate for captured log entry writes.
            /// </summary>
            private readonly object _gate = new();

            /// <summary>
            /// Captured log entries.
            /// </summary>
            internal List<CapturedLogEntry> Entries { get; } = [];

            /// <summary>
            /// Creates a logger for the specified category.
            /// </summary>
            /// <param name="categoryName">Logger category.</param>
            /// <returns>Capturing logger instance.</returns>
            public ILogger CreateLogger(string categoryName)
            {
                return new CapturingLogger(Entries, _gate);
            }

            /// <summary>
            /// Adds a provider.
            /// </summary>
            /// <param name="provider">Provider instance.</param>
            public void AddProvider(ILoggerProvider provider)
            {
            }

            /// <summary>
            /// Disposes provider resources.
            /// </summary>
            public void Dispose()
            {
            }

            /// <summary>
            /// Creates a typed logger.
            /// </summary>
            /// <typeparam name="T">Logger category type.</typeparam>
            /// <returns>Typed capturing logger.</returns>
            internal ILogger<T> CreateLogger<T>()
            {
                return new CapturingLogger<T>(Entries, _gate);
            }

            /// <summary>
            /// Non-generic capturing logger implementation.
            /// </summary>
            private sealed class CapturingLogger(List<CapturedLogEntry> entries, object gate) : ILogger
            {
                private readonly List<CapturedLogEntry> _entries = entries;
                private readonly object _gate = gate;

                /// <summary>
                /// Begins a logging scope.
                /// </summary>
                /// <typeparam name="TState">Scope state type.</typeparam>
                /// <param name="state">Scope state payload.</param>
                /// <returns>Scope disposable.</returns>
                public IDisposable BeginScope<TState>(TState state)
                    where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
                /// Gets a value indicating whether the log level is enabled.
                /// </summary>
                /// <param name="logLevel">Log level.</param>
                /// <returns>Always true for tests.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return true;
                }

                /// <summary>
                /// Captures one log record.
                /// </summary>
                /// <typeparam name="TState">Structured state type.</typeparam>
                /// <param name="logLevel">Log level.</param>
                /// <param name="eventId">Event identifier.</param>
                /// <param name="state">State payload.</param>
                /// <param name="exception">Associated exception.</param>
                /// <param name="formatter">Message formatter.</param>
                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    lock (_gate)
                    {
                        _entries.Add(new CapturedLogEntry(logLevel, message));
                    }
                }
            }

            /// <summary>
            /// Generic capturing logger implementation.
            /// </summary>
            /// <typeparam name="T">Category type.</typeparam>
            private sealed class CapturingLogger<T>(List<CapturedLogEntry> entries, object gate) : ILogger<T>
            {
                private readonly CapturingLogger _inner = new(entries, gate);

                /// <summary>
                /// Begins a logging scope.
                /// </summary>
                /// <typeparam name="TState">Scope state type.</typeparam>
                /// <param name="state">Scope state payload.</param>
                /// <returns>Scope disposable.</returns>
                public IDisposable BeginScope<TState>(TState state)
                    where TState : notnull
                {
                    return _inner.BeginScope(state);
                }

                /// <summary>
                /// Gets a value indicating whether the level is enabled.
                /// </summary>
                /// <param name="logLevel">Log level.</param>
                /// <returns>Always true for tests.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return _inner.IsEnabled(logLevel);
                }

                /// <summary>
                /// Captures one log record.
                /// </summary>
                /// <typeparam name="TState">Structured state type.</typeparam>
                /// <param name="logLevel">Log level.</param>
                /// <param name="eventId">Event identifier.</param>
                /// <param name="state">State payload.</param>
                /// <param name="exception">Associated exception.</param>
                /// <param name="formatter">Message formatter.</param>
                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    _inner.Log(logLevel, eventId, state, exception, formatter);
                }
            }

            /// <summary>
            /// Null logging scope.
            /// </summary>
            private sealed class NullScope : IDisposable
            {
                /// <summary>
                /// Singleton instance.
                /// </summary>
                internal static readonly NullScope Instance = new();

                /// <summary>
                /// Disposes the scope.
                /// </summary>
                public void Dispose()
                {
                }
            }
        }

        private sealed class FakeNntpServer : IAsyncDisposable
        {
            /// <summary>
            /// Transport mode for one accepted fake connection.
            /// </summary>
            internal enum ConnectionTransport
            {
                /// <summary>
                /// Plaintext NNTP transport without TLS wrapping.
                /// </summary>
                Plaintext = 0,

                /// <summary>
                /// Accepted connection is closed immediately without sending a greeting.
                /// </summary>
                ImmediateClose = 1,

                /// <summary>
                /// Implicit TLS transport where the connection starts with a TLS handshake.
                /// </summary>
                ImplicitTls = 2,
            }

            /// <summary>
            /// Listener socket for loopback NNTP test connections.
            /// </summary>
            private readonly TcpListener _listener;

            /// <summary>
            /// Cancellation source used to stop the accept loop.
            /// </summary>
            private readonly CancellationTokenSource _shutdown = new();

            /// <summary>
            /// Accept loop task.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
            /// Maximum number of connections to accept before completing the loop.
            /// </summary>
            private readonly int _acceptConnectionCount;

            /// <summary>
            /// Per-connection transport selector.
            /// </summary>
            private readonly Func<int, ConnectionTransport> _transportSelector;

            /// <summary>
            /// Shared transit TLS certificate fixture reused for implicit-TLS fake-server sessions.
            /// </summary>
            private readonly TestTlsCertificateFixture? _tlsCertificate;

            /// <summary>
            /// Optional per-connection callback used by cancellation-focused startup tests.
            /// </summary>
            private Func<TcpClient, CancellationToken, Task>? _connectionHandler;

            /// <summary>
            /// Running accepted connection count.
            /// </summary>
            private int _acceptedConnectionCount;

            /// <summary>
            /// Running active connection count.
            /// </summary>
            private int _activeConnectionCount;

            /// <summary>
            /// Number of accepted plaintext connections.
            /// </summary>
            private int _plaintextConnectionCount;

            /// <summary>
            /// Number of accepted implicit TLS connections.
            /// </summary>
            private int _tlsConnectionCount;

            /// <summary>
            /// Number of received AUTHINFO USER commands.
            /// </summary>
            private int _authInfoUserCommandCount;

            /// <summary>
            /// Number of received AUTHINFO PASS commands.
            /// </summary>
            private int _authInfoPassCommandCount;

            /// <summary>
            /// Initializes a new fake NNTP server instance.
            /// </summary>
            /// <param name="listener">Bound listener.</param>
            /// <param name="acceptConnectionCount">Maximum number of accepted connections.</param>
            /// <param name="transportSelector">Per-connection transport selector.</param>
            private FakeNntpServer(TcpListener listener, int acceptConnectionCount, Func<int, ConnectionTransport> transportSelector)
            {
                _listener = listener;
                _acceptConnectionCount = acceptConnectionCount;
                _transportSelector = transportSelector;
                _tlsCertificate = RequiresTlsTransport(acceptConnectionCount, transportSelector)
                    ? new TestTlsCertificateFixture()
                    : null;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Starts a fake NNTP server for control-plane reconciliation tests using plaintext transport for all accepted connections.
            /// </summary>
            /// <param name="acceptConnectionCount">Maximum number of accepted connections.</param>
            /// <returns>Started fake server.</returns>
            internal static async Task<FakeNntpServer> StartAsync(int acceptConnectionCount)
            {
                return await StartWithTransportPlanAsync([.. Enumerable.Repeat(ConnectionTransport.Plaintext, acceptConnectionCount)]).ConfigureAwait(false);
            }

            /// <summary>
            /// Starts a fake NNTP server for control-plane tests using plaintext transport and invokes a connection callback for each accepted client.
            /// </summary>
            /// <param name="connectionHandler">Callback invoked for each accepted plaintext client connection.</param>
            /// <returns>Started fake server.</returns>
            internal static async Task<FakeNntpServer> StartAsync(Func<TcpClient, CancellationToken, Task> connectionHandler)
            {
                ArgumentNullException.ThrowIfNull(connectionHandler);

                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                FakeNntpServer server = new(listener, acceptConnectionCount: 1, _ => ConnectionTransport.Plaintext)
                {
                    _connectionHandler = connectionHandler,
                };

                await Task.Delay(20).ConfigureAwait(false);
                return server;
            }

            /// <summary>
            /// Starts a fake NNTP server for control-plane tests with deterministic per-connection transport modes.
            /// </summary>
            /// <param name="transportPlan">Transport mode per accepted connection index.</param>
            /// <returns>Started fake server.</returns>
            internal static async Task<FakeNntpServer> StartWithTransportPlanAsync(params ConnectionTransport[] transportPlan)
            {
                ArgumentNullException.ThrowIfNull(transportPlan);
                if (transportPlan.Length == 0)
                {
                    throw new ArgumentException("At least one transport mode must be provided.", nameof(transportPlan));
                }

                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                FakeNntpServer server = new(listener, transportPlan.Length, connectionIndex => transportPlan[connectionIndex]);
                await Task.Delay(20).ConfigureAwait(false);
                return server;
            }

            /// <summary>
            /// Gets the loopback port for this fake server.
            /// </summary>
            internal ushort Port => checked((ushort)((IPEndPoint)_listener.LocalEndpoint).Port);

            /// <summary>
            /// Gets the number of accepted connections.
            /// </summary>
            internal int AcceptedConnectionCount => Volatile.Read(ref _acceptedConnectionCount);

            /// <summary>
            /// Gets the number of currently active connected clients.
            /// </summary>
            internal int ActiveConnectionCount => Volatile.Read(ref _activeConnectionCount);

            /// <summary>
            /// Gets the number of accepted plaintext connections.
            /// </summary>
            internal int PlaintextConnectionCount => Volatile.Read(ref _plaintextConnectionCount);

            /// <summary>
            /// Gets the number of accepted implicit TLS connections.
            /// </summary>
            internal int TlsConnectionCount => Volatile.Read(ref _tlsConnectionCount);

            /// <summary>
            /// Gets the number of observed AUTHINFO USER commands.
            /// </summary>
            internal int AuthInfoUserCommandCount => Volatile.Read(ref _authInfoUserCommandCount);

            /// <summary>
            /// Gets the number of observed AUTHINFO PASS commands.
            /// </summary>
            internal int AuthInfoPassCommandCount => Volatile.Read(ref _authInfoPassCommandCount);

            /// <summary>
            /// Gets the shared TLS server-certificate validation callback for this fake server fixture.
            /// </summary>
            internal RemoteCertificateValidationCallback? ServerCertificateValidationCallback => _tlsCertificate?.ServerCertificateValidationCallback;

            /// <summary>
            /// Disposes fake server resources and joins the accept loop.
            /// </summary>
            /// <returns>A task that completes when server shutdown is complete.</returns>
            public async ValueTask DisposeAsync()
            {
                _shutdown.Cancel();

                try
                {
                    _listener.Stop();
                }
                catch
                {
                }

                await _acceptLoop.ConfigureAwait(false);
                _shutdown.Dispose();
                _tlsCertificate?.Dispose();
            }

            /// <summary>
            /// Accept loop that dispatches deterministic per-connection transport handlers.
            /// </summary>
            /// <returns>A task that completes after the configured connection count is accepted or shutdown is requested.</returns>
            private async Task AcceptLoopAsync()
            {
                List<TcpClient> clients = [];
                List<Task> connectionTasks = [];

                try
                {
                    for (int i = 0; i < _acceptConnectionCount; i++)
                    {
                        TcpClient client = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                        clients.Add(client);
                        _ = Interlocked.Increment(ref _acceptedConnectionCount);
                        _ = Interlocked.Increment(ref _activeConnectionCount);

                        ConnectionTransport transport = _transportSelector(i);
                        connectionTasks.Add(Task.Run(() => ServeConnectionAsync(client, transport), _shutdown.Token));
                    }

                    await Task.WhenAll(connectionTasks).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
                finally
                {
                    foreach (TcpClient client in clients)
                    {
                        client.Dispose();
                    }
                }
            }

            /// <summary>
            /// Serves one accepted fake NNTP connection and records optional AUTHINFO traffic.
            /// </summary>
            /// <param name="client">Accepted client connection.</param>
            /// <param name="transport">Assigned transport mode for this connection.</param>
            /// <returns>A task that completes when the connection closes or shutdown is requested.</returns>
            private async Task ServeConnectionAsync(TcpClient client, ConnectionTransport transport)
            {
                try
                {
                    if (_connectionHandler is not null)
                    {
                        await _connectionHandler(client, _shutdown.Token).ConfigureAwait(false);
                        return;
                    }

                    using NetworkStream networkStream = client.GetStream();

                    if (transport == ConnectionTransport.ImmediateClose)
                    {
                        client.Dispose();
                        return;
                    }

                    Stream protocolStream = networkStream;

                    if (transport == ConnectionTransport.ImplicitTls)
                    {
                        TestTlsCertificateFixture tlsCertificate = _tlsCertificate ?? throw new InvalidOperationException("TLS transport requires a certificate fixture.");
                        SslStream sslStream = new(networkStream, leaveInnerStreamOpen: false);
                        await sslStream.AuthenticateAsServerAsync(
                            new SslServerAuthenticationOptions
                            {
                                ServerCertificate = tlsCertificate.ServerCertificate,
                                ClientCertificateRequired = false,
                                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                            },
                            _shutdown.Token).ConfigureAwait(false);

                        protocolStream = sslStream;
                        _ = Interlocked.Increment(ref _tlsConnectionCount);
                    }
                    else
                    {
                        _ = Interlocked.Increment(ref _plaintextConnectionCount);
                    }

                    await WriteAsciiLineAsync(protocolStream, "200 ready", _shutdown.Token).ConfigureAwait(false);

                    while (!_shutdown.IsCancellationRequested)
                    {
                        string? line = await ReadAsciiLineOrNullAsync(protocolStream, _shutdown.Token).ConfigureAwait(false);
                        if (line is null)
                        {
                            break;
                        }

                        if (line.StartsWith("AUTHINFO USER ", StringComparison.OrdinalIgnoreCase))
                        {
                            _ = Interlocked.Increment(ref _authInfoUserCommandCount);
                            await WriteAsciiLineAsync(protocolStream, "381 pass required", _shutdown.Token).ConfigureAwait(false);
                            continue;
                        }

                        if (line.StartsWith("AUTHINFO PASS ", StringComparison.OrdinalIgnoreCase))
                        {
                            _ = Interlocked.Increment(ref _authInfoPassCommandCount);
                            await WriteAsciiLineAsync(protocolStream, "281 authentication accepted", _shutdown.Token).ConfigureAwait(false);
                            continue;
                        }

                        if (line.Equals("DATE", StringComparison.OrdinalIgnoreCase))
                        {
                            await WriteAsciiLineAsync(protocolStream, "111 20260826010101", _shutdown.Token).ConfigureAwait(false);
                            continue;
                        }

                        await WriteAsciiLineAsync(protocolStream, "500 command not recognized", _shutdown.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (IOException)
                {
                }
                catch (AuthenticationException)
                {
                }
                finally
                {
                    _ = Interlocked.Decrement(ref _activeConnectionCount);
                }
            }

            /// <summary>
            /// Determines whether any configured accepted connection requires implicit TLS transport.
            /// </summary>
            /// <param name="acceptConnectionCount">Total accepted connection count.</param>
            /// <param name="transportSelector">Per-connection transport selector.</param>
            /// <returns><see langword="true"/> when at least one connection uses implicit TLS; otherwise <see langword="false"/>.</returns>
            private static bool RequiresTlsTransport(int acceptConnectionCount, Func<int, ConnectionTransport> transportSelector)
            {
                for (int i = 0; i < acceptConnectionCount; i++)
                {
                    if (transportSelector(i) == ConnectionTransport.ImplicitTls)
                    {
                        return true;
                    }
                }

                return false;
            }

            /// <summary>
            /// Reads one ASCII protocol line and returns null when the connection closes.
            /// </summary>
            /// <param name="stream">Protocol stream.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>ASCII line without CRLF, or null on EOF.</returns>
            private static async Task<string?> ReadAsciiLineOrNullAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> bytes = [];
                byte[] single = new byte[1];

                while (true)
                {
                    int read = await stream.ReadAsync(single, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        return bytes.Count == 0 ? null : Encoding.ASCII.GetString([.. bytes]);
                    }

                    if (single[0] == (byte)'\n')
                    {
                        break;
                    }

                    bytes.Add(single[0]);
                }

                if (bytes.Count > 0 && bytes[^1] == (byte)'\r')
                {
                    bytes.RemoveAt(bytes.Count - 1);
                }

                return Encoding.ASCII.GetString([.. bytes]);
            }

            /// <summary>
            /// Writes one ASCII protocol line with CRLF.
            /// </summary>
            /// <param name="stream">Protocol stream.</param>
            /// <param name="line">Line to write.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>A task that completes when write and flush finish.</returns>
            private static async Task WriteAsciiLineAsync(Stream stream, string line, CancellationToken cancellationToken)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

        }

        /// <summary>
        /// Fixed UTC time provider for deterministic control-plane timestamp behavior in tests.
        /// </summary>
        /// <param name="utcNow">Deterministic UTC timestamp returned for all calls.</param>
        private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
        {
            /// <summary>
            /// Gets the fixed UTC timestamp configured for this provider instance.
            /// </summary>
            /// <returns>Fixed UTC timestamp.</returns>
            public override DateTimeOffset GetUtcNow()
            {
                return utcNow;
            }
        }
    }
}


