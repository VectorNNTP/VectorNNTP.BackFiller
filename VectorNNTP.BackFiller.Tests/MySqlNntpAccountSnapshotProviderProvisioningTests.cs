// <copyright file="MySqlNntpAccountSnapshotProviderProvisioningTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests startup provisioning contract for MySQL account dependencies.
    /// </summary>
    public sealed class MySqlNntpAccountSnapshotProviderProvisioningTests
    {
        [Fact]
        public async Task EnsureStartupDependenciesAsync_UsesConfiguredDatabaseAndTableAndAuthoritativeSchema()
        {
            CapturingProvisioningStore store = new();

            MySqlNntpAccountSnapshotProvider provider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);

            _ = Assert.Single(store.Calls);
            (string databaseName, string tableName, string createTableSql) call = store.Calls[0];
            Assert.Equal("test", call.databaseName);
            Assert.Equal("nntpbackfilleraccounts", call.tableName);
            Assert.Equal(MySqlNntpAccountSnapshotProvider.AccountsTableCreateSql, call.createTableSql);

            Assert.Contains("CREATE TABLE IF NOT EXISTS `nntpbackfilleraccounts`", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`entryid` char(36) NOT NULL", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`backbone` enum('Abavia','Altopia','BaseIP','Eweka','Elbracht','Giganews','GTT','Highwinds','ItsHosted','Novia','UExpress','UsenetNode1') NOT NULL", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`hostname` varchar(150) NOT NULL", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`keepalive` tinyint unsigned NOT NULL DEFAULT '120'", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`maxconnections` tinyint unsigned NOT NULL", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`password` varchar(45) NOT NULL", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`port` smallint unsigned NOT NULL DEFAULT '119'", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`serverid` tinyint unsigned NOT NULL", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`username` varchar(45) NOT NULL", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("`usessl` enum('y','n') NOT NULL DEFAULT 'n'", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("PRIMARY KEY (`entryid`)", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains("KEY `idx_serverid` (`serverid`)", call.createTableSql, StringComparison.Ordinal);
            Assert.Contains(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", call.createTableSql, StringComparison.Ordinal);
        }

        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenRepeated_RemainsIdempotentAtProviderBoundary()
        {
            CapturingProvisioningStore store = new();

            MySqlNntpAccountSnapshotProvider provider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);
            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);

            Assert.Equal(2, store.Calls.Count);
            Assert.All(store.Calls, call =>
            {
                Assert.Equal("nntpbackfilleraccounts", call.tableName);
                Assert.Equal(MySqlNntpAccountSnapshotProvider.AccountsTableCreateSql, call.createTableSql);
            });
        }

        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenProvisioningFails_PropagatesFailure()
        {
            MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore store =
                new DelegateProvisioningStore(static _ => throw new InvalidOperationException("permission denied"));

            MySqlNntpAccountSnapshotProvider provider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            _ = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.EnsureStartupDependenciesAsync(CancellationToken.None));
        }

        [Fact]
        public void AccountsTableCreateSql_UsesNewBackfillerTableNameAndNotLegacyName()
        {
            Assert.Contains("nntpbackfilleraccounts", MySqlNntpAccountSnapshotProvider.AccountsTableCreateSql, StringComparison.Ordinal);
            Assert.DoesNotContain("nntpgrabberaccounts", MySqlNntpAccountSnapshotProvider.AccountsTableCreateSql, StringComparison.Ordinal);
        }

        private sealed class CapturingProvisioningStore : MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore
        {
            internal List<(string databaseName, string tableName, string createTableSql)> Calls { get; } = [];

            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken)
            {
                Calls.Add((databaseName, tableName, createTableSql));
                return Task.CompletedTask;
            }
        }

        private sealed class DelegateProvisioningStore(Func<CancellationToken, Task> callback) : MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore
        {
            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken)
            {
                return callback(cancellationToken);
            }
        }
    }
}
