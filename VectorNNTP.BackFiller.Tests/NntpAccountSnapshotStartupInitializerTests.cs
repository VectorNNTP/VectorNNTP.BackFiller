// <copyright file="NntpAccountSnapshotStartupInitializerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for nntp account snapshot startup initializer, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the nntp account snapshot startup initializer test suite.

using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests startup initializer behavior for NNTP account snapshot loading.
    /// </summary>
    public sealed class NntpAccountSnapshotStartupInitializerTests
    {
        /// <summary>
        /// Confirms the start async when entry id provided as string encoded guid publishes snapshot behavior.
        /// </summary>
        [Fact]
        public async Task StartAsync_WhenEntryIdProvidedAsStringEncodedGuid_PublishesSnapshot()
        {
            Guid expectedEntryId = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

            MySqlNntpAccountSnapshotProvider provider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([
                    new NntpAccountSnapshot(
                        EntryId: MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(expectedEntryId.ToString("D")),
                        Backbone: "BackboneA",
                        Hostname: "news.example.com",
                        KeepAliveSeconds: 30,
                        MaxConnections: 10,
                        Password: "secret",
                        Port: 563,
                        ServerId: 1,
                        Username: "user",
                        UseSsl: true)
                ]));

            NntpAccountSnapshotStartupInitializer initializer = new(
                provider,
                NullLogger<NntpAccountSnapshotStartupInitializer>.Instance);

            await initializer.StartAsync(CancellationToken.None);

            _ = Assert.Single(provider.CurrentSnapshot.Accounts);
            Assert.Equal(expectedEntryId, provider.CurrentSnapshot.Accounts[0].EntryId);
        }
        /// <summary>
        /// Confirms the start async provisions before initial snapshot load behavior.
        /// </summary>
        [Fact]
        public async Task StartAsync_ProvisionsBeforeInitialSnapshotLoad()
        {
            List<string> operations = [];

            MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore provisioningStore =
                new DelegateProvisioningStore(_ =>
                {
                    operations.Add("provision");
                    return Task.CompletedTask;
                });

            MySqlNntpAccountSnapshotProvider provider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ =>
                {
                    operations.Add("load");
                    return Task.FromResult<List<NntpAccountSnapshot>>([]);
                },
                provisioningStore);

            NntpAccountSnapshotStartupInitializer initializer = new(
                provider,
                NullLogger<NntpAccountSnapshotStartupInitializer>.Instance);

            await initializer.StartAsync(CancellationToken.None);

            Assert.Equal(["provision", "load"], operations);
        }
        /// <summary>
        /// Confirms the start async when provisioning fails throws and does not load snapshot behavior.
        /// </summary>
        [Fact]
        public async Task StartAsync_WhenProvisioningFails_ThrowsAndDoesNotLoadSnapshot()
        {
            List<string> operations = [];

            MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore provisioningStore =
                new DelegateProvisioningStore(_ =>
                {
                    operations.Add("provision");
                    throw new InvalidOperationException("provisioning failed");
                });

            MySqlNntpAccountSnapshotProvider provider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ =>
                {
                    operations.Add("load");
                    return Task.FromResult<List<NntpAccountSnapshot>>([]);
                },
                provisioningStore);

            NntpAccountSnapshotStartupInitializer initializer = new(
                provider,
                NullLogger<NntpAccountSnapshotStartupInitializer>.Instance);

            _ = await Assert.ThrowsAsync<InvalidOperationException>(() => initializer.StartAsync(CancellationToken.None));
            Assert.Equal(["provision"], operations);
        }

        /// <summary>
        /// Confirms the delegate provisioning store behavior.
        /// </summary>
        /// <returns>The value returned by the delegate provisioning store helper.</returns>
        /// <summary>
        /// Confirms the delegate provisioning store behavior.
        /// </summary>
        /// <param name="CancellationToken">The cancellation token used by this test scenario.</param>
        /// <param name="callback">The callback used by this test scenario.</param>
        /// <returns>The value returned by the delegate provisioning store helper.</returns>
        private sealed class DelegateProvisioningStore(Func<CancellationToken, Task> callback) : MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore
        {
            /// <summary>
        /// Confirms the ensure database and table async behavior.
            /// </summary>
        /// <returns>The value returned by the ensure database and table async helper.</returns>
        /// <summary>
        /// Confirms the ensure database and table async behavior.
        /// </summary>
        /// <param name="databaseName">The database name used by this test scenario.</param>
        /// <param name="tableName">The table name used by this test scenario.</param>
        /// <param name="createTableSql">The create table sql used by this test scenario.</param>
        /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
        /// <returns>The value returned by the ensure database and table async helper.</returns>
            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken)
            {
                return callback(cancellationToken);
            }
        }
    }
}
