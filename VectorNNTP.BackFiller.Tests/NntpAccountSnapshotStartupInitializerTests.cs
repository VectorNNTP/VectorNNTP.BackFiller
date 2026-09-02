// <copyright file="NntpAccountSnapshotStartupInitializerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for nntp account snapshot startup initializer, covering NNTP article and transport behavior.

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
        /// Exercises start async  when entry id provided as string encoded guid  publishes snapshot behavior, including the expected result and failure semantics.
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
        /// Exercises start async  provisions before initial snapshot load behavior, including the expected result and failure semantics.
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
        /// Exercises start async  when provisioning fails  throws and does not load snapshot behavior, including the expected result and failure semantics.
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
        /// Covers delegate provisioning store behavior and invariants exercised by this test suite.
        /// </summary>
        private sealed class DelegateProvisioningStore(Func<CancellationToken, Task> callback) : MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore
        {
            /// <summary>
            /// Exercises ensure database and table async behavior, including the expected result and failure semantics.
            /// </summary>
            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken)
            {
                return callback(cancellationToken);
            }
        }
    }
}
