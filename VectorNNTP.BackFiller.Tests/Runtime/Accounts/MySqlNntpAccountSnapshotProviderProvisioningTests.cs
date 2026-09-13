// <copyright file="MySqlNntpAccountSnapshotProviderProvisioningTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for my sql nntp account snapshot provider provisioning, covering NNTP article and transport behavior; dependency integration and failure handling.
// Primary responsibility: documents the executable contracts covered by the my sql nntp account snapshot provider provisioning test suite.

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Startup.Configuration;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Accounts
{
    /// <summary>
    /// Tests startup provisioning contract for MySQL account dependencies.
    /// </summary>
    public sealed class MySqlNntpAccountSnapshotProviderProvisioningTests
    {
        /// <summary>
        /// Confirms the ensure startup dependencies async uses configured database and table and authoritative schema behavior.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_UsesConfiguredDatabaseAndTableAndAuthoritativeSchema()
        {
            CapturingProvisioningStore store = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("Server=mysql-primary;Port=3307;Database=grabber_db_main;User ID=runtime_user;SslMode=Required");

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);

            _ = Assert.Single(store.Calls);
            (string databaseName, string tableName, string createTableSql) call = store.Calls[0];
            Assert.Equal("grabber_db_main", call.databaseName);
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
        /// <summary>
        /// Confirms the ensure startup dependencies async when repeated remains idempotent at provider boundary behavior.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenRepeated_RemainsIdempotentAtProviderBoundary()
        {
            CapturingProvisioningStore store = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("Server=mysql-primary;Port=3307;Database=grabber_db_main;User ID=runtime_user;SslMode=Required");

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
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
        /// <summary>
        /// Confirms the ensure startup dependencies async when provisioning fails propagates failure behavior.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenProvisioningFails_PropagatesFailure()
        {
            MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore store =
                new DelegateProvisioningStore(static _ => throw new InvalidOperationException("permission denied"));
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("Server=mysql-primary;Port=3307;Database=grabber_db_main;User ID=runtime_user;SslMode=Required");

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.EnsureStartupDependenciesAsync(CancellationToken.None));
            Assert.Equal("permission denied", exception.Message);
        }

        /// <summary>
        /// Confirms provider uses frozen runtime projection for startup provisioning target even when mutable inputs change after snapshot creation.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenFrozenRuntimeProjectionProvided_UsesFrozenDatabaseTarget()
        {
            CapturingProvisioningStore store = new();
            IConfigurationRoot configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=mysql-a;Port=3306;Database=DatabaseA;User ID=usera;Password=secret;SslMode=Required",
            });
            BackFillerOptions backFiller = configuration.GetSection("BackFiller").Get<BackFillerOptions>()
                ?? throw new InvalidOperationException("BackFiller section is required for this test scenario.");
            List<(string Setting, string Error)> errors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(
                configuration,
                backFiller,
                errors,
                includeLetsEncryptRuntimeOptions: false);

            Assert.NotNull(runtimeOptions);
            Assert.Empty(errors);

            configuration["ConnectionStrings:GrabberDB"] = "Server=mysql-b;Port=3307;Database=DatabaseB;User ID=userb;Password=secret2;SslMode=None";

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);

            (string databaseName, string tableName, string createTableSql) call = Assert.Single(store.Calls);
            Assert.Equal("DatabaseA", call.databaseName);
            Assert.Equal("nntpbackfilleraccounts", call.tableName);
            Assert.Equal(MySqlNntpAccountSnapshotProvider.AccountsTableCreateSql, call.createTableSql);
            Assert.DoesNotContain(store.Calls, static call => string.Equals(call.databaseName, "DatabaseB", StringComparison.Ordinal));
        }
        /// <summary>
        /// Confirms missing database startup path succeeds when provisioning boundary permits creation.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenDatabaseMissingAndProvisioningAllowed_Completes()
        {
            CapturingProvisioningStore store = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("Server=mysql-primary;Port=3307;Database=missing_db;User ID=runtime_user;SslMode=Required");

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);

            (string databaseName, _, _) = Assert.Single(store.Calls);
            Assert.Equal("missing_db", databaseName);
        }

        /// <summary>
        /// Confirms missing database startup path fails deterministically when provisioning creation is denied.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenDatabaseMissingAndProvisioningDenied_ThrowsDeterministicFailure()
        {
            MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore store =
                new DelegateProvisioningStore(static _ => throw new InvalidOperationException("MySQL startup provisioning failed at stage 'create-database' (Error #1044): Unable to create or verify the target database during startup provisioning."));
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("Server=mysql-primary;Port=3307;Database=missing_db;User ID=runtime_user;SslMode=Required");

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.EnsureStartupDependenciesAsync(CancellationToken.None));
            Assert.Equal(
                MySqlNntpAccountSnapshotProvider.FormatStartupProvisioningFailureMessage(
                    "create-database",
                    1044,
                    "Unable to create or verify the target database during startup provisioning."),
                exception.Message);
        }

        /// <summary>
        /// Confirms existing database path remains successful and provisioning boundary remains idempotent.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenDatabaseAlreadyExists_RemainsSuccessfulAndIdempotent()
        {
            CapturingProvisioningStore store = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("Server=mysql-primary;Port=3307;Database=existing_db;User ID=runtime_user;SslMode=Required");

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);
            await provider.EnsureStartupDependenciesAsync(CancellationToken.None);

            Assert.Equal(2, store.Calls.Count);
            Assert.All(store.Calls, static call => Assert.Equal("existing_db", call.databaseName));
        }

        /// <summary>
        /// Confirms missing required table startup path fails deterministically when table provisioning is denied.
        /// </summary>
        [Fact]
        public async Task EnsureStartupDependenciesAsync_WhenTableMissingAndProvisioningDenied_ThrowsDeterministicFailure()
        {
            MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore store =
                new DelegateProvisioningStore(static _ => throw new InvalidOperationException("MySQL startup provisioning failed at stage 'create-table' (Error #1142): Startup provisioning could not create or validate the required accounts table."));
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("Server=mysql-primary;Port=3307;Database=existing_db;User ID=runtime_user;SslMode=Required");

            MySqlNntpAccountSnapshotProvider provider = new(
                runtimeOptions,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                _ => Task.FromResult<List<NntpAccountSnapshot>>([]),
                store);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.EnsureStartupDependenciesAsync(CancellationToken.None));
            Assert.Equal(
                MySqlNntpAccountSnapshotProvider.FormatStartupProvisioningFailureMessage(
                    "create-table",
                    1142,
                    "Startup provisioning could not create or validate the required accounts table."),
                exception.Message);
        }

        /// <summary>
        /// Confirms startup provisioning failure formatting remains deterministic and preserves stage/error classification.
        /// </summary>
        [Theory]
        [InlineData("server-connect", 2003, "Unable to connect to MySQL server for startup provisioning.")]
        [InlineData("create-database", 1044, "Unable to create or verify the target database during startup provisioning.")]
        [InlineData("select-database", 1049, "Startup provisioning could not access the target database.")]
        [InlineData("create-table", 1142, "Startup provisioning could not create or validate the required accounts table.")]
        public void FormatStartupProvisioningFailureMessage_WhenCalled_ReturnsDeterministicClassifiedMessage(string stage, int errorNumber, string detail)
        {
            string message = MySqlNntpAccountSnapshotProvider.FormatStartupProvisioningFailureMessage(stage, errorNumber, detail);

            Assert.Equal($"MySQL startup provisioning failed at stage '{stage}' (Error #{errorNumber}): {detail}", message);
        }

        /// <summary>
        /// Confirms the accounts table create sql uses new backfiller table name and not legacy name behavior.
        /// </summary>
        [Fact]
        public void AccountsTableCreateSql_UsesNewBackfillerTableNameAndNotLegacyName()
        {
            Assert.Contains("nntpbackfilleraccounts", MySqlNntpAccountSnapshotProvider.AccountsTableCreateSql, StringComparison.Ordinal);
            Assert.DoesNotContain("nntpgrabberaccounts", MySqlNntpAccountSnapshotProvider.AccountsTableCreateSql, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms the capturing provisioning store behavior.
        /// </summary>
        private sealed class CapturingProvisioningStore : MySqlNntpAccountSnapshotProvider.IStartupProvisioningStore
        {
            internal List<(string databaseName, string tableName, string createTableSql)> Calls { get; } = [];

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
                Calls.Add((databaseName, tableName, createTableSql));
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Confirms the delegate provisioning store behavior.
        /// </summary>
        /// <returns>The value returned by the delegate provisioning store helper.</returns>
        /// <summary>
        /// Confirms the delegate provisioning store behavior.
        /// </summary>
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

        /// <summary>
        /// Creates runtime options with a deterministic frozen GrabberDB projection for provider-construction tests.
        /// </summary>
        /// <param name="connectionString">Connection string used for the frozen runtime projection.</param>
        /// <returns>A runtime options instance with the requested frozen GrabberDB projection.</returns>
        private static BackFillerRuntimeOptions CreateRuntimeOptions(string connectionString)
        {
            MySqlConnector.MySqlConnectionStringBuilder builder = new(connectionString);

            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "bf-1.example.com",
                BackFillerId: 1,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false)
            {
                GrabberDb = new GrabberDbRuntimeOptions(
                    ConnectionString: builder.ConnectionString,
                    Server: builder.Server,
                    Port: builder.Port,
                    Database: builder.Database,
                    UserId: builder.UserID,
                    SslMode: builder.SslMode),
            };
        }

        private static IConfigurationRoot BuildConfiguration(Dictionary<string, string?> values)
        {
            Dictionary<string, string?> baseline = new(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "1",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirLogs"] = Path.Combine(Path.GetTempPath(), "logs"),
                ["BackFiller:DirCerts"] = Path.Combine(Path.GetTempPath(), "certs"),
                ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "ops@example.com",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "account.key",
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = "secret",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:Hosts:0"] = "localhost",
                ["BackFiller:RabbitMQ:Port"] = "5672",
                ["BackFiller:RabbitMQ:EnableSsl"] = "false",
                ["BackFiller:RabbitMQ:Username"] = "nntparticles",
                ["BackFiller:RabbitMQ:Password"] = "password",
                ["BackFiller:RabbitMQ:VirtualHost"] = "/",
                ["BackFiller:TransitServer:Host"] = "localhost",
                ["BackFiller:TransitServer:Port"] = "119",
                ["BackFiller:TransitServer:UseSsl"] = "false",
            };

            foreach (KeyValuePair<string, string?> kv in values)
            {
                baseline[kv.Key] = kv.Value;
            }

            return new ConfigurationBuilder()
                .AddInMemoryCollection(baseline)
                .Build();
        }
    }
}
