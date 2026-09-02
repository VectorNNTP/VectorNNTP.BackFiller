// <copyright file="MySqlNntpAccountSnapshotProvider.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: my sql nntp account snapshot provider in the runtime accounts subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

using System.Collections.Immutable;
using System.Diagnostics;
using MySqlConnector;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Accounts
{
    /// <summary>
    /// Loads and atomically publishes immutable NNTP account snapshots from the GrabberDB MySQL control-plane database.
    /// </summary>
    /// <remarks>
    /// The provider owns startup provisioning checks, initial snapshot hydration, and periodic refresh publication
    /// for account-consuming runtime components.
    /// </remarks>
    internal sealed partial class MySqlNntpAccountSnapshotProvider
    {
        /// <summary>
        /// Limits accounts table name for my sql nntp account snapshot provider.
        /// </summary>
        internal const string AccountsTableName = "nntpbackfilleraccounts";

        /// <summary>
        /// CREATE TABLE statement used to provision the NNTP accounts table when startup provisioning is enabled.
        /// </summary>
        internal const string AccountsTableCreateSql = "CREATE TABLE IF NOT EXISTS `nntpbackfilleraccounts` (" +
            "`entryid` char(36) NOT NULL," +
            "`backbone` enum('Abavia','Altopia','BaseIP','Eweka','Elbracht','Giganews','GTT','Highwinds','ItsHosted','Novia','UExpress','UsenetNode1') NOT NULL," +
            "`hostname` varchar(150) NOT NULL," +
            "`keepalive` tinyint unsigned NOT NULL DEFAULT '120'," +
            "`maxconnections` tinyint unsigned NOT NULL," +
            "`password` varchar(45) NOT NULL," +
            "`port` smallint unsigned NOT NULL DEFAULT '119'," +
            "`serverid` tinyint unsigned NOT NULL," +
            "`username` varchar(45) NOT NULL," +
            "`usessl` enum('y','n') NOT NULL DEFAULT 'n'," +
            "PRIMARY KEY (`entryid`)," +
            "KEY `idx_serverid` (`serverid`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

        /// <summary>
        /// Limits accounts query for my sql nntp account snapshot provider.
        /// </summary>
        private const string AccountsQuery =
            "SELECT " +
            "entryid, " +
            "backbone, " +
            "hostname, " +
            "keepalive, " +
            "maxconnections, " +
            "password, " +
            "port, " +
            "serverid, " +
            "username, " +
            "usessl " +
            "FROM nntpbackfilleraccounts " +
            "WHERE serverid = @ServerId;";

        /// <summary>
        /// Stores connection string used by my sql nntp account snapshot provider.
        /// </summary>
        private readonly string _connectionString;
        /// <summary>
        /// Stores database name used by my sql nntp account snapshot provider.
        /// </summary>
        private readonly string _databaseName;
        /// <summary>
        /// Stores server id used by my sql nntp account snapshot provider.
        /// </summary>
        private readonly byte _serverId;
        /// <summary>
        /// Supplies the logger used by my sql nntp account snapshot provider.
        /// </summary>
        private readonly ILogger<MySqlNntpAccountSnapshotProvider> _logger;
        /// <summary>
        /// Limits query accounts for my sql nntp account snapshot provider.
        /// </summary>
        private readonly Func<CancellationToken, Task<List<NntpAccountSnapshot>>> _queryAccounts;
        /// <summary>
        /// Stores startup provisioning store used by my sql nntp account snapshot provider.
        /// </summary>
        private readonly IStartupProvisioningStore _startupProvisioningStore;

        /// <summary>
        /// Stores refresh in progress used by my sql nntp account snapshot provider.
        /// </summary>
        private int _refreshInProgress;
        /// <summary>
        /// Stores current snapshot used by my sql nntp account snapshot provider.
        /// </summary>
        private volatile NntpAccountSnapshotState _currentSnapshot;

        /// <summary>
        /// Initializes the provider from runtime configuration for production account snapshot loading.
        /// </summary>
        /// <param name="configuration">Application configuration containing the <c>GrabberDB</c> connection string.</param>
        /// <param name="runtimeOptions">Runtime options supplying the authoritative backfiller server identifier.</param>
        /// <param name="logger">Logger used for startup, refresh, and provisioning diagnostics.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when required runtime configuration values are missing or when the configured backfiller identifier is outside the supported byte range.
        /// </exception>
        public MySqlNntpAccountSnapshotProvider(
            IConfiguration configuration,
            BackFillerRuntimeOptions runtimeOptions,
            ILogger<MySqlNntpAccountSnapshotProvider> logger)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(logger);

            _connectionString = configuration.GetConnectionString("GrabberDB")
                ?? throw new InvalidOperationException("ConnectionStrings:GrabberDB is required for runtime NNTP account loading.");

            MySqlConnectionStringBuilder connectionStringBuilder = new(_connectionString);
            _databaseName = !string.IsNullOrWhiteSpace(connectionStringBuilder.Database)
                ? connectionStringBuilder.Database
                : throw new InvalidOperationException("ConnectionStrings:GrabberDB must include a database name for runtime NNTP account loading.");

            _serverId = runtimeOptions.BackFillerId is >= byte.MinValue and <= byte.MaxValue
                ? (byte)runtimeOptions.BackFillerId
                : throw new InvalidOperationException($"BackFiller.Id must be between {byte.MinValue} and {byte.MaxValue} for {AccountsTableName} query.");

            _logger = logger;
            _queryAccounts = QueryAccountsAsync;
            _startupProvisioningStore = new MySqlStartupProvisioningStore(_connectionString, _logger);
            _currentSnapshot = NntpAccountSnapshotState.Empty(_serverId);
        }

        /// <summary>
        /// Initializes a provider instance for tests or controlled runtime composition with injected dependencies.
        /// </summary>
        /// <param name="serverId">Server identifier used to stamp and filter snapshots.</param>
        /// <param name="logger">Logger used for startup, refresh, and provisioning diagnostics.</param>
        /// <param name="queryAccounts">Delegate that loads account rows from the authoritative store.</param>
        /// <param name="startupProvisioningStore">
        /// Optional startup provisioning implementation; when omitted, a no-op implementation is used.
        /// </param>
        internal MySqlNntpAccountSnapshotProvider(
            byte serverId,
            ILogger<MySqlNntpAccountSnapshotProvider> logger,
            Func<CancellationToken, Task<List<NntpAccountSnapshot>>> queryAccounts,
            IStartupProvisioningStore? startupProvisioningStore = null)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(queryAccounts);

            _connectionString = string.Empty;
            _databaseName = "test";
            _serverId = serverId;
            _logger = logger;
            _queryAccounts = queryAccounts;
            _startupProvisioningStore = startupProvisioningStore ?? NoOpStartupProvisioningStore.Instance;
            _currentSnapshot = NntpAccountSnapshotState.Empty(_serverId);
        }

        /// <summary>
        /// Gets the most recently published immutable account snapshot state.
        /// </summary>
        /// <value>
        /// The current authoritative snapshot visible to runtime consumers for this server identifier.
        /// </value>
        internal NntpAccountSnapshotState CurrentSnapshot => _currentSnapshot;

        /// <summary>
        /// Ensures startup-time MySQL database and table dependencies exist and are accessible.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async Task EnsureStartupDependenciesAsync(CancellationToken cancellationToken)
        {
            await _startupProvisioningStore.EnsureDatabaseAndTableAsync(
                _databaseName,
                AccountsTableName,
                AccountsTableCreateSql,
                cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Loads and publishes the initial runtime account snapshot before account-dependent runtime services proceed.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for startup cancellation.</param>
        /// <returns>A task that completes after the snapshot has been published.</returns>
        internal async Task LoadInitialSnapshotAsync(CancellationToken cancellationToken)
        {
            LogInitialAccountLoadStarting(_logger, _serverId);

            int accountCount = await LoadAndPublishSnapshotAsync(cancellationToken).ConfigureAwait(false);

            LogInitialAccountLoadSucceeded(_logger, _serverId, accountCount);
        }

        /// <summary>
        /// Attempts a periodic snapshot refresh and publishes the updated account state when no other refresh is in progress.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for shutdown-aware refresh.</param>
        /// <returns>
        /// A task whose result is <see langword="true"/> when this call performed and published a refresh,
        /// or <see langword="false"/> when a concurrent refresh was already running.
        /// </returns>
        internal async Task<bool> RefreshSnapshotAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.CompareExchange(ref _refreshInProgress, 1, 0) != 0)
            {
                LogPeriodicRefreshSkippedInProgress(_logger, _serverId);
                return false;
            }

            try
            {
                LogPeriodicRefreshStarting(_logger, _serverId);
                Stopwatch stopwatch = Stopwatch.StartNew();

                int accountCount = await LoadAndPublishSnapshotAsync(cancellationToken).ConfigureAwait(false);

                stopwatch.Stop();
                LogPeriodicRefreshSucceeded(_logger, _serverId, accountCount, stopwatch.ElapsedMilliseconds);
                return true;
            }
            finally
            {
                Volatile.Write(ref _refreshInProgress, 0);
            }
        }

        /// <summary>
        /// Loads account rows from the authoritative source and atomically replaces the published snapshot.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token propagated to account loading and publication checks.</param>
        /// <returns>The number of account entries in the newly published snapshot.</returns>
        private async Task<int> LoadAndPublishSnapshotAsync(CancellationToken cancellationToken)
        {
            List<NntpAccountSnapshot> loadedAccounts = await _queryAccounts(cancellationToken).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();

            ImmutableArray<NntpAccountSnapshot> immutableAccounts = [.. loadedAccounts];
            NntpAccountSnapshotState publishedSnapshot = new(_serverId, immutableAccounts);

            _currentSnapshot = publishedSnapshot;
            return immutableAccounts.Length;
        }

        /// <summary>
        /// Executes the accounts query and materializes each result row into runtime snapshot records.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token used for connection, command, and reader operations.</param>
        /// <returns>The ordered account rows returned for the configured server identifier.</returns>
        private async Task<List<NntpAccountSnapshot>> QueryAccountsAsync(CancellationToken cancellationToken)
        {
            MySqlConnection connection = new(_connectionString);
            await using (connection.ConfigureAwait(false))
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                MySqlCommand command = connection.CreateCommand();
                await using (command.ConfigureAwait(false))
                {
                    command.CommandText = AccountsQuery;
                    _ = command.Parameters.Add(new MySqlParameter("@ServerId", MySqlDbType.UByte) { Value = _serverId });

                    MySqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        List<NntpAccountSnapshot> accounts = [];

                        int entryIdOrdinal = reader.GetOrdinal("entryid");
                        int backboneOrdinal = reader.GetOrdinal("backbone");
                        int hostnameOrdinal = reader.GetOrdinal("hostname");
                        int keepAliveOrdinal = reader.GetOrdinal("keepalive");
                        int maxConnectionsOrdinal = reader.GetOrdinal("maxconnections");
                        int passwordOrdinal = reader.GetOrdinal("password");
                        int portOrdinal = reader.GetOrdinal("port");
                        int serverIdOrdinal = reader.GetOrdinal("serverid");
                        int usernameOrdinal = reader.GetOrdinal("username");
                        int useSslOrdinal = reader.GetOrdinal("usessl");

                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            Guid entryId = ParseEntryIdValue(reader.GetValue(entryIdOrdinal));
                            string backbone = reader.GetString(backboneOrdinal);
                            string hostname = reader.GetString(hostnameOrdinal);
                            byte keepAlive = ParseKeepAliveValue(reader.GetValue(keepAliveOrdinal));
                            byte maxConnections = checked((byte)reader.GetInt32(maxConnectionsOrdinal));
                            string password = reader.GetString(passwordOrdinal);
                            ushort port = checked((ushort)reader.GetInt32(portOrdinal));
                            byte serverId = checked((byte)reader.GetInt32(serverIdOrdinal));
                            string username = reader.GetString(usernameOrdinal);
                            bool useSsl = ParseUseSsl(reader.GetString(useSslOrdinal));

                            accounts.Add(new NntpAccountSnapshot(
                                EntryId: entryId,
                                Backbone: backbone,
                                Hostname: hostname,
                                KeepAliveSeconds: keepAlive,
                                MaxConnections: maxConnections,
                                Password: password,
                                Port: port,
                                ServerId: serverId,
                                Username: username,
                                UseSsl: useSsl));
                        }

                        return accounts;
                    }
                }
            }
        }

        /// <summary>
        /// Normalizes one raw database entry-id value into a runtime GUID identifier.
        /// </summary>
        /// <param name="rawEntryId">Raw entry-id value read from the accounts result set.</param>
        /// <returns>Parsed GUID entry identifier.</returns>
        internal static Guid ParseEntryIdValue(object rawEntryId)
        {
            return rawEntryId switch
            {
                Guid guid => guid,
                string text => ParseEntryId(text),
                _ => throw new InvalidOperationException($"Unexpected {AccountsTableName}.entryid type '{rawEntryId.GetType().FullName}'. Expected GUID or string GUID.")
            };
        }

        /// <summary>
        /// Parses an entry-id text value as a GUID.
        /// </summary>
        /// <param name="rawEntryId">Raw entry-id text value.</param>
        /// <returns>Parsed GUID entry identifier.</returns>
        internal static Guid ParseEntryId(string rawEntryId)
        {
            return Guid.TryParse(rawEntryId, out Guid parsed)
                ? parsed
                : throw new InvalidOperationException($"Invalid {AccountsTableName}.entryid value '{rawEntryId}'. Expected GUID format.");
        }

        /// <summary>
        /// Normalizes one raw keepalive value into the runtime byte representation.
        /// </summary>
        /// <param name="rawKeepAlive">Raw keepalive value read from the accounts result set.</param>
        /// <returns>Parsed keepalive value in seconds.</returns>
        internal static byte ParseKeepAliveValue(object rawKeepAlive)
        {
            return rawKeepAlive switch
            {
                byte value => value,
                sbyte value => checked((byte)value),
                short value => checked((byte)value),
                ushort value => checked((byte)value),
                int value => checked((byte)value),
                uint value => checked((byte)value),
                long value => checked((byte)value),
                ulong value => checked((byte)value),
                DBNull => throw new InvalidOperationException($"Unexpected {AccountsTableName}.keepalive NULL value. Expected non-null tinyint unsigned."),
                _ => throw new InvalidOperationException($"Unexpected {AccountsTableName}.keepalive type '{rawKeepAlive.GetType().FullName}'. Expected numeric non-null value.")
            };
        }

        /// <summary>
        /// Parses the persisted SSL flag from database wire values into runtime boolean form.
        /// </summary>
        /// <param name="rawUseSsl">Raw <c>usessl</c> column value.</param>
        /// <returns><see langword="true"/> for <c>"y"</c>; <see langword="false"/> for <c>"n"</c>.</returns>
        internal static bool ParseUseSsl(string rawUseSsl)
        {
            return rawUseSsl switch
            {
                "y" => true,
                "n" => false,
                _ => throw new InvalidOperationException($"Unexpected {AccountsTableName}.usessl value '{rawUseSsl}'. Expected 'y' or 'n'.")
            };
        }

        /// <summary>
        /// Defines startup provisioning operations that ensure required database artifacts exist before initial snapshot loading.
        /// </summary>
        internal interface IStartupProvisioningStore
        {
            /// <summary>
            /// Ensures that the configured database and accounts table exist and are accessible for runtime snapshot loading.
            /// </summary>
            /// <param name="databaseName">Name of the target database to ensure.</param>
            /// <param name="tableName">Name of the accounts table to ensure.</param>
            /// <param name="createTableSql">CREATE TABLE statement used when the table must be created.</param>
            /// <param name="cancellationToken">Cancellation token for shutdown-aware provisioning.</param>
            /// <returns>A task representing the asynchronous provisioning operation.</returns>
            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken);
        }

        /// <summary>
        /// No-op provisioning implementation used when startup artifact provisioning is intentionally bypassed.
        /// </summary>
        internal sealed class NoOpStartupProvisioningStore : IStartupProvisioningStore
        {
            /// <summary>
            /// Stores instance used by my sql nntp account snapshot provider.
            /// </summary>
            internal static readonly NoOpStartupProvisioningStore Instance = new();

            /// <summary>
            /// Initializes the singleton no-op provisioning instance.
            /// </summary>
            private NoOpStartupProvisioningStore()
            {
            }

            /// <summary>
            /// Completes immediately without provisioning any database artifacts.
            /// </summary>
            /// <param name="databaseName">Ignored database name.</param>
            /// <param name="tableName">Ignored table name.</param>
            /// <param name="createTableSql">Ignored CREATE TABLE statement.</param>
            /// <param name="cancellationToken">Ignored cancellation token.</param>
            /// <returns>A completed task.</returns>
            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// MySQL-backed startup provisioning implementation that ensures database and table prerequisites exist.
        /// </summary>
        private sealed class MySqlStartupProvisioningStore : IStartupProvisioningStore
        {
            /// <summary>
            /// Stores connection string used by my sql nntp account snapshot provider.
            /// </summary>
            private readonly string _connectionString;
            /// <summary>
            /// Supplies the logger used by my sql nntp account snapshot provider.
            /// </summary>
            private readonly ILogger<MySqlNntpAccountSnapshotProvider> _logger;

            /// <summary>
            /// Initializes a MySQL startup provisioning store.
            /// </summary>
            /// <param name="connectionString">Connection string used for server-level and database-level provisioning operations.</param>
            /// <param name="logger">Logger used for provisioning failure diagnostics.</param>
            internal MySqlStartupProvisioningStore(string connectionString, ILogger<MySqlNntpAccountSnapshotProvider> logger)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
                ArgumentNullException.ThrowIfNull(logger);

                _connectionString = connectionString;
                _logger = logger;
            }

            /// <summary>
            /// Ensures the runtime database exists and then ensures the runtime accounts table exists within that database.
            /// </summary>
            /// <param name="databaseName">Name of the target runtime database.</param>
            /// <param name="tableName">Name of the target runtime accounts table.</param>
            /// <param name="createTableSql">CREATE TABLE statement used to provision the accounts table.</param>
            /// <param name="cancellationToken">Cancellation token for startup shutdown coordination.</param>
            /// <returns>A task representing the asynchronous provisioning sequence.</returns>
            public async Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
                ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
                ArgumentException.ThrowIfNullOrWhiteSpace(createTableSql);

                MySqlConnectionStringBuilder baseBuilder = new(_connectionString);
                string serverTarget = string.IsNullOrWhiteSpace(baseBuilder.Server)
                    ? "unknown"
                    : baseBuilder.Server;

                await EnsureDatabaseExistsAsync(baseBuilder, serverTarget, databaseName, cancellationToken).ConfigureAwait(false);
                await EnsureTableExistsAsync(baseBuilder, serverTarget, databaseName, tableName, createTableSql, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Ensures that the target database exists on the configured server endpoint.
            /// </summary>
            private async Task EnsureDatabaseExistsAsync(
                MySqlConnectionStringBuilder baseBuilder,
                string serverTarget,
                string databaseName,
                CancellationToken cancellationToken)
            {
                MySqlConnectionStringBuilder serverBuilder = new(baseBuilder.ConnectionString)
                {
                    Database = string.Empty,
                };

                MySqlConnection connection = new(serverBuilder.ConnectionString);
                await using (connection.ConfigureAwait(false))
                {
                    try
                    {
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        LogProvisioningConnectServerFailed(_logger, serverTarget, ex);
                        throw;
                    }

                    MySqlCommand command = connection.CreateCommand();
                    await using (command.ConfigureAwait(false))
                    {
                        command.CommandText = "CREATE DATABASE IF NOT EXISTS `" + databaseName.Replace("`", "``", StringComparison.Ordinal) + "`;";

                        try
                        {
                            _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            LogProvisioningCreateDatabaseFailed(_logger, serverTarget, databaseName, ex);
                            throw;
                        }
                    }
                }
            }

            /// <summary>
            /// Ensures that the target accounts table exists in the selected runtime database.
            /// </summary>
            private async Task EnsureTableExistsAsync(
                MySqlConnectionStringBuilder baseBuilder,
                string serverTarget,
                string databaseName,
                string tableName,
                string createTableSql,
                CancellationToken cancellationToken)
            {
                MySqlConnectionStringBuilder databaseBuilder = new(baseBuilder.ConnectionString)
                {
                    Database = databaseName,
                };

                MySqlConnection connection = new(databaseBuilder.ConnectionString);
                await using (connection.ConfigureAwait(false))
                {
                    try
                    {
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        LogProvisioningSelectDatabaseFailed(_logger, serverTarget, databaseName, ex);
                        throw;
                    }

                    MySqlCommand command = connection.CreateCommand();
                    await using (command.ConfigureAwait(false))
                    {
                        command.CommandText = createTableSql;

                        try
                        {
                            _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            LogProvisioningCreateTableFailed(_logger, serverTarget, databaseName, tableName, ex);
                            throw;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Emits the startup account-load begin marker for one server identifier.
        /// </summary>
        /// <param name="logger">Logger receiving the startup load marker.</param>
        /// <param name="serverId">Server identifier associated with the load attempt.</param>
        [LoggerMessage(EventId = 2000, Level = LogLevel.Information, Message = "Initial NNTP account load starting for ServerId={ServerId}")]
        private static partial void LogInitialAccountLoadStarting(ILogger logger, byte serverId);

        /// <summary>
        /// Emits the startup account-load completion marker with loaded account count.
        /// </summary>
        /// <param name="logger">Logger receiving the completion marker.</param>
        /// <param name="serverId">Server identifier associated with the snapshot load.</param>
        /// <param name="accountCount">Number of account rows loaded into the published snapshot.</param>
        [LoggerMessage(EventId = 2001, Level = LogLevel.Information, Message = "Initial NNTP account load succeeded for ServerId={ServerId}; AccountsLoaded={AccountCount}")]
        private static partial void LogInitialAccountLoadSucceeded(ILogger logger, byte serverId, int accountCount);

        /// <summary>
        /// Emits a provisioning failure when a server-level MySQL connection cannot be established.
        /// </summary>
        /// <param name="logger">Logger receiving the provisioning failure event.</param>
        /// <param name="serverTarget">Server endpoint target used for the connection attempt.</param>
        /// <param name="exception">Exception captured from the failed connection attempt.</param>
        [LoggerMessage(EventId = 2004, Level = LogLevel.Error, Message = "MySQL startup provisioning failed while connecting to server target={ServerTarget}. Startup cannot continue.")]
        private static partial void LogProvisioningConnectServerFailed(ILogger logger, string serverTarget, Exception exception);

        /// <summary>
        /// Emits a provisioning failure when database creation cannot be completed.
        /// </summary>
        /// <param name="logger">Logger receiving the provisioning failure event.</param>
        /// <param name="serverTarget">Server endpoint target used for database provisioning.</param>
        /// <param name="databaseName">Database name requested for creation.</param>
        /// <param name="exception">Exception captured from the failed CREATE DATABASE operation.</param>
        [LoggerMessage(EventId = 2005, Level = LogLevel.Error, Message = "MySQL startup provisioning failed during CREATE DATABASE for server target={ServerTarget}, database={DatabaseName}. Startup cannot continue.")]
        private static partial void LogProvisioningCreateDatabaseFailed(ILogger logger, string serverTarget, string databaseName, Exception exception);

        /// <summary>
        /// Emits a provisioning failure when selecting the target database fails.
        /// </summary>
        /// <param name="logger">Logger receiving the provisioning failure event.</param>
        /// <param name="serverTarget">Server endpoint target used for database selection.</param>
        /// <param name="databaseName">Database name that could not be selected.</param>
        /// <param name="exception">Exception captured from the failed database selection attempt.</param>
        [LoggerMessage(EventId = 2006, Level = LogLevel.Error, Message = "MySQL startup provisioning failed while selecting database for server target={ServerTarget}, database={DatabaseName}. Startup cannot continue.")]
        private static partial void LogProvisioningSelectDatabaseFailed(ILogger logger, string serverTarget, string databaseName, Exception exception);

        /// <summary>
        /// Emits a provisioning failure when table creation cannot be completed.
        /// </summary>
        /// <param name="logger">Logger receiving the provisioning failure event.</param>
        /// <param name="serverTarget">Server endpoint target used for table provisioning.</param>
        /// <param name="databaseName">Database containing the target table.</param>
        /// <param name="tableName">Table name requested for creation.</param>
        /// <param name="exception">Exception captured from the failed CREATE TABLE operation.</param>
        [LoggerMessage(EventId = 2007, Level = LogLevel.Error, Message = "MySQL startup provisioning failed during CREATE TABLE for server target={ServerTarget}, database={DatabaseName}, table={TableName}. Startup cannot continue.")]
        private static partial void LogProvisioningCreateTableFailed(ILogger logger, string serverTarget, string databaseName, string tableName, Exception exception);

        /// <summary>
        /// Emits the periodic refresh begin marker for one server identifier.
        /// </summary>
        /// <param name="logger">Logger receiving the periodic refresh marker.</param>
        /// <param name="serverId">Server identifier associated with the refresh operation.</param>
        [LoggerMessage(EventId = 2100, Level = LogLevel.Debug, Message = "Periodic NNTP account refresh starting for ServerId={ServerId}")]
        private static partial void LogPeriodicRefreshStarting(ILogger logger, byte serverId);

        /// <summary>
        /// Emits the periodic refresh completion marker with loaded-account and duration diagnostics.
        /// </summary>
        /// <param name="logger">Logger receiving the refresh completion marker.</param>
        /// <param name="serverId">Server identifier associated with the completed refresh.</param>
        /// <param name="accountCount">Number of account rows loaded into the published snapshot.</param>
        /// <param name="durationMs">Elapsed refresh duration in milliseconds.</param>
        [LoggerMessage(EventId = 2101, Level = LogLevel.Information, Message = "Periodic NNTP account refresh succeeded for ServerId={ServerId}; AccountsLoaded={AccountCount}; DurationMs={DurationMs}")]
        private static partial void LogPeriodicRefreshSucceeded(ILogger logger, byte serverId, int accountCount, long durationMs);

        /// <summary>
        /// Emits a periodic-refresh skip marker when a concurrent refresh is already active.
        /// </summary>
        /// <param name="logger">Logger receiving the skip marker.</param>
        /// <param name="serverId">Server identifier associated with the skipped refresh attempt.</param>
        [LoggerMessage(EventId = 2102, Level = LogLevel.Debug, Message = "Periodic NNTP account refresh skipped because a refresh is already in progress for ServerId={ServerId}")]
        private static partial void LogPeriodicRefreshSkippedInProgress(ILogger logger, byte serverId);
    }
}
