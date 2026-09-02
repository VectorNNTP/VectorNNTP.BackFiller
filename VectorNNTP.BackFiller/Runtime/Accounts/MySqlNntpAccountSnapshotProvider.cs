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
    /// Loads and publishes immutable NNTP account snapshots from the GrabberDB MySQL control-plane database.
    /// </summary>
    internal sealed partial class MySqlNntpAccountSnapshotProvider
    {
        /// <summary>
        /// Limits accounts table name for my sql nntp account snapshot provider.
        /// </summary>
        internal const string AccountsTableName = "nntpbackfilleraccounts";

        /// <summary>
        /// Limits accounts table create sql for my sql nntp account snapshot provider.
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
        /// Handles my sql nntp account snapshot provider for my sql nntp account snapshot provider.
        /// </summary>
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
        /// Handles my sql nntp account snapshot provider for my sql nntp account snapshot provider.
        /// </summary>
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
        /// Returns the currently published immutable NNTP account snapshot.
        /// </summary>
        internal NntpAccountSnapshotState CurrentSnapshot => _currentSnapshot;

        /// <summary>
        /// Ensures startup-time MySQL database and table dependencies exist and are accessible.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        internal async Task EnsureStartupDependenciesAsync(CancellationToken cancellationToken)
        {
            await _startupProvisioningStore.EnsureDatabaseAndTableAsync(
                _databaseName,
                AccountsTableName,
                AccountsTableCreateSql,
                cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Loads and publishes the initial runtime account snapshot.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for startup cancellation.</param>
        internal async Task LoadInitialSnapshotAsync(CancellationToken cancellationToken)
        {
            LogInitialAccountLoadStarting(_logger, _serverId);

            int accountCount = await LoadAndPublishSnapshotAsync(cancellationToken).ConfigureAwait(false);

            LogInitialAccountLoadSucceeded(_logger, _serverId, accountCount);
        }

        /// <summary>
        /// Refreshes and publishes the runtime account snapshot.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for shutdown-aware refresh.</param>
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
        /// Handles load and publish snapshot async for my sql nntp account snapshot provider.
        /// </summary>
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
        /// Handles query accounts async for my sql nntp account snapshot provider.
        /// </summary>
        private async Task<List<NntpAccountSnapshot>> QueryAccountsAsync(CancellationToken cancellationToken)
        {
#pragma warning disable CA2007
            await using MySqlConnection connection = new(_connectionString);
#pragma warning restore CA2007
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

#pragma warning disable CA2007
            await using MySqlCommand command = connection.CreateCommand();
#pragma warning restore CA2007
            command.CommandText = AccountsQuery;
            _ = command.Parameters.Add(new MySqlParameter("@ServerId", MySqlDbType.UByte) { Value = _serverId });

#pragma warning disable CA2007
            await using MySqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
#pragma warning restore CA2007

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

        /// <summary>
        /// Handles parse entry id value for my sql nntp account snapshot provider.
        /// </summary>
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
        /// Handles parse entry id for my sql nntp account snapshot provider.
        /// </summary>
        internal static Guid ParseEntryId(string rawEntryId)
        {
            return Guid.TryParse(rawEntryId, out Guid parsed)
                ? parsed
                : throw new InvalidOperationException($"Invalid {AccountsTableName}.entryid value '{rawEntryId}'. Expected GUID format.");
        }

        /// <summary>
        /// Handles parse keep alive value for my sql nntp account snapshot provider.
        /// </summary>
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
        /// Handles parse use ssl for my sql nntp account snapshot provider.
        /// </summary>
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
        /// Defines istartup provisioning store and its my sql nntp account snapshot provider contract.
        /// </summary>
        internal interface IStartupProvisioningStore
        {
            /// <summary>
            /// Handles ensure database and table async for my sql nntp account snapshot provider.
            /// </summary>
            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken);
        }

        /// <summary>
        /// Defines no op startup provisioning store and its my sql nntp account snapshot provider contract.
        /// </summary>
        internal sealed class NoOpStartupProvisioningStore : IStartupProvisioningStore
        {
            /// <summary>
            /// Stores instance used by my sql nntp account snapshot provider.
            /// </summary>
            internal static readonly NoOpStartupProvisioningStore Instance = new();

            /// <summary>
            /// Handles no op startup provisioning store for my sql nntp account snapshot provider.
            /// </summary>
            private NoOpStartupProvisioningStore()
            {
            }

            /// <summary>
            /// Handles ensure database and table async for my sql nntp account snapshot provider.
            /// </summary>
            public Task EnsureDatabaseAndTableAsync(string databaseName, string tableName, string createTableSql, CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Defines my sql startup provisioning store and its my sql nntp account snapshot provider contract.
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
            /// Handles my sql startup provisioning store for my sql nntp account snapshot provider.
            /// </summary>
            internal MySqlStartupProvisioningStore(string connectionString, ILogger<MySqlNntpAccountSnapshotProvider> logger)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
                ArgumentNullException.ThrowIfNull(logger);

                _connectionString = connectionString;
                _logger = logger;
            }

            /// <summary>
            /// Handles ensure database and table async for my sql nntp account snapshot provider.
            /// </summary>
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
            /// Handles ensure database exists async for my sql nntp account snapshot provider.
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

#pragma warning disable CA2007
                await using MySqlConnection connection = new(serverBuilder.ConnectionString);
#pragma warning restore CA2007

                try
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogProvisioningConnectServerFailed(_logger, serverTarget, ex);
                    throw;
                }

#pragma warning disable CA2007
                await using MySqlCommand command = connection.CreateCommand();
#pragma warning restore CA2007
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

            /// <summary>
            /// Handles ensure table exists async for my sql nntp account snapshot provider.
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

#pragma warning disable CA2007
                await using MySqlConnection connection = new(databaseBuilder.ConnectionString);
#pragma warning restore CA2007

                try
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogProvisioningSelectDatabaseFailed(_logger, serverTarget, databaseName, ex);
                    throw;
                }

#pragma warning disable CA2007
                await using MySqlCommand command = connection.CreateCommand();
#pragma warning restore CA2007
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

        /// <summary>
        /// Emits the initial account load starting log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2000, Level = LogLevel.Information, Message = "Initial NNTP account load starting for ServerId={ServerId}")]
        private static partial void LogInitialAccountLoadStarting(ILogger logger, byte serverId);

        /// <summary>
        /// Emits the initial account load succeeded log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2001, Level = LogLevel.Information, Message = "Initial NNTP account load succeeded for ServerId={ServerId}; AccountsLoaded={AccountCount}")]
        private static partial void LogInitialAccountLoadSucceeded(ILogger logger, byte serverId, int accountCount);

        /// <summary>
        /// Emits the provisioning connect server failed log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2004, Level = LogLevel.Error, Message = "MySQL startup provisioning failed while connecting to server target={ServerTarget}. Startup cannot continue.")]
        private static partial void LogProvisioningConnectServerFailed(ILogger logger, string serverTarget, Exception exception);

        /// <summary>
        /// Emits the provisioning create database failed log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2005, Level = LogLevel.Error, Message = "MySQL startup provisioning failed during CREATE DATABASE for server target={ServerTarget}, database={DatabaseName}. Startup cannot continue.")]
        private static partial void LogProvisioningCreateDatabaseFailed(ILogger logger, string serverTarget, string databaseName, Exception exception);

        /// <summary>
        /// Emits the provisioning select database failed log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2006, Level = LogLevel.Error, Message = "MySQL startup provisioning failed while selecting database for server target={ServerTarget}, database={DatabaseName}. Startup cannot continue.")]
        private static partial void LogProvisioningSelectDatabaseFailed(ILogger logger, string serverTarget, string databaseName, Exception exception);

        /// <summary>
        /// Emits the provisioning create table failed log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2007, Level = LogLevel.Error, Message = "MySQL startup provisioning failed during CREATE TABLE for server target={ServerTarget}, database={DatabaseName}, table={TableName}. Startup cannot continue.")]
        private static partial void LogProvisioningCreateTableFailed(ILogger logger, string serverTarget, string databaseName, string tableName, Exception exception);

        /// <summary>
        /// Emits the periodic refresh starting log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2100, Level = LogLevel.Debug, Message = "Periodic NNTP account refresh starting for ServerId={ServerId}")]
        private static partial void LogPeriodicRefreshStarting(ILogger logger, byte serverId);

        /// <summary>
        /// Emits the periodic refresh succeeded log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2101, Level = LogLevel.Information, Message = "Periodic NNTP account refresh succeeded for ServerId={ServerId}; AccountsLoaded={AccountCount}; DurationMs={DurationMs}")]
        private static partial void LogPeriodicRefreshSucceeded(ILogger logger, byte serverId, int accountCount, long durationMs);

        /// <summary>
        /// Emits the periodic refresh skipped in progress log event for my sql nntp account snapshot provider.
        /// </summary>
        [LoggerMessage(EventId = 2102, Level = LogLevel.Debug, Message = "Periodic NNTP account refresh skipped because a refresh is already in progress for ServerId={ServerId}")]
        private static partial void LogPeriodicRefreshSkippedInProgress(ILogger logger, byte serverId);
    }
}
