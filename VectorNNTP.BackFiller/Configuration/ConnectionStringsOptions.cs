// ConnectionStringsOptions.cs -- Strongly-typed configuration for database connection strings.
//
// Provides validation for ConnectionStrings:GrabberDB according to specification 3.15.1.
//
// Validation rules:
//   - Must be present and non-empty
//   - Must contain valid server/host
//   - Must contain valid database name
//   - Must have valid connection string syntax
//   - Must contain User ID (password optional; can be supplied via ProvidePasswordCallback)
//   - Connection pooling settings are validated for control-plane usage patterns (warnings)
//
// The GrabberDB connection is a control-plane database used for configuration and operational state.
// It is NOT part of the high-throughput article retrieval data path and should use small connection pools.
//
// Provider: This application uses MySQL exclusively via MySqlConnector.
//           Authentication requires User ID; password may be in connection string or provided programmatically.

using System.ComponentModel.DataAnnotations;
using System.Data.Common;

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Severity level for connection string validation diagnostics.
    /// </summary>
    internal enum ValidationSeverity
    {
        /// <summary>
        /// Error: connection string is invalid or missing required components.
        /// The application cannot proceed with this configuration.
        /// </summary>
        Error,

        /// <summary>
        /// Warning: connection string is valid but uses suboptimal settings.
        /// The application can proceed, but configuration should be reviewed.
        /// </summary>
        Warning
    }

    /// <summary>
    /// Result of connection string validation.
    /// </summary>
    internal record ConnectionStringValidationResult(
        string Setting,
        string Message,
        ValidationSeverity Severity);

    /// <summary>
    /// Configuration options for database connection strings.
    /// </summary>
    /// <remarks>
    /// <para>ConnectionStrings:GrabberDB is the control-plane database connection used for:</para>
    /// <list type="bullet">
    /// <item><description>Application configuration retrieval</description></item>
    /// <item><description>Operational state management</description></item>
    /// <item><description>Provider connection lifecycle decisions</description></item>
    /// </list>
    /// <para><b>CRITICAL:</b> This database SHALL NOT be part of the article retrieval data path.
    /// Database queries MUST NOT be required for each article request.</para>
    /// </remarks>
    internal sealed class ConnectionStringsOptions
    {
        /// <summary>
        /// Gets or sets the connection string for the VectorNNTP.Grabber control-plane database.
        /// </summary>
        /// <remarks>
        /// <para>This connection is expected to have low utilization compared to Usenet provider connections.
        /// Operations should be infrequent, asynchronous, and isolated from the article retrieval pipeline.</para>
        /// 
        /// <para>Connection pooling: Use a single connection or very small pool. Large pools are NOT appropriate
        /// for this control-plane database access pattern.</para>
        /// 
        /// <para>Resilience: Temporary database outages MUST NOT interrupt active article retrieval.
        /// The application should continue with cached provider configuration while attempting reconnection.</para>
        /// </remarks>
        [Required(ErrorMessage = "ConnectionStrings:GrabberDB is required")]
        public string? GrabberDB { get; set; }
    }

    /// <summary>
    /// Custom validator for database connection strings.
    /// </summary>
    /// <remarks>
    /// <para>Validates connection string syntax and required components (server, database, credentials).</para>
    /// <para><b>Provider:</b> This application uses MySQL exclusively. Provider validation happens at runtime
    /// via actual connectivity testing with MySqlConnector, not via unreliable connection string heuristics.</para>
    /// <para>Returns errors for missing/invalid required components and warnings for suboptimal configuration.</para>
    /// </remarks>
    internal static class ConnectionStringValidator
    {
        /// <summary>
        /// Validates a database connection string for correctness and completeness.
        /// </summary>
        /// <param name="connectionString">The connection string to validate.</param>
        /// <param name="settingName">The configuration setting name (for diagnostic messages).</param>
        /// <returns>List of validation diagnostics (errors and warnings). Empty if valid with no warnings.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingName"/> is null, empty, or whitespace.</exception>
        public static List<ConnectionStringValidationResult> Validate(string? connectionString, string settingName)
        {
            // Validate settingName parameter (used in all diagnostic messages)
            ArgumentException.ThrowIfNullOrWhiteSpace(settingName);

            List<ConnectionStringValidationResult> diagnostics = [];

            // Rule 1: Must be present and non-empty
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue validation without a connection string
            }

            // Rule 2: Must have valid connection string syntax
            DbConnectionStringBuilder builder;
            try
            {
                builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            }
            catch (ArgumentException ex)
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    $"Invalid connection string syntax: {ex.Message}",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue if syntax is invalid
            }

            // Rule 2a: Must not have ambiguous/conflicting aliases
            // Example: Server=db01;Host=db02 is AMBIGUOUS (which server to use?)
            // This check MUST happen before individual property validation to catch conflicts early.
            //
            // Detection strategy:
            //   - If TryGetServer/Database/Username/etc. returns false, it means either:
            //     a) The property is missing entirely, OR
            //     b) Multiple aliases exist with conflicting values
            //   - To distinguish: check if builder.ContainsKey for any of the known aliases
            //   - If ContainsKey succeeds but TryGet fails => CONFLICT
            //
            // Note: Each TryGet* call parses the connection string independently. This is startup
            //       validation code, so redundant parsing is not a performance concern. If this
            //       ever becomes relevant, consider using TryParseEffective() once and extracting
            //       properties from the builder, or introduce an internal parsed representation.

            bool serverAliasExists = builder.ContainsKey("Server") || builder.ContainsKey("Host") ||
                                     builder.ContainsKey("Data Source") || builder.ContainsKey("DataSource") ||
                                     builder.ContainsKey("Address") || builder.ContainsKey("Addr") ||
                                     builder.ContainsKey("Network Address");

            if (serverAliasExists && !MySqlConnectionStringUtilities.TryGetServer(connectionString, out _))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string contains conflicting server/host aliases with different values (e.g., Server=db01;Host=db02). Remove duplicate aliases or ensure they specify the same value.",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue with ambiguous configuration
            }

            bool databaseAliasExists = builder.ContainsKey("Database") || builder.ContainsKey("Initial Catalog") ||
                                        builder.ContainsKey("InitialCatalog");

            if (databaseAliasExists && !MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out _))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string contains conflicting database aliases with different values (e.g., Database=dbA;Initial Catalog=dbB). Remove duplicate aliases or ensure they specify the same value.",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue with ambiguous configuration
            }

            bool usernameAliasExists = builder.ContainsKey("User ID") || builder.ContainsKey("UserID") ||
                                        builder.ContainsKey("Username") || builder.ContainsKey("Uid") ||
                                        builder.ContainsKey("User name") || builder.ContainsKey("User");

            if (usernameAliasExists && !MySqlConnectionStringUtilities.TryGetUsername(connectionString, out _))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string contains conflicting username aliases with different values (e.g., User ID=alice;Username=bob). Remove duplicate aliases or ensure they specify the same value.",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue with ambiguous configuration
            }

            bool passwordAliasExists = builder.ContainsKey("Password") || builder.ContainsKey("Pwd");

            if (passwordAliasExists && !MySqlConnectionStringUtilities.TryGetPassword(connectionString, out _))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string contains conflicting password aliases with different values. Remove duplicate aliases or ensure they specify the same value.",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue with ambiguous configuration
            }

            // Check for pool size alias conflicts and invalid pool size values
            bool minPoolAliasExists = builder.ContainsKey("Min Pool Size") || builder.ContainsKey("MinPoolSize") ||
                                       builder.ContainsKey("Minimum Pool Size") || builder.ContainsKey("MinimumPoolSize");

            if (minPoolAliasExists && !MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out _))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString)
                        ? "Connection string contains conflicting minimum pool size aliases with different values (e.g., Min Pool Size=5;MinimumPoolSize=10). Remove duplicate aliases or ensure they specify the same value."
                        : "Connection string contains an invalid minimum pool size value. Min Pool Size must be a valid non-negative integer.",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue with invalid or ambiguous configuration
            }

            bool maxPoolAliasExists = builder.ContainsKey("Max Pool Size") || builder.ContainsKey("MaxPoolSize") ||
                                       builder.ContainsKey("Maximum Pool Size") || builder.ContainsKey("MaximumPoolSize");

            if (maxPoolAliasExists && !MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out _))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString)
                        ? "Connection string contains conflicting maximum pool size aliases with different values (e.g., Max Pool Size=50;MaximumPoolSize=100). Remove duplicate aliases or ensure they specify the same value."
                        : "Connection string contains an invalid maximum pool size value. Max Pool Size must be a valid non-negative integer.",
                    ValidationSeverity.Error));
                return diagnostics; // Cannot continue with invalid or ambiguous configuration
            }

            // Rule 3: Must contain server/host (using canonical MySQL interpretation)
            if (!MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string must specify a server/host (Server, Host, Data Source, Address, Addr, or Network Address)",
                    ValidationSeverity.Error));
            }
            else if (string.IsNullOrWhiteSpace(server))
            {
                // Defensive check: shouldn't happen since DbConnectionStringBuilder removes empty values
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Server/host value cannot be empty",
                    ValidationSeverity.Error));
            }

            // Rule 4: Must contain database name (using canonical MySQL interpretation)
            if (!MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string must specify a database name (Database or Initial Catalog)",
                    ValidationSeverity.Error));
            }
            else if (string.IsNullOrWhiteSpace(database))
            {
                // Defensive check: shouldn't happen since DbConnectionStringBuilder removes empty values
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Database name cannot be empty",
                    ValidationSeverity.Error));
            }

            // Rule 5: Must contain authentication configuration for MySQL
            // MySQL (via MySqlConnector) requires a username (User ID).
            // Password may be supplied in the connection string OR programmatically via ProvidePasswordCallback.
            if (!MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username)
                || string.IsNullOrWhiteSpace(username))
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    "Connection string must specify a MySQL user ID",
                    ValidationSeverity.Error));
            }

            // Note: Password is optional in the connection string.
            // MySqlConnector supports ProvidePasswordCallback for programmatic password/token delivery.
            // We do not validate password presence here - runtime connectivity testing will catch auth failures.

            // Rule 6: Validate connection pooling settings (control-plane usage pattern - warnings only)
            ValidateConnectionPooling(connectionString, settingName, diagnostics);

            return diagnostics;
        }

        /// <summary>
        /// Validates connection pooling configuration for control-plane database usage.
        /// </summary>
        /// <remarks>
        /// <para>GrabberDB is a control-plane database with low utilization. Large connection pools are inappropriate.</para>
        /// <para>Emits warnings (not errors) if Min Pool Size > 1 or Max Pool Size > 10.</para>
        /// <para>MySqlConnector enables pooling by default. These are recommendations for optimal control-plane usage.</para>
        /// </remarks>
        private static void ValidateConnectionPooling(string connectionString, string settingName, List<ConnectionStringValidationResult> diagnostics)
        {
            // Check Min Pool Size (should be 0 or 1 for control-plane)
            if (MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize)
                && minPoolSize > 1)
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    $"Min Pool Size={minPoolSize} is excessive for control-plane database. Recommended: 0 or 1 " +
                    "(GrabberDB has low utilization and should use a single connection or very small pool)",
                    ValidationSeverity.Warning));
            }

            // Check Max Pool Size (should be <= 10 for control-plane)
            if (MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize)
                && maxPoolSize > 10)
            {
                diagnostics.Add(new ConnectionStringValidationResult(
                    settingName,
                    $"Max Pool Size={maxPoolSize} is excessive for control-plane database. Recommended: <=10 " +
                    "(GrabberDB is NOT part of the article retrieval data path and does not require large pools)",
                    ValidationSeverity.Warning));
            }
        }
    }
}
