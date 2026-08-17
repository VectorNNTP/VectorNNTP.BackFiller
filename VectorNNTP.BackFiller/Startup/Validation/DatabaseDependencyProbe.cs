using MySqlConnector;
using Serilog;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    internal static class DatabaseDependencyProbe
    {
        /// <summary>
        /// Validates MySQL database connectivity by attempting to establish a test connection.
        /// </summary>
        /// <remarks>
        /// <para><b>Runtime MySQL Validation:</b> This method performs actual connectivity testing using
        /// MySqlConnector to detect issues that static validation cannot catch:</para>
        /// <list type="bullet">
        /// <item><description>Invalid credentials (wrong username/password or expired token)</description></item>
        /// <item><description>MySQL server unreachable (network, firewall, or server down)</description></item>
        /// <item><description>Unsupported authentication plugin (e.g., caching_sha2_password issues)</description></item>
        /// <item><description>Database doesn't exist or is inaccessible</description></item>
        /// <item><description>Permission denied (user lacks necessary privileges)</description></item>
        /// <item><description>TLS/SSL configuration failures</description></item>
        /// <item><description>Incompatible MySQL server version or protocol mismatch</description></item>
        /// <item><description>Wrong port or connection refused</description></item>
        /// </list>
        /// 
        /// <para><b>Implementation:</b> Creates MySqlConnection, calls OpenAsync(), and executes a test query (SELECT 1)
        /// to verify both connectivity and basic database permissions.</para>
        /// 
        /// <para><b>Control-Plane Only:</b> This validation runs ONCE during startup, not in the article retrieval path.
        /// Temporary database outages after startup MUST NOT interrupt active article retrieval.</para>
        /// 
        /// <para>Per specification 3.15.1: "The application SHOULD establish a test connection during
        /// startup to verify that the configured database is reachable and that the supplied credentials
        /// are valid."</para>
        /// 
        /// <para>Failure to establish the initial database connection SHALL prevent startup if the database
        /// is considered a mandatory dependency for the initial operating state.</para>
        /// </remarks>
        internal static async Task<DependencyValidationResult> ValidateDatabaseConnectivityAsync(
            IConfiguration configuration,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            List<(string Dependency, string Reason)> failures = [];
            List<(string Category, string Message)> warnings = [];
            List<(string Category, string Message)> errors = [];

            string? connectionString = configuration.GetConnectionString("GrabberDB");

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                // This should have been caught by configuration validation, but guard anyway
                failures.Add(("GrabberDB", "Connection string is not configured"));
                return new DependencyValidationResult(failures, warnings, errors);
            }

            // Runtime MySQL connectivity validation using MySqlConnector
            // This validates beyond static syntax checking:
            //   - Network reachability (can we reach the MySQL server?)
            //   - Authentication (are credentials valid?)
            //   - Database accessibility (does the database exist and is it accessible?)
            //   - Permissions (can we execute basic queries?)
            //   - TLS/SSL negotiation (if required)
            //   - Protocol compatibility (server version, authentication plugins)
            try
            {
                using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(timeout);

#pragma warning disable CA2007 // Do not directly await a Task - await using declarations do not support ConfigureAwait
                await using MySqlConnection connection = new(connectionString);
#pragma warning restore CA2007

                Log.Debug("Validating GrabberDB connectivity: attempting to open connection (timeout: {Timeout})", timeout);

                // OpenAsync() validates:
                // - Network connectivity to MySQL server (host:port)
                // - TLS/SSL handshake (if SslMode is configured)
                // - MySQL protocol handshake and version compatibility
                // - Authentication (username/password or token via ProvidePasswordCallback)
                await connection.OpenAsync(cts.Token).ConfigureAwait(false);

                // Execute test query to verify database accessibility and permissions
                // This catches issues like:
                // - Database doesn't exist (MySQL error 1049)
                // - User lacks SELECT permission (MySQL error 1142)
                // - Database is in read-only mode or otherwise inaccessible
#pragma warning disable CA2007 // Do not directly await a Task - await using declarations do not support ConfigureAwait
                await using MySqlCommand cmd = connection.CreateCommand();
#pragma warning restore CA2007
                cmd.CommandText = "SELECT 1";
                double timeoutSeconds = timeout.TotalSeconds;
                int commandTimeoutSeconds = timeoutSeconds <= 0
                    ? 1
                    : timeoutSeconds >= int.MaxValue
                        ? int.MaxValue
                        : (int)Math.Ceiling(timeoutSeconds);
                cmd.CommandTimeout = commandTimeoutSeconds;
                _ = await cmd.ExecuteScalarAsync(cts.Token).ConfigureAwait(false);

                Log.Information("GrabberDB connectivity validated successfully (Server: {Server}, Database: {Database})",
                    connection.DataSource,
                    connection.Database);

                // Connection and query successful:
                // ✅ MySQL server is reachable
                // ✅ Credentials are valid
                // ✅ Database exists and is accessible
                // ✅ User has basic query permissions
                // ✅ Network, TLS, and authentication are working correctly
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw; // Propagate shutdown cancellation
            }
            catch (OperationCanceledException)
            {
                // Timeout during connection attempt (could be network issues, slow server, firewall, wrong host/port)
                failures.Add(("GrabberDB", $"Connection timeout after {timeout.TotalSeconds:F1}s"));
            }
            catch (MySqlException ex)
            {
                // MySQL-specific errors with error numbers for diagnosis.
                // Keep details sanitized to avoid exposing sensitive environment/provider text.
                string sanitizedReason = GetSanitizedMySqlConnectionFailureReason(ex.Number);
                failures.Add(("GrabberDB", $"{sanitizedReason} (Error #{ex.Number})"));
            }
            catch (Exception ex)
            {
                // Other errors (DNS resolution failure, socket errors, invalid connection string format, etc.)
                // Keep startup diagnostics sanitized and avoid surfacing provider exception text.
                Log.Debug(ex, "GrabberDB connectivity validation threw an unexpected exception during startup dependency validation.");
                failures.Add(("GrabberDB", "Failed to connect"));
            }

            return new DependencyValidationResult(failures, warnings, errors);
        }

        /// <summary>
        /// Maps MySQL provider error codes to sanitized, user-safe startup diagnostics.
        /// </summary>
        /// <param name="mySqlErrorNumber">MySQL provider error number.</param>
        /// <returns>Sanitized failure reason without provider-supplied environment details.</returns>
        internal static string GetSanitizedMySqlConnectionFailureReason(int mySqlErrorNumber)
        {
            return mySqlErrorNumber switch
            {
                1045 => "MySQL connection failed: Access denied",
                1049 => "MySQL connection failed: Unknown database",
                1130 => "MySQL connection failed: Host is not allowed to connect",
                2002 or 2003 => "MySQL connection failed: Unable to reach MySQL server",
                2013 => "MySQL connection failed: Lost connection during query",
                2026 => "MySQL connection failed: TLS/SSL handshake failed",
                2061 => "MySQL connection failed: Authentication plugin error",
                _ => "MySQL connection failed",
            };
        }
    }
}
