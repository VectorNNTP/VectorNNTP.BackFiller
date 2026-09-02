// <copyright file="DatabaseDependencyProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: database dependency probe in the startup validation subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

using MySqlConnector;
using Serilog;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Performs startup-time GrabberDB connectivity probing and translates provider/network failures into sanitized dependency-validation diagnostics.
    /// </summary>
    /// <remarks>
    /// The probe contributes one dependency-validation slice to the startup dependency pipeline. It emits structured
    /// informational/debug logs for operator diagnostics and returns failures through <see cref="DependencyValidationResult"/>
    /// so callers can aggregate database outcomes with other dependency checks before making startup decisions.
    /// </remarks>
    internal static class DatabaseDependencyProbe
    {
        /// <summary>
        /// Probes GrabberDB by opening a MySQL connection and executing <c>SELECT 1</c> within the configured timeout budget.
        /// </summary>
        /// <param name="configuration">Configuration root used to resolve the <c>ConnectionStrings:GrabberDB</c> value.</param>
        /// <param name="timeout">Per-probe timeout applied to both connection open and test-query execution.</param>
        /// <param name="cancellationToken">Startup cancellation token propagated to database I/O operations.</param>
        /// <returns>
        /// A task that completes with a <see cref="DependencyValidationResult"/> containing sanitized GrabberDB failure
        /// diagnostics. Successful probes return no failures and emit an informational structured log with server/database identity.
        /// </returns>
        /// <exception cref="OperationCanceledException">The outer <paramref name="cancellationToken"/> is canceled.</exception>
        /// <remarks>
        /// <para>Connectivity is validated at runtime (not just syntactically) by opening a real provider connection and executing a test query.</para>
        /// <para>Known provider error numbers are mapped via <see cref="GetSanitizedMySqlConnectionFailureReason(int)"/> to avoid leaking environment details.</para>
        /// <para>Unexpected exceptions are logged at debug level and converted to a generic dependency failure so startup can continue aggregating diagnostics.</para>
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

                MySqlConnection connection = new(connectionString);
                await using (connection.ConfigureAwait(false))
                {
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
                    MySqlCommand cmd = connection.CreateCommand();
                    await using (cmd.ConfigureAwait(false))
                    {
                        cmd.CommandText = "SELECT 1";
                        double timeoutSeconds = timeout.TotalSeconds;
                        int commandTimeoutSeconds = timeoutSeconds <= 0
                            ? 1
                            : timeoutSeconds >= int.MaxValue
                                ? int.MaxValue
                                : (int)Math.Ceiling(timeoutSeconds);
                        cmd.CommandTimeout = commandTimeoutSeconds;
                        _ = await cmd.ExecuteScalarAsync(cts.Token).ConfigureAwait(false);
                    }

                    Log.Information("GrabberDB connectivity validated successfully (Server: {Server}, Database: {Database})",
                        connection.DataSource,
                        connection.Database);
                }
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
        /// Maps MySQL provider error codes to sanitized startup diagnostic text.
        /// </summary>
        /// <param name="mySqlErrorNumber">MySqlConnector provider error number from a failed connection/query operation.</param>
        /// <returns>A user-safe reason string suitable for inclusion in dependency-validation failure messages.</returns>
        /// <remarks>
        /// Unknown error numbers intentionally collapse to a generic message to preserve diagnostic usefulness without
        /// surfacing provider-specific environment detail in startup output.
        /// </remarks>
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
