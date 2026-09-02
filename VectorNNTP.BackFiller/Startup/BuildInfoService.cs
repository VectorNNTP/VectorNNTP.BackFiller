// <copyright file="BuildInfoService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup
// Implements the build info service behavior.

using Serilog;

namespace VectorNNTP.Backfiller.Startup
{
    /// <summary>
    /// Coordinates creation, caching, and startup-time presentation of the process <see cref="BuildInfo"/> snapshot.
    /// </summary>
    /// <remarks>
    /// This bootstrap helper captures one <see cref="BuildInfo"/> instance from the current executable and startup timestamp,
    /// then reuses that cached snapshot for startup logs and command output.
    /// The type itself is static and is called directly from startup/command flows rather than being resolved through DI.
    /// </remarks>
    internal static class BuildInfoService
    {
        /// <summary>
        /// Cached build-info snapshot established during startup initialization.
        /// </summary>
        /// <remarks>
        /// This field is populated by <see cref="InitializeBuildInfo(DateTimeOffset)"/> and reused by subsequent
        /// logging/command helpers to avoid recomputing assembly metadata during the same process lifetime.
        /// </remarks>
        private static BuildInfo? _buildInfo;

        /// <summary>
        /// Creates and caches the process build-info snapshot used by startup logs and version-reporting commands.
        /// </summary>
        /// <param name="processStartedAt">Process start timestamp passed through to <see cref="BuildInfo.Create(DateTimeOffset)"/>.</param>
        /// <exception cref="ArgumentException">Propagated when <paramref name="processStartedAt"/> is the default timestamp.</exception>
        /// <remarks>
        /// The most recent invocation replaces the cached snapshot for the current process.
        /// </remarks>
        internal static void InitializeBuildInfo(DateTimeOffset processStartedAt)
        {
            _buildInfo = BuildInfo.Create(processStartedAt);
        }

        /// <summary>
        /// Emits startup log entries for compact version, commit/build provenance, and runtime framework identity.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when build info has not been initialized for the process.</exception>
        internal static void LogBuildInfo()
        {
            BuildInfo currentBuildInfo = GetBuildInfo();
            Log.Information("Application version: {Version}", currentBuildInfo.GetCompactVersion());
            Log.Information("Commit: {Commit} (Build: {BuildTimestamp}, {BuildConfiguration})",
                currentBuildInfo.Commit, currentBuildInfo.BuildTimestamp, currentBuildInfo.BuildConfiguration);
            Log.Information("Runtime: {DotNetVersion}", currentBuildInfo.DotNetVersion);
        }

        /// <summary>
        /// Logs a non-secret configuration fingerprint to support deployment/configuration drift comparison.
        /// </summary>
        /// <param name="configuration">Fully loaded merged configuration used to compute a redacted fingerprint.</param>
        /// <remarks>
        /// Fingerprint calculation failures are downgraded to a warning and do not block startup.
        /// </remarks>
        internal static void LogConfigurationFingerprint(IConfiguration configuration)
        {
            try
            {
                string fingerprint = Configuration.ConfigurationFingerprintService.CalculateConfigurationFingerprint(configuration);
                Log.Information("Non-secret ConfigurationId: {ConfigurationId} (passwords/keys/tokens excluded)",
                    fingerprint);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Failed to calculate configuration fingerprint; startup continues");
            }
        }

        /// <summary>
        /// Returns the multi-line diagnostic version summary used by version/diagnostics command output.
        /// </summary>
        /// <returns>The formatted version summary generated from the cached <see cref="BuildInfo"/> snapshot.</returns>
        /// <exception cref="InvalidOperationException">Thrown when build info has not been initialized for the process.</exception>
        internal static string GetFullVersionString()
        {
            return GetBuildInfo().GetVersionString();
        }

        /// <summary>
        /// Returns the cached build-info snapshot for the current process.
        /// </summary>
        /// <returns>The initialized <see cref="BuildInfo"/> instance.</returns>
        /// <exception cref="InvalidOperationException">Thrown when startup has not initialized build info yet.</exception>
        private static BuildInfo GetBuildInfo()
        {
            return _buildInfo is null
                ? throw new InvalidOperationException("BuildInfo must be initialized during startup before use.")
                : _buildInfo;
        }
    }
}
