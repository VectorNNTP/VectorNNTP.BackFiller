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
    /// Owns immutable build and process metadata creation, exposure, logging, and DI registration.
    /// </summary>
    internal static class BuildInfoService
    {
        /// <summary>
        /// Stores build info used by build info service.
        /// </summary>
        private static BuildInfo? _buildInfo;

        /// <summary>
        /// Initializes build information with an explicit process start timestamp.
        /// </summary>
        /// <param name="processStartedAt">The process start timestamp (UTC).</param>
        internal static void InitializeBuildInfo(DateTimeOffset processStartedAt)
        {
            _buildInfo = BuildInfo.Create(processStartedAt);
        }

        /// <summary>
        /// Logs build information at startup for operational visibility.
        /// </summary>
        internal static void LogBuildInfo()
        {
            BuildInfo currentBuildInfo = GetBuildInfo();
            Log.Information("Application version: {Version}", currentBuildInfo.GetCompactVersion());
            Log.Information("Commit: {Commit} (Build: {BuildTimestamp}, {BuildConfiguration})",
                currentBuildInfo.Commit, currentBuildInfo.BuildTimestamp, currentBuildInfo.BuildConfiguration);
            Log.Information("Runtime: {DotNetVersion}", currentBuildInfo.DotNetVersion);
        }

        /// <summary>
        /// Logs effective non-secret configuration fingerprint for deployment comparison.
        /// </summary>
        /// <param name="configuration">Fully loaded and merged configuration.</param>
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
        /// Gets the full version string suitable for console display.
        /// </summary>
        /// <returns>The full version string.</returns>
        internal static string GetFullVersionString()
        {
            return GetBuildInfo().GetVersionString();
        }

        /// <summary>
        /// Gets the initialized build information snapshot.
        /// </summary>
        /// <returns>The current build information snapshot.</returns>
        private static BuildInfo GetBuildInfo()
        {
            return _buildInfo is null
                ? throw new InvalidOperationException("BuildInfo must be initialized during startup before use.")
                : _buildInfo;
        }
    }
}
