// <copyright file="ValidationLogging.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: validation logging in the startup validation subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

using Serilog;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Defines validation logging and its validation logging contract.
    /// </summary>
    internal static class ValidationLogging
    {
        /// <summary>
        /// Logs configuration validation errors with detail and context.
        /// </summary>
        /// <param name="result">Configuration validation result.</param>
        internal static void LogConfigurationValidationErrors(ConfigurationValidationResult result)
        {
            if (result.Errors.Count > 0)
            {
                Log.Error("Configuration validation failed with {ErrorCount} error(s):", result.Errors.Count);

                foreach ((string setting, string error) in result.Errors)
                {
                    Log.Error("  - {Setting}: {Error}", setting, error);
                }
            }

            if (result.Warnings.Count > 0)
            {
                Log.Warning("Configuration validation raised {WarningCount} warning(s):", result.Warnings.Count);

                foreach ((string setting, string message) in result.Warnings)
                {
                    Log.Warning("  - {Setting}: {Message}", setting, message);
                }
            }
        }

        /// <summary>
        /// Logs dependency validation failures with detail and context.
        /// </summary>
        /// <param name="result">Dependency validation result.</param>
        internal static void LogDependencyValidationErrors(DependencyValidationResult result)
        {
            // Log dependency failures
            if (result.FailedDependencies.Count > 0)
            {
                Log.Error("Dependency validation failed with {FailureCount} failure(s):", result.FailedDependencies.Count);

                foreach ((string dependency, string reason) in result.FailedDependencies)
                {
                    Log.Error("  - {Dependency}: {Reason}", dependency, reason);
                }
            }

            if (result.Warnings.Count > 0)
            {
                Log.Warning("Dependency validation raised {WarningCount} warning(s):", result.Warnings.Count);

                foreach ((string category, string message) in result.Warnings)
                {
                    Log.Warning("  - {Category}: {Message}", category, message);
                }
            }

            if (result.Errors.Count > 0)
            {
                Log.Error("Dependency validation raised {ErrorCount} error(s):", result.Errors.Count);

                foreach ((string category, string message) in result.Errors)
                {
                    Log.Error("  - {Category}: {Message}", category, message);
                }
            }
        }
    }
}
