// <copyright file="ValidationLogging.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: validation logging in the startup validation subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

using Serilog;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Converts startup validation result objects into structured Serilog diagnostics without altering validation state.
    /// </summary>
    /// <remarks>
    /// These helpers are invoked by startup orchestration after validation completes so operators receive detailed
    /// per-item diagnostics while startup failure decisions remain owned by the validation pipeline and caller logic.
    /// </remarks>
    internal static class ValidationLogging
    {
        /// <summary>
        /// Emits configuration-validation diagnostics as structured error and warning log entries.
        /// </summary>
        /// <param name="result">
        /// Configuration validation snapshot whose aggregated <see cref="ConfigurationValidationResult.Errors"/> and
        /// <see cref="ConfigurationValidationResult.Warnings"/> are translated into log output.
        /// </param>
        /// <remarks>
        /// A summary error entry with <c>ErrorCount</c> is written when errors exist, followed by one error entry per
        /// <c>(Setting, Error)</c> pair using structured fields <c>Setting</c> and <c>Error</c>. Warnings are emitted
        /// similarly with <c>WarningCount</c> and per-item fields <c>Setting</c> and <c>Message</c>.
        /// </remarks>
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
        /// Emits dependency-validation diagnostics as structured error and warning log entries.
        /// </summary>
        /// <param name="result">
        /// Dependency validation snapshot whose failures, warnings, and errors are translated into severity-specific
        /// log messages.
        /// </param>
        /// <remarks>
        /// Failed dependencies produce an error summary with <c>FailureCount</c> and per-item entries with
        /// <c>Dependency</c>/<c>Reason</c>. Warning diagnostics produce warning summary/per-item entries with
        /// <c>WarningCount</c> and <c>Category</c>/<c>Message</c>. Error diagnostics produce error summary/per-item
        /// entries with <c>ErrorCount</c> and <c>Category</c>/<c>Message</c>.
        /// </remarks>
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
