// <copyright file="ExitCodePolicy.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup
// Implements the exit code policy behavior.

namespace VectorNNTP.Backfiller.Startup
{
    /// <summary>
    /// Centralizes process exit-code classifications used by startup, command handlers, and top-level host termination paths.
    /// </summary>
    /// <remarks>
    /// These values are written to <see cref="Environment.ExitCode"/> and are externally observable as the process exit status.
    /// The policy is intentionally shared so command-mode execution and full host startup/runtime paths report failures with
    /// consistent category codes.
    /// </remarks>
    internal static class ExitCodePolicy
    {
        /// <summary>
        /// Exit code for successful command completion or normal host shutdown.
        /// </summary>
        internal const int ExitCodeNormalShutdown = 0;

        /// <summary>
        /// Exit code for unexpected failures that are not classified as configuration, dependency, or startup-cancellation outcomes.
        /// </summary>
        internal const int ExitCodeUnexpectedFailure = 1;

        /// <summary>
        /// Exit code for configuration/validation input failures, including invalid arguments and missing required settings.
        /// </summary>
        internal const int ExitCodeConfigurationFailure = 2;

        /// <summary>
        /// Exit code for dependency validation failures where required external infrastructure is unavailable or invalid.
        /// </summary>
        internal const int ExitCodeDependencyFailure = 3;

        /// <summary>
        /// Exit code reserved for unrecoverable storage-layer failure classification.
        /// </summary>
        /// <remarks>
        /// This value is part of the shared policy surface even when specific execution paths in the current codebase do not assign it.
        /// </remarks>
        internal const int ExitCodeStorageFailure = 4;

        /// <summary>
        /// Exit code for startup-phase cancellation/failure before the host reaches steady-state execution.
        /// </summary>
        internal const int ExitCodeStartupFailure = 5;

        /// <summary>
        /// Maps an exit code to a human-readable category description for startup/shutdown logging.
        /// </summary>
        /// <param name="exitCode">Process exit code value to classify.</param>
        /// <returns>
        /// A stable description for known policy codes; signal-derived text for values &gt;= 128; otherwise an unknown-code description.
        /// </returns>
        /// <remarks>
        /// This method is diagnostic-only and does not alter classification behavior. Known policy constants take precedence over
        /// signal-style formatting.
        /// </remarks>
        internal static string GetExitCodeDescription(int exitCode)
        {
            return exitCode switch
            {
                ExitCodeNormalShutdown => "normal shutdown",
                ExitCodeUnexpectedFailure => "unexpected application failure",
                ExitCodeConfigurationFailure => "configuration failure",
                ExitCodeDependencyFailure => "dependency failure",
                ExitCodeStorageFailure => "unrecoverable storage failure",
                ExitCodeStartupFailure => "startup failure",
                _ when exitCode >= 128 => $"terminated by signal {exitCode - 128}",
                _ => $"unknown exit code {exitCode}"
            };
        }
    }
}
