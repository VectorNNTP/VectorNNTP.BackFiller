namespace VectorNNTP.Backfiller.Startup
{
    /// <summary>
    /// Owns exit-code constants and descriptions.
    /// </summary>
    internal static class ExitCodePolicy
    {
        /// <summary>
        /// Exit code for normal, successful service shutdown.
        /// </summary>
        internal const int ExitCodeNormalShutdown = 0;

        /// <summary>
        /// Exit code for unexpected application failure.
        /// </summary>
        internal const int ExitCodeUnexpectedFailure = 1;

        /// <summary>
        /// Exit code for invalid or missing configuration.
        /// </summary>
        internal const int ExitCodeConfigurationFailure = 2;

        /// <summary>
        /// Exit code for dependency unavailable.
        /// </summary>
        internal const int ExitCodeDependencyFailure = 3;

        /// <summary>
        /// Exit code for unrecoverable storage failure.
        /// </summary>
        internal const int ExitCodeStorageFailure = 4;

        /// <summary>
        /// Exit code for startup phase failure.
        /// </summary>
        internal const int ExitCodeStartupFailure = 5;

        /// <summary>
        /// Gets a human-readable description of an exit code for logging.
        /// </summary>
        /// <param name="exitCode">The exit code to describe.</param>
        /// <returns>A descriptive string suitable for log output.</returns>
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
