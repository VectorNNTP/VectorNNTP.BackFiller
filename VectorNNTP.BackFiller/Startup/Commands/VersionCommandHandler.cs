// <copyright file="VersionCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the version command handler behavior.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Handles the <c>version</c> operational command by writing build/version diagnostics to standard output.
    /// </summary>
    /// <remarks>
    /// This command-path helper is output-only and does not participate in configuration validation or validation
    /// logging. It delegates content generation to <see cref="BuildInfoService"/> and returns a process exit code.
    /// </remarks>
    internal static class VersionCommandHandler
    {
        /// <summary>
        /// Writes the full build/version summary for the running binary to standard output.
        /// </summary>
        /// <returns>
        /// <see cref="ExitCodePolicy.ExitCodeNormalShutdown"/> after successful command handling.
        /// </returns>
        internal static int Handle()
        {
            Console.WriteLine(BuildInfoService.GetFullVersionString());
            return ExitCodePolicy.ExitCodeNormalShutdown;
        }
    }
}
