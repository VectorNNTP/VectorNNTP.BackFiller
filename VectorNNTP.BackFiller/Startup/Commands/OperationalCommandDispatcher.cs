// <copyright file="OperationalCommandDispatcher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: operational command dispatcher in the startup commands subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="OperationalCommandDispatcher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Owns command-line parsing, command selection, and dispatch.
    /// </summary>
    internal static class OperationalCommandDispatcher
    {
        /// <summary>
        /// Parses command-line arguments and executes commands that do not require configuration.
        /// </summary>
        /// <param name="args">Command-line arguments to inspect.</param>
        /// <param name="command">When this method returns, contains a configuration-backed command that must be executed after configuration is available; otherwise <see langword="null"/>.</param>
        /// <param name="exitCode">When this method returns, contains the exit code for a completed parse or command-dispatch path; otherwise <see langword="null"/>.</param>
        /// <returns><see langword="true"/> when startup should continue; otherwise <see langword="false"/>.</returns>
        internal static bool TryDispatchPreConfigurationCommand(
            string[] args,
            out OperationalCommand? command,
            out int? exitCode)
        {
            if (!OperationalCommandParser.TryParseCommandLine(args, out command, out int? parseErrorExitCode))
            {
                exitCode = parseErrorExitCode ?? ExitCodePolicy.ExitCodeConfigurationFailure;
                return false;
            }

            if (command.HasValue && !OperationalCommandExecutor.CommandRequiresConfiguration(command.Value))
            {
                exitCode = OperationalCommandExecutor.ExecuteCommand(command.Value, configuration: null);
                command = null;
                return false;
            }

            exitCode = null;
            return true;
        }

        /// <summary>
        /// Executes a previously parsed command that requires configuration.
        /// </summary>
        /// <param name="command">The parsed operational command.</param>
        /// <param name="configuration">The configuration root used by configuration-backed commands.</param>
        /// <returns>The command exit code.</returns>
        internal static int DispatchPostConfigurationCommand(
            OperationalCommand command,
            IConfiguration configuration)
        {
            return OperationalCommandExecutor.ExecuteCommand(command, configuration);
        }
    }
}
