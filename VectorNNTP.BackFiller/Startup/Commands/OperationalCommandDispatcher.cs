// <copyright file="OperationalCommandDispatcher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the operational command dispatcher behavior.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Coordinates pre-configuration command parsing/dispatch and post-configuration command execution for startup.
    /// </summary>
    /// <remarks>
    /// This type orchestrates command-flow decisions only. Validation diagnostics and command-specific logging are
    /// produced by downstream command handlers and validation pipeline components.
    /// </remarks>
    internal static class OperationalCommandDispatcher
    {
        /// <summary>
        /// Parses command-line arguments, executes immediate commands that do not require configuration, and reports
        /// whether startup should continue to configuration loading.
        /// </summary>
        /// <param name="args">Raw command-line argument tokens.</param>
        /// <param name="command">
        /// When this method returns, contains the parsed command that must be executed after configuration is available;
        /// otherwise <see langword="null"/>.
        /// </param>
        /// <param name="exitCode">
        /// When this method returns, contains the exit code for parse failure or completed pre-configuration command
        /// execution; otherwise <see langword="null"/> when startup should continue.
        /// </param>
        /// <returns><see langword="true"/> when configuration loading and host startup should continue; otherwise <see langword="false"/>.</returns>
        /// <remarks>
        /// Parse failures and unsupported argument shapes are handled by <see cref="OperationalCommandParser"/>.
        /// Commands classified by <see cref="OperationalCommandExecutor.CommandRequiresConfiguration(OperationalCommand)"/>
        /// as non-configuration commands are executed immediately in this method.
        /// </remarks>
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
        /// Executes a previously parsed configuration-backed command after configuration has been constructed.
        /// </summary>
        /// <param name="command">Parsed operational command selected during pre-configuration parsing.</param>
        /// <param name="configuration">Loaded configuration root supplied to configuration-dependent command handlers.</param>
        /// <returns>The command exit code returned by <see cref="OperationalCommandExecutor.ExecuteCommand(OperationalCommand, IConfiguration?)"/>.</returns>
        internal static int DispatchPostConfigurationCommand(
            OperationalCommand command,
            IConfiguration configuration)
        {
            return OperationalCommandExecutor.ExecuteCommand(command, configuration);
        }
    }
}
