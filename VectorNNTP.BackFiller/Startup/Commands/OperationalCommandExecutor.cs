// <copyright file="OperationalCommandExecutor.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the operational command executor behavior.

using System.Diagnostics;

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Owns command execution selection and configuration requirements.
    /// </summary>
    internal static class OperationalCommandExecutor
    {
        /// <summary>
        /// Executes the parsed operational command.
        /// </summary>
        /// <param name="command">The parsed operational command.</param>
        /// <param name="configuration">Configuration used by commands that require it.</param>
        /// <returns>The operation result.</returns>
        internal static int ExecuteCommand(OperationalCommand command, IConfiguration? configuration)
        {
            return command switch
            {
                OperationalCommand.Help => HelpCommandHandler.Handle(),
                OperationalCommand.Version => VersionCommandHandler.Handle(),
                OperationalCommand.ValidateConfig => ValidateConfigCommandHandler.Handle(configuration),
                OperationalCommand.ValidateStartup => ValidateStartupCommandHandler.Handle(configuration),
                OperationalCommand.Diagnostics => DiagnosticsCommandHandler.Handle(),
                OperationalCommand.DumpConfig => DumpConfigCommandHandler.Handle(configuration),
                _ => throw new UnreachableException($"Unsupported command enum value: {command}")
            };
        }

        /// <summary>
        /// Determines whether a parsed command requires configuration to execute.
        /// </summary>
        /// <param name="command">The command value.</param>
        /// <returns>true when the operation succeeds; otherwise false.</returns>
        internal static bool CommandRequiresConfiguration(OperationalCommand command)
        {
            return command is OperationalCommand.ValidateConfig
                or OperationalCommand.ValidateStartup
                or OperationalCommand.DumpConfig;
        }
    }
}
