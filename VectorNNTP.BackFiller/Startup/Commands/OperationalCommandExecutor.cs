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
    /// Dispatches parsed operational commands to their handlers and exposes whether each command requires
    /// configuration to execute.
    /// </summary>
    /// <remarks>
    /// This type performs routing only; validation diagnostics and logging behavior are owned by the selected command
    /// handlers. Unsupported enum values are treated as programming errors via <see cref="UnreachableException"/>.
    /// </remarks>
    internal static class OperationalCommandExecutor
    {
        /// <summary>
        /// Executes one parsed operational command by delegating to its command-specific handler.
        /// </summary>
        /// <param name="command">The parsed operational command to execute.</param>
        /// <param name="configuration">
        /// Configuration root passed through to handlers that require configuration-backed execution.
        /// Informational commands ignore this argument and callers may pass <see langword="null"/> for those paths.
        /// </param>
        /// <returns>The exit code returned by the selected command handler.</returns>
        /// <exception cref="UnreachableException">The <paramref name="command"/> value is not a supported <see cref="OperationalCommand"/> member.</exception>
        /// <remarks>
        /// This dispatcher does not add logging, severity, or event-id semantics; those are defined by downstream
        /// command handlers.
        /// </remarks>
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
        /// Classifies commands that must wait until configuration has been built.
        /// </summary>
        /// <param name="command">The command value to classify.</param>
        /// <returns>
        /// <see langword="true"/> for commands that need configuration-backed execution
        /// (<see cref="OperationalCommand.ValidateConfig"/>, <see cref="OperationalCommand.ValidateStartup"/>, and <see cref="OperationalCommand.DumpConfig"/>);
        /// otherwise <see langword="false"/>.
        /// </returns>
        internal static bool CommandRequiresConfiguration(OperationalCommand command)
        {
            return command is OperationalCommand.ValidateConfig
                or OperationalCommand.ValidateStartup
                or OperationalCommand.DumpConfig;
        }
    }
}
