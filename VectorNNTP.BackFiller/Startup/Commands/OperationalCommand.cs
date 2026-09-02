// <copyright file="OperationalCommand.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the operational command behavior.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Defines the operational commands recognized by startup command parsing and dispatch.
    /// </summary>
    /// <remarks>
    /// Enum values are parsed from canonical command-line tokens by <see cref="OperationalCommandParser"/> and
    /// executed through <see cref="OperationalCommandExecutor"/>. Validation diagnostics and logging behavior are
    /// implemented by the command handlers selected for each value.
    /// </remarks>
    internal enum OperationalCommand
    {
        /// <summary>
        /// Shows command usage/help output.
        /// </summary>
        Help,

        /// <summary>
        /// Shows build and version information.
        /// </summary>
        Version,

        /// <summary>
        /// Runs configuration-only validation and reports configuration warnings/errors.
        /// </summary>
        ValidateConfig,

        /// <summary>
        /// Runs full startup readiness validation (configuration and dependencies).
        /// </summary>
        ValidateStartup,

        /// <summary>
        /// Emits startup diagnostics information.
        /// </summary>
        Diagnostics,

        /// <summary>
        /// Dumps effective configuration values to output.
        /// </summary>
        DumpConfig,
    }
}
