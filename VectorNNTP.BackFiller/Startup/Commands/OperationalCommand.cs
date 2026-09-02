// <copyright file="OperationalCommand.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the operational command responsibilities for this subsystem boundary.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Operational command recognized during early startup dispatch.
    /// </summary>
    internal enum OperationalCommand
    {
        Help,
        Version,
        ValidateConfig,
        ValidateStartup,
        Diagnostics,
        DumpConfig,
    }
}
