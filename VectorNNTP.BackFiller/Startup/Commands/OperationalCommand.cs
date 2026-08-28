// <copyright file="OperationalCommand.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

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
