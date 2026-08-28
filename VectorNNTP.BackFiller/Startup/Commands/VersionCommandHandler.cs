// <copyright file="VersionCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Owns the version operational command behavior.
    /// </summary>
    internal static class VersionCommandHandler
    {
        /// <summary>
        /// Displays version and build information.
        /// </summary>
        internal static int Handle()
        {
            Console.WriteLine(BuildInfoService.GetFullVersionString());
            return ExitCodePolicy.ExitCodeNormalShutdown;
        }
    }
}
