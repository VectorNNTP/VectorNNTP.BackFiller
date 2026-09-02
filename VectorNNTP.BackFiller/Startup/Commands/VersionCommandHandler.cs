// <copyright file="VersionCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the version command handler behavior.

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
