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
            Console.WriteLine(global::VectorNNTP.Backfiller.Startup.BuildInfoService.GetFullVersionString());
            return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeNormalShutdown;
        }
    }
}
