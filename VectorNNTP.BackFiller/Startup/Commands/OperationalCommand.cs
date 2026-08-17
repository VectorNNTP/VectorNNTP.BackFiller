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
