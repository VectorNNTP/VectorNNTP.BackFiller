// <copyright file="DiagnosticsCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the diagnostics command handler behavior.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Handles the <c>--diagnostics</c> operational command by printing build, runtime, process, and selected
    /// environment metadata.
    /// </summary>
    /// <remarks>
    /// This command is informational and does not run configuration/dependency validation or emit structured log
    /// events. Output is written directly to standard output for operator inspection.
    /// </remarks>
    internal static class DiagnosticsCommandHandler
    {
        /// <summary>
        /// Writes a snapshot of startup-relevant diagnostics, including build info, host/runtime facts, process context,
        /// and selected environment-variable values.
        /// </summary>
        /// <returns><see cref="ExitCodePolicy.ExitCodeNormalShutdown"/> after diagnostics output is emitted.</returns>
        internal static int Handle()
        {
            Console.WriteLine("=== VectorNNTP.Backfiller Diagnostics ===\n");

            Console.WriteLine("Build Information:");
            Console.WriteLine(BuildInfoService.GetFullVersionString());

            Console.WriteLine("\nSystem Information:");
            Console.WriteLine($"  OS: {System.Runtime.InteropServices.RuntimeInformation.OSDescription}");
            Console.WriteLine($"  Processor Count: {Environment.ProcessorCount}");

            string is64Bit = Environment.Is64BitOperatingSystem ? "Yes" : "No";
            Console.WriteLine($"  64-bit OS: {is64Bit}");

            Console.WriteLine("\nProcess Information:");
            Console.WriteLine($"  PID: {Environment.ProcessId}");
            Console.WriteLine($"  Working Directory: {Environment.CurrentDirectory}");
            Console.WriteLine("  Command: --diagnostics");

            Console.WriteLine("\nEnvironment:");
            Console.WriteLine($"  ASPNETCORE_ENVIRONMENT: {Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "not set"}");
            Console.WriteLine($"  BUILD_VERSION: {Environment.GetEnvironmentVariable("BUILD_VERSION") ?? "not set"}");
            Console.WriteLine($"  BUILD_COMMIT: {Environment.GetEnvironmentVariable("BUILD_COMMIT") ?? "not set"}");

            return ExitCodePolicy.ExitCodeNormalShutdown;
        }
    }
}
