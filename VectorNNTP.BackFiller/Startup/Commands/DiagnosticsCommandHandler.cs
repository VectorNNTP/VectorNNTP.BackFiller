// <copyright file="DiagnosticsCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the diagnostics command handler behavior.

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Owns the diagnostics operational command behavior.
    /// </summary>
    internal static class DiagnosticsCommandHandler
    {
        /// <summary>
        /// Displays startup diagnostics.
        /// </summary>
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
