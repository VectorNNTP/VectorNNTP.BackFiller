// <copyright file="ValidateStartupCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the validate startup command handler behavior.

using Serilog;

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Executes the <c>validate-startup</c> operational command by running startup validation and rendering the
    /// aggregated configuration/dependency outcomes to console output.
    /// </summary>
    /// <remarks>
    /// This handler translates <see cref="ConfigurationValidationResult"/> and <see cref="DependencyValidationResult"/>
    /// into operator-facing <c>[WARN]</c>/<c>[ERROR]</c> lines and maps each outcome to an <see cref="ExitCodePolicy"/>
    /// process result. Unexpected command-path failures are logged as fatal application errors through Serilog.
    /// </remarks>
    internal static class ValidateStartupCommandHandler
    {
        /// <summary>
        /// Runs startup configuration and dependency validation, prints collected warnings/errors, and returns the
        /// command exit code representing the validation outcome.
        /// </summary>
        /// <param name="configuration">The configuration root used by the startup validation pipeline.</param>
        /// <returns>
        /// <see cref="ExitCodePolicy.ExitCodeNormalShutdown"/> when both validation phases are valid;
        /// <see cref="ExitCodePolicy.ExitCodeConfigurationFailure"/> when configuration validation fails;
        /// <see cref="ExitCodePolicy.ExitCodeDependencyFailure"/> when dependency validation fails;
        /// otherwise <see cref="ExitCodePolicy.ExitCodeUnexpectedFailure"/>.
        /// </returns>
        /// <remarks>
        /// Validation diagnostics are emitted to standard output/error as plain text and are not source-generated logs.
        /// The only Serilog call in this method is a fatal log for unexpected exceptions, where <paramref name="configuration"/>
        /// data is not attached as structured fields.
        /// </remarks>
        internal static int Handle(IConfiguration? configuration)
        {
            Console.WriteLine("Validating startup readiness...");

            if (configuration == null)
            {
                Console.Error.WriteLine("ERROR: Configuration not available for validation");
                return ExitCodePolicy.ExitCodeUnexpectedFailure;
            }

            try
            {
                (ConfigurationValidationResult configResult, DependencyValidationResult dependencyResult) =
                    StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                        configuration,
                        dependencyTimeout: TimeSpan.FromSeconds(5),
                        cancellationToken: CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();

                if (configResult.Warnings.Count > 0)
                {
                    Console.WriteLine($"Configuration warnings ({configResult.Warnings.Count}):");
                    foreach ((string setting, string message) in configResult.Warnings)
                    {
                        Console.WriteLine($"  [WARN] {setting}: {message}");
                    }
                }

                if (!configResult.IsValid)
                {
                    Console.Error.WriteLine($"Configuration validation FAILED ({configResult.Errors.Count} error(s)):");
                    foreach ((string setting, string error) in configResult.Errors)
                    {
                        Console.Error.WriteLine($"  [ERROR] {setting}: {error}");
                    }

                    return ExitCodePolicy.ExitCodeConfigurationFailure;
                }

                if (dependencyResult.Warnings.Count > 0)
                {
                    Console.WriteLine($"Dependency warnings ({dependencyResult.Warnings.Count}):");
                    foreach ((string category, string message) in dependencyResult.Warnings)
                    {
                        Console.WriteLine($"  [WARN] {category}: {message}");
                    }
                }

                if (!dependencyResult.IsValid)
                {
                    int totalDependencyFailures = dependencyResult.FailedDependencies.Count + dependencyResult.Errors.Count;
                    Console.Error.WriteLine($"Dependency validation FAILED ({totalDependencyFailures} issue(s)):");

                    foreach ((string dependency, string reason) in dependencyResult.FailedDependencies)
                    {
                        Console.Error.WriteLine($"  [ERROR] {dependency}: {reason}");
                    }

                    foreach ((string category, string message) in dependencyResult.Errors)
                    {
                        Console.Error.WriteLine($"  [ERROR] {category}: {message}");
                    }

                    return ExitCodePolicy.ExitCodeDependencyFailure;
                }

                Console.WriteLine("Startup validation PASSED");
                return ExitCodePolicy.ExitCodeNormalShutdown;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"ERROR: {ex.Message}");
                Log.Fatal(ex, "Startup validation command failed");
                return ExitCodePolicy.ExitCodeUnexpectedFailure;
            }
        }
    }
}
