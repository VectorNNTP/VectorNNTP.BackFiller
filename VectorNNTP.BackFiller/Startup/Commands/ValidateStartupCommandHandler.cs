using Serilog;
using VectorNNTP.Backfiller.Startup.Validation;

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Owns the validate-startup operational command behavior.
    /// </summary>
    internal static class ValidateStartupCommandHandler
    {
        /// <summary>
        /// Validates startup readiness (configuration and dependencies) and returns an appropriate exit code.
        /// </summary>
        internal static int Handle(IConfiguration? configuration)
        {
            Console.WriteLine("Validating startup readiness...");

            if (configuration == null)
            {
                Console.Error.WriteLine("ERROR: Configuration not available for validation");
                return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeUnexpectedFailure;
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

                    return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeConfigurationFailure;
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

                    return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeDependencyFailure;
                }

                Console.WriteLine("Startup validation PASSED");
                return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeNormalShutdown;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"ERROR: {ex.Message}");
                Log.Fatal(ex, "Startup validation command failed");
                return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeUnexpectedFailure;
            }
        }
    }
}
