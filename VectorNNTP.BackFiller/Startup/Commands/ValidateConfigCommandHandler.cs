using Serilog;
using VectorNNTP.Backfiller.Startup.Validation;

namespace VectorNNTP.Backfiller.Startup.Commands
{
    /// <summary>
    /// Owns the validate-config operational command behavior.
    /// </summary>
    internal static class ValidateConfigCommandHandler
    {
        /// <summary>
        /// Validates configuration only and returns an appropriate exit code.
        /// </summary>
        internal static int Handle(IConfiguration? configuration)
        {
            Console.WriteLine("Validating configuration...");

            if (configuration == null)
            {
                Console.Error.WriteLine("ERROR: Configuration not available for validation");
                return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeUnexpectedFailure;
            }

            try
            {
                ConfigurationValidationResult configResult = BuildValidateConfigCommandResult(configuration);

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

                Console.WriteLine("Configuration validation PASSED");
                return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeNormalShutdown;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"ERROR: {ex.Message}");
                Log.Fatal(ex, "Configuration validation command failed");
                return global::VectorNNTP.Backfiller.Startup.ExitCodePolicy.ExitCodeUnexpectedFailure;
            }
        }

        internal static ConfigurationValidationResult BuildValidateConfigCommandResult(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);

            List<(string Setting, string Error)> errors = [];
            List<(string Setting, string Message)> warnings = [];
            global::VectorNNTP.Backfiller.Configuration.BackFillerOptions? backFiller = configuration
                .GetSection("BackFiller")
                .Get<global::VectorNNTP.Backfiller.Configuration.BackFillerOptions>();

            errors.AddRange(global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateConnectionStrings(configuration, warnings));
            errors.AddRange(global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(backFiller, warnings));

            if (errors.Count == 0)
            {
                _ = global::VectorNNTP.Backfiller.Startup.Configuration.RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(
                    configuration,
                    backFiller,
                    errors);
            }

            return new ConfigurationValidationResult(errors, warnings);
        }
    }
}
