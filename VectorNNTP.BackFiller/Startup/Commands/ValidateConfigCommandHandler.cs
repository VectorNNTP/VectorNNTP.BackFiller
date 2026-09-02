// <copyright file="ValidateConfigCommandHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Commands
// Implements the validate config command handler behavior.

using Serilog;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Configuration;

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
        /// <param name="configuration">The configuration value.</param>
        /// <returns>The operation result.</returns>
        internal static int Handle(IConfiguration? configuration)
        {
            Console.WriteLine("Validating configuration...");

            if (configuration == null)
            {
                Console.Error.WriteLine("ERROR: Configuration not available for validation");
                return ExitCodePolicy.ExitCodeUnexpectedFailure;
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

                    return ExitCodePolicy.ExitCodeConfigurationFailure;
                }

                Console.WriteLine("Configuration validation PASSED");
                return ExitCodePolicy.ExitCodeNormalShutdown;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"ERROR: {ex.Message}");
                Log.Fatal(ex, "Configuration validation command failed");
                return ExitCodePolicy.ExitCodeUnexpectedFailure;
            }
        }

        /// <summary>
        /// Handles build validate config command result for validate config command handler.
        /// </summary>
        /// <param name="configuration">The configuration value.</param>
        /// <returns>The operation result.</returns>
        internal static ConfigurationValidationResult BuildValidateConfigCommandResult(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);

            List<(string Setting, string Error)> errors = [];
            List<(string Setting, string Message)> warnings = [];
            BackFillerOptions? backFiller = configuration
                .GetSection("BackFiller")
                .Get<BackFillerOptions>();

            errors.AddRange(ConfigurationValidator.ValidateConnectionStrings(configuration, warnings));
            errors.AddRange(ConfigurationValidator.ValidateBackFillerOptions(backFiller, warnings));

            if (errors.Count == 0)
            {
                _ = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(
                    configuration,
                    backFiller,
                    errors);
            }

            return new ConfigurationValidationResult(errors, warnings);
        }
    }
}
