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
    /// Executes the <c>validate-config</c> operational command by running configuration-only validation and rendering
    /// the aggregated outcome to console output.
    /// </summary>
    /// <remarks>
    /// This handler converts <see cref="ConfigurationValidationResult"/> contents into operator-facing
    /// <c>[WARN]</c>/<c>[ERROR]</c> console lines and maps the result to <see cref="ExitCodePolicy"/> exit codes.
    /// Unexpected command-path failures are logged through Serilog as fatal application errors.
    /// </remarks>
    internal static class ValidateConfigCommandHandler
    {
        /// <summary>
        /// Validates configuration, writes collected warnings/errors to console output, and returns the exit code for
        /// the resulting validation state.
        /// </summary>
        /// <param name="configuration">The configuration root used to perform configuration-only validation.</param>
        /// <returns>
        /// <see cref="ExitCodePolicy.ExitCodeNormalShutdown"/> when configuration is valid;
        /// <see cref="ExitCodePolicy.ExitCodeConfigurationFailure"/> when validation errors are present;
        /// otherwise <see cref="ExitCodePolicy.ExitCodeUnexpectedFailure"/>.
        /// </returns>
        /// <remarks>
        /// Validation findings are emitted as plain console output rather than structured logging. The only Serilog call
        /// in this method records unexpected exceptions as fatal command failures.
        /// </remarks>
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
        /// Builds the aggregated configuration-validation result used by <see cref="Handle(IConfiguration?)"/>.
        /// </summary>
        /// <param name="configuration">The configuration root to validate.</param>
        /// <returns>
        /// A <see cref="ConfigurationValidationResult"/> containing all collected configuration errors and warnings.
        /// </returns>
        /// <remarks>
        /// This method runs structural configuration validators and, when no errors are present, attempts runtime
        /// snapshot construction to surface canonicalization failures as configuration errors in the same result.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is <see langword="null"/>.</exception>
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
