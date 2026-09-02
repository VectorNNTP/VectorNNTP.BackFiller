// <copyright file="StartupValidationPipeline.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Validation
// Implements the startup validation pipeline behavior.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Configuration;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Orchestrates startup validation by running configuration checks, optional runtime-option snapshot creation, and dependency probes in canonical order.
    /// </summary>
    /// <remarks>
    /// The pipeline returns aggregated result snapshots instead of logging or setting exit codes directly.
    /// Startup callers use those results to decide whether to continue initialization or terminate with a failure code.
    /// </remarks>
    internal class StartupValidationPipeline
    {
        /// <summary>
        /// Runs startup configuration validation and conditional dependency validation, returning both aggregated outcomes.
        /// </summary>
        /// <param name="configuration">Application configuration root to validate.</param>
        /// <param name="dependencyTimeout">Maximum duration allowed for each dependency probe operation.</param>
        /// <param name="cancellationToken">Cancellation token that aborts validation before or during dependency checks.</param>
        /// <returns>
        /// A task that completes with a tuple containing configuration and dependency validation snapshots.
        /// Dependency validation is skipped when configuration is invalid.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="dependencyTimeout"/> is less than or equal to <see cref="TimeSpan.Zero"/>.</exception>
        /// <exception cref="OperationCanceledException">The operation is canceled via <paramref name="cancellationToken"/>.</exception>
        /// <remarks>
        /// This overload delegates to <see cref="ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(IConfiguration, TimeSpan, CancellationToken)"/>
        /// and intentionally discards the runtime-options snapshot.
        /// </remarks>
        internal static async Task<(ConfigurationValidationResult, DependencyValidationResult)> ValidateConfigurationAndDependenciesAsync(
            IConfiguration configuration,
            TimeSpan dependencyTimeout,
            CancellationToken cancellationToken)
        {
            (ConfigurationValidationResult configurationValidationResult, DependencyValidationResult dependencyValidationResult, _) =
                await ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(
                    configuration,
                    dependencyTimeout,
                    cancellationToken).ConfigureAwait(false);

            return (configurationValidationResult, dependencyValidationResult);
        }

        /// <summary>
        /// Validates startup configuration, builds a runtime-options snapshot when configuration is valid, and then runs dependency probes.
        /// </summary>
        /// <param name="configuration">Application configuration root.</param>
        /// <param name="dependencyTimeout">Maximum duration allowed for each dependency probe operation.</param>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>
        /// A task that completes with a tuple containing:
        /// configuration validation results,
        /// dependency validation results,
        /// and a runtime-options snapshot that is non-null only when configuration validation succeeded and snapshot construction completed.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="dependencyTimeout"/> is less than or equal to <see cref="TimeSpan.Zero"/>.</exception>
        /// <exception cref="OperationCanceledException">The operation is canceled via <paramref name="cancellationToken"/>.</exception>
        /// <remarks>
        /// Configuration errors are accumulated before result creation. Dependency validation executes only when
        /// <see cref="ConfigurationValidationResult.IsValid"/> is <see langword="true"/> and a runtime snapshot exists;
        /// otherwise <see cref="DependencyValidationResult.Success()"/> is returned as the dependency result.
        /// </remarks>
        internal static async Task<(ConfigurationValidationResult ConfigurationValidationResult, DependencyValidationResult DependencyValidationResult, BackFillerRuntimeOptions? RuntimeOptions)> ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(
            IConfiguration configuration,
            TimeSpan dependencyTimeout,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(dependencyTimeout, TimeSpan.Zero);

            // Honor already-cancelled tokens immediately to respect the cancellation contract expected by callers/tests.
            // This prevents starting any validation work (including network probes) when shutdown has already been requested.
            cancellationToken.ThrowIfCancellationRequested();

            List<(string Setting, string Error)> configErrors = [];
            List<(string Setting, string Message)> configWarnings = [];

            BackFillerOptions? backFiller = configuration
                .GetSection("BackFiller")
                .Get<BackFillerOptions>();

            // Validate ConnectionStrings section.
            configErrors.AddRange(ConfigurationValidator.ValidateConnectionStrings(configuration, configWarnings));

            // Validate BackFiller section.
            configErrors.AddRange(ConfigurationValidator.ValidateBackFillerOptions(backFiller, configWarnings));

            BackFillerRuntimeOptions? runtimeOptions = null;
            if (configErrors.Count == 0)
            {
                runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(configuration, backFiller, configErrors);
            }

            ConfigurationValidationResult configResult = new(configErrors, configWarnings);

            DependencyValidationResult dependencyResult = configResult.IsValid && runtimeOptions != null
                ? await DependencyProbeRunner.ValidateDependenciesAsync(configuration, backFiller, runtimeOptions, dependencyTimeout, cancellationToken).ConfigureAwait(false)
                : DependencyValidationResult.Success();

            return (configResult, dependencyResult, runtimeOptions);
        }
    }
}
