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
    /// Owns the canonical startup validation sequence and aggregates configuration validation, canonicalization, and dependency validation into one startup outcome.
    /// </summary>
    internal class StartupValidationPipeline
    {
        /// <summary>
        /// Validates configuration and dependencies.
        /// </summary>
        /// <remarks>
        /// <para>Validates configuration settings for correctness and completeness:</para>
        /// <list type="bullet">
        /// <item><description>ConnectionStrings:GrabberDB - control-plane database connection</description></item>
        /// <item><description>BackFiller:BindAddress - optional explicit local or wildcard IP addresses for TCP listeners</description></item>
        /// <item><description>BackFiller:BindPort - TCP port for incoming connections</description></item>
        /// </list>
        /// <para>All configuration validation errors are collected before reporting.</para>
        /// <para>External dependency validation is executed only when configuration is structurally valid.</para>
        /// <para>Configuration errors always block startup.</para>
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
        /// Validates startup configuration/dependencies and builds an immutable runtime options snapshot.
        /// </summary>
        /// <param name="configuration">Application configuration root.</param>
        /// <param name="dependencyTimeout">Maximum timeout per dependency validation operation.</param>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>Validation results and immutable runtime options when configuration is valid.</returns>
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
