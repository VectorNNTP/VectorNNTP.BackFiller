// <copyright file="DependencyProbeRunner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Startup.Validation
// Runs startup dependency probes, DNS reconciliation, and certificate availability checks.

using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Owns live external dependency probes, startup DNS reconciliation, and certificate availability checks.
    /// </summary>
    /// <remarks>
    /// The runner first validates structural dependencies, then synchronizes the generated BackFiller A/AAAA records,
    /// and finally ensures a usable listener certificate is available before runtime services are allowed to continue.
    /// </remarks>
    internal class DependencyProbeRunner
    {
        /// <summary>
        /// Validates external dependencies after structural configuration validation succeeds.
        /// </summary>
        /// <param name="configuration">Application configuration root.</param>
        /// <param name="backFiller">Validated BackFiller options model.</param>
        /// <param name="runtimeOptions">Validated immutable runtime options snapshot.</param>
        /// <param name="dependencyTimeout">Maximum timeout per dependency operation.</param>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>Aggregated dependency validation outcome.</returns>
        internal static async Task<DependencyValidationResult> ValidateDependenciesAsync(
            IConfiguration configuration,
            BackFillerOptions? backFiller,
            BackFillerRuntimeOptions runtimeOptions,
            TimeSpan dependencyTimeout,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            Task<DependencyValidationResult> databaseDependencyTask = DatabaseDependencyProbe.ValidateDatabaseConnectivityAsync(
                configuration,
                dependencyTimeout,
                cancellationToken);

            Task<DependencyValidationResult> cloudflareDependencyTask = CloudflareDependencyProbe.ValidateCloudflareZoneDependencyAsync(
                backFiller,
                dependencyTimeout,
                cancellationToken);

            Task<DependencyValidationResult> transitServerDependencyTask = TransitServerDependencyProbe.ValidateTransitServerConnectivityAsync(
                backFiller,
                dependencyTimeout,
                cancellationToken);

            Task<DependencyValidationResult> rabbitMqDependencyTask = RabbitMqDependencyProbe.ValidateRabbitMqConnectivityAsync(
                runtimeOptions,
                dependencyTimeout,
                cancellationToken);

            DependencyValidationResult[] dependencyResults = await Task.WhenAll(
                databaseDependencyTask,
                cloudflareDependencyTask,
                transitServerDependencyTask,
                rabbitMqDependencyTask).ConfigureAwait(false);

            DependencyValidationResult databaseDependencyResult = dependencyResults[0];
            DependencyValidationResult cloudflareDependencyResult = dependencyResults[1];
            DependencyValidationResult transitServerDependencyResult = dependencyResults[2];
            DependencyValidationResult rabbitMqDependencyResult = dependencyResults[3];

            DependencyValidationResult baselineResult = new(
                databaseDependencyResult.FailedDependencies
                    .Concat(cloudflareDependencyResult.FailedDependencies)
                    .Concat(transitServerDependencyResult.FailedDependencies)
                    .Concat(rabbitMqDependencyResult.FailedDependencies),
                databaseDependencyResult.Warnings
                    .Concat(cloudflareDependencyResult.Warnings)
                    .Concat(transitServerDependencyResult.Warnings)
                    .Concat(rabbitMqDependencyResult.Warnings),
                databaseDependencyResult.Errors
                    .Concat(cloudflareDependencyResult.Errors)
                    .Concat(transitServerDependencyResult.Errors)
                    .Concat(rabbitMqDependencyResult.Errors));

            if (!baselineResult.IsValid)
            {
                return baselineResult;
            }

            DependencyValidationResult dnsSynchronizationResult = await CloudflareDnsSynchronizationProbe
                .SynchronizeGeneratedBackFillerDnsAsync(backFiller, runtimeOptions, dependencyTimeout, cancellationToken)
                .ConfigureAwait(false);

            if (!dnsSynchronizationResult.IsValid)
            {
                return new DependencyValidationResult(
                    baselineResult.FailedDependencies
                        .Concat(dnsSynchronizationResult.FailedDependencies),
                    baselineResult.Warnings
                        .Concat(dnsSynchronizationResult.Warnings),
                    baselineResult.Errors
                        .Concat(dnsSynchronizationResult.Errors));
            }

            DependencyValidationResult certificateResult = await LetsEncryptCertificateDependencyProbe
                .EnsureCertificateAvailabilityAsync(runtimeOptions, cancellationToken)
                .ConfigureAwait(false);

            return new DependencyValidationResult(
                baselineResult.FailedDependencies
                    .Concat(dnsSynchronizationResult.FailedDependencies)
                    .Concat(certificateResult.FailedDependencies),
                baselineResult.Warnings
                    .Concat(dnsSynchronizationResult.Warnings)
                    .Concat(certificateResult.Warnings),
                baselineResult.Errors
                    .Concat(dnsSynchronizationResult.Errors)
                    .Concat(certificateResult.Errors));
        }
    }
}
