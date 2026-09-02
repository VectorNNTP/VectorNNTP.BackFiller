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
    /// Orchestrates startup dependency probing by aggregating baseline connectivity checks, DNS synchronization, and certificate readiness validation.
    /// </summary>
    /// <remarks>
    /// This runner composes probe outputs into a single <see cref="DependencyValidationResult"/> and leaves logging and
    /// exit-code decisions to higher startup layers. Later validation phases run only when earlier phases are valid,
    /// preserving deterministic fail-fast gating while still returning aggregated diagnostics for each completed phase.
    /// </remarks>
    internal class DependencyProbeRunner
    {
        /// <summary>
        /// Runs the startup dependency-validation pipeline and returns an aggregated dependency outcome.
        /// </summary>
        /// <param name="configuration">Application configuration root used by dependency probes that require configuration access.</param>
        /// <param name="backFiller">Validated BackFiller options consumed by Cloudflare and transit-server dependency probes.</param>
        /// <param name="runtimeOptions">Validated immutable runtime options snapshot used by RabbitMQ, DNS, and certificate probes.</param>
        /// <param name="dependencyTimeout">Per-operation timeout passed to network dependency probes.</param>
        /// <param name="cancellationToken">Startup cancellation token propagated to all asynchronous probe operations.</param>
        /// <returns>
        /// A task that completes with a <see cref="DependencyValidationResult"/> containing aggregated failures, warnings,
        /// and errors from every executed dependency phase.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="configuration"/> or <paramref name="runtimeOptions"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="OperationCanceledException">The operation is canceled via <paramref name="cancellationToken"/>.</exception>
        /// <remarks>
        /// Baseline probes (database, Cloudflare zone, transit server, RabbitMQ) execute concurrently and are aggregated first.
        /// DNS synchronization executes only when the baseline aggregate is valid. Certificate availability executes only when
        /// both prior phases are valid. This method does not emit logs; it only returns structured diagnostics.
        /// </remarks>
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
