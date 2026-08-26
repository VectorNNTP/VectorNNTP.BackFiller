using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Owns live external dependency probes and startup DNS reconciliation.
    /// </summary>
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

            DependencyValidationResult[] dependencyResults = await Task.WhenAll(
                databaseDependencyTask,
                cloudflareDependencyTask,
                transitServerDependencyTask).ConfigureAwait(false);

            DependencyValidationResult databaseDependencyResult = dependencyResults[0];
            DependencyValidationResult cloudflareDependencyResult = dependencyResults[1];
            DependencyValidationResult transitServerDependencyResult = dependencyResults[2];

            DependencyValidationResult baselineResult = new(
                databaseDependencyResult.FailedDependencies
                    .Concat(cloudflareDependencyResult.FailedDependencies)
                    .Concat(transitServerDependencyResult.FailedDependencies),
                databaseDependencyResult.Warnings
                    .Concat(cloudflareDependencyResult.Warnings)
                    .Concat(transitServerDependencyResult.Warnings),
                databaseDependencyResult.Errors
                    .Concat(cloudflareDependencyResult.Errors)
                    .Concat(transitServerDependencyResult.Errors));

            if (!baselineResult.IsValid)
            {
                return baselineResult;
            }

            DependencyValidationResult dnsSynchronizationResult = await CloudflareDnsSynchronizationProbe
                .SynchronizeGeneratedBackFillerDnsAsync(backFiller, runtimeOptions, dependencyTimeout, cancellationToken)
                .ConfigureAwait(false);

            return new DependencyValidationResult(
                baselineResult.FailedDependencies
                    .Concat(dnsSynchronizationResult.FailedDependencies),
                baselineResult.Warnings
                    .Concat(dnsSynchronizationResult.Warnings),
                baselineResult.Errors
                    .Concat(dnsSynchronizationResult.Errors));
        }
    }
}
