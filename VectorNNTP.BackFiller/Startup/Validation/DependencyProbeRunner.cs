using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Owns live external dependency probes only.
    /// </summary>
    internal class DependencyProbeRunner
    {
        /// <summary>
        /// Validates external dependencies concurrently after structural configuration validation succeeds.
        /// </summary>
        internal static async Task<DependencyValidationResult> ValidateDependenciesAsync(
            IConfiguration configuration,
            BackFillerOptions? backFiller,
            TimeSpan dependencyTimeout,
            CancellationToken cancellationToken)
        {
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

            return new DependencyValidationResult(
                databaseDependencyResult.FailedDependencies
                    .Concat(cloudflareDependencyResult.FailedDependencies)
                    .Concat(transitServerDependencyResult.FailedDependencies),
                databaseDependencyResult.Warnings
                    .Concat(cloudflareDependencyResult.Warnings)
                    .Concat(transitServerDependencyResult.Warnings),
                databaseDependencyResult.Errors
                    .Concat(cloudflareDependencyResult.Errors)
                    .Concat(transitServerDependencyResult.Errors));
        }
    }
}
