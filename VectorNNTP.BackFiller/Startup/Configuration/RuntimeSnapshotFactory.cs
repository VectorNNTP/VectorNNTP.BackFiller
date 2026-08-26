using System.Net;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Owns canonicalization of validated configuration into the immutable BackFiller runtime and startup snapshot.
    /// </summary>
    internal class RuntimeSnapshotFactory
    {
        /// <summary>
        /// Builds an immutable runtime options snapshot from validated/canonicalized startup configuration.
        /// </summary>
        internal static BackFillerRuntimeOptions? BuildRuntimeOptionsSnapshot(
            IConfiguration configuration,
            BackFillerOptions? backFiller,
            List<(string Setting, string Error)> configErrors)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            ArgumentNullException.ThrowIfNull(configErrors);

            if (backFiller == null)
            {
                configErrors.Add(("BackFiller", "BackFiller section is missing from configuration"));
                return null;
            }

            try
            {
                string canonicalDnsSuffix = BackFillerIdentityValidator.CanonicalizeDnsSuffix(backFiller.DnsSuffix);
                string backFillerName = !string.IsNullOrWhiteSpace(backFiller.Name)
                    ? backFiller.Name
                    : throw new InvalidOperationException("BackFiller:Name is required to build runtime options.");
                int backFillerId = backFiller.Id ?? throw new InvalidOperationException("BackFiller:Id is required to build runtime options.");
                string canonicalBackFillerFqdn = BackFillerIdentityValidator.BuildBackFillerFqdn(backFillerName, backFillerId, canonicalDnsSuffix);

                string validatedLogDirectory = ResolveAndValidateLogDirectory(configuration);
                string validatedCertificateDirectory = ResolveAndValidateCertificateDirectory(configuration);

                string[] rabbitMqHosts = [.. (backFiller.RabbitMQ?.Hosts ?? [])
                    .Where(static x => !string.IsNullOrWhiteSpace(x))
                    .Select(static x => x.Trim())];

                string transitServerHost = backFiller.TransitServer?.Host?.Trim()
                    ?? throw new InvalidOperationException("BackFiller:TransitServer:Host is required to build runtime options.");

                IReadOnlyList<IPAddress> canonicalBindAddresses = BindAddressDnsAddressDeriver.DeriveCanonicalDnsAddresses(backFiller.BindAddress);

                return new BackFillerRuntimeOptions(
                    CanonicalBackFillerFqdn: canonicalBackFillerFqdn,
                    BackFillerId: backFillerId,
                    CanonicalDnsSuffix: canonicalDnsSuffix,
                    ValidatedLogDirectory: validatedLogDirectory,
                    ValidatedCertificateDirectory: validatedCertificateDirectory,
                    RabbitMqHosts: rabbitMqHosts,
                    RabbitMqPort: backFiller.RabbitMQ?.Port ?? 0,
                    RabbitMqEnableSsl: backFiller.RabbitMQ?.EnableSsl ?? false,
                    TransitServerHost: transitServerHost,
                    TransitServerPort: backFiller.TransitServer?.Port ?? 0,
                    TransitServerUseSsl: backFiller.TransitServer?.UseSsl ?? false,
                    ShutdownGracePeriodSeconds: backFiller.Shutdown?.GracePeriodSeconds ?? 30,
                    ShutdownDrainQueuedWork: backFiller.Shutdown?.DrainQueuedWork ?? true,
                    ShutdownFinishActiveArticles: backFiller.Shutdown?.FinishActiveArticles ?? true,
                    RabbitMqMaximumShutdownDrainTimeoutSeconds: backFiller.RabbitMQ?.MaximumShutdownDrainTimeoutSeconds ?? 30,
                    WriteBatchCoalesceMicroseconds: 250,
                    TransitQueueMaxItemCount: 2048,
                    TransitQueueMaxPayloadBytes: 536_870_912,
                    TransitRetryMaxAttempts: 3,
                    TransitShutdownDrainGracePeriod: TimeSpan.FromMinutes(5),
                    TransitShutdownDrainInactivityWatchdog: TimeSpan.FromSeconds(30),
                    TransitShutdownAbsoluteMaximum: TimeSpan.FromMinutes(30),
                    CanonicalBindAddresses: canonicalBindAddresses);
            }
            catch (Exception ex)
            {
                configErrors.Add(("BackFiller", $"Failed to build runtime options snapshot: {ex.Message}"));
                return null;
            }
        }

        /// <summary>
        /// Resolves and validates the configured log directory.
        /// </summary>
        /// <param name="configuration">Application configuration root.</param>
        /// <returns>Canonical absolute path to the validated logging directory.</returns>
        /// <exception cref="InvalidOperationException">Thrown when log directory configuration is invalid or unusable.</exception>
        private static string ResolveAndValidateLogDirectory(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            return OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration);
        }

        /// <summary>
        /// Resolves and validates the configured certificate directory.
        /// </summary>
        /// <param name="configuration">Application configuration root.</param>
        /// <returns>Canonical absolute path to the validated certificate directory.</returns>
        /// <exception cref="InvalidOperationException">Thrown when certificate directory configuration is invalid or unusable.</exception>
        private static string ResolveAndValidateCertificateDirectory(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            return OperationalDirectoryValidator.ResolveAndValidateCertificateDirectory(configuration);
        }
    }
}
