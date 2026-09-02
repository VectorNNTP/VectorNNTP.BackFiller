// <copyright file="RuntimeSnapshotFactory.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Startup.Configuration
// Builds the validated runtime snapshot consumed by the BackFiller host and certificate pipeline.

using System.Net;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Builds the immutable runtime configuration snapshot consumed by host composition and dependency/runtime services.
    /// </summary>
    /// <remarks>
    /// This factory is the canonicalization boundary between startup validation and runtime execution.
    /// It resolves normalized identity values, operational directories, transport settings, shutdown policy,
    /// and nested RabbitMQ/ACME option snapshots before hosted services begin running.
    /// </remarks>
    internal class RuntimeSnapshotFactory
    {
        /// <summary>
        /// Attempts to build the immutable runtime-options snapshot from validated startup configuration inputs.
        /// </summary>
        /// <param name="configuration">Configuration root used to resolve operational directories and related settings.</param>
        /// <param name="backFiller">Validated BackFiller options section used to construct runtime identity and service settings.</param>
        /// <param name="configErrors">Mutable validation-error accumulator that receives snapshot-construction failures.</param>
        /// <returns>
        /// A fully populated <see cref="BackFillerRuntimeOptions"/> snapshot when construction succeeds; otherwise
        /// <see langword="null"/> after appending an explanatory error to <paramref name="configErrors"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="configuration"/> or <paramref name="configErrors"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// This method does not surface configuration-construction exceptions to callers. It converts failures into
        /// a single startup validation error entry so the caller can continue unified validation reporting.
        /// </remarks>
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
                BackFillerLetsEncryptRuntimeOptions letsEncryptRuntimeOptions = BuildLetsEncryptRuntimeOptions(backFiller, validatedCertificateDirectory, canonicalBackFillerFqdn);
                RabbitMqRuntimeOptions rabbitMqRuntimeOptions = BuildRabbitMqRuntimeOptions(backFiller, canonicalBackFillerFqdn);

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
                    BindPort: backFiller.BindPort ?? 0,
                    ConfiguredBindAddressTokens: [.. backFiller.BindAddress ?? []],
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
                    CanonicalBindAddresses: canonicalBindAddresses,
                    LetsEncrypt: letsEncryptRuntimeOptions,
                    RabbitMq: rabbitMqRuntimeOptions);
            }
            catch (Exception ex)
            {
                configErrors.Add(("BackFiller", $"Failed to build runtime options snapshot: {ex.Message}"));
                return null;
            }
        }

        /// <summary>
        /// Builds immutable RabbitMQ runtime settings from validated BackFiller options.
        /// </summary>
        /// <param name="backFiller">Validated BackFiller options containing the RabbitMQ section.</param>
        /// <param name="canonicalBackFillerFqdn">Canonical BackFiller FQDN used for diagnostic payload identity settings.</param>
        /// <returns>A <see cref="RabbitMqRuntimeOptions"/> snapshot used by RabbitMQ runtime infrastructure.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="backFiller"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="canonicalBackFillerFqdn"/> is null/empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException">BackFiller RabbitMQ settings are missing from validated input.</exception>
        private static RabbitMqRuntimeOptions BuildRabbitMqRuntimeOptions(
            BackFillerOptions backFiller,
            string canonicalBackFillerFqdn)
        {
            ArgumentNullException.ThrowIfNull(backFiller);
            ArgumentException.ThrowIfNullOrWhiteSpace(canonicalBackFillerFqdn);

            RabbitMqOptions rabbitMq = backFiller.RabbitMQ
                ?? throw new InvalidOperationException("BackFiller:RabbitMQ is required to build runtime options.");

            string[] hosts = [.. (rabbitMq.Hosts ?? [])
                .Where(static x => !string.IsNullOrWhiteSpace(x))
                .Select(static x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)];

            string? username = !string.IsNullOrWhiteSpace(rabbitMq.Username)
                ? rabbitMq.Username.Trim()
                : null;

            string? password = rabbitMq.Password;

            string virtualHost = !string.IsNullOrWhiteSpace(rabbitMq.VirtualHost)
                ? rabbitMq.VirtualHost.Trim()
                : "/";

            return new RabbitMqRuntimeOptions(
                Hosts: hosts,
                Port: rabbitMq.Port ?? 0,
                Username: username,
                Password: password,
                VirtualHost: virtualHost,
                EnableSsl: rabbitMq.EnableSsl ?? false,
                ChannelLeaseTimeoutSeconds: rabbitMq.ChannelLeaseTimeoutSeconds ?? 0,
                RpcTimeoutSeconds: rabbitMq.RpcTimeoutSeconds ?? 0,
                ConnectionBlockedTimeoutSeconds: rabbitMq.ConnectionBlockedTimeoutSeconds ?? 0,
                ChannelPoolSize: rabbitMq.ChannelPoolSize ?? 0,
                MinConnections: rabbitMq.MinConnections ?? 0,
                MaxConnections: rabbitMq.MaxConnections ?? 0,
                MaxConsecutiveRecoveryFailures: rabbitMq.MaxConsecutiveRecoveryFailures ?? 0,
                MaxPendingLeaseWaiters: rabbitMq.MaxPendingLeaseWaiters ?? 0,
                ConnectionScaleDownIdleSeconds: rabbitMq.ConnectionScaleDownIdleSeconds ?? 0,
                ScaleDownCooldownSeconds: rabbitMq.ScaleDownCooldownSeconds ?? 0,
                NetworkRecoveryIntervalSeconds: rabbitMq.NetworkRecoveryIntervalSeconds ?? 0,
                PoolReconnectBaseDelayMs: rabbitMq.PoolReconnectBaseDelayMs ?? 0,
                PoolReconnectMaxDelayMs: rabbitMq.PoolReconnectMaxDelayMs ?? 0,
                MinimumConnectionLifetimeSeconds: rabbitMq.MinimumConnectionLifetimeSeconds ?? 0,
                PublishConfirmTimeoutSeconds: rabbitMq.PublishConfirmTimeoutSeconds ?? 0,
                MaximumShutdownDrainTimeoutSeconds: rabbitMq.MaximumShutdownDrainTimeoutSeconds ?? 0,
                DegradedThreshold: rabbitMq.DegradedThreshold ?? 0,
                UnhealthyThreshold: rabbitMq.UnhealthyThreshold ?? 0,
                RequestedHeartbeatSeconds: rabbitMq.RequestedHeartbeatSeconds ?? 0,
                SocketTimeoutSeconds: rabbitMq.SocketTimeoutSeconds ?? 0,
                RequestedChannelMax: rabbitMq.RequestedChannelMax ?? 0,
                ConsumerPrefetchCount: rabbitMq.ConsumerPrefetchCount,
                DiagnosticPayloadCorrelationId: rabbitMq.DiagnosticPayloadCorrelationId);
        }

        /// <summary>
        /// Builds immutable Let’s Encrypt runtime settings, including canonical certificate paths and ACME policy values.
        /// </summary>
        /// <param name="backFiller">Validated BackFiller options containing Let’s Encrypt settings.</param>
        /// <param name="validatedCertificateDirectory">Canonical validated certificate directory used to compose runtime file paths.</param>
        /// <param name="canonicalBackFillerFqdn">Authoritative generated BackFiller FQDN used as certificate subject identity.</param>
        /// <returns>A <see cref="BackFillerLetsEncryptRuntimeOptions"/> snapshot consumed by certificate provisioning/renewal services.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="backFiller"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="validatedCertificateDirectory"/> or <paramref name="canonicalBackFillerFqdn"/> is null/empty/whitespace.</exception>
        /// <exception cref="InvalidOperationException">Required Let’s Encrypt settings are missing from validated input.</exception>
        private static BackFillerLetsEncryptRuntimeOptions BuildLetsEncryptRuntimeOptions(
            BackFillerOptions backFiller,
            string validatedCertificateDirectory,
            string canonicalBackFillerFqdn)
        {
            ArgumentNullException.ThrowIfNull(backFiller);
            ArgumentException.ThrowIfNullOrWhiteSpace(validatedCertificateDirectory);
            ArgumentException.ThrowIfNullOrWhiteSpace(canonicalBackFillerFqdn);

            LetsEncryptOptions letsEncrypt = backFiller.LetsEncrypt
                ?? throw new InvalidOperationException("BackFiller:LetsEncrypt is required to build runtime options.");

            string acmeAccountEmail = !string.IsNullOrWhiteSpace(letsEncrypt.AcmeAccountEmail)
                ? letsEncrypt.AcmeAccountEmail.Trim()
                : throw new InvalidOperationException("BackFiller:LetsEncrypt:AcmeAccountEmail is required to build runtime options.");

            string acmeAccountKeyPem = !string.IsNullOrWhiteSpace(letsEncrypt.AcmeAccountKeyPem)
                ? letsEncrypt.AcmeAccountKeyPem.Trim()
                : throw new InvalidOperationException("BackFiller:LetsEncrypt:AcmeAccountKeyPem is required to build runtime options.");

            string pfxExportPassword = !string.IsNullOrWhiteSpace(letsEncrypt.PfxExportPassword)
                ? letsEncrypt.PfxExportPassword
                : throw new InvalidOperationException("BackFiller:LetsEncrypt:PfxExportPassword is required to build runtime options.");

            string cloudFlareApiToken = !string.IsNullOrWhiteSpace(letsEncrypt.CloudFlareApiToken)
                ? letsEncrypt.CloudFlareApiToken.Trim()
                : throw new InvalidOperationException("BackFiller:LetsEncrypt:CloudFlareApiToken is required to build runtime options.");

            string cloudFlareZoneId = !string.IsNullOrWhiteSpace(letsEncrypt.CloudFlareZoneId)
                ? letsEncrypt.CloudFlareZoneId.Trim()
                : throw new InvalidOperationException("BackFiller:LetsEncrypt:CloudFlareZoneId is required to build runtime options.");

            string accountKeyPath = Path.Combine(validatedCertificateDirectory, acmeAccountKeyPem);
            string certificatePrivateKeyPemPath = Path.Combine(validatedCertificateDirectory, Runtime.Certificates.CertificateFileConventions.CertificatePrivateKeyPemFileName);
            string certificatePfxPath = Path.Combine(validatedCertificateDirectory, Runtime.Certificates.CertificateFileConventions.ListenerPfxFileName);

            return new BackFillerLetsEncryptRuntimeOptions(
                Enabled: letsEncrypt.Enabled,
                CanonicalCertificateSubjectName: canonicalBackFillerFqdn,
                AcmeAccountEmail: acmeAccountEmail,
                AcmeAccountKeyPemPath: accountKeyPath,
                CertificatePfxPath: certificatePfxPath,
                CertificatePrivateKeyPemPath: certificatePrivateKeyPemPath,
                PfxExportPassword: pfxExportPassword,
                RenewBeforeExpiryDays: letsEncrypt.RenewBeforeExpiryDays ?? 7,
                RenewalCheckIntervalHours: letsEncrypt.RenewalCheckIntervalHours ?? 6,
                RenewalJitterRatio: letsEncrypt.RenewalJitterRatio ?? 0.1,
                UseStagingDirectory: letsEncrypt.UseStagingDirectory,
                AcmeTransientRetryMaxAttempts: letsEncrypt.AcmeTransientRetryMaxAttempts ?? 5,
                DnsPropagationDelaySeconds: letsEncrypt.DnsPropagationDelaySeconds ?? 15,
                DnsTxtPollIntervalSeconds: letsEncrypt.DnsTxtPollIntervalSeconds ?? 3,
                DnsTxtPollTimeoutSeconds: letsEncrypt.DnsTxtPollTimeoutSeconds ?? 600,
                DnsAuthoritativeNsCacheMinutes: letsEncrypt.DnsAuthoritativeNsCacheMinutes ?? 5,
                DnsAuthoritativeQuorumRatio: letsEncrypt.DnsAuthoritativeQuorumRatio ?? 0.7,
                CloudFlareApiToken: cloudFlareApiToken,
                CloudFlareZoneId: cloudFlareZoneId);
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
