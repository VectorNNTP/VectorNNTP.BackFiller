// <copyright file="BackFillerLetsEncryptRuntimeOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Configuration
// ACME runtime settings consumed by certificate provisioning, renewal, and listener startup.

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Immutable ACME settings used to issue, validate, persist, and renew the BackFiller listener certificate.
    /// </summary>
    /// <remarks>
    /// The values in this snapshot are consumed by startup validation, certificate issuance, renewal, and the
    /// inbound TLS listener. It intentionally carries only the fields needed to complete the ACME DNS-01 workflow
    /// and to activate the resulting listener certificate; no secrets are exposed through logging helpers.
    /// The Cloudflare token and ACME account key are treated as sensitive inputs and are only read from their
    /// configured sources.
    /// </remarks>
    /// <param name="Enabled">Whether ACME certificate management is enabled.</param>
    /// <param name="CanonicalCertificateSubjectName">Authoritative generated BackFiller FQDN used for certificate identity.</param>
    /// <param name="AcmeAccountEmail">ACME account contact email address.</param>
    /// <param name="AcmeAccountKeyPemPath">Absolute path to PEM-encoded ACME account private key.</param>
    /// <param name="CertificatePfxPath">Absolute path to the listener PFX certificate bundle.</param>
    /// <param name="CertificatePrivateKeyPemPath">Absolute path to the persisted certificate private key PEM.</param>
    /// <param name="PfxExportPassword">Password used to protect PFX export operations.</param>
    /// <param name="RenewBeforeExpiryDays">Renewal threshold in days before certificate expiry.</param>
    /// <param name="RenewalCheckIntervalHours">Periodic renewal evaluation interval in hours.</param>
    /// <param name="RenewalJitterRatio">Scheduling jitter ratio applied to periodic renewal checks.</param>
    /// <param name="UseStagingDirectory">Whether ACME staging directory should be used.</param>
    /// <param name="AcmeTransientRetryMaxAttempts">Maximum ACME transient retry attempts.</param>
    /// <param name="DnsPropagationDelaySeconds">Initial DNS propagation delay before authoritative polling.</param>
    /// <param name="DnsTxtPollIntervalSeconds">DNS TXT polling interval in seconds.</param>
    /// <param name="DnsTxtPollTimeoutSeconds">DNS TXT polling timeout in seconds.</param>
    /// <param name="DnsAuthoritativeNsCacheMinutes">Authoritative nameserver cache TTL in minutes.</param>
    /// <param name="DnsAuthoritativeQuorumRatio">Required authoritative TXT visibility quorum ratio.</param>
    /// <param name="CloudFlareApiToken">Cloudflare API token for DNS challenge record lifecycle.</param>
    /// <param name="CloudFlareZoneId">Cloudflare zone identifier for DNS operations.</param>
    internal sealed record BackFillerLetsEncryptRuntimeOptions(
        bool Enabled,
        string CanonicalCertificateSubjectName,
        string AcmeAccountEmail,
        string AcmeAccountKeyPemPath,
        string CertificatePfxPath,
        string CertificatePrivateKeyPemPath,
        string PfxExportPassword,
        int RenewBeforeExpiryDays,
        int RenewalCheckIntervalHours,
        double RenewalJitterRatio,
        bool UseStagingDirectory,
        int AcmeTransientRetryMaxAttempts,
        int DnsPropagationDelaySeconds,
        int DnsTxtPollIntervalSeconds,
        int DnsTxtPollTimeoutSeconds,
        int DnsAuthoritativeNsCacheMinutes,
        double DnsAuthoritativeQuorumRatio,
        string CloudFlareApiToken,
        string CloudFlareZoneId);
}
