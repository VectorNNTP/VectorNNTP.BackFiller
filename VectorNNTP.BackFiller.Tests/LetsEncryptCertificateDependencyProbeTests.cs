// <copyright file="LetsEncryptCertificateDependencyProbeTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for lets encrypt certificate dependency probe.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Documents the LetsEncryptCertificateDependencyProbeTests test type and its protected contract.
    /// </summary>
    public sealed class LetsEncryptCertificateDependencyProbeTests
    {
        /// <summary>
        /// Verifies the EnsureCertificateAvailabilityAsync_WhenAcmeAccountKeyMissing_ReturnsCertificateDependencyFailure scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenAcmeAccountKeyMissing_ReturnsCertificateDependencyFailure()
        {
            string certDir = CreateUniqueTempDirectory();

            try
            {
                BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(certDir);

                DependencyValidationResult dep = await LetsEncryptCertificateDependencyProbe
                    .EnsureCertificateAvailabilityAsync(runtimeOptions, CancellationToken.None);

                Assert.False(dep.IsValid);
                Assert.Contains(dep.FailedDependencies, static f => f.Dependency == "LetsEncryptCertificate");
            }
            finally
            {
                DeleteDirectoryIfExists(certDir);
            }
        }

        /// <summary>
        /// Verifies the CreateRuntimeOptions scenario and expected contract.
        /// </summary>
        private static BackFillerRuntimeOptions CreateRuntimeOptions(string certDir)
        {
            BackFillerLetsEncryptRuntimeOptions letsEncrypt = new(
                Enabled: true,
                CanonicalCertificateSubjectName: "backfiller-01.usenet.ninja",
                AcmeAccountEmail: "security@usenet.ninja",
                AcmeAccountKeyPemPath: Path.Combine(certDir, "missing-account.key"),
                CertificatePfxPath: Path.Combine(certDir, "backfiller-listener.pfx"),
                CertificatePrivateKeyPemPath: Path.Combine(certDir, "certificate.key"),
                PfxExportPassword: "UnitTest-PfxPassword-123!",
                RenewBeforeExpiryDays: 7,
                RenewalCheckIntervalHours: 6,
                RenewalJitterRatio: 0.1,
                UseStagingDirectory: true,
                AcmeTransientRetryMaxAttempts: 5,
                DnsPropagationDelaySeconds: 0,
                DnsTxtPollIntervalSeconds: 1,
                DnsTxtPollTimeoutSeconds: 5,
                DnsAuthoritativeNsCacheMinutes: 5,
                DnsAuthoritativeQuorumRatio: 0.7,
                CloudFlareApiToken: "token",
                CloudFlareZoneId: "zoneid");

            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "backfiller-01.usenet.ninja",
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
                ValidatedLogDirectory: certDir,
                ValidatedCertificateDirectory: certDir,
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 119,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 120,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                LetsEncrypt: letsEncrypt);
        }

        /// <summary>
        /// Verifies the CreateUniqueTempDirectory scenario and expected contract.
        /// </summary>
        private static string CreateUniqueTempDirectory()
        {
            string path = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-LetsEncryptProbeTests-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(path);
            return path;
        }

        /// <summary>
        /// Verifies the DeleteDirectoryIfExists scenario and expected contract.
        /// </summary>
        private static void DeleteDirectoryIfExists(string path)
        {
            if (!Directory.Exists(path))
            {
                return;
            }

            try
            {
                Directory.Delete(path, recursive: true);
            }
            catch
            {
            }
        }
    }
}
