// <copyright file="LetsEncryptCertificateDependencyProbeTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for lets encrypt certificate dependency probe, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the lets encrypt certificate dependency probe test suite.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Confirms the lets encrypt certificate dependency probe tests behavior.
    /// </summary>
    public sealed class LetsEncryptCertificateDependencyProbeTests
    {
        /// <summary>
        /// Confirms the ensure certificate availability async when acme account key missing returns certificate dependency failure behavior.
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
                (string Dependency, string Reason) failure = Assert.Single(dep.FailedDependencies);
                Assert.Equal("LetsEncryptCertificate", failure.Dependency);
                Assert.StartsWith("TLS certificate provisioning failed: ", failure.Reason, StringComparison.Ordinal);

                string sanitizedSummary = failure.Reason["TLS certificate provisioning failed: ".Length..];
                Assert.Contains(": ", sanitizedSummary, StringComparison.Ordinal);
                Assert.Contains("Exception", sanitizedSummary, StringComparison.Ordinal);
                Assert.DoesNotContain(Environment.NewLine, failure.Reason, StringComparison.Ordinal);
                Assert.DoesNotContain(" at ", failure.Reason, StringComparison.Ordinal);
                Assert.DoesNotContain("--- End of stack trace", failure.Reason, StringComparison.Ordinal);
                Assert.DoesNotContain("LetsEncryptCertificateDependencyProbe.cs", failure.Reason, StringComparison.Ordinal);
                Assert.DoesNotContain("BackFillerCertificateProvisioningService.cs", failure.Reason, StringComparison.Ordinal);
            }
            finally
            {
                DeleteDirectoryIfExists(certDir);
            }
        }

        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create runtime options helper.</returns>
        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <param name="certDir">The cert dir used by this test scenario.</param>
        /// <returns>The value returned by the create runtime options helper.</returns>
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
        /// Confirms the create unique temp directory behavior.
        /// </summary>
        /// <returns>The value returned by the create unique temp directory helper.</returns>
        /// <summary>
        /// Confirms the create unique temp directory behavior.
        /// </summary>
        /// <returns>The value returned by the create unique temp directory helper.</returns>
        private static string CreateUniqueTempDirectory()
        {
            string path = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-LetsEncryptProbeTests-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(path);
            return path;
        }

        /// <summary>
        /// Confirms the delete directory if exists behavior.
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
