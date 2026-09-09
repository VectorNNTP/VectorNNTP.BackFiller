// <copyright file="BackFillerCertificateProvisioningServiceTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for back filler certificate provisioning service, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the back filler certificate provisioning service test suite.

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Certificates
{
    /// <summary>
    /// Confirms the back filler certificate provisioning service tests behavior.
    /// </summary>
    public sealed class BackFillerCertificateProvisioningServiceTests
    {
        /// <summary>
        /// Confirms the ensure certificate availability async when certificate missing provisions and publishes behavior.
        /// </summary>
        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenCertificateMissing_ProvisionsAndPublishes()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);

                BackFillerCertificateState state = new();
                BackFillerCertificateStore store = new();
                FakeAcmeCertificateIssuer issuer = new("bf-01.example.com");
                BackFillerCertificateProvisioningService service = new(
                    store,
                    issuer,
                    state,
                    NullLogger<BackFillerCertificateProvisioningService>.Instance,
                    TimeProvider.System);

                await service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None);

                Assert.Equal(1, issuer.IssueCallCount);
                Assert.True(state.HasCertificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }
        /// <summary>
        /// Confirms the try renew if due async when certificate not due does not issue behavior.
        /// </summary>
        [Fact]
        public async Task TryRenewIfDueAsync_WhenCertificateNotDue_DoesNotIssue()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com", renewBeforeExpiryDays: 7);
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                WriteValidPfx(letsEncrypt.CertificatePfxPath, letsEncrypt.PfxExportPassword, "bf-01.example.com", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

                BackFillerCertificateState state = new();
                BackFillerCertificateStore store = new();
                FakeAcmeCertificateIssuer issuer = new("bf-01.example.com");
                BackFillerCertificateProvisioningService service = new(
                    store,
                    issuer,
                    state,
                    NullLogger<BackFillerCertificateProvisioningService>.Instance,
                    TimeProvider.System);

                bool renewed = await service.TryRenewIfDueAsync(runtime, CancellationToken.None);

                Assert.False(renewed);
                Assert.Equal(0, issuer.IssueCallCount);
                Assert.True(state.HasCertificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }
        /// <summary>
        /// Confirms the ensure certificate availability async when called concurrently only provisions once behavior.
        /// </summary>
        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenCalledConcurrently_OnlyProvisionsOnce()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);

                BackFillerCertificateState state = new();
                BackFillerCertificateStore store = new();
                FakeAcmeCertificateIssuer issuer = new("bf-01.example.com");
                BackFillerCertificateProvisioningService service = new(
                    store,
                    issuer,
                    state,
                    NullLogger<BackFillerCertificateProvisioningService>.Instance,
                    TimeProvider.System);

                Task first = service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None);
                Task second = service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None);

                await Task.WhenAll(first, second);

                Assert.Equal(1, issuer.IssueCallCount);
                Assert.True(state.HasCertificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task TryRenewIfDueAsync_WhenReplacementSucceeds_DisposesEvaluatedBundle()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com", renewBeforeExpiryDays: 10);
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                WriteValidPfx(letsEncrypt.CertificatePfxPath, letsEncrypt.PfxExportPassword, "bf-01.example.com", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(5));

                BackFillerCertificateBundle? observedEvaluatedBundle = null;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    new FakeAcmeCertificateIssuer("bf-01.example.com"),
                    evaluateExistingCertificateAsync: null,
                    evaluatedBundleObserver: bundle => observedEvaluatedBundle = bundle);

                bool renewed = await service.TryRenewIfDueAsync(runtime, CancellationToken.None);

                Assert.True(renewed);
                Assert.NotNull(observedEvaluatedBundle);
                AssertCertificateDisposed(observedEvaluatedBundle!.Certificate);

                using X509Certificate2? clone = state.GetCurrentCertificateClone();
                Assert.NotNull(clone);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenReusingEvaluatedBundle_TransfersOwnershipAndEvaluatorDoesNotDispose()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com", renewBeforeExpiryDays: 7);
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                WriteValidPfx(letsEncrypt.CertificatePfxPath, letsEncrypt.PfxExportPassword, "bf-01.example.com", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

                BackFillerCertificateBundle? observedEvaluatedBundle = null;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    new UnexpectedIssueAcmeCertificateIssuer(),
                    evaluateExistingCertificateAsync: null,
                    evaluatedBundleObserver: bundle => observedEvaluatedBundle = bundle);

                await service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None);

                Assert.NotNull(observedEvaluatedBundle);
                Assert.True(state.HasCertificate);
                AssertCertificateUsable(observedEvaluatedBundle!.Certificate);

                state.Dispose();
                AssertCertificateDisposed(observedEvaluatedBundle.Certificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task TryRenewIfDueAsync_WhenReplacementCanceledAfterEvaluation_DisposesEvaluatedBundle()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com", renewBeforeExpiryDays: 10);
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                WriteValidPfx(letsEncrypt.CertificatePfxPath, letsEncrypt.PfxExportPassword, "bf-01.example.com", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(5));

                BackFillerCertificateBundle? observedEvaluatedBundle = null;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    new CanceledIssueAcmeCertificateIssuer(),
                    evaluateExistingCertificateAsync: null,
                    evaluatedBundleObserver: bundle => observedEvaluatedBundle = bundle);

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.TryRenewIfDueAsync(runtime, CancellationToken.None));

                Assert.NotNull(observedEvaluatedBundle);
                Assert.False(state.HasCertificate);
                AssertCertificateDisposed(observedEvaluatedBundle!.Certificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenProvisioningFailsWithUsableEvaluation_TransfersFallbackAndEvaluatorDoesNotDispose()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com", renewBeforeExpiryDays: 10);
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                WriteValidPfx(letsEncrypt.CertificatePfxPath, letsEncrypt.PfxExportPassword, "bf-01.example.com", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(5));

                BackFillerCertificateBundle? observedEvaluatedBundle = null;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    new ThrowingIssueAcmeCertificateIssuer(new InvalidOperationException("renewal failed")),
                    evaluateExistingCertificateAsync: null,
                    evaluatedBundleObserver: bundle => observedEvaluatedBundle = bundle);

                await service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None);

                Assert.NotNull(observedEvaluatedBundle);
                Assert.True(state.HasCertificate);
                AssertCertificateUsable(observedEvaluatedBundle!.Certificate);

                state.Dispose();
                AssertCertificateDisposed(observedEvaluatedBundle.Certificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenProvisioningFailsWithoutUsableFallback_DisposesEvaluatedBundleAndPropagatesException()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com", renewBeforeExpiryDays: 10);
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                BackFillerCertificateBundle evaluatedBundle = CreateInMemoryBundle("bf-01.example.com");
                BackFillerCertificateBundle? observedEvaluatedBundle = null;

                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    new ThrowingIssueAcmeCertificateIssuer(new InvalidOperationException("provision failed")),
                    (_, _, _) => Task.FromResult(new CertificateEvaluationResult(
                        HasCertificate: true,
                        IsUsable: false,
                        RequiresRenewal: true,
                        Reason: "unusable but loaded",
                        Certificate: evaluatedBundle)),
                    bundle => observedEvaluatedBundle = bundle);

                await Assert.ThrowsAsync<InvalidOperationException>(() => service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None));

                Assert.NotNull(observedEvaluatedBundle);
                Assert.False(state.HasCertificate);
                AssertCertificateDisposed(observedEvaluatedBundle!.Certificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        /// <summary>
        /// Creates a provisioning service configured for deterministic ownership tests while exercising real production disposal behavior.
        /// </summary>
        /// <param name="state">Runtime certificate state that receives ownership when publication occurs.</param>
        /// <param name="acmeIssuer">Issuer controlling provisioning outcome for the scenario.</param>
        /// <param name="evaluateExistingCertificateAsync">Optional evaluation delegate for scenarios that cannot be produced by persisted-certificate inputs.</param>
        /// <param name="evaluatedBundleObserver">Optional observer receiving the evaluated bundle before ownership resolution.</param>
        /// <returns>A provisioning service instance wired for ownership assertions.</returns>
        private static BackFillerCertificateProvisioningService CreateServiceForOwnershipTests(
            BackFillerCertificateState state,
            IAcmeCertificateIssuer acmeIssuer,
            Func<BackFillerLetsEncryptRuntimeOptions, TimeProvider, CancellationToken, Task<CertificateEvaluationResult>>? evaluateExistingCertificateAsync,
            Action<BackFillerCertificateBundle>? evaluatedBundleObserver)
        {
            BackFillerCertificateStore store = new();
            return new BackFillerCertificateProvisioningService(
                store,
                acmeIssuer,
                state,
                NullLogger<BackFillerCertificateProvisioningService>.Instance,
                TimeProvider.System,
                evaluateExistingCertificateAsync,
                evaluatedBundleObserver);
        }

        /// <summary>
        /// Creates one in-memory listener certificate bundle for deterministic ownership/disposal assertions.
        /// </summary>
        /// <param name="fqdn">FQDN written into CN/SAN.</param>
        /// <returns>A certificate bundle owned by the caller.</returns>
        private static BackFillerCertificateBundle CreateInMemoryBundle(string fqdn)
        {
            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new(
                $"CN={fqdn}",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);

            SubjectAlternativeNameBuilder sanBuilder = new();
            sanBuilder.AddDnsName(fqdn);
            request.CertificateExtensions.Add(sanBuilder.Build());
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, true));

            using X509Certificate2 cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
            const string bundlePassword = "BackFiller-ProvisioningServiceTests-Bundle";
            byte[] pfx = cert.Export(X509ContentType.Pkcs12, bundlePassword);
            X509Certificate2 owned = new(
                pfx,
                bundlePassword,
                X509KeyStorageFlags.EphemeralKeySet | X509KeyStorageFlags.Exportable);

            return new BackFillerCertificateBundle(owned, "memory", DateTimeOffset.UtcNow);
        }

        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create runtime options helper.</returns>
        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <param name="letsEncrypt">The lets encrypt used by this test scenario.</param>
        /// <returns>The value returned by the create runtime options helper.</returns>
        private static BackFillerRuntimeOptions CreateRuntimeOptions(BackFillerLetsEncryptRuntimeOptions letsEncrypt)
        {
            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: letsEncrypt.CanonicalCertificateSubjectName,
                BackFillerId: 1,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetDirectoryName(letsEncrypt.CertificatePfxPath)!,
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
        /// Confirms the create lets encrypt options behavior.
        /// </summary>
        /// <returns>The value returned by the create lets encrypt options helper.</returns>
        /// <summary>
        /// Confirms the create lets encrypt options behavior.
        /// </summary>
        /// <param name="tempDir">The temp dir used by this test scenario.</param>
        /// <param name="fqdn">The fqdn used by this test scenario.</param>
        /// <param name="renewBeforeExpiryDays">The renew before expiry days used by this test scenario.</param>
        /// <returns>The value returned by the create lets encrypt options helper.</returns>
        private static BackFillerLetsEncryptRuntimeOptions CreateLetsEncryptOptions(string tempDir, string fqdn, int renewBeforeExpiryDays = 7)
        {
            _ = Directory.CreateDirectory(tempDir);
            return new BackFillerLetsEncryptRuntimeOptions(
                CanonicalCertificateSubjectName: fqdn,
                AcmeAccountEmail: "security@example.com",
                AcmeAccountKeyPemPath: Path.Combine(tempDir, "account.key"),
                CertificatePfxPath: Path.Combine(tempDir, "backfiller-listener.pfx"),
                CertificatePrivateKeyPemPath: Path.Combine(tempDir, "certificate.key"),
                PfxExportPassword: "UnitTest-PfxPassword-123!",
                RenewBeforeExpiryDays: renewBeforeExpiryDays,
                RenewalCheckIntervalHours: 6,
                RenewalJitterRatio: 0.1,
                UseStagingDirectory: true,
                AcmeTransientRetryMaxAttempts: 5,
                DnsPropagationDelaySeconds: 0,
                DnsTxtPollIntervalSeconds: 1,
                DnsTxtPollTimeoutSeconds: 10,
                DnsAuthoritativeNsCacheMinutes: 1,
                DnsAuthoritativeQuorumRatio: 0.7,
                CloudFlareApiToken: "token",
                CloudFlareZoneId: "zone");
        }

        /// <summary>
        /// Confirms the write valid pfx behavior.
        /// </summary>
        private static void WriteValidPfx(string pfxPath, string password, string fqdn, DateTimeOffset notBeforeUtc, DateTimeOffset notAfterUtc)
        {
            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new(
                $"CN={fqdn}",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);

            SubjectAlternativeNameBuilder sanBuilder = new();
            sanBuilder.AddDnsName(fqdn);
            request.CertificateExtensions.Add(sanBuilder.Build());
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, true));

            using X509Certificate2 certificate = request.CreateSelfSigned(notBeforeUtc, notAfterUtc);
            File.WriteAllBytes(pfxPath, certificate.Export(X509ContentType.Pkcs12, password));
        }

        /// <summary>
        /// Throws when issuance is unexpectedly invoked in reuse scenarios.
        /// </summary>
        private sealed class UnexpectedIssueAcmeCertificateIssuer : IAcmeCertificateIssuer
        {
            public Task<AcmeOrderIssueResult> IssueCertificateAsync(BackFillerLetsEncryptRuntimeOptions letsEncryptOptions, CancellationToken cancellationToken)
            {
                throw new InvalidOperationException("Issuance should not be invoked for this scenario.");
            }
        }

        /// <summary>
        /// Returns a deterministic successful issuance result for replacement-success scenarios.
        /// </summary>
        /// <param name="fqdn">FQDN encoded into the issued certificate artifacts.</param>
        private sealed class FakeAcmeCertificateIssuer(string fqdn) : IAcmeCertificateIssuer
        {
            private readonly string _fqdn = fqdn;

            internal int IssueCallCount { get; private set; }

            public Task<AcmeOrderIssueResult> IssueCertificateAsync(BackFillerLetsEncryptRuntimeOptions letsEncryptOptions, CancellationToken cancellationToken)
            {
                IssueCallCount++;

                using RSA rsa = RSA.Create(2048);
                CertificateRequest request = new(
                    $"CN={_fqdn}",
                    rsa,
                    HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1);
                SubjectAlternativeNameBuilder san = new();
                san.AddDnsName(_fqdn);
                request.CertificateExtensions.Add(san.Build());

                using X509Certificate2 certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
                AcmeOrderIssueResult result = new(
                    LeafCertificateDer: certificate.Export(X509ContentType.Cert),
                    ChainDer: [],
                    CertificatePrivateKeyPem: rsa.ExportPkcs8PrivateKeyPem());

                return Task.FromResult(result);
            }
        }

        /// <summary>
        /// Throws a deterministic cancellation when issuance is requested.
        /// </summary>
        private sealed class CanceledIssueAcmeCertificateIssuer : IAcmeCertificateIssuer
        {
            public Task<AcmeOrderIssueResult> IssueCertificateAsync(BackFillerLetsEncryptRuntimeOptions letsEncryptOptions, CancellationToken cancellationToken)
            {
                return Task.FromCanceled<AcmeOrderIssueResult>(new CancellationToken(canceled: true));
            }
        }

        /// <summary>
        /// Throws a configured deterministic exception when issuance is requested.
        /// </summary>
        /// <param name="exception">Exception instance to throw from issuance.</param>
        private sealed class ThrowingIssueAcmeCertificateIssuer(Exception exception) : IAcmeCertificateIssuer
        {
            private readonly Exception _exception = exception;

            public Task<AcmeOrderIssueResult> IssueCertificateAsync(BackFillerLetsEncryptRuntimeOptions letsEncryptOptions, CancellationToken cancellationToken)
            {
                return Task.FromException<AcmeOrderIssueResult>(_exception);
            }
        }

        /// <summary>
        /// Asserts that a certificate instance remains usable and has not been disposed.
        /// </summary>
        /// <param name="certificate">Certificate to validate.</param>
        private static void AssertCertificateUsable(X509Certificate2 certificate)
        {
            _ = certificate.Export(X509ContentType.Cert);
        }

        /// <summary>
        /// Asserts that a certificate instance has been disposed.
        /// </summary>
        /// <param name="certificate">Certificate expected to be disposed.</param>
        private static void AssertCertificateDisposed(X509Certificate2 certificate)
        {
            _ = Assert.ThrowsAny<CryptographicException>(() => certificate.Export(X509ContentType.Cert));
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
            string path = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-ProvisionTests-{Guid.NewGuid():N}");
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
