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
        public async Task TryRenewIfDueAsync_WhenReplacementSucceeds_DisposesEvaluatedBundleExactlyOnce()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                BackFillerCertificateBundle evaluatedBundle = CreateInMemoryBundle("bf-01.example.com");

                int disposeCount = 0;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    evaluateExistingCertificateAsync: (_, _, _) => Task.FromResult(new CertificateEvaluationResult(
                        HasCertificate: true,
                        IsUsable: true,
                        RequiresRenewal: true,
                        Reason: "inside renewal window",
                        Certificate: evaluatedBundle)),
                    provisionNewCertificateAsync: (_, _) => Task.CompletedTask,
                    disposeUntransferredEvaluatedBundle: bundle =>
                    {
                        disposeCount++;
                        bundle.Certificate.Dispose();
                    });

                bool renewed = await service.TryRenewIfDueAsync(runtime, CancellationToken.None);

                Assert.True(renewed);
                Assert.Equal(1, disposeCount);
                Assert.False(state.HasCertificate);
                state.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenReusingEvaluatedBundle_TransfersOwnershipWithoutEvaluatorDisposal()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                BackFillerCertificateBundle evaluatedBundle = CreateInMemoryBundle("bf-01.example.com");

                int disposeCount = 0;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    evaluateExistingCertificateAsync: (_, _, _) => Task.FromResult(new CertificateEvaluationResult(
                        HasCertificate: true,
                        IsUsable: true,
                        RequiresRenewal: false,
                        Reason: "outside renewal window",
                        Certificate: evaluatedBundle)),
                    provisionNewCertificateAsync: (_, _) => Task.FromException(new InvalidOperationException("provision should not run")),
                    disposeUntransferredEvaluatedBundle: bundle =>
                    {
                        disposeCount++;
                        bundle.Certificate.Dispose();
                    });

                await service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None);

                Assert.Equal(0, disposeCount);
                Assert.True(state.HasCertificate);

                using X509Certificate2? clone = state.GetCurrentCertificateClone();
                Assert.NotNull(clone);

                state.Dispose();
                Assert.False(state.HasCertificate);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task TryRenewIfDueAsync_WhenReplacementCanceledAfterEvaluation_DisposesEvaluatedBundleExactlyOnce()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                BackFillerCertificateBundle evaluatedBundle = CreateInMemoryBundle("bf-01.example.com");

                int disposeCount = 0;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    evaluateExistingCertificateAsync: (_, _, _) => Task.FromResult(new CertificateEvaluationResult(
                        HasCertificate: true,
                        IsUsable: true,
                        RequiresRenewal: true,
                        Reason: "inside renewal window",
                        Certificate: evaluatedBundle)),
                    provisionNewCertificateAsync: static (_, _) => Task.FromCanceled(new CancellationToken(canceled: true)),
                    disposeUntransferredEvaluatedBundle: bundle =>
                    {
                        disposeCount++;
                        bundle.Certificate.Dispose();
                    });

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.TryRenewIfDueAsync(runtime, CancellationToken.None));
                Assert.Equal(1, disposeCount);
                Assert.False(state.HasCertificate);
                state.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenReplacementFailsWithUsableEvaluation_PublishesFallbackWithoutEvaluatorDisposal()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                BackFillerCertificateBundle evaluatedBundle = CreateInMemoryBundle("bf-01.example.com");

                int disposeCount = 0;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    evaluateExistingCertificateAsync: (_, _, _) => Task.FromResult(new CertificateEvaluationResult(
                        HasCertificate: true,
                        IsUsable: true,
                        RequiresRenewal: true,
                        Reason: "inside renewal window",
                        Certificate: evaluatedBundle)),
                    provisionNewCertificateAsync: (_, _) => Task.FromException(new InvalidOperationException("renewal failed")),
                    disposeUntransferredEvaluatedBundle: bundle =>
                    {
                        disposeCount++;
                        bundle.Certificate.Dispose();
                    });

                await service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None);

                Assert.Equal(0, disposeCount);
                Assert.True(state.HasCertificate);

                state.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task EnsureCertificateAvailabilityAsync_WhenProvisionFailsWithoutFallbackTransfer_DisposesEvaluatedBundleExactlyOnceAndThrows()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions letsEncrypt = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                BackFillerRuntimeOptions runtime = CreateRuntimeOptions(letsEncrypt);
                BackFillerCertificateBundle evaluatedBundle = CreateInMemoryBundle("bf-01.example.com");

                int disposeCount = 0;
                BackFillerCertificateState state = new();
                BackFillerCertificateProvisioningService service = CreateServiceForOwnershipTests(
                    state,
                    evaluateExistingCertificateAsync: (_, _, _) => Task.FromResult(new CertificateEvaluationResult(
                        HasCertificate: true,
                        IsUsable: false,
                        RequiresRenewal: true,
                        Reason: "unusable but loaded",
                        Certificate: evaluatedBundle)),
                    provisionNewCertificateAsync: (_, _) => Task.FromException(new InvalidOperationException("provision failed")),
                    disposeUntransferredEvaluatedBundle: bundle =>
                    {
                        disposeCount++;
                        bundle.Certificate.Dispose();
                    });

                await Assert.ThrowsAsync<InvalidOperationException>(() => service.EnsureCertificateAvailabilityAsync(runtime, CancellationToken.None));

                Assert.Equal(1, disposeCount);
                Assert.False(state.HasCertificate);
                state.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        /// <summary>
        /// Creates a provisioning service configured with deterministic evaluation/provision/disposal operations for ownership tests.
        /// </summary>
        /// <param name="state">Runtime certificate state that receives ownership when publication occurs.</param>
        /// <param name="evaluateExistingCertificateAsync">Deterministic evaluation delegate for the scenario.</param>
        /// <param name="provisionNewCertificateAsync">Deterministic provisioning delegate for the scenario.</param>
        /// <param name="disposeUntransferredEvaluatedBundle">Callback invoked when an evaluated bundle is not transferred and is disposed by the evaluator.</param>
        /// <returns>A provisioning service instance wired to deterministic ownership transitions.</returns>
        private static BackFillerCertificateProvisioningService CreateServiceForOwnershipTests(
            BackFillerCertificateState state,
            Func<BackFillerLetsEncryptRuntimeOptions, TimeProvider, CancellationToken, Task<CertificateEvaluationResult>> evaluateExistingCertificateAsync,
            Func<BackFillerLetsEncryptRuntimeOptions, CancellationToken, Task> provisionNewCertificateAsync,
            Action<BackFillerCertificateBundle> disposeUntransferredEvaluatedBundle)
        {
            BackFillerCertificateStore store = new();
            FakeAcmeCertificateIssuer issuer = new("bf-01.example.com");
            return new BackFillerCertificateProvisioningService(
                store,
                issuer,
                state,
                NullLogger<BackFillerCertificateProvisioningService>.Instance,
                TimeProvider.System,
                evaluateExistingCertificateAsync,
                provisionNewCertificateAsync,
                disposeUntransferredEvaluatedBundle);
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
        /// Confirms the fake acme certificate issuer behavior.
        /// </summary>
        /// <returns>The value returned by the fake acme certificate issuer helper.</returns>
        /// <summary>
        /// Confirms the fake acme certificate issuer behavior.
        /// </summary>
        /// <param name="fqdn">The fqdn used by this test scenario.</param>
        /// <returns>The value returned by the fake acme certificate issuer helper.</returns>
        private sealed class FakeAcmeCertificateIssuer(string fqdn) : IAcmeCertificateIssuer
        {
            /// <summary>
            /// Supplies  fqdn for the fixture or scenario under test.
            /// </summary>
            private readonly string _fqdn = fqdn;

            /// <summary>
            /// Supplies issue call count for the fixture or scenario under test.
            /// </summary>
            internal int IssueCallCount { get; private set; }

            /// <summary>
            /// Confirms the issue certificate async behavior.
            /// </summary>
            /// <returns>The value returned by the issue certificate async helper.</returns>
            /// <summary>
            /// Confirms the issue certificate async behavior.
            /// </summary>
            /// <param name="letsEncryptOptions">The lets encrypt options used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the issue certificate async helper.</returns>
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
