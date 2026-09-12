// <copyright file="BackFillerCertificateStoreTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for back filler certificate store, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the back filler certificate store test suite.

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Certificates
{
    /// <summary>
    /// Confirms the back filler certificate store tests behavior.
    /// </summary>
    public sealed class BackFillerCertificateStoreTests
    {
        /// <summary>
        /// Confirms the evaluate existing certificate async when certificate missing returns unusable and requires renewal behavior.
        /// </summary>
        [Fact]
        public async Task EvaluateExistingCertificateAsync_WhenCertificateMissing_ReturnsUnusableAndRequiresRenewal()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

                Assert.False(result.HasCertificate);
                Assert.False(result.IsUsable);
                Assert.True(result.RequiresRenewal);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }
        /// <summary>
        /// Confirms the evaluate existing certificate async when valid certificate outside renewal window returns usable without renewal behavior.
        /// </summary>
        [Fact]
        public async Task EvaluateExistingCertificateAsync_WhenValidCertificateOutsideRenewalWindow_ReturnsUsableWithoutRenewal()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                string fqdn = "bf-01.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn, renewBeforeExpiryDays: 7);
                CreateAndWritePfx(options.CertificatePfxPath, options.PfxExportPassword, fqdn, notBeforeUtc: DateTimeOffset.UtcNow.AddDays(-2), notAfterUtc: DateTimeOffset.UtcNow.AddDays(30));

                CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

                Assert.True(result.HasCertificate);
                Assert.True(result.IsUsable);
                Assert.False(result.RequiresRenewal);
                result.Certificate?.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }
        /// <summary>
        /// Confirms the evaluate existing certificate async when certificate inside renewal window returns requires renewal behavior.
        /// </summary>
        [Fact]
        public async Task EvaluateExistingCertificateAsync_WhenCertificateInsideRenewalWindow_ReturnsRequiresRenewal()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                string fqdn = "bf-01.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn, renewBeforeExpiryDays: 10);
                CreateAndWritePfx(options.CertificatePfxPath, options.PfxExportPassword, fqdn, notBeforeUtc: DateTimeOffset.UtcNow.AddDays(-2), notAfterUtc: DateTimeOffset.UtcNow.AddDays(5));

                CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

                Assert.True(result.HasCertificate);
                Assert.True(result.IsUsable);
                Assert.True(result.RequiresRenewal);
                result.Certificate?.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }
        /// <summary>
        /// Confirms the evaluate existing certificate async when certificate fqdn mismatch returns unusable behavior.
        /// </summary>
        [Fact]
        public async Task EvaluateExistingCertificateAsync_WhenCertificateFqdnMismatch_ReturnsUnusable()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
                CreateAndWritePfx(options.CertificatePfxPath, options.PfxExportPassword, "bf-99.example.com", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(20));

                CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

                Assert.True(result.HasCertificate);
                Assert.False(result.IsUsable);
                Assert.True(result.RequiresRenewal);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }
        /// <summary>
        /// Confirms the persist issued certificate async writes loadable pfx and key behavior.
        /// </summary>
        [Fact]
        public async Task PersistIssuedCertificateAsync_WritesLoadablePfxAndKey()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                string fqdn = "bf-01.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);
                using RSA rsa = RSA.Create(2048);
                CertificateRequest request = new(
                    $"CN={fqdn}",
                    rsa,
                    HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1);
                SubjectAlternativeNameBuilder san = new();
                san.AddDnsName(fqdn);
                request.CertificateExtensions.Add(san.Build());

                using X509Certificate2 certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
                AcmeOrderIssueResult issueResult = new(
                    LeafCertificateDer: certificate.Export(X509ContentType.Cert),
                    ChainDer: [],
                    CertificatePrivateKeyPem: rsa.ExportPkcs8PrivateKeyPem());

                await BackFillerCertificateStore.PersistIssuedCertificateAsync(options, issueResult, CancellationToken.None);

                Assert.True(File.Exists(options.CertificatePfxPath));
                Assert.True(File.Exists(options.CertificatePrivateKeyPemPath));

                BackFillerCertificateBundle bundle = await BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None);
                Assert.True(bundle.Certificate.HasPrivateKey);
                bundle.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }
        /// <summary>
        /// Confirms the persist issued certificate async when leaf is ecdsa writes loadable pfx and key behavior.
        /// </summary>
        [Fact]
        public async Task PersistIssuedCertificateAsync_WhenLeafIsEcdsa_WritesLoadablePfxAndKey()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                string fqdn = "bf-01.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);
                using ECDsa ecdsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);
                CertificateRequest request = new(
                    $"CN={fqdn}",
                    ecdsa,
                    HashAlgorithmName.SHA256);
                SubjectAlternativeNameBuilder san = new();
                san.AddDnsName(fqdn);
                request.CertificateExtensions.Add(san.Build());

                using X509Certificate2 certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
                AcmeOrderIssueResult issueResult = new(
                    LeafCertificateDer: certificate.Export(X509ContentType.Cert),
                    ChainDer: [],
                    CertificatePrivateKeyPem: ecdsa.ExportPkcs8PrivateKeyPem());

                await BackFillerCertificateStore.PersistIssuedCertificateAsync(options, issueResult, CancellationToken.None);

                BackFillerCertificateBundle bundle = await BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None);
                Assert.True(bundle.Certificate.HasPrivateKey);
                Assert.Equal("1.2.840.10045.2.1", bundle.Certificate.PublicKey.Oid?.Value);
                bundle.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task LoadCertificateBundleAsync_WhenPersistedPfxContainsIntermediate_PreservesIntermediateRawIdentity()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                string fqdn = "bf-chain.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);
                using GeneratedCertificateChain chain = CreateGeneratedCertificateChain(fqdn);

                AcmeOrderIssueResult issueResult = new(
                    LeafCertificateDer: chain.LeafCertificate.Export(X509ContentType.Cert),
                    ChainDer: [chain.IntermediateCertificate.Export(X509ContentType.Cert)],
                    CertificatePrivateKeyPem: chain.LeafPrivateKeyPem);

                await BackFillerCertificateStore.PersistIssuedCertificateAsync(options, issueResult, CancellationToken.None);

                using BackFillerCertificateBundle bundle = await BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None);

                Assert.True(bundle.Certificate.HasPrivateKey);
                Assert.Single(bundle.IntermediateCertificates);
                Assert.Equal(chain.IntermediateCertificate.RawData, bundle.IntermediateCertificates[0].RawData);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task EvaluateExistingCertificateAsync_WhenIntermediateOnlyExistsInPersistedPfx_IsUsable()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                string fqdn = "bf-extrastore.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);
                using GeneratedCertificateChain chain = CreateGeneratedCertificateChain(fqdn);

                AcmeOrderIssueResult issueResult = new(
                    LeafCertificateDer: chain.LeafCertificate.Export(X509ContentType.Cert),
                    ChainDer:
                    [
                        chain.IntermediateCertificate.Export(X509ContentType.Cert),
                        chain.RootCertificate.Export(X509ContentType.Cert),
                    ],
                    CertificatePrivateKeyPem: chain.LeafPrivateKeyPem);

                await BackFillerCertificateStore.PersistIssuedCertificateAsync(options, issueResult, CancellationToken.None);

                CertificateEvaluationResult evaluation = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

                Assert.True(evaluation.HasCertificate);
                Assert.True(evaluation.IsUsable);
                Assert.NotNull(evaluation.Certificate);
                Assert.Contains(evaluation.Certificate!.IntermediateCertificates, cert => cert.RawData.AsSpan().SequenceEqual(chain.IntermediateCertificate.RawData));
                Assert.Contains(evaluation.Certificate.IntermediateCertificates, cert => cert.RawData.AsSpan().SequenceEqual(chain.RootCertificate.RawData));
                evaluation.Certificate.Dispose();
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task LoadCertificateBundleAsync_WhenPersistedPfxHasNoPrivateKeyCertificate_ThrowsAndEvaluationIsUnusable()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                const string fqdn = "bf-malformed-zero-key.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);

                using X509Certificate2 leafWithKey = CreateEndEntityCertificateWithKey(fqdn);
                using X509Certificate2 leafWithoutKey = new(leafWithKey.Export(X509ContentType.Cert));
                WritePfxCollection(options.CertificatePfxPath, options.PfxExportPassword, leafWithoutKey);

                CryptographicException loadFailure = await Assert.ThrowsAsync<CryptographicException>(
                    () => BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None));
                Assert.Contains("exactly one private-key certificate", loadFailure.Message);

                CertificateEvaluationResult evaluation = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);
                Assert.True(evaluation.HasCertificate);
                Assert.False(evaluation.IsUsable);
                Assert.True(evaluation.RequiresRenewal);
                Assert.Contains("exactly one private-key certificate", evaluation.Reason);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task LoadCertificateBundleAsync_WhenPersistedPfxHasMultiplePrivateKeyCertificates_ThrowsAndEvaluationIsUnusable()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                const string fqdn = "bf-malformed-multi-key.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);

                using X509Certificate2 firstLeaf = CreateEndEntityCertificateWithKey(fqdn);
                using X509Certificate2 secondLeaf = CreateEndEntityCertificateWithKey($"secondary-{fqdn}");
                WritePfxCollection(options.CertificatePfxPath, options.PfxExportPassword, firstLeaf, secondLeaf);

                CryptographicException loadFailure = await Assert.ThrowsAsync<CryptographicException>(
                    () => BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None));
                Assert.Contains("exactly one private-key certificate", loadFailure.Message);

                CertificateEvaluationResult evaluation = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);
                Assert.True(evaluation.HasCertificate);
                Assert.False(evaluation.IsUsable);
                Assert.True(evaluation.RequiresRenewal);
                Assert.Contains("exactly one private-key certificate", evaluation.Reason);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
        }

        [Fact]
        public async Task LoadCertificateBundleAsync_WhenPersistedPfxContainsNonCaExtraCertificate_ThrowsAndEvaluationIsUnusable()
        {
            string tempDir = CreateUniqueTempDirectory();
            try
            {
                const string fqdn = "bf-malformed-nonca.example.com";
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);

                using X509Certificate2 leaf = CreateEndEntityCertificateWithKey(fqdn);
                using X509Certificate2 nonCaExtra = CreateEndEntityCertificateWithoutPrivateKey("bf-nonca-extra.example.com");
                WritePfxCollection(options.CertificatePfxPath, options.PfxExportPassword, leaf, nonCaExtra);

                CryptographicException loadFailure = await Assert.ThrowsAsync<CryptographicException>(
                    () => BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None));
                Assert.Contains("non-CA certificates", loadFailure.Message);

                CertificateEvaluationResult evaluation = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);
                Assert.True(evaluation.HasCertificate);
                Assert.False(evaluation.IsUsable);
                Assert.True(evaluation.RequiresRenewal);
                Assert.Contains("non-CA certificates", evaluation.Reason);
            }
            finally
            {
                DeleteDirectoryIfExists(tempDir);
            }
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
        /// Confirms the create and write pfx behavior.
        /// </summary>
        private static void CreateAndWritePfx(string pfxPath, string password, string fqdn, DateTimeOffset notBeforeUtc, DateTimeOffset notAfterUtc)
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
            byte[] pfx = certificate.Export(X509ContentType.Pkcs12, password);
            File.WriteAllBytes(pfxPath, pfx);
        }

        private static X509Certificate2 CreateEndEntityCertificateWithKey(string fqdn)
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
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, true));
            request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));

            using X509Certificate2 issued = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
            const string Password = "BackFiller-CertificateStoreTests-Leaf";
            byte[] pfx = issued.Export(X509ContentType.Pkcs12, Password);
            return new X509Certificate2(pfx, Password, X509KeyStorageFlags.EphemeralKeySet | X509KeyStorageFlags.Exportable);
        }

        private static X509Certificate2 CreateEndEntityCertificateWithoutPrivateKey(string fqdn)
        {
            using X509Certificate2 withKey = CreateEndEntityCertificateWithKey(fqdn);
            return new X509Certificate2(withKey.Export(X509ContentType.Cert));
        }

        private static void WritePfxCollection(string pfxPath, string password, params X509Certificate2[] certificates)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pfxPath);
            ArgumentNullException.ThrowIfNull(certificates);
            if (certificates.Length == 0)
            {
                throw new ArgumentException("At least one certificate is required.", nameof(certificates));
            }

            X509Certificate2Collection collection = new();
            for (int index = 0; index < certificates.Length; index++)
            {
                _ = collection.Add(certificates[index]);
            }

            byte[] pfx = collection.Export(X509ContentType.Pkcs12, password)
                ?? throw new CryptographicException("Failed to export malformed PFX test fixture.");
            File.WriteAllBytes(pfxPath, pfx);
        }

        private static GeneratedCertificateChain CreateGeneratedCertificateChain(string fqdn)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);

            RSA rootKey = RSA.Create(2048);
            CertificateRequest rootRequest = new(
                "CN=BackFiller Test Root CA",
                rootKey,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            rootRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            rootRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(rootRequest.PublicKey, false));

            DateTimeOffset now = DateTimeOffset.UtcNow;
            X509Certificate2 rootCertificate = rootRequest.CreateSelfSigned(now.AddDays(-2), now.AddDays(90));

            RSA intermediateKey = RSA.Create(2048);
            CertificateRequest intermediateRequest = new(
                "CN=BackFiller Test Intermediate CA",
                intermediateKey,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            intermediateRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            intermediateRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            intermediateRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(intermediateRequest.PublicKey, false));

            byte[] intermediateSerial = RandomNumberGenerator.GetBytes(16);
            using X509Certificate2 intermediateSignedNoKey = intermediateRequest.Create(rootCertificate, now.AddDays(-2), now.AddDays(60), intermediateSerial);
            X509Certificate2 intermediateCertificate = intermediateSignedNoKey.CopyWithPrivateKey(intermediateKey);

            using RSA leafKey = RSA.Create(2048);
            CertificateRequest leafRequest = new(
                $"CN={fqdn}",
                leafKey,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            SubjectAlternativeNameBuilder sanBuilder = new();
            sanBuilder.AddDnsName(fqdn);
            leafRequest.CertificateExtensions.Add(sanBuilder.Build());
            leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            leafRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
            leafRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, true));
            leafRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(leafRequest.PublicKey, false));

            byte[] leafSerial = RandomNumberGenerator.GetBytes(16);
            using X509Certificate2 leafSignedNoKey = leafRequest.Create(intermediateCertificate, now.AddDays(-1), now.AddDays(30), leafSerial);
            X509Certificate2 leafCertificate = leafSignedNoKey.CopyWithPrivateKey(leafKey);

            return new GeneratedCertificateChain(rootCertificate, rootKey, intermediateCertificate, intermediateKey, leafCertificate, leafKey.ExportPkcs8PrivateKeyPem());
        }

        private sealed class GeneratedCertificateChain(
            X509Certificate2 rootCertificate,
            RSA rootKey,
            X509Certificate2 intermediateCertificate,
            RSA intermediateKey,
            X509Certificate2 leafCertificate,
            string leafPrivateKeyPem) : IDisposable
        {
            public X509Certificate2 RootCertificate { get; } = rootCertificate;

            public RSA RootKey { get; } = rootKey;

            public X509Certificate2 IntermediateCertificate { get; } = intermediateCertificate;

            public RSA IntermediateKey { get; } = intermediateKey;

            public X509Certificate2 LeafCertificate { get; } = leafCertificate;

            public string LeafPrivateKeyPem { get; } = leafPrivateKeyPem;

            public void Dispose()
            {
                LeafCertificate.Dispose();
                IntermediateCertificate.Dispose();
                RootCertificate.Dispose();
                IntermediateKey.Dispose();
                RootKey.Dispose();
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
            string path = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-StoreTests-{Guid.NewGuid():N}");
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
