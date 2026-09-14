// <copyright file="BackFillerCertificateStateTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for back filler certificate state, covering active bundle ownership and runtime clone behavior.
// Primary responsibility: documents executable ownership/disposal contracts for certificate state publication and replacement.

using System.Formats.Asn1;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Certificates
{
    /// <summary>
    /// Confirms certificate state publication and runtime clone ownership behavior.
    /// </summary>
    public sealed class BackFillerCertificateStateTests
    {
        [Fact]
        public void GetCurrentRuntimeCertificateMaterialClone_WhenBundleContainsIntermediate_PreservesIntermediateRawIdentity()
        {
            using GeneratedCertificateChain chain = CreateGeneratedCertificateChain("bf-state-chain.example.com");
            using BackFillerCertificateBundle publishedBundle = CreateBundleFromChain(chain);
            using BackFillerCertificateState state = new();

            byte[] expectedIntermediateRaw = chain.IntermediateCertificate.RawData;

            state.Publish(publishedBundle.CloneOwned());

            using BackFillerCertificateState.RuntimeCertificateMaterial? runtimeMaterial = state.GetCurrentRuntimeCertificateMaterialClone();
            Assert.NotNull(runtimeMaterial);
            Assert.NotNull(runtimeMaterial!.CertificateContext);
            Assert.True(runtimeMaterial.Bundle.Certificate.HasPrivateKey);
            Assert.Contains(runtimeMaterial.Bundle.IntermediateCertificates, cert => cert.RawData.AsSpan().SequenceEqual(expectedIntermediateRaw));
        }

        [Fact]
        public void Publish_WhenReplacingBundle_DisposesPreviousBundleButKeepsExistingRuntimeCloneIndependent()
        {
            using GeneratedCertificateChain chainA = CreateGeneratedCertificateChain("bf-state-a.example.com");
            using GeneratedCertificateChain chainB = CreateGeneratedCertificateChain("bf-state-b.example.com");
            using BackFillerCertificateState state = new();

            using BackFillerCertificateBundle bundleA = CreateBundleFromChain(chainA);
            using BackFillerCertificateBundle bundleB = CreateBundleFromChain(chainB);

            byte[] expectedIntermediateA = chainA.IntermediateCertificate.RawData;

            state.Publish(bundleA);
            using BackFillerCertificateState.RuntimeCertificateMaterial? runtimeMaterialA = state.GetCurrentRuntimeCertificateMaterialClone();
            Assert.NotNull(runtimeMaterialA);

            state.Publish(bundleB);

            _ = Assert.ThrowsAny<CryptographicException>(() => bundleA.Certificate.Export(X509ContentType.Cert));
            _ = Assert.ThrowsAny<CryptographicException>(() => bundleA.IntermediateCertificates[0].Export(X509ContentType.Cert));

            Assert.NotNull(runtimeMaterialA!.CertificateContext);
            Assert.Contains(runtimeMaterialA.Bundle.IntermediateCertificates, cert => cert.RawData.AsSpan().SequenceEqual(expectedIntermediateA));
            Assert.True(runtimeMaterialA.Bundle.Certificate.HasPrivateKey);
            _ = runtimeMaterialA.Bundle.Certificate.GetRSAPrivateKey() ?? throw new InvalidOperationException("Expected cloned runtime material to retain private key.");
        }

        [Fact]
        public void RuntimeCertificateMaterial_WhenContextCreationFails_DisposesOwnedBundleAndRethrows()
        {
            using GeneratedCertificateChain chain = CreateGeneratedCertificateChain("bf-state-failure.example.com");
            BackFillerCertificateBundle bundle = CreateBundleFromChain(chain);
            bundle.Certificate.Dispose();

            _ = Assert.ThrowsAny<Exception>(() => new BackFillerCertificateState.RuntimeCertificateMaterial(bundle));
            _ = Assert.ThrowsAny<CryptographicException>(() => bundle.Certificate.Export(X509ContentType.Cert));
            _ = Assert.ThrowsAny<CryptographicException>(() => bundle.IntermediateCertificates[0].Export(X509ContentType.Cert));
        }

        [Fact]
        public void GetCurrentRuntimeCertificateMaterialClone_WhenMultipleClonesExist_DisposingOneDoesNotAffectAnother()
        {
            using GeneratedCertificateChain chainA = CreateGeneratedCertificateChain("bf-state-multi-clone.example.com");
            using GeneratedCertificateChain chainB = CreateGeneratedCertificateChain("bf-state-multi-clone-replacement.example.com");
            using BackFillerCertificateState state = new();
            using BackFillerCertificateBundle bundleA = CreateBundleFromChain(chainA);
            using BackFillerCertificateBundle bundleB = CreateBundleFromChain(chainB);

            byte[] expectedIntermediateA = chainA.IntermediateCertificate.RawData;
            state.Publish(bundleA);

            using BackFillerCertificateState.RuntimeCertificateMaterial? runtimeMaterialA1 = state.GetCurrentRuntimeCertificateMaterialClone();
            using BackFillerCertificateState.RuntimeCertificateMaterial? runtimeMaterialA2 = state.GetCurrentRuntimeCertificateMaterialClone();
            Assert.NotNull(runtimeMaterialA1);
            Assert.NotNull(runtimeMaterialA2);

            runtimeMaterialA1!.Dispose();
            Assert.NotNull(runtimeMaterialA2!.CertificateContext);
            Assert.True(runtimeMaterialA2.Bundle.Certificate.HasPrivateKey);
            Assert.Contains(runtimeMaterialA2.Bundle.IntermediateCertificates, cert => cert.RawData.AsSpan().SequenceEqual(expectedIntermediateA));
            _ = runtimeMaterialA2.Bundle.Certificate.GetRSAPrivateKey() ?? throw new InvalidOperationException("Expected surviving runtime clone to retain private key.");

            state.Publish(bundleB);

            Assert.NotNull(runtimeMaterialA2.CertificateContext);
            Assert.True(runtimeMaterialA2.Bundle.Certificate.HasPrivateKey);
            Assert.Contains(runtimeMaterialA2.Bundle.IntermediateCertificates, cert => cert.RawData.AsSpan().SequenceEqual(expectedIntermediateA));
            _ = runtimeMaterialA2.Bundle.Certificate.GetRSAPrivateKey() ?? throw new InvalidOperationException("Expected runtime clone to remain independent after publication replacement.");
        }

        [Fact]
        public void CertificateContextCreate_WhenUsingGeneratedFixture_OriginalAndClonedMaterialBehaveConsistently()
        {
            using GeneratedCertificateChain chain = CreateGeneratedCertificateChain("bf-state-context-repro.example.com");
            using BackFillerCertificateBundle originalBundle = CreateBundleFromChain(chain);
            using BackFillerCertificateBundle clonedBundle = originalBundle.CloneOwned();

            bool originalSucceeded = TryCreateOfflineContext(originalBundle, out _);
            bool clonedSucceeded = TryCreateOfflineContext(clonedBundle, out _);

            Assert.Equal(originalSucceeded, clonedSucceeded);
        }

        [Fact]
        public void CertificateContextCreate_WhenUsingGeneratedFixture_DirectAndBundleMaterialBehaveConsistently()
        {
            using GeneratedCertificateChain chain = CreateGeneratedCertificateChain("bf-state-context-direct.example.com");
            using X509Certificate2 directIntermediate = new(chain.IntermediateCertificate.RawData);
            using BackFillerCertificateBundle bundleWithoutRoot = CreateBundleFromChain(chain);

            X509Certificate2Collection directExtraStore = [directIntermediate];
            bool directSucceeded = TryCreateOfflineContext(chain.LeafCertificate, directExtraStore, out _);
            bool bundledSucceeded = TryCreateOfflineContext(bundleWithoutRoot, out _);

            Assert.Equal(directSucceeded, bundledSucceeded);
        }

        [Fact]
        public void CloneOwned_WhenSourceDisposed_CloneCertificatesRemainUsableAndIndependent()
        {
            using GeneratedCertificateChain chain = CreateGeneratedCertificateChain("bf-state-clone-independent.example.com");
            BackFillerCertificateBundle sourceBundle = CreateBundleFromChain(chain);
            using BackFillerCertificateBundle clonedBundle = sourceBundle.CloneOwned();

            sourceBundle.Dispose();

            _ = clonedBundle.Certificate.Export(X509ContentType.Cert);
            Assert.Single(clonedBundle.IntermediateCertificates);
            _ = clonedBundle.IntermediateCertificates[0].Export(X509ContentType.Cert);
            _ = clonedBundle.Certificate.GetRSAPrivateKey() ?? throw new InvalidOperationException("Expected clone to retain usable private key after source disposal.");
        }

        [Fact]
        public void CertificateContextCreate_WhenUsingGeneratedFixture_AddingRootDoesNotChangeOutcome()
        {
            using GeneratedCertificateChain chain = CreateGeneratedCertificateChain("bf-state-context-root.example.com");

            using BackFillerCertificateBundle bundleWithoutRoot = CreateBundleFromChain(chain);
            using BackFillerCertificateBundle bundleWithRoot = CreateBundleFromChainIncludingRoot(chain);

            bool withoutRootSucceeded = TryCreateOfflineContext(bundleWithoutRoot, out _);
            bool withRootSucceeded = TryCreateOfflineContext(bundleWithRoot, out _);

            Assert.Equal(withoutRootSucceeded, withRootSucceeded);
        }

        [Fact]
        public void CertificateContextCreate_WhenUsingSelfSignedLeaf_Succeeds()
        {
            using X509Certificate2 selfSigned = CreateSelfSignedServerCertificate("bf-state-context-selfsigned.example.com");
            X509Certificate2Collection noIntermediates = [];

            bool selfSignedSucceeded = TryCreateOfflineContext(selfSigned, noIntermediates, out _);

            Assert.True(selfSignedSucceeded);
        }

        [Fact]
        public void CreateGeneratedCertificateChain_WhenAuthorityKeyIdentifierEncoded_UsesImplicitTagAndMatchesIssuerSubjectKeyIdentifier()
        {
            using GeneratedCertificateChain chain = CreateGeneratedCertificateChain("bf-state-aki.example.com");

            AssertAuthorityKeyIdentifierMatchesIssuerSubjectKeyIdentifier(chain.IntermediateCertificate, chain.RootCertificate);
            AssertAuthorityKeyIdentifierMatchesIssuerSubjectKeyIdentifier(chain.LeafCertificate, chain.IntermediateCertificate);
        }

        private static BackFillerCertificateBundle CreateBundleFromChain(GeneratedCertificateChain chain)
        {
            ArgumentNullException.ThrowIfNull(chain);

            X509Certificate2 ownedLeaf = CloneLeafWithPrivateKey(chain.LeafCertificate);
            List<X509Certificate2> intermediates = [new X509Certificate2(chain.IntermediateCertificate.RawData)];

            return new BackFillerCertificateBundle(ownedLeaf, intermediates, "memory", DateTimeOffset.UtcNow);
        }

        private static BackFillerCertificateBundle CreateBundleFromChainIncludingRoot(GeneratedCertificateChain chain)
        {
            ArgumentNullException.ThrowIfNull(chain);

            X509Certificate2 ownedLeaf = CloneLeafWithPrivateKey(chain.LeafCertificate);
            List<X509Certificate2> intermediates =
            [
                new X509Certificate2(chain.IntermediateCertificate.RawData),
                new X509Certificate2(chain.RootCertificate.RawData),
            ];

            return new BackFillerCertificateBundle(ownedLeaf, intermediates, "memory", DateTimeOffset.UtcNow);
        }

        private static bool TryCreateOfflineContext(BackFillerCertificateBundle bundle, out SslStreamCertificateContext? certificateContext)
        {
            ArgumentNullException.ThrowIfNull(bundle);

            X509Certificate2Collection intermediateCollection = [];
            for (int index = 0; index < bundle.IntermediateCertificates.Count; index++)
            {
                _ = intermediateCollection.Add(bundle.IntermediateCertificates[index]);
            }

            return TryCreateOfflineContext(bundle.Certificate, intermediateCollection, out certificateContext);
        }

        private static bool TryCreateOfflineContext(
            X509Certificate2 certificate,
            X509Certificate2Collection extraStore,
            out SslStreamCertificateContext? certificateContext)
        {
            ArgumentNullException.ThrowIfNull(certificate);
            ArgumentNullException.ThrowIfNull(extraStore);

            try
            {
                certificateContext = SslStreamCertificateContext.Create(certificate, extraStore, offline: true);
                return true;
            }
            catch (CryptographicException)
            {
                certificateContext = null;
                return false;
            }
        }

        private static X509Certificate2 CloneLeafWithPrivateKey(X509Certificate2 leaf)
        {
            ArgumentNullException.ThrowIfNull(leaf);

            string clonePassword = Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));
            byte[] pfx = leaf.Export(X509ContentType.Pkcs12, clonePassword);
            return new X509Certificate2(
                pfx,
                clonePassword,
                X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
        }

        private static X509Certificate2 CreateSelfSignedServerCertificate(string fqdn)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);

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

            DateTimeOffset now = DateTimeOffset.UtcNow;
            using X509Certificate2 selfSigned = request.CreateSelfSigned(now.AddDays(-1), now.AddDays(30));
            return CloneLeafWithPrivateKey(selfSigned);
        }

        private static X509Extension CreateAuthorityKeyIdentifierExtension(X509SubjectKeyIdentifierExtension issuerSubjectKeyIdentifier)
        {
            ArgumentNullException.ThrowIfNull(issuerSubjectKeyIdentifier);

            AsnWriter extensionWriter = new(AsnEncodingRules.DER);
            extensionWriter.PushSequence();
            extensionWriter.WriteOctetString(ParseSubjectKeyIdentifierHex(issuerSubjectKeyIdentifier.SubjectKeyIdentifier), new Asn1Tag(TagClass.ContextSpecific, 0));
            extensionWriter.PopSequence();
            return new X509Extension("2.5.29.35", extensionWriter.Encode(), critical: false);
        }

        private static byte[] ParseSubjectKeyIdentifierHex(string? subjectKeyIdentifier)
        {
            if (string.IsNullOrWhiteSpace(subjectKeyIdentifier))
            {
                throw new InvalidOperationException("Issuer Subject Key Identifier must be available for AKI extension generation.");
            }

            return Convert.FromHexString(subjectKeyIdentifier);
        }

        private static void AssertAuthorityKeyIdentifierMatchesIssuerSubjectKeyIdentifier(X509Certificate2 childCertificate, X509Certificate2 issuerCertificate)
        {
            ArgumentNullException.ThrowIfNull(childCertificate);
            ArgumentNullException.ThrowIfNull(issuerCertificate);

            X509SubjectKeyIdentifierExtension issuerSubjectKeyIdentifierExtension = issuerCertificate.Extensions
                .OfType<X509SubjectKeyIdentifierExtension>()
                .FirstOrDefault()
                ?? throw new Xunit.Sdk.XunitException("Issuer certificate does not contain Subject Key Identifier extension.");

            byte[] expectedKeyIdentifier = ParseSubjectKeyIdentifierHex(issuerSubjectKeyIdentifierExtension.SubjectKeyIdentifier);
            X509Extension authorityKeyIdentifierExtension = childCertificate.Extensions["2.5.29.35"]
                ?? throw new Xunit.Sdk.XunitException("Child certificate does not contain Authority Key Identifier extension.");

            AsnReader extensionReader = new(authorityKeyIdentifierExtension.RawData, AsnEncodingRules.DER);
            AsnReader sequenceReader = extensionReader.ReadSequence();
            byte[] actualKeyIdentifier = sequenceReader.ReadOctetString(new Asn1Tag(TagClass.ContextSpecific, 0));

            Assert.False(sequenceReader.HasData);
            Assert.False(extensionReader.HasData);
            Assert.Equal(expectedKeyIdentifier, actualKeyIdentifier);
        }

        private static GeneratedCertificateChain CreateGeneratedCertificateChain(string fqdn)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);

            RSA rootKey = RSA.Create(2048);
            CertificateRequest rootRequest = new(
                $"CN=BackFiller Test Root CA {fqdn}",
                rootKey,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            rootRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            X509SubjectKeyIdentifierExtension rootSki = new(rootRequest.PublicKey, false);
            rootRequest.CertificateExtensions.Add(rootSki);

            DateTimeOffset now = DateTimeOffset.UtcNow;
            X509Certificate2 rootCertificate = rootRequest.CreateSelfSigned(now.AddDays(-2), now.AddDays(90));

            RSA intermediateKey = RSA.Create(2048);
            CertificateRequest intermediateRequest = new(
                $"CN=BackFiller Test Intermediate CA {fqdn}",
                intermediateKey,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            intermediateRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            intermediateRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            X509SubjectKeyIdentifierExtension intermediateSki = new(intermediateRequest.PublicKey, false);
            intermediateRequest.CertificateExtensions.Add(intermediateSki);
            intermediateRequest.CertificateExtensions.Add(CreateAuthorityKeyIdentifierExtension(rootSki));

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
            sanBuilder.AddDnsName("localhost");
            sanBuilder.AddDnsName(fqdn);
            sanBuilder.AddIpAddress(System.Net.IPAddress.Loopback);
            leafRequest.CertificateExtensions.Add(sanBuilder.Build());
            leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            leafRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
            leafRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, true));
            leafRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(leafRequest.PublicKey, false));
            leafRequest.CertificateExtensions.Add(CreateAuthorityKeyIdentifierExtension(intermediateSki));

            byte[] leafSerial = RandomNumberGenerator.GetBytes(16);
            using X509Certificate2 leafSignedNoKey = leafRequest.Create(intermediateCertificate, now.AddDays(-1), now.AddDays(30), leafSerial);
            X509Certificate2 leafCertificate = leafSignedNoKey.CopyWithPrivateKey(leafKey);

            return new GeneratedCertificateChain(rootCertificate, rootKey, intermediateCertificate, intermediateKey, leafCertificate);
        }

        private sealed class GeneratedCertificateChain(
            X509Certificate2 rootCertificate,
            RSA rootKey,
            X509Certificate2 intermediateCertificate,
            RSA intermediateKey,
            X509Certificate2 leafCertificate) : IDisposable
        {
            public X509Certificate2 RootCertificate { get; } = rootCertificate;

            public RSA RootKey { get; } = rootKey;

            public X509Certificate2 IntermediateCertificate { get; } = intermediateCertificate;

            public RSA IntermediateKey { get; } = intermediateKey;

            public X509Certificate2 LeafCertificate { get; } = leafCertificate;

            public void Dispose()
            {
                LeafCertificate.Dispose();
                IntermediateCertificate.Dispose();
                RootCertificate.Dispose();
                IntermediateKey.Dispose();
                RootKey.Dispose();
            }
        }
    }
}
