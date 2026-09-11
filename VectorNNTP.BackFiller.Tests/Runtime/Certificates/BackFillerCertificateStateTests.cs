// <copyright file="BackFillerCertificateStateTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for back filler certificate state, covering active bundle ownership and runtime clone behavior.
// Primary responsibility: documents executable ownership/disposal contracts for certificate state publication and replacement.

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
            Assert.True(runtimeMaterial!.Bundle.Certificate.HasPrivateKey);
            Assert.Single(runtimeMaterial.Bundle.IntermediateCertificates);
            Assert.Equal(expectedIntermediateRaw, runtimeMaterial.Bundle.IntermediateCertificates[0].RawData);
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

            Assert.Single(runtimeMaterialA!.Bundle.IntermediateCertificates);
            Assert.Equal(expectedIntermediateA, runtimeMaterialA.Bundle.IntermediateCertificates[0].RawData);
            Assert.True(runtimeMaterialA.Bundle.Certificate.HasPrivateKey);
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

        private static BackFillerCertificateBundle CreateBundleFromChain(GeneratedCertificateChain chain)
        {
            ArgumentNullException.ThrowIfNull(chain);

            X509Certificate2 ownedLeaf = CloneLeafWithPrivateKey(chain.LeafCertificate);
            List<X509Certificate2> intermediates = [new X509Certificate2(chain.IntermediateCertificate.RawData)];

            return new BackFillerCertificateBundle(ownedLeaf, intermediates, "memory", DateTimeOffset.UtcNow);
        }

        private static X509Certificate2 CloneLeafWithPrivateKey(X509Certificate2 leaf)
        {
            ArgumentNullException.ThrowIfNull(leaf);

            const string ClonePassword = "BackFiller-CertificateStateTests-Leaf";
            byte[] pfx = leaf.Export(X509ContentType.Pkcs12, ClonePassword);
            return new X509Certificate2(
                pfx,
                ClonePassword,
                X509KeyStorageFlags.EphemeralKeySet | X509KeyStorageFlags.Exportable);
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

            RSA leafKey = RSA.Create(2048);
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
