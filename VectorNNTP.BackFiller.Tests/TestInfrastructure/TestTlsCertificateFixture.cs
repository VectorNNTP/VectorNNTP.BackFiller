// <copyright file="TestTlsCertificateFixture.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for test tls certificate fixture, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the test tls certificate fixture test suite.

using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace VectorNNTP.Backfiller.Tests.TestInfrastructure
{
    /// <summary>
    /// Provides one deterministic in-memory loopback TLS certificate and strict thumbprint-based
    /// validation callback for unattended test-only TLS handshakes.
    /// </summary>
    public sealed class TestTlsCertificateFixture : IDisposable
    {
        /// <summary>
        /// Trusted test server certificate with private key for server-side TLS authentication.
        /// </summary>
        private X509Certificate2 ServerCertificateInstance { get; }

        /// <summary>
        /// Initializes a new shared TLS test-certificate fixture.
        /// </summary>
        public TestTlsCertificateFixture()
        {
            string pfxPassword = Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));

            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new("CN=127.0.0.1", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: false));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], critical: false));
            request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));

            SubjectAlternativeNameBuilder san = new();
            san.AddDnsName("localhost");
            san.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(san.Build());

            DateTimeOffset now = DateTimeOffset.UtcNow;
            using X509Certificate2 ephemeralCertificate = request.CreateSelfSigned(now.AddDays(-1), now.AddDays(7));
            byte[] pfx = ephemeralCertificate.Export(X509ContentType.Pkcs12, pfxPassword);

            ServerCertificateInstance = new X509Certificate2(
                pfx,
                pfxPassword,
                X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
        }

        /// <summary>
        /// Gets the in-memory server certificate used by fake TLS servers.
        /// </summary>
        internal X509Certificate2 ServerCertificate => ServerCertificateInstance;

        /// <summary>
        /// Gets the strict client-side validation callback that accepts only this fixture's certificate.
        /// </summary>
        internal RemoteCertificateValidationCallback ServerCertificateValidationCallback => ValidateServerCertificate;

        /// <summary>
        /// Validates the remote certificate identity by comparing SHA-256 thumbprints.
        /// </summary>
        /// <param name="sender">TLS sender.</param>
        /// <param name="certificate">Remote certificate.</param>
        /// <param name="chain">Remote certificate chain.</param>
        /// <param name="sslPolicyErrors">Policy errors reported by the platform validator.</param>
        /// <returns><see langword="true"/> only when the remote certificate matches this fixture's certificate.</returns>
        private bool ValidateServerCertificate(object? sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors sslPolicyErrors)
        {
            if (certificate is null)
            {
                return false;
            }

            string? thumbprint = certificate.GetCertHashString(HashAlgorithmName.SHA256);
            string? expectedThumbprint = ServerCertificateInstance.GetCertHashString(HashAlgorithmName.SHA256);

            return string.Equals(thumbprint, expectedThumbprint, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Releases certificate resources.
        /// </summary>
        public void Dispose()
        {
            ServerCertificateInstance.Dispose();
        }
    }
}
