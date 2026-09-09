// <copyright file="TestAcmeAccountKeyFixture.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Shared test fixture for process-wide deterministic ACME account key material.

using System.Security.Cryptography;

namespace VectorNNTP.BackFiller.Tests.TestInfrastructure.Certificates
{
    /// <summary>
    /// Provides one lazily generated ACME account private key reused across the current test process.
    /// </summary>
    internal static class TestAcmeAccountKeyFixture
    {
        private static readonly Lazy<AcmeAccountKeyMaterial> Shared = new(CreateShared, isThreadSafe: true);

        /// <summary>
        /// Gets canonical process-scoped PEM private-key text.
        /// </summary>
        internal static string Pem => Shared.Value.Pem;

        /// <summary>
        /// Gets canonical process-scoped PKCS#8 private-key bytes.
        /// </summary>
        internal static byte[] Pkcs8Bytes => Shared.Value.Pkcs8Bytes.AsSpan().ToArray();

        private static AcmeAccountKeyMaterial CreateShared()
        {
            using RSA rsa = RSA.Create(2048);
            return new AcmeAccountKeyMaterial(rsa.ExportPkcs8PrivateKeyPem(), rsa.ExportPkcs8PrivateKey());
        }

        private sealed record AcmeAccountKeyMaterial(string Pem, byte[] Pkcs8Bytes);
    }
}
