// <copyright file="TestAcmeAccountKeyFileMaterializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Shared test helper that materializes the process-scoped ACME account key fixture file.

using System.Security.Cryptography;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Certificates;

namespace VectorNNTP.BackFiller.Tests.TestInfrastructure.Certificates
{
    /// <summary>
    /// Materializes a stable process-scoped ACME account key fixture into a certificate directory.
    /// </summary>
    internal static class TestAcmeAccountKeyFileMaterializer
    {
        /// <summary>
        /// Ensures the relative ACME account-key fixture file exists and contains the shared process-scoped private key.
        /// </summary>
        /// <param name="fixtureDirectory">Optional target certificate directory. Defaults to <c>AppContext.BaseDirectory/certs</c>.</param>
        /// <returns>The relative ACME account-key fixture file name.</returns>
        internal static string EnsureRelativeAcmeAccountKeyPemFile(string? fixtureDirectory = null)
        {
            string certDirectory = string.IsNullOrWhiteSpace(fixtureDirectory)
                ? Path.Combine(AppContext.BaseDirectory, "certs")
                : fixtureDirectory;
            _ = Directory.CreateDirectory(certDirectory);

            string canonicalPem = TestAcmeAccountKeyFixture.Pem;
            byte[] canonicalPrivateKey = TestAcmeAccountKeyFixture.Pkcs8Bytes;

            const string fileName = "account.key";
            string keyFilePath = Path.Combine(certDirectory, fileName);

            if (TryReadPrivateKeyPkcs8Bytes(keyFilePath, out byte[] existingPrivateKey) && existingPrivateKey.AsSpan().SequenceEqual(canonicalPrivateKey))
            {
                return fileName;
            }

            string tempPath = CertificateFileConventions.BuildAtomicTempPath(keyFilePath);

            try
            {
                byte[] payload = Encoding.UTF8.GetBytes(canonicalPem);
                using (FileStream stream = new(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                {
                    stream.Write(payload, 0, payload.Length);
                    stream.Flush(true);
                }

                File.Move(tempPath, keyFilePath, overwrite: true);
            }
            finally
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }

            if (!TryReadPrivateKeyPkcs8Bytes(keyFilePath, out byte[] finalPrivateKey) || !finalPrivateKey.AsSpan().SequenceEqual(canonicalPrivateKey))
            {
                throw new InvalidOperationException("Failed to create a valid process-scoped ACME account key test fixture.");
            }

            return fileName;
        }

        private static bool TryReadPrivateKeyPkcs8Bytes(string keyFilePath, out byte[] privateKey)
        {
            privateKey = [];
            if (!File.Exists(keyFilePath))
            {
                return false;
            }

            try
            {
                string pem = File.ReadAllText(keyFilePath);
                if (string.IsNullOrWhiteSpace(pem))
                {
                    return false;
                }

                using RSA rsa = RSA.Create();
                rsa.ImportFromPem(pem);
                privateKey = rsa.ExportPkcs8PrivateKey();
                return true;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or CryptographicException or ArgumentException)
            {
                return false;
            }
        }
    }
}
