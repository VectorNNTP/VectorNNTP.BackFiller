// <copyright file="TestAcmeAccountKeyFileMaterializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Shared test helper that materializes the process-scoped ACME account key fixture file.

using System.Collections.Concurrent;
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
        private static readonly ConcurrentDictionary<string, PathLockState> PathLocks = new(StringComparer.Ordinal);

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
            certDirectory = Path.GetFullPath(certDirectory);
            _ = Directory.CreateDirectory(certDirectory);

            const string fileName = "account.key";
            string keyFilePath = Path.GetFullPath(Path.Combine(certDirectory, fileName));
            string lockKey = GetPathLockKey(keyFilePath);

            PathLockState lockState = PathLocks.GetOrAdd(lockKey, static _ => new PathLockState());
            Interlocked.Increment(ref lockState.ReferenceCount);

            try
            {
                lock (lockState.Gate)
                {
                    string canonicalPem = TestAcmeAccountKeyFixture.Pem;
                    byte[] canonicalPrivateKey = TestAcmeAccountKeyFixture.Pkcs8Bytes;

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
            }
            finally
            {
                if (Interlocked.Decrement(ref lockState.ReferenceCount) == 0)
                {
                    _ = PathLocks.TryRemove(new KeyValuePair<string, PathLockState>(lockKey, lockState));
                }
            }
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

        private static string GetPathLockKey(string keyFilePath)
        {
            return OperatingSystem.IsWindows()
                ? keyFilePath.ToUpperInvariant()
                : keyFilePath;
        }

        private sealed class PathLockState
        {
            internal object Gate { get; } = new();

            internal int ReferenceCount;
        }
    }
}
