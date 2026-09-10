// <copyright file="TestAcmeAccountKeyFileMaterializerConcurrencyTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Deterministic concurrency tests for shared ACME account-key fixture materialization.

using System.Security.Cryptography;
using VectorNNTP.BackFiller.Tests.Startup.Validation;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.TestInfrastructure.Certificates
{
    /// <summary>
    /// Verifies deterministic concurrent behavior for ACME account key fixture materialization.
    /// </summary>
    public sealed class TestAcmeAccountKeyFileMaterializerConcurrencyTests
    {
        /// <summary>
        /// Verifies concurrent callers targeting the same fixture directory converge on canonical account-key content and leave no temp artifacts.
        /// </summary>
        [Fact]
        public async Task EnsureRelativeAcmeAccountKeyPemFile_WhenCalledConcurrentlyForSameDirectory_CompletesWithCanonicalKeyWithoutTempArtifacts()
        {
            string certDirectory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(certDirectory);
            string keyFilePath = Path.Combine(certDirectory, "account.key");

            string stalePem;
            using (RSA staleRsa = RSA.Create(2048))
            {
                stalePem = staleRsa.ExportPkcs8PrivateKeyPem();
            }

            File.WriteAllText(keyFilePath, stalePem);

            const int callerCount = 8;
            using Barrier startBarrier = new(participantCount: callerCount + 1);

            Task<string>[] calls = Enumerable.Range(0, callerCount)
                .Select(_ => Task.Run(() =>
                {
                    startBarrier.SignalAndWait();
                    return TestAcmeAccountKeyFileMaterializer.EnsureRelativeAcmeAccountKeyPemFile(certDirectory);
                }))
                .ToArray();

            startBarrier.SignalAndWait();

            string[] returned = await Task.WhenAll(calls).ConfigureAwait(false);

            Assert.All(returned, static value => Assert.Equal("account.key", value));
            Assert.Equal(TestAcmeAccountKeyFixture.Pem, File.ReadAllText(keyFilePath));

            Assert.True(TryReadValidPrivateKeyPem(keyFilePath, out string pem));
            Assert.False(string.IsNullOrWhiteSpace(pem));

            using RSA rsa = RSA.Create();
            rsa.ImportFromPem(pem);
            byte[] actualPrivateKey = rsa.ExportPkcs8PrivateKey();
            byte[] canonicalPrivateKey = TestAcmeAccountKeyFixture.Pkcs8Bytes;
            Assert.True(actualPrivateKey.AsSpan().SequenceEqual(canonicalPrivateKey));

            string[] tempArtifacts = Directory.GetFiles(certDirectory, "account.key.*.tmp", SearchOption.TopDirectoryOnly);
            Assert.Empty(tempArtifacts);

            Directory.Delete(certDirectory, recursive: true);
        }

        /// <summary>
        /// Verifies path-scoped synchronization by allowing independent fixture directories to materialize concurrently.
        /// </summary>
        [Fact]
        public async Task EnsureRelativeAcmeAccountKeyPemFile_WhenCalledConcurrentlyForDifferentDirectories_MaterializesEachDirectoryIndependently()
        {
            string root = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            string certDirectoryA = Path.Combine(root, "A");
            string certDirectoryB = Path.Combine(root, "B");
            _ = Directory.CreateDirectory(certDirectoryA);
            _ = Directory.CreateDirectory(certDirectoryB);

            using Barrier startBarrier = new(participantCount: 3);
            Task<string> taskA = Task.Run(() =>
            {
                startBarrier.SignalAndWait();
                return TestAcmeAccountKeyFileMaterializer.EnsureRelativeAcmeAccountKeyPemFile(certDirectoryA);
            });

            Task<string> taskB = Task.Run(() =>
            {
                startBarrier.SignalAndWait();
                return TestAcmeAccountKeyFileMaterializer.EnsureRelativeAcmeAccountKeyPemFile(certDirectoryB);
            });

            startBarrier.SignalAndWait();

            await Task.WhenAll(taskA, taskB).ConfigureAwait(false);

            string keyFilePathA = Path.Combine(certDirectoryA, "account.key");
            string keyFilePathB = Path.Combine(certDirectoryB, "account.key");

            Assert.Equal("account.key", taskA.Result);
            Assert.Equal("account.key", taskB.Result);
            Assert.Equal(TestAcmeAccountKeyFixture.Pem, File.ReadAllText(keyFilePathA));
            Assert.Equal(TestAcmeAccountKeyFixture.Pem, File.ReadAllText(keyFilePathB));
            Assert.True(TryReadValidPrivateKeyPem(keyFilePathA, out _));
            Assert.True(TryReadValidPrivateKeyPem(keyFilePathB, out _));
            Assert.Empty(Directory.GetFiles(certDirectoryA, "account.key.*.tmp", SearchOption.TopDirectoryOnly));
            Assert.Empty(Directory.GetFiles(certDirectoryB, "account.key.*.tmp", SearchOption.TopDirectoryOnly));

            Directory.Delete(root, recursive: true);
        }

        private static bool TryReadValidPrivateKeyPem(string keyFilePath, out string pem)
        {
            pem = string.Empty;
            if (!File.Exists(keyFilePath))
            {
                return false;
            }

            try
            {
                pem = File.ReadAllText(keyFilePath);
                if (string.IsNullOrWhiteSpace(pem))
                {
                    return false;
                }

                using RSA rsa = RSA.Create();
                rsa.ImportFromPem(pem);
                _ = rsa.ExportPkcs8PrivateKey();
                return true;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or CryptographicException or ArgumentException)
            {
                return false;
            }
        }
    }
}
