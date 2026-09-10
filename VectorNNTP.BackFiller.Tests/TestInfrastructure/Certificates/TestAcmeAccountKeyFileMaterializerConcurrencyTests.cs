// <copyright file="TestAcmeAccountKeyFileMaterializerConcurrencyTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Deterministic concurrency tests for shared ACME account-key fixture materialization.

using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Threading;
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
            string keyFilePath = TestAcmeAccountKeyPathLockCoordinator.GetAccountKeyPath(certDirectory);

            IDisposable? occupiedPathLock = null;
            try
            {
                string stalePem;
                using (RSA staleRsa = RSA.Create(2048))
                {
                    stalePem = staleRsa.ExportPkcs8PrivateKeyPem();
                }

                File.WriteAllText(keyFilePath, stalePem);

                occupiedPathLock = TestAcmeAccountKeyPathLockCoordinator.Acquire(keyFilePath);

                using GateAcquireObserver observer = new();
                using IDisposable observationScope = TestAcmeAccountKeyPathLockCoordinator.BeginObservation(observer);

                Task<string> blockedCaller = Task.Run(() =>
                    TestAcmeAccountKeyFileMaterializer.EnsureRelativeAcmeAccountKeyPemFile(certDirectory));

                observer.WaitForAttempt(keyFilePath);
                Assert.False(observer.HasEntered(keyFilePath));

                Task<bool> contenderAttempt = Task.Run(() =>
                {
                    bool acquired = TestAcmeAccountKeyPathLockCoordinator.TryAcquire(keyFilePath, out IDisposable? contenderLock);
                    contenderLock?.Dispose();
                    return acquired;
                });

                Assert.False(contenderAttempt.GetAwaiter().GetResult());

                occupiedPathLock.Dispose();
                occupiedPathLock = null;

                string blockedCallerResult = await blockedCaller;
                Assert.Equal("account.key", blockedCallerResult);

                string secondCallerResult = TestAcmeAccountKeyFileMaterializer.EnsureRelativeAcmeAccountKeyPemFile(certDirectory);
                Assert.Equal("account.key", secondCallerResult);

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
            }
            finally
            {
                occupiedPathLock?.Dispose();
                if (Directory.Exists(certDirectory))
                {
                    Directory.Delete(certDirectory, recursive: true);
                }
            }
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

            string keyFilePathA = TestAcmeAccountKeyPathLockCoordinator.GetAccountKeyPath(certDirectoryA);
            string keyFilePathB = TestAcmeAccountKeyPathLockCoordinator.GetAccountKeyPath(certDirectoryB);

            Task<string>? taskA = null;
            Task<string>? taskB = null;

            using var releasePathA = new ManualResetEventSlim(false);
            using var observerA = new GateAcquireObserver();
            observerA.BlockOnEnter(keyFilePathA, releasePathA);

            using var observerB = new GateAcquireObserver();

            try
            {
                taskA = Task.Run(() =>
                {
                    using IDisposable observation = TestAcmeAccountKeyPathLockCoordinator.BeginObservation(observerA);
                    return TestAcmeAccountKeyFileMaterializer.EnsureRelativeAcmeAccountKeyPemFile(certDirectoryA);
                });

                observerA.WaitForEnter(keyFilePathA);

                taskB = Task.Run(() =>
                {
                    using IDisposable observation = TestAcmeAccountKeyPathLockCoordinator.BeginObservation(observerB);
                    return TestAcmeAccountKeyFileMaterializer.EnsureRelativeAcmeAccountKeyPemFile(certDirectoryB);
                });

                observerB.WaitForEnter(keyFilePathB);

                string resultB = await taskB;
                Assert.Equal("account.key", resultB);
                Assert.False(taskA.IsCompleted);

                releasePathA.Set();
                string resultA = await taskA;
                Assert.Equal("account.key", resultA);

                Assert.Equal(TestAcmeAccountKeyFixture.Pem, File.ReadAllText(keyFilePathA));
                Assert.Equal(TestAcmeAccountKeyFixture.Pem, File.ReadAllText(keyFilePathB));
                Assert.True(TryReadValidPrivateKeyPem(keyFilePathA, out string pemA));
                Assert.True(TryReadValidPrivateKeyPem(keyFilePathB, out string pemB));

                using RSA rsaA = RSA.Create();
                rsaA.ImportFromPem(pemA);
                Assert.True(rsaA.ExportPkcs8PrivateKey().AsSpan().SequenceEqual(TestAcmeAccountKeyFixture.Pkcs8Bytes));

                using RSA rsaB = RSA.Create();
                rsaB.ImportFromPem(pemB);
                Assert.True(rsaB.ExportPkcs8PrivateKey().AsSpan().SequenceEqual(TestAcmeAccountKeyFixture.Pkcs8Bytes));

                Assert.Empty(Directory.GetFiles(certDirectoryA, "account.key.*.tmp", SearchOption.TopDirectoryOnly));
                Assert.Empty(Directory.GetFiles(certDirectoryB, "account.key.*.tmp", SearchOption.TopDirectoryOnly));
            }
            finally
            {
                releasePathA.Set();

                Exception? taskFailure = null;
                try
                {
                    await AwaitIfStartedAsync(taskA);
                    await AwaitIfStartedAsync(taskB);
                }
                catch (Exception ex)
                {
                    taskFailure = ex;
                }

                Exception? cleanupFailure = null;
                try
                {
                    if (Directory.Exists(root))
                    {
                        Directory.Delete(root, recursive: true);
                    }
                }
                catch (Exception ex)
                {
                    cleanupFailure = ex;
                }

                if (taskFailure is not null)
                {
                    if (cleanupFailure is not null)
                    {
                        throw new AggregateException(taskFailure, cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(taskFailure).Throw();
                }

                if (cleanupFailure is not null)
                {
                    ExceptionDispatchInfo.Capture(cleanupFailure).Throw();
                }
            }
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

        private static async Task AwaitIfStartedAsync(Task<string>? task)
        {
            if (task is null)
            {
                return;
            }

            await task;
        }

        private sealed class GateAcquireObserver : TestAcmeAccountKeyPathLockCoordinator.IAcquireObserver, IDisposable
        {
            private readonly ConcurrentDictionary<string, ManualResetEventSlim> _attempted = new(StringComparer.Ordinal);
            private readonly ConcurrentDictionary<string, ManualResetEventSlim> _entered = new(StringComparer.Ordinal);
            private readonly ConcurrentDictionary<string, ManualResetEventSlim> _entryBlocks = new(StringComparer.Ordinal);

            public void BlockOnEnter(string keyFilePath, ManualResetEventSlim blockSignal)
            {
                string lockKey = GetLockKey(keyFilePath);
                _entryBlocks[lockKey] = blockSignal;
            }

            public void OnAcquireAttempt(string lockKey)
            {
                ManualResetEventSlim attempted = _attempted.GetOrAdd(lockKey, _ => new ManualResetEventSlim(false));
                attempted.Set();
            }

            public void OnEntered(string lockKey)
            {
                ManualResetEventSlim entered = _entered.GetOrAdd(lockKey, _ => new ManualResetEventSlim(false));
                entered.Set();

                if (_entryBlocks.TryGetValue(lockKey, out ManualResetEventSlim? blocker))
                {
                    blocker.Wait();
                }
            }

            public void OnExited(string lockKey)
            {
            }

            public void WaitForAttempt(string keyFilePath)
            {
                string lockKey = GetLockKey(keyFilePath);
                ManualResetEventSlim attempted = _attempted.GetOrAdd(lockKey, _ => new ManualResetEventSlim(false));
                attempted.Wait();
            }

            public void WaitForEnter(string keyFilePath)
            {
                string lockKey = GetLockKey(keyFilePath);
                ManualResetEventSlim entered = _entered.GetOrAdd(lockKey, _ => new ManualResetEventSlim(false));
                entered.Wait();
            }

            public bool HasEntered(string keyFilePath)
            {
                string lockKey = GetLockKey(keyFilePath);
                return _entered.TryGetValue(lockKey, out ManualResetEventSlim? entered) && entered.IsSet;
            }

            public void Dispose()
            {
                foreach (ManualResetEventSlim attempted in _attempted.Values)
                {
                    attempted.Dispose();
                }

                foreach (ManualResetEventSlim entered in _entered.Values)
                {
                    entered.Dispose();
                }
            }

            private static string GetLockKey(string keyFilePath)
            {
                string fullPath = Path.GetFullPath(keyFilePath);
                return OperatingSystem.IsWindows()
                    ? fullPath.ToUpperInvariant()
                    : fullPath;
            }
        }
    }
}
