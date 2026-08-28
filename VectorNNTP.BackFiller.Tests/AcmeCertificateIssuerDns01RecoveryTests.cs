// <copyright file="AcmeCertificateIssuerDns01RecoveryTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>

using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    public sealed class AcmeCertificateIssuerDns01RecoveryTests
    {
        [Fact]
        public async Task IssueCertificateAsync_WhenNoExistingTxtRecord_CreatesChallengeAndCleansUp()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(initialRecords: [], shouldFailValidation: false, shouldFailFinalize: false, throwOnDelete: false, cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(1, result.Api.DeleteCallCount);
            Assert.Empty(result.Api.Records);
        }

        [Fact]
        public async Task IssueCertificateAsync_WhenStaleChallengeRecordExists_DeletesStaleRecordAndCreatesReplacement()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords: [
                    new CloudflareTxtRecordInfo("stale-1", RecoveryScenario.Fqdn, "old-value", CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 60, "BackFiller stale challenge", ["acme"], null, null),
                    new CloudflareTxtRecordInfo("unrelated-1", RecoveryScenario.Fqdn, "unrelated-value", CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 120, "keep", ["other"], null, null)],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(1, result.Api.DeleteCallCount);
            Assert.Contains(result.Api.Records, record => record.Content == "unrelated-value");
            Assert.DoesNotContain(result.Api.Records, record => record.Content == "old-value");
        }

        [Fact]
        public async Task IssueCertificateAsync_WhenExactTxtAlreadyExists_ReusesExistingChallengeValue()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords: [new CloudflareTxtRecordInfo("existing-owned", RecoveryScenario.Fqdn, RecoveryScenario.ExpectedTxtValue, CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 60, "BackFiller challenge", ["acme"], null, null)],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(0, result.Api.AddCallCount);
            Assert.Equal(0, result.Api.DeleteCallCount);
        }

        [Fact]
        public async Task IssueCertificateAsync_WhenIssuanceFails_StillAttemptsCleanup()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(initialRecords: [], shouldFailValidation: true, shouldFailFinalize: false, throwOnDelete: false, cancellationToken: CancellationToken.None, failChallengeAfterCreate: true);

            Assert.False(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(1, result.Api.DeleteCallCount);
        }

        private static async Task<RecoveryScenarioResult> ExecuteScenarioAsync(
            IReadOnlyList<CloudflareTxtRecordInfo> initialRecords,
            bool shouldFailValidation,
            bool shouldFailFinalize,
            bool throwOnDelete,
            CancellationToken cancellationToken,
            bool failChallengeAfterCreate = false)
        {
            string tempDir = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-AcmeDns01-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(tempDir);

            try
            {
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir);
                FakeCloudflareTxtRecordApi api = new(initialRecords, throwOnDelete);
                FakeAuthoritativeDnsTxtPropagationVerifier verifier = new();
                AcmeCertificateIssuer issuer = new(TimeProvider.System, NullLogger<AcmeCertificateIssuer>.Instance, verifier, _ => api);
                FakeAcmeContextFactory acmeFactory = new(shouldFailValidation, shouldFailFinalize, failChallengeAfterCreate);

                AcmeOrderIssueResult? result = null;
                Exception? error = null;
                try
                {
                    result = await issuer.IssueCertificateAsync(options, cancellationToken);
                }
                catch (Exception ex)
                {
                    error = ex;
                }

                return new RecoveryScenarioResult(api, result is not null, error);
            }
            finally
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }

        private static BackFillerLetsEncryptRuntimeOptions CreateLetsEncryptOptions(string tempDir)
        {
            return new BackFillerLetsEncryptRuntimeOptions(
                Enabled: true,
                CanonicalCertificateSubjectName: RecoveryScenario.Fqdn,
                AcmeAccountEmail: "security@example.com",
                AcmeAccountKeyPemPath: Path.Combine(tempDir, "account.key"),
                CertificatePfxPath: Path.Combine(tempDir, "certificate.pfx"),
                CertificatePrivateKeyPemPath: Path.Combine(tempDir, "certificate.key"),
                PfxExportPassword: "UnitTest-PfxPassword-123!",
                RenewBeforeExpiryDays: 7,
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

        private static class RecoveryScenario
        {
            internal const string Fqdn = "backfiller01.usenet.ninja";
            internal const string ExpectedTxtValue = "challenge-value";
        }

        private sealed record RecoveryScenarioResult(FakeCloudflareTxtRecordApi Api, bool WasSuccessful, Exception? Error);

        private sealed class FakeCloudflareTxtRecordApi : ICloudflareTxtRecordApi
        {
            private readonly List<CloudflareTxtRecordInfo> _records;
            private readonly bool _throwOnDelete;
            private int _nextId = 1000;

            internal FakeCloudflareTxtRecordApi(IEnumerable<CloudflareTxtRecordInfo> initialRecords, bool throwOnDelete)
            {
                _records = [.. initialRecords];
                _throwOnDelete = throwOnDelete;
            }

            internal IReadOnlyList<CloudflareTxtRecordInfo> Records => _records;
            internal int AddCallCount { get; private set; }
            internal int DeleteCallCount { get; private set; }

            public Task<IReadOnlyList<CloudflareTxtRecordInfo>> GetTxtRecordsAsync(string zoneId, string recordName, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult<IReadOnlyList<CloudflareTxtRecordInfo>>([.. _records.Where(record => record.Name == recordName)]);
            }

            public Task<CloudflareTxtRecordInfo> AddTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                AddCallCount++;
                CloudflareTxtRecordInfo record = new($"new-{_nextId++}", recordName, recordValue, CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 60, "BackFiller ACME", ["acme"], null, null);
                _records.Add(record);
                return Task.FromResult(record);
            }

            public Task DeleteTxtRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                DeleteCallCount++;
                if (_throwOnDelete)
                {
                    throw new InvalidOperationException("delete failed");
                }

                _ = _records.RemoveAll(record => record.Id == recordId);
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class FakeAuthoritativeDnsTxtPropagationVerifier : IAuthoritativeDnsTxtPropagationVerifier
        {
            public Task WaitForPropagationAsync(string fqdn, string expectedTxtValue, BackFillerLetsEncryptRuntimeOptions options, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
        }

        private sealed class FakeAcmeContextFactory(bool shouldFailValidation, bool shouldFailFinalize, bool failChallengeAfterCreate)
        {
            private bool ShouldFailValidation { get; } = shouldFailValidation;
            private bool ShouldFailFinalize { get; } = shouldFailFinalize;
            private bool FailChallengeAfterCreate { get; } = failChallengeAfterCreate;
        }
    }
}
