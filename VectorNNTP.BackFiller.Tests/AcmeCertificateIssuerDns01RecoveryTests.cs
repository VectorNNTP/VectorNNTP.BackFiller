// <copyright file="AcmeCertificateIssuerDns01RecoveryTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for acme certificate issuer dns01 recovery, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the acme certificate issuer dns01 recovery test suite.

using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
        /// Verifies the acme certificate issuer dns01 recovery tests scenario and its documented contract.
    /// </summary>
    public sealed class AcmeCertificateIssuerDns01RecoveryTests
    {
        /// <summary>
        /// Verifies the issue certificate async when no existing txt record creates challenge and cleans up scenario and its documented contract.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenNoExistingTxtRecord_CreatesChallengeAndCleansUp()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(initialRecords: [], shouldFailValidation: false, shouldFailFinalize: false, throwOnDelete: false, cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(1, result.Api.DeleteCallCount);
            Assert.Empty(result.Api.Records);
        }
        /// <summary>
        /// Verifies the issue certificate async when stale challenge record exists deletes stale record and creates replacement scenario and its documented contract.
        /// </summary>
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
        /// <summary>
        /// Verifies the issue certificate async when exact txt already exists reuses existing challenge value scenario and its documented contract.
        /// </summary>
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
        /// <summary>
        /// Verifies the issue certificate async when issuance fails still attempts cleanup scenario and its documented contract.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenIssuanceFails_StillAttemptsCleanup()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(initialRecords: [], shouldFailValidation: true, shouldFailFinalize: false, throwOnDelete: false, cancellationToken: CancellationToken.None, failChallengeAfterCreate: true);

            Assert.False(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(1, result.Api.DeleteCallCount);
        }

        /// <summary>
        /// Verifies the execute scenario async scenario and its documented contract.
        /// </summary>
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

        /// <summary>
        /// Verifies the create lets encrypt options scenario and its documented contract.
        /// </summary>
        /// <returns>The create lets encrypt options value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create lets encrypt options scenario and its documented contract.
        /// </summary>
        /// <param name="tempDir">The temp dir supplied to the helper.</param>
        /// <returns>The create lets encrypt options value produced for the requested scenario.</returns>
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

        /// <summary>
        /// Verifies the recovery scenario scenario and its documented contract.
        /// </summary>
        private static class RecoveryScenario
        {
            /// <summary>
            /// Supplies fqdn for the fixture or scenario under test.
            /// </summary>
            internal const string Fqdn = "backfiller01.usenet.ninja";
            /// <summary>
            /// Supplies expected txt value for the fixture or scenario under test.
            /// </summary>
            internal const string ExpectedTxtValue = "challenge-value";
        }

        /// <summary>
        /// Verifies the recovery scenario result scenario and its documented contract.
        /// </summary>
        /// <returns>The recovery scenario result value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the recovery scenario result scenario and its documented contract.
        /// </summary>
        /// <param name="Api">The api supplied to the helper.</param>
        /// <param name="WasSuccessful">The was successful supplied to the helper.</param>
        /// <param name="Error">The error supplied to the helper.</param>
        /// <returns>The recovery scenario result value produced for the requested scenario.</returns>
        private sealed record RecoveryScenarioResult(FakeCloudflareTxtRecordApi Api, bool WasSuccessful, Exception? Error);

        /// <summary>
        /// Verifies the fake cloudflare txt record api scenario and its documented contract.
        /// </summary>
        private sealed class FakeCloudflareTxtRecordApi : ICloudflareTxtRecordApi
        {
            /// <summary>
            /// Supplies  records for the fixture or scenario under test.
            /// </summary>
            private readonly List<CloudflareTxtRecordInfo> _records;
            /// <summary>
            /// Supplies  throw on delete for the fixture or scenario under test.
            /// </summary>
            private readonly bool _throwOnDelete;
            /// <summary>
            /// Supplies  next id for the fixture or scenario under test.
            /// </summary>
            private int _nextId = 1000;

            /// <summary>
        /// Verifies the fake cloudflare txt record api scenario and its documented contract.
            /// </summary>
            internal FakeCloudflareTxtRecordApi(IEnumerable<CloudflareTxtRecordInfo> initialRecords, bool throwOnDelete)
            {
                _records = [.. initialRecords];
                _throwOnDelete = throwOnDelete;
            }

            /// <summary>
            /// Supplies records for the fixture or scenario under test.
            /// </summary>
            internal IReadOnlyList<CloudflareTxtRecordInfo> Records => _records;
            /// <summary>
            /// Supplies add call count for the fixture or scenario under test.
            /// </summary>
            internal int AddCallCount { get; private set; }
            /// <summary>
            /// Supplies delete call count for the fixture or scenario under test.
            /// </summary>
            internal int DeleteCallCount { get; private set; }

            /// <summary>
        /// Verifies the get txt records async scenario and its documented contract.
            /// </summary>
        /// <returns>The get txt records async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the get txt records async scenario and its documented contract.
        /// </summary>
        /// <param name="zoneId">The zone id supplied to the helper.</param>
        /// <param name="recordName">The record name supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The get txt records async value produced for the requested scenario.</returns>
            public Task<IReadOnlyList<CloudflareTxtRecordInfo>> GetTxtRecordsAsync(string zoneId, string recordName, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult<IReadOnlyList<CloudflareTxtRecordInfo>>([.. _records.Where(record => record.Name == recordName)]);
            }

            /// <summary>
        /// Verifies the add txt record async scenario and its documented contract.
            /// </summary>
        /// <returns>The add txt record async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the add txt record async scenario and its documented contract.
        /// </summary>
        /// <param name="zoneId">The zone id supplied to the helper.</param>
        /// <param name="recordName">The record name supplied to the helper.</param>
        /// <param name="recordValue">The record value supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The add txt record async value produced for the requested scenario.</returns>
            public Task<CloudflareTxtRecordInfo> AddTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                AddCallCount++;
                CloudflareTxtRecordInfo record = new($"new-{_nextId++}", recordName, recordValue, CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 60, "BackFiller ACME", ["acme"], null, null);
                _records.Add(record);
                return Task.FromResult(record);
            }

            /// <summary>
        /// Verifies the delete txt record async scenario and its documented contract.
            /// </summary>
        /// <returns>The delete txt record async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the delete txt record async scenario and its documented contract.
        /// </summary>
        /// <param name="zoneId">The zone id supplied to the helper.</param>
        /// <param name="recordId">The record id supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The delete txt record async value produced for the requested scenario.</returns>
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

            /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
            /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
        /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Verifies the fake authoritative dns txt propagation verifier scenario and its documented contract.
        /// </summary>
        private sealed class FakeAuthoritativeDnsTxtPropagationVerifier : IAuthoritativeDnsTxtPropagationVerifier
        {
            /// <summary>
        /// Verifies the wait for propagation async scenario and its documented contract.
            /// </summary>
        /// <returns>The wait for propagation async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the wait for propagation async scenario and its documented contract.
        /// </summary>
        /// <param name="fqdn">The fqdn supplied to the helper.</param>
        /// <param name="expectedTxtValue">The expected txt value supplied to the helper.</param>
        /// <param name="options">The options supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The wait for propagation async value produced for the requested scenario.</returns>
            public Task WaitForPropagationAsync(string fqdn, string expectedTxtValue, BackFillerLetsEncryptRuntimeOptions options, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Verifies the fake acme context factory scenario and its documented contract.
        /// </summary>
        /// <returns>The fake acme context factory value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the fake acme context factory scenario and its documented contract.
        /// </summary>
        /// <param name="shouldFailValidation">The should fail validation supplied to the helper.</param>
        /// <param name="shouldFailFinalize">The should fail finalize supplied to the helper.</param>
        /// <param name="failChallengeAfterCreate">The fail challenge after create supplied to the helper.</param>
        /// <returns>The fake acme context factory value produced for the requested scenario.</returns>
        private sealed class FakeAcmeContextFactory(bool shouldFailValidation, bool shouldFailFinalize, bool failChallengeAfterCreate)
        {
            /// <summary>
            /// Supplies should fail validation for the fixture or scenario under test.
            /// </summary>
            private bool ShouldFailValidation { get; } = shouldFailValidation;
            /// <summary>
            /// Supplies should fail finalize for the fixture or scenario under test.
            /// </summary>
            private bool ShouldFailFinalize { get; } = shouldFailFinalize;
            /// <summary>
            /// Supplies fail challenge after create for the fixture or scenario under test.
            /// </summary>
            private bool FailChallengeAfterCreate { get; } = failChallengeAfterCreate;
        }
    }
}
