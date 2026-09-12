// <copyright file="AcmeCertificateIssuerDns01RecoveryTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for acme certificate issuer dns01 recovery, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the acme certificate issuer dns01 recovery test suite.

using System.Reflection;
using CloudFlare.Client.Enumerators;
using Certes;
using Certes.Acme;
using Certes.Acme.Resource;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Certificates
{
    /// <summary>
    /// Confirms the acme certificate issuer dns01 recovery tests behavior.
    /// </summary>
    public sealed class AcmeCertificateIssuerDns01RecoveryTests
    {
        /// <summary>
        /// Verifies production Cloudflare TXT request construction includes canonical ownership comment metadata and
        /// does not require tags.
        /// </summary>
        [Fact]
        public void CreateAcmeTxtRecordRequest_UsesCanonicalOwnershipCommentWithoutTags()
        {
            const string recordName = RecoveryScenario.RecordName;
            const string recordValue = "challenge-value";

            CloudFlare.Client.Api.Zones.DnsRecord.NewDnsRecord request = CloudflareTxtRecordApi.CreateAcmeTxtRecordRequest(recordName, recordValue);

            Assert.Equal(recordName, request.Name);
            Assert.Equal(DnsRecordType.Txt, request.Type);
            Assert.Equal(recordValue, request.Content);
            Assert.False(request.Proxied);
            Assert.Equal(60, request.Ttl);
            Assert.Equal(AcmeDnsTxtRecordOwnership.OwnershipComment, request.Comment);
            Assert.True(request.Tags is null || request.Tags.Count == 0);
        }

        /// <summary>
        /// Confirms a newly created record carries canonical ownership metadata and is removed by same-attempt cleanup.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenNoExistingTxtRecord_CreatesOwnedChallengeAndCleansUp()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(initialRecords: [], shouldFailValidation: false, shouldFailFinalize: false, throwOnDelete: false, cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(1, result.Api.DeleteCallCount);
            Assert.Single(result.Api.AddedRecords);
            Assert.True(RecordHasCanonicalOwnership(result.Api.AddedRecords[0]));
            Assert.Empty(result.Api.Records);
        }
        /// <summary>
        /// Verifies stale owned TXT records are removed, replacements are created with canonical ownership metadata,
        /// and unrelated records remain untouched.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenStaleOwnedRecordExists_DeletesOwnedStaleCreatesOwnedReplacementAndPreservesUnrelated()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords: [
                    CreateRecord("stale-1", RecoveryScenario.RecordName, "old-value", tags: [], comment: AcmeDnsTxtRecordOwnership.OwnershipComment),
                    CreateRecord("unrelated-1", RecoveryScenario.RecordName, "unrelated-value", tags: ["other-service:acme-dns01"], comment: "operator managed")],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(2, result.Api.DeleteCallCount);
            Assert.Contains("stale-1", result.Api.DeletedRecordIds);
            Assert.Contains(result.Api.Records, record => string.Equals(record.Content, "unrelated-value", StringComparison.Ordinal));
            Assert.DoesNotContain(result.Api.Records, record => string.Equals(record.Content, "old-value", StringComparison.Ordinal));
            Assert.Contains(result.Api.AddedRecords, record => RecordHasCanonicalOwnership(record));
        }

        /// <summary>
        /// Verifies an existing exact TXT challenge value is reused and not deleted by final cleanup when this attempt
        /// did not create it.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenExactTxtAlreadyExists_ReusesExistingChallengeValueWithoutDeletingIt()
        {
            CloudflareTxtRecordInfo existingRecord = CreateRecord(
                "existing-unowned",
                RecoveryScenario.RecordName,
                RecoveryScenario.ExpectedTxtValue,
                tags: ["external.workflow"],
                comment: "external owner");

            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords: [existingRecord],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(0, result.Api.AddCallCount);
            Assert.Equal(0, result.Api.DeleteCallCount);
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, existingRecord.Id, StringComparison.Ordinal));
        }
        /// <summary>
        /// Confirms the issue certificate async when issuance fails still attempts cleanup behavior.
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
        /// Verifies records with non-canonical or near-match metadata are preserved during reconciliation.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenOwnershipMetadataIsNonCanonical_PreservesRecords()
        {
            string currentValue = RecoveryScenario.ExpectedTxtValue;
            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords:
                [
                    CreateRecord("suffix", RecoveryScenario.RecordName, "old-1", tags: [], comment: AcmeDnsTxtRecordOwnership.OwnershipComment + "-extra"),
                    CreateRecord("prefix", RecoveryScenario.RecordName, "old-2", tags: [], comment: "legacy-" + AcmeDnsTxtRecordOwnership.OwnershipComment),
                    CreateRecord("different-case", RecoveryScenario.RecordName, "old-3", tags: [], comment: "vectornntp.backfiller:acme-dns01"),
                    CreateRecord("legacy-invalid", RecoveryScenario.RecordName, "old-4", tags: [], comment: "vectornntp.backfiller.acme-dns01"),
                    CreateRecord("generic-acme", RecoveryScenario.RecordName, "old-5", tags: [], comment: "acme"),
                    CreateRecord("backfiller-only", RecoveryScenario.RecordName, "old-6", tags: [], comment: "BackFiller"),
                    CreateRecord("other-app", RecoveryScenario.RecordName, "old-7", tags: [], comment: "OtherApp:acme-dns01"),
                    CreateRecord("canonical-tag-only", RecoveryScenario.RecordName, "old-8", tags: ["vectornntp_backfiller:acme-dns01"], comment: null),
                    CreateRecord("empty-comment", RecoveryScenario.RecordName, "old-9", tags: [], comment: string.Empty),
                    CreateRecord("exact-current", RecoveryScenario.RecordName, currentValue, tags: ["external"], comment: "external reuse")
                ],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(0, result.Api.AddCallCount);
            Assert.Equal(0, result.Api.DeleteCallCount);
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "suffix", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "prefix", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "different-case", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "legacy-invalid", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "generic-acme", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "backfiller-only", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "other-app", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "canonical-tag-only", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "empty-comment", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "exact-current", StringComparison.Ordinal));
        }

        /// <summary>
        /// Verifies canonical comment ownership does not authorize deletion when record identity constraints fail,
        /// even when provider list results include records outside the requested name.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenRecordIdentityDoesNotMatch_PreservesCanonicalCommentRecords()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords:
                [
                    CreateRecord("wrong-name", "_acme-challenge.other.example.net", "old-name", tags: [], comment: AcmeDnsTxtRecordOwnership.OwnershipComment),
                    CreateRecord("wrong-type", RecoveryScenario.RecordName, "old-type", type: DnsRecordType.A, tags: [], comment: AcmeDnsTxtRecordOwnership.OwnershipComment)
                ],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None,
                returnAllRecordsFromGet: true);

            Assert.True(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(1, result.Api.DeleteCallCount);
            Assert.DoesNotContain("wrong-name", result.Api.DeletedRecordIds);
            Assert.DoesNotContain("wrong-type", result.Api.DeletedRecordIds);
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "wrong-name", StringComparison.Ordinal));
            Assert.Contains(result.Api.Records, record => string.Equals(record.Id, "wrong-type", StringComparison.Ordinal));
        }

        /// <summary>
        /// Models restart recovery: process A creates owned TXT and terminates before cleanup, then process B
        /// reconciles the persisted stale-owned record and creates a replacement.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenProcessRestarts_ReconcilesPreviouslyOwnedStaleRecordOnly()
        {
            FakeCloudflareTxtRecordApi sharedProvider = new([], throwOnDelete: false);
            CloudflareTxtRecordInfo staleOwnedRecord = await sharedProvider
                .AddTxtRecordAsync("zone", RecoveryScenario.RecordName, RecoveryScenario.ExpectedTxtValue, CancellationToken.None)
                .ConfigureAwait(false);

            Assert.Single(sharedProvider.Records);
            Assert.True(RecordHasCanonicalOwnership(staleOwnedRecord));

            string replacementValue = RecoveryScenario.GetExpectedTxtValue("dns01-test-token-restart");
            RecoveryScenarioResult secondAttempt = await ExecuteScenarioAsync(
                initialRecords: [],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None,
                challengeTokenOverride: "dns01-test-token-restart",
                persistentApi: sharedProvider);

            Assert.True(secondAttempt.WasSuccessful);
            Assert.Equal(2, sharedProvider.AddCallCount);
            Assert.Equal(2, sharedProvider.DeleteCallCount);
            Assert.Contains(staleOwnedRecord.Id, sharedProvider.DeletedRecordIds);
            Assert.Contains(sharedProvider.AddedRecords, record => string.Equals(record.Content, replacementValue, StringComparison.Ordinal));
            Assert.DoesNotContain(sharedProvider.Records, record => string.Equals(record.Id, staleOwnedRecord.Id, StringComparison.Ordinal));
        }

        /// <summary>
        /// Executes one focused DNS-01 authorization scenario against the production ownership-aware challenge flow.
        /// </summary>
        /// <param name="initialRecords">Initial TXT records visible at the ACME challenge name.</param>
        /// <param name="shouldFailValidation">Whether the simulated ACME challenge should fail validation.</param>
        /// <param name="shouldFailFinalize">Unused legacy scenario flag retained to avoid widening the test diff.</param>
        /// <param name="throwOnDelete">Whether TXT-record cleanup should throw.</param>
        /// <param name="cancellationToken">Cancellation token for the scenario.</param>
        /// <param name="failChallengeAfterCreate">Whether the simulated challenge should throw immediately after record reconciliation/creation.</param>
        /// <param name="challengeTokenOverride">
        /// Optional challenge token override used to model a different issuance attempt, such as a restart where the
        /// later process receives a different ACME DNS-01 token.
        /// </param>
        /// <param name="persistentApi">
        /// Optional shared fake provider state used to model records surviving a process boundary and being observed by
        /// a subsequent issuance attempt.
        /// </param>
        /// <param name="returnAllRecordsFromGet">
        /// When <see langword="true"/>, the fake provider returns all seeded records without record-name filtering so
        /// identity-negative tests can pass mismatched-name fixtures into production reconciliation logic.
        /// </param>
        /// <returns>The scenario result capturing TXT API activity and any surfaced exception.</returns>
        private static async Task<RecoveryScenarioResult> ExecuteScenarioAsync(
            IReadOnlyList<CloudflareTxtRecordInfo> initialRecords,
            bool shouldFailValidation,
            bool shouldFailFinalize,
            bool throwOnDelete,
            CancellationToken cancellationToken,
            bool failChallengeAfterCreate = false,
            string? challengeTokenOverride = null,
            FakeCloudflareTxtRecordApi? persistentApi = null,
            bool returnAllRecordsFromGet = false)
        {
            _ = shouldFailFinalize;
            string tempDir = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-AcmeDns01-{Guid.NewGuid():N}");
            _ = System.IO.Directory.CreateDirectory(tempDir);

            try
            {
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir);
                FakeCloudflareTxtRecordApi api = persistentApi ?? new(initialRecords, throwOnDelete, returnAllRecordsFromGet);
                if (persistentApi is not null)
                {
                    api.Seed(initialRecords);
                }

                FakeAuthoritativeDnsTxtPropagationVerifier verifier = new();
                AcmeCertificateIssuer issuer = new(TimeProvider.System, NullLogger<AcmeCertificateIssuer>.Instance, verifier, _ => api);
                FakeAuthorizationContext authorizationContext = new(shouldFailValidation, failChallengeAfterCreate, challengeTokenOverride ?? RecoveryScenario.ChallengeToken);
                AcmeContext acmeContext = new(WellKnownServers.LetsEncryptStagingV2, RecoveryScenario.AccountKey);

                MethodInfo completeAuthorizationMethod = typeof(AcmeCertificateIssuer).GetMethod(
                    "CompleteAuthorizationAsync",
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    [typeof(AcmeContext), typeof(IAuthorizationContext), typeof(ICloudflareTxtRecordApi), typeof(BackFillerLetsEncryptRuntimeOptions), typeof(CancellationToken)])
                    ?? throw new InvalidOperationException("CompleteAuthorizationAsync was not found.");

                Exception? error = null;
                try
                {
                    Task operation = (Task)(completeAuthorizationMethod.Invoke(issuer, [acmeContext, authorizationContext, api, options, cancellationToken])
                        ?? throw new InvalidOperationException("CompleteAuthorizationAsync did not return a task."));
                    await operation.ConfigureAwait(false);
                }
                catch (TargetInvocationException ex) when (ex.InnerException is not null)
                {
                    error = ex.InnerException;
                }
                catch (Exception ex)
                {
                    error = ex;
                }

                return new RecoveryScenarioResult(api, error is null, error);
            }
            finally
            {
                System.IO.Directory.Delete(tempDir, recursive: true);
            }
        }

        /// <summary>
        /// Confirms the create lets encrypt options behavior.
        /// </summary>
        /// <returns>The value returned by the create lets encrypt options helper.</returns>
        /// <summary>
        /// Confirms the create lets encrypt options behavior.
        /// </summary>
        /// <param name="tempDir">The temp dir used by this test scenario.</param>
        /// <returns>The value returned by the create lets encrypt options helper.</returns>
        private static BackFillerLetsEncryptRuntimeOptions CreateLetsEncryptOptions(string tempDir)
        {
            return new BackFillerLetsEncryptRuntimeOptions(
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
        /// Confirms the recovery scenario behavior.
        /// </summary>
        private static class RecoveryScenario
        {
            /// <summary>
            /// Supplies fqdn for the fixture or scenario under test.
            /// </summary>
            internal const string Fqdn = "backfiller01.usenet.ninja";

            /// <summary>
            /// Supplies the DNS-01 TXT record host name used by the production issuer.
            /// </summary>
            internal const string RecordName = "_acme-challenge." + Fqdn;

            /// <summary>
            /// Supplies the deterministic challenge token used by the fake ACME challenge.
            /// </summary>
            internal const string ChallengeToken = "dns01-test-token";

            /// <summary>
            /// Supplies the shared ACME account key used to derive the DNS-01 TXT value for the scenario.
            /// </summary>
            internal static IKey AccountKey { get; } = KeyFactory.NewKey(KeyAlgorithm.ES256);

            /// <summary>
            /// Supplies the expected TXT value derived from the same ACME account-key logic used in production.
            /// </summary>
            internal static string ExpectedTxtValue => AccountKey.DnsTxt(ChallengeToken);

            /// <summary>
            /// Derives the expected TXT value for one explicit challenge token.
            /// </summary>
            internal static string GetExpectedTxtValue(string challengeToken)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(challengeToken);
                return AccountKey.DnsTxt(challengeToken);
            }
        }

        /// <summary>
        /// Confirms the recovery scenario result behavior.
        /// </summary>
        /// <returns>The value returned by the recovery scenario result helper.</returns>
        /// <summary>
        /// Confirms the recovery scenario result behavior.
        /// </summary>
        /// <param name="Api">The api used by this test scenario.</param>
        /// <param name="WasSuccessful">The was successful used by this test scenario.</param>
        /// <param name="Error">The error used by this test scenario.</param>
        /// <returns>The value returned by the recovery scenario result helper.</returns>
        private sealed record RecoveryScenarioResult(FakeCloudflareTxtRecordApi Api, bool WasSuccessful, Exception? Error);

        /// <summary>
        /// Creates one deterministic TXT record fixture with explicit metadata.
        /// </summary>
        private static CloudflareTxtRecordInfo CreateRecord(
            string id,
            string name,
            string value,
            DnsRecordType type = DnsRecordType.Txt,
            IReadOnlyList<string>? tags = null,
            string? comment = null)
        {
            return new CloudflareTxtRecordInfo(id, name, value, type, false, 60, comment, tags ?? [], null, null);
        }

        /// <summary>
        /// Determines whether a TXT record contains the canonical persisted ownership metadata.
        /// </summary>
        private static bool RecordHasCanonicalOwnership(CloudflareTxtRecordInfo record)
        {
            return string.Equals(record.Comment, AcmeDnsTxtRecordOwnership.OwnershipComment, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms the fake cloudflare txt record api behavior.
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
            /// Controls whether list calls should bypass record-name filtering to exercise identity checks in
            /// production reconciliation logic.
            /// </summary>
            private readonly bool _returnAllRecordsFromGet;
            /// <summary>
            /// Supplies  next id for the fixture or scenario under test.
            /// </summary>
            private int _nextId = 1000;

            /// <summary>
            /// Confirms the fake cloudflare txt record api behavior.
            /// </summary>
            /// <param name="initialRecords">Initial provider records seeded for the scenario.</param>
            /// <param name="throwOnDelete">Whether delete operations should throw to simulate cleanup failures.</param>
            /// <param name="returnAllRecordsFromGet">
            /// Whether list operations should bypass record-name filtering so identity-negative fixtures can reach
            /// production reconciliation logic.
            /// </param>
            internal FakeCloudflareTxtRecordApi(IEnumerable<CloudflareTxtRecordInfo> initialRecords, bool throwOnDelete, bool returnAllRecordsFromGet = false)
            {
                _records = [.. initialRecords];
                _throwOnDelete = throwOnDelete;
                _returnAllRecordsFromGet = returnAllRecordsFromGet;
            }

            /// <summary>
            /// Adds deterministic seed records to the provider state when modeling restart/reload boundaries.
            /// </summary>
            internal void Seed(IEnumerable<CloudflareTxtRecordInfo> records)
            {
                _records.AddRange(records);
            }

            /// <summary>
            /// Supplies records for the fixture or scenario under test.
            /// </summary>
            internal IReadOnlyList<CloudflareTxtRecordInfo> Records => _records;
            /// <summary>
            /// Captures provider-created records for request-mapping assertions.
            /// </summary>
            internal List<CloudflareTxtRecordInfo> AddedRecords { get; } = [];

            /// <summary>
            /// Captures record identifiers deleted by reconciliation or final cleanup.
            /// </summary>
            internal List<string> DeletedRecordIds { get; } = [];

            /// <summary>
            /// Supplies add call count for the fixture or scenario under test.
            /// </summary>
            internal int AddCallCount { get; private set; }
            /// <summary>
            /// Supplies delete call count for the fixture or scenario under test.
            /// </summary>
            internal int DeleteCallCount { get; private set; }

            /// <summary>
            /// Confirms the get txt records async behavior.
            /// </summary>
            /// <returns>The value returned by the get txt records async helper.</returns>
            /// <summary>
            /// Confirms the get txt records async behavior.
            /// </summary>
            /// <param name="zoneId">The zone id used by this test scenario.</param>
            /// <param name="recordName">The record name used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the get txt records async helper.</returns>
            public Task<IReadOnlyList<CloudflareTxtRecordInfo>> GetTxtRecordsAsync(string zoneId, string recordName, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_returnAllRecordsFromGet)
                {
                    return Task.FromResult<IReadOnlyList<CloudflareTxtRecordInfo>>([.. _records]);
                }

                return Task.FromResult<IReadOnlyList<CloudflareTxtRecordInfo>>([.. _records.Where(record => record.Name == recordName)]);
            }

            /// <summary>
            /// Confirms the add txt record async behavior.
            /// </summary>
            /// <returns>The value returned by the add txt record async helper.</returns>
            /// <summary>
            /// Confirms the add txt record async behavior.
            /// </summary>
            /// <param name="zoneId">The zone id used by this test scenario.</param>
            /// <param name="recordName">The record name used by this test scenario.</param>
            /// <param name="recordValue">The record value used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the add txt record async helper.</returns>
            public Task<CloudflareTxtRecordInfo> AddTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                AddCallCount++;
                CloudflareTxtRecordInfo record = new(
                    $"new-{_nextId++}",
                    recordName,
                    recordValue,
                    CloudFlare.Client.Enumerators.DnsRecordType.Txt,
                    false,
                    60,
                    AcmeDnsTxtRecordOwnership.OwnershipComment,
                    [],
                    null,
                    null);

                AddedRecords.Add(record);
                _records.Add(record);
                return Task.FromResult(record);
            }

            /// <summary>
            /// Confirms the delete txt record async behavior.
            /// </summary>
            /// <returns>The value returned by the delete txt record async helper.</returns>
            /// <summary>
            /// Confirms the delete txt record async behavior.
            /// </summary>
            /// <param name="zoneId">The zone id used by this test scenario.</param>
            /// <param name="recordId">The record id used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the delete txt record async helper.</returns>
            public Task DeleteTxtRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                DeleteCallCount++;
                if (_throwOnDelete)
                {
                    throw new InvalidOperationException("delete failed");
                }

                DeletedRecordIds.Add(recordId);
                _ = _records.RemoveAll(record => record.Id == recordId);
                return Task.CompletedTask;
            }

            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Confirms the fake authoritative dns txt propagation verifier behavior.
        /// </summary>
        private sealed class FakeAuthoritativeDnsTxtPropagationVerifier : IAuthoritativeDnsTxtPropagationVerifier
        {
            /// <summary>
            /// Confirms the wait for propagation async behavior.
            /// </summary>
            /// <returns>The value returned by the wait for propagation async helper.</returns>
            /// <summary>
            /// Confirms the wait for propagation async behavior.
            /// </summary>
            /// <param name="fqdn">The fqdn used by this test scenario.</param>
            /// <param name="expectedTxtValue">The expected txt value used by this test scenario.</param>
            /// <param name="options">The options used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the wait for propagation async helper.</returns>
            public Task WaitForPropagationAsync(string fqdn, string expectedTxtValue, BackFillerLetsEncryptRuntimeOptions options, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Simulates one ACME authorization resource for focused DNS-01 recovery tests.
        /// </summary>
        private sealed class FakeAuthorizationContext : IAuthorizationContext
        {
            /// <summary>
            /// Tracks the backing authorization resource exposed to the production polling logic.
            /// </summary>
            private readonly Authorization _authorization;

            /// <summary>
            /// Tracks the single DNS-01 challenge context returned to the production issuer.
            /// </summary>
            private readonly FakeChallengeContext _challengeContext;

            /// <summary>
            /// Initializes one fake authorization context.
            /// </summary>
            /// <param name="shouldFailValidation">Whether challenge validation should transition to invalid.</param>
            /// <param name="failChallengeAfterCreate">Whether validation should throw immediately after DNS setup.</param>
            /// <param name="challengeToken">
            /// DNS-01 challenge token used for this simulated attempt so restart scenarios can model token changes across
            /// separate process executions.
            /// </param>
            internal FakeAuthorizationContext(bool shouldFailValidation, bool failChallengeAfterCreate, string challengeToken)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(challengeToken);

                _authorization = new Authorization
                {
                    Identifier = new Identifier { Type = IdentifierType.Dns, Value = RecoveryScenario.Fqdn },
                    Status = AuthorizationStatus.Pending,
                    Challenges =
                    [
                        new Challenge
                        {
                            Type = ChallengeTypes.Dns01,
                            Token = challengeToken,
                            Status = ChallengeStatus.Pending,
                        },
                    ],
                };
                _challengeContext = new FakeChallengeContext(_authorization, shouldFailValidation, failChallengeAfterCreate);
            }

            /// <summary>
            /// Supplies the fake location required by the Certes resource-context contract.
            /// </summary>
            public Uri Location => new("https://unit.test/acme/authorization");

            /// <summary>
            /// Supplies a zero retry-after delay because these tests do not model server pacing.
            /// </summary>
            public int RetryAfter => 0;

            /// <summary>
            /// Returns the current fake authorization resource state.
            /// </summary>
            /// <returns>The current authorization state.</returns>
            public Task<Authorization> Resource()
            {
                return Task.FromResult(_authorization);
            }

            /// <summary>
            /// Returns the single DNS-01 challenge context used by the production issuer.
            /// </summary>
            /// <returns>The fake DNS-01 challenge context.</returns>
            public Task<IChallengeContext> Dns()
            {
                return Task.FromResult<IChallengeContext>(_challengeContext);
            }

            /// <summary>
            /// Returns all fake challenge contexts for interface completeness.
            /// </summary>
            /// <returns>The single fake DNS-01 challenge context.</returns>
            public Task<IEnumerable<IChallengeContext>> Challenges()
            {
                return Task.FromResult<IEnumerable<IChallengeContext>>([_challengeContext]);
            }

            /// <summary>
            /// Deactivates the fake authorization.
            /// </summary>
            /// <returns>The updated authorization resource.</returns>
            public Task<Authorization> Deactivate()
            {
                _authorization.Status = AuthorizationStatus.Deactivated;
                return Task.FromResult(_authorization);
            }
        }

        /// <summary>
        /// Simulates one ACME DNS-01 challenge resource for focused recovery tests.
        /// </summary>
        private sealed class FakeChallengeContext : IChallengeContext
        {
            /// <summary>
            /// Tracks the fake authorization so validation can update its final status.
            /// </summary>
            private readonly Authorization _authorization;

            /// <summary>
            /// Indicates whether validation should transition the challenge to invalid.
            /// </summary>
            private readonly bool _shouldFailValidation;

            /// <summary>
            /// Indicates whether validation should throw after DNS setup to verify cleanup-on-failure.
            /// </summary>
            private readonly bool _failChallengeAfterCreate;

            /// <summary>
            /// Tracks the fake challenge resource returned to the issuer.
            /// </summary>
            private readonly Challenge _challenge;

            /// <summary>
            /// Initializes one fake challenge context.
            /// </summary>
            /// <param name="authorization">Owning fake authorization resource.</param>
            /// <param name="shouldFailValidation">Whether validation should transition to invalid.</param>
            /// <param name="failChallengeAfterCreate">Whether validation should throw after DNS setup.</param>
            internal FakeChallengeContext(Authorization authorization, bool shouldFailValidation, bool failChallengeAfterCreate)
            {
                _authorization = authorization;
                _shouldFailValidation = shouldFailValidation;
                _failChallengeAfterCreate = failChallengeAfterCreate;
                _challenge = authorization.Challenges.Single();
            }

            /// <summary>
            /// Supplies the fake location required by the Certes resource-context contract.
            /// </summary>
            public Uri Location => new("https://unit.test/acme/challenge");

            /// <summary>
            /// Supplies a zero retry-after delay because these tests do not model server pacing.
            /// </summary>
            public int RetryAfter => 0;

            /// <summary>
            /// Supplies the ACME key-authorization placeholder for interface completeness.
            /// </summary>
            public string KeyAuthz => "unused-key-authorization";

            /// <summary>
            /// Supplies the deterministic token used to derive the DNS-01 TXT value.
            /// </summary>
            public string Token => _challenge.Token;

            /// <summary>
            /// Supplies the DNS-01 challenge type expected by production.
            /// </summary>
            public string Type => ChallengeTypes.Dns01;

            /// <summary>
            /// Returns the current fake challenge resource state.
            /// </summary>
            /// <returns>The current challenge state.</returns>
            public Task<Challenge> Resource()
            {
                return Task.FromResult(_challenge);
            }

            /// <summary>
            /// Simulates the ACME validation transition after DNS setup completes.
            /// </summary>
            /// <returns>The updated challenge resource.</returns>
            public Task<Challenge> Validate()
            {
                if (_failChallengeAfterCreate)
                {
                    throw new InvalidOperationException("Simulated challenge validation failure.");
                }

                if (_shouldFailValidation)
                {
                    _challenge.Status = ChallengeStatus.Invalid;
                    _authorization.Status = AuthorizationStatus.Invalid;
                }
                else
                {
                    _challenge.Status = ChallengeStatus.Valid;
                    _challenge.Validated = DateTimeOffset.UtcNow;
                    _authorization.Status = AuthorizationStatus.Valid;
                }

                return Task.FromResult(_challenge);
            }
        }
    }
}
