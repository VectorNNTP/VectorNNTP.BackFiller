// <copyright file="AcmeCertificateIssuerDns01RecoveryTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for acme certificate issuer dns01 recovery, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the acme certificate issuer dns01 recovery test suite.

using System.Reflection;
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
        /// Confirms the issue certificate async when no existing txt record creates challenge and cleans up behavior.
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
        /// Confirms the issue certificate async when stale challenge record exists deletes stale record and creates replacement behavior.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenStaleChallengeRecordExists_DeletesStaleRecordAndCreatesReplacement()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords: [
                    new CloudflareTxtRecordInfo("stale-1", RecoveryScenario.RecordName, "old-value", CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 60, "BackFiller stale challenge", ["acme"], null, null),
                    new CloudflareTxtRecordInfo("unrelated-1", RecoveryScenario.RecordName, "unrelated-value", CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 120, "keep", ["other"], null, null)],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(1, result.Api.AddCallCount);
            Assert.Equal(2, result.Api.DeleteCallCount);
            Assert.Contains(result.Api.Records, record => record.Content == "unrelated-value");
            Assert.DoesNotContain(result.Api.Records, record => record.Content == "old-value");
        }
        /// <summary>
        /// Confirms the issue certificate async when exact txt already exists reuses existing challenge value behavior.
        /// </summary>
        [Fact]
        public async Task IssueCertificateAsync_WhenExactTxtAlreadyExists_ReusesExistingChallengeValue()
        {
            RecoveryScenarioResult result = await ExecuteScenarioAsync(
                initialRecords: [new CloudflareTxtRecordInfo("existing-owned", RecoveryScenario.RecordName, RecoveryScenario.ExpectedTxtValue, CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 60, "BackFiller challenge", ["acme"], null, null)],
                shouldFailValidation: false,
                shouldFailFinalize: false,
                throwOnDelete: false,
                cancellationToken: CancellationToken.None);

            Assert.True(result.WasSuccessful);
            Assert.Equal(0, result.Api.AddCallCount);
            Assert.Equal(0, result.Api.DeleteCallCount);
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
        /// Executes one focused DNS-01 authorization scenario against the production ownership-aware challenge flow.
        /// </summary>
        /// <param name="initialRecords">Initial TXT records visible at the ACME challenge name.</param>
        /// <param name="shouldFailValidation">Whether the simulated ACME challenge should fail validation.</param>
        /// <param name="shouldFailFinalize">Unused legacy scenario flag retained to avoid widening the test diff.</param>
        /// <param name="throwOnDelete">Whether TXT-record cleanup should throw.</param>
        /// <param name="cancellationToken">Cancellation token for the scenario.</param>
        /// <param name="failChallengeAfterCreate">Whether the simulated challenge should throw immediately after record reconciliation/creation.</param>
        /// <returns>The scenario result capturing TXT API activity and any surfaced exception.</returns>
        private static async Task<RecoveryScenarioResult> ExecuteScenarioAsync(
            IReadOnlyList<CloudflareTxtRecordInfo> initialRecords,
            bool shouldFailValidation,
            bool shouldFailFinalize,
            bool throwOnDelete,
            CancellationToken cancellationToken,
            bool failChallengeAfterCreate = false)
        {
            _ = shouldFailFinalize;
            string tempDir = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-AcmeDns01-{Guid.NewGuid():N}");
            _ = System.IO.Directory.CreateDirectory(tempDir);

            try
            {
                BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir);
                FakeCloudflareTxtRecordApi api = new(initialRecords, throwOnDelete);
                FakeAuthoritativeDnsTxtPropagationVerifier verifier = new();
                AcmeCertificateIssuer issuer = new(TimeProvider.System, NullLogger<AcmeCertificateIssuer>.Instance, verifier, _ => api);
                FakeAuthorizationContext authorizationContext = new(shouldFailValidation, failChallengeAfterCreate);
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
            /// Supplies  next id for the fixture or scenario under test.
            /// </summary>
            private int _nextId = 1000;

            /// <summary>
            /// Confirms the fake cloudflare txt record api behavior.
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
                CloudflareTxtRecordInfo record = new($"new-{_nextId++}", recordName, recordValue, CloudFlare.Client.Enumerators.DnsRecordType.Txt, false, 60, "BackFiller ACME", ["acme"], null, null);
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
            internal FakeAuthorizationContext(bool shouldFailValidation, bool failChallengeAfterCreate)
            {
                _authorization = new Authorization
                {
                    Identifier = new Identifier { Type = IdentifierType.Dns, Value = RecoveryScenario.Fqdn },
                    Status = AuthorizationStatus.Pending,
                    Challenges =
                    [
                        new Challenge
                        {
                            Type = ChallengeTypes.Dns01,
                            Token = RecoveryScenario.ChallengeToken,
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
            public string Token => RecoveryScenario.ChallengeToken;

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
