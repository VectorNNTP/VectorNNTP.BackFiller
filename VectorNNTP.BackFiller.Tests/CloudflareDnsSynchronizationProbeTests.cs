// <copyright file="CloudflareDnsSynchronizationProbeTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for cloudflare dns synchronization probe, covering certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the cloudflare dns synchronization probe test suite.

using System.Net;
using CloudFlare.Client.Enumerators;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests startup Cloudflare DNS synchronization for generated BackFiller FQDN bind-address reconciliation.
    /// </summary>
    public sealed class CloudflareDnsSynchronizationProbeTests
    {
        /// <summary>
        /// Supplies generated fqdn for the fixture or scenario under test.
        /// </summary>
        private const string GeneratedFqdn = "nntpbackfiller01.usenet.ninja";

        /// <summary>
        /// Verifies IPv4 reconciliation keeps matching records, creates missing records, and removes obsolete records.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenIpv4RecordsDiffer_ReconcilesExactARecordSet()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("a-match", DnsRecordType.A, "192.0.2.10"),
                    CreateRecord("a-obsolete", DnsRecordType.A, "192.0.2.99"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("192.0.2.10", "192.0.2.11");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(result.IsValid);
            Assert.Equal(1, facade.AddCallCount);
            Assert.Equal(1, facade.DeleteCallCount);
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.A && record.Content == "192.0.2.10");
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.A && record.Content == "192.0.2.11");
            Assert.DoesNotContain(facade.Records, record => record.Content == "192.0.2.99");
        }

        /// <summary>
        /// Verifies IPv6 reconciliation keeps matching records, creates missing records, and removes obsolete records.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenIpv6RecordsDiffer_ReconcilesExactAaaaRecordSet()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("aaaa-match", DnsRecordType.Aaaa, "2001:db8::10"),
                    CreateRecord("aaaa-obsolete", DnsRecordType.Aaaa, "2001:db8::99"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("2001:db8::10", "2001:db8::11");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(result.IsValid);
            Assert.Equal(1, facade.AddCallCount);
            Assert.Equal(1, facade.DeleteCallCount);
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.Aaaa && record.Content == "2001:db8::10");
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.Aaaa && record.Content == "2001:db8::11");
            Assert.DoesNotContain(facade.Records, record => record.Content == "2001:db8::99");
        }

        /// <summary>
        /// Verifies mixed IPv4/IPv6 reconciliation updates both A and AAAA records in one synchronization run.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenMixedAddressFamiliesConfigured_ReconcilesAAndAaaa()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("a-old", DnsRecordType.A, "198.51.100.20"),
                    CreateRecord("aaaa-old", DnsRecordType.Aaaa, "2001:db8::20"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("198.51.100.10", "2001:db8::10");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(result.IsValid);
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.A && record.Content == "198.51.100.10");
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.Aaaa && record.Content == "2001:db8::10");
            Assert.DoesNotContain(facade.Records, record => record.Content == "198.51.100.20");
            Assert.DoesNotContain(facade.Records, record => record.Content == "2001:db8::20");
        }

        /// <summary>
        /// Verifies full-set synchronization removes obsolete records and creates missing records so final state is exact.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenDesiredSetDiffers_ComputesExactFinalState()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("a1", DnsRecordType.A, "198.51.100.1"),
                    CreateRecord("a2", DnsRecordType.A, "198.51.100.2"),
                    CreateRecord("aaaa1", DnsRecordType.Aaaa, "2001:db8::1"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("198.51.100.1", "2001:db8::2");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(result.IsValid);
            Assert.Equal(2, facade.DeleteCallCount);
            Assert.Equal(1, facade.AddCallCount);
            Assert.Equal(
                ["198.51.100.1", "2001:db8::2"],
                [.. facade.Records
                    .Where(static record => record.RecordType is DnsRecordType.A or DnsRecordType.Aaaa)
                    .Select(static record => IPAddress.Parse(record.Content).ToString())
                    .OrderBy(static address => address, StringComparer.Ordinal)]);
        }

        /// <summary>
        /// Verifies duplicate A/AAAA records for equivalent addresses are reduced to one canonical record.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenDuplicateRecordsExist_RemovesDuplicates()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("dup1", DnsRecordType.A, "192.0.2.10"),
                    CreateRecord("dup2", DnsRecordType.A, "192.0.2.10"),
                    CreateRecord("dup3", DnsRecordType.Aaaa, "2001:db8::10"),
                    CreateRecord("dup4", DnsRecordType.Aaaa, "2001:0db8:0:0:0:0:0:10"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("192.0.2.10", "2001:db8::10");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(result.IsValid);
            Assert.Equal(2, facade.DeleteCallCount);
            _ = Assert.Single(facade.Records, static record => record.RecordType == DnsRecordType.A);
            _ = Assert.Single(facade.Records, static record => record.RecordType == DnsRecordType.Aaaa);
        }

        /// <summary>
        /// Verifies synchronization is idempotent and emits no create/delete operations on the second run with unchanged state.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenRunTwiceWithSameState_IsIdempotent()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("idemp-a", DnsRecordType.A, "203.0.113.10"),
                    CreateRecord("idemp-aaaa", DnsRecordType.Aaaa, "2001:db8::10"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("203.0.113.10", "2001:db8::10");

            DependencyValidationResult firstRun = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);
            int addCallsAfterFirstRun = facade.AddCallCount;
            int deleteCallsAfterFirstRun = facade.DeleteCallCount;

            DependencyValidationResult secondRun = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(firstRun.IsValid);
            Assert.True(secondRun.IsValid);
            Assert.Equal(addCallsAfterFirstRun, facade.AddCallCount);
            Assert.Equal(deleteCallsAfterFirstRun, facade.DeleteCallCount);
        }

        /// <summary>
        /// Verifies non-address DNS records remain untouched during A/AAAA reconciliation.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenUnrelatedRecordTypesExist_LeavesThemUntouched()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("txt1", DnsRecordType.Txt, "challenge-token"),
                    CreateRecord("cname1", DnsRecordType.Cname, "other.example.com"),
                    CreateRecord("a-old", DnsRecordType.A, "198.51.100.99"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("198.51.100.10");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(result.IsValid);
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.Txt && record.Content == "challenge-token");
            Assert.Contains(facade.Records, record => record.RecordType == DnsRecordType.Cname && record.Content == "other.example.com");
            Assert.DoesNotContain(facade.DeletedRecordIds, static recordId => recordId is "txt1" or "cname1");
        }

        /// <summary>
        /// Verifies equivalent IPv6 textual representations compare by address semantics and do not trigger churn.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenIpv6RepresentationsDifferTextually_TreatsAsEquivalent()
        {
            FakeCloudflareDnsFacade facade = new(
                [
                    CreateRecord("v6-canonical", DnsRecordType.Aaaa, "2001:db8::1"),
                ]);

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("2001:0db8:0:0:0:0:0:1");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.True(result.IsValid);
            Assert.Equal(0, facade.AddCallCount);
            Assert.Equal(0, facade.DeleteCallCount);
        }

        /// <summary>
        /// Verifies cancellation propagates as cancellation instead of being converted into a synchronization failure.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenCancelled_PropagatesOperationCanceledException()
        {
            FakeCloudflareDnsFacade facade = new([])
            {
                ThrowCancellationFromZoneLookup = true,
            };

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("198.51.100.10");

            using CancellationTokenSource cancellationTokenSource = new();
            cancellationTokenSource.Cancel();

            _ = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await RunSynchronizationAsync(runtimeOptions, facade, cancellationTokenSource.Token));
        }

        /// <summary>
        /// Verifies Cloudflare API failure is surfaced as dependency failure so startup can fail with existing semantics.
        /// </summary>
        [Fact]
        public async Task SynchronizeGeneratedBackFillerDnsAsync_WhenCloudflareFacadeThrows_ReturnsDependencyFailure()
        {
            FakeCloudflareDnsFacade facade = new([])
            {
                ThrowFailureFromDnsQuery = true,
            };

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions("198.51.100.10");

            DependencyValidationResult result = await RunSynchronizationAsync(runtimeOptions, facade, CancellationToken.None);

            Assert.False(result.IsValid);
            Assert.Contains(result.FailedDependencies, static failure =>
                failure.Dependency == "CloudflareDnsSynchronization" &&
                failure.Reason.Contains("Cloudflare DNS synchronization failed", StringComparison.Ordinal));
        }

        /// <summary>
        /// Executes DNS synchronization against a fake Cloudflare facade.
        /// </summary>
        /// <param name="runtimeOptions">Runtime options containing canonical FQDN and bind addresses.</param>
        /// <param name="facade">Fake facade representing Cloudflare state.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Synchronization validation result.</returns>
        private static Task<DependencyValidationResult> RunSynchronizationAsync(
            BackFillerRuntimeOptions runtimeOptions,
            FakeCloudflareDnsFacade facade,
            CancellationToken cancellationToken)
        {
            return CloudflareDnsSynchronizationProbe.SynchronizeGeneratedBackFillerDnsAsync(
                CreateBackFillerOptions(),
                runtimeOptions,
                TimeSpan.FromSeconds(10),
                cancellationToken,
                _ => facade);
        }

        /// <summary>
        /// Creates baseline BackFiller options containing required Cloudflare settings for synchronization tests.
        /// </summary>
        /// <returns>BackFiller options fixture.</returns>
        private static BackFillerOptions CreateBackFillerOptions()
        {
            return new BackFillerOptions
            {
                LetsEncrypt = new LetsEncryptOptions
                {
                    CloudFlareApiToken = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                    CloudFlareZoneId = "5811a29d39a0732afb5f160c9b137c3d",
                },
            };
        }

        /// <summary>
        /// Creates runtime options fixture with canonical FQDN and canonical bind addresses.
        /// </summary>
        /// <param name="bindAddresses">Bind addresses included in desired DNS state.</param>
        /// <returns>Runtime options fixture.</returns>
        private static BackFillerRuntimeOptions CreateRuntimeOptions(params string[] bindAddresses)
        {
            IReadOnlyList<IPAddress> canonicalBindAddresses = [.. bindAddresses.Select(IPAddress.Parse)];

            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: GeneratedFqdn,
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
                ValidatedLogDirectory: "C:\\logs",
                ValidatedCertificateDirectory: "C:\\certs",
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: true,
                TransitServerHost: "127.0.0.1",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 119,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 30,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                CanonicalBindAddresses: canonicalBindAddresses);
        }

        /// <summary>
        /// Creates one Cloudflare DNS record fixture.
        /// </summary>
        /// <param name="id">Record identifier.</param>
        /// <param name="recordType">Record type.</param>
        /// <param name="content">Record content.</param>
        /// <param name="name">Optional record host name; defaults to synchronized generated FQDN.</param>
        /// <returns>Cloudflare DNS record fixture.</returns>
        private static CloudflareDnsRecordInfo CreateRecord(string id, DnsRecordType recordType, string content, string? name = null)
        {
            return new CloudflareDnsRecordInfo(id, name ?? GeneratedFqdn, recordType, content, Proxied: false, Ttl: 120);
        }

        /// <summary>
        /// In-memory Cloudflare facade used for deterministic synchronization tests.
        /// </summary>
        private sealed class FakeCloudflareDnsFacade : ICloudflareDnsFacade
        {
            /// <summary>
            /// Supplies  records for the fixture or scenario under test.
            /// </summary>
            private readonly List<CloudflareDnsRecordInfo> _records;
            /// <summary>
            /// Supplies  next identifier for the fixture or scenario under test.
            /// </summary>
            private int _nextIdentifier = 1000;

            /// <summary>
            /// Initializes the fake facade with existing records.
            /// </summary>
            /// <param name="records">Initial Cloudflare record state.</param>
            internal FakeCloudflareDnsFacade(IEnumerable<CloudflareDnsRecordInfo> records)
            {
                _records = [.. records];
            }

            /// <summary>
            /// Gets the mutable in-memory Cloudflare record state.
            /// </summary>
            internal IReadOnlyList<CloudflareDnsRecordInfo> Records => _records;

            /// <summary>
            /// Gets the count of add operations invoked.
            /// </summary>
            internal int AddCallCount { get; private set; }

            /// <summary>
            /// Gets the count of delete operations invoked.
            /// </summary>
            internal int DeleteCallCount { get; private set; }

            /// <summary>
            /// Gets the deleted record identifier list.
            /// </summary>
            internal List<string> DeletedRecordIds { get; } = [];

            /// <summary>
            /// Gets or sets a value indicating whether zone lookup should throw cancellation.
            /// </summary>
            internal bool ThrowCancellationFromZoneLookup { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether DNS query should throw a Cloudflare failure.
            /// </summary>
            internal bool ThrowFailureFromDnsQuery { get; set; }

            /// <summary>
            /// Gets zone details from fake state.
            /// </summary>
            /// <param name="zoneId">Zone identifier.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>Fake zone information.</returns>
            public Task<CloudflareZoneInfo> GetZoneDetailsAsync(string zoneId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                return ThrowCancellationFromZoneLookup
                    ? throw new OperationCanceledException(cancellationToken)
                    : Task.FromResult(new CloudflareZoneInfo(zoneId, "usenet.ninja", ZoneStatus.Active));
            }

            /// <summary>
            /// Gets fake DNS records.
            /// </summary>
            /// <param name="zoneId">Zone identifier.</param>
            /// <param name="fqdn">Queried FQDN.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>Current fake record snapshot.</returns>
            public Task<IReadOnlyList<CloudflareDnsRecordInfo>> GetDnsRecordsAsync(string zoneId, string fqdn, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                return ThrowFailureFromDnsQuery
                    ? throw new InvalidOperationException("API request failed")
                    : Task.FromResult<IReadOnlyList<CloudflareDnsRecordInfo>>([.. _records]);
            }

            /// <summary>
            /// Adds one DNS record to fake state.
            /// </summary>
            /// <param name="zoneId">Zone identifier.</param>
            /// <param name="fqdn">Record name.</param>
            /// <param name="recordType">Record type.</param>
            /// <param name="address">Record address.</param>
            /// <param name="proxied">Proxied mode.</param>
            /// <param name="ttl">Record TTL.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            public Task AddDnsRecordAsync(
                string zoneId,
                string fqdn,
                DnsRecordType recordType,
                IPAddress address,
                bool? proxied,
                int? ttl,
                CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                AddCallCount++;
                _records.Add(new CloudflareDnsRecordInfo(
                    Id: $"new-{_nextIdentifier++}",
                    Name: fqdn,
                    RecordType: recordType,
                    Content: address.ToString(),
                    Proxied: proxied,
                    Ttl: ttl));

                return Task.CompletedTask;
            }

            /// <summary>
            /// Deletes one DNS record from fake state.
            /// </summary>
            /// <param name="zoneId">Zone identifier.</param>
            /// <param name="recordId">Record identifier.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            public Task DeleteDnsRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                DeleteCallCount++;
                DeletedRecordIds.Add(recordId);
                _ = _records.RemoveAll(record => string.Equals(record.Id, recordId, StringComparison.Ordinal));
                return Task.CompletedTask;
            }

            /// <summary>
            /// Disposes fake resources.
            /// </summary>
            /// <returns>A completed value task.</returns>
            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
