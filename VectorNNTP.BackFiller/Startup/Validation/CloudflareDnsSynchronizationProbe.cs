// <copyright file="CloudflareDnsSynchronizationProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Startup Cloudflare DNS reconciliation for the authoritative BackFiller FQDN.

using System.Net;
using CloudFlare.Client;
using CloudFlare.Client.Api.Display;
using CloudFlare.Client.Api.Result;
using CloudFlare.Client.Api.Zones;
using CloudFlare.Client.Api.Zones.DnsRecord;
using CloudFlare.Client.Contexts;
using CloudFlare.Client.Enumerators;
using Serilog;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Executes startup-time reconciliation of Cloudflare address records for the generated canonical BackFiller FQDN.
    /// </summary>
    /// <remarks>
    /// <para>Compares desired canonical bind addresses with existing Cloudflare A/AAAA records and applies only the required add/delete operations.</para>
    /// <para>This probe contributes dependency diagnostics through <see cref="DependencyValidationResult"/> and does not decide startup exit behavior directly.</para>
    /// <para>The reconciliation path is idempotent: when desired and existing address records already match, no mutation calls are issued.</para>
    /// </remarks>
    internal static class CloudflareDnsSynchronizationProbe
    {
        /// <summary>
        /// Dependency category name used when reporting synchronization failures.
        /// </summary>
        private const string DependencyName = "CloudflareDnsSynchronization";

        /// <summary>
        /// Synchronizes Cloudflare A/AAAA records for the generated canonical BackFiller FQDN.
        /// </summary>
        /// <param name="backFiller">Validated BackFiller configuration model containing optional Cloudflare zone/token settings.</param>
        /// <param name="runtimeOptions">Validated immutable runtime snapshot containing the canonical FQDN and canonical bind-address set.</param>
        /// <param name="timeout">Maximum wall-clock budget for the full synchronization operation.</param>
        /// <param name="cancellationToken">Startup cancellation token propagated to all Cloudflare API calls.</param>
        /// <param name="dnsFacadeFactory">Optional factory used by tests to inject a custom DNS facade implementation.</param>
        /// <returns>
        /// A task that completes with a <see cref="DependencyValidationResult"/> describing synchronization success/failure.
        /// Missing Cloudflare zone/token configuration returns <see cref="DependencyValidationResult.Success()"/> and skips API calls.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="runtimeOptions"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is less than or equal to <see cref="TimeSpan.Zero"/>.</exception>
        /// <exception cref="OperationCanceledException">The outer <paramref name="cancellationToken"/> is canceled.</exception>
        /// <remarks>
        /// Emits structured informational logs for start, evaluated state, per-record add/delete operations, and completion counts;
        /// unexpected synchronization exceptions are logged as errors and converted into dependency-failure entries.
        /// </remarks>
        internal static async Task<DependencyValidationResult> SynchronizeGeneratedBackFillerDnsAsync(
            BackFillerOptions? backFiller,
            BackFillerRuntimeOptions runtimeOptions,
            TimeSpan timeout,
            CancellationToken cancellationToken,
            Func<string, ICloudflareDnsFacade>? dnsFacadeFactory = null)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

            List<(string Dependency, string Reason)> failures = [];
            List<(string Category, string Message)> warnings = [];
            List<(string Category, string Message)> errors = [];

            string? zoneId = backFiller?.LetsEncrypt?.CloudFlareZoneId;
            string? apiToken = backFiller?.LetsEncrypt?.CloudFlareApiToken;

            if (string.IsNullOrWhiteSpace(zoneId) || string.IsNullOrWhiteSpace(apiToken))
            {
                return DependencyValidationResult.Success();
            }

            string trimmedZoneId = zoneId.Trim();
            string canonicalFqdn = NormalizeDnsName(runtimeOptions.CanonicalBackFillerFqdn);

            HashSet<DnsAddressKey> desiredAddresses = BuildDesiredAddressKeys(runtimeOptions.EffectiveCanonicalBindAddresses);
            int desiredIpv4Count = desiredAddresses.Count(static key => key.RecordType == DnsRecordType.A);
            int desiredIpv6Count = desiredAddresses.Count(static key => key.RecordType == DnsRecordType.Aaaa);

            Log.Information(
                "CloudFlare DNS synchronization started for generated FQDN {Fqdn}; DesiredA={DesiredA}; DesiredAAAA={DesiredAAAA}",
                canonicalFqdn,
                desiredIpv4Count,
                desiredIpv6Count);

            try
            {
                using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(timeout);

                ICloudflareDnsFacade dnsFacade = dnsFacadeFactory is null
                    ? new CloudflareDnsFacade(apiToken.Trim())
                    : dnsFacadeFactory(apiToken.Trim());
                await using (dnsFacade.ConfigureAwait(false))
                {
                    CloudflareZoneInfo zone = await dnsFacade.GetZoneDetailsAsync(trimmedZoneId, cts.Token).ConfigureAwait(false);
                    if (zone.Status != ZoneStatus.Active)
                    {
                        throw new InvalidOperationException($"Cloudflare zone '{zone.Name}' is not active (status: {zone.Status}).");
                    }

                    string zoneName = NormalizeDnsName(zone.Name);
                    IReadOnlyList<CloudflareDnsRecordInfo> allFqdnRecords = await dnsFacade.GetDnsRecordsAsync(trimmedZoneId, canonicalFqdn, cts.Token).ConfigureAwait(false);

                    List<CloudflareDnsRecordInfo> existingAddressRecords = [.. allFqdnRecords
                        .Where(record => IsDnsAddressRecordType(record.RecordType) && string.Equals(NormalizeDnsName(record.Name), canonicalFqdn, StringComparison.Ordinal))];

                    int existingIpv4Count = existingAddressRecords.Count(static record => record.RecordType == DnsRecordType.A);
                    int existingIpv6Count = existingAddressRecords.Count(static record => record.RecordType == DnsRecordType.Aaaa);

                    Log.Information(
                        "CloudFlare DNS synchronization evaluated generated FQDN {Fqdn} in zone {ZoneName}; ExistingA={ExistingA}; ExistingAAAA={ExistingAAAA}",
                        canonicalFqdn,
                        zoneName,
                        existingIpv4Count,
                        existingIpv6Count);

                    Dictionary<DnsAddressKey, List<CloudflareDnsRecordInfo>> existingRecordsByAddress = BuildExistingRecordMap(existingAddressRecords);
                    Dictionary<DnsRecordType, CloudflareDnsRecordTemplate> recordTemplates = BuildRecordTemplates(existingAddressRecords);

                    List<CloudflareDnsRecordInfo> recordsToDelete = BuildDeletionList(existingRecordsByAddress, desiredAddresses);
                    List<DnsAddressKey> recordsToAdd = BuildAdditionList(existingRecordsByAddress, desiredAddresses);

                    if (recordsToDelete.Count > 0)
                    {
                        Task[] deleteTasks = [.. recordsToDelete
                            .OrderBy(static record => record.RecordType)
                            .ThenBy(static record => NormalizeAddressText(record.Content), StringComparer.Ordinal)
                            .ThenBy(static record => record.Id, StringComparer.Ordinal)
                            .Select(record => DeleteDnsRecordAsync(dnsFacade, trimmedZoneId, canonicalFqdn, record, cts.Token))];

                        await Task.WhenAll(deleteTasks).ConfigureAwait(false);
                    }

                    if (recordsToAdd.Count > 0)
                    {
                        Task[] addTasks = [.. recordsToAdd
                            .OrderBy(static key => key.RecordType)
                            .ThenBy(static key => NormalizeAddressText(key.Address.ToString()), StringComparer.Ordinal)
                            .Select(key => AddDnsRecordAsync(
                                dnsFacade,
                                trimmedZoneId,
                                canonicalFqdn,
                                key,
                                recordTemplates,
                                cts.Token))];

                        await Task.WhenAll(addTasks).ConfigureAwait(false);
                    }

                    Log.Information(
                        "CloudFlare DNS synchronization completed for generated FQDN {Fqdn}; AddedRecords={AddedRecords}; RemovedRecords={RemovedRecords}",
                        canonicalFqdn,
                        recordsToAdd.Count,
                        recordsToDelete.Count);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                failures.Add((DependencyName, $"Cloudflare DNS synchronization timed out after {timeout.TotalSeconds:F1}s"));
            }
            catch (Exception ex)
            {
                Log.Error(ex, "CloudFlare DNS synchronization failed for generated FQDN {Fqdn}", canonicalFqdn);
                failures.Add((DependencyName, $"Cloudflare DNS synchronization failed for generated FQDN '{canonicalFqdn}': {ex.Message}"));
            }

            return new DependencyValidationResult(failures, warnings, errors);
        }

        /// <summary>
        /// Adds one DNS address record and emits a structured add event.
        /// </summary>
        /// <param name="dnsFacade">Cloudflare DNS facade.</param>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="fqdn">Canonical FQDN being synchronized.</param>
        /// <param name="addressKey">Address key identifying record type and content.</param>
        /// <param name="recordTemplates">Per-type record templates used to preserve relevant record properties.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <remarks>
        /// When an existing record of the same type is present, its proxied and TTL settings are reused for the new record.
        /// </remarks>
        private static async Task AddDnsRecordAsync(
            ICloudflareDnsFacade dnsFacade,
            string zoneId,
            string fqdn,
            DnsAddressKey addressKey,
            Dictionary<DnsRecordType, CloudflareDnsRecordTemplate> recordTemplates,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(dnsFacade);
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);
            ArgumentNullException.ThrowIfNull(recordTemplates);

            bool? proxied = recordTemplates.TryGetValue(addressKey.RecordType, out CloudflareDnsRecordTemplate? template)
                ? template.Proxied
                : null;
            int? ttl = template?.Ttl;

            await dnsFacade.AddDnsRecordAsync(
                zoneId,
                fqdn,
                addressKey.RecordType,
                addressKey.Address,
                proxied,
                ttl,
                cancellationToken).ConfigureAwait(false);

            Log.Information(
                "CloudFlare DNS record added for generated FQDN {Fqdn}; RecordType={RecordType}; Address={Address}",
                fqdn,
                addressKey.RecordType,
                NormalizeAddressText(addressKey.Address.ToString()));
        }

        /// <summary>
        /// Deletes one DNS address record and emits a structured remove event.
        /// </summary>
        /// <param name="dnsFacade">Cloudflare DNS facade.</param>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="fqdn">Canonical FQDN being synchronized.</param>
        /// <param name="record">Cloudflare DNS record to remove.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private static async Task DeleteDnsRecordAsync(
            ICloudflareDnsFacade dnsFacade,
            string zoneId,
            string fqdn,
            CloudflareDnsRecordInfo record,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(dnsFacade);
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);
            ArgumentNullException.ThrowIfNull(record);

            await dnsFacade.DeleteDnsRecordAsync(zoneId, record.Id, cancellationToken).ConfigureAwait(false);
            Log.Information(
                "CloudFlare DNS record removed for generated FQDN {Fqdn}; RecordType={RecordType}; Address={Address}; RecordId={RecordId}",
                fqdn,
                record.RecordType,
                NormalizeAddressText(record.Content),
                record.Id);
        }

        /// <summary>
        /// Builds the deduplicated desired A/AAAA record set from canonical bind addresses.
        /// </summary>
        /// <param name="bindAddresses">Canonical runtime bind-address set.</param>
        /// <returns>Desired DNS-address key set keyed by record type plus parsed IP-address semantics.</returns>
        private static HashSet<DnsAddressKey> BuildDesiredAddressKeys(IReadOnlyList<IPAddress> bindAddresses)
        {
            ArgumentNullException.ThrowIfNull(bindAddresses);

            HashSet<DnsAddressKey> desiredAddresses = [];
            foreach (IPAddress bindAddress in bindAddresses)
            {
                if (bindAddress.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                {
                    _ = desiredAddresses.Add(new DnsAddressKey(DnsRecordType.A, bindAddress));
                }
                else if (bindAddress.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
                {
                    _ = desiredAddresses.Add(new DnsAddressKey(DnsRecordType.Aaaa, bindAddress));
                }
            }

            return desiredAddresses;
        }

        /// <summary>
        /// Builds a map of existing records keyed by parsed address semantics.
        /// </summary>
        /// <param name="existingAddressRecords">Existing Cloudflare A/AAAA records for the synchronized FQDN.</param>
        /// <returns>Map of address key to all existing records that resolve to that key.</returns>
        /// <remarks>
        /// Records whose content cannot be parsed into the expected address family are ignored because they cannot
        /// participate safely in semantic reconciliation.
        /// </remarks>
        private static Dictionary<DnsAddressKey, List<CloudflareDnsRecordInfo>> BuildExistingRecordMap(
            IReadOnlyList<CloudflareDnsRecordInfo> existingAddressRecords)
        {
            ArgumentNullException.ThrowIfNull(existingAddressRecords);

            Dictionary<DnsAddressKey, List<CloudflareDnsRecordInfo>> recordsByAddress = [];

            foreach (CloudflareDnsRecordInfo record in existingAddressRecords)
            {
                if (!TryBuildAddressKey(record.RecordType, record.Content, out DnsAddressKey key))
                {
                    continue;
                }

                if (!recordsByAddress.TryGetValue(key, out List<CloudflareDnsRecordInfo>? records))
                {
                    records = [];
                    recordsByAddress[key] = records;
                }

                records.Add(record);
            }

            return recordsByAddress;
        }

        /// <summary>
        /// Builds a per-type template map from existing records to preserve TTL and proxy behavior on added records.
        /// </summary>
        /// <param name="existingAddressRecords">Existing Cloudflare A/AAAA records for the synchronized FQDN.</param>
        /// <returns>Per-type template map.</returns>
        /// <remarks>
        /// The first existing record encountered for each type becomes the template for later additions of that type.
        /// </remarks>
        private static Dictionary<DnsRecordType, CloudflareDnsRecordTemplate> BuildRecordTemplates(
            IReadOnlyList<CloudflareDnsRecordInfo> existingAddressRecords)
        {
            ArgumentNullException.ThrowIfNull(existingAddressRecords);

            Dictionary<DnsRecordType, CloudflareDnsRecordTemplate> templates = [];
            foreach (CloudflareDnsRecordInfo record in existingAddressRecords)
            {
                if (!templates.ContainsKey(record.RecordType))
                {
                    templates[record.RecordType] = new CloudflareDnsRecordTemplate(record.Proxied, record.Ttl);
                }
            }

            return templates;
        }

        /// <summary>
        /// Calculates the exact set of existing records that must be deleted.
        /// </summary>
        /// <param name="existingRecordsByAddress">Existing records keyed by address semantics.</param>
        /// <param name="desiredAddresses">Desired canonical address keys.</param>
        /// <returns>Record deletion list.</returns>
        /// <remarks>
        /// When duplicates exist for one desired address, the lexicographically smallest record identifier is retained
        /// and the remaining duplicates are deleted.
        /// </remarks>
        private static List<CloudflareDnsRecordInfo> BuildDeletionList(
            IReadOnlyDictionary<DnsAddressKey, List<CloudflareDnsRecordInfo>> existingRecordsByAddress,
            IReadOnlySet<DnsAddressKey> desiredAddresses)
        {
            ArgumentNullException.ThrowIfNull(existingRecordsByAddress);
            ArgumentNullException.ThrowIfNull(desiredAddresses);

            List<CloudflareDnsRecordInfo> recordsToDelete = [];
            foreach (KeyValuePair<DnsAddressKey, List<CloudflareDnsRecordInfo>> pair in existingRecordsByAddress)
            {
                List<CloudflareDnsRecordInfo> records = pair.Value;
                if (records.Count == 0)
                {
                    continue;
                }

                if (!desiredAddresses.Contains(pair.Key))
                {
                    recordsToDelete.AddRange(records);
                    continue;
                }

                if (records.Count > 1)
                {
                    IEnumerable<CloudflareDnsRecordInfo> duplicateRecords = records
                        .OrderBy(static record => record.Id, StringComparer.Ordinal)
                        .Skip(1);
                    recordsToDelete.AddRange(duplicateRecords);
                }
            }

            return recordsToDelete;
        }

        /// <summary>
        /// Calculates the exact set of desired addresses that are currently missing from existing records.
        /// </summary>
        /// <param name="existingRecordsByAddress">Existing records keyed by address semantics.</param>
        /// <param name="desiredAddresses">Desired canonical address keys.</param>
        /// <returns>Address keys requiring record creation.</returns>
        /// <remarks>
        /// Desired addresses that already have at least one semantically equivalent record are not re-created.
        /// </remarks>
        private static List<DnsAddressKey> BuildAdditionList(
            IReadOnlyDictionary<DnsAddressKey, List<CloudflareDnsRecordInfo>> existingRecordsByAddress,
            IReadOnlySet<DnsAddressKey> desiredAddresses)
        {
            ArgumentNullException.ThrowIfNull(existingRecordsByAddress);
            ArgumentNullException.ThrowIfNull(desiredAddresses);

            List<DnsAddressKey> additions = [];
            foreach (DnsAddressKey desiredAddress in desiredAddresses)
            {
                if (!existingRecordsByAddress.ContainsKey(desiredAddress))
                {
                    additions.Add(desiredAddress);
                }
            }

            return additions;
        }

        /// <summary>
        /// Builds one address key from one Cloudflare record-type/content pair.
        /// </summary>
        /// <param name="recordType">Cloudflare record type.</param>
        /// <param name="content">Cloudflare DNS content string.</param>
        /// <param name="key">Built key when parse succeeds.</param>
        /// <returns><see langword="true"/> when a valid A/AAAA key could be created.</returns>
        /// <remarks>
        /// The parsed IP address must match the supplied DNS record type's address family.
        /// </remarks>
        private static bool TryBuildAddressKey(
            DnsRecordType recordType,
            string content,
            out DnsAddressKey key)
        {
            key = default;

            if (!IsDnsAddressRecordType(recordType) ||
                string.IsNullOrWhiteSpace(content) ||
                !IPAddress.TryParse(content, out IPAddress? address))
            {
                return false;
            }

            if (recordType == DnsRecordType.A && address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
            {
                return false;
            }

            if (recordType == DnsRecordType.Aaaa && address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
            {
                return false;
            }

            key = new DnsAddressKey(recordType, address);
            return true;
        }

        /// <summary>
        /// Determines whether a DNS record type is one of the synchronized address types.
        /// </summary>
        /// <param name="recordType">Record type to evaluate.</param>
        /// <returns><see langword="true"/> when the record type is A or AAAA.</returns>
        private static bool IsDnsAddressRecordType(DnsRecordType recordType)
        {
            return recordType is DnsRecordType.A or DnsRecordType.Aaaa;
        }

        /// <summary>
        /// Normalizes DNS host labels for deterministic host-name equality comparisons.
        /// </summary>
        /// <param name="name">DNS host name.</param>
        /// <returns>Canonicalized host name without trailing dot.</returns>
        private static string NormalizeDnsName(string name)
        {
            return name.Trim().TrimEnd('.').ToLowerInvariant();
        }

        /// <summary>
        /// Normalizes IP-address text for structured logging consistency.
        /// </summary>
        /// <param name="addressText">Address text to normalize.</param>
        /// <returns>Canonical textual IP-address representation when parse succeeds; otherwise trimmed original text.</returns>
        private static string NormalizeAddressText(string addressText)
        {
            return IPAddress.TryParse(addressText, out IPAddress? parsedAddress)
                ? parsedAddress.ToString()
                : addressText.Trim();
        }

        /// <summary>
        /// Immutable DNS-address comparison key using record type plus parsed IP-address semantics.
        /// </summary>
        /// <param name="RecordType">Cloudflare DNS record type.</param>
        /// <param name="Address">Parsed IP address value.</param>
        private readonly record struct DnsAddressKey(DnsRecordType RecordType, IPAddress Address);
    }

    /// <summary>
    /// Immutable Cloudflare zone details used during startup synchronization.
    /// </summary>
    /// <param name="Id">Cloudflare zone identifier.</param>
    /// <param name="Name">Cloudflare zone DNS name.</param>
    /// <param name="Status">Cloudflare zone lifecycle status.</param>
    internal sealed record CloudflareZoneInfo(string Id, string Name, ZoneStatus Status);

    /// <summary>
    /// Immutable Cloudflare DNS record projection used by synchronization logic.
    /// </summary>
    /// <param name="Id">Cloudflare DNS record identifier.</param>
    /// <param name="Name">Record host name.</param>
    /// <param name="RecordType">Record DNS type.</param>
    /// <param name="Content">Record address content.</param>
    /// <param name="Proxied">Cloudflare proxy mode setting.</param>
    /// <param name="Ttl">DNS TTL setting.</param>
    internal sealed record CloudflareDnsRecordInfo(
        string Id,
        string Name,
        DnsRecordType RecordType,
        string Content,
        bool? Proxied,
        int? Ttl);

    /// <summary>
    /// Immutable Cloudflare DNS-record property template reused when creating new address records.
    /// </summary>
    /// <param name="Proxied">Cloudflare proxy mode to preserve.</param>
    /// <param name="Ttl">TTL to preserve.</param>
    internal sealed record CloudflareDnsRecordTemplate(bool? Proxied, int? Ttl);

    /// <summary>
    /// Abstraction over Cloudflare zone and DNS-record operations required by startup synchronization.
    /// </summary>
    /// <remarks>
    /// Enables deterministic tests for reconciliation logic by decoupling API transport details from synchronization policy.
    /// Implementations are expected to preserve Cloudflare API cancellation and to dispose any underlying transport resources.
    /// </remarks>
    internal interface ICloudflareDnsFacade : IAsyncDisposable
    {
        /// <summary>
        /// Retrieves Cloudflare zone details for a specific zone identifier.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="cancellationToken">Cancellation token for the API request.</param>
        /// <returns>A task that completes with resolved zone details used by synchronization.</returns>
        public Task<CloudflareZoneInfo> GetZoneDetailsAsync(string zoneId, CancellationToken cancellationToken);

        /// <summary>
        /// Retrieves DNS records for one synchronized FQDN.
        /// </summary>
        /// <param name="zoneId">Zone identifier.</param>
        /// <param name="fqdn">FQDN to query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Retrieved Cloudflare records.</returns>
        public Task<IReadOnlyList<CloudflareDnsRecordInfo>> GetDnsRecordsAsync(string zoneId, string fqdn, CancellationToken cancellationToken);

        /// <summary>
        /// Creates one DNS address record.
        /// </summary>
        /// <param name="zoneId">Zone identifier.</param>
        /// <param name="fqdn">Record host name.</param>
        /// <param name="recordType">Record type (A or AAAA).</param>
        /// <param name="address">Record IP address content.</param>
        /// <param name="proxied">Optional proxied mode.</param>
        /// <param name="ttl">Optional DNS TTL.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task AddDnsRecordAsync(
            string zoneId,
            string fqdn,
            DnsRecordType recordType,
            IPAddress address,
            bool? proxied,
            int? ttl,
            CancellationToken cancellationToken);

        /// <summary>
        /// Deletes one DNS record by identifier.
        /// </summary>
        /// <param name="zoneId">Zone identifier.</param>
        /// <param name="recordId">Record identifier.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task DeleteDnsRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Production implementation of <see cref="ICloudflareDnsFacade"/> backed by <see cref="CloudFlareClient"/>.
    /// </summary>
    /// <remarks>
    /// Maps Cloudflare API responses to synchronization models and converts provider error payloads to sanitized failure text.
    /// The underlying client is owned by the facade and released through <see cref="DisposeAsync"/>.
    /// </remarks>
    internal sealed class CloudflareDnsFacade : ICloudflareDnsFacade
    {
        /// <summary>
        /// Cloudflare API client used for zone and DNS-record operations.
        /// </summary>
        private readonly CloudFlareClient _client;

        /// <summary>
        /// Initializes the facade with one Cloudflare API token.
        /// </summary>
        /// <param name="apiToken">Cloudflare API token.</param>
        /// <exception cref="ArgumentException"><paramref name="apiToken"/> is <see langword="null"/>, empty, or whitespace.</exception>
        public CloudflareDnsFacade(string apiToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(apiToken);
            _client = new CloudFlareClient(apiToken.Trim(), new ConnectionInfo());
        }

        /// <summary>
        /// Resolves Cloudflare zone details for the supplied zone identifier.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="cancellationToken">Cancellation token for the Cloudflare API call.</param>
        /// <returns>A task that completes with normalized zone information used by synchronization.</returns>
        /// <exception cref="ArgumentException"><paramref name="zoneId"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="InvalidOperationException">Cloudflare returns an unsuccessful or structurally invalid zone response.</exception>
        public async Task<CloudflareZoneInfo> GetZoneDetailsAsync(string zoneId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);

            CloudFlareResult<Zone> zoneResult = await _client.Zones.GetDetailsAsync(zoneId, cancellationToken).ConfigureAwait(false);
            return !zoneResult.Success || zoneResult.Result == null
                ? throw new InvalidOperationException($"Cloudflare zone resolution failed: {SanitizeCloudflareApiFailureReason(zoneResult.Errors.Select(static error => error.Message))}")
                : new CloudflareZoneInfo(zoneResult.Result.Id, zoneResult.Result.Name ?? string.Empty, zoneResult.Result.Status);
        }

        /// <summary>
        /// Retrieves DNS records for one synchronized FQDN.
        /// </summary>
        /// <param name="zoneId">Zone identifier.</param>
        /// <param name="fqdn">FQDN to query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Retrieved Cloudflare records.</returns>
        /// <exception cref="ArgumentException"><paramref name="zoneId"/> or <paramref name="fqdn"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="InvalidOperationException">Cloudflare returns an unsuccessful or structurally invalid DNS-record response.</exception>
        public async Task<IReadOnlyList<CloudflareDnsRecordInfo>> GetDnsRecordsAsync(string zoneId, string fqdn, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);

            DnsRecordFilter recordFilter = new()
            {
                Name = fqdn,
            };

            DisplayOptions displayOptions = new()
            {
                PerPage = 5000,
            };

            CloudFlareResult<IReadOnlyList<DnsRecord>> recordsResult = await _client.Zones.DnsRecords
                .GetAsync(zoneId, recordFilter, displayOptions, cancellationToken)
                .ConfigureAwait(false);

            return !recordsResult.Success || recordsResult.Result == null
                ? throw new InvalidOperationException($"Cloudflare DNS record query failed: {SanitizeCloudflareApiFailureReason(recordsResult.Errors.Select(static error => error.Message))}")
                : [.. recordsResult.Result.Select(static record =>
                new CloudflareDnsRecordInfo(
                    record.Id ?? string.Empty,
                    record.Name ?? string.Empty,
                    record.Type,
                    record.Content ?? string.Empty,
                    record.Proxied,
                    record.Ttl))];
        }

        /// <summary>
        /// Creates one DNS address record.
        /// </summary>
        /// <param name="zoneId">Zone identifier.</param>
        /// <param name="fqdn">Record host name.</param>
        /// <param name="recordType">Record type (A or AAAA).</param>
        /// <param name="address">Record IP address content.</param>
        /// <param name="proxied">Optional proxied mode.</param>
        /// <param name="ttl">Optional DNS TTL.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentException"><paramref name="zoneId"/> or <paramref name="fqdn"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="address"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Cloudflare reports that record creation was unsuccessful.</exception>
        public async Task AddDnsRecordAsync(
            string zoneId,
            string fqdn,
            DnsRecordType recordType,
            IPAddress address,
            bool? proxied,
            int? ttl,
            CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);
            ArgumentNullException.ThrowIfNull(address);

            NewDnsRecord newDnsRecord = new()
            {
                Name = fqdn,
                Type = recordType,
                Content = address.ToString(),
                Proxied = proxied,
                Ttl = ttl,
            };

            CloudFlareResult<DnsRecord> addResult = await _client.Zones.DnsRecords
                .AddAsync(zoneId, newDnsRecord, cancellationToken)
                .ConfigureAwait(false);

            if (!addResult.Success)
            {
                throw new InvalidOperationException($"Cloudflare DNS record create failed: {SanitizeCloudflareApiFailureReason(addResult.Errors.Select(static error => error.Message))}");
            }
        }

        /// <summary>
        /// Deletes one DNS record by identifier.
        /// </summary>
        /// <param name="zoneId">Zone identifier.</param>
        /// <param name="recordId">Record identifier.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentException"><paramref name="zoneId"/> or <paramref name="recordId"/> is <see langword="null"/>, empty, or whitespace.</exception>
        /// <exception cref="InvalidOperationException">Cloudflare reports that record deletion was unsuccessful.</exception>
        public async Task DeleteDnsRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordId);

            CloudFlareResult<DnsRecord> deleteResult = await _client.Zones.DnsRecords
                .DeleteAsync(zoneId, recordId, cancellationToken)
                .ConfigureAwait(false);

            if (!deleteResult.Success)
            {
                throw new InvalidOperationException($"Cloudflare DNS record delete failed: {SanitizeCloudflareApiFailureReason(deleteResult.Errors.Select(static error => error.Message))}");
            }
        }

        /// <summary>
        /// Disposes the owned Cloudflare client.
        /// </summary>
        /// <returns>A completed asynchronous dispose operation wrapping synchronous client disposal.</returns>
        public ValueTask DisposeAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Maps provider-reported Cloudflare error text to a bounded sanitized failure reason.
        /// </summary>
        /// <param name="providerMessages">Cloudflare provider error messages.</param>
        /// <returns>A controlled diagnostic reason suitable for startup failure output.</returns>
        /// <remarks>
        /// Keyword matching intentionally collapses broad provider payloads into stable categories
        /// (authentication, access denied, zone not found, or generic API failure).
        /// </remarks>
        private static string SanitizeCloudflareApiFailureReason(IEnumerable<string> providerMessages)
        {
            ArgumentNullException.ThrowIfNull(providerMessages);

            string combinedMessage = string.Join("; ", providerMessages.Where(static message => !string.IsNullOrWhiteSpace(message))).ToLowerInvariant();
            return string.IsNullOrWhiteSpace(combinedMessage)
                ? "Invalid response"
                : combinedMessage.Contains("unauthorized", StringComparison.Ordinal) ||
                combinedMessage.Contains("authentication", StringComparison.Ordinal) ||
                combinedMessage.Contains("api token", StringComparison.Ordinal) ||
                combinedMessage.Contains("invalid token", StringComparison.Ordinal)
                ? "Authentication failed"
                : combinedMessage.Contains("forbidden", StringComparison.Ordinal) ||
                combinedMessage.Contains("access denied", StringComparison.Ordinal) ||
                combinedMessage.Contains("permission", StringComparison.Ordinal)
                ? "Access denied"
                : combinedMessage.Contains("not found", StringComparison.Ordinal) ||
                combinedMessage.Contains("zone not found", StringComparison.Ordinal) ||
                combinedMessage.Contains("invalid zone", StringComparison.Ordinal)
                ? "Zone not found"
                : "API request failed";
        }
    }
}
