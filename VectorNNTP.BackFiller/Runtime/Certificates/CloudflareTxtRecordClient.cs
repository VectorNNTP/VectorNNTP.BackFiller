// <copyright file="CloudflareTxtRecordClient.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Manages Cloudflare TXT challenge records used by ACME DNS-01 validation.

using CloudFlare.Client;
using CloudFlare.Client.Api.Result;
using CloudFlare.Client.Api.Zones.DnsRecord;
using CloudFlare.Client.Contexts;
using CloudFlare.Client.Enumerators;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Creates and removes Cloudflare TXT records for ACME DNS-01 challenge validation.
    /// </summary>
    /// <remarks>
    /// The client only manages TXT challenge records, keeps proxied disabled, and uses a short TTL so challenge
    /// records are not confused with the application's A/AAAA reconciliation flow.
    /// </remarks>
    internal sealed class CloudflareTxtRecordClient : ICloudflareTxtRecordClient, IAsyncDisposable
    {
        private readonly CloudFlareClient _client;

        /// <summary>
        /// Initializes one TXT record client using the configured Cloudflare API token.
        /// </summary>
        /// <param name="apiToken">Cloudflare API token.</param>
        public CloudflareTxtRecordClient(string apiToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(apiToken);
            _client = new CloudFlareClient(apiToken.Trim(), new ConnectionInfo());
        }

        /// <inheritdoc/>
        public async Task<string> CreateTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordName);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordValue);

            NewDnsRecord newDnsRecord = new()
            {
                Name = recordName,
                Type = DnsRecordType.Txt,
                Content = recordValue,
                Proxied = false,
                Ttl = 60,
            };

            CloudFlareResult<DnsRecord> addResult = await _client.Zones.DnsRecords
                .AddAsync(zoneId, newDnsRecord, cancellationToken)
                .ConfigureAwait(false);

            return !addResult.Success || addResult.Result == null || string.IsNullOrWhiteSpace(addResult.Result.Id)
                ? throw new InvalidOperationException("Cloudflare TXT record create failed for ACME challenge.")
                : addResult.Result.Id;
        }

        /// <inheritdoc/>
        public async Task DeleteRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordId);

            CloudFlareResult<DnsRecord> deleteResult = await _client.Zones.DnsRecords
                .DeleteAsync(zoneId, recordId, cancellationToken)
                .ConfigureAwait(false);

            if (!deleteResult.Success)
            {
                throw new InvalidOperationException("Cloudflare TXT record delete failed for ACME challenge cleanup.");
            }
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
