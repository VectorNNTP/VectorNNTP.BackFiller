// <copyright file="CloudflareTxtRecordClient.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Legacy adapter retained for compatibility; use CloudflareTxtRecordApi for ownership-aware DNS-01 lifecycles.

using System.Text;
using CloudFlare.Client;
using CloudFlare.Client.Api.Display;
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
        /// <summary>
        /// Underlying Cloudflare client used for TXT-record list, create, and delete operations.
        /// </summary>
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
        public async Task<IReadOnlyList<CloudflareTxtRecordInfo>> GetTxtRecordsAsync(string zoneId, string recordName, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordName);

            DnsRecordFilter recordFilter = new()
            {
                Type = DnsRecordType.Txt,
                Name = recordName,
                Match = CloudFlare.Client.Enumerators.MatchType.All,
            };

            DisplayOptions displayOptions = new()
            {
                PerPage = 5000,
            };

            CloudFlareResult<IReadOnlyList<DnsRecord>> queryResult;
            try
            {
                queryResult = await _client.Zones.DnsRecords
                    .GetAsync(zoneId, recordFilter, displayOptions, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    BuildQueryFailureMessage(zoneId, recordName, null, null, null, null),
                    ex);
            }

            return !queryResult.Success || queryResult.Result is null
                ? throw new InvalidOperationException(
                    BuildQueryFailureMessage(zoneId, recordName, queryResult.Success, queryResult.Messages, queryResult.Errors, queryResult.Timing))
                : [.. queryResult.Result.Select(static record => new CloudflareTxtRecordInfo(
                Id: record.Id ?? string.Empty,
                Name: record.Name ?? string.Empty,
                Content: record.Content ?? string.Empty,
                Type: record.Type,
                Proxied: record.Proxied,
                Ttl: record.Ttl,
                Comment: record.Comment,
                Tags: record.Tags is null ? [] : [.. record.Tags],
                CreatedDateUtc: record.CreatedDate,
                ModifiedDateUtc: record.ModifiedDate))];
        }

        /// <summary>
        /// Creates a TXT record for ACME validation, or reuses an existing record with the same value.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordName">Fully qualified TXT record host name.</param>
        /// <param name="recordValue">TXT value required by the ACME challenge.</param>
        /// <param name="cancellationToken">Cancellation token propagated to the provider operations.</param>
        /// <returns>The Cloudflare identifier of the created or reused TXT record.</returns>
        public async Task<string> CreateTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordName);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordValue);

            IReadOnlyList<CloudflareTxtRecordInfo> existingRecords = await GetTxtRecordsAsync(zoneId, recordName, cancellationToken).ConfigureAwait(false);
            CloudflareTxtRecordInfo? ownedRecord = null;
            for (int i = 0; i < existingRecords.Count; i++)
            {
                CloudflareTxtRecordInfo record = existingRecords[i];
                if (string.Equals(record.Content, recordValue, StringComparison.Ordinal))
                {
                    ownedRecord = record;
                    break;
                }
            }

            if (ownedRecord is not null)
            {
                return ownedRecord.Id;
            }

            NewDnsRecord newDnsRecord = new()
            {
                Name = recordName,
                Type = DnsRecordType.Txt,
                Content = recordValue,
                Proxied = false,
                Ttl = 60,
            };

            CloudFlareResult<DnsRecord> addResult;
            try
            {
                addResult = await _client.Zones.DnsRecords
                    .AddAsync(zoneId, newDnsRecord, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    BuildCreateFailureMessage(recordName, DnsRecordType.Txt, null, null, null, null),
                    ex);
            }

            return addResult.Success && addResult.Result is not null && !string.IsNullOrWhiteSpace(addResult.Result.Id)
                ? addResult.Result.Id
                : throw new InvalidOperationException(
                BuildCreateFailureMessage(recordName, DnsRecordType.Txt, addResult.Success, addResult.Messages, addResult.Errors, addResult.Timing));
        }

        /// <inheritdoc/>
        public async Task DeleteRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordId);

            CloudFlareResult<DnsRecord> deleteResult;
            try
            {
                deleteResult = await _client.Zones.DnsRecords
                    .DeleteAsync(zoneId, recordId, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    BuildDeleteFailureMessage(recordId, null, null, null, null),
                    ex);
            }

            if (!deleteResult.Success)
            {
                throw new InvalidOperationException(
                    BuildDeleteFailureMessage(recordId, deleteResult.Messages, deleteResult.Errors, deleteResult.Result?.Id, deleteResult.Timing));
            }
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Builds the failure text used when Cloudflare rejects or does not confirm TXT-record creation.
        /// </summary>
        private static string BuildCreateFailureMessage(
            string recordName,
            DnsRecordType recordType,
            bool? success,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            StringBuilder message = new();
            _ = message.Append("Cloudflare TXT record create failed for ACME challenge.");
            AppendCloudflareDetails(message, recordName, recordType, success, messages, errors, timing);
            return message.ToString();
        }

        /// <summary>
        /// Builds the failure text used when TXT-record reconciliation cannot list the current Cloudflare records.
        /// </summary>
        private static string BuildQueryFailureMessage(
            string zoneId,
            string recordName,
            bool? success,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            StringBuilder message = new();
            _ = message.Append("Cloudflare TXT record query failed for ACME challenge reconciliation.");
            _ = message.Append(" ZoneId=");
            _ = message.Append(zoneId);
            AppendCloudflareDetails(message, recordName, DnsRecordType.Txt, success, messages, errors, timing);
            return message.ToString();
        }

        /// <summary>
        /// Builds the failure text used when ACME challenge cleanup cannot delete a Cloudflare TXT record.
        /// </summary>
        private static string BuildDeleteFailureMessage(
            string recordId,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            string? resultId,
            TimingInfo? timing)
        {
            StringBuilder message = new();
            _ = message.Append("Cloudflare TXT record delete failed for ACME challenge cleanup.");
            _ = message.Append(" RecordId=");
            _ = message.Append(recordId);
            if (!string.IsNullOrWhiteSpace(resultId))
            {
                _ = message.Append("; ResultId=");
                _ = message.Append(resultId);
            }

            AppendCloudflareCollections(message, messages, errors, timing);
            return message.ToString();
        }

        /// <summary>
        /// Appends common Cloudflare operation metadata to a provider-failure message.
        /// </summary>
        private static void AppendCloudflareDetails(
            StringBuilder message,
            string recordName,
            DnsRecordType recordType,
            bool? success,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            _ = message.Append(" RecordName=");
            _ = message.Append(recordName);
            _ = message.Append("; RecordType=");
            _ = message.Append(recordType);
            if (success.HasValue)
            {
                _ = message.Append("; Success=");
                _ = message.Append(success.Value);
            }

            AppendCloudflareCollections(message, messages, errors, timing);
        }

        /// <summary>
        /// Appends Cloudflare message, error-chain, and timing collections to a provider-failure message.
        /// </summary>
        private static void AppendCloudflareCollections(
            StringBuilder message,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            if (messages is not null && messages.Count > 0)
            {
                _ = message.Append("; Messages=[");
                for (int i = 0; i < messages.Count; i++)
                {
                    if (i > 0)
                    {
                        _ = message.Append(" | ");
                    }

                    ErrorDetails detail = messages[i];
                    _ = message.Append("Code=");
                    _ = message.Append(detail.Code);
                    _ = message.Append(", Message=");
                    _ = message.Append(detail.Message);
                }

                _ = message.Append(']');
            }

            if (errors is not null && errors.Count > 0)
            {
                _ = message.Append("; Errors=[");
                for (int i = 0; i < errors.Count; i++)
                {
                    if (i > 0)
                    {
                        _ = message.Append(" | ");
                    }

                    ApiError error = errors[i];
                    _ = message.Append("Code=");
                    _ = message.Append(error.Code);
                    _ = message.Append(", Message=");
                    _ = message.Append(error.Message);

                    if (error.ErrorChain is not null && error.ErrorChain.Count > 0)
                    {
                        _ = message.Append(", Chain=[");
                        for (int j = 0; j < error.ErrorChain.Count; j++)
                        {
                            if (j > 0)
                            {
                                _ = message.Append(" | ");
                            }

                            ErrorDetails chained = error.ErrorChain[j];
                            _ = message.Append("Code=");
                            _ = message.Append(chained.Code);
                            _ = message.Append(", Message=");
                            _ = message.Append(chained.Message);
                        }

                        _ = message.Append(']');
                    }
                }

                _ = message.Append(']');
            }

            if (timing is not null)
            {
                _ = message.Append("; Timing=");
                _ = message.Append(timing.ProcessTime);
            }
        }
    }
}
