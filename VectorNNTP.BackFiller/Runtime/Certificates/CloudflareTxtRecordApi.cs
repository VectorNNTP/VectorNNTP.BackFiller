// <copyright file="CloudflareTxtRecordApi.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Cloudflare TXT record adapter used by ACME DNS-01 issuance.

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
    /// Cloudflare TXT record adapter used by ACME DNS-01 issuance and cleanup.
    /// </summary>
    internal sealed class CloudflareTxtRecordApi : ICloudflareTxtRecordApi, IAsyncDisposable
    {
        private readonly CloudFlareClient _client;

        /// <summary>
        /// Initializes one adapter using the configured Cloudflare API token.
        /// </summary>
        /// <param name="apiToken">Cloudflare API token.</param>
        public CloudflareTxtRecordApi(string apiToken)
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

            CloudFlareResult<IReadOnlyList<DnsRecord>> queryResult = await _client.Zones.DnsRecords
                .GetAsync(zoneId, recordFilter, displayOptions, cancellationToken)
                .ConfigureAwait(false);

            return !queryResult.Success || queryResult.Result is null
                ? throw new InvalidOperationException(BuildQueryFailureMessage(zoneId, recordName, queryResult.Success, queryResult.Messages, queryResult.Errors, queryResult.Timing))
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

        /// <inheritdoc/>
        public async Task<CloudflareTxtRecordInfo> AddTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken)
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

            CloudFlareResult<DnsRecord> addResult;
            try
            {
                addResult = await _client.Zones.DnsRecords
                    .AddAsync(zoneId, newDnsRecord, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(BuildCreateFailureMessage(recordName, DnsRecordType.Txt, null, null, null, null), ex);
            }

            return !addResult.Success || addResult.Result is null || string.IsNullOrWhiteSpace(addResult.Result.Id)
                ? throw new InvalidOperationException(BuildCreateFailureMessage(recordName, DnsRecordType.Txt, addResult.Success, addResult.Messages, addResult.Errors, addResult.Timing))
                : new CloudflareTxtRecordInfo(
                Id: addResult.Result.Id,
                Name: addResult.Result.Name ?? recordName,
                Content: addResult.Result.Content ?? string.Empty,
                Type: addResult.Result.Type,
                Proxied: addResult.Result.Proxied,
                Ttl: addResult.Result.Ttl,
                Comment: addResult.Result.Comment,
                Tags: addResult.Result.Tags is null ? [] : [.. addResult.Result.Tags],
                CreatedDateUtc: addResult.Result.CreatedDate,
                ModifiedDateUtc: addResult.Result.ModifiedDate);
        }

        /// <inheritdoc/>
        public async Task DeleteTxtRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken)
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
                throw new InvalidOperationException(BuildDeleteFailureMessage(recordId, null, null, null, null), ex);
            }

            if (!deleteResult.Success)
            {
                throw new InvalidOperationException(BuildDeleteFailureMessage(recordId, deleteResult.Messages, deleteResult.Errors, deleteResult.Result?.Id, deleteResult.Timing));
            }
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Handles build create failure message for cloudflare txt record api.
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
        /// Handles build query failure message for cloudflare txt record api.
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
        /// Handles build delete failure message for cloudflare txt record api.
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
        /// Handles append cloudflare details for cloudflare txt record api.
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
        /// Handles append cloudflare collections for cloudflare txt record api.
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

