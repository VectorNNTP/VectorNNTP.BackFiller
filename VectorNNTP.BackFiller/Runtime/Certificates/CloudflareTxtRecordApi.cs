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

            if (!queryResult.Success || queryResult.Result is null)
            {
                throw new InvalidOperationException(BuildQueryFailureMessage(zoneId, recordName, queryResult.Success, queryResult.Messages, queryResult.Errors, queryResult.Timing));
            }

            return [.. queryResult.Result.Select(static record => new CloudflareTxtRecordInfo(
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

            if (!addResult.Success || addResult.Result is null || string.IsNullOrWhiteSpace(addResult.Result.Id))
            {
                throw new InvalidOperationException(BuildCreateFailureMessage(recordName, DnsRecordType.Txt, addResult.Success, addResult.Messages, addResult.Errors, addResult.Timing));
            }

            return new CloudflareTxtRecordInfo(
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

        private static string BuildCreateFailureMessage(
            string recordName,
            DnsRecordType recordType,
            bool? success,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            StringBuilder message = new();
            message.Append("Cloudflare TXT record create failed for ACME challenge.");
            AppendCloudflareDetails(message, recordName, recordType, success, messages, errors, timing);
            return message.ToString();
        }

        private static string BuildQueryFailureMessage(
            string zoneId,
            string recordName,
            bool? success,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            StringBuilder message = new();
            message.Append("Cloudflare TXT record query failed for ACME challenge reconciliation.");
            message.Append(" ZoneId=");
            message.Append(zoneId);
            AppendCloudflareDetails(message, recordName, DnsRecordType.Txt, success, messages, errors, timing);
            return message.ToString();
        }

        private static string BuildDeleteFailureMessage(
            string recordId,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            string? resultId,
            TimingInfo? timing)
        {
            StringBuilder message = new();
            message.Append("Cloudflare TXT record delete failed for ACME challenge cleanup.");
            message.Append(" RecordId=");
            message.Append(recordId);
            if (!string.IsNullOrWhiteSpace(resultId))
            {
                message.Append("; ResultId=");
                message.Append(resultId);
            }

            AppendCloudflareCollections(message, messages, errors, timing);
            return message.ToString();
        }

        private static void AppendCloudflareDetails(
            StringBuilder message,
            string recordName,
            DnsRecordType recordType,
            bool? success,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            message.Append(" RecordName=");
            message.Append(recordName);
            message.Append("; RecordType=");
            message.Append(recordType);
            if (success.HasValue)
            {
                message.Append("; Success=");
                message.Append(success.Value);
            }

            AppendCloudflareCollections(message, messages, errors, timing);
        }

        private static void AppendCloudflareCollections(
            StringBuilder message,
            IReadOnlyList<ErrorDetails>? messages,
            IReadOnlyList<ApiError>? errors,
            TimingInfo? timing)
        {
            if (messages is not null && messages.Count > 0)
            {
                message.Append("; Messages=[");
                for (int i = 0; i < messages.Count; i++)
                {
                    if (i > 0)
                    {
                        message.Append(" | ");
                    }

                    ErrorDetails detail = messages[i];
                    message.Append("Code=");
                    message.Append(detail.Code);
                    message.Append(", Message=");
                    message.Append(detail.Message);
                }

                message.Append(']');
            }

            if (errors is not null && errors.Count > 0)
            {
                message.Append("; Errors=[");
                for (int i = 0; i < errors.Count; i++)
                {
                    if (i > 0)
                    {
                        message.Append(" | ");
                    }

                    ApiError error = errors[i];
                    message.Append("Code=");
                    message.Append(error.Code);
                    message.Append(", Message=");
                    message.Append(error.Message);

                    if (error.ErrorChain is not null && error.ErrorChain.Count > 0)
                    {
                        message.Append(", Chain=[");
                        for (int j = 0; j < error.ErrorChain.Count; j++)
                        {
                            if (j > 0)
                            {
                                message.Append(" | ");
                            }

                            ErrorDetails chained = error.ErrorChain[j];
                            message.Append("Code=");
                            message.Append(chained.Code);
                            message.Append(", Message=");
                            message.Append(chained.Message);
                        }

                        message.Append(']');
                    }
                }

                message.Append(']');
            }

            if (timing is not null)
            {
                message.Append("; Timing=");
                message.Append(timing.ProcessTime);
            }
        }
    }
}
