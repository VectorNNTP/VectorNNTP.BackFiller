// <copyright file="AcmeCertificateIssuer.Dns01Helpers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// DNS-01 stale-record recovery helpers for ACME issuance.

using Certes;
using Certes.Acme;
using Certes.Acme.Resource;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Defines acme certificate issuer and its acme certificate issuer.dns01 helpers contract.
    /// </summary>
    internal sealed partial class AcmeCertificateIssuer
    {
        /// <summary>
        /// Reconciles existing TXT records for the current ACME challenge name before creating a new challenge.
        /// </summary>
        /// <remarks>
        /// Stale records from interrupted prior attempts may legitimately remain at the same ACME challenge name.
        /// This helper removes only records that are clearly owned by the current BackFiller ACME lifecycle and
        /// returns the identifier of an already-present matching challenge value when one exists.
        /// </remarks>
        private static async Task<string?> ReconcileExistingChallengeRecordsAsync(
            ICloudflareTxtRecordApi txtRecordClient,
            string zoneId,
            string recordName,
            string recordValue,
            IReadOnlyList<CloudflareTxtRecordInfo> existingTxtRecords,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(txtRecordClient);
            ArgumentException.ThrowIfNullOrWhiteSpace(zoneId);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordName);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordValue);
            ArgumentNullException.ThrowIfNull(existingTxtRecords);

            CloudflareTxtRecordInfo? matchingRecord = null;
            List<CloudflareTxtRecordInfo> staleOwnedRecords = [];

            for (int index = 0; index < existingTxtRecords.Count; index++)
            {
                CloudflareTxtRecordInfo record = existingTxtRecords[index];
                if (!string.Equals(record.Name, recordName, StringComparison.Ordinal) || record.Type != CloudFlare.Client.Enumerators.DnsRecordType.Txt)
                {
                    continue;
                }

                if (string.Equals(record.Content, recordValue, StringComparison.Ordinal))
                {
                    matchingRecord ??= record;
                    continue;
                }

                if (IsOwnedAcmeChallengeRecord(record, recordName))
                {
                    staleOwnedRecords.Add(record);
                }
            }

            for (int i = 0; i < staleOwnedRecords.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await txtRecordClient.DeleteTxtRecordAsync(zoneId, staleOwnedRecords[i].Id, cancellationToken).ConfigureAwait(false);
            }

            return matchingRecord?.Id;
        }

        /// <summary>
        /// Determines whether one TXT record is clearly owned by the BackFiller ACME workflow.
        /// </summary>
        /// <remarks>
        /// Ownership is inferred conservatively from metadata that the implementation itself can safely control.
        /// Unrelated TXT values are never deleted.
        /// </remarks>
        private static bool IsOwnedAcmeChallengeRecord(CloudflareTxtRecordInfo record, string recordName)
        {
            ArgumentNullException.ThrowIfNull(record);
            ArgumentException.ThrowIfNullOrWhiteSpace(recordName);

            if (!string.Equals(record.Name, recordName, StringComparison.Ordinal) || record.Type != CloudFlare.Client.Enumerators.DnsRecordType.Txt)
            {
                return false;
            }

            if (record.Comment is not null && record.Comment.Contains("BackFiller", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (record.Tags is not null)
            {
                for (int index = 0; index < record.Tags.Count; index++)
                {
                    if (record.Tags[index].Contains("BackFiller", StringComparison.OrdinalIgnoreCase) ||
                        record.Tags[index].Contains("acme", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}
