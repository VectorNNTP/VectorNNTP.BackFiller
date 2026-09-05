// <copyright file="AcmeCertificateIssuer.Dns01Helpers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// DNS-01 stale-record recovery helpers for ACME issuance.


namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// DNS-01 record-reconciliation helpers used by <see cref="AcmeCertificateIssuer"/>.
    /// </summary>
    internal sealed partial class AcmeCertificateIssuer
    {
        /// <summary>
        /// Reconciles existing TXT records for the current ACME challenge name before creating a new challenge record.
        /// </summary>
        /// <remarks>
        /// Stale records from interrupted prior attempts may legitimately remain at the same ACME challenge name.
        /// This helper removes only records that are clearly owned by the current BackFiller ACME lifecycle and
        /// returns the identifier of an already-present matching challenge value when one exists.
        /// </remarks>
        /// <param name="txtRecordClient">Cloudflare TXT-record API used to remove stale owned records.</param>
        /// <param name="zoneId">Cloudflare zone that owns <paramref name="recordName"/>.</param>
        /// <param name="recordName">Fully qualified ACME TXT host name being reconciled.</param>
        /// <param name="recordValue">TXT value expected for the current ACME challenge.</param>
        /// <param name="existingTxtRecords">Existing TXT records returned for the challenge host name.</param>
        /// <param name="cancellationToken">Cancellation token observed before each provider delete operation.</param>
        /// <returns>
        /// The Cloudflare record identifier for an already-present matching TXT value when the current attempt can
        /// reuse it; otherwise <see langword="null"/>.
        /// </returns>
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
        /// Determines whether one TXT record is safe to treat as BackFiller-owned ACME challenge state.
        /// </summary>
        /// <remarks>
        /// Ownership is inferred conservatively from metadata that the implementation itself can safely control.
        /// Unrelated TXT values are never deleted.
        /// </remarks>
        /// <param name="record">TXT record candidate returned from Cloudflare.</param>
        /// <param name="recordName">Fully qualified ACME TXT host name expected for the challenge.</param>
        /// <returns><see langword="true"/> when the record name/type match and BackFiller ownership markers are present.</returns>
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
