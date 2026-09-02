// <copyright file="ICloudflareTxtRecordClient.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Defines the Cloudflare TXT record lifecycle contract for ACME challenges.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Manages Cloudflare TXT DNS records for ACME DNS-01 challenge lifecycles.
    /// </summary>
    /// <remarks>
    /// This interface exists only for temporary validation records used by ACME issuance and renewal. It does not
    /// participate in the application's startup A/AAAA reconciliation of the generated BackFiller FQDN.
    /// </remarks>
    internal interface ICloudflareTxtRecordClient : IAsyncDisposable
    {
        /// <summary>
        /// Lists TXT records for one DNS name within one zone.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordName">TXT record host name.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The current TXT records for the queried name.</returns>
        public Task<IReadOnlyList<CloudflareTxtRecordInfo>> GetTxtRecordsAsync(string zoneId, string recordName, CancellationToken cancellationToken);

        /// <summary>
        /// Creates one TXT record and returns the created record identifier.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordName">TXT record host name.</param>
        /// <param name="recordValue">TXT record content value.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Created record identifier.</returns>
        public Task<string> CreateTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken);

        /// <summary>
        /// Deletes one DNS record by Cloudflare identifier.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordId">Record identifier to delete.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when deletion has finished.</returns>
        public Task DeleteRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken);
    }
}
