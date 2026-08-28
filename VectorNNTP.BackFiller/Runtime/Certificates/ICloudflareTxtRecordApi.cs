// <copyright file="ICloudflareTxtRecordApi.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Abstraction over Cloudflare TXT record operations used by ACME DNS-01 lifecycles.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Abstraction over the Cloudflare TXT-record operations needed by ACME DNS-01 challenge handling.
    /// </summary>
    /// <remarks>
    /// This interface is intentionally narrow so tests can exercise retry, cleanup, and stale-record handling without
    /// depending on the live Cloudflare API.
    /// </remarks>
    internal interface ICloudflareTxtRecordApi : IAsyncDisposable
    {
        /// <summary>
        /// Retrieves TXT records for one DNS name.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordName">DNS record name.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The TXT records currently stored for the name.</returns>
        public Task<IReadOnlyList<CloudflareTxtRecordInfo>> GetTxtRecordsAsync(string zoneId, string recordName, CancellationToken cancellationToken);

        /// <summary>
        /// Creates one TXT record.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordName">DNS record name.</param>
        /// <param name="recordValue">TXT value content.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Created TXT record information.</returns>
        public Task<CloudflareTxtRecordInfo> AddTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken);

        /// <summary>
        /// Deletes one TXT record by identifier.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordId">Record identifier.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public Task DeleteTxtRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken);
    }
}
