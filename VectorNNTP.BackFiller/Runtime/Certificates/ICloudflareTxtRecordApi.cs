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
        /// <param name="recordName">Fully qualified TXT record name.</param>
        /// <param name="cancellationToken">Cancellation token propagated to the provider operation.</param>
        /// <returns>The TXT records currently stored for <paramref name="recordName"/>.</returns>
        public Task<IReadOnlyList<CloudflareTxtRecordInfo>> GetTxtRecordsAsync(string zoneId, string recordName, CancellationToken cancellationToken);

        /// <summary>
        /// Creates one TXT record for ACME challenge publication.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordName">Fully qualified TXT record name.</param>
        /// <param name="recordValue">TXT value content to publish.</param>
        /// <param name="cancellationToken">Cancellation token propagated to the provider operation.</param>
        /// <returns>Metadata describing the created TXT record.</returns>
        public Task<CloudflareTxtRecordInfo> AddTxtRecordAsync(string zoneId, string recordName, string recordValue, CancellationToken cancellationToken);

        /// <summary>
        /// Deletes one TXT record by provider identifier.
        /// </summary>
        /// <param name="zoneId">Cloudflare zone identifier.</param>
        /// <param name="recordId">Cloudflare record identifier to delete.</param>
        /// <param name="cancellationToken">Cancellation token propagated to the provider operation.</param>
        /// <returns>A task representing the asynchronous delete operation.</returns>
        public Task DeleteTxtRecordAsync(string zoneId, string recordId, CancellationToken cancellationToken);
    }
}
