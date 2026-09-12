// <copyright file="AcmeDnsTxtRecordOwnership.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Canonical ownership metadata for ACME DNS-01 TXT records managed by BackFiller.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Defines the canonical Cloudflare metadata contract that grants BackFiller ACME DNS-01 TXT deletion authority.
    /// </summary>
    /// <remarks>
    /// This marker is persisted on create and later re-read from Cloudflare to prove workflow ownership across process
    /// restarts. Only exact canonical marker matches are considered owned for stale-record reconciliation.
    /// </remarks>
    internal static class AcmeDnsTxtRecordOwnership
    {
        /// <summary>
        /// Exact Cloudflare tag persisted on BackFiller ACME DNS-01 TXT records.
        /// </summary>
        internal const string CanonicalOwnershipTag = "vectornntp.backfiller.acme-dns01";

        /// <summary>
        /// Operator-facing Cloudflare comment set when creating BackFiller ACME DNS-01 TXT records.
        /// </summary>
        /// <remarks>
        /// This comment is informational only and does not grant deletion authority.
        /// </remarks>
        internal const string OwnershipComment = "VectorNNTP.BackFiller ACME DNS-01";
    }
}
