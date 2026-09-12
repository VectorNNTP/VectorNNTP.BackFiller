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
    /// This exact marker is persisted in the Cloudflare DNS record comment on create and later re-read from Cloudflare
    /// to prove workflow ownership across process restarts. Only exact equality matches are considered owned.
    /// </remarks>
    internal static class AcmeDnsTxtRecordOwnership
    {
        /// <summary>
        /// Exact Cloudflare record-comment marker persisted on BackFiller ACME DNS-01 TXT records.
        /// </summary>
        internal const string OwnershipComment = "VectorNNTP.BackFiller:acme-dns01";
    }
}
