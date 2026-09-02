// <copyright file="DnsChallengeRecordLease.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Stores the lifecycle of one ACME DNS-01 TXT record used by cleanup.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Maintains one created DNS-01 TXT record so ACME challenge cleanup can reliably remove it.
    /// </summary>
    /// <remarks>
    /// This lease is scoped only to ACME challenge TXT records and is unrelated to the A/AAAA records that keep the
    /// generated BackFiller FQDN synchronized with BindAddress values.
    /// </remarks>
    /// <param name="ZoneId">Cloudflare zone identifier.</param>
    /// <param name="RecordId">Created Cloudflare DNS record identifier.</param>
    /// <param name="RecordName">TXT host name.</param>
    /// <param name="RecordValue">TXT content value.</param>
    /// <param name="IsOwnedByCurrentAttempt">Whether this issuance created the record and therefore owns cleanup.</param>
    internal sealed record DnsChallengeRecordLease(
        string ZoneId,
        string RecordId,
        string RecordName,
        string RecordValue,
        bool IsOwnedByCurrentAttempt);
}
