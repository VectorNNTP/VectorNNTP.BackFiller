// <copyright file="DnsChallengeRecordLease.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Stores the lifecycle of one ACME DNS-01 TXT record used by cleanup.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Represents one created or reused DNS-01 TXT record and the cleanup ownership associated with it.
    /// </summary>
    /// <remarks>
    /// This lease is scoped only to ACME challenge TXT records and is unrelated to the A/AAAA records that keep the
    /// generated BackFiller FQDN synchronized with BindAddress values.
    /// </remarks>
    /// <param name="ZoneId">Cloudflare zone identifier that owns the TXT record.</param>
    /// <param name="RecordId">Cloudflare DNS record identifier.</param>
    /// <param name="RecordName">Fully qualified ACME TXT host name.</param>
    /// <param name="RecordValue">TXT content value expected by ACME validation.</param>
    /// <param name="IsOwnedByCurrentAttempt">Whether the current issuance attempt created the record and therefore must delete it during cleanup.</param>
    internal sealed record DnsChallengeRecordLease(
        string ZoneId,
        string RecordId,
        string RecordName,
        string RecordValue,
        bool IsOwnedByCurrentAttempt);
}
