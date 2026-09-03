// <copyright file="CloudflareTxtRecordInfo.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Represents a Cloudflare TXT record relevant to ACME DNS-01 ownership and cleanup.

using CloudFlare.Client.Enumerators;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Describes one Cloudflare TXT record used during ACME DNS-01 challenge handling.
    /// </summary>
    /// <remarks>
    /// The record metadata is intentionally minimal and excludes secrets. It is used to distinguish records owned by
    /// the current ACME issuance from unrelated TXT values that may legitimately coexist at the same challenge name.
    /// </remarks>
    /// <param name="Id">Cloudflare record identifier.</param>
    /// <param name="Name">Fully qualified DNS record name returned by Cloudflare.</param>
    /// <param name="Content">TXT value content.</param>
    /// <param name="Type">Cloudflare DNS record type.</param>
    /// <param name="Proxied">Cloudflare proxy state reported for the record.</param>
    /// <param name="Ttl">Cloudflare TTL value reported for the record.</param>
    /// <param name="Comment">Cloudflare record comment, when present.</param>
    /// <param name="Tags">Cloudflare tags associated with the record.</param>
    /// <param name="CreatedDateUtc">Record creation timestamp, when Cloudflare reports it.</param>
    /// <param name="ModifiedDateUtc">Record modification timestamp, when Cloudflare reports it.</param>
    internal sealed record CloudflareTxtRecordInfo(
        string Id,
        string Name,
        string Content,
        DnsRecordType Type,
        bool? Proxied,
        int? Ttl,
        string? Comment,
        IReadOnlyList<string> Tags,
        DateTime? CreatedDateUtc,
        DateTime? ModifiedDateUtc);
}
