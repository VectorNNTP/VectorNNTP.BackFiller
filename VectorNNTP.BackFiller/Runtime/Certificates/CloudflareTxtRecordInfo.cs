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
    /// <param name="Name">DNS record name.</param>
    /// <param name="Content">TXT value content.</param>
    /// <param name="Type">DNS record type.</param>
    /// <param name="Proxied">Cloudflare proxy state.</param>
    /// <param name="Ttl">DNS TTL.</param>
    /// <param name="Comment">Cloudflare record comment.</param>
    /// <param name="Tags">Cloudflare record tags.</param>
    /// <param name="CreatedDateUtc">Creation timestamp.</param>
    /// <param name="ModifiedDateUtc">Modification timestamp.</param>
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
