// <copyright file="NntpAccountSnapshot.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Accounts
// Implements the nntp account snapshot behavior.

namespace VectorNNTP.Backfiller.Runtime.Accounts
{
    /// <summary>
    /// Immutable runtime NNTP account snapshot for one configured account row.
    /// </summary>
    /// <param name="EntryId">Stable account entry identifier.</param>
    /// <param name="Backbone">Configured backbone enum value from database.</param>
    /// <param name="Hostname">NNTP provider hostname.</param>
    /// <param name="KeepAliveSeconds">Keepalive interval in seconds.</param>
    /// <param name="MaxConnections">Maximum allowed NNTP connections.</param>
    /// <param name="Password">Provider password credential.</param>
    /// <param name="Port">NNTP provider port.</param>
    /// <param name="ServerId">Authoritative BackFiller server identifier.</param>
    /// <param name="Username">Provider username credential.</param>
    /// <param name="UseSsl">Whether SSL/TLS is enabled for provider connectivity.</param>
    internal sealed record NntpAccountSnapshot(
        Guid EntryId,
        string Backbone,
        string Hostname,
        byte KeepAliveSeconds,
        byte MaxConnections,
        string Password,
        ushort Port,
        byte ServerId,
        string Username,
        bool UseSsl);

    /// <summary>
    /// Immutable runtime account snapshot state published atomically by provider.
    /// </summary>
    /// <param name="ServerId">Server identifier the snapshot was loaded for.</param>
    /// <param name="Accounts">Read-only account collection.</param>
    internal sealed record NntpAccountSnapshotState(
        byte ServerId,
        IReadOnlyList<NntpAccountSnapshot> Accounts)
    {
        /// <summary>
        /// Empty account snapshot for a specific server id.
        /// </summary>
        /// <param name="serverId">Server identifier.</param>
        /// <returns>Empty immutable snapshot state.</returns>
        internal static NntpAccountSnapshotState Empty(byte serverId)
        {
            return new NntpAccountSnapshotState(serverId, []);
        }
    }
}
