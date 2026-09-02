// <copyright file="NntpAccountSnapshot.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Accounts
// Implements the nntp account snapshot behavior.

namespace VectorNNTP.Backfiller.Runtime.Accounts
{
    /// <summary>
    /// Immutable runtime NNTP account snapshot for one authoritative provider-account entry.
    /// </summary>
    /// <remarks>
    /// Instances represent the normalized account view projected by the snapshot provider and consumed by
    /// session-management reconciliation paths.
    /// </remarks>
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
    /// Immutable runtime snapshot envelope published atomically by the account snapshot provider.
    /// </summary>
    /// <remarks>
    /// The envelope couples server identity with the full account set used by control-plane and acquisition
    /// components so consumers can swap to a coherent snapshot in one read.
    /// </remarks>
    /// <param name="ServerId">Server identifier the snapshot was loaded for.</param>
    /// <param name="Accounts">Read-only account collection.</param>
    internal sealed record NntpAccountSnapshotState(
        byte ServerId,
        IReadOnlyList<NntpAccountSnapshot> Accounts)
    {
        /// <summary>
        /// Creates an empty snapshot state for one server identifier.
        /// </summary>
        /// <param name="serverId">Server identifier used to stamp the empty snapshot envelope.</param>
        /// <returns>Empty immutable snapshot state.</returns>
        internal static NntpAccountSnapshotState Empty(byte serverId)
        {
            return new NntpAccountSnapshotState(serverId, []);
        }
    }
}
