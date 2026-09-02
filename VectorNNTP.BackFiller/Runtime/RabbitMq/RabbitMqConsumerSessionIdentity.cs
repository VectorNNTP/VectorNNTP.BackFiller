// <copyright file="RabbitMqConsumerSessionIdentity.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq consumer session identity behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Immutable logical identity for one RabbitMQ consumer session.
    /// </summary>
    /// <param name="Backbone">Backbone namespace consumed by this session.</param>
    /// <param name="AccountId">Owning account identifier used by reconciliation.</param>
    /// <param name="AccountUsername">Owning account username used for connection-scoped logging.</param>
    /// <param name="ConnectionNumber">One-based account connection number represented by this consumer session.</param>
    /// <param name="ConnectionLimit">Configured connection limit for the owning account.</param>
    /// <param name="ServerId">Owning BackFiller server identifier for topology initializer compatibility.</param>
    /// <param name="Host">Owning account NNTP host used for connection-scoped logging context.</param>
    /// <param name="Port">Owning account NNTP port used for connection-scoped logging context.</param>
    /// <param name="UseSsl">Owning account NNTP TLS mode used for connection-scoped logging context.</param>
    internal sealed record RabbitMqConsumerSessionIdentity(
        string Backbone,
        Guid AccountId,
        string AccountUsername,
        int ConnectionNumber,
        int ConnectionLimit,
        byte ServerId,
        string Host,
        int Port,
        bool UseSsl)
    {
        /// <summary>
        /// Returns the one-based session ordinal used by existing consumer/session diagnostics.
        /// </summary>
        internal int SessionOrdinal => ConnectionNumber;

        /// <summary>
        /// Returns the stable logical session key used in diagnostics and reconciliation maps.
        /// </summary>
        internal string SessionKey => $"{AccountId:N}:{ConnectionNumber}";
    }
}
