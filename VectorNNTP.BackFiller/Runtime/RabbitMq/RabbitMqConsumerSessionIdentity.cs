// <copyright file="RabbitMqConsumerSessionIdentity.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq consumer session identity behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Immutable identity for one logical RabbitMQ consumer session created from the authoritative account snapshot.
    /// </summary>
    /// <param name="Backbone">Backbone namespace consumed by the session.</param>
    /// <param name="AccountId">Stable account identifier used by reconciliation and retirement logic.</param>
    /// <param name="AccountUsername">Account username included in connection-scoped diagnostics.</param>
    /// <param name="ConnectionNumber">One-based logical connection ordinal represented by the session.</param>
    /// <param name="ConnectionLimit">Configured maximum connection count for the owning account.</param>
    /// <param name="ServerId">BackFiller server identifier forwarded to topology initialization call sites.</param>
    /// <param name="Host">Account NNTP host used for diagnostics.</param>
    /// <param name="Port">Account NNTP port used for diagnostics.</param>
    /// <param name="UseSsl">Account NNTP TLS mode used for diagnostics.</param>
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
        /// Gets the one-based logical session ordinal used by existing consumer-session diagnostics.
        /// </summary>
        internal int SessionOrdinal => ConnectionNumber;

        /// <summary>
        /// Gets the stable reconciliation key composed from account identifier and logical connection number.
        /// </summary>
        internal string SessionKey => $"{AccountId:N}:{ConnectionNumber}";
    }
}
