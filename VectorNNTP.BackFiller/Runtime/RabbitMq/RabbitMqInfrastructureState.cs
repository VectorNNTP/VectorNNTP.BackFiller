// <copyright file="RabbitMqInfrastructureState.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq infrastructure state behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Lifecycle states used to report RabbitMQ infrastructure readiness and shutdown progress.
    /// </summary>
    internal enum RabbitMqInfrastructureState
    {
        /// <summary>
        /// No connection attempt has been started.
        /// </summary>
        NotInitialized,

        /// <summary>
        /// An initial broker connection attempt is in progress.
        /// </summary>
        Connecting,

        /// <summary>
        /// A broker connection is open but topology declaration may still be pending.
        /// </summary>
        Connected,

        /// <summary>
        /// Broker connection and required topology declarations are ready for channel consumers and publishers.
        /// </summary>
        TopologyReady,

        /// <summary>
        /// Application-managed recovery is attempting to replace a failed connection.
        /// </summary>
        Reconnecting,

        /// <summary>
        /// Connection establishment failed, or recovery failed beyond the configured tolerance.
        /// </summary>
        Failed,

        /// <summary>
        /// Shutdown or disposal has started and new operations should be rejected.
        /// </summary>
        Stopping,

        /// <summary>
        /// All owned lifecycle resources have been released.
        /// </summary>
        Stopped,
    }
}
