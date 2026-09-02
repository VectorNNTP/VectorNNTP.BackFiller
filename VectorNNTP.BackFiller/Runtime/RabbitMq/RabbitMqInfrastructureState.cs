// <copyright file="RabbitMqInfrastructureState.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq infrastructure state behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// RabbitMQ infrastructure readiness states used by startup and runtime lifecycle management.
    /// </summary>
    internal enum RabbitMqInfrastructureState
    {
        NotInitialized,
        Connecting,
        Connected,
        TopologyReady,
        Reconnecting,
        Failed,
        Stopping,
        Stopped,
    }
}
