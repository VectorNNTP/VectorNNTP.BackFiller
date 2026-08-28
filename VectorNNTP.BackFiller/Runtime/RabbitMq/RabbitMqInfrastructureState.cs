// <copyright file="RabbitMqInfrastructureState.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

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
