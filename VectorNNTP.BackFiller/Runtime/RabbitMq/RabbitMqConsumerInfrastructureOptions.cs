// <copyright file="RabbitMqConsumerInfrastructureOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: rabbit mq consumer infrastructure options in the runtime rabbit mq subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="RabbitMqConsumerInfrastructureOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Immutable infrastructure options for RabbitMQ consumer session orchestration.
    /// </summary>
    /// <param name="DeliveryBufferCapacity">Bounded in-memory delivery capacity shared by infrastructure delivery sink.</param>
    /// <param name="PrefetchCount">Optional consumer prefetch count; when null, broker/channel defaults are used.</param>
    internal sealed record RabbitMqConsumerInfrastructureOptions(
        int DeliveryBufferCapacity,
        ushort? PrefetchCount)
    {
        /// <summary>
        /// Builds consumer infrastructure options from validated runtime RabbitMQ settings.
        /// </summary>
        /// <param name="runtimeOptions">Validated immutable runtime options.</param>
        /// <returns>Consumer infrastructure options aligned with runtime configuration.</returns>
        internal static RabbitMqConsumerInfrastructureOptions FromRuntimeOptions(BackFillerRuntimeOptions runtimeOptions)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            RabbitMqRuntimeOptions rabbitMq = runtimeOptions.RabbitMq
                ?? throw new InvalidOperationException("Validated runtime RabbitMQ settings were not provided.");

            return new RabbitMqConsumerInfrastructureOptions(
                DeliveryBufferCapacity: rabbitMq.ChannelPoolSize,
                PrefetchCount: rabbitMq.ConsumerPrefetchCount);
        }
    }
}
