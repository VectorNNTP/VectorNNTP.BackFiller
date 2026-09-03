// <copyright file="RabbitMqConsumerInfrastructureOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq consumer infrastructure options behavior.

using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Immutable configuration used by the consumer service to size its delivery buffer and optional channel prefetch.
    /// </summary>
    /// <param name="DeliveryBufferCapacity">Bounded delivery-channel capacity shared between RabbitMQ consumers and downstream processing.</param>
    /// <param name="PrefetchCount">Optional per-channel prefetch count; when <see langword="null"/>, the broker default remains in effect.</param>
    internal sealed record RabbitMqConsumerInfrastructureOptions(
        int DeliveryBufferCapacity,
        ushort? PrefetchCount)
    {
        /// <summary>
        /// Derives consumer-service infrastructure settings from the validated runtime snapshot.
        /// </summary>
        /// <param name="runtimeOptions">Validated immutable application runtime options.</param>
        /// <returns>Consumer infrastructure settings aligned with the active RabbitMQ configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="runtimeOptions"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when validated RabbitMQ options are unavailable.</exception>
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
