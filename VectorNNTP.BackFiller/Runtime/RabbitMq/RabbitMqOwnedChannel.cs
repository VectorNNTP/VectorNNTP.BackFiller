// <copyright file="RabbitMqOwnedChannel.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq owned channel behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Represents an independently owned RabbitMQ channel lease.
    /// </summary>
    /// <remarks>
    /// Channel instances are not shared globally. Each caller receives and disposes its own owned channel.
    /// </remarks>
    internal sealed class RabbitMqOwnedChannel : IAsyncDisposable
    {
        /// <summary>
        /// Stores channel used by rabbit mq owned channel.
        /// </summary>
        private readonly IRabbitMqChannel _channel;

        /// <summary>
        /// Initializes a new owned RabbitMQ channel wrapper.
        /// </summary>
        /// <param name="channel">Owned RabbitMQ channel.</param>
        /// <param name="owner">Logical owner identifier for diagnostics.</param>
        /// <param name="connectionGeneration">Connection generation associated with this channel lease.</param>
        internal RabbitMqOwnedChannel(IRabbitMqChannel channel, string owner, long connectionGeneration)
        {
            _channel = channel ?? throw new ArgumentNullException(nameof(channel));
            Owner = !string.IsNullOrWhiteSpace(owner)
                ? owner
                : throw new ArgumentException("Owner is required.", nameof(owner));

            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(connectionGeneration);
            ConnectionGeneration = connectionGeneration;
        }

        /// <summary>
        /// Gets the logical owner identifier for this channel lease.
        /// </summary>
        internal string Owner { get; }

        /// <summary>
        /// Gets the owned RabbitMQ channel adapter.
        /// </summary>
        internal IRabbitMqChannel Channel => _channel;

        /// <summary>
        /// Gets the RabbitMQ connection generation associated with this channel lease.
        /// </summary>
        internal long ConnectionGeneration { get; }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            return _channel.DisposeAsync();
        }
    }
}
