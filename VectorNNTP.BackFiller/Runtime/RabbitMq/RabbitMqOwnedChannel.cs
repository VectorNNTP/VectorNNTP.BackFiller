// <copyright file="RabbitMqOwnedChannel.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq owned channel behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Wraps a RabbitMQ channel lease together with the logical owner and connection generation that created it.
    /// </summary>
    /// <remarks>
    /// Channels are intentionally not shared globally. Each caller receives an independently disposable lease so channel
    /// ownership, lifetime, and stale-generation detection remain explicit.
    /// </remarks>
    internal sealed class RabbitMqOwnedChannel : IAsyncDisposable
    {
        /// <summary>
        /// Channel adapter owned exclusively by this lease.
        /// </summary>
        private readonly IRabbitMqChannel _channel;

        /// <summary>
        /// Initializes a new owned RabbitMQ channel lease.
        /// </summary>
        /// <param name="channel">Channel adapter that becomes owned by this lease.</param>
        /// <param name="owner">Logical owner label used in diagnostics.</param>
        /// <param name="connectionGeneration">Connection generation on which the channel was created.</param>
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
        /// Gets the logical owner label associated with this channel lease.
        /// </summary>
        internal string Owner { get; }

        /// <summary>
        /// Gets the channel adapter owned by this lease.
        /// </summary>
        internal IRabbitMqChannel Channel => _channel;

        /// <summary>
        /// Gets the RabbitMQ connection generation on which the channel was created.
        /// </summary>
        internal long ConnectionGeneration { get; }

        /// <summary>
        /// Disposes the owned channel.
        /// </summary>
        /// <returns>A value task that completes after the underlying channel has been disposed.</returns>
        public ValueTask DisposeAsync()
        {
            return _channel.DisposeAsync();
        }
    }
}
