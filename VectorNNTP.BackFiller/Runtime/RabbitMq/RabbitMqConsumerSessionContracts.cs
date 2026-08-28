// <copyright file="RabbitMqConsumerSessionContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using System.Threading.Channels;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Owns one logical RabbitMQ consumer session lifecycle for one backbone.
    /// </summary>
    internal interface IRabbitMqConsumerSession : IAsyncDisposable
    {
        /// <summary>
        /// Gets the immutable logical identity for this consumer session.
        /// </summary>
        public RabbitMqConsumerSessionIdentity Identity { get; }

        /// <summary>
        /// Gets whether this consumer session currently has an active broker registration.
        /// </summary>
        public bool IsRunning { get; }

        /// <summary>
        /// Gets the connection generation currently bound to this consumer session, or zero when not running.
        /// </summary>
        public long ActiveConnectionGeneration { get; }

        /// <summary>
        /// Starts or refreshes the broker consumer registration for the current active connection generation.
        /// </summary>
        /// <param name="cancellationToken">Startup/shutdown-aware cancellation token.</param>
        public Task StartAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Requests cooperative stop and disposes broker registration/channel for this session.
        /// </summary>
        /// <param name="cancellationToken">Shutdown-aware cancellation token.</param>
        public Task StopAsync(CancellationToken cancellationToken);
    }

    /// <summary>
    /// Receives RabbitMQ deliveries from infrastructure-owned consumer sessions.
    /// </summary>
    internal interface IRabbitMqDeliverySink
    {
        /// <summary>
        /// Accepts one infrastructure delivery.
        /// </summary>
        /// <param name="delivery">Delivery envelope.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ValueTask OnDeliveryAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Bounded in-memory RabbitMQ delivery buffer for phase-boundary handoff.
    /// </summary>
    internal sealed class RabbitMqDeliveryChannelSink : IRabbitMqDeliverySink
    {
        private readonly ChannelWriter<RabbitMqArticleDelivery> _writer;

        internal RabbitMqDeliveryChannelSink(ChannelWriter<RabbitMqArticleDelivery> writer)
        {
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        public ValueTask OnDeliveryAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _writer.WriteAsync(delivery, cancellationToken);
        }
    }
}
