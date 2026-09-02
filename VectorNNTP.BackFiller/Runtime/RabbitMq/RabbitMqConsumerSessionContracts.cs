// <copyright file="RabbitMqConsumerSessionContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq consumer session contracts behavior.

using System.Threading.Channels;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Owns one logical RabbitMQ consumer session lifecycle for one backbone.
    /// </summary>
    internal interface IRabbitMqConsumerSession : IAsyncDisposable
    {
        /// <summary>
        /// Returns the immutable logical identity for this consumer session.
        /// </summary>
        public RabbitMqConsumerSessionIdentity Identity { get; }

        /// <summary>
        /// Gets whether this consumer session currently has an active broker registration.
        /// </summary>
        public bool IsRunning { get; }

        /// <summary>
        /// Returns the connection generation currently bound to this consumer session, or zero when not running.
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
        /// <param name="cancelAdmittedWork">When <see langword="true"/>, cancels admitted-delivery processing tokens as part of shutdown semantics.</param>
        public Task StopAsync(CancellationToken cancellationToken, bool cancelAdmittedWork);
    }

    /// <summary>
    /// Handles account-capacity retirement boundaries for RabbitMQ consumer sessions.
    /// </summary>
    internal interface IRabbitMqCapacityRetirementCoordinator
    {
        /// <summary>
        /// Retires all logical RabbitMQ consumer sessions for one account above the retained capacity boundary.
        /// </summary>
        /// <param name="accountId">Stable account identifier.</param>
        /// <param name="retainConnectionCount">Number of logical consumer ordinals that must remain active.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes after targeted sessions have drained and been disposed.</returns>
        public Task RetireCapacityAsync(Guid accountId, int retainConnectionCount, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Owns settlement of one RabbitMQ delivery on its original consumer channel.
    /// </summary>
    internal interface IRabbitMqDeliverySettlement
    {
        /// <summary>
        /// Acknowledges the delivery.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ValueTask AckAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Negatively acknowledges the delivery.
        /// </summary>
        /// <param name="requeue">Whether RabbitMQ should requeue the delivery.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken);
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
        /// <summary>
        /// Stores writer used by rabbit mq consumer session contracts.
        /// </summary>
        private readonly ChannelWriter<RabbitMqArticleDelivery> _writer;

        /// <summary>
        /// Handles rabbit mq delivery channel sink for rabbit mq consumer session contracts.
        /// </summary>
        internal RabbitMqDeliveryChannelSink(ChannelWriter<RabbitMqArticleDelivery> writer)
        {
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        /// <summary>
        /// Handles on delivery async for rabbit mq consumer session contracts.
        /// </summary>
        public ValueTask OnDeliveryAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _writer.WriteAsync(delivery, cancellationToken);
        }
    }
}
