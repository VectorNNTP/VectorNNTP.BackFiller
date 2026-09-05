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
    /// Owns the lifecycle of one logical RabbitMQ consumer registration bound to a single backbone queue.
    /// </summary>
    internal interface IRabbitMqConsumerSession : IAsyncDisposable
    {
        /// <summary>
        /// Gets the stable logical identity used for reconciliation, diagnostics, and connection-scoped logging.
        /// </summary>
        public RabbitMqConsumerSessionIdentity Identity { get; }

        /// <summary>
        /// Gets whether the session currently owns an active broker consumer registration.
        /// </summary>
        public bool IsRunning { get; }

        /// <summary>
        /// Gets the RabbitMQ connection generation currently bound to the session, or zero when no consumer is active.
        /// </summary>
        public long ActiveConnectionGeneration { get; }

        /// <summary>
        /// Creates the broker consumer registration for the current connection generation when the session is stopped.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for startup or reconciliation shutdown.</param>
        /// <returns>A task that completes after the session reaches its running state or observes that it is already started.</returns>
        public Task StartAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Stops the broker consumer, drains admitted deliveries, and releases session-owned channel resources.
        /// </summary>
        /// <param name="cancelAdmittedWork"><see langword="true"/> to cancel admitted-delivery tokens while draining work; otherwise in-flight work is allowed to continue cooperatively.</param>
        /// <param name="cancellationToken">Cancellation token for the stop operation.</param>
        /// <returns>A task that completes after the session has drained and torn down its broker registration.</returns>
        public Task StopAsync(bool cancelAdmittedWork, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Coordinates retirement of excess consumer sessions when account capacity shrinks.
    /// </summary>
    internal interface IRabbitMqCapacityRetirementCoordinator
    {
        /// <summary>
        /// Retires consumer sessions for one account whose one-based connection number exceeds the retained capacity boundary.
        /// </summary>
        /// <param name="accountId">Stable account identifier whose sessions are being trimmed.</param>
        /// <param name="retainConnectionCount">Number of lowest-numbered logical sessions that must remain active.</param>
        /// <param name="cancellationToken">Cancellation token for the retirement wait.</param>
        /// <returns>A task that completes after targeted sessions have drained and been disposed.</returns>
        public Task RetireCapacityAsync(Guid accountId, int retainConnectionCount, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Owns exactly-once ACK or NACK settlement for a delivery on the consumer channel that admitted it.
    /// </summary>
    internal interface IRabbitMqDeliverySettlement
    {
        /// <summary>
        /// Acknowledges the delivery on its original RabbitMQ consumer channel generation.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the broker settlement call.</param>
        /// <returns>A value task that completes after the broker ACK has been issued.</returns>
        public ValueTask AckAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Negatively acknowledges the delivery on its original RabbitMQ consumer channel generation.
        /// </summary>
        /// <param name="requeue"><see langword="true"/> to request broker requeue; otherwise the message is rejected without requeue.</param>
        /// <param name="cancellationToken">Cancellation token for the broker settlement call.</param>
        /// <returns>A value task that completes after the broker NACK has been issued.</returns>
        public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Accepts deliveries emitted by infrastructure-owned consumer sessions.
    /// </summary>
    internal interface IRabbitMqDeliverySink
    {
        /// <summary>
        /// Accepts one admitted delivery for downstream processing.
        /// </summary>
        /// <param name="delivery">Immutable delivery envelope, including payload ownership and settlement handle.</param>
        /// <param name="cancellationToken">Cancellation token for the handoff.</param>
        /// <returns>A value task that completes after the sink has accepted or rejected the delivery handoff.</returns>
        public ValueTask OnDeliveryAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Writes admitted deliveries into a bounded in-memory channel shared with the next processing phase.
    /// </summary>
    /// <remarks>
    /// The sink does not settle deliveries itself. It only transfers ownership into the configured channel writer and
    /// therefore inherits that channel's backpressure behavior.
    /// </remarks>
    internal sealed class RabbitMqDeliveryChannelSink : IRabbitMqDeliverySink
    {
        /// <summary>
        /// Channel writer that receives admitted deliveries for downstream processing.
        /// </summary>
        private readonly ChannelWriter<RabbitMqArticleDelivery> _writer;

        /// <summary>
        /// Initializes a sink that forwards deliveries into the supplied channel writer.
        /// </summary>
        /// <param name="writer">Bounded or unbounded channel writer that owns downstream buffering semantics.</param>
        internal RabbitMqDeliveryChannelSink(ChannelWriter<RabbitMqArticleDelivery> writer)
        {
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        /// <summary>
        /// Writes the admitted delivery into the configured channel writer.
        /// </summary>
        /// <param name="delivery">Delivery to buffer for downstream processing.</param>
        /// <param name="cancellationToken">Cancellation token for the channel write.</param>
        /// <returns>A value task that completes after the delivery is accepted by the channel writer.</returns>
        public ValueTask OnDeliveryAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _writer.WriteAsync(delivery, cancellationToken);
        }
    }
}
