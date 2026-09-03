// <copyright file="RabbitMqArticleDelivery.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq article delivery behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Tracks whether a delivery admitted by a consumer session has reached a terminal settlement path.
    /// </summary>
    /// <remarks>
    /// Consumer-session shutdown waits for all admitted deliveries to be marked settled before disposing the owning
    /// channel. Implementations therefore provide exactly-once drain accounting rather than broker settlement itself.
    /// </remarks>
    internal interface IRabbitMqAdmittedDeliveryTracker
    {
        /// <summary>
        /// Marks the admitted delivery as terminally settled for session-drain accounting.
        /// </summary>
        public void MarkSettled();
    }

    /// <summary>
    /// Immutable RabbitMQ delivery envelope handed from infrastructure-owned consumers to article-processing code.
    /// </summary>
    /// <param name="Backbone">Backbone namespace whose queue produced the delivery.</param>
    /// <param name="Queue">Queue name from which the broker delivered the message.</param>
    /// <param name="ConsumerTag">Broker-assigned consumer tag for the active channel registration.</param>
    /// <param name="ConsumerIdentity">Stable logical consumer-session identity that admitted the delivery.</param>
    /// <param name="DeliveryTag">Broker delivery tag that must be used for ACK or NACK on the original channel.</param>
    /// <param name="Redelivered">Indicates whether RabbitMQ marked the delivery as a redelivery.</param>
    /// <param name="RoutingKey">Routing key observed on the inbound delivery frame.</param>
    /// <param name="Exchange">Exchange observed on the inbound delivery frame.</param>
    /// <param name="ConnectionGeneration">Application-managed RabbitMQ connection generation active when the delivery was admitted.</param>
    /// <param name="RabbitMqMessageId">Optional RabbitMQ <c>BasicProperties.MessageId</c> value supplied by the publisher.</param>
    /// <param name="CorrelationId">Optional RabbitMQ <c>BasicProperties.CorrelationId</c> value supplied by the publisher.</param>
    /// <param name="ReplyTo">Optional RabbitMQ <c>BasicProperties.ReplyTo</c> value supplied by the publisher.</param>
    /// <param name="Payload">Owned delivery payload bytes copied from the broker callback buffer.</param>
    /// <param name="CancellationToken">Session-scoped cancellation token that is canceled when admitted work should stop cooperatively.</param>
    /// <param name="Settlement">Exactly-once settlement handle that ACKs or NACKs on the original consumer channel generation.</param>
    /// <param name="AdmissionTracker">Optional drain-accounting tracker used to release session shutdown once work is settled.</param>
    /// <remarks>
    /// The payload is copied before publication to downstream code so the delivery remains valid after RabbitMQ callback
    /// buffers are reused. Settlement and admission tracking are separate concerns: settlement talks to the broker,
    /// while the optional tracker releases consumer-session drain waits once a terminal outcome is observed.
    /// </remarks>
    internal sealed record RabbitMqArticleDelivery(
        string Backbone,
        string Queue,
        string ConsumerTag,
        string ConsumerIdentity,
        ulong DeliveryTag,
        bool Redelivered,
        string RoutingKey,
        string Exchange,
        long ConnectionGeneration,
        string? RabbitMqMessageId,
        string? CorrelationId,
        string? ReplyTo,
        ReadOnlyMemory<byte> Payload,
        CancellationToken CancellationToken,
        IRabbitMqDeliverySettlement Settlement,
        IRabbitMqAdmittedDeliveryTracker? AdmissionTracker = null);
}
