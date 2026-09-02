// <copyright file="RabbitMqArticleDelivery.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq article delivery responsibilities for this subsystem boundary.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Tracks admission lifecycle for one RabbitMQ delivery accepted by a logical consumer session.
    /// </summary>
    internal interface IRabbitMqAdmittedDeliveryTracker
    {
        /// <summary>
        /// Marks the admitted delivery as terminally settled on its owning channel.
        /// </summary>
        public void MarkSettled();
    }

    /// <summary>
    /// Immutable RabbitMQ article-work delivery envelope passed from infrastructure to processing layers.
    /// </summary>
    /// <param name="Backbone">Backbone namespace that owns the delivery queue.</param>
    /// <param name="Queue">Queue name receiving the delivery.</param>
    /// <param name="ConsumerTag">Broker-assigned consumer tag for the receiving consumer.</param>
    /// <param name="ConsumerIdentity">Stable logical consumer-session identity that received the delivery.</param>
    /// <param name="DeliveryTag">Broker delivery tag used by future ACK/NACK operations.</param>
    /// <param name="Redelivered">Whether the broker marked this delivery as a redelivery.</param>
    /// <param name="RoutingKey">Routing key observed on delivery.</param>
    /// <param name="Exchange">Exchange observed on delivery.</param>
    /// <param name="ConnectionGeneration">Connection generation associated with the consumer channel that received this delivery.</param>
    /// <param name="RabbitMqMessageId">Optional RabbitMQ BasicProperties MessageId value set by the publisher.</param>
    /// <param name="CorrelationId">Optional RabbitMQ BasicProperties CorrelationId value set by the publisher.</param>
    /// <param name="ReplyTo">Optional RabbitMQ BasicProperties ReplyTo value set by the publisher.</param>
    /// <param name="Payload">Raw RabbitMQ payload bytes.</param>
    /// <param name="CancellationToken">Delivery cancellation token associated with delivery lifecycle semantics.</param>
    /// <param name="AdmissionTracker">Optional admitted-delivery tracker used by session drain accounting.</param>
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
