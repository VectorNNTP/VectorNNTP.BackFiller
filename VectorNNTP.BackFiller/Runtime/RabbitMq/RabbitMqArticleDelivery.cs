namespace VectorNNTP.Backfiller.Runtime.RabbitMq;

/// <summary>
/// Immutable RabbitMQ article-work delivery envelope passed from infrastructure to processing layers.
/// </summary>
/// <param name="Backbone">Backbone namespace that owns the delivery queue.</param>
/// <param name="Queue">Queue name receiving the delivery.</param>
/// <param name="ConsumerTag">Broker-assigned consumer tag for the receiving consumer.</param>
/// <param name="DeliveryTag">Broker delivery tag used by future ACK/NACK operations.</param>
/// <param name="Redelivered">Whether the broker marked this delivery as a redelivery.</param>
/// <param name="RoutingKey">Routing key observed on delivery.</param>
/// <param name="Exchange">Exchange observed on delivery.</param>
/// <param name="ConnectionGeneration">Connection generation associated with the consumer channel that received this delivery.</param>
/// <param name="Payload">Raw RabbitMQ payload bytes.</param>
/// <param name="CancellationToken">Delivery cancellation token that is canceled when the owning consumer session is stopped.</param>
internal sealed record RabbitMqArticleDelivery(
    string Backbone,
    string Queue,
    string ConsumerTag,
    ulong DeliveryTag,
    bool Redelivered,
    string RoutingKey,
    string Exchange,
    long ConnectionGeneration,
    ReadOnlyMemory<byte> Payload,
    CancellationToken CancellationToken);
