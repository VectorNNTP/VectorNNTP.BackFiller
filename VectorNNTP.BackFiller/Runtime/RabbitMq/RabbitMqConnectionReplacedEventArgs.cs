namespace VectorNNTP.Backfiller.Runtime.RabbitMq;

/// <summary>
/// Event payload raised when the RabbitMQ connection is established or replaced.
/// </summary>
/// <param name="ConnectionGeneration">Monotonic connection generation; increments on every successful connection establishment.</param>
/// <param name="IsReplacement">Whether the connection is a replacement for a previously established connection.</param>
internal sealed record RabbitMqConnectionReplacedEventArgs(
    long ConnectionGeneration,
    bool IsReplacement);
