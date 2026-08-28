// <copyright file="RabbitMqConnectionReplacedEventArgs.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Event payload raised when the RabbitMQ connection is established or replaced.
    /// </summary>
    /// <param name="ConnectionGeneration">Monotonic connection generation; increments on every successful connection establishment.</param>
    /// <param name="IsReplacement">Whether the connection is a replacement for a previously established connection.</param>
    internal sealed record RabbitMqConnectionReplacedEventArgs(
        long ConnectionGeneration,
        bool IsReplacement);
}
