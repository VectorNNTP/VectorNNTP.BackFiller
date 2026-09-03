// <copyright file="RabbitMqConnectionReplacedEventArgs.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq connection replaced event args behavior.

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Event payload published after the connection manager establishes a RabbitMQ connection generation.
    /// </summary>
    /// <param name="ConnectionGeneration">Monotonic application-managed generation number assigned to the newly active connection.</param>
    /// <param name="IsReplacement"><see langword="true"/> when a prior generation existed and consumers may need to recreate stale channels.</param>
    internal sealed record RabbitMqConnectionReplacedEventArgs(
        long ConnectionGeneration,
        bool IsReplacement);
}
