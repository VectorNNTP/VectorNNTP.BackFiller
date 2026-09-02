// <copyright file="QueuedArticle.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/QueuedArticle: carries an article identifier and payload size through the benchmark queue.

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Represents the queued Article record struct used by the benchmark or regression gate.
    /// </summary>
    internal readonly record struct QueuedArticle(string MessageId, int PayloadLength);
}
