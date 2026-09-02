// <copyright file="QueuedArticle.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/QueuedArticle: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the queued Article record struct used by this benchmark or regression-gate component.
/// </summary>
internal readonly record struct QueuedArticle(string MessageId, int PayloadLength);
