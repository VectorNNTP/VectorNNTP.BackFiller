// <copyright file="QueuedArticle.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/QueuedArticle: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the queued Article record struct for benchmark or isolated-regression execution.
/// </summary>
internal readonly record struct QueuedArticle(string MessageId, int PayloadLength);
