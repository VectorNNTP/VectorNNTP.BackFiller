// <copyright file="PreparedBenchmarkWorkload.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Workload/PreparedBenchmarkWorkload: prepares and drives reproducible benchmark input workloads.

using System.Diagnostics.CodeAnalysis;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the prepared BenchmarkWorkload class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class PreparedBenchmarkWorkload : IDisposable
{
    /// <summary>
    /// Gets or sets the _messageIds value.
    /// </summary>
    private readonly string[] _messageIds;
    /// <summary>
    /// Gets or sets the _nextMessageIndex value.
    /// </summary>
    private int _nextMessageIndex;

    /// <summary>
    /// Performs the prepared BenchmarkWorkload operation.
    /// </summary>
    internal PreparedBenchmarkWorkload(string[] messageIds, byte[] reusablePayloadBytes, WorkloadPreparationSummary summary)
    {
        _messageIds = messageIds;
        ReusableArticlePayload = reusablePayloadBytes;
        PreparationSummary = summary;
    }

    /// <summary>
    /// Gets or sets the reusable ArticlePayload value.
    /// </summary>
    internal ReadOnlyMemory<byte> ReusableArticlePayload { get; }

    /// <summary>
    /// Gets or sets the payload Length value.
    /// </summary>
    internal int PayloadLength => ReusableArticlePayload.Length;

    /// <summary>
    /// Gets or sets the preparation Summary value.
    /// </summary>
    internal WorkloadPreparationSummary PreparationSummary { get; }

    /// <summary>
    /// Performs the try TakeNextMessageId operation.
    /// </summary>
    internal bool TryTakeNextMessageId([NotNullWhen(true)] out string? messageId)
    {
        int index = Interlocked.Increment(ref _nextMessageIndex) - 1;
        if ((uint)index >= (uint)_messageIds.Length)
        {
            messageId = null;
            return false;
        }

        messageId = _messageIds[index];
        return true;
    }

    /// <summary>
    /// Performs the dispose operation.
    /// </summary>
    public void Dispose()
    {
    }
}
