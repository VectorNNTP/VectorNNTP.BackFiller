// <copyright file="PreparedBenchmarkWorkload.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Workload/PreparedBenchmarkWorkload: prepares and drives reproducible benchmark input workloads.

using System.Diagnostics.CodeAnalysis;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the prepared BenchmarkWorkload class used by the benchmark or regression gate.
/// </summary>
internal sealed class PreparedBenchmarkWorkload : IDisposable
{
    /// <summary>
    /// Gets or sets the _messageIds.
    /// </summary>
    private readonly string[] _messageIds;
    /// <summary>
    /// Gets or sets the _nextMessageIndex.
    /// </summary>
    private int _nextMessageIndex;

    /// <summary>
    /// Implements the prepared BenchmarkWorkload contract.
    /// </summary>
    /// <param name="messageIds">The ordered message-id set consumed during workload dispatch.</param>
    /// <param name="reusablePayloadBytes">The reusable payload bytes sent for each message-id.</param>
    /// <param name="summary">Metadata describing workload construction inputs and counts.</param>
    internal PreparedBenchmarkWorkload(string[] messageIds, byte[] reusablePayloadBytes, WorkloadPreparationSummary summary)
    {
        _messageIds = messageIds;
        ReusableArticlePayload = reusablePayloadBytes;
        PreparationSummary = summary;
    }

    /// <summary>
    /// Gets or sets the reusable ArticlePayload.
    /// </summary>
    internal ReadOnlyMemory<byte> ReusableArticlePayload { get; }

    /// <summary>
    /// Gets or sets the payload Length.
    /// </summary>
    internal int PayloadLength => ReusableArticlePayload.Length;

    /// <summary>
    /// Gets or sets the preparation Summary.
    /// </summary>
    internal WorkloadPreparationSummary PreparationSummary { get; }

    /// <summary>
    /// Implements the try TakeNextMessageId contract.
    /// </summary>
    /// <param name="messageId">When this method returns <see langword="true"/>, receives the next message-id to dispatch; otherwise <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when another message-id was available; otherwise <see langword="false"/>.</returns>
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
    /// Releases resources held by this instance.
    /// </summary>
    public void Dispose()
    {
    }
}
