// <copyright file="PreparedBenchmarkWorkload.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Workload/PreparedBenchmarkWorkload: prepares and drives reproducible benchmark input workloads.

using System.Diagnostics.CodeAnalysis;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the prepared BenchmarkWorkload class used by this benchmark or regression-gate component.
/// </summary>
internal sealed class PreparedBenchmarkWorkload : IDisposable
{
    /// <summary>
    /// Gets or sets the _messageIds value used by this component.
    /// </summary>
    private readonly string[] _messageIds;
    /// <summary>
    /// Gets or sets the _nextMessageIndex value used by this component.
    /// </summary>
    private int _nextMessageIndex;

    /// <summary>
    /// Executes the prepared BenchmarkWorkload operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal PreparedBenchmarkWorkload(string[] messageIds, byte[] reusablePayloadBytes, WorkloadPreparationSummary summary)
    {
        _messageIds = messageIds;
        ReusableArticlePayload = reusablePayloadBytes;
        PreparationSummary = summary;
    }

    /// <summary>
    /// Gets or sets the reusable ArticlePayload value used by this component.
    /// </summary>
    internal ReadOnlyMemory<byte> ReusableArticlePayload { get; }

    /// <summary>
    /// Gets or sets the payload Length value used by this component.
    /// </summary>
    internal int PayloadLength => ReusableArticlePayload.Length;

    /// <summary>
    /// Gets or sets the preparation Summary value used by this component.
    /// </summary>
    internal WorkloadPreparationSummary PreparationSummary { get; }

    /// <summary>
    /// Executes the try TakeNextMessageId operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the dispose operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    public void Dispose()
    {
    }
}
