// <copyright file="PreparedBenchmarkWorkload.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Workload/PreparedBenchmarkWorkload: prepares and drives reproducible benchmark input workloads.

using System.Diagnostics.CodeAnalysis;

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Represents the prepared BenchmarkWorkload class used by the benchmark or regression gate.
    /// </summary>
    internal sealed class PreparedBenchmarkWorkload : IDisposable
    {
        private readonly string[] _messageIds;
        private int _nextMessageIndex;
        internal PreparedBenchmarkWorkload(string[] messageIds, byte[] reusablePayloadBytes, WorkloadPreparationSummary summary)
        {
            _messageIds = messageIds;
            ReusableArticlePayload = reusablePayloadBytes;
            PreparationSummary = summary;
        }
        internal ReadOnlyMemory<byte> ReusableArticlePayload { get; }
        internal int PayloadLength => ReusableArticlePayload.Length;
        internal WorkloadPreparationSummary PreparationSummary { get; }
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
}



