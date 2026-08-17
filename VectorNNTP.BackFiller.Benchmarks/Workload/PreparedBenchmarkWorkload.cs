namespace VectorNNTP.BackFiller.Benchmarks;

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

    internal bool TryTakeNextMessageId(out string? messageId)
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

    public void Dispose()
    {
    }
}
