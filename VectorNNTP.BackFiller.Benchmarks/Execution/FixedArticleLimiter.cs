namespace VectorNNTP.BackFiller.Benchmarks;

internal sealed class FixedArticleLimiter
{
    private readonly int _targetCount;
    private int _issuedCount;

    internal FixedArticleLimiter(int targetCount)
    {
        if (targetCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(targetCount), targetCount, "Target count must be positive.");
        }

        _targetCount = targetCount;
    }

    internal bool TryReserveNext()
    {
        int next = Interlocked.Increment(ref _issuedCount);
        return next <= _targetCount;
    }
}
