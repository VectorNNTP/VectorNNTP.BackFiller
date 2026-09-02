// <copyright file="FixedArticleLimiter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/FixedArticleLimiter: admits exactly a configured number of benchmark articles.

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Represents the fixed ArticleLimiter class used by the benchmark or regression gate.
    /// </summary>
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
}


