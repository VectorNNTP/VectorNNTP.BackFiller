// <copyright file="FixedArticleLimiter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/FixedArticleLimiter: admits exactly a configured number of benchmark articles.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the fixed ArticleLimiter class used by the benchmark or regression gate.
/// </summary>
internal sealed class FixedArticleLimiter
{
    /// <summary>
    /// Gets or sets the _targetCount.
    /// </summary>
    private readonly int _targetCount;
    /// <summary>
    /// Gets or sets the _issuedCount.
    /// </summary>
    private int _issuedCount;

    /// <summary>
    /// Implements the fixed ArticleLimiter contract.
    /// </summary>
    internal FixedArticleLimiter(int targetCount)
    {
        if (targetCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(targetCount), targetCount, "Target count must be positive.");
        }

        _targetCount = targetCount;
    }

    /// <summary>
    /// Implements the try ReserveNext contract.
    /// </summary>
    internal bool TryReserveNext()
    {
        int next = Interlocked.Increment(ref _issuedCount);
        return next <= _targetCount;
    }
}
