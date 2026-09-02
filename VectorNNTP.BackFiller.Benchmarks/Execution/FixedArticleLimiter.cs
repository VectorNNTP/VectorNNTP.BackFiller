// <copyright file="FixedArticleLimiter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/FixedArticleLimiter: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the fixed ArticleLimiter class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class FixedArticleLimiter
{
    /// <summary>
    /// Gets or sets the _targetCount value.
    /// </summary>
    private readonly int _targetCount;
    /// <summary>
    /// Gets or sets the _issuedCount value.
    /// </summary>
    private int _issuedCount;

    /// <summary>
    /// Performs the fixed ArticleLimiter operation.
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
    /// Performs the try ReserveNext operation.
    /// </summary>
    internal bool TryReserveNext()
    {
        int next = Interlocked.Increment(ref _issuedCount);
        return next <= _targetCount;
    }
}
