// <copyright file="FixedArticleLimiter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/FixedArticleLimiter: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the fixed ArticleLimiter class used by this benchmark or regression-gate component.
/// </summary>
internal sealed class FixedArticleLimiter
{
    /// <summary>
    /// Gets or sets the _targetCount value used by this component.
    /// </summary>
    private readonly int _targetCount;
    /// <summary>
    /// Gets or sets the _issuedCount value used by this component.
    /// </summary>
    private int _issuedCount;

    /// <summary>
    /// Executes the fixed ArticleLimiter operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the try ReserveNext operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal bool TryReserveNext()
    {
        int next = Interlocked.Increment(ref _issuedCount);
        return next <= _targetCount;
    }
}
