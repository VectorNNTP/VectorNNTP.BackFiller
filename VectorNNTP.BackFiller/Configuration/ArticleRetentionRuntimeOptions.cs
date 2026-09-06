// <copyright file="ArticleRetentionRuntimeOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller.Configuration
// Immutable article-retention runtime options projected from validated BackFiller configuration.

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Immutable runtime options that configure in-memory retained article ownership, expiration, and sweep cadence.
    /// </summary>
    /// <param name="MaximumRetainedPayloadBytes">Maximum aggregate retained article payload bytes before pressure eviction/admission rejection.</param>
    /// <param name="RetentionTtl">Absolute insertion-age retention limit for retained articles.</param>
    /// <param name="SweepInterval">Cadence used by runtime retention sweep processing.</param>
    /// <remarks>
    /// These values are runtime projections: external configuration supplies gigabytes and integer seconds, and startup projection converts once into
    /// byte and <see cref="TimeSpan"/> representations consumed by retention runtime components.
    /// </remarks>
    internal sealed record ArticleRetentionRuntimeOptions(
        long MaximumRetainedPayloadBytes,
        TimeSpan RetentionTtl,
        TimeSpan SweepInterval);
}
