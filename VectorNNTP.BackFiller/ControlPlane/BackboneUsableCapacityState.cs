// <copyright file="BackboneUsableCapacityState.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller ControlPlane
// Standalone thread-safe backbone usable-capacity state shared between control-plane publication and RabbitMQ admission gating.

using System.Collections.Immutable;

namespace VectorNNTP.Backfiller.ControlPlane
{
    /// <summary>
    /// Exposes the current read-only backbone usable-capacity view consumed by admission-control paths.
    /// </summary>
    internal interface IBackboneUsableCapacityProvider
    {
        /// <summary>
        /// Returns whether a backbone currently has at least one usable NNTP session slot.
        /// </summary>
        /// <param name="backbone">Backbone namespace to evaluate.</param>
        /// <returns><see langword="true"/> when the latest published snapshot contains a positive usable capacity for <paramref name="backbone"/>.</returns>
        public bool HasUsableCapacityForBackbone(string backbone);
    }

    /// <summary>
    /// Publishes authoritative backbone usable-capacity snapshots produced by the control plane.
    /// </summary>
    internal interface IBackboneUsableCapacityStateWriter
    {
        /// <summary>
        /// Replaces the current snapshot with a new authoritative backbone-to-usable-capacity map.
        /// </summary>
        /// <param name="capacityByBackbone">Backbone capacity map where each value is the currently usable session count.</param>
        public void PublishSnapshot(IReadOnlyDictionary<string, int> capacityByBackbone);
    }

    /// <summary>
    /// Holds the latest authoritative backbone usable-capacity snapshot for both publication and admission checks.
    /// </summary>
    /// <remarks>
    /// The state is updated by atomically swapping an immutable dictionary built from the latest control-plane snapshot.
    /// Invalid entries (blank backbone names or non-positive capacity) are intentionally excluded from published state.
    /// </remarks>
    internal sealed class BackboneUsableCapacityState : IBackboneUsableCapacityProvider, IBackboneUsableCapacityStateWriter
    {
        /// <summary>
        /// Most recently published usable-capacity snapshot keyed by backbone name.
        /// </summary>
        private ImmutableDictionary<string, int> _capacityByBackbone = ImmutableDictionary<string, int>.Empty.WithComparers(StringComparer.OrdinalIgnoreCase);

        /// <inheritdoc/>
        public bool HasUsableCapacityForBackbone(string backbone)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
            return _capacityByBackbone.TryGetValue(backbone, out int usableCount) && usableCount > 0;
        }

        /// <inheritdoc/>
        public void PublishSnapshot(IReadOnlyDictionary<string, int> capacityByBackbone)
        {
            ArgumentNullException.ThrowIfNull(capacityByBackbone);

            ImmutableDictionary<string, int>.Builder builder = ImmutableDictionary.CreateBuilder<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach ((string backbone, int usableCount) in capacityByBackbone)
            {
                if (string.IsNullOrWhiteSpace(backbone) || usableCount <= 0)
                {
                    continue;
                }

                builder[backbone] = usableCount;
            }

            _capacityByBackbone = builder.ToImmutable();
        }
    }
}
