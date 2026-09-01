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
    /// Read-only backbone usable-capacity view for admission-control consumers.
    /// </summary>
    internal interface IBackboneUsableCapacityProvider
    {
        /// <summary>
        /// Gets whether the specified backbone currently has at least one usable NNTP session.
        /// </summary>
        /// <param name="backbone">Backbone namespace to evaluate.</param>
        /// <returns><see langword="true"/> when at least one usable session is currently available for the backbone.</returns>
        public bool HasUsableCapacityForBackbone(string backbone);
    }

    /// <summary>
    /// Writer contract for publishing authoritative backbone usable-capacity state.
    /// </summary>
    internal interface IBackboneUsableCapacityStateWriter
    {
        /// <summary>
        /// Replaces the entire authoritative backbone usable-capacity snapshot.
        /// </summary>
        /// <param name="capacityByBackbone">Backbone capacity map where values represent currently usable session counts.</param>
        public void PublishSnapshot(IReadOnlyDictionary<string, int> capacityByBackbone);
    }

    /// <summary>
    /// Singleton state holder for backbone usable-capacity snapshots.
    /// </summary>
    internal sealed class BackboneUsableCapacityState : IBackboneUsableCapacityProvider, IBackboneUsableCapacityStateWriter
    {
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
