// <copyright file="IPhysicalSystemMemoryProvider.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Startup / Configuration
// Provides access to total physical system memory for deterministic configuration-policy validation.

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Provides total physical system memory in bytes for startup configuration validation.
    /// </summary>
    internal interface IPhysicalSystemMemoryProvider
    {
        /// <summary>
        /// Gets total physical system memory in bytes.
        /// </summary>
        /// <returns>Total physical system memory in bytes.</returns>
        public ulong GetTotalPhysicalMemoryBytes();
    }
}
