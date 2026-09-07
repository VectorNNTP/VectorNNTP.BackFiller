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
    /// <remarks>
    /// Implementations must return total physical system memory, not available memory, process memory,
    /// or container/cgroup limits. When that quantity cannot be determined due to expected environmental
    /// discovery failures, implementations must throw <see cref="PhysicalSystemMemoryDiscoveryException"/>.
    /// </remarks>
    internal interface IPhysicalSystemMemoryProvider
    {
        /// <summary>
        /// Gets total physical system memory in bytes.
        /// </summary>
        /// <returns>Total physical system memory in bytes.</returns>
        /// <exception cref="PlatformNotSupportedException">The current operating system is not supported for physical-memory discovery.</exception>
        /// <exception cref="PhysicalSystemMemoryDiscoveryException">Physical system memory could not be determined from the authoritative platform source.</exception>
        public ulong GetTotalPhysicalMemoryBytes();
    }
}
