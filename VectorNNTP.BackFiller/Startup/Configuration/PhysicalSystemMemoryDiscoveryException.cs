// <copyright file="PhysicalSystemMemoryDiscoveryException.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Startup / Configuration
// Defines deterministic environmental failure classification for physical-memory discovery used by startup validation.

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Represents an expected environmental failure while determining total physical system memory for startup policy validation.
    /// </summary>
    /// <remarks>
    /// This exception is intended for recoverable discovery failures such as missing/unreadable platform files,
    /// malformed platform data, or platform API call failures. Validators may convert this into deterministic
    /// configuration errors that block startup when retention safety boundaries cannot be established.
    /// </remarks>
    internal sealed class PhysicalSystemMemoryDiscoveryException : InvalidOperationException
    {
        /// <summary>
        /// Initializes a new physical-system-memory discovery exception.
        /// </summary>
        /// <param name="message">Human-readable failure reason.</param>
        /// <param name="innerException">Optional underlying environmental exception.</param>
        internal PhysicalSystemMemoryDiscoveryException(string message, Exception? innerException = null)
            : base(message, innerException)
        {
        }
    }
}
