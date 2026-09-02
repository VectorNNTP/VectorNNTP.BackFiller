// <copyright file="BackFillerCertificateBundle.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: back filler certificate bundle in the runtime certificates subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="BackFillerCertificateBundle.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Represents a loaded listener certificate bundle and its source file.

using System.Security.Cryptography.X509Certificates;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Represents the loaded BackFiller listener certificate bundle and its source on disk.
    /// </summary>
    /// <remarks>
    /// The bundle is a disposable owning container for the loaded X509 certificate. Consumers that publish a bundle
    /// into shared runtime state must clone the certificate if they need to keep it beyond the current ownership
    /// boundary.
    /// </remarks>
    /// <param name="Certificate">Loaded certificate with private key.</param>
    /// <param name="SourcePath">Source PFX path used to load the certificate.</param>
    /// <param name="LoadedAtUtc">UTC timestamp when the bundle was loaded.</param>
    internal sealed record BackFillerCertificateBundle(
        X509Certificate2 Certificate,
        string SourcePath,
        DateTimeOffset LoadedAtUtc);
}
