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
    /// The bundle transfers ownership of the contained <see cref="X509Certificate2"/> across certificate-management
    /// boundaries. Consumers that publish a bundle into shared runtime state must clone the certificate if they need to
    /// keep it beyond the current ownership boundary.
    /// </remarks>
    /// <param name="Certificate">Loaded listener certificate, including the private key when loading succeeded.</param>
    /// <param name="SourcePath">PFX path from which <paramref name="Certificate"/> was loaded.</param>
    /// <param name="LoadedAtUtc">UTC timestamp captured after the certificate was loaded into memory.</param>
    internal sealed record BackFillerCertificateBundle(
        X509Certificate2 Certificate,
        string SourcePath,
        DateTimeOffset LoadedAtUtc);
}
