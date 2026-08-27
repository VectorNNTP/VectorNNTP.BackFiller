// <copyright file="CertificateEvaluationResult.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Classifies whether a persisted listener certificate can be served and renewed.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Captures whether a persisted listener certificate can be served and whether renewal should be attempted.
    /// </summary>
    /// <remarks>
    /// When <see cref="Certificate"/> is present, the caller owns the clone and is responsible for disposing the
    /// loaded certificate bundle once the active state has been updated or rejected.
    /// </remarks>
    /// <param name="HasCertificate">Whether a certificate artifact was successfully loaded.</param>
    /// <param name="IsUsable">Whether the certificate is currently valid for listener use.</param>
    /// <param name="RequiresRenewal">Whether renewal should be attempted now.</param>
    /// <param name="Reason">Human-readable evaluation reason.</param>
    /// <param name="Certificate">Loaded certificate bundle when available.</param>
    internal sealed record CertificateEvaluationResult(
        bool HasCertificate,
        bool IsUsable,
        bool RequiresRenewal,
        string Reason,
        BackFillerCertificateBundle? Certificate);
}
