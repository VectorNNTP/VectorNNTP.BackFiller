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
    /// When <see cref="Certificate"/> is present, the caller owns the loaded certificate bundle and must either publish
    /// it into <see cref="BackFillerCertificateState"/> or dispose the contained certificate when it will not be activated.
    /// </remarks>
    /// <param name="HasCertificate">Whether a certificate artifact was successfully located on disk.</param>
    /// <param name="IsUsable">Whether the located certificate passed listener-usage validation.</param>
    /// <param name="RequiresRenewal">Whether the certificate should trigger immediate renewal work.</param>
    /// <param name="Reason">Human-readable explanation describing the evaluation outcome.</param>
    /// <param name="Certificate">Loaded certificate bundle when validation produced an activatable certificate.</param>
    internal sealed record CertificateEvaluationResult(
        bool HasCertificate,
        bool IsUsable,
        bool RequiresRenewal,
        string Reason,
        BackFillerCertificateBundle? Certificate);
}
