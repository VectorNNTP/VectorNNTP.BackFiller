// <copyright file="AcmeOrderIssueResult.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Represents the certificate artifacts returned from one ACME order.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Captures the certificate artifacts returned after one successful ACME order finalization.
    /// </summary>
    /// <remarks>
    /// The ACME protocol yields the leaf and issuer chain bytes, while the matching private key remains the local
    /// .NET-generated key that was used to create the CSR and later persisted alongside the listener certificate.
    /// </remarks>
    /// <param name="LeafCertificateDer">Leaf certificate DER bytes returned by the ACME order download.</param>
    /// <param name="ChainDer">Issuer certificates returned with the order, preserved in ACME download order.</param>
    /// <param name="CertificatePrivateKeyPem">PEM-encoded private key that matches <paramref name="LeafCertificateDer"/>.</param>
    internal sealed record AcmeOrderIssueResult(
        byte[] LeafCertificateDer,
        IReadOnlyList<byte[]> ChainDer,
        string CertificatePrivateKeyPem);
}
