// <copyright file="IAcmeCertificateIssuer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Defines the ACME DNS-01 issuer contract used by certificate provisioning.

using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Issues BackFiller listener certificates through the ACME DNS-01 workflow.
    /// </summary>
    /// <remarks>
    /// Implementations are expected to create or reuse the ACME account, create an order for the generated BackFiller
    /// FQDN, publish DNS-01 TXT challenges, wait for authoritative propagation, validate the challenge, and return
    /// the resulting certificate material for persistence.
    /// </remarks>
    internal interface IAcmeCertificateIssuer
    {
        /// <summary>
        /// Executes a full ACME order workflow and returns the certificate artifacts required for persistence.
        /// </summary>
        /// <param name="letsEncryptOptions">Validated ACME runtime options that identify certificate, DNS, and account inputs.</param>
        /// <param name="cancellationToken">Cancellation token that aborts issuance before a result is returned.</param>
        /// <returns>Issued certificate artifacts for the listener certificate bundle.</returns>
        public Task<AcmeOrderIssueResult> IssueCertificateAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken);
    }
}
