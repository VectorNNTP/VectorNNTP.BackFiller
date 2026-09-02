// <copyright file="IAuthoritativeDnsTxtPropagationVerifier.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Defines the authoritative TXT propagation verifier contract used during ACME issuance.

using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Verifies authoritative DNS TXT propagation for ACME DNS-01 challenge values.
    /// </summary>
    /// <remarks>
    /// Implementations query authoritative nameserver addresses directly and must not rely on recursive resolver
    /// caching alone, because the ACME challenge must be visible at the zone authority before validation proceeds.
    /// </remarks>
    internal interface IAuthoritativeDnsTxtPropagationVerifier
    {
        /// <summary>
        /// Waits until authoritative DNS nameserver responses satisfy configured TXT visibility quorum.
        /// </summary>
        /// <param name="fqdn">Challenge FQDN.</param>
        /// <param name="expectedTxtValue">Expected TXT content value.</param>
        /// <param name="options">Validated ACME runtime options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when propagation criteria are met.</returns>
        public Task WaitForPropagationAsync(
            string fqdn,
            string expectedTxtValue,
            BackFillerLetsEncryptRuntimeOptions options,
            CancellationToken cancellationToken);
    }
}
