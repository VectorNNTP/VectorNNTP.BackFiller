// <copyright file="LetsEncryptCertificateDependencyProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Startup.Validation
// Verifies that the inbound listener certificate can be provisioned before the listener starts.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Performs startup-time ACME validation by constructing the certificate stack and ensuring the listener bundle exists.
    /// </summary>
    /// <remarks>
    /// This probe intentionally creates an isolated certificate stack rather than resolving the host's production
    /// registrations, so startup validation can fail fast before the listener service is started. When Let’s Encrypt
    /// is disabled, the probe succeeds without touching certificate storage or ACME infrastructure.
    /// </remarks>
    internal static class LetsEncryptCertificateDependencyProbe
    {
        /// <summary>
        /// Stores dependency name used by lets encrypt certificate dependency probe.
        /// </summary>
        private const string DependencyName = "LetsEncryptCertificate";

        /// <summary>
        /// Ensures TLS certificate availability according to BackFiller ACME policy.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options snapshot.</param>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>Dependency validation result.</returns>
        internal static async Task<DependencyValidationResult> EnsureCertificateAvailabilityAsync(
            BackFillerRuntimeOptions runtimeOptions,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions = runtimeOptions.EffectiveLetsEncrypt;
            if (!letsEncryptOptions.Enabled)
            {
                return DependencyValidationResult.Success();
            }

            List<(string Dependency, string Reason)> failures = [];
            List<(string Category, string Message)> warnings = [];
            List<(string Category, string Message)> errors = [];

            try
            {
                TimeProvider timeProvider = TimeProvider.System;
                BackFillerCertificateState certificateState = new();
                BackFillerCertificateStore certificateStore = new();
                AuthoritativeDnsTxtPropagationVerifier propagationVerifier = new(
                    timeProvider,
                    Microsoft.Extensions.Logging.Abstractions.NullLogger<AuthoritativeDnsTxtPropagationVerifier>.Instance);

                AcmeCertificateIssuer acmeIssuer = new(
                    timeProvider,
                    Microsoft.Extensions.Logging.Abstractions.NullLogger<AcmeCertificateIssuer>.Instance,
                    propagationVerifier);

                BackFillerCertificateProvisioningService provisioningService = new(
                    certificateStore,
                    acmeIssuer,
                    certificateState,
                    Microsoft.Extensions.Logging.Abstractions.NullLogger<BackFillerCertificateProvisioningService>.Instance,
                    timeProvider);

                await provisioningService
                    .EnsureCertificateAvailabilityAsync(runtimeOptions, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                string sanitizedMessage =
                    string.IsNullOrWhiteSpace(ex.Message)
                        ? ex.GetType().Name
                        : $"{ex.GetType().Name}: {ex.Message}";

                failures.Add((DependencyName, $"TLS certificate provisioning failed: {sanitizedMessage}"));
            }

            return new DependencyValidationResult(failures, warnings, errors);
        }
    }
}
