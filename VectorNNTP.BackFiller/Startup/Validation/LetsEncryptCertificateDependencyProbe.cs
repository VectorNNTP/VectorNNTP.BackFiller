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
    /// Executes startup certificate dependency probing by attempting listener-certificate provisioning with an isolated ACME stack.
    /// </summary>
    /// <remarks>
    /// This probe is part of dependency validation and returns diagnostics through <see cref="DependencyValidationResult"/>
    /// instead of controlling process exit behavior directly. It intentionally constructs probe-local certificate services
    /// so startup can validate certificate readiness before host-managed listener services are initialized.
    /// </remarks>
    internal static class LetsEncryptCertificateDependencyProbe
    {
        /// <summary>
        /// Dependency category name used when reporting certificate-provisioning probe failures.
        /// </summary>
        private const string DependencyName = "LetsEncryptCertificate";

        /// <summary>
        /// Probes TLS certificate availability according to effective Let’s Encrypt runtime policy.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options snapshot that provides effective ACME configuration.</param>
        /// <param name="cancellationToken">Startup cancellation token propagated to certificate provisioning.</param>
        /// <returns>
        /// A task that completes with a dependency-validation snapshot. When Let’s Encrypt is disabled, the result is
        /// immediately successful; otherwise, probe failures are returned as a single dependency failure entry.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="runtimeOptions"/> is <see langword="null"/>.</exception>
        /// <exception cref="OperationCanceledException">The operation is canceled via <paramref name="cancellationToken"/>.</exception>
        /// <remarks>
        /// Non-cancellation exceptions from certificate provisioning are sanitized into failure text and captured in
        /// the returned result rather than rethrown, allowing startup validation to continue aggregating diagnostics.
        /// </remarks>
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
