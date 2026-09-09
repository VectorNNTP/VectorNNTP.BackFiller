// <copyright file="BackFillerCertificateProvisioningService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Owns listener certificate discovery, issuance, activation, and renewal.

using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Serializes certificate evaluation, issuance, activation, and renewal for the BackFiller listener TLS stack.
    /// </summary>
    /// <remarks>
    /// The service preserves the currently active certificate when renewal fails and the existing certificate is still
    /// usable. It is responsible for publishing the active certificate into shared runtime state and for ensuring
    /// that startup only activates a certificate that can be served by the inbound listener.
    /// </remarks>
    internal sealed partial class BackFillerCertificateProvisioningService : IDisposable
    {
        /// <summary>
        /// ACME issuer used when a replacement listener certificate must be requested.
        /// </summary>
        private readonly IAcmeCertificateIssuer _acmeIssuer;
        /// <summary>
        /// Shared runtime certificate state updated after evaluation or successful issuance.
        /// </summary>
        private readonly BackFillerCertificateState _certificateState;
        /// <summary>
        /// Logger used for provisioning, activation, and fallback diagnostics.
        /// </summary>
        private readonly ILogger<BackFillerCertificateProvisioningService> _logger;
        /// <summary>
        /// Clock source used when evaluating certificate validity and renewal windows.
        /// </summary>
        private readonly TimeProvider _timeProvider;
        /// <summary>
        /// Process-wide gate that serializes certificate evaluation and issuance across all service callers.
        /// </summary>
        private static readonly SemaphoreSlim ProvisionGate = new(1, 1);

        /// <summary>
        /// Certificate-evaluation operation that classifies persisted listener-certificate usability and renewal requirements.
        /// </summary>
        private readonly Func<BackFillerLetsEncryptRuntimeOptions, TimeProvider, CancellationToken, Task<CertificateEvaluationResult>> _evaluateExistingCertificateAsync;

        /// <summary>
        /// Certificate-provisioning operation that issues, persists, reloads, and publishes replacement listener certificates.
        /// </summary>
        private readonly Func<BackFillerLetsEncryptRuntimeOptions, CancellationToken, Task> _provisionNewCertificateAsync;

        /// <summary>
        /// Disposal operation for evaluated certificate bundles that are not transferred into runtime ownership.
        /// </summary>
        private readonly Action<BackFillerCertificateBundle> _disposeUntransferredEvaluatedBundle;

        /// <summary>
        /// Initializes one certificate provisioning coordinator.
        /// </summary>
        /// <param name="certificateStore">Certificate store.</param>
        /// <param name="acmeIssuer">ACME certificate issuer.</param>
        /// <param name="certificateState">Runtime active certificate state.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="timeProvider">Unified time provider.</param>
        public BackFillerCertificateProvisioningService(
            BackFillerCertificateStore certificateStore,
            IAcmeCertificateIssuer acmeIssuer,
            BackFillerCertificateState certificateState,
            ILogger<BackFillerCertificateProvisioningService> logger,
            TimeProvider timeProvider)
            : this(certificateStore, acmeIssuer, certificateState, logger, timeProvider, evaluateExistingCertificateAsync: null, provisionNewCertificateAsync: null, disposeUntransferredEvaluatedBundle: null)
        {
        }

        /// <summary>
        /// Initializes one certificate provisioning coordinator with optional test seams for evaluation/provision operations.
        /// </summary>
        /// <param name="certificateStore">Certificate store.</param>
        /// <param name="acmeIssuer">ACME certificate issuer.</param>
        /// <param name="certificateState">Runtime active certificate state.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="timeProvider">Unified time provider.</param>
        /// <param name="evaluateExistingCertificateAsync">Optional evaluation delegate; defaults to <see cref="BackFillerCertificateStore.EvaluateExistingCertificateAsync(BackFillerLetsEncryptRuntimeOptions, TimeProvider, CancellationToken)"/>.</param>
        /// <param name="provisionNewCertificateAsync">Optional provisioning delegate; defaults to the built-in ACME persistence/reload workflow.</param>
        /// <param name="disposeUntransferredEvaluatedBundle">Optional evaluated-bundle disposal delegate; defaults to disposing <see cref="BackFillerCertificateBundle.Certificate"/>.</param>
        internal BackFillerCertificateProvisioningService(
            BackFillerCertificateStore certificateStore,
            IAcmeCertificateIssuer acmeIssuer,
            BackFillerCertificateState certificateState,
            ILogger<BackFillerCertificateProvisioningService> logger,
            TimeProvider timeProvider,
            Func<BackFillerLetsEncryptRuntimeOptions, TimeProvider, CancellationToken, Task<CertificateEvaluationResult>>? evaluateExistingCertificateAsync,
            Func<BackFillerLetsEncryptRuntimeOptions, CancellationToken, Task>? provisionNewCertificateAsync,
            Action<BackFillerCertificateBundle>? disposeUntransferredEvaluatedBundle)
        {
            ArgumentNullException.ThrowIfNull(certificateStore);
            ArgumentNullException.ThrowIfNull(acmeIssuer);
            ArgumentNullException.ThrowIfNull(certificateState);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(timeProvider);

            _acmeIssuer = acmeIssuer;
            _certificateState = certificateState;
            _logger = logger;
            _timeProvider = timeProvider;
            _evaluateExistingCertificateAsync = evaluateExistingCertificateAsync ?? BackFillerCertificateStore.EvaluateExistingCertificateAsync;
            _provisionNewCertificateAsync = provisionNewCertificateAsync ?? ProvisionNewCertificateAsync;
            _disposeUntransferredEvaluatedBundle = disposeUntransferredEvaluatedBundle ?? (static bundle => bundle.Certificate.Dispose());
        }

        /// <summary>
        /// Ensures a usable listener certificate exists and is published into runtime state.
        /// </summary>
        /// <remarks>
        /// Callers serialize through <see cref="ProvisionGate"/> so only one evaluation or issuance workflow runs at a time.
        /// </remarks>
        /// <param name="runtimeOptions">Validated runtime options snapshot that provides the effective ACME policy.</param>
        /// <param name="cancellationToken">Cancellation token that aborts evaluation or provisioning.</param>
        /// <returns>A task that completes when certificate availability has been decided and applied.</returns>
        public async Task EnsureCertificateAvailabilityAsync(
            BackFillerRuntimeOptions runtimeOptions,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions = runtimeOptions.EffectiveLetsEncrypt;

            await ProvisionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await EnsureCertificateAvailabilityCoreAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = ProvisionGate.Release();
            }
        }

        /// <summary>
        /// Attempts certificate renewal when the persisted listener certificate is due.
        /// </summary>
        /// <remarks>
        /// If renewal fails but the existing certificate remains usable, that existing certificate is republished and the
        /// method reports <see langword="false"/> so the listener can continue serving traffic.
        /// </remarks>
        /// <param name="runtimeOptions">Validated runtime options snapshot that provides the effective ACME policy.</param>
        /// <param name="cancellationToken">Cancellation token that aborts evaluation or renewal.</param>
        /// <returns><see langword="true"/> when a new certificate was issued and activated; otherwise <see langword="false"/>.</returns>
        public async Task<bool> TryRenewIfDueAsync(BackFillerRuntimeOptions runtimeOptions, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions = runtimeOptions.EffectiveLetsEncrypt;

            await ProvisionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                CertificateEvaluationResult evaluation = await _evaluateExistingCertificateAsync(letsEncryptOptions, _timeProvider, cancellationToken).ConfigureAwait(false);
                bool evaluationCertificateTransferred = false;

                try
                {
                    if (!evaluation.RequiresRenewal)
                    {
                        if (evaluation.Certificate is not null)
                        {
                            _certificateState.Publish(evaluation.Certificate);
                            evaluationCertificateTransferred = true;
                        }

                        return false;
                    }

                    if (!evaluation.IsUsable)
                    {
                        LogCertificateRenewalRequiredWithUnusableCertificate(_logger, evaluation.Reason);
                    }

                    try
                    {
                        await _provisionNewCertificateAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
                        return true;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        if (evaluation.IsUsable && evaluation.Certificate is not null)
                        {
                            LogCertificateRenewalFailedUsingExistingCertificate(_logger, ex);
                            _certificateState.Publish(evaluation.Certificate);
                            evaluationCertificateTransferred = true;
                            return false;
                        }

                        throw;
                    }
                }
                finally
                {
                    if (!evaluationCertificateTransferred && evaluation.Certificate is not null)
                    {
                        _disposeUntransferredEvaluatedBundle(evaluation.Certificate);
                    }
                }
            }
            finally
            {
                _ = ProvisionGate.Release();
            }
        }

        /// <summary>
        /// Applies evaluation results by activating an existing certificate or provisioning a replacement.
        /// </summary>
        /// <remarks>
        /// Startup uses this core path after entering <see cref="ProvisionGate"/> so only one caller decides whether the
        /// listener can reuse persisted state, attempt renewal, or require fresh issuance.
        /// </remarks>
        private async Task EnsureCertificateAvailabilityCoreAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken)
        {
            CertificateEvaluationResult evaluation = await _evaluateExistingCertificateAsync(letsEncryptOptions, _timeProvider, cancellationToken).ConfigureAwait(false);
            bool evaluationCertificateTransferred = false;

            try
            {
                if (evaluation.IsUsable && !evaluation.RequiresRenewal && evaluation.Certificate is not null)
                {
                    LogUsingExistingListenerCertificate(_logger, evaluation.Reason);
                    _certificateState.Publish(evaluation.Certificate);
                    evaluationCertificateTransferred = true;
                    return;
                }

                if (evaluation.IsUsable && evaluation.RequiresRenewal && evaluation.Certificate is not null)
                {
                    LogListenerCertificateInsideRenewalWindow(_logger);
                    try
                    {
                        await _provisionNewCertificateAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
                        return;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        LogCertificateRenewalFailedRetainingExistingCertificate(_logger, ex);
                        _certificateState.Publish(evaluation.Certificate);
                        evaluationCertificateTransferred = true;
                        return;
                    }
                }

                LogListenerCertificateUnavailableOrUnusable(_logger, evaluation.Reason);
                await _provisionNewCertificateAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (!evaluationCertificateTransferred && evaluation.Certificate is not null)
                {
                    _disposeUntransferredEvaluatedBundle(evaluation.Certificate);
                }
            }
        }

        /// <summary>
        /// Issues, persists, reloads, and activates a replacement listener certificate.
        /// </summary>
        /// <remarks>
        /// The workflow intentionally reloads the persisted artifacts before publication so activation reflects the same
        /// on-disk material that later restarts will consume.
        /// </remarks>
        /// <param name="letsEncryptOptions">Validated ACME runtime options for the certificate target being renewed.</param>
        /// <param name="cancellationToken">Cancellation token observed between issuance, persistence, reload, and activation.</param>
        private async Task ProvisionNewCertificateAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            AcmeOrderIssueResult issued;
            try
            {
                issued = await _acmeIssuer
                    .IssueCertificateAsync(letsEncryptOptions, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogCertificateIssuanceFailed(_logger, letsEncryptOptions.CanonicalCertificateSubjectName, letsEncryptOptions.CertificatePfxPath, letsEncryptOptions.CertificatePrivateKeyPemPath, ex);
                throw;
            }

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await BackFillerCertificateStore.PersistIssuedCertificateAsync(letsEncryptOptions, issued, cancellationToken, _logger).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogCertificatePersistenceFailed(_logger, letsEncryptOptions.CanonicalCertificateSubjectName, letsEncryptOptions.CertificatePfxPath, letsEncryptOptions.CertificatePrivateKeyPemPath, ex);
                throw;
            }

            BackFillerCertificateBundle activated;
            try
            {
                activated = await BackFillerCertificateStore
                    .LoadCertificateBundleAsync(letsEncryptOptions, _timeProvider, cancellationToken, _logger)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogCertificateReloadFailed(_logger, letsEncryptOptions.CanonicalCertificateSubjectName, letsEncryptOptions.CertificatePfxPath, letsEncryptOptions.CertificatePrivateKeyPemPath, ex);
                throw;
            }

            _certificateState.Publish(activated);
            DateTimeOffset activatedNotAfterUtc = activated.Certificate.NotAfter.ToUniversalTime();
            LogListenerCertificateActivatedSuccessfully(
                _logger,
                activated.Certificate.Subject,
                activatedNotAfterUtc);
        }

        /// <summary>
        /// Performs no resource cleanup beyond suppressing finalization for the DI-managed service instance.
        /// </summary>
        /// <remarks>
        /// The provisioning workflow does not own per-instance unmanaged resources. The static gate remains available for
        /// the lifetime of the process.
        /// </remarks>
        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Emits the warning log indicating renewal is required but the existing certificate cannot be reused.
        /// </summary>
        /// <param name="logger">Logger receiving the renewal-required event.</param>
        /// <param name="reason">Reason the existing certificate cannot be reused.</param>
        [LoggerMessage(EventId = 2712, Level = LogLevel.Warning, Message = "Certificate renewal required with unusable certificate: {Reason}")]
        private static partial void LogCertificateRenewalRequiredWithUnusableCertificate(ILogger logger, string reason);

        /// <summary>
        /// Emits the warning log indicating renewal failed but the previous certificate remains usable.
        /// </summary>
        /// <param name="logger">Logger receiving the renewal-failed event.</param>
        /// <param name="exception">Exception describing the renewal failure.</param>
        [LoggerMessage(EventId = 2713, Level = LogLevel.Warning, Message = "Certificate renewal failed; continuing with active valid certificate.")]
        private static partial void LogCertificateRenewalFailedUsingExistingCertificate(ILogger logger, Exception exception);

        /// <summary>
        /// Emits the informational log indicating the service is reusing the current listener certificate.
        /// </summary>
        /// <param name="logger">Logger receiving the reuse event.</param>
        /// <param name="reason">Reason the existing listener certificate is being reused.</param>
        [LoggerMessage(EventId = 2714, Level = LogLevel.Information, Message = "Using existing listener certificate: {Reason}")]
        private static partial void LogUsingExistingListenerCertificate(ILogger logger, string reason);

        /// <summary>
        /// Emits the informational log indicating renewal is about to be attempted inside the renewal window.
        /// </summary>
        /// <param name="logger">Logger receiving the renewal-window event.</param>
        [LoggerMessage(EventId = 2715, Level = LogLevel.Information, Message = "Listener certificate inside renewal window; attempting renewal.")]
        private static partial void LogListenerCertificateInsideRenewalWindow(ILogger logger);

        /// <summary>
        /// Emits the warning log indicating renewal failed but the existing valid certificate can still be retained.
        /// </summary>
        /// <param name="logger">Logger receiving the retained-certificate event.</param>
        /// <param name="exception">Exception describing the renewal failure.</param>
        [LoggerMessage(EventId = 2716, Level = LogLevel.Warning, Message = "Certificate renewal failed; retaining existing valid certificate.")]
        private static partial void LogCertificateRenewalFailedRetainingExistingCertificate(ILogger logger, Exception exception);

        /// <summary>
        /// Emits the informational log indicating the current certificate cannot be reused and ACME provisioning must begin.
        /// </summary>
        /// <param name="logger">Logger receiving the ACME-provisioning event.</param>
        /// <param name="reason">Reason the current listener certificate cannot be reused.</param>
        [LoggerMessage(EventId = 2717, Level = LogLevel.Information, Message = "Listener certificate unavailable or unusable: {Reason}. Starting ACME provisioning.")]
        private static partial void LogListenerCertificateUnavailableOrUnusable(ILogger logger, string reason);

        /// <summary>
        /// Emits the informational log indicating a newly provisioned listener certificate becomes active.
        /// </summary>
        /// <param name="logger">Logger receiving the activation event.</param>
        /// <param name="subject">Subject name of the activated listener certificate.</param>
        /// <param name="notAfterUtc">UTC expiration timestamp of the activated listener certificate.</param>
        [LoggerMessage(EventId = 2718, Level = LogLevel.Information, Message = "Listener certificate activated successfully; Subject={Subject}; NotAfterUtc={NotAfterUtc}")]
        private static partial void LogListenerCertificateActivatedSuccessfully(ILogger logger, string subject, DateTimeOffset notAfterUtc);

        /// <summary>
        /// Emits the error log indicating ACME issuance failed for the configured listener certificate target.
        /// </summary>
        /// <param name="logger">Logger receiving the issuance-failure event.</param>
        /// <param name="fqdn">Generated listener FQDN associated with the ACME order.</param>
        /// <param name="certificatePfxPath">Output path for the generated PFX artifact.</param>
        /// <param name="certificatePrivateKeyPemPath">Output path for the generated private-key PEM artifact.</param>
        /// <param name="exception">Exception captured from the ACME issuance failure path.</param>
        [LoggerMessage(EventId = 2719, Level = LogLevel.Error, Message = "ACME certificate issuance failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}")]
        private static partial void LogCertificateIssuanceFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception);

        /// <summary>
        /// Emits the error log indicating persistence of newly issued certificate artifacts failed.
        /// </summary>
        /// <param name="logger">Logger receiving the persistence-failure event.</param>
        /// <param name="fqdn">Generated listener FQDN associated with the ACME order.</param>
        /// <param name="certificatePfxPath">Output path for the generated PFX artifact.</param>
        /// <param name="certificatePrivateKeyPemPath">Output path for the generated private-key PEM artifact.</param>
        /// <param name="exception">Exception captured from the persistence failure path.</param>
        [LoggerMessage(EventId = 2720, Level = LogLevel.Error, Message = "ACME certificate persistence failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}")]
        private static partial void LogCertificatePersistenceFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception);

        /// <summary>
        /// Emits the error log indicating reload of the persisted certificate bundle failed before activation.
        /// </summary>
        /// <param name="logger">Logger receiving the reload-failure event.</param>
        /// <param name="fqdn">Generated listener FQDN associated with the ACME order.</param>
        /// <param name="certificatePfxPath">Output path for the generated PFX artifact.</param>
        /// <param name="certificatePrivateKeyPemPath">Output path for the generated private-key PEM artifact.</param>
        /// <param name="exception">Exception captured from the reload failure path.</param>
        [LoggerMessage(EventId = 2710, Level = LogLevel.Error, Message = "ACME certificate reload failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}")]
        private static partial void LogCertificateReloadFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception);
    }
}
