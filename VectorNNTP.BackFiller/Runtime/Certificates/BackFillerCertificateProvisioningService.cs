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
    internal sealed class BackFillerCertificateProvisioningService : IDisposable
    {
        /// <summary>
        /// Registered certificate-store dependency supplied when composing the provisioning stack.
        /// </summary>
        /// <remarks>
        /// The current implementation relies on <see cref="BackFillerCertificateStore"/>'s static helpers rather than
        /// invoking this instance directly.
        /// </remarks>
        private readonly BackFillerCertificateStore _certificateStore;
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
        {
            ArgumentNullException.ThrowIfNull(certificateStore);
            ArgumentNullException.ThrowIfNull(acmeIssuer);
            ArgumentNullException.ThrowIfNull(certificateState);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(timeProvider);

            _certificateStore = certificateStore;
            _acmeIssuer = acmeIssuer;
            _certificateState = certificateState;
            _logger = logger;
            _timeProvider = timeProvider;
        }

        /// <summary>
        /// Ensures a usable listener certificate exists and is published into runtime state.
        /// </summary>
        /// <remarks>
        /// When Let's Encrypt is disabled the method logs the skip decision and returns immediately. Otherwise callers
        /// serialize through <see cref="ProvisionGate"/> so only one evaluation or issuance workflow runs at a time.
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
            if (!letsEncryptOptions.Enabled)
            {
                LogCertificateProvisioningDisabled(_logger);
                return;
            }

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
            if (!letsEncryptOptions.Enabled)
            {
                return false;
            }

            await ProvisionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                CertificateEvaluationResult evaluation = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(letsEncryptOptions, _timeProvider, cancellationToken).ConfigureAwait(false);

                if (!evaluation.RequiresRenewal)
                {
                    if (evaluation.Certificate is not null)
                    {
                        _certificateState.Publish(evaluation.Certificate);
                    }

                    return false;
                }

                if (!evaluation.IsUsable)
                {
                    LogCertificateRenewalRequiredWithUnusableCertificate(_logger, evaluation.Reason);
                }

                try
                {
                    await ProvisionNewCertificateAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
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
                        return false;
                    }

                    throw;
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
            CertificateEvaluationResult evaluation = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(letsEncryptOptions, _timeProvider, cancellationToken).ConfigureAwait(false);

            if (evaluation.IsUsable && !evaluation.RequiresRenewal && evaluation.Certificate is not null)
            {
                LogUsingExistingListenerCertificate(_logger, evaluation.Reason);
                _certificateState.Publish(evaluation.Certificate);
                return;
            }

            if (evaluation.IsUsable && evaluation.RequiresRenewal && evaluation.Certificate is not null)
            {
                LogListenerCertificateInsideRenewalWindow(_logger);
                try
                {
                    await ProvisionNewCertificateAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
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
                    return;
                }
            }

            LogListenerCertificateUnavailableOrUnusable(_logger, evaluation.Reason);
            await ProvisionNewCertificateAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
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
                _logger.LogError(ex, "ACME certificate issuance failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}", letsEncryptOptions.CanonicalCertificateSubjectName, letsEncryptOptions.CertificatePfxPath, letsEncryptOptions.CertificatePrivateKeyPemPath);
                throw;
            }

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await BackFillerCertificateStore.PersistIssuedCertificateAsync(letsEncryptOptions, issued, cancellationToken, _logger).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ACME certificate persistence failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}", letsEncryptOptions.CanonicalCertificateSubjectName, letsEncryptOptions.CertificatePfxPath, letsEncryptOptions.CertificatePrivateKeyPemPath);
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
                _logger.LogError(ex, "ACME certificate reload failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}", letsEncryptOptions.CanonicalCertificateSubjectName, letsEncryptOptions.CertificatePfxPath, letsEncryptOptions.CertificatePrivateKeyPemPath);
                throw;
            }

            _certificateState.Publish(activated);
            LogListenerCertificateActivatedSuccessfully(
                _logger,
                activated.Certificate.Subject,
                activated.Certificate.NotAfter.ToUniversalTime());
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
        /// Logs when certificate provisioning is disabled by configuration.
        /// </summary>
        private static readonly Action<ILogger, Exception?> LogCertificateProvisioningDisabledMessage =
            LoggerMessage.Define(
                LogLevel.Warning,
                new EventId(2700, nameof(LogCertificateProvisioningDisabled)),
                "BackFiller TLS certificate provisioning is disabled by configuration.");

        /// <summary>
        /// Logs when renewal is required but the existing certificate cannot be reused.
        /// </summary>
        private static readonly Action<ILogger, string, Exception?> LogCertificateRenewalRequiredWithUnusableCertificateMessage =
            LoggerMessage.Define<string>(
                LogLevel.Warning,
                new EventId(2701, nameof(LogCertificateRenewalRequiredWithUnusableCertificate)),
                "Certificate renewal required with unusable certificate: {Reason}");

        /// <summary>
        /// Logs when renewal failed but the previous certificate remains usable.
        /// </summary>
        private static readonly Action<ILogger, Exception?> LogCertificateRenewalFailedUsingExistingCertificateMessage =
            LoggerMessage.Define(
                LogLevel.Warning,
                new EventId(2702, nameof(LogCertificateRenewalFailedUsingExistingCertificate)),
                "Certificate renewal failed; continuing with active valid certificate.");

        /// <summary>
        /// Logs when the service is reusing the current listener certificate.
        /// </summary>
        private static readonly Action<ILogger, string, Exception?> LogUsingExistingListenerCertificateMessage =
            LoggerMessage.Define<string>(
                LogLevel.Information,
                new EventId(2703, nameof(LogUsingExistingListenerCertificate)),
                "Using existing listener certificate: {Reason}");

        /// <summary>
        /// Logs when renewal is about to be attempted inside the renewal window.
        /// </summary>
        private static readonly Action<ILogger, Exception?> LogListenerCertificateInsideRenewalWindowMessage =
            LoggerMessage.Define(
                LogLevel.Information,
                new EventId(2704, nameof(LogListenerCertificateInsideRenewalWindow)),
                "Listener certificate inside renewal window; attempting renewal.");

        /// <summary>
        /// Logs when renewal failed but the existing valid certificate can still be retained.
        /// </summary>
        private static readonly Action<ILogger, Exception?> LogCertificateRenewalFailedRetainingExistingCertificateMessage =
            LoggerMessage.Define(
                LogLevel.Warning,
                new EventId(2705, nameof(LogCertificateRenewalFailedRetainingExistingCertificate)),
                "Certificate renewal failed; retaining existing valid certificate.");

        /// <summary>
        /// Logs when the current certificate cannot be reused and ACME provisioning must begin.
        /// </summary>
        private static readonly Action<ILogger, string, Exception?> LogListenerCertificateUnavailableOrUnusableMessage =
            LoggerMessage.Define<string>(
                LogLevel.Information,
                new EventId(2706, nameof(LogListenerCertificateUnavailableOrUnusable)),
                "Listener certificate unavailable or unusable: {Reason}. Starting ACME provisioning.");

        /// <summary>
        /// Logs when a newly provisioned listener certificate becomes active.
        /// </summary>
        private static readonly Action<ILogger, string, DateTimeOffset, Exception?> LogListenerCertificateActivatedSuccessfullyMessage =
            LoggerMessage.Define<string, DateTimeOffset>(
                LogLevel.Information,
                new EventId(2707, nameof(LogListenerCertificateActivatedSuccessfully)),
                "Listener certificate activated successfully; Subject={Subject}; NotAfterUtc={NotAfterUtc}");

        /// <summary>
        /// Emits the warning log indicating certificate provisioning is disabled by configuration.
        /// </summary>
        private static void LogCertificateProvisioningDisabled(ILogger logger)
        {
            LogCertificateProvisioningDisabledMessage(logger, null);
        }

        /// <summary>
        /// Emits the warning log indicating renewal is required and the currently loaded certificate cannot be reused.
        /// </summary>
        private static void LogCertificateRenewalRequiredWithUnusableCertificate(ILogger logger, string reason)
        {
            LogCertificateRenewalRequiredWithUnusableCertificateMessage(logger, reason, null);
        }

        /// <summary>
        /// Emits the warning log indicating renewal failed but the previously active certificate remains usable.
        /// </summary>
        private static void LogCertificateRenewalFailedUsingExistingCertificate(ILogger logger, Exception exception)
        {
            LogCertificateRenewalFailedUsingExistingCertificateMessage(logger, exception);
        }

        /// <summary>
        /// Emits the informational log indicating the persisted listener certificate was reused without renewal.
        /// </summary>
        private static void LogUsingExistingListenerCertificate(ILogger logger, string reason)
        {
            LogUsingExistingListenerCertificateMessage(logger, reason, null);
        }

        /// <summary>
        /// Emits the informational log indicating renewal will be attempted because the current certificate is inside its renewal window.
        /// </summary>
        private static void LogListenerCertificateInsideRenewalWindow(ILogger logger)
        {
            LogListenerCertificateInsideRenewalWindowMessage(logger, null);
        }

        /// <summary>
        /// Emits the warning log indicating startup renewal failed and the existing valid certificate will remain active.
        /// </summary>
        private static void LogCertificateRenewalFailedRetainingExistingCertificate(ILogger logger, Exception exception)
        {
            LogCertificateRenewalFailedRetainingExistingCertificateMessage(logger, exception);
        }

        /// <summary>
        /// Emits the informational log indicating persisted certificate state cannot be used and fresh ACME provisioning will start.
        /// </summary>
        private static void LogListenerCertificateUnavailableOrUnusable(ILogger logger, string reason)
        {
            LogListenerCertificateUnavailableOrUnusableMessage(logger, reason, null);
        }

        /// <summary>
        /// Emits the informational log indicating a newly loaded listener certificate bundle became active.
        /// </summary>
        private static void LogListenerCertificateActivatedSuccessfully(ILogger logger, string subject, DateTimeOffset notAfterUtc)
        {
            LogListenerCertificateActivatedSuccessfullyMessage(logger, subject, notAfterUtc, null);
        }

        /// <summary>
        /// Precompiled error log for ACME issuance failures before persistence begins.
        /// </summary>
        private static readonly Action<ILogger, string, string, string, Exception?> LogCertificateIssuanceFailedMessage =
            LoggerMessage.Define<string, string, string>(
                LogLevel.Error,
                new EventId(2708, nameof(LogCertificateIssuanceFailed)),
                "ACME certificate issuance failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}");

        /// <summary>
        /// Precompiled error log for failures while persisting newly issued certificate artifacts.
        /// </summary>
        private static readonly Action<ILogger, string, string, string, Exception?> LogCertificatePersistenceFailedMessage =
            LoggerMessage.Define<string, string, string>(
                LogLevel.Error,
                new EventId(2709, nameof(LogCertificatePersistenceFailed)),
                "ACME certificate persistence failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}");

        /// <summary>
        /// Precompiled error log for failures while reloading persisted certificate artifacts before activation.
        /// </summary>
        private static readonly Action<ILogger, string, string, string, Exception?> LogCertificateReloadFailedMessage =
            LoggerMessage.Define<string, string, string>(
                LogLevel.Error,
                new EventId(2710, nameof(LogCertificateReloadFailed)),
                "ACME certificate reload failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}");

        /// <summary>
        /// Emits the error log indicating ACME issuance failed for the configured listener certificate target.
        /// </summary>
        private static void LogCertificateIssuanceFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception)
        {
            LogCertificateIssuanceFailedMessage(logger, fqdn, certificatePfxPath, certificatePrivateKeyPemPath, exception);
        }

        /// <summary>
        /// Emits the error log indicating persistence of newly issued certificate artifacts failed.
        /// </summary>
        private static void LogCertificatePersistenceFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception)
        {
            LogCertificatePersistenceFailedMessage(logger, fqdn, certificatePfxPath, certificatePrivateKeyPemPath, exception);
        }

        /// <summary>
        /// Emits the error log indicating reload of the persisted certificate bundle failed before activation.
        /// </summary>
        private static void LogCertificateReloadFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception)
        {
            LogCertificateReloadFailedMessage(logger, fqdn, certificatePfxPath, certificatePrivateKeyPemPath, exception);
        }
    }
}
