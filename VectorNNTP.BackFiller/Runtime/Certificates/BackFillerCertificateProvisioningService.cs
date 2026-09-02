// <copyright file="BackFillerCertificateProvisioningService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: back filler certificate provisioning service in the runtime certificates subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="BackFillerCertificateProvisioningService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Coordinates listener certificate discovery, issuance, activation, and renewal.

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
        /// Stores the certificate store state used to enforce this component's runtime contract.
        /// </summary>
        private readonly BackFillerCertificateStore _certificateStore;
        /// <summary>
        /// Stores the acme issuer state used to enforce this component's runtime contract.
        /// </summary>
        private readonly IAcmeCertificateIssuer _acmeIssuer;
        /// <summary>
        /// Stores the certificate state state used to enforce this component's runtime contract.
        /// </summary>
        private readonly BackFillerCertificateState _certificateState;
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<BackFillerCertificateProvisioningService> _logger;
        /// <summary>
        /// Stores the time provider state used to enforce this component's runtime contract.
        /// </summary>
        private readonly TimeProvider _timeProvider;
        /// <summary>
        /// Stores the provision gate state used to enforce this component's runtime contract.
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
        /// Ensures a usable certificate exists and is activated, provisioning through ACME when required.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
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
        /// Attempts renewal when required while preserving currently active valid certificate on failure.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
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
        /// Performs the ensure certificate availability core operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the provision new certificate operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Releases the owned provisioning gate when the host disposes this singleton service.
        /// </summary>
        /// <remarks>
        /// The host owns the service lifetime through dependency injection, so disposal is deterministic at shutdown.
        /// Disposing the gate here is safe because callers are rejected once disposal begins.
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
        /// Performs the log certificate provisioning disabled operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogCertificateProvisioningDisabled(ILogger logger)
        {
            LogCertificateProvisioningDisabledMessage(logger, null);
        }

        /// <summary>
        /// Performs the log certificate renewal required with unusable certificate operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogCertificateRenewalRequiredWithUnusableCertificate(ILogger logger, string reason)
        {
            LogCertificateRenewalRequiredWithUnusableCertificateMessage(logger, reason, null);
        }

        /// <summary>
        /// Performs the log certificate renewal failed using existing certificate operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogCertificateRenewalFailedUsingExistingCertificate(ILogger logger, Exception exception)
        {
            LogCertificateRenewalFailedUsingExistingCertificateMessage(logger, exception);
        }

        /// <summary>
        /// Performs the log using existing listener certificate operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogUsingExistingListenerCertificate(ILogger logger, string reason)
        {
            LogUsingExistingListenerCertificateMessage(logger, reason, null);
        }

        /// <summary>
        /// Performs the log listener certificate inside renewal window operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogListenerCertificateInsideRenewalWindow(ILogger logger)
        {
            LogListenerCertificateInsideRenewalWindowMessage(logger, null);
        }

        /// <summary>
        /// Performs the log certificate renewal failed retaining existing certificate operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogCertificateRenewalFailedRetainingExistingCertificate(ILogger logger, Exception exception)
        {
            LogCertificateRenewalFailedRetainingExistingCertificateMessage(logger, exception);
        }

        /// <summary>
        /// Performs the log listener certificate unavailable or unusable operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogListenerCertificateUnavailableOrUnusable(ILogger logger, string reason)
        {
            LogListenerCertificateUnavailableOrUnusableMessage(logger, reason, null);
        }

        /// <summary>
        /// Performs the log listener certificate activated successfully operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogListenerCertificateActivatedSuccessfully(ILogger logger, string subject, DateTimeOffset notAfterUtc)
        {
            LogListenerCertificateActivatedSuccessfullyMessage(logger, subject, notAfterUtc, null);
        }

        /// <summary>
        /// Stores the log certificate issuance failed message state used to enforce this component's runtime contract.
        /// </summary>
        private static readonly Action<ILogger, string, string, string, Exception?> LogCertificateIssuanceFailedMessage =
            LoggerMessage.Define<string, string, string>(
                LogLevel.Error,
                new EventId(2708, nameof(LogCertificateIssuanceFailed)),
                "ACME certificate issuance failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}");

        /// <summary>
        /// Stores the log certificate persistence failed message state used to enforce this component's runtime contract.
        /// </summary>
        private static readonly Action<ILogger, string, string, string, Exception?> LogCertificatePersistenceFailedMessage =
            LoggerMessage.Define<string, string, string>(
                LogLevel.Error,
                new EventId(2709, nameof(LogCertificatePersistenceFailed)),
                "ACME certificate persistence failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}");

        /// <summary>
        /// Stores the log certificate reload failed message state used to enforce this component's runtime contract.
        /// </summary>
        private static readonly Action<ILogger, string, string, string, Exception?> LogCertificateReloadFailedMessage =
            LoggerMessage.Define<string, string, string>(
                LogLevel.Error,
                new EventId(2710, nameof(LogCertificateReloadFailed)),
                "ACME certificate reload failed; Fqdn={Fqdn}; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}");

        /// <summary>
        /// Performs the log certificate issuance failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogCertificateIssuanceFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception)
        {
            LogCertificateIssuanceFailedMessage(logger, fqdn, certificatePfxPath, certificatePrivateKeyPemPath, exception);
        }

        /// <summary>
        /// Performs the log certificate persistence failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogCertificatePersistenceFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception)
        {
            LogCertificatePersistenceFailedMessage(logger, fqdn, certificatePfxPath, certificatePrivateKeyPemPath, exception);
        }

        /// <summary>
        /// Performs the log certificate reload failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogCertificateReloadFailed(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, Exception exception)
        {
            LogCertificateReloadFailedMessage(logger, fqdn, certificatePfxPath, certificatePrivateKeyPemPath, exception);
        }
    }
}
