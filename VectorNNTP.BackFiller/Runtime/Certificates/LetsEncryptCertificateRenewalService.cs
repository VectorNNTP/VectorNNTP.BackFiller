// <copyright file="LetsEncryptCertificateRenewalService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Periodically re-evaluates and renews the BackFiller listener certificate when needed.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Shutdown;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Periodically re-evaluates the BackFiller listener certificate and renews it when the configured threshold is reached.
    /// </summary>
    /// <remarks>
    /// The background loop links host shutdown with graceful-shutdown signaling so renewal work stops before the
    /// listener shutdown sequence finishes. Renewal failures are logged and retried on later intervals instead of
    /// terminating the hosted service.
    /// </remarks>
    internal sealed partial class LetsEncryptCertificateRenewalService : BackgroundService
    {
        /// <summary>
        /// Validated runtime snapshot that supplies the effective ACME renewal policy.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions;

        /// <summary>
        /// Provisioning coordinator that decides whether renewal is due and activates replacement certificates.
        /// </summary>
        private readonly BackFillerCertificateProvisioningService _provisioningService;

        /// <summary>
        /// Shutdown coordinator whose graceful token stops new renewal iterations during service shutdown.
        /// </summary>
        private readonly ShutdownCoordinator _shutdownCoordinator;

        /// <summary>
        /// Logger used for renewal-loop outcome diagnostics.
        /// </summary>
        private readonly ILogger<LetsEncryptCertificateRenewalService> _logger;

        /// <summary>
        /// Initializes the certificate-renewal background service.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options that provide effective Let's Encrypt policy.</param>
        /// <param name="provisioningService">Certificate provisioning coordinator.</param>
        /// <param name="shutdownCoordinator">Shutdown coordinator used to stop renewal work during graceful shutdown.</param>
        /// <param name="logger">Logger for renewal-loop diagnostics.</param>
        public LetsEncryptCertificateRenewalService(
            BackFillerRuntimeOptions runtimeOptions,
            BackFillerCertificateProvisioningService provisioningService,
            ShutdownCoordinator shutdownCoordinator,
            ILogger<LetsEncryptCertificateRenewalService> logger)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(provisioningService);
            ArgumentNullException.ThrowIfNull(shutdownCoordinator);
            ArgumentNullException.ThrowIfNull(logger);

            _runtimeOptions = runtimeOptions;
            _provisioningService = provisioningService;
            _shutdownCoordinator = shutdownCoordinator;
            _logger = logger;
        }

        /// <summary>
        /// Runs the periodic renewal loop until hosted-service or graceful-shutdown cancellation is requested.
        /// </summary>
        /// <param name="stoppingToken">Host-managed cancellation token for the background service.</param>
        /// <returns>A task that completes when the renewal loop exits.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            BackFillerLetsEncryptRuntimeOptions letsEncrypt = _runtimeOptions.EffectiveLetsEncrypt;
            if (!letsEncrypt.Enabled)
            {
                LogServiceDisabled(_logger);
                return;
            }

            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _shutdownCoordinator.GracefulShutdownStartedToken);
            CancellationToken token = linked.Token;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    TimeSpan delay = ComputeNextDelay(letsEncrypt);
                    await Task.Delay(delay, token).ConfigureAwait(false);

                    token.ThrowIfCancellationRequested();
                    bool renewed = await _provisioningService.TryRenewIfDueAsync(_runtimeOptions, token).ConfigureAwait(false);
                    if (renewed)
                    {
                        LogRenewalSucceeded(_logger);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogRenewalIterationFailed(_logger, ex);
                }
            }
        }

        /// <summary>
        /// Computes the next renewal-check delay by applying symmetric jitter to the configured interval.
        /// </summary>
        /// <param name="options">Validated ACME runtime options that define the base interval and jitter ratio.</param>
        /// <returns>A delay that is never shorter than six minutes.</returns>
        private static TimeSpan ComputeNextDelay(BackFillerLetsEncryptRuntimeOptions options)
        {
            double hours = options.RenewalCheckIntervalHours;
            double jitterRatio = options.RenewalJitterRatio;

            double jitter = hours * jitterRatio;
            double delta = jitter <= 0d
                ? 0d
                : (Random.Shared.NextDouble() * 2d * jitter) - jitter;

            double effectiveHours = Math.Max(0.1d, hours + delta);
            return TimeSpan.FromHours(effectiveHours);
        }

    }
}
