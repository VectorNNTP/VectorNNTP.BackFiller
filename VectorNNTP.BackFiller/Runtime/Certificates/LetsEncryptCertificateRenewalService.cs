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
    internal sealed partial class LetsEncryptCertificateRenewalService : BackgroundService
    {
        /// <summary>
        /// Stores the runtime options state used to enforce this component's runtime contract.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions;
        /// <summary>
        /// Stores the provisioning service state used to enforce this component's runtime contract.
        /// </summary>
        private readonly BackFillerCertificateProvisioningService _provisioningService;
        /// <summary>
        /// Stores the shutdown coordinator state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ShutdownCoordinator _shutdownCoordinator;
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<LetsEncryptCertificateRenewalService> _logger;

        /// <summary>
        /// Initializes the renewal service.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options.</param>
        /// <param name="provisioningService">Certificate provisioning coordinator.</param>
        /// <param name="shutdownCoordinator">Shutdown coordinator.</param>
        /// <param name="logger">Logger.</param>
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

        /// <inheritdoc/>
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
        /// Performs the compute next delay operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
