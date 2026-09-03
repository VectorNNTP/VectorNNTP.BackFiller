// <copyright file="BackFillerCertificateStartupInitializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Starts certificate provisioning before the inbound listener service begins accepting traffic.

using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Hosted startup initializer that verifies listener-certificate availability before the TLS listener starts.
    /// </summary>
    /// <remarks>
    /// This initializer makes certificate readiness part of host startup ordering instead of relying on the listener to
    /// discover missing or unusable certificate state after bind attempts have already begun.
    /// </remarks>
    internal sealed partial class BackFillerCertificateStartupInitializer(
        BackFillerRuntimeOptions runtimeOptions,
        BackFillerCertificateProvisioningService provisioningService,
        ILogger<BackFillerCertificateStartupInitializer> logger) : IHostedService
    {
        /// <summary>
        /// Validated runtime snapshot that determines whether certificate management is enabled.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));

        /// <summary>
        /// Provisioning coordinator that evaluates, issues, and publishes the active listener certificate.
        /// </summary>
        private readonly BackFillerCertificateProvisioningService _provisioningService = provisioningService ?? throw new ArgumentNullException(nameof(provisioningService));

        /// <summary>
        /// Logger used for startup availability diagnostics.
        /// </summary>
        private readonly ILogger<BackFillerCertificateStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Ensures certificate state is available for runtime listener services.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token propagated into certificate provisioning.</param>
        /// <returns>
        /// A task that completes after certificate availability has been confirmed or, when Let's Encrypt is disabled,
        /// immediately after the skip decision is logged.
        /// </returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (!_runtimeOptions.EffectiveLetsEncrypt.Enabled)
            {
                LogCertificateStartupInitializerDisabled(_logger);
                return;
            }

            LogCertificateStartupInitializerBeginning(_logger);
            await _provisioningService.EnsureCertificateAvailabilityAsync(_runtimeOptions, cancellationToken).ConfigureAwait(false);
            LogCertificateStartupInitializerCompleted(_logger);
        }

        /// <summary>
        /// Leaves shutdown work to the provisioning coordinator and runtime listener services.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token supplied by the host.</param>
        /// <returns>A completed task.</returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Defines the informational log emitted before startup certificate availability checks begin.
        /// </summary>
        [LoggerMessage(EventId = 2600, Level = LogLevel.Information, Message = "Certificate startup initializer beginning certificate availability verification")]
        private static partial void LogCertificateStartupInitializerBeginning(ILogger logger);

        /// <summary>
        /// Defines the informational log emitted after startup certificate state has been activated successfully.
        /// </summary>
        [LoggerMessage(EventId = 2601, Level = LogLevel.Information, Message = "Certificate startup initializer completed certificate state activation")]
        private static partial void LogCertificateStartupInitializerCompleted(ILogger logger);

        /// <summary>
        /// Defines the informational log emitted when startup certificate checks are skipped because ACME management is disabled.
        /// </summary>
        [LoggerMessage(EventId = 2602, Level = LogLevel.Information, Message = "Certificate startup initializer skipped because Let's Encrypt is disabled")]
        private static partial void LogCertificateStartupInitializerDisabled(ILogger logger);
    }
}
