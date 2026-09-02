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
    /// Hosted startup initializer that ensures the inbound listener certificate is available before listener services start.
    /// </summary>
    internal sealed partial class BackFillerCertificateStartupInitializer(
        BackFillerRuntimeOptions runtimeOptions,
        BackFillerCertificateProvisioningService provisioningService,
        ILogger<BackFillerCertificateStartupInitializer> logger) : IHostedService
    {
        /// <summary>
        /// Stores runtime options used by back filler certificate startup initializer.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
        /// <summary>
        /// Stores provisioning service used by back filler certificate startup initializer.
        /// </summary>
        private readonly BackFillerCertificateProvisioningService _provisioningService = provisioningService ?? throw new ArgumentNullException(nameof(provisioningService));
        /// <summary>
        /// Supplies the logger used by back filler certificate startup initializer.
        /// </summary>
        private readonly ILogger<BackFillerCertificateStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Ensures certificate state is available for runtime listener services.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task that completes after certificate provisioning and state publication succeed.</returns>
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
        /// No-op stop behavior; certificate state is managed by provisioning and listener runtime services.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token.</param>
        /// <returns>A completed task.</returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Emits the certificate startup initializer beginning log event for back filler certificate startup initializer.
        /// </summary>
        [LoggerMessage(EventId = 2600, Level = LogLevel.Information, Message = "Certificate startup initializer beginning certificate availability verification")]
        private static partial void LogCertificateStartupInitializerBeginning(ILogger logger);

        /// <summary>
        /// Emits the certificate startup initializer completed log event for back filler certificate startup initializer.
        /// </summary>
        [LoggerMessage(EventId = 2601, Level = LogLevel.Information, Message = "Certificate startup initializer completed certificate state activation")]
        private static partial void LogCertificateStartupInitializerCompleted(ILogger logger);

        /// <summary>
        /// Emits the certificate startup initializer disabled log event for back filler certificate startup initializer.
        /// </summary>
        [LoggerMessage(EventId = 2602, Level = LogLevel.Information, Message = "Certificate startup initializer skipped because Let's Encrypt is disabled")]
        private static partial void LogCertificateStartupInitializerDisabled(ILogger logger);
    }
}
