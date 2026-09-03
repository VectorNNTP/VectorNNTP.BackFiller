// <copyright file="TransitPublisherStartupInitializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit publisher startup initializer behavior.

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Hosted-service initializer that brings the transit publisher to readiness before producer loops begin.
    /// </summary>
    internal sealed partial class TransitPublisherStartupInitializer(
        TransitPublisher transitPublisher,
        ILogger<TransitPublisherStartupInitializer> logger) : IHostedService
    {
        /// <summary>
        /// Transit publisher whose connection workers are initialized during startup.
        /// </summary>
        private readonly TransitPublisher _transitPublisher = transitPublisher ?? throw new ArgumentNullException(nameof(transitPublisher));

        /// <summary>
        /// Logger for transit startup lifecycle events.
        /// </summary>
        private readonly ILogger<TransitPublisherStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Initializes the transit publisher and blocks host startup until publisher readiness is established.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task that completes after transit connection workers have been initialized.</returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            LogTransitStartupInitializerBeginning(_logger);
            await _transitPublisher.InitializeAsync(cancellationToken).ConfigureAwait(false);
            LogTransitStartupInitializerCompleted(_logger, _transitPublisher.CurrentState);
        }

        /// <summary>
        /// Disposes the transit publisher during host shutdown.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token.</param>
        /// <returns>A task that completes after publisher teardown finishes.</returns>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _transitPublisher.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Declares the informational log emitted when transit publisher initialization begins.
        /// </summary>
        [LoggerMessage(EventId = 2206, Level = LogLevel.Information, Message = "Transit publisher startup initializer beginning connection initialization")]
        private static partial void LogTransitStartupInitializerBeginning(ILogger logger);

        /// <summary>
        /// Declares the informational log emitted after publisher initialization completes.
        /// </summary>
        [LoggerMessage(EventId = 2207, Level = LogLevel.Information, Message = "Transit publisher startup initializer completed; State={State}")]
        private static partial void LogTransitStartupInitializerCompleted(ILogger logger, TransitConnectionState state);
    }
}
