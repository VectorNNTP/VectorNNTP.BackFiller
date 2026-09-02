// <copyright file="TransitPublisherStartupInitializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit publisher startup initializer behavior.

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Hosted startup initializer that performs transit publisher initialization before runtime loops begin.
    /// </summary>
    internal sealed partial class TransitPublisherStartupInitializer(
        TransitPublisher transitPublisher,
        ILogger<TransitPublisherStartupInitializer> logger) : IHostedService
    {
        /// <summary>
        /// Tracks transit publisher for transit publisher startup initializer.
        /// </summary>
        private readonly TransitPublisher _transitPublisher = transitPublisher ?? throw new ArgumentNullException(nameof(transitPublisher));
        /// <summary>
        /// Provides logging for transit publisher startup initializer.
        /// </summary>
        private readonly ILogger<TransitPublisherStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Performs startup-time transit connection initialization and blocks host startup until complete.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task that completes after transit publisher initialization succeeds.</returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            LogTransitStartupInitializerBeginning(_logger);
            await _transitPublisher.InitializeAsync(cancellationToken).ConfigureAwait(false);
            LogTransitStartupInitializerCompleted(_logger, _transitPublisher.CurrentState);
        }

        /// <summary>
        /// Disposes transit publisher resources when host shutdown begins.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token.</param>
        /// <returns>A task that completes after publisher disposal finishes.</returns>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _transitPublisher.DisposeAsync().ConfigureAwait(false);
        }

                /// <summary>
        /// Coordinates log transit startup initializer beginning for transit publisher startup initializer.
        /// </summary>
        [LoggerMessage(EventId = 2206, Level = LogLevel.Information, Message = "Transit publisher startup initializer beginning connection initialization")]
        private static partial void LogTransitStartupInitializerBeginning(ILogger logger);

                /// <summary>
        /// Coordinates log transit startup initializer completed for transit publisher startup initializer.
        /// </summary>
        [LoggerMessage(EventId = 2207, Level = LogLevel.Information, Message = "Transit publisher startup initializer completed; State={State}")]
        private static partial void LogTransitStartupInitializerCompleted(ILogger logger, TransitConnectionState state);
    }
}
