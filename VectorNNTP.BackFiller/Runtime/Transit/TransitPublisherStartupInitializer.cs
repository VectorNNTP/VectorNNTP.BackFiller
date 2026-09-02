// <copyright file="TransitPublisherStartupInitializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit publisher startup initializer responsibilities for this subsystem boundary.

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
        /// Stores the transit publisher state used to enforce this component's runtime contract.
        /// </summary>
        private readonly TransitPublisher _transitPublisher = transitPublisher ?? throw new ArgumentNullException(nameof(transitPublisher));
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
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

        [LoggerMessage(EventId = 2206, Level = LogLevel.Information, Message = "Transit publisher startup initializer beginning connection initialization")]
        /// <summary>
        /// Performs the log transit startup initializer beginning operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogTransitStartupInitializerBeginning(ILogger logger);

        [LoggerMessage(EventId = 2207, Level = LogLevel.Information, Message = "Transit publisher startup initializer completed; State={State}")]
        /// <summary>
        /// Performs the log transit startup initializer completed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static partial void LogTransitStartupInitializerCompleted(ILogger logger, TransitConnectionState state);
    }
}
