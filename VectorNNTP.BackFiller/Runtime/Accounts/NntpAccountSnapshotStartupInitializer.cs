// <copyright file="NntpAccountSnapshotStartupInitializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Accounts
// Implements the nntp account snapshot startup initializer behavior.

namespace VectorNNTP.Backfiller.Runtime.Accounts
{
    /// <summary>
    /// Hosted startup initializer that performs the initial NNTP account snapshot load.
    /// </summary>
    internal sealed partial class NntpAccountSnapshotStartupInitializer(
        MySqlNntpAccountSnapshotProvider snapshotProvider,
        ILogger<NntpAccountSnapshotStartupInitializer> logger) : IHostedService
    {
        /// <summary>
        /// Tracks snapshot provider for nntp account snapshot startup initializer.
        /// </summary>
        private readonly MySqlNntpAccountSnapshotProvider _snapshotProvider = snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));
        /// <summary>
        /// Provides logging for nntp account snapshot startup initializer.
        /// </summary>
        private readonly ILogger<NntpAccountSnapshotStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Performs the startup-time account snapshot load and blocks host startup until complete.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task that completes after initial snapshot load succeeds.</returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            LogStartupInitializerBeginning(_logger);
            await _snapshotProvider.EnsureStartupDependenciesAsync(cancellationToken).ConfigureAwait(false);
            await _snapshotProvider.LoadInitialSnapshotAsync(cancellationToken).ConfigureAwait(false);
            LogStartupInitializerCompleted(_logger, _snapshotProvider.CurrentSnapshot.Accounts.Count);
        }

        /// <summary>
        /// No-op stop behavior; snapshot remains in-memory for process lifetime.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token.</param>
        /// <returns>A completed task.</returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        [LoggerMessage(EventId = 2002, Level = LogLevel.Information, Message = "NNTP account startup initializer beginning initial snapshot load")]
        /// <summary>
        /// Coordinates log startup initializer beginning for nntp account snapshot startup initializer.
        /// </summary>
        private static partial void LogStartupInitializerBeginning(ILogger logger);

        [LoggerMessage(EventId = 2003, Level = LogLevel.Information, Message = "NNTP account startup initializer completed; AccountsLoaded={AccountCount}")]
        /// <summary>
        /// Coordinates log startup initializer completed for nntp account snapshot startup initializer.
        /// </summary>
        private static partial void LogStartupInitializerCompleted(ILogger logger, int accountCount);
    }
}
