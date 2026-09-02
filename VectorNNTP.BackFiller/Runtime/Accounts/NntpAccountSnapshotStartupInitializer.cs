// <copyright file="NntpAccountSnapshotStartupInitializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Accounts
// Implements the nntp account snapshot startup initializer behavior.

namespace VectorNNTP.Backfiller.Runtime.Accounts
{
    /// <summary>
    /// Hosted startup gate that loads the initial NNTP account snapshot before runtime services begin processing.
    /// </summary>
    /// <remarks>
    /// This initializer participates in host startup ordering via <see cref="IHostedService"/> and ensures
    /// snapshot dependencies are ready before account-dependent runtime paths execute.
    /// </remarks>
    internal sealed partial class NntpAccountSnapshotStartupInitializer(
        MySqlNntpAccountSnapshotProvider snapshotProvider,
        ILogger<NntpAccountSnapshotStartupInitializer> logger) : IHostedService
    {
        /// <summary>
        /// Snapshot provider responsible for startup dependency checks and initial account-state load.
        /// </summary>
        private readonly MySqlNntpAccountSnapshotProvider _snapshotProvider = snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));
        /// <summary>
        /// Supplies the logger used by nntp account snapshot startup initializer.
        /// </summary>
        private readonly ILogger<NntpAccountSnapshotStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Executes startup-time account snapshot initialization and blocks host startup until completion.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token propagated to dependency and snapshot-load operations.</param>
        /// <returns>A task that completes after startup dependencies are satisfied and the initial snapshot is loaded.</returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            LogStartupInitializerBeginning(_logger);
            await _snapshotProvider.EnsureStartupDependenciesAsync(cancellationToken).ConfigureAwait(false);
            await _snapshotProvider.LoadInitialSnapshotAsync(cancellationToken).ConfigureAwait(false);
            LogStartupInitializerCompleted(_logger, _snapshotProvider.CurrentSnapshot.Accounts.Count);
        }

        /// <summary>
        /// Performs no shutdown work because loaded snapshot state is owned by the provider for process lifetime.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token.</param>
        /// <returns>A completed task.</returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Emits the startup marker indicating initial NNTP account snapshot loading is beginning.
        /// </summary>
        /// <param name="logger">Logger receiving the startup marker event.</param>
        [LoggerMessage(EventId = 2002, Level = LogLevel.Information, Message = "NNTP account startup initializer beginning initial snapshot load")]
        private static partial void LogStartupInitializerBeginning(ILogger logger);

        /// <summary>
        /// Emits the startup completion marker after the initial account snapshot has been loaded.
        /// </summary>
        /// <param name="logger">Logger receiving the completion event.</param>
        /// <param name="accountCount">Number of accounts present in the loaded snapshot.</param>
        [LoggerMessage(EventId = 2003, Level = LogLevel.Information, Message = "NNTP account startup initializer completed; AccountsLoaded={AccountCount}")]
        private static partial void LogStartupInitializerCompleted(ILogger logger, int accountCount);
    }
}
