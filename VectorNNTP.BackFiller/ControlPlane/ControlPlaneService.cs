using VectorNNTP.Backfiller.Runtime.Accounts;

namespace VectorNNTP.Backfiller.ControlPlane
{
    /// <summary>
    /// Runs the control-plane background loop for the service lifetime.
    /// </summary>
    /// <param name="logger">The logger used for control-plane diagnostics.</param>
    /// <param name="timeProvider">The unified time provider used for control-plane timestamps.</param>
    /// <param name="snapshotProvider">The runtime NNTP account snapshot provider.</param>
    internal sealed partial class ControlPlaneService(
        ILogger<ControlPlaneService> logger,
        TimeProvider timeProvider,
        MySqlNntpAccountSnapshotProvider snapshotProvider) : BackgroundService
    {
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan RefreshInterval = TimeSpan.FromSeconds(60);

        /// <summary>
        /// The logger used by this control-plane service instance.
        /// </summary>
        private readonly ILogger<ControlPlaneService> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// The unified time provider used for startup and heartbeat timestamps.
        /// </summary>
        private readonly TimeProvider _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

        /// <summary>
        /// The runtime NNTP account snapshot provider.
        /// </summary>
        private readonly MySqlNntpAccountSnapshotProvider _snapshotProvider = snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));

        /// <summary>
        /// Gets a value indicating whether mandatory control-plane startup initialization completed.
        /// </summary>
        internal bool IsStartupInitializationComplete { get; private set; }

        /// <summary>
        /// Performs control-plane startup initialization before the background loop begins.
        /// </summary>
        /// <param name="cancellationToken">Token that cancels startup initialization.</param>
        /// <returns>A task that completes when the control plane has established its startup barrier.</returns>
        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await InitializeControlPlaneAsync(cancellationToken).ConfigureAwait(false);
            await base.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes the control-plane loop until the host requests cancellation.
        /// </summary>
        /// <param name="stoppingToken">The token that signals when processing should stop.</param>
        /// <returns>A task that completes when control-plane execution stops.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            long heartbeatCount = 0;
            int refreshEveryHeartbeats = Math.Max(1, (int)Math.Ceiling(RefreshInterval.TotalSeconds / HeartbeatInterval.TotalSeconds));

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    heartbeatCount++;

                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        DateTimeOffset currentTime = _timeProvider.GetUtcNow();
                        LogControlPlaneRunning(_logger, currentTime);
                    }

                    if (heartbeatCount % refreshEveryHeartbeats == 0)
                    {
                        await TryRefreshNntpAccountsAsync(stoppingToken).ConfigureAwait(false);
                    }

                    await Task.Delay(HeartbeatInterval, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Normal Generic Host shutdown: Task.Delay observes cancellation and exits cooperatively.
            }
        }

        private Task InitializeControlPlaneAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_logger.IsEnabled(LogLevel.Information))
            {
                DateTimeOffset currentTime = _timeProvider.GetUtcNow();
                LogControlPlaneStartupInitialized(_logger, currentTime);
            }

            IsStartupInitializationComplete = true;
            return Task.CompletedTask;
        }

        private async Task TryRefreshNntpAccountsAsync(CancellationToken stoppingToken)
        {
            try
            {
                _ = await _snapshotProvider.RefreshSnapshotAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Normal shutdown cancellation; not an operational refresh failure.
            }
            catch (Exception ex)
            {
                LogNntpAccountRefreshFailed(_logger, ex);
            }
        }

        /// <summary>
        /// Logs successful completion of the control-plane startup barrier.
        /// </summary>
        /// <param name="logger">The logger receiving the startup entry.</param>
        /// <param name="currentTime">The current timestamp captured for the startup barrier completion.</param>
        [LoggerMessage(EventId = 999, Level = LogLevel.Information, Message = "Control plane startup initialization completed at: {CurrentTime}")]
        private static partial void LogControlPlaneStartupInitialized(ILogger logger, DateTimeOffset currentTime);

        /// <summary>
        /// Logs the periodic control-plane heartbeat.
        /// </summary>
        /// <param name="logger">The logger receiving the heartbeat entry.</param>
        /// <param name="currentTime">The current timestamp captured for the heartbeat.</param>
        [LoggerMessage(EventId = 1000, Level = LogLevel.Debug, Message = "Control plane running at: {CurrentTime}")]
        private static partial void LogControlPlaneRunning(ILogger logger, DateTimeOffset currentTime);

        [LoggerMessage(EventId = 1001, Level = LogLevel.Warning, Message = "Periodic NNTP account snapshot refresh failed")]
        private static partial void LogNntpAccountRefreshFailed(ILogger logger, Exception exception);
    }
}
