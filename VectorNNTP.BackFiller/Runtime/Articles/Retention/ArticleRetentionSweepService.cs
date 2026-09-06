// <copyright file="ArticleRetentionSweepService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Retention
// Hosted background sweep that applies periodic TTL expiration and closes admission on shutdown.

using VectorNNTP.Backfiller.Runtime.Articles.Processing;

namespace VectorNNTP.Backfiller.Runtime.Articles.Retention
{
    /// <summary>
    /// Runs periodic retention expiration sweeps and coordinates retention shutdown admission closure.
    /// </summary>
    internal sealed class ArticleRetentionSweepService(
        IArticleRetentionAuthority retentionAuthority,
        IArticleProcessingDrainBarrier processingDrainBarrier) : BackgroundService
    {
        private readonly IArticleRetentionAuthority _retentionAuthority = retentionAuthority ?? throw new ArgumentNullException(nameof(retentionAuthority));
        private readonly IArticleProcessingDrainBarrier _processingDrainBarrier = processingDrainBarrier ?? throw new ArgumentNullException(nameof(processingDrainBarrier));

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            TimeSpan interval = _retentionAuthority.SweepInterval;

            while (!stoppingToken.IsCancellationRequested)
            {
                _ = _retentionAuthority.ExpireEligibleArticles(DateTimeOffset.UtcNow);
                try
                {
                    await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await _processingDrainBarrier.WaitForDrainAsync(cancellationToken).ConfigureAwait(false);
            _retentionAuthority.BeginShutdown();
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
