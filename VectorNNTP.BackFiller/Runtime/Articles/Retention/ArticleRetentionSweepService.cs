// <copyright file="ArticleRetentionSweepService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Retention
// Hosted background sweep that applies periodic TTL expiration and closes admission on shutdown.

namespace VectorNNTP.Backfiller.Runtime.Articles.Retention
{
    /// <summary>
    /// Runs periodic retention expiration sweeps and coordinates retention shutdown admission closure.
    /// </summary>
    internal sealed class ArticleRetentionSweepService(IArticleRetentionAuthority retentionAuthority) : BackgroundService
    {
        private readonly IArticleRetentionAuthority _retentionAuthority = retentionAuthority ?? throw new ArgumentNullException(nameof(retentionAuthority));

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
            _retentionAuthority.BeginShutdown();
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
