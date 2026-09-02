// <copyright file="BackboneArticleRetriever.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Backbone-scoped integration layer that acquires NNTP session leases from control-plane managed
// account runtimes and executes one grabber workflow operation per RabbitMQ work request.

using System.Diagnostics;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Represents one provider retrieval execution outcome with lease/account context.
    /// </summary>
    /// <param name="Lease">Session lease used for this retrieval operation.</param>
    /// <param name="GrabberResult">Workflow result emitted by the grabber processor.</param>
    internal sealed record BackboneArticleRetrievalResult(
        NntpArticleSessionLease? Lease,
        NntpArticleGrabberResult GrabberResult);

    /// <summary>
    /// Executes backbone-scoped article retrieval operations over control-plane managed session pools.
    /// </summary>
    internal interface IBackboneArticleRetriever
    {
        /// <summary>
        /// Retrieves one article for the specified work request.
        /// </summary>
        /// <param name="request">Parsed article-work request.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Retrieval result containing lease context and deterministic workflow result.</returns>
        public ValueTask<BackboneArticleRetrievalResult> RetrieveAsync(RabbitMqArticleWorkRequest request, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Default retrieval implementation that reuses existing grabber workflow and control-plane session managers.
    /// </summary>
    internal sealed partial class BackboneArticleRetriever : IBackboneArticleRetriever
    {
        /// <summary>
        /// Stores lease provider used by backbone article retriever.
        /// </summary>
        private readonly IBackboneSessionLeaseProvider _leaseProvider;
        /// <summary>
        /// Stores workflow used by backbone article retriever.
        /// </summary>
        private readonly NntpArticleGrabberWorkflow _workflow;
        /// <summary>
        /// Supplies the logger used by backbone article retriever.
        /// </summary>
        private readonly ILogger<BackboneArticleRetriever> _logger;

        /// <summary>
        /// Initializes a new backbone article retriever.
        /// </summary>
        /// <param name="leaseProvider">Backbone-scoped lease provider backed by control-plane account runtimes.</param>
        /// <param name="workflow">Grabber workflow that performs acquisition and parser classification.</param>
        /// <param name="logger">Logger for retrieval outcomes.</param>
        public BackboneArticleRetriever(
            IBackboneSessionLeaseProvider leaseProvider,
            NntpArticleGrabberWorkflow workflow,
            ILogger<BackboneArticleRetriever> logger)
        {
            _leaseProvider = leaseProvider ?? throw new ArgumentNullException(nameof(leaseProvider));
            _workflow = workflow ?? throw new ArgumentNullException(nameof(workflow));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public async ValueTask<BackboneArticleRetrievalResult> RetrieveAsync(RabbitMqArticleWorkRequest request, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);

            Stopwatch stopwatch = Stopwatch.StartNew();
            NntpArticleSessionLease lease = await _leaseProvider
                .AcquireSessionLeaseAsync(request.Backbone, request.MessageId, cancellationToken)
                .ConfigureAwait(false);

            try
            {
                NntpArticleGrabberResult grabberResult = await _workflow
                    .ProcessAsync(lease.Session, new NntpArticleGrabberWorkItem(request.MessageId), cancellationToken)
                    .ConfigureAwait(false);

                lease.ReportAcquisitionOutcome(grabberResult.AcquisitionFailureCode ?? NntpArticleAcquisitionFailureCode.None);
                _logger.LogInformation(
                    "Article retrieval completed. MessageId={MessageId} Backbone={Backbone} AccountId={AccountId} SlotId={SlotId} GrabberFailure={GrabberFailure} AcquisitionFailure={AcquisitionFailure} DurationMs={DurationMs}",
                    request.MessageId,
                    request.Backbone,
                    lease.AccountId,
                    lease.SlotId,
                    grabberResult.FailureCode,
                    grabberResult.AcquisitionFailureCode,
                    stopwatch.Elapsed.TotalMilliseconds);

                return new BackboneArticleRetrievalResult(lease, grabberResult);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                lease.ReportAcquisitionOutcome(NntpArticleAcquisitionFailureCode.Cancelled);
                throw;
            }
            catch
            {
                lease.ReportAcquisitionOutcome(NntpArticleAcquisitionFailureCode.ConnectionFailure);
                throw;
            }
        }

    }
}
