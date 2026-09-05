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
    /// Carries the grabber workflow result together with the NNTP session lease used to obtain it.
    /// </summary>
    /// <param name="Lease">
    /// Lease associated with the retrieval attempt. The concrete retriever returns a non-null lease after acquisition succeeds; test doubles may use <see langword="null"/>.
    /// </param>
    /// <param name="GrabberResult">Workflow result emitted by the grabber processor.</param>
    internal sealed record BackboneArticleRetrievalResult(
        NntpArticleSessionLease? Lease,
        NntpArticleGrabberResult GrabberResult);

    /// <summary>
    /// Acquires a backbone-scoped NNTP session lease and runs one grabber workflow operation against it.
    /// </summary>
    internal interface IBackboneArticleRetriever
    {
        /// <summary>
        /// Retrieves one article for the specified work request.
        /// </summary>
        /// <param name="request">Parsed article-work request.</param>
        /// <param name="cancellationToken">Cancellation token that aborts lease acquisition or the underlying grabber workflow.</param>
        /// <returns>
        /// Retrieval result containing the workflow classification and any acquired lease. Callers that receive a non-null lease must dispose it exactly once.
        /// </returns>
        public ValueTask<BackboneArticleRetrievalResult> RetrieveAsync(RabbitMqArticleWorkRequest request, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Default retriever that reuses control-plane session pools and the existing grabber workflow.
    /// </summary>
    /// <remarks>
    /// The retriever reports an acquisition outcome back to the lease before control returns so the session manager can account for the attempt when the lease is released.
    /// </remarks>
    internal sealed partial class BackboneArticleRetriever : IBackboneArticleRetriever
    {
        /// <summary>
        /// Control-plane lease provider used to route work to a backbone-specific account runtime.
        /// </summary>
        private readonly IBackboneSessionLeaseProvider _leaseProvider;
        /// <summary>
        /// Grabber workflow that performs acquisition, parsing, and failure classification once a session is leased.
        /// </summary>
        private readonly NntpArticleGrabberWorkflow _workflow;
        /// <summary>
        /// Supplies the logger used by backbone article retriever.
        /// </summary>
        private readonly ILogger<BackboneArticleRetriever> _logger;

        /// <summary>
        /// Initializes a retriever bound to the control-plane lease provider and grabber workflow.
        /// </summary>
        /// <param name="leaseProvider">Backbone-scoped lease provider backed by control-plane managed account runtimes.</param>
        /// <param name="workflow">Grabber workflow that downloads, parses, and classifies one article over the leased session.</param>
        /// <param name="logger">Logger used for correlated per-retrieval outcome diagnostics.</param>
        public BackboneArticleRetriever(
            IBackboneSessionLeaseProvider leaseProvider,
            NntpArticleGrabberWorkflow workflow,
            ILogger<BackboneArticleRetriever> logger)
        {
            _leaseProvider = leaseProvider ?? throw new ArgumentNullException(nameof(leaseProvider));
            _workflow = workflow ?? throw new ArgumentNullException(nameof(workflow));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Acquires a backbone session, runs the grabber workflow, and records the terminal acquisition outcome on the lease before returning.
        /// </summary>
        /// <param name="request">Parsed RabbitMQ work request that supplies the target backbone and canonical Message-ID.</param>
        /// <param name="cancellationToken">Cancellation token that cancels lease acquisition or the underlying workflow execution.</param>
        /// <returns>
        /// A retrieval result that transfers lease ownership to the caller together with the deterministic workflow result.
        /// </returns>
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
                LogArticleRetrievalCompleted(
                    _logger,
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

        /// <summary>
        /// Emits the article retrieval completed log event after the leased backbone session has been classified.
        /// </summary>
        /// <param name="logger">Logger receiving the retrieval completion event.</param>
        /// <param name="messageId">Canonical Message-ID associated with the retrieved article.</param>
        /// <param name="backbone">Backbone name used to acquire the session lease.</param>
        /// <param name="accountId">Account identifier of the leased NNTP session.</param>
        /// <param name="slotId">Slot identifier of the leased NNTP session.</param>
        /// <param name="grabberFailure">Workflow-level failure classification reported by the grabber result.</param>
        /// <param name="acquisitionFailure">Acquisition-layer failure classification when the workflow did not succeed.</param>
        /// <param name="durationMs">Elapsed retrieval duration in milliseconds.</param>
        [LoggerMessage(
            EventId = 3302,
            Level = LogLevel.Information,
            Message = "Article retrieval completed. MessageId={MessageId} Backbone={Backbone} AccountId={AccountId} SlotId={SlotId} GrabberFailure={GrabberFailure} AcquisitionFailure={AcquisitionFailure} DurationMs={DurationMs}")]
        private static partial void LogArticleRetrievalCompleted(
            ILogger logger,
            string messageId,
            string backbone,
            Guid accountId,
            int slotId,
            NntpArticleGrabberFailureCode grabberFailure,
            NntpArticleAcquisitionFailureCode? acquisitionFailure,
            double durationMs);
    }
}
