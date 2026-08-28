// <copyright file="NntpArticleGrabberWorkflow.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Grabber
// Session-oriented article workflow orchestrator that acquires by Message-ID,
// parses raw bytes, preserves typed failure semantics, and emits correlated outcomes.

using System.Diagnostics;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;

namespace VectorNNTP.Backfiller.Runtime.Articles.Grabber
{
    /// <summary>
    /// Orchestrates one reusable-session NNTP article workflow from Message-ID acquisition through parser validation.
    /// </summary>
    internal sealed partial class NntpArticleGrabberWorkflow
    {
        /// <summary>
        /// Logger used for workflow-level article correlation and failure diagnostics.
        /// </summary>
        private readonly ILogger<NntpArticleGrabberWorkflow> _logger;

        /// <summary>
        /// Parser used to validate and classify successfully acquired article payload bytes.
        /// </summary>
        private readonly NntpArticleParser _parser;

        /// <summary>
        /// Initializes a new workflow orchestrator bound to the validated runtime parser identity.
        /// </summary>
        /// <param name="runtimeOptions">Validated immutable runtime snapshot used to build parser identity.</param>
        /// <param name="logger">Workflow logger.</param>
        internal NntpArticleGrabberWorkflow(
            BackFillerRuntimeOptions runtimeOptions,
            ILogger<NntpArticleGrabberWorkflow> logger)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _parser = new NntpArticleParser(runtimeOptions.CanonicalBackFillerFqdn);
        }

        /// <summary>
        /// Acquires and parses one article over an already-connected authenticated acquisition session.
        /// </summary>
        /// <param name="session">Reusable authenticated acquisition session.</param>
        /// <param name="workItem">Grabber work item containing canonical Message-ID.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Deterministic grabber result preserving acquisition and parser classifications.</returns>
        internal async ValueTask<NntpArticleGrabberResult> ProcessAsync(
            NntpArticleAcquisitionSession session,
            NntpArticleGrabberWorkItem workItem,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(session);

            string messageId = workItem.MessageId;
            if (string.IsNullOrWhiteSpace(messageId) || !NntpMessageIdValidation.IsValidMessageId(messageId.AsSpan()))
            {
                return NntpArticleGrabberResult.Failed(
                    messageId,
                    NntpArticleGrabberFailureCode.InvalidMessageId,
                    NntpArticleAcquisitionFailureCode.InvalidMessageId,
                    parseFailureCode: null,
                    yEncStatus: null,
                    responseCode: null,
                    responseText: "Message-ID does not satisfy NNTP/INN grammar.");
            }

            Stopwatch workflowTimer = Stopwatch.StartNew();
            NntpArticleAcquisitionResult acquisition = await session.DownloadArticleAsync(messageId, cancellationToken).ConfigureAwait(false);
            if (!acquisition.IsSuccess)
            {
                NntpArticleGrabberResult failedResult = NntpArticleGrabberResult.Failed(
                    messageId,
                    MapAcquisitionFailure(acquisition.FailureCode),
                    acquisition.FailureCode,
                    parseFailureCode: null,
                    yEncStatus: null,
                    responseCode: acquisition.ResponseCode,
                    responseText: acquisition.ResponseText);

                NntpArticleAcquisitionFailureCode acquisitionFailureCode = acquisition.FailureCode;
                acquisition.Dispose();
                LogWorkflowFailure(_logger, messageId, failedResult.FailureCode, acquisitionFailureCode, null, null, FormatElapsed(workflowTimer.Elapsed));
                return failedResult;
            }

            NntpArticleParseResult parse = NntpArticleAcquisitionParserBridge.ParseSuccessfulArticle(_parser, acquisition);
            if (!parse.IsAccepted)
            {
                NntpArticleGrabberResult failedResult = NntpArticleGrabberResult.Failed(
                    messageId,
                    MapParseFailure(parse),
                    NntpArticleAcquisitionFailureCode.None,
                    parseFailureCode: parse.FailureCode,
                    yEncStatus: parse.YEncValidation.Status,
                    responseCode: acquisition.ResponseCode,
                    responseText: acquisition.ResponseText);

                NntpArticleParseFailureCode parseFailureCode = parse.FailureCode;
                YEncArticleValidationStatus yEncStatus = parse.YEncValidation.Status;
                acquisition.Dispose();
                LogWorkflowFailure(_logger, messageId, failedResult.FailureCode, NntpArticleAcquisitionFailureCode.None, parseFailureCode, yEncStatus, FormatElapsed(workflowTimer.Elapsed));
                return failedResult;
            }

            NntpArticleGrabberResult success = NntpArticleGrabberResult.Successful(messageId, acquisition, parse);
            LogWorkflowSuccess(_logger, messageId, parse.ArticleType, parse.BodyBytes.Length, FormatElapsed(workflowTimer.Elapsed));
            return success;
        }

        /// <summary>
        /// Maps acquisition failure classifications into workflow-level deterministic failure semantics.
        /// </summary>
        /// <param name="acquisitionFailureCode">Acquisition failure classification.</param>
        /// <returns>Workflow failure classification.</returns>
        private static NntpArticleGrabberFailureCode MapAcquisitionFailure(NntpArticleAcquisitionFailureCode acquisitionFailureCode)
        {
            return acquisitionFailureCode switch
            {
                NntpArticleAcquisitionFailureCode.InvalidMessageId => NntpArticleGrabberFailureCode.InvalidMessageId,
                NntpArticleAcquisitionFailureCode.ArticleNotFound => NntpArticleGrabberFailureCode.ArticleNotFound,
                NntpArticleAcquisitionFailureCode.AuthenticationFailure => NntpArticleGrabberFailureCode.AuthenticationFailure,
                NntpArticleAcquisitionFailureCode.ConnectionFailure => NntpArticleGrabberFailureCode.ConnectionFailure,
                NntpArticleAcquisitionFailureCode.Timeout => NntpArticleGrabberFailureCode.Timeout,
                NntpArticleAcquisitionFailureCode.TruncatedArticle or NntpArticleAcquisitionFailureCode.ArticleTooLarge => NntpArticleGrabberFailureCode.ArticleFramingFailure,
                NntpArticleAcquisitionFailureCode.Cancelled => NntpArticleGrabberFailureCode.Cancelled,
                NntpArticleAcquisitionFailureCode.MalformedResponse or NntpArticleAcquisitionFailureCode.ProtocolFailure or NntpArticleAcquisitionFailureCode.RemoteRejected => NntpArticleGrabberFailureCode.ProtocolFailure,
                _ => NntpArticleGrabberFailureCode.AcquisitionFailure,
            };
        }

        /// <summary>
        /// Maps parser rejection classifications into workflow-level deterministic failure semantics.
        /// </summary>
        /// <param name="parse">Rejected parse result.</param>
        /// <returns>Workflow failure classification.</returns>
        private static NntpArticleGrabberFailureCode MapParseFailure(NntpArticleParseResult parse)
        {
            return parse.FailureCode == NntpArticleParseFailureCode.YEncDecodingFailed
                ? NntpArticleGrabberFailureCode.YEncValidationFailure
                : parse.FailureCode switch
                {
                    NntpArticleParseFailureCode.MissingOrInvalidDate => NntpArticleGrabberFailureCode.InvalidDate,
                    NntpArticleParseFailureCode.MissingMessageId
                    or NntpArticleParseFailureCode.InvalidMessageId
                    or NntpArticleParseFailureCode.DuplicateMessageId
                    or NntpArticleParseFailureCode.MissingNewsgroups
                    or NntpArticleParseFailureCode.InvalidNewsgroups
                    or NntpArticleParseFailureCode.InvalidFrom
                    or NntpArticleParseFailureCode.InvalidPath
                    or NntpArticleParseFailureCode.DuplicateNewsgroups
                    or NntpArticleParseFailureCode.DuplicatePath => NntpArticleGrabberFailureCode.InvalidHeaders,
                    _ => NntpArticleGrabberFailureCode.MalformedArticle,
                };
        }

        /// <summary>
        /// Formats elapsed duration with invariant machine-facing representation.
        /// </summary>
        /// <param name="elapsed">Elapsed duration.</param>
        /// <returns>Formatted duration.</returns>
        private static string FormatElapsed(TimeSpan elapsed)
        {
            return elapsed.TotalSeconds.ToString("F2", System.Globalization.CultureInfo.InvariantCulture) + "s";
        }

        /// <summary>
        /// Logs successful workflow outcomes with canonical message correlation.
        /// </summary>
        /// <param name="logger">Logger receiving the entry.</param>
        /// <param name="messageId">Canonical message identifier.</param>
        /// <param name="articleType">Detected article type.</param>
        /// <param name="articleSize">Article body size in bytes.</param>
        /// <param name="duration">Operation duration text.</param>
        private static void LogWorkflowSuccess(ILogger logger, string messageId, NntpArticleType articleType, int articleSize, string duration)
        {
            logger.LogInformation(
                "Article workflow completed for {MessageId} in {Duration} (Outcome=Success, ArticleType={ArticleType}, ArticleSize={ArticleSize})",
                messageId,
                duration,
                articleType,
                articleSize);
        }

        /// <summary>
        /// Logs failed workflow outcomes while preserving distinct acquisition/parser/yEnc failure classes.
        /// </summary>
        /// <param name="logger">Logger receiving the entry.</param>
        /// <param name="messageId">Canonical message identifier.</param>
        /// <param name="failureCode">Workflow-level deterministic failure code.</param>
        /// <param name="acquisitionFailureCode">Acquisition-level failure code when available.</param>
        /// <param name="parseFailureCode">Parser-level failure code when available.</param>
        /// <param name="yEncStatus">yEnc validation status when available.</param>
        /// <param name="duration">Operation duration text.</param>
        private static void LogWorkflowFailure(
            ILogger logger,
            string messageId,
            NntpArticleGrabberFailureCode failureCode,
            NntpArticleAcquisitionFailureCode? acquisitionFailureCode,
            NntpArticleParseFailureCode? parseFailureCode,
            YEncArticleValidationStatus? yEncStatus,
            string duration)
        {
            logger.LogInformation(
                "Article workflow failed for {MessageId} in {Duration} (Outcome=Failure, FailureCode={FailureCode}, AcquisitionFailure={AcquisitionFailureCode}, ParseFailure={ParseFailureCode}, YEncStatus={YEncStatus})",
                messageId,
                duration,
                failureCode,
                acquisitionFailureCode,
                parseFailureCode,
                yEncStatus);
        }
    }
}
