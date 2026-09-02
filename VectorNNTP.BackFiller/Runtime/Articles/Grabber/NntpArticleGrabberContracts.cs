// <copyright file="NntpArticleGrabberContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Grabber
// Contracts for grabber-level ARTICLE workflow orchestration that preserves acquisition,
// parser, and yEnc failure semantics without collapsing them into generic errors.

using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;

namespace VectorNNTP.Backfiller.Runtime.Articles.Grabber
{
    /// <summary>
    /// Represents one logical article request accepted by the grabber orchestration flow.
    /// </summary>
    /// <param name="MessageId">Canonical Message-ID correlation identifier passed through acquisition and parsing unchanged.</param>
    internal readonly record struct NntpArticleGrabberWorkItem(string MessageId);

    /// <summary>
    /// Represents deterministic terminal classifications returned by the grabber article workflow.
    /// </summary>
    internal enum NntpArticleGrabberFailureCode
    {
        /// <summary>
        /// Workflow completed successfully.
        /// </summary>
        None = 0,

        /// <summary>
        /// Message-ID argument is invalid.
        /// </summary>
        InvalidMessageId = 1,

        /// <summary>
        /// Provider reported article not found.
        /// </summary>
        ArticleNotFound = 2,

        /// <summary>
        /// Acquisition could not authenticate to the remote provider.
        /// </summary>
        AuthenticationFailure = 3,

        /// <summary>
        /// Acquisition transport/session establishment or I/O failed.
        /// </summary>
        ConnectionFailure = 4,

        /// <summary>
        /// Acquisition operation timed out.
        /// </summary>
        Timeout = 5,

        /// <summary>
        /// Remote protocol or framing was malformed.
        /// </summary>
        ProtocolFailure = 6,

        /// <summary>
        /// Article framing or size constraints were violated.
        /// </summary>
        ArticleFramingFailure = 7,

        /// <summary>
        /// Parser rejected article due to malformed syntax/headers/content.
        /// </summary>
        MalformedArticle = 8,

        /// <summary>
        /// Parser rejected article due to missing/invalid date semantics.
        /// </summary>
        InvalidDate = 9,

        /// <summary>
        /// Parser rejected article due to invalid header semantics unrelated to date.
        /// </summary>
        InvalidHeaders = 10,

        /// <summary>
        /// yEnc validation failed, including decode or CRC/size integrity failures.
        /// </summary>
        YEncValidationFailure = 11,

        /// <summary>
        /// Operation was cancelled by caller.
        /// </summary>
        Cancelled = 12,

        /// <summary>
        /// Other explicit acquisition failure propagated from acquisition contracts.
        /// </summary>
        AcquisitionFailure = 13,
    }

    /// <summary>
    /// Represents one successful article workflow outcome that includes acquisition and parse metadata.
    /// </summary>
    /// <param name="MessageId">Canonical Message-ID correlation identifier.</param>
    /// <param name="Acquisition">Successful acquisition result containing raw article bytes.</param>
    /// <param name="Parse">Accepted parse result including header/body slices and yEnc validation metadata.</param>
    internal sealed record NntpArticleGrabberSuccess(
        string MessageId,
        NntpArticleAcquisitionResult Acquisition,
        NntpArticleParseResult Parse) : IDisposable
    {
        /// <summary>
        /// Disposes acquisition-owned buffers after downstream processing is complete.
        /// </summary>
        public void Dispose()
        {
            Acquisition.Dispose();
        }
    }

    /// <summary>
    /// Represents one terminal workflow result for a grabber article operation.
    /// </summary>
    /// <param name="MessageId">Canonical Message-ID correlation identifier.</param>
    /// <param name="IsSuccess">Indicates whether acquisition and parse completed successfully.</param>
    /// <param name="FailureCode">Deterministic failure classification when unsuccessful.</param>
    /// <param name="AcquisitionFailureCode">Source acquisition failure classification when applicable.</param>
    /// <param name="ParseFailureCode">Source parser failure classification when applicable.</param>
    /// <param name="YEncStatus">yEnc validation status when parsing reached yEnc validation.</param>
    /// <param name="ResponseCode">NNTP response code from acquisition when available.</param>
    /// <param name="ResponseText">Protocol/local detail text from acquisition when available.</param>
    /// <param name="Success">Successful workflow payload when <paramref name="IsSuccess"/> is true.</param>
    internal sealed record NntpArticleGrabberResult(
        string MessageId,
        bool IsSuccess,
        NntpArticleGrabberFailureCode FailureCode,
        NntpArticleAcquisitionFailureCode? AcquisitionFailureCode,
        NntpArticleParseFailureCode? ParseFailureCode,
        YEncArticleValidationStatus? YEncStatus,
        int? ResponseCode,
        string? ResponseText,
        NntpArticleGrabberSuccess? Success) : IDisposable
    {
        /// <summary>
        /// Creates a successful grabber result.
        /// </summary>
        /// <param name="messageId">Canonical message identifier.</param>
        /// <param name="acquisition">Successful acquisition result.</param>
        /// <param name="parse">Accepted parse result.</param>
        /// <returns>Successful grabber result with owned success payload.</returns>
        internal static NntpArticleGrabberResult Successful(string messageId, NntpArticleAcquisitionResult acquisition, NntpArticleParseResult parse)
        {
            return new NntpArticleGrabberResult(
                MessageId: messageId,
                IsSuccess: true,
                FailureCode: NntpArticleGrabberFailureCode.None,
                AcquisitionFailureCode: null,
                ParseFailureCode: null,
                YEncStatus: parse.YEncValidation.Status,
                ResponseCode: acquisition.ResponseCode,
                ResponseText: acquisition.ResponseText,
                Success: new NntpArticleGrabberSuccess(messageId, acquisition, parse));
        }

        /// <summary>
        /// Creates a failed grabber result without owned success payload.
        /// </summary>
        /// <param name="messageId">Canonical message identifier.</param>
        /// <param name="failureCode">Deterministic workflow failure code.</param>
        /// <param name="acquisitionFailureCode">Acquisition failure code when available.</param>
        /// <param name="parseFailureCode">Parser failure code when available.</param>
        /// <param name="yEncStatus">yEnc validation status when available.</param>
        /// <param name="responseCode">NNTP response code when available.</param>
        /// <param name="responseText">Failure detail text.</param>
        /// <returns>Failed workflow result.</returns>
        internal static NntpArticleGrabberResult Failed(
            string messageId,
            NntpArticleGrabberFailureCode failureCode,
            NntpArticleAcquisitionFailureCode? acquisitionFailureCode,
            NntpArticleParseFailureCode? parseFailureCode,
            YEncArticleValidationStatus? yEncStatus,
            int? responseCode,
            string? responseText)
        {
            return new NntpArticleGrabberResult(
                MessageId: messageId,
                IsSuccess: false,
                FailureCode: failureCode,
                AcquisitionFailureCode: acquisitionFailureCode,
                ParseFailureCode: parseFailureCode,
                YEncStatus: yEncStatus,
                ResponseCode: responseCode,
                ResponseText: responseText,
                Success: null);
        }

        /// <summary>
        /// Disposes successful payload ownership when present.
        /// </summary>
        public void Dispose()
        {
            Success?.Dispose();
        }
    }
}
