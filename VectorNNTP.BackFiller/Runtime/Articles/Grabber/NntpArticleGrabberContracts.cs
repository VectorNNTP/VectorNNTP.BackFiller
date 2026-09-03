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
    /// Identifies one logical grabber work item by the canonical Message-ID that should be acquired and parsed.
    /// </summary>
    /// <param name="MessageId">Canonical Message-ID correlation identifier passed through acquisition and parsing unchanged.</param>
    internal readonly record struct NntpArticleGrabberWorkItem(string MessageId);

    /// <summary>
    /// Classifies deterministic terminal outcomes of the grabber workflow after acquisition, parsing, and yEnc validation are reconciled.
    /// </summary>
    internal enum NntpArticleGrabberFailureCode
    {
        /// <summary>
        /// Acquisition and parsing completed successfully.
        /// </summary>
        None = 0,

        /// <summary>
        /// The requested Message-ID was invalid before or during acquisition.
        /// </summary>
        InvalidMessageId = 1,

        /// <summary>
        /// The provider reported that the article does not exist.
        /// </summary>
        ArticleNotFound = 2,

        /// <summary>
        /// Acquisition could not authenticate to the remote provider.
        /// </summary>
        AuthenticationFailure = 3,

        /// <summary>
        /// Acquisition transport setup or I/O failed.
        /// </summary>
        ConnectionFailure = 4,

        /// <summary>
        /// Acquisition timed out.
        /// </summary>
        Timeout = 5,

        /// <summary>
        /// The NNTP protocol exchange was malformed or command-unexpected.
        /// </summary>
        ProtocolFailure = 6,

        /// <summary>
        /// ARTICLE framing terminated early or exceeded configured size limits.
        /// </summary>
        ArticleFramingFailure = 7,

        /// <summary>
        /// The parser rejected the article as malformed.
        /// </summary>
        MalformedArticle = 8,

        /// <summary>
        /// The parser rejected the article because no usable date could be resolved.
        /// </summary>
        InvalidDate = 9,

        /// <summary>
        /// The parser rejected the article because one or more required headers were missing, duplicated, or semantically invalid.
        /// </summary>
        InvalidHeaders = 10,

        /// <summary>
        /// yEnc validation failed, including decode, CRC, or size-integrity failures.
        /// </summary>
        YEncValidationFailure = 11,

        /// <summary>
        /// The caller cancelled the workflow.
        /// </summary>
        Cancelled = 12,

        /// <summary>
        /// Another explicit acquisition failure was propagated without a more specific workflow mapping.
        /// </summary>
        AcquisitionFailure = 13,
    }

    /// <summary>
    /// Carries a successful workflow payload together with the acquisition and parser state that produced it.
    /// </summary>
    /// <param name="MessageId">Canonical Message-ID correlation identifier.</param>
    /// <param name="Acquisition">Successful acquisition result that owns the raw article bytes.</param>
    /// <param name="Parse">Accepted parse result describing the downloaded article.</param>
    /// <remarks>
    /// Disposal returns the pooled acquisition buffer once downstream processing no longer needs the raw article bytes.
    /// </remarks>
    internal sealed record NntpArticleGrabberSuccess(
        string MessageId,
        NntpArticleAcquisitionResult Acquisition,
        NntpArticleParseResult Parse) : IDisposable
    {
        /// <summary>
        /// Disposes the acquisition-owned buffer after downstream processing is complete.
        /// </summary>
        public void Dispose()
        {
            Acquisition.Dispose();
        }
    }

    /// <summary>
    /// Represents one terminal outcome of the grabber workflow.
    /// </summary>
    /// <param name="MessageId">Canonical Message-ID correlation identifier.</param>
    /// <param name="IsSuccess">Whether acquisition and parsing completed successfully.</param>
    /// <param name="FailureCode">Workflow-level deterministic failure classification when unsuccessful.</param>
    /// <param name="AcquisitionFailureCode">Underlying acquisition classification when one exists.</param>
    /// <param name="ParseFailureCode">Underlying parser classification when one exists.</param>
    /// <param name="YEncStatus">yEnc validation status when parsing reached yEnc validation.</param>
    /// <param name="ResponseCode">NNTP response code preserved from acquisition when available.</param>
    /// <param name="ResponseText">Protocol or local detail preserved from acquisition when available.</param>
    /// <param name="Success">Successful workflow payload when <paramref name="IsSuccess"/> is <see langword="true"/>.</param>
    /// <remarks>
    /// Failed results never own acquisition buffers. Successful results transfer buffer ownership into <paramref name="Success"/>, which callers should dispose after processing.
    /// </remarks>
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
        /// Creates a successful grabber result that transfers ownership of the acquired article buffer.
        /// </summary>
        /// <param name="messageId">Canonical Message-ID.</param>
        /// <param name="acquisition">Successful acquisition result owning the raw article bytes.</param>
        /// <param name="parse">Accepted parse result for the acquired bytes.</param>
        /// <returns>A successful workflow result whose <see cref="Success"/> payload must eventually be disposed.</returns>
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
        /// Creates a failed grabber result that preserves acquisition and parser diagnostics without owning payload buffers.
        /// </summary>
        /// <param name="messageId">Canonical Message-ID.</param>
        /// <param name="failureCode">Workflow-level failure classification.</param>
        /// <param name="acquisitionFailureCode">Underlying acquisition failure code when available.</param>
        /// <param name="parseFailureCode">Underlying parser failure code when available.</param>
        /// <param name="yEncStatus">Underlying yEnc validation status when available.</param>
        /// <param name="responseCode">NNTP response code when available.</param>
        /// <param name="responseText">Protocol or local detail text.</param>
        /// <returns>A failed workflow result.</returns>
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
        /// Disposes the successful payload, if this result currently owns one.
        /// </summary>
        public void Dispose()
        {
            Success?.Dispose();
        }
    }
}
