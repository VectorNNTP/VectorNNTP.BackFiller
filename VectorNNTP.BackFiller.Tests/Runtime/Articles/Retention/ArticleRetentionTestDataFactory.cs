// <copyright file="ArticleRetentionTestDataFactory.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Retention
// Shared helper for constructing minimal successful grabber payloads used by retention integration tests.

using System.Buffers;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.DateParser;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Retention
{
    /// <summary>
    /// Creates deterministic successful grabber payloads for ownership-transfer and retention-admission tests.
    /// </summary>
    internal static class ArticleRetentionTestDataFactory
    {
        /// <summary>
        /// Creates a successful grabber result with pooled payload ownership suitable for retention-admission tests.
        /// </summary>
        /// <param name="messageId">Message-ID assigned to the successful result.</param>
        /// <param name="payloadText">ASCII payload text copied into a rented pooled buffer.</param>
        /// <returns>Successful grabber result that owns the pooled payload owner through the normal disposal chain.</returns>
        internal static NntpArticleGrabberResult CreateSuccessfulGrabberResult(string messageId, string payloadText)
        {
            byte[] payloadBytes = Encoding.ASCII.GetBytes(payloadText);
            byte[] rented = ArrayPool<byte>.Shared.Rent(payloadBytes.Length);
            Array.Copy(payloadBytes, rented, payloadBytes.Length);

            DownloadedArticleBuffer downloaded = new(rented, payloadBytes.Length);
            NntpArticleAcquisitionResult acquisition = NntpArticleAcquisitionResult.Success(220, "article follows", downloaded);
            NntpArticleParseResult parse = new(
                IsAccepted: true,
                FailureCode: NntpArticleParseFailureCode.None,
                ArticleType: NntpArticleType.Text,
                ArticleBytes: downloaded.Memory,
                HeaderBytes: ReadOnlyMemory<byte>.Empty,
                BodyBytes: downloaded.Memory,
                Headers: [],
                DateFailureReason: DateParseFailureReason.None,
                CanonicalUtcDate: string.Empty,
                OriginalDateValue: ReadOnlyMemory<byte>.Empty,
                CanonicalPath: string.Empty,
                OriginalPathValue: ReadOnlyMemory<byte>.Empty,
                YEncDetected: false,
                YEncValidation: YEncArticleValidationResult.ValidNonYEnc());

            return NntpArticleGrabberResult.Successful(messageId, acquisition, parse);
        }
    }
}
