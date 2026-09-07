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
            return CreateSuccessfulGrabberResult(messageId, messageId, payloadText);
        }

        /// <summary>
        /// Creates a successful grabber result where request identity and article Message-ID can be controlled independently.
        /// </summary>
        /// <param name="requestMessageId">Message-ID assigned to the workflow/result identity.</param>
        /// <param name="articleMessageId">Message-ID written into the article header bytes.</param>
        /// <param name="payloadText">ASCII payload text copied into a rented pooled buffer.</param>
        /// <returns>Successful grabber result that owns the pooled payload owner through the normal disposal chain.</returns>
        internal static NntpArticleGrabberResult CreateSuccessfulGrabberResult(string requestMessageId, string articleMessageId, string payloadText)
        {
            string articleText = $"Date: Tue, 10 May 2011 13:48:50 -0500\r\nMessage-ID: {articleMessageId}\r\nNewsgroups: alt.test\r\nFrom: user@example.test\r\nPath: num2.nntp.ams.giganews.com!not-for-mail\r\nX-Test-Header: preserve-me\r\n\r\n{payloadText}\r\n";
            byte[] articleBytes = Encoding.ASCII.GetBytes(articleText);
            byte[] rented = ArrayPool<byte>.Shared.Rent(articleBytes.Length);
            Array.Copy(articleBytes, rented, articleBytes.Length);

            DownloadedArticleBuffer downloaded = new(rented, articleBytes.Length);
            NntpArticleAcquisitionResult acquisition = NntpArticleAcquisitionResult.Success(220, "article follows", downloaded);
            NntpArticleParser parser = new("backfiller01.usenet.ninja");
            NntpArticleParseResult parse = parser.Parse(downloaded.Memory);
            if (!parse.IsAccepted)
            {
                throw new InvalidOperationException($"Test fixture article must parse successfully. FailureCode={parse.FailureCode}");
            }

            return NntpArticleGrabberResult.Successful(requestMessageId, acquisition, parse);
        }
    }
}
