// <copyright file="NntpArticleParserContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Parsing
// Hot-path parse contracts for byte-oriented NNTP article parsing, classification,
// validation outcomes, and normalized header-derived metadata.

using VectorNNTP.Backfiller.Runtime.Articles.DateParser;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;

namespace VectorNNTP.Backfiller.Runtime.Articles.Parsing
{
    /// <summary>
    /// Represents the detected article content classification produced by the parser.
    /// </summary>
    internal enum NntpArticleType
    {
        /// <summary>
        /// The parser could not confidently classify content type.
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// The article appears to be ordinary textual content.
        /// </summary>
        Text = 1,

        /// <summary>
        /// The article advertises or resembles MIME multipart content.
        /// </summary>
        MimeMultipart = 2,

        /// <summary>
        /// The article body contains yEnc section markers.
        /// </summary>
        YEnc = 3,

        /// <summary>
        /// The article advertises binary or encoded transfer content.
        /// </summary>
        BinaryEncoded = 4,

        /// <summary>
        /// The article has malformed structure preventing reliable classification.
        /// </summary>
        Malformed = 5,
    }

    /// <summary>
    /// Represents deterministic parser rejection or terminal parse-state classification.
    /// </summary>
    internal enum NntpArticleParseFailureCode
    {
        /// <summary>
        /// Parsing completed successfully.
        /// </summary>
        None = 0,

        /// <summary>
        /// Article payload is empty.
        /// </summary>
        EmptyArticle = 1,

        /// <summary>
        /// Header/body boundary was not found within parser limits.
        /// </summary>
        MissingHeaderBodySeparator = 2,

        /// <summary>
        /// Article size exceeds configured maximum bytes.
        /// </summary>
        ArticleTooLarge = 23,

        /// <summary>
        /// Header section exceeded configured maximum size.
        /// </summary>
        HeaderSectionTooLarge = 3,

        /// <summary>
        /// Header count exceeded configured maximum.
        /// </summary>
        TooManyHeaders = 4,

        /// <summary>
        /// Header line exceeded configured maximum line length.
        /// </summary>
        HeaderLineTooLong = 5,

        /// <summary>
        /// Header syntax is malformed.
        /// </summary>
        MalformedHeader = 6,

        /// <summary>
        /// Header continuation line appeared without a preceding header.
        /// </summary>
        MalformedHeaderContinuation = 7,

        /// <summary>
        /// Header name length exceeded configured maximum.
        /// </summary>
        HeaderNameTooLong = 8,

        /// <summary>
        /// Header value length exceeded configured maximum.
        /// </summary>
        HeaderValueTooLong = 9,

        /// <summary>
        /// Input contains NUL bytes.
        /// </summary>
        ContainsNul = 10,

        /// <summary>
        /// Input contains illegal control-byte values in header fields.
        /// </summary>
        ContainsIllegalControlByte = 11,

        /// <summary>
        /// Article does not contain a usable date header.
        /// </summary>
        MissingOrInvalidDate = 12,

        /// <summary>
        /// Message-ID is missing.
        /// </summary>
        MissingMessageId = 13,

        /// <summary>
        /// Message-ID is malformed.
        /// </summary>
        InvalidMessageId = 14,

        /// <summary>
        /// Newsgroups header is missing.
        /// </summary>
        MissingNewsgroups = 15,

        /// <summary>
        /// Newsgroups header is malformed.
        /// </summary>
        InvalidNewsgroups = 16,

        /// <summary>
        /// From header is malformed.
        /// </summary>
        InvalidFrom = 17,

        /// <summary>
        /// Path header is malformed.
        /// </summary>
        InvalidPath = 18,

        /// <summary>
        /// Duplicate Message-ID headers were found.
        /// </summary>
        DuplicateMessageId = 19,

        /// <summary>
        /// Duplicate Newsgroups headers were found.
        /// </summary>
        DuplicateNewsgroups = 20,

        /// <summary>
        /// Duplicate Path headers were found.
        /// </summary>
        DuplicatePath = 21,

        /// <summary>
        /// yEnc content was detected and validation failed.
        /// </summary>
        YEncDecodingFailed = 22,
    }

    /// <summary>
    /// Identifies frequently accessed NNTP header names without allocating strings per header.
    /// </summary>
    internal enum NntpArticleHeaderName
    {
        /// <summary>
        /// Header name is not one of the parser's known fast-path names.
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// <c>Date</c> header.
        /// </summary>
        Date = 1,

        /// <summary>
        /// <c>Injection-Date</c> header.
        /// </summary>
        InjectionDate = 2,

        /// <summary>
        /// <c>NNTP-Posting-Date</c> header.
        /// </summary>
        NntpPostingDate = 3,

        /// <summary>
        /// <c>Posted</c> header.
        /// </summary>
        Posted = 4,

        /// <summary>
        /// <c>X-Date</c> header.
        /// </summary>
        XDate = 5,

        /// <summary>
        /// <c>Delivery-Date</c> header.
        /// </summary>
        DeliveryDate = 6,

        /// <summary>
        /// <c>Path</c> header.
        /// </summary>
        Path = 7,

        /// <summary>
        /// <c>Message-ID</c> header.
        /// </summary>
        MessageId = 8,

        /// <summary>
        /// <c>Newsgroups</c> header.
        /// </summary>
        Newsgroups = 9,

        /// <summary>
        /// <c>From</c> header.
        /// </summary>
        From = 10,

        /// <summary>
        /// <c>Subject</c> header.
        /// </summary>
        Subject = 11,

        /// <summary>
        /// <c>Content-Type</c> header.
        /// </summary>
        ContentType = 12,

        /// <summary>
        /// <c>Content-Transfer-Encoding</c> header.
        /// </summary>
        ContentTransferEncoding = 13,
    }

    /// <summary>
    /// Describes one parsed header as slices into the original article buffer.
    /// </summary>
    /// <param name="KnownName">Known-name classifier for fast-path lookup.</param>
    /// <param name="NameOffset">Byte offset of header name start within original article buffer.</param>
    /// <param name="NameLength">Header-name byte length.</param>
    /// <param name="ValueOffset">Byte offset of first header-value byte within original article buffer.</param>
    /// <param name="ValueLength">Header-value byte length spanning folded continuation bytes exactly as received.</param>
    internal readonly record struct NntpArticleHeaderEntry(
        NntpArticleHeaderName KnownName,
        int NameOffset,
        int NameLength,
        int ValueOffset,
        int ValueLength);

    /// <summary>
    /// Defines the parser limits that bound hostile-input scanning and memory use on the hot path.
    /// </summary>
    /// <param name="MaxArticleBytes">Maximum article payload size accepted by the parser.</param>
    /// <param name="MaxHeaderSectionBytes">Maximum bytes scanned while searching header/body separation.</param>
    /// <param name="MaxHeaderCount">Maximum number of header fields accepted.</param>
    /// <param name="MaxHeaderLineBytes">Maximum bytes for one unfolded physical header line.</param>
    /// <param name="MaxHeaderNameBytes">Maximum bytes for one header name token.</param>
    /// <param name="MaxHeaderValueBytes">Maximum bytes for one unfolded header value.</param>
    /// <param name="YEncDetectionScanBytes">Maximum body bytes scanned for yEnc marker detection prior to optional full validation.</param>
    internal readonly record struct NntpArticleParserOptions(
        int MaxArticleBytes,
        int MaxHeaderSectionBytes,
        int MaxHeaderCount,
        int MaxHeaderLineBytes,
        int MaxHeaderNameBytes,
        int MaxHeaderValueBytes,
        int YEncDetectionScanBytes)
    {
        /// <summary>
        /// Gets the default parser limits tuned for hostile-input safety and transit workloads.
        /// </summary>
        /// <value>Default guardrails for article size, header scanning, and bounded yEnc detection.</value>
        internal static NntpArticleParserOptions Default => new(
            MaxArticleBytes: 64 * 1024 * 1024,
            MaxHeaderSectionBytes: 256 * 1024,
            MaxHeaderCount: 1024,
            MaxHeaderLineBytes: 16 * 1024,
            MaxHeaderNameBytes: 128,
            MaxHeaderValueBytes: 64 * 1024,
            YEncDetectionScanBytes: 64 * 1024);
    }

    /// <summary>
    /// Represents the complete output of one NNTP article parse operation.
    /// </summary>
    /// <remarks>
    /// Header, body, and original-header-value members are slices over the caller-supplied article buffer; they do not copy payload data.
    /// </remarks>
    /// <param name="IsAccepted">Indicates whether the article passed parser validation and is suitable for downstream processing.</param>
    /// <param name="FailureCode">Machine-readable rejection classification when <paramref name="IsAccepted"/> is false.</param>
    /// <param name="ArticleType">Detected article type classification.</param>
    /// <param name="ArticleBytes">Original article bytes supplied to the parser.</param>
    /// <param name="HeaderBytes">Header section bytes as a slice of <paramref name="ArticleBytes"/>.</param>
    /// <param name="BodyBytes">Body section bytes as a slice of <paramref name="ArticleBytes"/>.</param>
    /// <param name="Headers">Parsed header entries preserving original order.</param>
    /// <param name="DateFailureReason">Date parse classification when date canonicalization fails.</param>
    /// <param name="CanonicalUtcDate">Canonical UTC date string when date canonicalization succeeds.</param>
    /// <param name="OriginalDateValue">Original date-header value bytes used by the date resolver.</param>
    /// <param name="CanonicalPath">Canonical Path value after deterministic BackFiller FQDN augmentation logic.</param>
    /// <param name="OriginalPathValue">Original Path-header bytes when present.</param>
    /// <param name="YEncDetected">Indicates whether yEnc markers were detected in the body scan.</param>
    /// <param name="YEncValidation">yEnc validation result when yEnc was detected; non-yEnc success when not detected.</param>
    internal readonly record struct NntpArticleParseResult(
        bool IsAccepted,
        NntpArticleParseFailureCode FailureCode,
        NntpArticleType ArticleType,
        ReadOnlyMemory<byte> ArticleBytes,
        ReadOnlyMemory<byte> HeaderBytes,
        ReadOnlyMemory<byte> BodyBytes,
        IReadOnlyList<NntpArticleHeaderEntry> Headers,
        DateParseFailureReason DateFailureReason,
        string CanonicalUtcDate,
        ReadOnlyMemory<byte> OriginalDateValue,
        string CanonicalPath,
        ReadOnlyMemory<byte> OriginalPathValue,
        bool YEncDetected,
        YEncArticleValidationResult YEncValidation)
    {
        /// <summary>
        /// Creates a rejected parse result while preserving already parsed slices and metadata.
        /// </summary>
        /// <param name="failureCode">Failure classification.</param>
        /// <param name="articleType">Current best-effort article classification.</param>
        /// <param name="articleBytes">Original article bytes.</param>
        /// <param name="headerBytes">Header section bytes.</param>
        /// <param name="bodyBytes">Body section bytes.</param>
        /// <param name="headers">Parsed headers.</param>
        /// <param name="dateFailureReason">Date parse failure reason if applicable.</param>
        /// <param name="canonicalUtcDate">Canonical date value if one was produced before rejection.</param>
        /// <param name="originalDateValue">Original date-header value bytes if available.</param>
        /// <param name="canonicalPath">Canonical path value if available.</param>
        /// <param name="originalPathValue">Original path value bytes if available.</param>
        /// <param name="yEncDetected">Indicates whether yEnc was detected.</param>
        /// <param name="yEncValidation">yEnc validation result.</param>
        /// <returns>Rejected parse result.</returns>
        internal static NntpArticleParseResult Rejected(
            NntpArticleParseFailureCode failureCode,
            NntpArticleType articleType,
            ReadOnlyMemory<byte> articleBytes,
            ReadOnlyMemory<byte> headerBytes,
            ReadOnlyMemory<byte> bodyBytes,
            IReadOnlyList<NntpArticleHeaderEntry> headers,
            DateParseFailureReason dateFailureReason = DateParseFailureReason.None,
            string canonicalUtcDate = "",
            ReadOnlyMemory<byte> originalDateValue = default,
            string canonicalPath = "",
            ReadOnlyMemory<byte> originalPathValue = default,
            bool yEncDetected = false,
            YEncArticleValidationResult yEncValidation = default)
        {
            if (yEncValidation == default)
            {
                yEncValidation = YEncArticleValidationResult.ValidNonYEnc();
            }

            return new NntpArticleParseResult(
                IsAccepted: false,
                FailureCode: failureCode,
                ArticleType: articleType,
                ArticleBytes: articleBytes,
                HeaderBytes: headerBytes,
                BodyBytes: bodyBytes,
                Headers: headers,
                DateFailureReason: dateFailureReason,
                CanonicalUtcDate: canonicalUtcDate,
                OriginalDateValue: originalDateValue,
                CanonicalPath: canonicalPath,
                OriginalPathValue: originalPathValue,
                YEncDetected: yEncDetected,
                YEncValidation: yEncValidation);
        }
    }
}
