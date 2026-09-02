// <copyright file="NntpArticleParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Parsing
// High-performance byte-oriented NNTP article parser that separates headers/body,
// validates critical headers, canonicalizes Date and Path, classifies article type,
// and conditionally validates yEnc payload integrity.

using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.DateParser;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;

namespace VectorNNTP.Backfiller.Runtime.Articles.Parsing
{
    /// <summary>
    /// Parses untrusted NNTP article bytes into a compact structured result for downstream policy decisions.
    /// </summary>
    /// <remarks>
    /// <para>Contract: input bytes are expected to be article-normalized payload bytes received after transport-level framing.</para>
    /// <para>The parser does not perform download, retry, queueing, or publish decisions; it only validates and classifies content.</para>
    /// </remarks>
    internal sealed class NntpArticleParser
    {
        /// <summary>
        /// Maximum accepted Message-ID value length.
        /// </summary>
        private const int MaxMessageIdLength = 512;

        /// <summary>
        /// Maximum accepted Newsgroups value length.
        /// </summary>
        private const int MaxNewsgroupsLength = 4096;

        /// <summary>
        /// Maximum accepted Path value length.
        /// </summary>
        private const int MaxPathLength = 8192;

        /// <summary>
        /// Maximum accepted From value length.
        /// </summary>
        private const int MaxFromLength = 2048;

        /// <summary>
        /// yEnc marker used for bounded detection scans.
        /// </summary>
        private static ReadOnlySpan<byte> YEncBeginMarker => "=ybegin "u8;

        /// <summary>
        /// MIME multipart indicator used as a type hint.
        /// </summary>
        private static ReadOnlySpan<byte> MultipartMarker => "multipart/"u8;

        /// <summary>
        /// Binary content-transfer-encoding hints.
        /// </summary>
        private static ReadOnlySpan<byte> Base64Encoding => "base64"u8;

        /// <summary>
        /// Binary content-transfer-encoding hints.
        /// </summary>
        private static ReadOnlySpan<byte> BinaryEncoding => "binary"u8;

        /// <summary>
        /// Canonical BackFiller FQDN to prepend into Path normalization.
        /// </summary>
        private readonly string _canonicalBackFillerFqdn;

        /// <summary>
        /// Immutable parser limits for hostile-input protection.
        /// </summary>
        private readonly NntpArticleParserOptions _options;

        /// <summary>
        /// Initializes a new parser instance with immutable runtime identity and parse limits.
        /// </summary>
        /// <param name="canonicalBackFillerFqdn">Canonical BackFiller FQDN used for Path augmentation.</param>
        /// <param name="options">Parser guardrail options.</param>
        internal NntpArticleParser(string canonicalBackFillerFqdn, NntpArticleParserOptions options)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(canonicalBackFillerFqdn);
            _canonicalBackFillerFqdn = canonicalBackFillerFqdn.Trim();
            _options = options;
        }

        /// <summary>
        /// Initializes a new parser instance with default parser limits.
        /// </summary>
        /// <param name="canonicalBackFillerFqdn">Canonical BackFiller FQDN used for Path augmentation.</param>
        internal NntpArticleParser(string canonicalBackFillerFqdn)
            : this(canonicalBackFillerFqdn, NntpArticleParserOptions.Default)
        {
        }

        /// <summary>
        /// Parses one NNTP article payload and returns deterministic acceptance/rejection metadata.
        /// </summary>
        /// <param name="articleBytes">Complete article bytes.</param>
        /// <returns>Parse result with classification, normalized values, and failure metadata.</returns>
        internal NntpArticleParseResult Parse(ReadOnlyMemory<byte> articleBytes)
        {
            if (articleBytes.IsEmpty)
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: NntpArticleParseFailureCode.EmptyArticle,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: ReadOnlyMemory<byte>.Empty,
                    bodyBytes: ReadOnlyMemory<byte>.Empty,
                    headers: []);
            }

            if (articleBytes.Length > _options.MaxArticleBytes)
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: NntpArticleParseFailureCode.ArticleTooLarge,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: ReadOnlyMemory<byte>.Empty,
                    bodyBytes: ReadOnlyMemory<byte>.Empty,
                    headers: []);
            }

            ReadOnlySpan<byte> articleSpan = articleBytes.Span;
            HeaderParseOutcome headerOutcome = TryParseHeaders(articleSpan, articleBytes, _options);
            if (!headerOutcome.Success)
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: headerOutcome.FailureCode,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: headerOutcome.HeaderBytes,
                    bodyBytes: headerOutcome.BodyBytes,
                    headers: headerOutcome.Headers);
            }

            if (!TryValidateMessageId(articleSpan, headerOutcome, out _, out NntpArticleParseFailureCode messageIdFailure))
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: messageIdFailure,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: headerOutcome.HeaderBytes,
                    bodyBytes: headerOutcome.BodyBytes,
                    headers: headerOutcome.Headers);
            }

            if (!TryValidateNewsgroups(articleSpan, headerOutcome, out NntpArticleParseFailureCode newsgroupsFailure))
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: newsgroupsFailure,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: headerOutcome.HeaderBytes,
                    bodyBytes: headerOutcome.BodyBytes,
                    headers: headerOutcome.Headers);
            }

            if (!TryValidateFrom(articleSpan, headerOutcome, out NntpArticleParseFailureCode fromFailure))
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: fromFailure,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: headerOutcome.HeaderBytes,
                    bodyBytes: headerOutcome.BodyBytes,
                    headers: headerOutcome.Headers);
            }

            if (!ArticleDateHeaderResolver.TryGetCanonicalArticleDate(
                    articleBytes,
                    headerOutcome.Headers,
                    out string canonicalDate,
                    out ReadOnlyMemory<byte> originalDateValue,
                    out DateParseFailureReason dateFailure))
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: NntpArticleParseFailureCode.MissingOrInvalidDate,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: headerOutcome.HeaderBytes,
                    bodyBytes: headerOutcome.BodyBytes,
                    headers: headerOutcome.Headers,
                    dateFailureReason: dateFailure);
            }

            if (!TryCanonicalizePath(articleSpan, headerOutcome, _canonicalBackFillerFqdn, out string canonicalPath, out ReadOnlyMemory<byte> originalPathValue, out NntpArticleParseFailureCode pathFailure))
            {
                return NntpArticleParseResult.Rejected(
                    failureCode: pathFailure,
                    articleType: NntpArticleType.Malformed,
                    articleBytes: articleBytes,
                    headerBytes: headerOutcome.HeaderBytes,
                    bodyBytes: headerOutcome.BodyBytes,
                    headers: headerOutcome.Headers,
                    dateFailureReason: DateParseFailureReason.None,
                    canonicalUtcDate: canonicalDate,
                    originalDateValue: originalDateValue);
            }

            bool yEncDetected = DetectYEnc(headerOutcome.BodyBytes.Span, _options.YEncDetectionScanBytes);
            NntpArticleType articleType = ClassifyArticle(articleSpan, headerOutcome, yEncDetected);
            YEncArticleValidationResult yEncValidation = YEncArticleValidationResult.ValidNonYEnc();
            if (yEncDetected)
            {
                yEncValidation = YEncArticleValidator.Validate(headerOutcome.BodyBytes.Span);
                if (!yEncValidation.IsValid)
                {
                    return NntpArticleParseResult.Rejected(
                        failureCode: NntpArticleParseFailureCode.YEncDecodingFailed,
                        articleType: NntpArticleType.YEnc,
                        articleBytes: articleBytes,
                        headerBytes: headerOutcome.HeaderBytes,
                        bodyBytes: headerOutcome.BodyBytes,
                        headers: headerOutcome.Headers,
                        dateFailureReason: DateParseFailureReason.None,
                        canonicalUtcDate: canonicalDate,
                        originalDateValue: originalDateValue,
                        canonicalPath: canonicalPath,
                        originalPathValue: originalPathValue,
                        yEncDetected: true,
                        yEncValidation: yEncValidation);
                }

                articleType = yEncValidation.Status == YEncArticleValidationStatus.ValidMultiPart
                    ? NntpArticleType.YEnc
                    : NntpArticleType.YEnc;
            }

            return new NntpArticleParseResult(
                IsAccepted: true,
                FailureCode: NntpArticleParseFailureCode.None,
                ArticleType: articleType,
                ArticleBytes: articleBytes,
                HeaderBytes: headerOutcome.HeaderBytes,
                BodyBytes: headerOutcome.BodyBytes,
                Headers: headerOutcome.Headers,
                DateFailureReason: DateParseFailureReason.None,
                CanonicalUtcDate: canonicalDate,
                OriginalDateValue: originalDateValue,
                CanonicalPath: canonicalPath,
                OriginalPathValue: originalPathValue,
                YEncDetected: yEncDetected,
                YEncValidation: yEncValidation);
        }

        /// <summary>
        /// Parses headers and computes body offsets without decoding the full article.
        /// </summary>
        /// <param name="articleSpan">Article bytes.</param>
        /// <param name="articleBytes">Article memory for slicing.</param>
        /// <param name="options">Parser limits.</param>
        /// <returns>Header parse outcome.</returns>
        private static HeaderParseOutcome TryParseHeaders(ReadOnlySpan<byte> articleSpan, ReadOnlyMemory<byte> articleBytes, NntpArticleParserOptions options)
        {
            int index = 0;
            int headerStart = 0;
            int headerBytesScanned = 0;
            List<NntpArticleHeaderEntry> headers = new(capacity: 48);

            int currentHeaderNameOffset = -1;
            int currentHeaderNameLength = 0;
            int currentHeaderValueOffset = -1;
            int currentHeaderValueEndExclusive = -1;
            NntpArticleHeaderName currentKnownName = NntpArticleHeaderName.Unknown;
            bool currentHeaderHasValue = false;

            bool firstLineChecked = false;

            while (index < articleSpan.Length)
            {
                int lineEnd = FindLineTerminator(articleSpan, index);
                int lineContentEnd = lineEnd >= 0 ? lineEnd : articleSpan.Length;
                int lineLength = lineContentEnd - index;
                if (lineLength > options.MaxHeaderLineBytes)
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.HeaderLineTooLong, articleBytes, headerStart, index, headers);
                }

                if (lineLength == 0)
                {
                    if (currentHeaderNameOffset >= 0)
                    {
                        headers.Add(new NntpArticleHeaderEntry(
                            currentKnownName,
                            currentHeaderNameOffset,
                            currentHeaderNameLength,
                            currentHeaderValueOffset,
                            currentHeaderHasValue ? currentHeaderValueEndExclusive - currentHeaderValueOffset : 0));
                    }

                    int bodyOffset = lineEnd >= 0 ? AdvancePastTerminator(articleSpan, lineEnd) : lineContentEnd;
                    int headerLength = bodyOffset;
                    return headerLength > options.MaxHeaderSectionBytes
                        ? HeaderParseOutcome.Fail(NntpArticleParseFailureCode.HeaderSectionTooLarge, articleBytes, headerStart, bodyOffset, headers)
                        : HeaderParseOutcome.SuccessResult(
                        articleBytes,
                        articleBytes[..headerLength],
                        articleBytes[bodyOffset..],
                        [.. headers]);
                }

                headerBytesScanned += lineLength;
                if (headerBytesScanned > options.MaxHeaderSectionBytes)
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.HeaderSectionTooLarge, articleBytes, headerStart, lineContentEnd, headers);
                }

                ReadOnlySpan<byte> line = articleSpan.Slice(index, lineLength);
                if (ContainsIllegalHeaderControl(line))
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.ContainsIllegalControlByte, articleBytes, headerStart, lineContentEnd, headers);
                }

                if (!firstLineChecked)
                {
                    firstLineChecked = true;
                    int firstColon = line.IndexOf((byte)':');
                    if (firstColon < 0)
                    {
                        return line[0] is (byte)' ' or (byte)'\t'
                            ? HeaderParseOutcome.Fail(NntpArticleParseFailureCode.MalformedHeaderContinuation, articleBytes, headerStart, lineContentEnd, headers)
                            : HeaderParseOutcome.SuccessResult(articleBytes, ReadOnlyMemory<byte>.Empty, articleBytes, []);
                    }
                }

                bool continuation = line[0] is (byte)' ' or (byte)'\t';
                if (continuation)
                {
                    if (currentHeaderNameOffset < 0)
                    {
                        return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.MalformedHeaderContinuation, articleBytes, headerStart, lineContentEnd, headers);
                    }

                    currentHeaderHasValue = true;
                    currentHeaderValueEndExclusive = lineContentEnd;
                    index = lineEnd >= 0 ? AdvancePastTerminator(articleSpan, lineEnd) : articleSpan.Length;
                    continue;
                }

                if (currentHeaderNameOffset >= 0)
                {
                    headers.Add(new NntpArticleHeaderEntry(
                        currentKnownName,
                        currentHeaderNameOffset,
                        currentHeaderNameLength,
                        currentHeaderValueOffset,
                        currentHeaderHasValue ? currentHeaderValueEndExclusive - currentHeaderValueOffset : 0));
                }

                if (headers.Count > options.MaxHeaderCount)
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.TooManyHeaders, articleBytes, headerStart, lineContentEnd, headers);
                }

                int colonIndex = line.IndexOf((byte)':');
                if (colonIndex < 0)
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.MissingHeaderBodySeparator, articleBytes, headerStart, lineContentEnd, headers);
                }

                if (colonIndex == 0)
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.MalformedHeader, articleBytes, headerStart, lineContentEnd, headers);
                }

                if (colonIndex > options.MaxHeaderNameBytes)
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.HeaderNameTooLong, articleBytes, headerStart, lineContentEnd, headers);
                }

                int valueStartInLine = colonIndex + 1;
                while (valueStartInLine < line.Length && (line[valueStartInLine] == (byte)' ' || line[valueStartInLine] == (byte)'\t'))
                {
                    valueStartInLine++;
                }

                int valueEndInLine = line.Length;
                while (valueEndInLine > valueStartInLine && (line[valueEndInLine - 1] == (byte)' ' || line[valueEndInLine - 1] == (byte)'\t'))
                {
                    valueEndInLine--;
                }

                int headerValueLength = valueEndInLine - valueStartInLine;
                if (headerValueLength > options.MaxHeaderValueBytes)
                {
                    return HeaderParseOutcome.Fail(NntpArticleParseFailureCode.HeaderValueTooLong, articleBytes, headerStart, lineContentEnd, headers);
                }

                currentHeaderNameOffset = index;
                currentHeaderNameLength = colonIndex;
                currentHeaderValueOffset = index + valueStartInLine;
                currentHeaderValueEndExclusive = index + valueEndInLine;
                currentHeaderHasValue = headerValueLength > 0;
                currentKnownName = ClassifyKnownHeaderName(articleSpan.Slice(currentHeaderNameOffset, currentHeaderNameLength));

                index = lineEnd >= 0 ? AdvancePastTerminator(articleSpan, lineEnd) : articleSpan.Length;
            }

            return HeaderParseOutcome.Fail(
                NntpArticleParseFailureCode.MissingHeaderBodySeparator,
                articleBytes,
                headerStart,
                articleBytes.Length,
                headers);
        }

        /// <summary>
        /// Classifies known header names using allocation-free ASCII comparisons.
        /// </summary>
        /// <param name="nameBytes">Header-name bytes.</param>
        /// <returns>Known header-name classification.</returns>
        private static NntpArticleHeaderName ClassifyKnownHeaderName(ReadOnlySpan<byte> nameBytes)
        {
            return AsciiEqualsIgnoreCase(nameBytes, "Date"u8)
                ? NntpArticleHeaderName.Date
                : AsciiEqualsIgnoreCase(nameBytes, "Injection-Date"u8)
                ? NntpArticleHeaderName.InjectionDate
                : AsciiEqualsIgnoreCase(nameBytes, "NNTP-Posting-Date"u8)
                ? NntpArticleHeaderName.NntpPostingDate
                : AsciiEqualsIgnoreCase(nameBytes, "Posted"u8)
                ? NntpArticleHeaderName.Posted
                : AsciiEqualsIgnoreCase(nameBytes, "X-Date"u8)
                ? NntpArticleHeaderName.XDate
                : AsciiEqualsIgnoreCase(nameBytes, "Delivery-Date"u8)
                ? NntpArticleHeaderName.DeliveryDate
                : AsciiEqualsIgnoreCase(nameBytes, "Path"u8)
                ? NntpArticleHeaderName.Path
                : AsciiEqualsIgnoreCase(nameBytes, "Message-ID"u8)
                ? NntpArticleHeaderName.MessageId
                : AsciiEqualsIgnoreCase(nameBytes, "Newsgroups"u8)
                ? NntpArticleHeaderName.Newsgroups
                : AsciiEqualsIgnoreCase(nameBytes, "From"u8)
                ? NntpArticleHeaderName.From
                : AsciiEqualsIgnoreCase(nameBytes, "Subject"u8)
                ? NntpArticleHeaderName.Subject
                : AsciiEqualsIgnoreCase(nameBytes, "Content-Type"u8)
                ? NntpArticleHeaderName.ContentType
                : AsciiEqualsIgnoreCase(nameBytes, "Content-Transfer-Encoding"u8)
                ? NntpArticleHeaderName.ContentTransferEncoding
                : NntpArticleHeaderName.Unknown;
        }

        /// <summary>
        /// Validates Message-ID header presence, uniqueness, and minimal syntax.
        /// </summary>
        /// <param name="articleSpan">Article bytes.</param>
        /// <param name="headerOutcome">Parsed header outcome.</param>
        /// <param name="messageIdBytes">Message-ID value bytes when valid.</param>
        /// <param name="failureCode">Failure code when invalid.</param>
        /// <returns><see langword="true"/> when Message-ID is valid.</returns>
        private static bool TryValidateMessageId(
            ReadOnlySpan<byte> articleSpan,
            HeaderParseOutcome headerOutcome,
            out ReadOnlyMemory<byte> messageIdBytes,
            out NntpArticleParseFailureCode failureCode)
        {
            messageIdBytes = default;
            failureCode = NntpArticleParseFailureCode.None;

            NntpArticleHeaderEntry messageIdHeader = default;
            bool found = false;

            for (int i = 0; i < headerOutcome.Headers.Count; i++)
            {
                NntpArticleHeaderEntry entry = headerOutcome.Headers[i];
                if (entry.KnownName != NntpArticleHeaderName.MessageId)
                {
                    continue;
                }

                if (found)
                {
                    failureCode = NntpArticleParseFailureCode.DuplicateMessageId;
                    return false;
                }

                found = true;
                messageIdHeader = entry;
            }

            if (!found)
            {
                failureCode = NntpArticleParseFailureCode.MissingMessageId;
                return false;
            }

            if (messageIdHeader.ValueLength is <= 2 or > MaxMessageIdLength)
            {
                failureCode = NntpArticleParseFailureCode.InvalidMessageId;
                return false;
            }

            ReadOnlySpan<byte> value = articleSpan.Slice(messageIdHeader.ValueOffset, messageIdHeader.ValueLength);
            if (value[0] != (byte)'<' || value[^1] != (byte)'>')
            {
                failureCode = NntpArticleParseFailureCode.InvalidMessageId;
                return false;
            }

            int atIndex = value.IndexOf((byte)'@');
            if (atIndex <= 1 || atIndex >= value.Length - 2)
            {
                failureCode = NntpArticleParseFailureCode.InvalidMessageId;
                return false;
            }

            for (int i = 1; i < value.Length - 1; i++)
            {
                byte b = value[i];
                if (b is < 0x21 or > 0x7E or ((byte)'<') or ((byte)'>') or ((byte)' '))
                {
                    failureCode = NntpArticleParseFailureCode.InvalidMessageId;
                    return false;
                }
            }

            messageIdBytes = headerOutcome.ArticleBytes.Slice(messageIdHeader.ValueOffset, messageIdHeader.ValueLength);
            return true;
        }

        /// <summary>
        /// Validates Newsgroups header presence, uniqueness, and basic structure.
        /// </summary>
        /// <param name="articleSpan">Article bytes.</param>
        /// <param name="headerOutcome">Header parse outcome.</param>
        /// <param name="failureCode">Failure code when invalid.</param>
        /// <returns><see langword="true"/> when valid.</returns>
        private static bool TryValidateNewsgroups(ReadOnlySpan<byte> articleSpan, HeaderParseOutcome headerOutcome, out NntpArticleParseFailureCode failureCode)
        {
            failureCode = NntpArticleParseFailureCode.None;
            NntpArticleHeaderEntry newsgroupsHeader = default;
            bool found = false;

            for (int i = 0; i < headerOutcome.Headers.Count; i++)
            {
                NntpArticleHeaderEntry entry = headerOutcome.Headers[i];
                if (entry.KnownName != NntpArticleHeaderName.Newsgroups)
                {
                    continue;
                }

                if (found)
                {
                    failureCode = NntpArticleParseFailureCode.DuplicateNewsgroups;
                    return false;
                }

                found = true;
                newsgroupsHeader = entry;
            }

            if (!found)
            {
                failureCode = NntpArticleParseFailureCode.MissingNewsgroups;
                return false;
            }

            if (newsgroupsHeader.ValueLength is 0 or > MaxNewsgroupsLength)
            {
                failureCode = NntpArticleParseFailureCode.InvalidNewsgroups;
                return false;
            }

            ReadOnlySpan<byte> value = articleSpan.Slice(newsgroupsHeader.ValueOffset, newsgroupsHeader.ValueLength);
            bool tokenHasChar = false;
            for (int i = 0; i < value.Length; i++)
            {
                byte b = value[i];
                if (b == (byte)',')
                {
                    if (!tokenHasChar)
                    {
                        failureCode = NntpArticleParseFailureCode.InvalidNewsgroups;
                        return false;
                    }

                    tokenHasChar = false;
                    continue;
                }

                if (b is (byte)' ' or (byte)'\t')
                {
                    continue;
                }

                if (!IsPlausibleNewsgroupChar(b))
                {
                    failureCode = NntpArticleParseFailureCode.InvalidNewsgroups;
                    return false;
                }

                tokenHasChar = true;
            }

            if (!tokenHasChar)
            {
                failureCode = NntpArticleParseFailureCode.InvalidNewsgroups;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Validates optional From header structure when present.
        /// </summary>
        /// <param name="articleSpan">Article bytes.</param>
        /// <param name="headerOutcome">Header parse outcome.</param>
        /// <param name="failureCode">Failure code when invalid.</param>
        /// <returns><see langword="true"/> when valid or absent.</returns>
        private static bool TryValidateFrom(ReadOnlySpan<byte> articleSpan, HeaderParseOutcome headerOutcome, out NntpArticleParseFailureCode failureCode)
        {
            failureCode = NntpArticleParseFailureCode.None;

            for (int i = 0; i < headerOutcome.Headers.Count; i++)
            {
                NntpArticleHeaderEntry entry = headerOutcome.Headers[i];
                if (entry.KnownName != NntpArticleHeaderName.From)
                {
                    continue;
                }

                if (entry.ValueLength is 0 or > MaxFromLength)
                {
                    failureCode = NntpArticleParseFailureCode.InvalidFrom;
                    return false;
                }

                ReadOnlySpan<byte> value = articleSpan.Slice(entry.ValueOffset, entry.ValueLength);
                int at = value.IndexOf((byte)'@');
                if (at <= 0 || at >= value.Length - 1)
                {
                    failureCode = NntpArticleParseFailureCode.InvalidFrom;
                    return false;
                }

                bool hasPrintable = false;
                for (int c = 0; c < value.Length; c++)
                {
                    byte b = value[c];
                    if (b is < 0x20 or > 0x7E)
                    {
                        failureCode = NntpArticleParseFailureCode.InvalidFrom;
                        return false;
                    }

                    if (b is not (byte)' ' and not (byte)'\t')
                    {
                        hasPrintable = true;
                    }
                }

                if (!hasPrintable)
                {
                    failureCode = NntpArticleParseFailureCode.InvalidFrom;
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Canonicalizes Path header by prepending local FQDN when missing and preserving deterministic form.
        /// </summary>
        /// <param name="articleSpan">Article bytes.</param>
        /// <param name="headerOutcome">Header parse outcome.</param>
        /// <param name="fqdn">Configured canonical BackFiller FQDN.</param>
        /// <param name="canonicalPath">Canonicalized path output.</param>
        /// <param name="originalPathValue">Original path value slice when present.</param>
        /// <param name="failureCode">Failure code when invalid.</param>
        /// <returns><see langword="true"/> when canonicalization succeeds.</returns>
        private static bool TryCanonicalizePath(
            ReadOnlySpan<byte> articleSpan,
            HeaderParseOutcome headerOutcome,
            string fqdn,
            out string canonicalPath,
            out ReadOnlyMemory<byte> originalPathValue,
            out NntpArticleParseFailureCode failureCode)
        {
            canonicalPath = fqdn;
            originalPathValue = default;
            failureCode = NntpArticleParseFailureCode.None;

            NntpArticleHeaderEntry pathHeader = default;
            bool found = false;

            for (int i = 0; i < headerOutcome.Headers.Count; i++)
            {
                NntpArticleHeaderEntry entry = headerOutcome.Headers[i];
                if (entry.KnownName != NntpArticleHeaderName.Path)
                {
                    continue;
                }

                if (found)
                {
                    failureCode = NntpArticleParseFailureCode.DuplicatePath;
                    return false;
                }

                found = true;
                pathHeader = entry;
            }

            if (!found)
            {
                return true;
            }

            if (pathHeader.ValueLength > MaxPathLength)
            {
                failureCode = NntpArticleParseFailureCode.InvalidPath;
                return false;
            }

            ReadOnlySpan<byte> rawPath = articleSpan.Slice(pathHeader.ValueOffset, pathHeader.ValueLength);
            for (int i = 0; i < rawPath.Length; i++)
            {
                byte b = rawPath[i];
                if (b is < 0x20 or > 0x7E)
                {
                    failureCode = NntpArticleParseFailureCode.InvalidPath;
                    return false;
                }
            }

            originalPathValue = headerOutcome.ArticleBytes.Slice(pathHeader.ValueOffset, pathHeader.ValueLength);
            string existing = Encoding.ASCII.GetString(rawPath).Trim();
            if (existing.Length == 0)
            {
                canonicalPath = fqdn;
                return true;
            }

            string[] splitParts = existing.Split('!', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            List<string> parts = new(splitParts.Length + 1);

            bool alreadyPresent = false;
            for (int i = 0; i < splitParts.Length; i++)
            {
                string part = splitParts[i];
                if (!IsValidPathComponent(part))
                {
                    failureCode = NntpArticleParseFailureCode.InvalidPath;
                    return false;
                }

                if (string.Equals(part, fqdn, StringComparison.OrdinalIgnoreCase))
                {
                    alreadyPresent = true;
                }

                parts.Add(part);
            }

            if (parts.Count == 0)
            {
                canonicalPath = fqdn;
                return true;
            }

            if (alreadyPresent)
            {
                canonicalPath = string.Join('!', parts);
                return true;
            }

            canonicalPath = string.Create(
                GetPathLength(fqdn, parts),
                (fqdn, parts),
                // <summary>
                // Coordinates static for nntp article parser.
                // </summary>
                static (span, state) =>
                {
                    int offset = 0;
                    state.fqdn.AsSpan().CopyTo(span);
                    offset += state.fqdn.Length;
                    for (int i = 0; i < state.parts.Count; i++)
                    {
                        span[offset++] = '!';
                        state.parts[i].AsSpan().CopyTo(span[offset..]);
                        offset += state.parts[i].Length;
                    }
                });
            return true;
        }

        /// <summary>
        /// Computes canonical-path string length.
        /// </summary>
        /// <param name="fqdn">Configured FQDN component.</param>
        /// <param name="parts">Existing path components.</param>
        /// <returns>Total character count.</returns>
        private static int GetPathLength(string fqdn, List<string> parts)
        {
            int total = fqdn.Length;
            for (int i = 0; i < parts.Count; i++)
            {
                total += 1 + parts[i].Length;
            }

            return total;
        }

        /// <summary>
        /// Performs low-cost content classification based on header hints and previously computed yEnc detection state.
        /// </summary>
        /// <param name="articleSpan">Article bytes.</param>
        /// <param name="headerOutcome">Header parse outcome.</param>
        /// <param name="yEncDetected">Precomputed yEnc marker detection result for the configured scan window.</param>
        /// <returns>Detected article type.</returns>
        private static NntpArticleType ClassifyArticle(ReadOnlySpan<byte> articleSpan, HeaderParseOutcome headerOutcome, bool yEncDetected)
        {
            bool mimeMultipart = false;
            bool binaryTransferEncoding = false;

            for (int i = 0; i < headerOutcome.Headers.Count; i++)
            {
                NntpArticleHeaderEntry header = headerOutcome.Headers[i];
                if (header.KnownName == NntpArticleHeaderName.ContentType)
                {
                    ReadOnlySpan<byte> value = articleSpan.Slice(header.ValueOffset, header.ValueLength);
                    if (IndexOfAsciiIgnoreCase(value, MultipartMarker) >= 0)
                    {
                        mimeMultipart = true;
                    }
                }
                else if (header.KnownName == NntpArticleHeaderName.ContentTransferEncoding)
                {
                    ReadOnlySpan<byte> value = articleSpan.Slice(header.ValueOffset, header.ValueLength);
                    if (IndexOfAsciiIgnoreCase(value, Base64Encoding) >= 0 || IndexOfAsciiIgnoreCase(value, BinaryEncoding) >= 0)
                    {
                        binaryTransferEncoding = true;
                    }
                }
            }

            return yEncDetected
                ? NntpArticleType.YEnc
                : mimeMultipart
                ? NntpArticleType.MimeMultipart
                : binaryTransferEncoding
                ? NntpArticleType.BinaryEncoded
                : IsLikelyTextBody(headerOutcome.BodyBytes.Span) ? NntpArticleType.Text : NntpArticleType.Unknown;
        }

        /// <summary>
        /// Returns a value indicating whether body bytes contain yEnc begin markers within a bounded scan window.
        /// </summary>
        /// <param name="body">Article body bytes.</param>
        /// <param name="maxScanBytes">Maximum bytes to inspect.</param>
        /// <returns><see langword="true"/> when yEnc markers are detected.</returns>
        private static bool DetectYEnc(ReadOnlySpan<byte> body, int maxScanBytes)
        {
            int scanLength = Math.Min(body.Length, maxScanBytes);
            int position = 0;
            while (position < scanLength)
            {
                int lineEnd = FindLineTerminator(body, position);
                int lineContentEnd = lineEnd >= 0 ? lineEnd : scanLength;
                ReadOnlySpan<byte> line = body[position..lineContentEnd];
                if (line.StartsWith(YEncBeginMarker))
                {
                    return true;
                }

                if (lineEnd < 0)
                {
                    break;
                }

                position = AdvancePastTerminator(body, lineEnd);
            }

            return false;
        }

        /// <summary>
        /// Determines whether a body appears primarily textual using bounded control-byte heuristics.
        /// </summary>
        /// <param name="body">Body bytes.</param>
        /// <returns><see langword="true"/> when body appears textual.</returns>
        private static bool IsLikelyTextBody(ReadOnlySpan<byte> body)
        {
            int sampleLength = Math.Min(body.Length, 4096);
            if (sampleLength == 0)
            {
                return true;
            }

            int suspicious = 0;
            for (int i = 0; i < sampleLength; i++)
            {
                byte b = body[i];
                if (b == 0)
                {
                    return false;
                }

                if (b is < 0x09 or (> 0x0D and < 0x20))
                {
                    suspicious++;
                }
            }

            return sampleLength < 16 ? suspicious == 0 : suspicious < (sampleLength / 16);
        }

        /// <summary>
        /// Finds one line terminator from an index, supporting CRLF, LF-only, and CR-only separators.
        /// </summary>
        /// <param name="buffer">Input bytes.</param>
        /// <param name="start">Start offset.</param>
        /// <returns>Line-terminator index, or -1 when none remains.</returns>
        private static int FindLineTerminator(ReadOnlySpan<byte> buffer, int start)
        {
            for (int i = start; i < buffer.Length; i++)
            {
                byte b = buffer[i];
                if (b == (byte)'\r')
                {
                    return i;
                }

                if (b == (byte)'\n')
                {
                    return i;
                }

                if (b == 0)
                {
                    return -2;
                }
            }

            return -1;
        }

        /// <summary>
        /// Advances index past CRLF or LF line terminators.
        /// </summary>
        /// <param name="buffer">Input bytes.</param>
        /// <param name="lineTerminatorIndex">Terminator index returned by <see cref="FindLineTerminator"/>.</param>
        /// <returns>Offset at next line start.</returns>
        private static int AdvancePastTerminator(ReadOnlySpan<byte> buffer, int lineTerminatorIndex)
        {
            return lineTerminatorIndex < 0 || lineTerminatorIndex >= buffer.Length
                ? buffer.Length
                : buffer[lineTerminatorIndex] == (byte)'\r' && lineTerminatorIndex + 1 < buffer.Length && buffer[lineTerminatorIndex + 1] == (byte)'\n'
                ? lineTerminatorIndex + 2
                : lineTerminatorIndex + 1;
        }

        /// <summary>
        /// Returns a value indicating whether a header line contains illegal control bytes.
        /// </summary>
        /// <param name="line">Header line bytes excluding terminator.</param>
        /// <returns><see langword="true"/> when illegal bytes are present.</returns>
        private static bool ContainsIllegalHeaderControl(ReadOnlySpan<byte> line)
        {
            for (int i = 0; i < line.Length; i++)
            {
                byte b = line[i];
                if (b == 0)
                {
                    return true;
                }

                if (b is < 0x20 and not ((byte)'\t'))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Returns a value indicating whether a byte is plausible within a newsgroup token.
        /// </summary>
        /// <param name="b">Byte to inspect.</param>
        /// <returns><see langword="true"/> when plausible.</returns>
        private static bool IsPlausibleNewsgroupChar(byte b)
        {
            return b is (>= ((byte)'a') and <= ((byte)'z')) or (>= ((byte)'A') and <= ((byte)'Z')) or (>= ((byte)'0') and <= ((byte)'9')) or (byte)'.' or (byte)'-' or (byte)'_' or (byte)'+';
        }

        /// <summary>
        /// Returns a value indicating whether one Path component is syntactically acceptable.
        /// </summary>
        /// <param name="component">Path component.</param>
        /// <returns><see langword="true"/> when component is valid.</returns>
        private static bool IsValidPathComponent(string component)
        {
            for (int i = 0; i < component.Length; i++)
            {
                char c = component[i];
                if (c is <= (char)0x20 or '!' or > (char)0x7E)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Performs case-insensitive ASCII equality without allocations.
        /// </summary>
        /// <param name="left">First byte span.</param>
        /// <param name="right">Second byte span.</param>
        /// <returns><see langword="true"/> when equal ignoring ASCII case.</returns>
        private static bool AsciiEqualsIgnoreCase(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
        {
            if (left.Length != right.Length)
            {
                return false;
            }

            for (int i = 0; i < left.Length; i++)
            {
                if (ToLowerAscii(left[i]) != ToLowerAscii(right[i]))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Performs case-insensitive ASCII substring search.
        /// </summary>
        /// <param name="haystack">Input bytes.</param>
        /// <param name="needle">Pattern bytes.</param>
        /// <returns>First index or -1 when not found.</returns>
        private static int IndexOfAsciiIgnoreCase(ReadOnlySpan<byte> haystack, ReadOnlySpan<byte> needle)
        {
            if (needle.Length == 0)
            {
                return 0;
            }

            if (needle.Length > haystack.Length)
            {
                return -1;
            }

            int max = haystack.Length - needle.Length;
            for (int i = 0; i <= max; i++)
            {
                bool match = true;
                for (int j = 0; j < needle.Length; j++)
                {
                    if (ToLowerAscii(haystack[i + j]) != ToLowerAscii(needle[j]))
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    return i;
                }
            }

            return -1;
        }

        /// <summary>
        /// Converts ASCII uppercase letters to lowercase.
        /// </summary>
        /// <param name="value">Input byte.</param>
        /// <returns>Lowercased ASCII byte.</returns>
        private static byte ToLowerAscii(byte value)
        {
            return (uint)(value - (byte)'A') <= ('Z' - 'A') ? (byte)(value + 32) : value;
        }

        /// <summary>
        /// Represents intermediate header-parse state and slices.
        /// </summary>
        /// <param name="Success">Indicates whether parsing succeeded.</param>
        /// <param name="FailureCode">Failure code when parse fails.</param>
        /// <param name="ArticleBytes">Original article bytes.</param>
        /// <param name="HeaderBytes">Header bytes slice.</param>
        /// <param name="BodyBytes">Body bytes slice.</param>
        /// <param name="Headers">Parsed headers.</param>
        private readonly record struct HeaderParseOutcome(
            bool Success,
            NntpArticleParseFailureCode FailureCode,
            ReadOnlyMemory<byte> ArticleBytes,
            ReadOnlyMemory<byte> HeaderBytes,
            ReadOnlyMemory<byte> BodyBytes,
            IReadOnlyList<NntpArticleHeaderEntry> Headers)
        {
            /// <summary>
            /// Creates success outcome.
            /// </summary>
            /// <param name="articleBytes">Original article bytes.</param>
            /// <param name="headerBytes">Header bytes slice.</param>
            /// <param name="bodyBytes">Body bytes slice.</param>
            /// <param name="headers">Parsed headers.</param>
            /// <returns>Success outcome.</returns>
            internal static HeaderParseOutcome SuccessResult(
                ReadOnlyMemory<byte> articleBytes,
                ReadOnlyMemory<byte> headerBytes,
                ReadOnlyMemory<byte> bodyBytes,
                IReadOnlyList<NntpArticleHeaderEntry> headers)
            {
                return new HeaderParseOutcome(true, NntpArticleParseFailureCode.None, articleBytes, headerBytes, bodyBytes, headers);
            }

            /// <summary>
            /// Creates failure outcome.
            /// </summary>
            /// <param name="failureCode">Failure code.</param>
            /// <param name="article">Original article bytes.</param>
            /// <param name="headerOffset">Header start offset.</param>
            /// <param name="bodyOffset">Body offset hint.</param>
            /// <param name="headers">Headers parsed before failure.</param>
            /// <returns>Failure outcome.</returns>
            internal static HeaderParseOutcome Fail(
                NntpArticleParseFailureCode failureCode,
                ReadOnlyMemory<byte> article,
                int headerOffset,
                int bodyOffset,
                List<NntpArticleHeaderEntry> headers)
            {
                int boundedBodyOffset = bodyOffset < 0 ? 0 : bodyOffset > article.Length ? article.Length : bodyOffset;
                ReadOnlyMemory<byte> headerBytes = article[headerOffset..boundedBodyOffset];
                ReadOnlyMemory<byte> bodyBytes = article[boundedBodyOffset..];
                return new HeaderParseOutcome(false, failureCode, article, headerBytes, bodyBytes, [.. headers]);
            }
        }
    }
}
