// <copyright file="YEncArticleValidator.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / YEnc
// Byte-oriented yEnc article validator that parses =ybegin/=ypart/=yend sections,
// streams decoded bytes through CRC32, and classifies malformed/corrupt article data
// without payload materialization or exception-driven control flow.

using System.Runtime.CompilerServices;

namespace VectorNNTP.Backfiller.Runtime.Articles.YEnc
{
    /// <summary>
    /// Validates yEnc correctness for raw NNTP article body bytes without materializing decoded payload output.
    /// </summary>
    /// <remarks>
    /// <para>The validator scans for <c>=ybegin</c> sections, optionally handles <c>=ypart</c>, decodes payload bytes
    /// directly into a streaming CRC accumulator, and validates trailer metadata from <c>=yend</c>.</para>
    /// <para>Corrupt or malformed remote article data is reported through <see cref="YEncArticleValidationResult"/>
    /// instead of exceptions so callers can classify failures as yEnc decoding failed and retry alternate backbones.</para>
    /// </remarks>
    internal static class YEncArticleValidator
    {
        /// <summary>
        /// yEnc additive offset applied to decoded bytes during encode/decode transforms.
        /// </summary>
        private const int YEncOffset = 42;

        /// <summary>
        /// Additional offset used when decoding escaped yEnc payload bytes.
        /// </summary>
        private const int YEncEscapedByteDelta = 64;

        /// <summary>
        /// Batch size used for stack-allocated decoded data before each CRC update.
        /// </summary>
        private const int CrcBatchSize = 512;

        /// <summary>
        /// Escape marker introducing two-byte escaped payload sequences.
        /// </summary>
        private const byte EscapeChar = (byte)'=';

        /// <summary>
        /// <c>=ybegin</c> control-line prefix including required trailing space.
        /// </summary>
        private static ReadOnlySpan<byte> YEncBegin => "=ybegin "u8;

        /// <summary>
        /// <c>=ybegin</c> control-line stem used to validate required delimiter spacing.
        /// </summary>
        private static ReadOnlySpan<byte> YEncBeginStem => "=ybegin"u8;

        /// <summary>
        /// <c>=ypart</c> control-line prefix including required trailing space.
        /// </summary>
        private static ReadOnlySpan<byte> YEncPart => "=ypart "u8;

        /// <summary>
        /// <c>=yend</c> control-line prefix including required trailing space.
        /// </summary>
        private static ReadOnlySpan<byte> YEncEnd => "=yend "u8;

        /// <summary>
        /// Multipart trailer CRC key where value represents per-part decoded CRC.
        /// </summary>
        private static ReadOnlySpan<byte> YEncPcrc32KeyWithLeadingSpace => " pcrc32="u8;

        /// <summary>
        /// Trailer CRC key where value represents decoded CRC.
        /// </summary>
        private static ReadOnlySpan<byte> YEncCrc32KeyWithLeadingSpace => " crc32="u8;

        /// <summary>
        /// Trailer/body metadata key identifying declared decoded byte count.
        /// </summary>
        private static ReadOnlySpan<byte> YEncSizeKeyWithLeadingSpace => " size="u8;

        /// <summary>
        /// Multipart metadata key identifying declared 1-based part begin offset.
        /// </summary>
        private static ReadOnlySpan<byte> YEncPartBeginKeyWithLeadingSpace => " begin="u8;

        /// <summary>
        /// Multipart metadata key identifying declared inclusive part end offset.
        /// </summary>
        private static ReadOnlySpan<byte> YEncPartEndKeyWithLeadingSpace => " end="u8;

        /// <summary>
        /// Validates yEnc correctness for a raw NNTP article body.
        /// </summary>
        /// <param name="articleBody">Raw article body bytes.</param>
        /// <returns>Allocation-free validation result with status classification and validated section count.</returns>
        /// <remarks>
        /// <para>The validator performs a single forward scan through section metadata and encoded payload lines.</para>
        /// <para>Decoded bytes are streamed directly into CRC computation without allocating a decoded payload buffer.</para>
        /// </remarks>
        internal static YEncArticleValidationResult Validate(ReadOnlySpan<byte> articleBody)
        {
            int position = 0;
            int sectionsValidated = 0;
            bool sawMultipart = false;

            while (position < articleBody.Length)
            {
                int beginLineStart = ArticleLineScanner.FindLineStartingWith(articleBody, position, YEncBegin);
                int beginStemLineStart = ArticleLineScanner.FindLineStartingWith(articleBody, position, YEncBeginStem);

                if (beginStemLineStart >= 0 && (beginLineStart < 0 || beginStemLineStart <= beginLineStart))
                {
                    int beginStemLineEnd = ArticleLineScanner.IndexOfCrLf(articleBody, beginStemLineStart);
                    int beginStemContentEnd = beginStemLineEnd >= 0 ? beginStemLineEnd : articleBody.Length;
                    ReadOnlySpan<byte> beginStemLine = articleBody[beginStemLineStart..beginStemContentEnd];

                    bool hasRequiredSpaceAfterYBegin = beginStemLine.Length > YEncBeginStem.Length && beginStemLine[YEncBeginStem.Length] == (byte)' ';
                    if (!hasRequiredSpaceAfterYBegin)
                    {
                        return new YEncArticleValidationResult(YEncArticleValidationStatus.InvalidMetadata, sectionsValidated);
                    }
                }

                if (beginLineStart < 0)
                {
                    break;
                }

                int beginLineEnd = ArticleLineScanner.IndexOfCrLf(articleBody, beginLineStart);
                if (beginLineEnd < 0)
                {
                    return new YEncArticleValidationResult(YEncArticleValidationStatus.Truncated, sectionsValidated);
                }

                ReadOnlySpan<byte> beginLine = articleBody[beginLineStart..beginLineEnd];
                if (!TryParseYBeginSize(beginLine, out long yBeginDeclaredSize))
                {
                    return new YEncArticleValidationResult(YEncArticleValidationStatus.InvalidMetadata, sectionsValidated);
                }

                int payloadStart = ArticleLineScanner.AdvancePastLineTerminator(articleBody, beginLineEnd);
                bool isMultipart = false;
                long partBegin = 0;
                long partEnd = 0;

                if (payloadStart < articleBody.Length && articleBody[payloadStart..].StartsWith(YEncPart))
                {
                    isMultipart = true;
                    int partLineEnd = ArticleLineScanner.IndexOfCrLf(articleBody, payloadStart);
                    if (partLineEnd < 0)
                    {
                        return new YEncArticleValidationResult(YEncArticleValidationStatus.Truncated, sectionsValidated);
                    }

                    ReadOnlySpan<byte> partLine = articleBody[payloadStart..partLineEnd];
                    if (!TryParseYPartRange(partLine, out partBegin, out partEnd))
                    {
                        return new YEncArticleValidationResult(YEncArticleValidationStatus.InvalidMetadata, sectionsValidated);
                    }

                    if (partBegin <= 0 || partEnd < partBegin)
                    {
                        return new YEncArticleValidationResult(YEncArticleValidationStatus.InvalidMetadata, sectionsValidated);
                    }

                    if (yBeginDeclaredSize >= 0 && partEnd > yBeginDeclaredSize)
                    {
                        return new YEncArticleValidationResult(YEncArticleValidationStatus.InvalidMetadata, sectionsValidated);
                    }

                    payloadStart = ArticleLineScanner.AdvancePastLineTerminator(articleBody, partLineEnd);
                    sawMultipart = true;
                }

                if (!TryFindAndParseYEncEndLine(articleBody, payloadStart, isMultipart, out EndLineMetadata endMetadata, out YEncArticleValidationStatus endFailureStatus))
                {
                    return new YEncArticleValidationResult(endFailureStatus, sectionsValidated);
                }

                ReadOnlySpan<byte> encodedPayload = articleBody[payloadStart..endMetadata.LineStart];
                YEncArticleValidationStatus decodeStatus = TryComputeDecodedCrc32AndLength(encodedPayload, out uint computedCrc32, out long decodedByteCount);
                if (decodeStatus != YEncArticleValidationStatus.ValidSinglePart)
                {
                    return new YEncArticleValidationResult(decodeStatus, sectionsValidated);
                }

                if (decodedByteCount != endMetadata.DeclaredSize)
                {
                    return new YEncArticleValidationResult(YEncArticleValidationStatus.DecodedSizeMismatch, sectionsValidated);
                }

                if (isMultipart)
                {
                    long expectedPartSize = partEnd - partBegin + 1;
                    if (decodedByteCount != expectedPartSize)
                    {
                        return new YEncArticleValidationResult(YEncArticleValidationStatus.DecodedSizeMismatch, sectionsValidated);
                    }
                }
                else if (yBeginDeclaredSize >= 0 && yBeginDeclaredSize != endMetadata.DeclaredSize)
                {
                    return new YEncArticleValidationResult(YEncArticleValidationStatus.InvalidMetadata, sectionsValidated);
                }

                if (computedCrc32 != endMetadata.DeclaredCrc32)
                {
                    return new YEncArticleValidationResult(YEncArticleValidationStatus.CrcMismatch, sectionsValidated);
                }

                sectionsValidated++;
                position = endMetadata.NextOffset;
            }

            if (sectionsValidated == 0)
            {
                return YEncArticleValidationResult.ValidNonYEnc();
            }

            YEncArticleValidationStatus successStatus = sawMultipart
                ? YEncArticleValidationStatus.ValidMultiPart
                : YEncArticleValidationStatus.ValidSinglePart;

            return new YEncArticleValidationResult(successStatus, sectionsValidated);
        }

        /// <summary>
        /// Parses the declared yEnc total file size from a <c>=ybegin</c> line.
        /// </summary>
        /// <param name="beginLine">Line bytes without line terminator.</param>
        /// <param name="size">Declared size value when parsing succeeds.</param>
        /// <returns><see langword="true"/> when the line has a parseable non-negative <c> size=</c> value.</returns>
        private static bool TryParseYBeginSize(ReadOnlySpan<byte> beginLine, out long size)
        {
            if (!beginLine.StartsWith(YEncBegin))
            {
                size = 0;
                return false;
            }

            return TryParseDecimalValue(beginLine, YEncSizeKeyWithLeadingSpace, out size) && size >= 0;
        }

        /// <summary>
        /// Parses multipart section range metadata from a <c>=ypart</c> line.
        /// </summary>
        /// <param name="partLine">Line bytes without line terminator.</param>
        /// <param name="partBegin">Parsed 1-based section begin offset.</param>
        /// <param name="partEnd">Parsed inclusive section end offset.</param>
        /// <returns><see langword="true"/> when both <c> begin=</c> and <c> end=</c> fields were parsed.</returns>
        private static bool TryParseYPartRange(ReadOnlySpan<byte> partLine, out long partBegin, out long partEnd)
        {
            if (!partLine.StartsWith(YEncPart))
            {
                partBegin = 0;
                partEnd = 0;
                return false;
            }

            if (!TryParseDecimalValue(partLine, YEncPartBeginKeyWithLeadingSpace, out partBegin))
            {
                partEnd = 0;
                return false;
            }

            return TryParseDecimalValue(partLine, YEncPartEndKeyWithLeadingSpace, out partEnd);
        }

        /// <summary>
        /// Finds and parses the next plausible <c>=yend</c> line after <paramref name="startOffset"/>.
        /// </summary>
        /// <param name="body">Article body bytes.</param>
        /// <param name="startOffset">Payload search start offset.</param>
        /// <param name="isMultipart">When <see langword="true"/>, prefer <c>pcrc32=</c> and fallback to <c>crc32=</c>.</param>
        /// <param name="metadata">Parsed end-line metadata.</param>
        /// <param name="failureStatus">Failure status classification when no valid end line is found.</param>
        /// <returns><see langword="true"/> when metadata for a terminal <c>=yend</c> line is available.</returns>
        private static bool TryFindAndParseYEncEndLine(
            ReadOnlySpan<byte> body,
            int startOffset,
            bool isMultipart,
            out EndLineMetadata metadata,
            out YEncArticleValidationStatus failureStatus)
        {
            int searchOffset = startOffset;

            while (searchOffset < body.Length)
            {
                int candidateStart = ArticleLineScanner.FindLineStartingWith(body, searchOffset, YEncEnd);
                if (candidateStart < 0)
                {
                    metadata = default;
                    failureStatus = YEncArticleValidationStatus.Truncated;
                    return false;
                }

                int candidateEnd = ArticleLineScanner.IndexOfCrLf(body, candidateStart);
                int candidateContentEnd = candidateEnd >= 0 ? candidateEnd : body.Length;
                ReadOnlySpan<byte> candidateLine = body[candidateStart..candidateContentEnd];

                if (!IsLikelyYEncMetadataLine(candidateLine))
                {
                    if (candidateEnd < 0)
                    {
                        metadata = default;
                        failureStatus = YEncArticleValidationStatus.Truncated;
                        return false;
                    }

                    searchOffset = ArticleLineScanner.AdvancePastLineTerminator(body, candidateEnd);
                    continue;
                }

                bool hasSizeKey = candidateLine.IndexOf(YEncSizeKeyWithLeadingSpace) >= 0;
                bool hasPreferredCrcKey = candidateLine.IndexOf(isMultipart ? YEncPcrc32KeyWithLeadingSpace : YEncCrc32KeyWithLeadingSpace) >= 0;
                bool hasFallbackCrcKey = isMultipart && candidateLine.IndexOf(YEncCrc32KeyWithLeadingSpace) >= 0;
                bool hasAnyCrcKey = hasPreferredCrcKey || hasFallbackCrcKey;

                if (!hasSizeKey && !hasAnyCrcKey)
                {
                    if (candidateEnd < 0)
                    {
                        metadata = default;
                        failureStatus = YEncArticleValidationStatus.Truncated;
                        return false;
                    }

                    searchOffset = ArticleLineScanner.AdvancePastLineTerminator(body, candidateEnd);
                    continue;
                }

                if (!TryParseDecimalValue(candidateLine, YEncSizeKeyWithLeadingSpace, out long declaredSize) || declaredSize < 0)
                {
                    metadata = default;
                    failureStatus = YEncArticleValidationStatus.InvalidMetadata;
                    return false;
                }

                ReadOnlySpan<byte> crcKey = isMultipart ? YEncPcrc32KeyWithLeadingSpace : YEncCrc32KeyWithLeadingSpace;
                int crcKeyIndex = candidateLine.IndexOf(crcKey);
                if (crcKeyIndex < 0 && isMultipart)
                {
                    crcKey = YEncCrc32KeyWithLeadingSpace;
                    crcKeyIndex = candidateLine.IndexOf(crcKey);
                }

                if (crcKeyIndex < 0)
                {
                    metadata = default;
                    failureStatus = YEncArticleValidationStatus.InvalidMetadata;
                    return false;
                }

                ReadOnlySpan<byte> crcValueBytes = candidateLine[(crcKeyIndex + crcKey.Length)..];
                if (!HexUInt32Parser.TryParseHexUInt32(crcValueBytes, out uint declaredCrc32))
                {
                    metadata = default;
                    failureStatus = YEncArticleValidationStatus.InvalidMetadata;
                    return false;
                }

                int nextOffset = candidateEnd >= 0
                    ? ArticleLineScanner.AdvancePastLineTerminator(body, candidateEnd)
                    : body.Length;

                metadata = new EndLineMetadata(candidateStart, nextOffset, declaredSize, declaredCrc32);
                failureStatus = YEncArticleValidationStatus.ValidSinglePart;
                return true;
            }

            metadata = default;
            failureStatus = YEncArticleValidationStatus.Truncated;
            return false;
        }

        /// <summary>
        /// Decodes yEnc payload bytes and computes CRC and decoded length in a single pass.
        /// </summary>
        /// <remarks>
        /// <para>NNTP dot-stuffed lines are normalized by removing one leading dot when the encoded line starts with <c>..</c>.</para>
        /// <para>Invalid trailing escape markers are classified as <see cref="YEncArticleValidationStatus.InvalidEscapeSequence"/>.</para>
        /// </remarks>
        /// <param name="encodedPayload">Encoded payload bytes between data start and <c>=yend</c>.</param>
        /// <param name="crc32">Computed CRC32 for decoded bytes when decoding succeeds.</param>
        /// <param name="decodedByteCount">Total decoded bytes when decoding succeeds.</param>
        /// <returns>
        /// <see cref="YEncArticleValidationStatus.ValidSinglePart"/> when decoding succeeds;
        /// otherwise a decode-failure status.
        /// </returns>
        private static YEncArticleValidationStatus TryComputeDecodedCrc32AndLength(
            ReadOnlySpan<byte> encodedPayload,
            out uint crc32,
            out long decodedByteCount)
        {
            uint crcAccumulator = 0xFFFFFFFFu;
            Span<byte> decodedBatch = stackalloc byte[CrcBatchSize];
            int batchWriteIndex = 0;
            long decodedCount = 0;
            int lineStart = 0;

            while (lineStart < encodedPayload.Length)
            {
                int lineEnd = ArticleLineScanner.IndexOfCrLf(encodedPayload, lineStart);
                bool isFinalLine = lineEnd < 0;
                int lineContentEnd = isFinalLine ? encodedPayload.Length : lineEnd;
                ReadOnlySpan<byte> line = encodedPayload[lineStart..lineContentEnd];

                if (line.Length >= 2 && line[0] == (byte)'.' && line[1] == (byte)'.')
                {
                    line = line[1..];
                }

                for (int i = 0; i < line.Length; i++)
                {
                    byte current = line[i];
                    byte decoded;

                    if (current == EscapeChar)
                    {
                        if (i + 1 >= line.Length)
                        {
                            crc32 = 0;
                            decodedByteCount = 0;
                            return YEncArticleValidationStatus.InvalidEscapeSequence;
                        }

                        decoded = unchecked((byte)(line[i + 1] - YEncOffset - YEncEscapedByteDelta));
                        i++;
                    }
                    else
                    {
                        decoded = unchecked((byte)(current - YEncOffset));
                    }

                    decodedBatch[batchWriteIndex++] = decoded;
                    decodedCount++;

                    if (batchWriteIndex == CrcBatchSize)
                    {
                        crcAccumulator = UpdateCrc32(crcAccumulator, decodedBatch);
                        batchWriteIndex = 0;
                    }
                }

                lineStart = isFinalLine
                    ? encodedPayload.Length
                    : ArticleLineScanner.AdvancePastLineTerminator(encodedPayload, lineEnd);
            }

            if (batchWriteIndex > 0)
            {
                crcAccumulator = UpdateCrc32(crcAccumulator, decodedBatch[..batchWriteIndex]);
            }

            crc32 = crcAccumulator ^ 0xFFFFFFFFu;
            decodedByteCount = decodedCount;
            return YEncArticleValidationStatus.ValidSinglePart;
        }

        /// <summary>
        /// Determines whether a candidate <c>=yend</c> line looks like structured ASCII metadata rather than binary payload bytes.
        /// </summary>
        /// <param name="line">Candidate line bytes without line terminator.</param>
        /// <returns><see langword="true"/> when the line is composed of printable ASCII metadata tokens.</returns>
        private static bool IsLikelyYEncMetadataLine(ReadOnlySpan<byte> line)
        {
            if (!line.StartsWith(YEncEnd) || line.Length < YEncEnd.Length + 8)
            {
                return false;
            }

            for (int i = 0; i < line.Length; i++)
            {
                byte b = line[i];
                if (b is < 0x20 or > 0x7E)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Parses an unsigned decimal value for a metadata key in a yEnc control line.
        /// </summary>
        /// <param name="line">Line bytes without line terminator.</param>
        /// <param name="key">Metadata key including its leading space.</param>
        /// <param name="value">Parsed decimal value when successful.</param>
        /// <returns><see langword="true"/> when at least one decimal digit was parsed without overflow.</returns>
        private static bool TryParseDecimalValue(ReadOnlySpan<byte> line, ReadOnlySpan<byte> key, out long value)
        {
            value = 0;

            int keyIndex = line.IndexOf(key);
            if (keyIndex < 0)
            {
                return false;
            }

            int digitIndex = keyIndex + key.Length;
            bool hasDigits = false;

            for (int i = digitIndex; i < line.Length; i++)
            {
                int digit = line[i] - (byte)'0';
                if ((uint)digit > 9)
                {
                    break;
                }

                if (value > ((long.MaxValue - digit) / 10))
                {
                    return false;
                }

                value = (value * 10) + digit;
                hasDigits = true;
            }

            return hasDigits;
        }

        /// <summary>
        /// Updates a CRC-32 accumulator with additional decoded bytes.
        /// </summary>
        /// <param name="crc">Current CRC accumulator value.</param>
        /// <param name="data">Data bytes to append.</param>
        /// <returns>Updated CRC accumulator value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint UpdateCrc32(uint crc, ReadOnlySpan<byte> data)
        {
            ReadOnlySpan<uint> table = Crc32Table;

            for (int i = 0; i < data.Length; i++)
            {
                crc = (crc >> 8) ^ table[(int)((crc ^ data[i]) & 0xFF)];
            }

            return crc;
        }

        /// <summary>
        /// Builds the static CRC-32 lookup table for polynomial 0xEDB88320.
        /// </summary>
        /// <returns>Initialized 256-entry CRC lookup table.</returns>
        private static uint[] CreateCrc32Table()
        {
            uint[] table = new uint[256];

            for (uint i = 0; i < table.Length; i++)
            {
                uint value = i;
                for (int bit = 0; bit < 8; bit++)
                {
                    value = (value & 1) == 0
                        ? value >> 1
                        : (value >> 1) ^ 0xEDB88320u;
                }

                table[i] = value;
            }

            return table;
        }

        /// <summary>
        /// Immutable parsed metadata for a selected yEnc end line.
        /// </summary>
        /// <remarks>Encapsulates the chosen terminal control-line location and parsed validation fields.</remarks>
        /// <param name="LineStart">Start offset of the end line.</param>
        /// <param name="NextOffset">Offset of first byte after the end line terminator.</param>
        /// <param name="DeclaredSize">Declared decoded size from <c>size=</c>.</param>
        /// <param name="DeclaredCrc32">Declared decoded CRC from <c>crc32=</c> or <c>pcrc32=</c>.</param>
        private readonly record struct EndLineMetadata(
            int LineStart,
            int NextOffset,
            long DeclaredSize,
            uint DeclaredCrc32);

        /// <summary>
        /// Shared CRC-32 lookup table used by <see cref="UpdateCrc32(uint, ReadOnlySpan{byte})"/>.
        /// </summary>
        private static readonly uint[] Crc32Table = CreateCrc32Table();
    }
}
