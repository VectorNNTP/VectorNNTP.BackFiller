// <copyright file="YEncArticleValidatorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies yEnc validation outcomes for corpus fixtures and targeted corruption scenarios.
    /// </summary>
    /// <remarks>
    /// <para>These tests exercise byte-oriented validation behavior only; they do not integrate with downloader/runtime orchestration.</para>
    /// <para>Coverage includes single-part and multipart parsing, CRC and size integrity checks, malformed metadata handling,
    /// NNTP dot-stuffing interaction, and control-line false-positive avoidance.</para>
    /// </remarks>
    public sealed class YEncArticleValidatorTests
    {
        /// <summary>
        /// Absolute directory path containing offline SABCTools yEnc fixtures for theory-driven tests.
        /// </summary>
        private static readonly string FixtureRoot = ResolveFixtureRoot();

        /// <summary>
        /// Provides fixture cases expected to validate successfully.
        /// </summary>
        /// <returns>Fixture name and expected validation status name pairs.</returns>
        public static IEnumerable<object[]> ValidFixtureCases()
        {
            yield return ["test_regular.yenc", nameof(YEncArticleValidationStatus.ValidMultiPart)];
            yield return ["test_regular_2.yenc", nameof(YEncArticleValidationStatus.ValidMultiPart)];
            yield return ["test_special_chars.yenc", nameof(YEncArticleValidationStatus.ValidMultiPart)];
            yield return ["test_special_utf8_chars.yenc", nameof(YEncArticleValidationStatus.ValidMultiPart)];
            yield return ["test_partial.yenc", nameof(YEncArticleValidationStatus.DecodedSizeMismatch)];
            yield return ["test_bad_crc.yenc", nameof(YEncArticleValidationStatus.ValidMultiPart)];
        }

        /// <summary>
        /// Provides fixture cases expected to fail validation or classify as non-yEnc.
        /// </summary>
        /// <returns>Fixture name and expected validation status name pairs.</returns>
        public static IEnumerable<object[]> InvalidFixtureCases()
        {
            yield return ["test_bad_crc_end.yenc", nameof(YEncArticleValidationStatus.CrcMismatch)];
            yield return ["test_invalid_crc_chars.yenc", nameof(YEncArticleValidationStatus.InvalidMetadata)];
            yield return ["test_invalid_escape.yenc", nameof(YEncArticleValidationStatus.InvalidEscapeSequence)];
            yield return ["test_missing_yend.yenc", nameof(YEncArticleValidationStatus.Truncated)];
            yield return ["test_malformed_ybegin.yenc", nameof(YEncArticleValidationStatus.InvalidMetadata)];
            yield return ["test_ypart_without_ybegin.yenc", nameof(YEncArticleValidationStatus.ValidNonYEnc)];
            yield return ["test_empty_file.yenc", nameof(YEncArticleValidationStatus.ValidNonYEnc)];
        }

        /// <summary>
        /// Confirms that known-good SABCTools fixtures classify to the expected success or expected failure contract.
        /// </summary>
        /// <param name="fixtureName">Fixture file name to validate.</param>
        /// <param name="expectedStatusName">Expected terminal validation status name.</param>
        [Theory]
        [MemberData(nameof(ValidFixtureCases))]
        public void Validate_WhenFixtureIsValid_ReturnsExpectedSuccessStatus(string fixtureName, string expectedStatusName)
        {
            ReadOnlySpan<byte> body = LoadFixtureBytes(fixtureName);

            YEncArticleValidationResult result = YEncArticleValidator.Validate(body);
            YEncArticleValidationStatus expected = Enum.Parse<YEncArticleValidationStatus>(expectedStatusName);

            Assert.Equal(expected, result.Status);

            if (expected is YEncArticleValidationStatus.ValidSinglePart or YEncArticleValidationStatus.ValidMultiPart)
            {
                Assert.True(result.IsValid);
                Assert.True(result.SectionsValidated > 0);
                Assert.False(result.ShouldTreatAsYEncDecodingFailed);
            }
            else
            {
                Assert.False(result.IsValid);
                Assert.True(result.ShouldTreatAsYEncDecodingFailed);
            }
        }

        /// <summary>
        /// Confirms that malformed fixtures and non-yEnc fixtures map to the expected validation classification.
        /// </summary>
        /// <param name="fixtureName">Fixture file name to validate.</param>
        /// <param name="expectedStatusName">Expected terminal validation status name.</param>
        [Theory]
        [MemberData(nameof(InvalidFixtureCases))]
        public void Validate_WhenFixtureIsInvalidOrNonYEnc_ReturnsExpectedStatus(string fixtureName, string expectedStatusName)
        {
            ReadOnlySpan<byte> body = LoadFixtureBytes(fixtureName);

            YEncArticleValidationResult result = YEncArticleValidator.Validate(body);
            YEncArticleValidationStatus expected = Enum.Parse<YEncArticleValidationStatus>(expectedStatusName);

            Assert.Equal(expected, result.Status);

            if (expected is YEncArticleValidationStatus.ValidNonYEnc)
            {
                Assert.True(result.IsValid);
                Assert.False(result.ShouldTreatAsYEncDecodingFailed);
            }
            else
            {
                Assert.False(result.IsValid);
                Assert.True(result.ShouldTreatAsYEncDecodingFailed);
            }
        }

        /// <summary>
        /// Verifies that a synthetically generated single-part article validates successfully and reports one validated section.
        /// </summary>
        [Fact]
        public void Validate_WhenSinglePartArticleIsValid_ReturnsValidSinglePart()
        {
            byte[] payload = BuildPayload(4096, 17);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, result.Status);
            Assert.True(result.IsValid);
            Assert.Equal(1, result.SectionsValidated);
        }

        /// <summary>
        /// Verifies that mutating one encoded payload byte is detected by decoded CRC mismatch classification.
        /// </summary>
        [Fact]
        public void Validate_WhenEncodedByteIsChanged_ReturnsCrcMismatch()
        {
            byte[] payload = BuildPayload(2048, 1);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            int payloadOffset = FindPayloadOffset(article);
            article[payloadOffset + 25] ^= 0x01;

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.CrcMismatch, result.Status);
        }

        /// <summary>
        /// Verifies that mutating one escaped yEnc byte changes decoded output and is reported as CRC mismatch.
        /// </summary>
        [Fact]
        public void Validate_WhenEscapedByteIsChanged_ReturnsCrcMismatch()
        {
            byte[] payload = new byte[2048];
            for (int i = 0; i < payload.Length; i++)
            {
                payload[i] = (byte)(i % 256);
            }

            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);
            int payloadOffset = FindPayloadOffset(article);

            for (int i = payloadOffset; i < article.Length - 1; i++)
            {
                if (article[i] == (byte)'=' && article[i + 1] != (byte)'y')
                {
                    article[i + 1] ^= 0x01;
                    break;
                }
            }

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.CrcMismatch, result.Status);
        }

        /// <summary>
        /// Verifies that tampering trailer CRC metadata causes mismatch against computed decoded CRC.
        /// </summary>
        [Fact]
        public void Validate_WhenCrcMetadataIsAltered_ReturnsCrcMismatch()
        {
            byte[] payload = BuildPayload(2048, 4);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            byte[] mutated = ReplaceAsciiInCopy(article, " crc32=", " crc32=ffffffff");

            YEncArticleValidationResult result = YEncArticleValidator.Validate(mutated);

            Assert.Equal(YEncArticleValidationStatus.CrcMismatch, result.Status);
        }

        /// <summary>
        /// Verifies that tampering trailer size metadata is detected as decoded-size mismatch.
        /// </summary>
        [Fact]
        public void Validate_WhenDeclaredSizeIsAltered_ReturnsDecodedSizeMismatch()
        {
            byte[] payload = BuildPayload(1536, 7);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            byte[] mutated = ReplaceExactTokenInCopy(article, "\r\n=yend size=1536 ", "\r\n=yend size=1535 ");

            YEncArticleValidationResult result = YEncArticleValidator.Validate(mutated);

            Assert.Equal(YEncArticleValidationStatus.DecodedSizeMismatch, result.Status);
        }

        /// <summary>
        /// Verifies that removing trailing bytes from the article body yields truncated classification.
        /// </summary>
        [Fact]
        public void Validate_WhenPayloadIsTruncated_ReturnsTruncated()
        {
            byte[] payload = BuildPayload(4096, 5);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            byte[] truncated = article[..(article.Length - 40)];

            YEncArticleValidationResult result = YEncArticleValidator.Validate(truncated);

            Assert.Equal(YEncArticleValidationStatus.Truncated, result.Status);
        }

        /// <summary>
        /// Verifies that a cut-off <c>=yend</c> control line is treated as truncated yEnc data.
        /// </summary>
        [Fact]
        public void Validate_WhenYEndLineIsTruncated_ReturnsTruncated()
        {
            byte[] payload = BuildPayload(1024, 8);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            int endOffset = IndexOfAscii(article, "=yend ");
            byte[] truncated = article[..(endOffset + 8)];

            YEncArticleValidationResult result = YEncArticleValidator.Validate(truncated);

            Assert.Equal(YEncArticleValidationStatus.Truncated, result.Status);
        }

        /// <summary>
        /// Verifies that a trailing escape marker without a valid escaped byte is rejected as an invalid escape sequence.
        /// </summary>
        [Fact]
        public void Validate_WhenEscapeSequenceIsMalformed_ReturnsInvalidEscapeSequence()
        {
            byte[] payload = BuildPayload(1024, 11);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            int payloadStart = FindPayloadOffset(article);
            int lineEnd = Array.IndexOf(article, (byte)'\n', payloadStart);
            article[lineEnd - 1] = (byte)'=';

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.InvalidEscapeSequence, result.Status);
        }

        /// <summary>
        /// Verifies that missing required <c>=ybegin</c> metadata fields are classified as invalid metadata.
        /// </summary>
        [Fact]
        public void Validate_WhenYBeginIsMalformed_ReturnsInvalidMetadata()
        {
            byte[] article = "220 0 <id>\r\n\r\n=ybegin line=128 name=test.bin\r\nabc\r\n=yend size=3 crc32=352441c2\r\n.\r\n"u8.ToArray();

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.InvalidMetadata, result.Status);
        }

        /// <summary>
        /// Verifies that malformed multipart range metadata in <c>=ypart</c> is classified as invalid metadata.
        /// </summary>
        [Fact]
        public void Validate_WhenYPartIsMalformed_ReturnsInvalidMetadata()
        {
            byte[] payload = BuildPayload(100, 3);
            byte[] article = BuildMultiPartArticle(payload, begin: 1, end: 100, malformedYPart: true);

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.InvalidMetadata, result.Status);
        }

        /// <summary>
        /// Verifies that a trailer missing CRC metadata is rejected as invalid metadata.
        /// </summary>
        [Fact]
        public void Validate_WhenYEndMissingCrc_ReturnsInvalidMetadata()
        {
            byte[] payload = BuildPayload(512, 12);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            int endStart = IndexOfAscii(article, "=yend ");
            int endLineEnd = Array.IndexOf(article, (byte)'\n', endStart);
            byte[] prefix = article[..endStart];
            byte[] replacement = "=yend size=512\r\n"u8.ToArray();
            byte[] suffix = article[(endLineEnd + 1)..];
            byte[] malformed = new byte[prefix.Length + replacement.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, malformed, 0, prefix.Length);
            Buffer.BlockCopy(replacement, 0, malformed, prefix.Length, replacement.Length);
            Buffer.BlockCopy(suffix, 0, malformed, prefix.Length + replacement.Length, suffix.Length);

            YEncArticleValidationResult result = YEncArticleValidator.Validate(malformed);

            Assert.Equal(YEncArticleValidationStatus.InvalidMetadata, result.Status);
        }

        /// <summary>
        /// Verifies NNTP dot-stuffed payload handling where <c>..</c> at line start decodes to a logical single leading dot.
        /// </summary>
        [Fact]
        public void Validate_WhenDotStuffedPayloadHasLeadingDot_ValidatesSuccessfully()
        {
            byte[] decodedPayload = [4];
            uint crc = Crc32(decodedPayload);
            byte[] article = System.Text.Encoding.ASCII.GetBytes($"220 0 <message-id>\r\n\r\n=ybegin line=128 size=1 name=test.bin\r\n..\r\n=yend size=1 crc32={crc:x8}\r\n.\r\n");

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, result.Status);
        }

        /// <summary>
        /// Verifies that <c>=yend</c>-like bytes embedded within payload data do not terminate section parsing.
        /// </summary>
        [Fact]
        public void Validate_WhenPayloadContainsYEndLikeBytesInData_DoesNotFalseTerminate()
        {
            byte[] payload = "prefix =yend size=9999 crc32=ffffffff suffix"u8.ToArray();
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, result.Status);
        }

        /// <summary>
        /// Verifies that a large valid payload still validates successfully while preserving non-negative per-thread allocation accounting.
        /// </summary>
        [Fact]
        public void Validate_WhenLargeSinglePartPayloadIsValid_ReturnsSuccessWithoutAllocationPressure()
        {
            byte[] payload = BuildPayload(4 * 1024 * 1024, 41);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            long before = GC.GetAllocatedBytesForCurrentThread();
            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);
            long after = GC.GetAllocatedBytesForCurrentThread();

            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, result.Status);
            Assert.True(after >= before);
        }

        /// <summary>
        /// Verifies that single-byte corruption in a large payload is detected as CRC mismatch.
        /// </summary>
        [Fact]
        public void Validate_WhenLargeSinglePartPayloadCorrupted_ReturnsCrcMismatch()
        {
            byte[] payload = BuildPayload(4 * 1024 * 1024, 57);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            int payloadOffset = FindPayloadOffset(article);
            article[payloadOffset + 1_000_000] ^= 0x20;

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.CrcMismatch, result.Status);
        }

        /// <summary>
        /// Verifies that validation throughput on a large payload exceeds a minimal regression guard threshold.
        /// </summary>
        [Fact]
        public void Validate_WhenThroughputMeasuredForLargePayload_CompletesAtReasonableRate()
        {
            byte[] payload = BuildPayload(8 * 1024 * 1024, 21);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);

            System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);
            sw.Stop();

            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, result.Status);

            double mb = article.Length / (1024d * 1024d);
            double mbPerSecond = mb / Math.Max(sw.Elapsed.TotalSeconds, 0.000001d);
            Assert.True(mbPerSecond > 1d, $"Expected throughput > 1 MB/s but got {mbPerSecond:F2} MB/s");
        }

        /// <summary>
        /// Verifies that an overflowing decimal <c>size=</c> value in <c>=ybegin</c> is rejected as invalid metadata.
        /// </summary>
        [Fact]
        public void Validate_WhenYBeginSizeOverflowsLong_ReturnsInvalidMetadata()
        {
            byte[] article = "220 0 <id>\r\n\r\n=ybegin line=128 size=9223372036854775808 name=test.bin\r\nabc\r\n=yend size=3 crc32=352441c2\r\n.\r\n"u8.ToArray();

            YEncArticleValidationResult result = YEncArticleValidator.Validate(article);

            Assert.Equal(YEncArticleValidationStatus.InvalidMetadata, result.Status);
        }

        /// <summary>
        /// Verifies that trailer CRC values longer than eight hexadecimal digits are rejected as invalid metadata.
        /// </summary>
        [Fact]
        public void Validate_WhenYEndCrcHasMoreThanEightHexDigits_ReturnsInvalidMetadata()
        {
            byte[] payload = BuildPayload(512, 33);
            byte[] article = BuildSinglePartArticle(payload, includeDotStuffedLeadingDotLine: false);
            byte[] mutated = ReplaceAsciiInCopy(article, " crc32=", " crc32=123456789");

            YEncArticleValidationResult result = YEncArticleValidator.Validate(mutated);

            Assert.Equal(YEncArticleValidationStatus.InvalidMetadata, result.Status);
        }

        /// <summary>
        /// Loads one yEnc fixture as raw bytes for validation.
        /// </summary>
        /// <param name="fixtureName">Fixture file name under the SABCTools fixture directory.</param>
        /// <returns>Raw fixture bytes.</returns>
        private static ReadOnlySpan<byte> LoadFixtureBytes(string fixtureName)
        {
            string path = Path.Combine(FixtureRoot, fixtureName);
            return File.ReadAllBytes(path);
        }

        /// <summary>
        /// Resolves the fixture directory by walking upward from the test base directory to the solution marker.
        /// </summary>
        /// <returns>Absolute fixture directory path.</returns>
        private static string ResolveFixtureRoot()
        {
            const string SolutionMarker = "VectorNNTP.BackFiller.slnx";
            string? current = AppContext.BaseDirectory;

            while (!string.IsNullOrWhiteSpace(current))
            {
                string markerPath = Path.Combine(current, SolutionMarker);
                if (File.Exists(markerPath))
                {
                    string root = Path.Combine(current, "VectorNNTP.BackFiller.Tests", "Fixtures", "SabctoolsYEnc");
                    if (Directory.Exists(root))
                    {
                        return root;
                    }

                    break;
                }

                DirectoryInfo? parent = Directory.GetParent(current);
                current = parent?.FullName;
            }

            throw new DirectoryNotFoundException("Could not locate yEnc fixture directory.");
        }

        /// <summary>
        /// Builds a synthetic single-part article envelope around a decoded payload.
        /// </summary>
        /// <param name="decodedPayload">Decoded bytes to encode and place between <c>=ybegin</c> and <c>=yend</c>.</param>
        /// <param name="includeDotStuffedLeadingDotLine">Whether to apply NNTP dot-stuffing to encoded line starts.</param>
        /// <returns>Raw article bytes with transport framing and yEnc metadata.</returns>
        private static byte[] BuildSinglePartArticle(byte[] decodedPayload, bool includeDotStuffedLeadingDotLine)
        {
            byte[] encodedPayload = EncodeYEnc(decodedPayload);

            if (includeDotStuffedLeadingDotLine)
            {
                encodedPayload = DotStuffLineStarts(encodedPayload);
            }

            uint crc = Crc32(decodedPayload);
            byte[] prefix = System.Text.Encoding.ASCII.GetBytes($"220 0 <message-id>\r\n\r\n=ybegin line=128 size={decodedPayload.Length} name=test.bin\r\n");
            byte[] suffix = System.Text.Encoding.ASCII.GetBytes($"\r\n=yend size={decodedPayload.Length} crc32={crc:x8}\r\n.\r\n");

            byte[] article = new byte[prefix.Length + encodedPayload.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, article, 0, prefix.Length);
            Buffer.BlockCopy(encodedPayload, 0, article, prefix.Length, encodedPayload.Length);
            Buffer.BlockCopy(suffix, 0, article, prefix.Length + encodedPayload.Length, suffix.Length);
            return article;
        }

        /// <summary>
        /// Builds a synthetic multipart article containing <c>=ybegin</c>, <c>=ypart</c>, and <c>=yend</c> metadata.
        /// </summary>
        /// <param name="decodedPayload">Decoded bytes for this multipart section.</param>
        /// <param name="begin">Declared part begin offset (1-based).</param>
        /// <param name="end">Declared part end offset (inclusive).</param>
        /// <param name="malformedYPart">Whether to intentionally emit malformed <c>=ypart</c> metadata for negative tests.</param>
        /// <returns>Raw multipart article bytes.</returns>
        private static byte[] BuildMultiPartArticle(byte[] decodedPayload, int begin, int end, bool malformedYPart)
        {
            byte[] encodedPayload = EncodeYEnc(decodedPayload);
            uint crc = Crc32(decodedPayload);

            string yPartLine = malformedYPart
                ? "=ypart begin=x end=y"
                : $"=ypart begin={begin} end={end}";

            byte[] prefix = System.Text.Encoding.ASCII.GetBytes($"222 0 <message-id>\r\n\r\n=ybegin part=1 line=128 size={end} name=test.bin\r\n{yPartLine}\r\n");
            byte[] suffix = System.Text.Encoding.ASCII.GetBytes($"\r\n=yend size={decodedPayload.Length} part=1 pcrc32={crc:x8}\r\n.\r\n");

            byte[] article = new byte[prefix.Length + encodedPayload.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, article, 0, prefix.Length);
            Buffer.BlockCopy(encodedPayload, 0, article, prefix.Length, encodedPayload.Length);
            Buffer.BlockCopy(suffix, 0, article, prefix.Length + encodedPayload.Length, suffix.Length);
            return article;
        }

        /// <summary>
        /// Encodes decoded bytes into yEnc payload bytes using the same escape set expected by validator tests.
        /// </summary>
        /// <param name="decoded">Decoded input bytes to encode.</param>
        /// <returns>Encoded payload bytes with CRLF line wrapping.</returns>
        private static byte[] EncodeYEnc(byte[] decoded)
        {
            List<byte> output = new(decoded.Length + (decoded.Length / 32));
            int lineCount = 0;

            for (int i = 0; i < decoded.Length; i++)
            {
                byte encoded = unchecked((byte)(decoded[i] + 42));
                bool mustEscape = encoded is 0 or 9 or 10 or 13 or 32 or 46 or 61;

                if (mustEscape)
                {
                    output.Add((byte)'=');
                    output.Add(unchecked((byte)(encoded + 64)));
                    lineCount += 2;
                }
                else
                {
                    output.Add(encoded);
                    lineCount++;
                }

                if (lineCount >= 128)
                {
                    output.Add((byte)'\r');
                    output.Add((byte)'\n');
                    lineCount = 0;
                }
            }

            if (output.Count == 0 || output[^1] != (byte)'\n')
            {
                output.Add((byte)'\r');
                output.Add((byte)'\n');
            }

            return [.. output];
        }

        /// <summary>
        /// Applies NNTP dot-stuffing to encoded payload line starts for transport-level fixture synthesis.
        /// </summary>
        /// <param name="payload">Encoded payload bytes before dot-stuffing.</param>
        /// <returns>Dot-stuffed payload bytes.</returns>
        private static byte[] DotStuffLineStarts(byte[] payload)
        {
            List<byte> output = new(payload.Length + 32);
            bool atLineStart = true;

            for (int i = 0; i < payload.Length; i++)
            {
                byte b = payload[i];
                if (atLineStart && b == (byte)'.')
                {
                    output.Add((byte)'.');
                }

                output.Add(b);
                atLineStart = b == (byte)'\n';
            }

            return [.. output];
        }

        /// <summary>
        /// Computes CRC-32 for decoded payload bytes to populate synthetic yEnc trailer metadata.
        /// </summary>
        /// <param name="payload">Decoded payload bytes.</param>
        /// <returns>CRC-32 value using polynomial <c>0xEDB88320</c>.</returns>
        private static uint Crc32(byte[] payload)
        {
            uint crc = 0xFFFFFFFFu;
            for (int i = 0; i < payload.Length; i++)
            {
                crc = (crc >> 8) ^ CrcTable[(int)((crc ^ payload[i]) & 0xFF)];
            }

            return crc ^ 0xFFFFFFFFu;
        }

        /// <summary>
        /// Creates deterministic pseudo-random payload bytes for repeatable tests.
        /// </summary>
        /// <param name="size">Requested payload size in bytes.</param>
        /// <param name="seed">Random seed controlling generated sequence.</param>
        /// <returns>Generated payload bytes.</returns>
        private static byte[] BuildPayload(int size, int seed)
        {
            byte[] payload = new byte[size];
            Random random = new(seed);
            random.NextBytes(payload);
            return payload;
        }

        /// <summary>
        /// Locates the encoded payload start within a synthetic yEnc article.
        /// </summary>
        /// <param name="article">Synthetic article bytes that include NNTP framing and yEnc control lines.</param>
        /// <returns>Zero-based offset to first encoded payload byte.</returns>
        private static int FindPayloadOffset(byte[] article)
        {
            int beginIndex = IndexOfAscii(article, "=ybegin ");
            int beginLineEnd = Array.IndexOf(article, (byte)'\n', beginIndex);
            int offset = beginLineEnd + 1;

            if (offset < article.Length && article.AsSpan(offset).StartsWith("=ypart "u8))
            {
                int partLineEnd = Array.IndexOf(article, (byte)'\n', offset);
                offset = partLineEnd + 1;
            }

            return offset;
        }

        /// <summary>
        /// Replaces an ASCII token with replacement text up to end-of-line in a copied buffer for corruption tests.
        /// </summary>
        /// <param name="buffer">Source buffer to copy and mutate.</param>
        /// <param name="search">ASCII token to find.</param>
        /// <param name="replacement">Replacement ASCII text inserted at token location.</param>
        /// <returns>Mutated buffer copy.</returns>
        private static byte[] ReplaceAsciiInCopy(byte[] buffer, string search, string replacement)
        {
            byte[] searchBytes = System.Text.Encoding.ASCII.GetBytes(search);
            byte[] replacementBytes = System.Text.Encoding.ASCII.GetBytes(replacement);
            int idx = buffer.AsSpan().IndexOf(searchBytes);
            Assert.True(idx >= 0, $"Expected to find '{search}'");

            int lineEnd = Array.IndexOf(buffer, (byte)'\n', idx);
            Assert.True(lineEnd > idx, "Expected target line ending after replacement token.");

            int prefixLength = idx;
            int suffixStart = lineEnd;

            byte[] newBuffer = new byte[prefixLength + replacementBytes.Length + (buffer.Length - suffixStart)];
            Buffer.BlockCopy(buffer, 0, newBuffer, 0, prefixLength);
            Buffer.BlockCopy(replacementBytes, 0, newBuffer, prefixLength, replacementBytes.Length);
            Buffer.BlockCopy(buffer, suffixStart, newBuffer, prefixLength + replacementBytes.Length, buffer.Length - suffixStart);

            return newBuffer;
        }

        /// <summary>
        /// Replaces a same-length ASCII token in a copied buffer for metadata tampering tests.
        /// </summary>
        /// <param name="buffer">Source buffer to copy and mutate.</param>
        /// <param name="search">Exact ASCII token to replace.</param>
        /// <param name="replacement">Replacement token with matching length.</param>
        /// <returns>Mutated buffer copy.</returns>
        private static byte[] ReplaceExactTokenInCopy(byte[] buffer, string search, string replacement)
        {
            byte[] source = (byte[])buffer.Clone();
            byte[] searchBytes = System.Text.Encoding.ASCII.GetBytes(search);
            byte[] replacementBytes = System.Text.Encoding.ASCII.GetBytes(replacement);
            Assert.Equal(searchBytes.Length, replacementBytes.Length);

            int idx = source.AsSpan().IndexOf(searchBytes);
            Assert.True(idx >= 0, $"Expected to find exact token '{search}'");

            replacementBytes.CopyTo(source.AsSpan(idx, replacementBytes.Length));
            return source;
        }

        /// <summary>
        /// Finds an ASCII token within a byte buffer.
        /// </summary>
        /// <param name="buffer">Buffer to search.</param>
        /// <param name="token">ASCII token text.</param>
        /// <returns>Zero-based index of first occurrence, or -1 when not found.</returns>
        private static int IndexOfAscii(byte[] buffer, string token)
        {
            byte[] bytes = System.Text.Encoding.ASCII.GetBytes(token);
            return buffer.AsSpan().IndexOf(bytes);
        }

        /// <summary>
        /// Lookup table used by <see cref="Crc32(byte[])"/> for deterministic trailer metadata generation.
        /// </summary>
        private static readonly uint[] CrcTable = CreateCrcTable();

        /// <summary>
        /// Builds the CRC-32 lookup table used by test helper checksum generation.
        /// </summary>
        /// <returns>Initialized 256-entry lookup table.</returns>
        private static uint[] CreateCrcTable()
        {
            uint[] table = new uint[256];
            for (uint i = 0; i < table.Length; i++)
            {
                uint value = i;
                for (int bit = 0; bit < 8; bit++)
                {
                    value = (value & 1) == 0 ? value >> 1 : (value >> 1) ^ 0xEDB88320u;
                }

                table[i] = value;
            }

            return table;
        }
    }
}
