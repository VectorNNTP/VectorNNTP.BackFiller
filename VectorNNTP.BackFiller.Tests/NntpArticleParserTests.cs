// <copyright file="NntpArticleParserTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / Articles / Parsing
// Contract tests for the high-performance NNTP article parser covering classification,
// hostile input handling, date/path normalization, and yEnc validation integration.

using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.DateParser;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies deterministic parser behavior for valid, malformed, and hostile NNTP article inputs.
    /// </summary>
    public sealed class NntpArticleParserTests
    {
        /// <summary>
        /// Canonical local identity used for Path augmentation assertions.
        /// </summary>
        private const string LocalFqdn = "bf01.usenet.ninja";

        /// <summary>
        /// Root directory containing SABCTools yEnc fixtures used for parser yEnc validation tests.
        /// </summary>
        private static readonly string FixtureRoot = ResolveFixtureRoot();

        /// <summary>
        /// Verifies basic text article parsing and canonical metadata extraction.
        /// </summary>
        [Fact]
        public void Parse_WhenValidTextArticle_ReturnsAcceptedTextResult()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0200",
                    "Message-ID: <m1@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Subject: hello",
                ],
                body: "This is text.\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
            Assert.Equal(NntpArticleType.Text, result.ArticleType);
            Assert.Equal("Fri, 23 Aug 2024 05:30:10 +0000", result.CanonicalUtcDate);
            Assert.Equal(LocalFqdn, result.CanonicalPath);
            Assert.False(result.YEncDetected);
            Assert.Equal(YEncArticleValidationStatus.ValidNonYEnc, result.YEncValidation.Status);
        }

        /// <summary>
        /// Verifies MIME multipart classification from Content-Type hint.
        /// </summary>
        [Fact]
        public void Parse_WhenMultipartContentTypePresent_ClassifiesMimeMultipart()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m2@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Content-Type: multipart/mixed; boundary=abc",
                ],
                body: "--abc\r\npart\r\n--abc--\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
            Assert.Equal(NntpArticleType.MimeMultipart, result.ArticleType);
        }

        /// <summary>
        /// Verifies binary/encoded classification from transfer-encoding hint.
        /// </summary>
        [Fact]
        public void Parse_WhenBase64EncodingPresent_ClassifiesBinaryEncoded()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m3@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Content-Transfer-Encoding: base64",
                ],
                body: "QUJDREVGRw==\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
            Assert.Equal(NntpArticleType.BinaryEncoded, result.ArticleType);
        }

        /// <summary>
        /// Verifies date parse failures are rejected with explicit date failure classification.
        /// </summary>
        [Fact]
        public void Parse_WhenDateInvalid_RejectsWithDateFailureReason()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: INVALID-DATE",
                    "Message-ID: <m4@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                body: "body\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.False(result.IsAccepted);
            Assert.Equal(NntpArticleParseFailureCode.MissingOrInvalidDate, result.FailureCode);
            Assert.Equal(DateParseFailureReason.ParseFailed, result.DateFailureReason);
        }

        /// <summary>
        /// Verifies duplicate Message-ID headers are rejected deterministically.
        /// </summary>
        [Fact]
        public void Parse_WhenMessageIdDuplicated_Rejects()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m5@example.test>",
                    "Message-ID: <m6@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                body: "body\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.False(result.IsAccepted);
            Assert.Equal(NntpArticleParseFailureCode.DuplicateMessageId, result.FailureCode);
        }

        /// <summary>
        /// Verifies Path augmentation prepends local host exactly once.
        /// </summary>
        [Fact]
        public void Parse_WhenPathPresentAndMissingLocalHost_PrependsLocalHost()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m7@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: news.example.org!feed2",
                ],
                body: "body\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
            Assert.Equal("bf01.usenet.ninja!news.example.org!feed2", result.CanonicalPath);
        }

        /// <summary>
        /// Verifies Path normalization avoids double-inserting configured local host.
        /// </summary>
        [Fact]
        public void Parse_WhenPathAlreadyContainsLocalHost_DoesNotDuplicate()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m8@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: bf01.usenet.ninja!news.example.org",
                ],
                body: "body\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
            Assert.Equal("bf01.usenet.ninja!news.example.org", result.CanonicalPath);
        }

        /// <summary>
        /// Verifies malformed path bytes are rejected.
        /// </summary>
        [Fact]
        public void Parse_WhenPathContainsControlByte_Rejects()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticleRaw(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m9@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                rawPath: "feed1\u001f!feed2",
                body: "body\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.False(result.IsAccepted);
            Assert.Equal(NntpArticleParseFailureCode.ContainsIllegalControlByte, result.FailureCode);
        }

        /// <summary>
        /// Verifies missing header/body separator is rejected.
        /// </summary>
        [Fact]
        public void Parse_WhenHeaderBodySeparatorMissing_Rejects()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                "Message-ID: <m10@example.test>\r\n" +
                "Newsgroups: alt.test\r\n" +
                "From: user@example.test\r\n" +
                "NoTerminator");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.False(result.IsAccepted);
            Assert.Equal(NntpArticleParseFailureCode.MissingHeaderBodySeparator, result.FailureCode);
        }

        /// <summary>
        /// Verifies header continuation lines append to preceding header value without rejection.
        /// </summary>
        [Fact]
        public void Parse_WhenHeaderContinuationPresent_Accepts()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                "Message-ID: <m11@example.test>\r\n" +
                "Newsgroups: alt.test\r\n" +
                "From: user@example.test\r\n" +
                "Subject: first\r\n" +
                "\tcontinued\r\n" +
                "\r\n" +
                "body\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
        }

        /// <summary>
        /// Verifies continuation without a preceding header is rejected.
        /// </summary>
        [Fact]
        public void Parse_WhenContinuationWithoutPrecedingHeader_Rejects()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = Encoding.ASCII.GetBytes("\tbroken\r\n\r\nbody\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.False(result.IsAccepted);
            Assert.Equal(NntpArticleParseFailureCode.MalformedHeaderContinuation, result.FailureCode);
        }

        /// <summary>
        /// Verifies non-yEnc articles do not trigger expensive yEnc rejection semantics.
        /// </summary>
        [Fact]
        public void Parse_WhenNoYEncMarkers_DoesNotRejectAsYEnc()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m12@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                body: "normal text body\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
            Assert.False(result.YEncDetected);
        }

        /// <summary>
        /// Verifies parser integrates with yEnc validator and accepts valid synthetic single-part yEnc content.
        /// </summary>
        [Fact]
        public void Parse_WhenYEncFixtureValid_AcceptsAndClassifiesYEnc()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] body = BuildSyntheticSinglePartYEncBody(4096, "fixture-compat.bin");
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m13@example.test>",
                    "Newsgroups: alt.binaries.test",
                    "From: user@example.test",
                ],
                bodyBytes: body);

            NntpArticleParseResult result = parser.Parse(article);

            Assert.True(result.IsAccepted);
            Assert.True(result.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, result.ArticleType);
            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, result.YEncValidation.Status);
        }

        /// <summary>
        /// Verifies yEnc failures are rejected with yEnc-decoding-failed semantics.
        /// </summary>
        [Fact]
        public void Parse_WhenYEncFixtureInvalid_RejectsAsYEncDecodingFailed()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] body = File.ReadAllBytes(Path.Combine(FixtureRoot, "test_bad_crc_end.yenc"));
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <m14@example.test>",
                    "Newsgroups: alt.binaries.test",
                    "From: user@example.test",
                ],
                bodyBytes: body);

            NntpArticleParseResult result = parser.Parse(article);

            Assert.False(result.IsAccepted);
            Assert.True(result.YEncDetected);
            Assert.Equal(NntpArticleParseFailureCode.YEncDecodingFailed, result.FailureCode);
            Assert.Equal(YEncArticleValidationStatus.CrcMismatch, result.YEncValidation.Status);
        }

        /// <summary>
        /// Verifies parser rejects NUL bytes in headers as hostile input.
        /// </summary>
        [Fact]
        public void Parse_WhenHeaderContainsNul_Rejects()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] article = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                "Message-ID: <m15@example.test>\0\r\n" +
                "Newsgroups: alt.test\r\n" +
                "From: user@example.test\r\n\r\nbody\r\n");

            NntpArticleParseResult result = parser.Parse(article);

            Assert.False(result.IsAccepted);
            Assert.Equal(NntpArticleParseFailureCode.ContainsIllegalControlByte, result.FailureCode);
        }

        /// <summary>
        /// Verifies deterministic parser behavior for random hostile input and absence of runtime exceptions.
        /// </summary>
        [Fact]
        public void Parse_WhenFuzzedInputProvided_DoesNotThrowAndIsDeterministic()
        {
            NntpArticleParser parser = new(LocalFqdn);
            Random random = new(12345);

            for (int i = 0; i < 500; i++)
            {
                int length = random.Next(0, 8192);
                byte[] data = new byte[length];
                random.NextBytes(data);

                Exception? ex1 = Record.Exception(() => parser.Parse(data));
                Exception? ex2 = Record.Exception(() => parser.Parse(data));
                Assert.Null(ex1);
                Assert.Null(ex2);

                NntpArticleParseResult first = parser.Parse(data);
                NntpArticleParseResult second = parser.Parse(data);
                Assert.Equal(first.IsAccepted, second.IsAccepted);
                Assert.Equal(first.FailureCode, second.FailureCode);
                Assert.Equal(first.ArticleType, second.ArticleType);
                Assert.Equal(first.YEncDetected, second.YEncDetected);
                Assert.Equal(first.YEncValidation.Status, second.YEncValidation.Status);
            }
        }

        /// <summary>
        /// Builds an article with explicit header lines and optional byte body.
        /// </summary>
        /// <param name="headers">Header lines without CRLF.</param>
        /// <param name="body">Body text when <paramref name="bodyBytes"/> is null.</param>
        /// <param name="bodyBytes">Raw body bytes.</param>
        /// <returns>Article bytes with CRLF separator.</returns>
        private static byte[] BuildArticle(IEnumerable<string> headers, string? body = null, byte[]? bodyBytes = null)
        {
            StringBuilder sb = new();
            foreach (string header in headers)
            {
                _ = sb.Append(header).Append("\r\n");
            }

            _ = sb.Append("\r\n");
            byte[] prefix = Encoding.ASCII.GetBytes(sb.ToString());

            bodyBytes ??= Encoding.ASCII.GetBytes(body ?? string.Empty);

            byte[] article = new byte[prefix.Length + bodyBytes.Length];
            Buffer.BlockCopy(prefix, 0, article, 0, prefix.Length);
            Buffer.BlockCopy(bodyBytes, 0, article, prefix.Length, bodyBytes.Length);
            return article;
        }

        /// <summary>
        /// Builds article with a raw path value that may include control bytes.
        /// </summary>
        /// <param name="headers">All headers except Path.</param>
        /// <param name="rawPath">Raw Path value literal.</param>
        /// <param name="body">Body text.</param>
        /// <returns>Article bytes.</returns>
        private static byte[] BuildArticleRaw(IEnumerable<string> headers, string rawPath, string body)
        {
            StringBuilder sb = new();
            foreach (string header in headers)
            {
                _ = sb.Append(header).Append("\r\n");
            }

            _ = sb.Append("Path: ").Append(rawPath).Append("\r\n\r\n").Append(body);
            return Encoding.ASCII.GetBytes(sb.ToString());
        }

        /// <summary>
        /// Builds a deterministic valid single-part yEnc body used by parser acceptance tests.
        /// </summary>
        /// <param name="payloadLength">Decoded payload length.</param>
        /// <param name="name">Payload name metadata.</param>
        /// <returns>Valid yEnc body bytes.</returns>
        private static byte[] BuildSyntheticSinglePartYEncBody(int payloadLength, string name)
        {
            byte[] payload = new byte[payloadLength];
            Random random = new(17);
            random.NextBytes(payload);

            byte[] encodedPayload = EncodeYEncPayload(payload);
            uint crc = ComputeCrc32(payload);

            byte[] prefix = Encoding.ASCII.GetBytes($"=ybegin line=128 size={payload.Length} name={name}\r\n");
            byte[] suffix = Encoding.ASCII.GetBytes($"\r\n=yend size={payload.Length} crc32={crc:x8}\r\n");

            byte[] body = new byte[prefix.Length + encodedPayload.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, body, 0, prefix.Length);
            Buffer.BlockCopy(encodedPayload, 0, body, prefix.Length, encodedPayload.Length);
            Buffer.BlockCopy(suffix, 0, body, prefix.Length + encodedPayload.Length, suffix.Length);
            return body;
        }

        /// <summary>
        /// Encodes decoded payload bytes to yEnc payload bytes using the validator test escape rules.
        /// </summary>
        /// <param name="decoded">Decoded payload bytes.</param>
        /// <returns>yEnc-encoded payload bytes with CRLF line wrapping.</returns>
        private static byte[] EncodeYEncPayload(byte[] decoded)
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
        /// Computes CRC32 for decoded payload bytes using the yEnc polynomial.
        /// </summary>
        /// <param name="data">Decoded payload bytes.</param>
        /// <returns>CRC32 checksum.</returns>
        private static uint ComputeCrc32(byte[] data)
        {
            uint crc = 0xFFFFFFFFu;
            for (int i = 0; i < data.Length; i++)
            {
                crc ^= data[i];
                for (int bit = 0; bit < 8; bit++)
                {
                    uint mask = (uint)-(int)(crc & 1);
                    crc = (crc >> 1) ^ (0xEDB88320u & mask);
                }
            }

            return ~crc;
        }

        /// <summary>
        /// Resolves fixture root path for SABCTools yEnc samples.
        /// </summary>
        /// <returns>Absolute fixture directory path.</returns>
        private static string ResolveFixtureRoot()
        {
            string current = AppContext.BaseDirectory;
            for (int i = 0; i < 8; i++)
            {
                string candidate = Path.GetFullPath(Path.Combine(current, "..", "..", "..", "..", "VectorNNTP.BackFiller.Tests", "Fixtures", "SabctoolsYEnc"));
                if (Directory.Exists(candidate))
                {
                    return candidate;
                }

                current = Path.Combine(current, "..");
            }

            throw new DirectoryNotFoundException("Unable to locate SABCTools fixture root for parser tests.");
        }
    }

}
