// <copyright file="NntpArticleParserBenchmarks.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Benchmarks / Articles / Parsing
// BenchmarkDotNet suite for the NNTP article parser hot path across representative
// text, binary, malformed, and yEnc article shapes.

using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;

namespace VectorNNTP.BackFiller.Benchmarks
{
    /// <summary>
    /// Measures parser throughput and allocations across representative article inputs.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 10)]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    public class NntpArticleParserBenchmarks
    {
        /// <summary>
        /// Configured local FQDN used for Path normalization during parse runs.
        /// </summary>
        private const string LocalFqdn = "bf01.usenet.ninja";

        /// <summary>
        /// Optional single-part yEnc body override used only by benchmark contract tests.
        /// </summary>
        private readonly byte[]? _yencSingleBodyOverride;

        /// <summary>
        /// Optional multi-part yEnc body override used only by benchmark contract tests.
        /// </summary>
        private readonly byte[]? _yencMultipartBodyOverride;

        /// <summary>
        /// Parser instance reused across benchmark invocations.
        /// </summary>
        private NntpArticleParser _parser = null!;

        /// <summary>
        /// Tiny textual article sample.
        /// </summary>
        private byte[] _tinyText = null!;

        /// <summary>
        /// Typical textual article sample.
        /// </summary>
        private byte[] _typicalText = null!;

        /// <summary>
        /// Large textual article sample.
        /// </summary>
        private byte[] _largeText = null!;

        /// <summary>
        /// Valid yEnc single-part article sample.
        /// </summary>
        private byte[] _yencSingle = null!;

        /// <summary>
        /// Valid yEnc multipart article sample.
        /// </summary>
        private byte[] _yencMultipart = null!;

        /// <summary>
        /// Malformed article sample.
        /// </summary>
        private byte[] _malformed = null!;

        /// <summary>
        /// Invalid date article sample.
        /// </summary>
        private byte[] _invalidDate = null!;

        /// <summary>
        /// Large header-set article sample.
        /// </summary>
        private byte[] _largeHeaderSet = null!;

        /// <summary>
        /// Large binary article sample.
        /// </summary>
        private byte[] _largeBinary = null!;

        /// <summary>
        /// Initializes parser benchmarks with production fixture generation.
        /// </summary>
        public NntpArticleParserBenchmarks()
        {
        }

        /// <summary>
        /// Initializes parser benchmarks with optional deterministic yEnc fixture overrides for contract tests.
        /// </summary>
        /// <param name="yencSingleBodyOverride">Optional single-part yEnc body bytes used by <see cref="Setup"/> when provided.</param>
        /// <param name="yencMultipartBodyOverride">Optional multi-part yEnc body bytes used by <see cref="Setup"/> when provided.</param>
        internal NntpArticleParserBenchmarks(byte[]? yencSingleBodyOverride, byte[]? yencMultipartBodyOverride)
        {
            _yencSingleBodyOverride = yencSingleBodyOverride;
            _yencMultipartBodyOverride = yencMultipartBodyOverride;
        }

        /// <summary>
        /// Builds deterministic benchmark fixtures.
        /// </summary>
        [GlobalSetup]
        public void Setup()
        {
            _parser = new NntpArticleParser(LocalFqdn);

            _tinyText = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-tiny@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                body: "hi\r\n");

            _typicalText = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-typical@example.test>",
                    "Newsgroups: alt.test,alt.binaries.misc",
                    "From: user@example.test",
                    "Subject: typical",
                    "Path: feed1!feed2",
                ],
                body: BuildRepeatedTextLine("typical text line", 512));

            _largeText = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-large-text@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Subject: large text",
                ],
                body: BuildRepeatedTextLine("Lorem ipsum dolor sit amet", 32_768));

            _yencSingle = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-yenc-single@example.test>",
                    "Newsgroups: alt.binaries.test",
                    "From: user@example.test",
                ],
                bodyBytes: _yencSingleBodyOverride ?? BuildSyntheticSinglePartYEnc(4096, "single.bin"));

            _yencMultipart = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-yenc-multipart@example.test>",
                    "Newsgroups: alt.binaries.test",
                    "From: user@example.test",
                ],
                bodyBytes: _yencMultipartBodyOverride ?? BuildSyntheticMultiPartYEnc(8192, "multi.bin", partIndex: 1));

            _malformed = Encoding.ASCII.GetBytes(
                "Date Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                "Message-ID: <bench-malformed@example.test>\r\n" +
                "broken\r\n");

            _invalidDate = BuildArticle(
                headers:
                [
                    "Date: BAD-DATE",
                    "Message-ID: <bench-invalid-date@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                body: "body\r\n");

            _largeHeaderSet = BuildArticle(
                headers: BuildLargeHeaderSet(),
                body: "body\r\n");

            _largeBinary = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-large-bin@example.test>",
                    "Newsgroups: alt.binaries.misc",
                    "From: user@example.test",
                    "Content-Transfer-Encoding: binary",
                ],
                bodyBytes: BuildBinaryBody(2_097_152));

            ValidateYEncFixtureAccepted(_parser, _yencSingle, YEncArticleValidationStatus.ValidSinglePart);
            ValidateYEncFixtureAccepted(_parser, _yencMultipart, YEncArticleValidationStatus.ValidMultiPart);
        }

        /// <summary>
        /// Measures tiny text parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Baseline = true, Description = "TinyText")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseTinyText()
        {
            return _parser.Parse(_tinyText).IsAccepted;
        }

        /// <summary>
        /// Measures typical text parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "TypicalText")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseTypicalText()
        {
            return _parser.Parse(_typicalText).IsAccepted;
        }

        /// <summary>
        /// Measures large text parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "LargeText")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseLargeText()
        {
            return _parser.Parse(_largeText).IsAccepted;
        }

        /// <summary>
        /// Measures valid yEnc single-part parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "YEncSinglePart")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseYEncSinglePart()
        {
            return _parser.Parse(_yencSingle).IsAccepted;
        }

        /// <summary>
        /// Measures valid yEnc multipart parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "YEncMultiPart")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseYEncMultiPart()
        {
            return _parser.Parse(_yencMultipart).IsAccepted;
        }

        /// <summary>
        /// Measures malformed article handling performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "MalformedArticle")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseMalformedArticle()
        {
            return _parser.Parse(_malformed).IsAccepted;
        }

        /// <summary>
        /// Measures invalid date rejection performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "InvalidDate")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseInvalidDate()
        {
            return _parser.Parse(_invalidDate).IsAccepted;
        }

        /// <summary>
        /// Measures large-header-set parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "LargeHeaderSet")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseLargeHeaderSet()
        {
            return _parser.Parse(_largeHeaderSet).IsAccepted;
        }

        /// <summary>
        /// Measures large binary article parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Description = "LargeBinary")]
        [BenchmarkCategory("ArticleParser")]

        public bool ParseLargeBinary()
        {
            return _parser.Parse(_largeBinary).IsAccepted;
        }

        /// <summary>
        /// Builds an article byte array from headers and text body.
        /// </summary>
        /// <param name="headers">Header lines.</param>
        /// <param name="body">Text body when byte body is not supplied.</param>
        /// <param name="bodyBytes">Optional byte body.</param>
        /// <returns>Complete article bytes.</returns>
        private static byte[] BuildArticle(IEnumerable<string> headers, string? body = null, byte[]? bodyBytes = null)
        {
            StringBuilder sb = new();
            foreach (string header in headers)
            {
                _ = sb.Append(header).Append("\r\n");
            }

            _ = sb.Append("\r\n");
            byte[] headerBytes = Encoding.ASCII.GetBytes(sb.ToString());
            byte[] payload = bodyBytes ?? Encoding.ASCII.GetBytes(body ?? string.Empty);
            byte[] article = new byte[headerBytes.Length + payload.Length];
            Buffer.BlockCopy(headerBytes, 0, article, 0, headerBytes.Length);
            Buffer.BlockCopy(payload, 0, article, headerBytes.Length, payload.Length);
            return article;
        }

        /// <summary>
        /// Validates that a benchmark yEnc fixture is accepted through the production parser and yEnc validator path.
        /// </summary>
        /// <param name="parser">Parser instance used by the benchmark.</param>
        /// <param name="article">Fixture article bytes.</param>
        /// <param name="expectedStatus">Expected terminal yEnc validator status.</param>
        /// <exception cref="InvalidOperationException">Thrown when the fixture is rejected or validates to an unexpected status.</exception>
        private static void ValidateYEncFixtureAccepted(NntpArticleParser parser, byte[] article, YEncArticleValidationStatus expectedStatus)
        {
            NntpArticleParseResult parseResult = parser.Parse(article);
            if (!parseResult.IsAccepted || parseResult.ArticleType != NntpArticleType.YEnc || !parseResult.YEncDetected || parseResult.YEncValidation.Status != expectedStatus)
            {
                throw new InvalidOperationException(
                    $"Benchmark yEnc fixture validation failed. Accepted={parseResult.IsAccepted}, Type={parseResult.ArticleType}, Detected={parseResult.YEncDetected}, Status={parseResult.YEncValidation.Status}.");
            }
        }

        /// <summary>
        /// Builds deterministic repeated text lines.
        /// </summary>
        /// <param name="line">Line content.</param>
        /// <param name="repeatCount">Number of lines.</param>
        /// <returns>Repeated text body.</returns>
        private static string BuildRepeatedTextLine(string line, int repeatCount)
        {
            StringBuilder sb = new(line.Length * repeatCount);
            for (int i = 0; i < repeatCount; i++)
            {
                _ = sb.Append(line).Append("\r\n");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Builds deterministic large header set.
        /// </summary>
        /// <returns>Header sequence.</returns>
        private static IEnumerable<string> BuildLargeHeaderSet()
        {
            List<string> headers =
            [
                "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                "Message-ID: <bench-large-headers@example.test>",
                "Newsgroups: alt.test",
                "From: user@example.test",
            ];

            for (int i = 0; i < 512; i++)
            {
                headers.Add($"X-Header-{i}: value-{i}");
            }

            return headers;
        }

        /// <summary>
        /// Builds deterministic binary body bytes.
        /// </summary>
        /// <param name="size">Body size in bytes.</param>
        /// <returns>Binary body.</returns>
        private static byte[] BuildBinaryBody(int size)
        {
            byte[] bytes = new byte[size];
            Random random = new(1234);
            random.NextBytes(bytes);
            if (size > 0)
            {
                bytes[^1] = (byte)'\n';
            }

            return bytes;
        }

        /// <summary>
        /// Builds deterministic single-part yEnc body with valid metadata.
        /// </summary>
        /// <param name="payloadLength">Decoded payload length.</param>
        /// <param name="name">File name metadata.</param>
        /// <returns>Valid yEnc body bytes.</returns>
        private static byte[] BuildSyntheticSinglePartYEnc(int payloadLength, string name)
        {
            byte[] payload = BuildPayload(payloadLength, seed: 17);
            EncodeYEncPayload(payload, out byte[] encoded, out uint crc);

            byte[] prefix = Encoding.ASCII.GetBytes($"=ybegin line=128 size={payload.Length} name={name}\r\n");
            byte[] suffix = Encoding.ASCII.GetBytes($"=yend size={payload.Length} crc32={crc:x8}\r\n");

            byte[] body = new byte[prefix.Length + encoded.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, body, 0, prefix.Length);
            Buffer.BlockCopy(encoded, 0, body, prefix.Length, encoded.Length);
            Buffer.BlockCopy(suffix, 0, body, prefix.Length + encoded.Length, suffix.Length);
            return body;
        }

        /// <summary>
        /// Builds deterministic multipart yEnc body with valid metadata.
        /// </summary>
        /// <param name="payloadLength">Decoded payload length.</param>
        /// <param name="name">File name metadata.</param>
        /// <param name="partIndex">Part index marker.</param>
        /// <returns>Valid multipart yEnc body bytes.</returns>
        private static byte[] BuildSyntheticMultiPartYEnc(int payloadLength, string name, int partIndex)
        {
            byte[] payload = BuildPayload(payloadLength, seed: 23);
            EncodeYEncPayload(payload, out byte[] encoded, out uint crc);

            byte[] prefix = Encoding.ASCII.GetBytes($"=ybegin part={partIndex} line=128 size={payload.Length} name={name}\r\n=ypart begin=1 end={payload.Length}\r\n");
            byte[] suffix = Encoding.ASCII.GetBytes($"=yend size={payload.Length} pcrc32={crc:x8}\r\n");

            byte[] body = new byte[prefix.Length + encoded.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, body, 0, prefix.Length);
            Buffer.BlockCopy(encoded, 0, body, prefix.Length, encoded.Length);
            Buffer.BlockCopy(suffix, 0, body, prefix.Length + encoded.Length, suffix.Length);
            return body;
        }

        /// <summary>
        /// Builds deterministic source payload bytes.
        /// </summary>
        /// <param name="length">Payload length.</param>
        /// <param name="seed">PRNG seed.</param>
        /// <returns>Payload bytes.</returns>
        private static byte[] BuildPayload(int length, int seed)
        {
            byte[] payload = new byte[length];
            Random random = new(seed);
            random.NextBytes(payload);
            return payload;
        }

        /// <summary>
        /// Encodes payload bytes to yEnc payload bytes and computes CRC32.
        /// </summary>
        /// <param name="payload">Decoded payload bytes.</param>
        /// <param name="encoded">Encoded yEnc payload bytes.</param>
        /// <param name="crc">CRC32 of decoded payload.</param>
        private static void EncodeYEncPayload(byte[] payload, out byte[] encoded, out uint crc)
        {
            uint crcValue = 0xFFFFFFFFu;
            List<byte> output = new(payload.Length + (payload.Length / 4));

            int lineLength = 0;
            for (int i = 0; i < payload.Length; i++)
            {
                byte original = payload[i];
                crcValue ^= original;
                for (int j = 0; j < 8; j++)
                {
                    uint mask = (uint)-(int)(crcValue & 1);
                    crcValue = (crcValue >> 1) ^ (0xEDB88320u & mask);
                }

                byte encodedByte = (byte)((original + 42) & 0xFF);
                bool escape = encodedByte is 0 or ((byte)'\r') or ((byte)'\n') or ((byte)'=');
                if (escape)
                {
                    output.Add((byte)'=');
                    encodedByte = (byte)((encodedByte + 64) & 0xFF);
                    output.Add(encodedByte);
                    lineLength += 2;
                }
                else
                {
                    output.Add(encodedByte);
                    lineLength++;
                }

                if (lineLength >= 128)
                {
                    output.Add((byte)'\r');
                    output.Add((byte)'\n');
                    lineLength = 0;
                }
            }

            if (lineLength > 0)
            {
                output.Add((byte)'\r');
                output.Add((byte)'\n');
            }

            crc = ~crcValue;
            encoded = [.. output];
        }
    }
}
