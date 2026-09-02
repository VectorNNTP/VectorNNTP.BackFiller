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

namespace VectorNNTP.BackFiller.Benchmarks
{
    /// <summary>
    /// Measures parser throughput and allocations across representative article inputs.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 10)]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    /// <summary>
    /// Represents the nntp ArticleParserBenchmarks class used by the benchmark or regression gate.
    /// </summary>
    public class NntpArticleParserBenchmarks
    {
        /// <summary>
        /// Configured local FQDN used for Path normalization during parse runs.
        /// </summary>
        private const string LocalFqdn = "bf01.usenet.ninja";

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
        /// Builds deterministic benchmark fixtures.
        /// </summary>
        [GlobalSetup]
        /// <summary>
        /// Initializes the reusable benchmark state.
        /// </summary>
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
                body: BuildSyntheticSinglePartYEnc(4096, "single.bin"));

            _yencMultipart = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-yenc-multipart@example.test>",
                    "Newsgroups: alt.binaries.test",
                    "From: user@example.test",
                ],
                body: BuildSyntheticMultiPartYEnc(8192, "multi.bin", partIndex: 1));

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
        }

        /// <summary>
        /// Measures tiny text parse performance.
        /// </summary>
        /// <returns>Accepted flag for sink prevention.</returns>
        [Benchmark(Baseline = true, Description = "TinyText")]
        [BenchmarkCategory("ArticleParser")]
        /// <summary>
        /// Parses ext.

        /// </summary>
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
        /// <summary>
        /// Parses alText.

        /// </summary>
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
        /// <summary>
        /// Parses Text.

        /// </summary>
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
        /// <summary>
        /// Parses SinglePart.

        /// </summary>
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
        /// <summary>
        /// Parses MultiPart.

        /// </summary>
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
        /// <summary>
        /// Parses rmedArticle.

        /// </summary>
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
        /// <summary>
        /// Parses idDate.

        /// </summary>
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
        /// <summary>
        /// Parses HeaderSet.

        /// </summary>
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
        /// <summary>
        /// Parses Binary.

        /// </summary>
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
        private static string BuildSyntheticSinglePartYEnc(int payloadLength, string name)
        {
            byte[] payload = BuildPayload(payloadLength, seed: 17);
            EncodeYEncPayload(payload, out string encoded, out uint crc);

            return $"=ybegin line=128 size={payload.Length} name={name}\r\n"
                + encoded
                + $"=yend size={payload.Length} crc32={crc:x8}\r\n";
        }

        /// <summary>
        /// Builds deterministic multipart yEnc body with valid metadata.
        /// </summary>
        /// <param name="payloadLength">Decoded payload length.</param>
        /// <param name="name">File name metadata.</param>
        /// <param name="partIndex">Part index marker.</param>
        /// <returns>Valid multipart yEnc body bytes.</returns>
        private static string BuildSyntheticMultiPartYEnc(int payloadLength, string name, int partIndex)
        {
            byte[] payload = BuildPayload(payloadLength, seed: 23);
            EncodeYEncPayload(payload, out string encoded, out uint crc);

            return $"=ybegin part={partIndex} line=128 size={payload.Length} name={name}\r\n"
                + $"=ypart begin=1 end={payload.Length}\r\n"
                + encoded
                + $"=yend size={payload.Length} pcrc32={crc:x8}\r\n";
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
        /// Encodes payload bytes to yEnc text and computes CRC32.
        /// </summary>
        /// <param name="payload">Decoded payload bytes.</param>
        /// <param name="encoded">Encoded yEnc text.</param>
        /// <param name="crc">CRC32 of decoded payload.</param>
        private static void EncodeYEncPayload(byte[] payload, out string encoded, out uint crc)
        {
            uint crcValue = 0xFFFFFFFFu;
            StringBuilder sb = new(payload.Length + (payload.Length / 4));

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
                    _ = sb.Append('=');
                    encodedByte = (byte)((encodedByte + 64) & 0xFF);
                    _ = sb.Append((char)encodedByte);
                    lineLength += 2;
                }
                else
                {
                    _ = sb.Append((char)encodedByte);
                    lineLength++;
                }

                if (lineLength >= 128)
                {
                    _ = sb.Append("\r\n");
                    lineLength = 0;
                }
            }

            if (lineLength > 0)
            {
                _ = sb.Append("\r\n");
            }

            crc = ~crcValue;
            encoded = sb.ToString();
        }
    }
}
