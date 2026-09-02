// <copyright file="YEncArticleValidatorBenchmarks.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Benchmarks / Articles / YEnc
// Focused BenchmarkDotNet coverage for yEnc validator correctness-path and hostile-input-path throughput/allocation baselines.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;

namespace VectorNNTP.BackFiller.Benchmarks
{
    /// <summary>
    /// Measures yEnc validator hot-path cost across valid, malformed, and hostile article body shapes.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(launchCount: 1, warmupCount: 3, iterationCount: 10)]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    /// <summary>
    /// Represents the y EncArticleValidatorBenchmarks class used by this benchmark or regression-gate component.
    /// </summary>
    public class YEncArticleValidatorBenchmarks
    {
        /// <summary>
        /// Cached valid small yEnc payload article.
        /// </summary>
        private byte[] _validSmall = null!;

        /// <summary>
        /// Cached valid large yEnc payload article.
        /// </summary>
        private byte[] _validLarge = null!;

        /// <summary>
        /// Cached valid multipart yEnc payload article.
        /// </summary>
        private byte[] _validMultipart = null!;

        /// <summary>
        /// Cached malformed yEnc metadata article.
        /// </summary>
        private byte[] _malformedMetadata = null!;

        /// <summary>
        /// Cached CRC-mismatch article.
        /// </summary>
        private byte[] _crcMismatch = null!;

        /// <summary>
        /// Cached invalid-escape article.
        /// </summary>
        private byte[] _invalidEscape = null!;

        /// <summary>
        /// Cached non-yEnc body sample.
        /// </summary>
        private byte[] _nonYEnc = null!;

        /// <summary>
        /// Cached dot-stuffed valid yEnc sample.
        /// </summary>
        private byte[] _dotStuffed = null!;

        /// <summary>
        /// Cached metadata-heavy valid yEnc sample.
        /// </summary>
        private byte[] _metadataHeavy = null!;

        /// <summary>
        /// Creates deterministic benchmark fixtures.
        /// </summary>
        [GlobalSetup]
        /// <summary>
        /// Executes the setup operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public void Setup()
        {
            _validSmall = BuildSinglePartArticle(BuildPayload(512, 7), false);
            _validLarge = BuildSinglePartArticle(BuildPayload(256 * 1024, 11), false);
            _validMultipart = BuildMultiPartArticle(BuildPayload(16 * 1024, 13), 1, 16 * 1024);
            _malformedMetadata = "=ybegin line=128 name=test.bin\r\nabc\r\n=yend size=3 crc32=352441c2\r\n"u8.ToArray();

            _crcMismatch = BuildSinglePartArticle(BuildPayload(4096, 17), false);
            int crcMismatchPayloadOffset = FindPayloadOffset(_crcMismatch);
            _crcMismatch[crcMismatchPayloadOffset + 123] ^= 0x40;

            _invalidEscape = BuildSinglePartArticle(BuildPayload(4096, 19), false);
            int invalidEscapePayloadOffset = FindPayloadOffset(_invalidEscape);
            int invalidEscapeLineEnd = Array.IndexOf(_invalidEscape, (byte)'\n', invalidEscapePayloadOffset);
            _invalidEscape[invalidEscapeLineEnd - 1] = (byte)'=';

            _nonYEnc = Encoding.ASCII.GetBytes("Subject: plain\r\n\r\nThis is a plain article body without yEnc control lines.\r\n");
            _dotStuffed = BuildSinglePartArticle(BuildPayload(4096, 23), true);

            _metadataHeavy = BuildMetadataHeavyArticle(BuildPayload(8192, 29));
        }

        /// <summary>
        /// Measures small valid yEnc validation throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Baseline = true, Description = "ValidSmall")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate ValidSmall operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateValidSmall() => (int)YEncArticleValidator.Validate(_validSmall).Status;

        /// <summary>
        /// Measures large valid yEnc validation throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "ValidLarge")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate ValidLarge operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateValidLarge() => (int)YEncArticleValidator.Validate(_validLarge).Status;

        /// <summary>
        /// Measures valid multipart section validation throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "ValidMultipart")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate ValidMultipart operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateValidMultipart() => (int)YEncArticleValidator.Validate(_validMultipart).Status;

        /// <summary>
        /// Measures malformed metadata rejection throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "MalformedMetadata")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate MalformedMetadata operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateMalformedMetadata() => (int)YEncArticleValidator.Validate(_malformedMetadata).Status;

        /// <summary>
        /// Measures CRC-mismatch detection throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "CrcMismatch")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate CrcMismatch operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateCrcMismatch() => (int)YEncArticleValidator.Validate(_crcMismatch).Status;

        /// <summary>
        /// Measures invalid escape-sequence rejection throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "InvalidEscape")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate InvalidEscape operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateInvalidEscape() => (int)YEncArticleValidator.Validate(_invalidEscape).Status;

        /// <summary>
        /// Measures non-yEnc short-circuit classification throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "ValidNonYEnc")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate Nony Enc operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateNonYEnc() => (int)YEncArticleValidator.Validate(_nonYEnc).Status;

        /// <summary>
        /// Measures dot-stuffed yEnc handling throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "DotStuffedValid")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate DotStuffedValid operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateDotStuffedValid() => (int)YEncArticleValidator.Validate(_dotStuffed).Status;

        /// <summary>
        /// Measures metadata-heavy yEnc validation throughput.
        /// </summary>
        /// <returns>Validation status code to prevent dead-code elimination.</returns>
        [Benchmark(Description = "MetadataHeavy")]
        [BenchmarkCategory("YEncValidator")]
        /// <summary>
        /// Executes the validate MetadataHeavy operation while preserving the component's benchmark or test-harness contract.
        /// </summary>
        public int ValidateMetadataHeavy() => (int)YEncArticleValidator.Validate(_metadataHeavy).Status;

        /// <summary>
        /// Builds one synthetic single-part yEnc article body.
        /// </summary>
        /// <param name="decodedPayload">Decoded bytes used to produce encoded content.</param>
        /// <param name="dotStuffed">Whether line-start dots are NNTP dot-stuffed.</param>
        /// <returns>Article body bytes containing yEnc control lines and encoded payload.</returns>
        private static byte[] BuildSinglePartArticle(byte[] decodedPayload, bool dotStuffed)
        {
            byte[] encoded = EncodeYEnc(decodedPayload);
            if (dotStuffed)
            {
                encoded = DotStuffLineStarts(encoded);
            }

            uint crc = Crc32(decodedPayload);
            byte[] prefix = Encoding.ASCII.GetBytes($"=ybegin line=128 size={decodedPayload.Length} name=test.bin\r\n");
            byte[] suffix = Encoding.ASCII.GetBytes($"\r\n=yend size={decodedPayload.Length} crc32={crc:x8}\r\n");
            byte[] result = new byte[prefix.Length + encoded.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, result, 0, prefix.Length);
            Buffer.BlockCopy(encoded, 0, result, prefix.Length, encoded.Length);
            Buffer.BlockCopy(suffix, 0, result, prefix.Length + encoded.Length, suffix.Length);
            return result;
        }

        /// <summary>
        /// Builds one synthetic multipart yEnc article body.
        /// </summary>
        /// <param name="decodedPayload">Decoded bytes for the section payload.</param>
        /// <param name="begin">Declared part begin offset.</param>
        /// <param name="end">Declared part end offset.</param>
        /// <returns>Multipart article body bytes.</returns>
        private static byte[] BuildMultiPartArticle(byte[] decodedPayload, int begin, int end)
        {
            byte[] encoded = EncodeYEnc(decodedPayload);
            uint crc = Crc32(decodedPayload);
            byte[] prefix = Encoding.ASCII.GetBytes($"=ybegin part=1 line=128 size={end} name=test.bin\r\n=ypart begin={begin} end={end}\r\n");
            byte[] suffix = Encoding.ASCII.GetBytes($"\r\n=yend size={decodedPayload.Length} part=1 pcrc32={crc:x8}\r\n");
            byte[] result = new byte[prefix.Length + encoded.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, result, 0, prefix.Length);
            Buffer.BlockCopy(encoded, 0, result, prefix.Length, encoded.Length);
            Buffer.BlockCopy(suffix, 0, result, prefix.Length + encoded.Length, suffix.Length);
            return result;
        }

        /// <summary>
        /// Builds a metadata-heavy but valid yEnc article body.
        /// </summary>
        /// <param name="decodedPayload">Decoded bytes used to produce encoded content.</param>
        /// <returns>Metadata-heavy article bytes.</returns>
        private static byte[] BuildMetadataHeavyArticle(byte[] decodedPayload)
        {
            byte[] encoded = EncodeYEnc(decodedPayload);
            uint crc = Crc32(decodedPayload);
            byte[] prefix = Encoding.ASCII.GetBytes($"=ybegin part=1 total=17 line=128 size={decodedPayload.Length} name=bench_payload.bin\r\n=ypart begin=1 end={decodedPayload.Length}\r\n");
            byte[] suffix = Encoding.ASCII.GetBytes($"\r\n=yend size={decodedPayload.Length} part=1 total=17 line=128 name=bench_payload.bin pcrc32={crc:x8} crc32={crc:x8}\r\n");
            byte[] result = new byte[prefix.Length + encoded.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, result, 0, prefix.Length);
            Buffer.BlockCopy(encoded, 0, result, prefix.Length, encoded.Length);
            Buffer.BlockCopy(suffix, 0, result, prefix.Length + encoded.Length, suffix.Length);
            return result;
        }

        /// <summary>
        /// Encodes decoded bytes to yEnc bytes with CRLF wrapping.
        /// </summary>
        /// <param name="decoded">Decoded bytes.</param>
        /// <returns>Encoded yEnc bytes.</returns>
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
        /// Applies NNTP dot-stuffing for payload line starts.
        /// </summary>
        /// <param name="payload">Encoded payload bytes.</param>
        /// <returns>Dot-stuffed encoded payload bytes.</returns>
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
        /// Computes CRC-32 for decoded payload bytes.
        /// </summary>
        /// <param name="payload">Decoded payload bytes.</param>
        /// <returns>CRC-32 value.</returns>
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
        /// Generates deterministic payload bytes.
        /// </summary>
        /// <param name="size">Number of bytes to create.</param>
        /// <param name="seed">Pseudo-random generator seed.</param>
        /// <returns>Deterministic payload bytes.</returns>
        private static byte[] BuildPayload(int size, int seed)
        {
            byte[] payload = new byte[size];
            Random random = new(seed);
            random.NextBytes(payload);
            return payload;
        }

        /// <summary>
        /// Finds the encoded payload start offset in a synthetic yEnc article body.
        /// </summary>
        /// <param name="article">Article body bytes.</param>
        /// <returns>Offset to first encoded payload byte.</returns>
        private static int FindPayloadOffset(byte[] article)
        {
            int beginIndex = article.AsSpan().IndexOf("=ybegin "u8);
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
        /// CRC-32 lookup table for deterministic benchmark fixture generation.
        /// </summary>
        private static readonly uint[] CrcTable = CreateCrcTable();

        /// <summary>
        /// Creates the CRC-32 lookup table used for fixture metadata generation.
        /// </summary>
        /// <returns>Initialized table.</returns>
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
