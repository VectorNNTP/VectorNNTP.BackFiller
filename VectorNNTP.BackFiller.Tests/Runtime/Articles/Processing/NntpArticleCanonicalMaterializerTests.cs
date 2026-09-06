// <copyright file="NntpArticleCanonicalMaterializerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Processing
// Focused tests for post-validation article materialization that rewrites canonical Date and Path bytes.

using System.Buffers;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Processing
{
    /// <summary>
    /// Verifies canonical retained-article byte materialization behavior for Date and Path headers.
    /// </summary>
    public sealed class NntpArticleCanonicalMaterializerTests
    {
        private const string LocalFqdn = "backfiller01.usenet.ninja";

        [Fact]
        public void Materialize_WhenPathMissing_InsertsCanonicalPathHeaderAndRewritesDate()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-missing@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Subject: keep",
                ],
                "body-1\r\n");

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string headers = GetHeaderText(materialized.Memory.Span);
            Assert.Contains("Date: Tue, 10 May 2011 18:48:50 +0000\r\n", headers, StringComparison.Ordinal);
            Assert.Contains($"Path: {LocalFqdn}", headers, StringComparison.Ordinal);
            Assert.Contains("Subject: keep\r\n", headers, StringComparison.Ordinal);

            Assert.Equal(parse.BodyBytes.ToArray(), GetBodyBytes(materialized.Memory.Span));
        }

        [Fact]
        public void Materialize_WhenPathPresentWithoutLocalFqdn_ReplacesPathValueWithCanonicalPath()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-replace@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: news.example.org!feed2",
                    "X-Custom: preserve",
                ],
                "body-2\r\n");

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);
            Assert.Equal("backfiller01.usenet.ninja!news.example.org!feed2", parse.CanonicalPath);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string headers = GetHeaderText(materialized.Memory.Span);
            Assert.Contains("Path: backfiller01.usenet.ninja!news.example.org!feed2", headers, StringComparison.Ordinal);
            Assert.DoesNotContain("Path: news.example.org!feed2", headers, StringComparison.Ordinal);
            Assert.Contains("X-Custom: preserve", headers, StringComparison.Ordinal);
            Assert.Equal(parse.BodyBytes.ToArray(), GetBodyBytes(materialized.Memory.Span));
        }

        [Fact]
        public void Materialize_WhenPathAlreadyContainsLocalFqdn_DoesNotDuplicatePathComponent()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-existing-fqdn@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: backfiller01.usenet.ninja!news.example.org",
                ],
                "body-3\r\n");

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string headers = GetHeaderText(materialized.Memory.Span);
            Assert.Contains("Path: backfiller01.usenet.ninja!news.example.org", headers, StringComparison.Ordinal);
            Assert.Equal(1, CountOccurrences(headers, "backfiller01.usenet.ninja"));
        }

        [Fact]
        public void Materialize_WhenDateComesFromInjectionDate_RewritesWinningCandidateHeader()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Injection-Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-injection-date@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                "body-4\r\n");

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);
            Assert.Equal(NntpArticleHeaderName.InjectionDate, parse.SelectedDateHeaderName);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string headers = GetHeaderText(materialized.Memory.Span);
            Assert.Contains("Injection-Date: Tue, 10 May 2011 18:48:50 +0000\r\n", headers, StringComparison.Ordinal);
        }

        [Fact]
        public void Materialize_WhenArticleIsRejected_ThrowsAndDoesNotBypassValidation()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: INVALID",
                    "Message-ID: <materialize-invalid-date@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                "body-5\r\n");

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.False(parse.IsAccepted);

            Assert.Throws<ArgumentException>(() => NntpArticleCanonicalMaterializer.Materialize(parse));
        }

        [Fact]
        public void Materialize_WhenYEncArticleAccepted_PreservesOriginalValidatedYEncBodyBytes()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] yEncBody = BuildSyntheticSinglePartYEncBody([0x00, 0x2E, 0x3D, 0x41, 0x20]);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-yenc-preserve@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                yEncBody);

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);
            Assert.True(parse.YEncDetected);
            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, parse.YEncValidation.Status);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            byte[] materializedBody = GetBodyBytes(materialized.Memory.Span);
            Assert.Equal(parse.BodyBytes.ToArray(), materializedBody);
            Assert.Equal(yEncBody, materializedBody);
        }

        private static byte[] BuildArticle(IReadOnlyList<string> headers, string body)
        {
            return BuildArticle(headers, Encoding.ASCII.GetBytes(body));
        }

        private static byte[] BuildArticle(IReadOnlyList<string> headers, byte[] body)
        {
            StringBuilder builder = new();
            for (int i = 0; i < headers.Count; i++)
            {
                _ = builder.Append(headers[i]).Append("\r\n");
            }

            _ = builder.Append("\r\n");
            byte[] headerBytes = Encoding.ASCII.GetBytes(builder.ToString());
            byte[] article = new byte[headerBytes.Length + body.Length];
            Buffer.BlockCopy(headerBytes, 0, article, 0, headerBytes.Length);
            Buffer.BlockCopy(body, 0, article, headerBytes.Length, body.Length);
            return article;
        }

        private static string GetHeaderText(ReadOnlySpan<byte> article)
        {
            int headerEnd = FindHeaderSeparator(article);
            return Encoding.ASCII.GetString(article[..headerEnd]);
        }

        private static byte[] GetBodyBytes(ReadOnlySpan<byte> article)
        {
            int headerEnd = FindHeaderSeparator(article);
            return article[(headerEnd + 4)..].ToArray();
        }

        private static int FindHeaderSeparator(ReadOnlySpan<byte> article)
        {
            for (int i = 0; i <= article.Length - 4; i++)
            {
                if (article[i] == (byte)'\r'
                    && article[i + 1] == (byte)'\n'
                    && article[i + 2] == (byte)'\r'
                    && article[i + 3] == (byte)'\n')
                {
                    return i;
                }
            }

            throw new InvalidOperationException("Article did not contain CRLF header separator.");
        }

        private static int CountOccurrences(string source, string value)
        {
            int count = 0;
            int index = 0;
            while (true)
            {
                index = source.IndexOf(value, index, StringComparison.Ordinal);
                if (index < 0)
                {
                    return count;
                }

                count++;
                index += value.Length;
            }
        }

        private static byte[] BuildSyntheticSinglePartYEncBody(ReadOnlySpan<byte> payload)
        {
            byte[] encodedPayload = EncodeYEncPayload(payload);
            uint crc = ComputeCrc32(payload);

            byte[] prefix = Encoding.ASCII.GetBytes($"=ybegin line=128 size={payload.Length} name=test.bin\r\n");
            byte[] suffix = Encoding.ASCII.GetBytes($"\r\n=yend size={payload.Length} crc32={crc:x8}\r\n");

            byte[] body = new byte[prefix.Length + encodedPayload.Length + suffix.Length];
            Buffer.BlockCopy(prefix, 0, body, 0, prefix.Length);
            Buffer.BlockCopy(encodedPayload, 0, body, prefix.Length, encodedPayload.Length);
            Buffer.BlockCopy(suffix, 0, body, prefix.Length + encodedPayload.Length, suffix.Length);
            return body;
        }

        private static byte[] EncodeYEncPayload(ReadOnlySpan<byte> decoded)
        {
            List<byte> output = [];
            for (int i = 0; i < decoded.Length; i++)
            {
                byte encoded = unchecked((byte)(decoded[i] + 42));
                bool mustEscape = encoded is 0 or 9 or 10 or 13 or 32 or 46 or 61;
                if (mustEscape)
                {
                    output.Add((byte)'=');
                    output.Add(unchecked((byte)(encoded + 64)));
                }
                else
                {
                    output.Add(encoded);
                }
            }

            if (output.Count == 0 || output[^1] != (byte)'\n')
            {
                output.Add((byte)'\r');
                output.Add((byte)'\n');
            }

            return [.. output];
        }

        private static uint ComputeCrc32(ReadOnlySpan<byte> data)
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
    }
}
