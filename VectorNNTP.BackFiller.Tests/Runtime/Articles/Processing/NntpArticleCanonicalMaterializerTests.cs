// <copyright file="NntpArticleCanonicalMaterializerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Processing
// Focused tests for post-validation article materialization that rewrites canonical Date and Path bytes.

using System.Buffers;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles;
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
        public void Materialize_WhenCrOnlyAndPathMissing_InsertsPathWithCrOnlyAndPreservesBodyBoundary()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-cr-only-path-missing@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Subject: keep",
                ],
                "body-cr-only\r",
                "\r");

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string article = Encoding.ASCII.GetString(materialized.Memory.Span);

            Assert.Contains($"Path: {LocalFqdn}\r", article, StringComparison.Ordinal);
            Assert.Contains("Date: Tue, 10 May 2011 18:48:50 +0000\r", article, StringComparison.Ordinal);
            Assert.Contains("\r\rbody-cr-only\r", article, StringComparison.Ordinal);
            Assert.DoesNotContain("\r\n", article, StringComparison.Ordinal);
            Assert.Equal(parse.BodyBytes.ToArray(), GetBodyBytes(materialized.Memory.Span, "\r"));
        }

        [Fact]
        public void Materialize_WhenCrOnlyAndPathPresent_RewritesPathAndPreservesCrOnlySeparators()
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-cr-only-path-present@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: news.example.org!feed2",
                ],
                "body-cr-only-path\r",
                "\r");

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string article = Encoding.ASCII.GetString(materialized.Memory.Span);

            Assert.Contains($"Path: {LocalFqdn}!news.example.org!feed2\r", article, StringComparison.Ordinal);
            Assert.DoesNotContain("\r\n", article, StringComparison.Ordinal);
            Assert.Equal(parse.BodyBytes.ToArray(), GetBodyBytes(materialized.Memory.Span, "\r"));
        }

        [Theory]
        [InlineData("\r\n")]
        [InlineData("\n")]
        [InlineData("\r")]
        public void Materialize_PreservesHeaderSeparatorStyleAndBodyBytesAcrossSupportedSeparators(string separator)
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] originalArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-separator-variant@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Subject: separator-variant",
                ],
                $"body-{separator.Length}x{((int)separator[0]).ToString()}\r",
                separator);

            NntpArticleParseResult parse = parser.Parse(originalArticle);
            Assert.True(parse.IsAccepted);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string article = Encoding.ASCII.GetString(materialized.Memory.Span);

            Assert.Contains($"Path: {LocalFqdn}{separator}", article, StringComparison.Ordinal);
            Assert.Contains($"Date: Tue, 10 May 2011 18:48:50 +0000{separator}", article, StringComparison.Ordinal);
            Assert.Contains($"{separator}{separator}", article, StringComparison.Ordinal);
            Assert.Equal(parse.BodyBytes.ToArray(), GetBodyBytes(materialized.Memory.Span, separator));
        }

        [Theory]
        [InlineData("\n", "\r\n")]
        [InlineData("\r\n", "\r")]
        [InlineData("\r", "\r\n")]
        public void Materialize_WhenPathMissingAndBoundaryUsesMixedTerminators_PreservesBodyAndMaterializes(string lastHeaderTerminator, string boundaryTerminator)
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] body = Encoding.ASCII.GetBytes("body-a\r\nbody-b\nbody-c\r\n\rmarker");
            byte[] article = BuildArticleWithCustomBoundary(
                [
                    ("Date: Tue, 10 May 2011 13:48:50 -0500", "\r\n"),
                    ("Message-ID: <materialize-mixed-path-missing@example.test>", "\r\n"),
                    ("Newsgroups: alt.test", "\r\n"),
                    ("From: user@example.test", "\r\n"),
                    ("X-Keep: separator-contract", lastHeaderTerminator),
                ],
                boundaryTerminator,
                body);

            NntpArticleParseResult parse = parser.Parse(article);
            Assert.True(parse.IsAccepted);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string materializedText = Encoding.ASCII.GetString(materialized.Memory.Span);
            Assert.Contains($"Path: {LocalFqdn}{lastHeaderTerminator}", materializedText, StringComparison.Ordinal);
            Assert.Contains($"Date: Tue, 10 May 2011 18:48:50 +0000\r\n", materializedText, StringComparison.Ordinal);
            Assert.Contains($"X-Keep: separator-contract{lastHeaderTerminator}", materializedText, StringComparison.Ordinal);

            NntpArticleParseResult reparsed = parser.Parse(materialized.Memory.ToArray());
            Assert.True(reparsed.IsAccepted);
            Assert.Equal(parse.BodyBytes.ToArray(), reparsed.BodyBytes.ToArray());
        }

        [Theory]
        [InlineData("\n", "\r\n")]
        [InlineData("\r\n", "\r")]
        public void Materialize_WhenPathPresentAsFinalHeaderAndBoundaryUsesMixedTerminators_RewritesPathWithoutChangingBody(string lastHeaderTerminator, string boundaryTerminator)
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] body = Encoding.ASCII.GetBytes("body-path-last\r\n\ntrailer");
            byte[] article = BuildArticleWithCustomBoundary(
                [
                    ("Date: Tue, 10 May 2011 13:48:50 -0500", "\r\n"),
                    ("Message-ID: <materialize-mixed-path-last@example.test>", "\r\n"),
                    ("Newsgroups: alt.test", "\r\n"),
                    ("From: user@example.test", "\r\n"),
                    ("Path: upstream.example.test!feed2", lastHeaderTerminator),
                ],
                boundaryTerminator,
                body);

            NntpArticleParseResult parse = parser.Parse(article);
            Assert.True(parse.IsAccepted);
            Assert.Equal($"{LocalFqdn}!upstream.example.test!feed2", parse.CanonicalPath);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string materializedText = Encoding.ASCII.GetString(materialized.Memory.Span);
            Assert.Contains($"Path: {LocalFqdn}!upstream.example.test!feed2{lastHeaderTerminator}", materializedText, StringComparison.Ordinal);
            Assert.DoesNotContain("Path: upstream.example.test!feed2", materializedText, StringComparison.Ordinal);

            NntpArticleParseResult reparsed = parser.Parse(materialized.Memory.ToArray());
            Assert.True(reparsed.IsAccepted);
            Assert.Equal(parse.BodyBytes.ToArray(), reparsed.BodyBytes.ToArray());
        }

        [Theory]
        [InlineData("\n", "\r\n")]
        [InlineData("\r\n", "\r")]
        public void Materialize_WhenDateIsFinalHeaderAndBoundaryUsesMixedTerminators_RewritesDateAndPreservesBody(string lastHeaderTerminator, string boundaryTerminator)
        {
            NntpArticleParser parser = new(LocalFqdn);
            byte[] body = Encoding.ASCII.GetBytes("body-date-last\n\r\nEND");
            byte[] article = BuildArticleWithCustomBoundary(
                [
                    ("Message-ID: <materialize-mixed-date-last@example.test>", "\r\n"),
                    ("Newsgroups: alt.test", "\r\n"),
                    ("From: user@example.test", "\r\n"),
                    ("Date: Tue, 10 May 2011 13:48:50 -0500", lastHeaderTerminator),
                ],
                boundaryTerminator,
                body);

            NntpArticleParseResult parse = parser.Parse(article);
            Assert.True(parse.IsAccepted);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string materializedText = Encoding.ASCII.GetString(materialized.Memory.Span);
            Assert.Contains($"Date: Tue, 10 May 2011 18:48:50 +0000{lastHeaderTerminator}", materializedText, StringComparison.Ordinal);
            Assert.Contains($"Path: {LocalFqdn}{lastHeaderTerminator}", materializedText, StringComparison.Ordinal);

            NntpArticleParseResult reparsed = parser.Parse(materialized.Memory.ToArray());
            Assert.True(reparsed.IsAccepted);
            Assert.Equal(parse.BodyBytes.ToArray(), reparsed.BodyBytes.ToArray());
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

        [Fact]
        public void Materialize_WhenCanonicalDestinationWouldExceedHardArticleBytes_RejectsBeforeRentingDestination()
        {
            NntpArticleParser parser = new(LocalFqdn);
            string[] headers =
            [
                "Date: Tue, 10 May 2011 13:48:50 -0500",
                "Message-ID: <materialize-size-boundary@example.test>",
                "Newsgroups: alt.test",
                "From: user@example.test",
                "Path: b",
            ];

            byte[] baselineArticle = BuildArticle(headers, Array.Empty<byte>());
            NntpArticleParseResult baselineParse = parser.Parse(baselineArticle);
            Assert.True(baselineParse.IsAccepted);

            int canonicalGrowth = (baselineParse.CanonicalUtcDate.Length - baselineParse.OriginalDateValue.Length)
                + (baselineParse.CanonicalPath.Length - baselineParse.OriginalPathValue.Length);
            Assert.True(canonicalGrowth > 0);

            int targetAcceptedLength = ArticleResourceLimits.MaxArticleBytes - canonicalGrowth + 1;
            int bodyLength = targetAcceptedLength - baselineParse.HeaderBytes.Length;
            Assert.True(bodyLength > 0);

            byte[] article = BuildArticle(headers, BuildSafeBody(bodyLength));
            NntpArticleParseResult parse = parser.Parse(article);
            Assert.True(parse.IsAccepted);
            Assert.Equal(targetAcceptedLength, parse.ArticleBytes.Length);

            int expectedCanonicalLength = parse.ArticleBytes.Length
                + parse.CanonicalUtcDate.Length
                - parse.OriginalDateValue.Length
                + parse.CanonicalPath.Length
                - parse.OriginalPathValue.Length;
            Assert.True(expectedCanonicalLength > ArticleResourceLimits.MaxArticleBytes);

            NntpArticleCanonicalBoundaryException ex = Assert.Throws<NntpArticleCanonicalBoundaryException>(() => NntpArticleCanonicalMaterializer.Materialize(parse));
            Assert.Contains("exceeding hard maximum", ex.Message, StringComparison.Ordinal);
        }

        [Fact]
        public void Materialize_WhenCanonicalPathRewriteWouldExceedHardLineBytes_RejectsMaterialization()
        {
            string longFqdn = new string('a', 1017);
            NntpArticleParser parser = new(longFqdn);
            byte[] article = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-line-limit@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: b",
                ],
                "body\r\n");

            NntpArticleParseResult parse = parser.Parse(article);
            Assert.True(parse.IsAccepted);
            Assert.True(parse.CanonicalPath.Length + "Path: ".Length > ArticleResourceLimits.MaxArticleLineBytes);

            NntpArticleCanonicalBoundaryException ex = Assert.Throws<NntpArticleCanonicalBoundaryException>(() => NntpArticleCanonicalMaterializer.Materialize(parse));
            Assert.Contains("Path rewrite", ex.Message, StringComparison.Ordinal);
        }

        [Fact]
        public void Materialize_WhenCanonicalPathInsertionIsExactlyHardBoundary_SucceedsAtBoundary()
        {
            string boundaryFqdn = new string('a', ArticleResourceLimits.MaxArticleLineBytes - "Path: ".Length);
            NntpArticleParser parser = new(boundaryFqdn);
            byte[] article = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-insert-boundary@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                "body\r\n");

            NntpArticleParseResult parse = parser.Parse(article);
            Assert.True(parse.IsAccepted);
            Assert.Equal(boundaryFqdn, parse.CanonicalPath);

            using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
            string headers = GetHeaderText(materialized.Memory.Span);
            string? pathLine = FindHeaderLine(headers, "Path:");
            Assert.NotNull(pathLine);
            Assert.Equal($"Path: {boundaryFqdn}", pathLine);
            Assert.Equal(ArticleResourceLimits.MaxArticleLineBytes, Encoding.ASCII.GetByteCount(pathLine));
        }

        [Fact]
        public void Materialize_WhenRejectedForCanonicalBoundaries_DoesNotDisposeSourcePayloadOwner()
        {
            string longFqdn = new string('a', 1017);
            NntpArticleParser parser = new(longFqdn);
            byte[] article = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-source-owner-preserved@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: b",
                ],
                "body\r\n");

            byte[] rented = ArrayPool<byte>.Shared.Rent(article.Length);
            Buffer.BlockCopy(article, 0, rented, 0, article.Length);
            using DownloadedArticleBuffer sourceOwner = new(rented, article.Length);

            NntpArticleParseResult parse = parser.Parse(sourceOwner.Memory.ToArray());
            Assert.True(parse.IsAccepted);

            _ = Assert.Throws<NntpArticleCanonicalBoundaryException>(() => NntpArticleCanonicalMaterializer.Materialize(parse));

            int firstByte = sourceOwner.Memory.Span[0];
            Assert.Equal((int)(byte)'D', firstByte);
        }

        [Theory]
        [InlineData("\r\n", 1024)]
        [InlineData("\r\n", 1025)]
        [InlineData("\n", 1024)]
        [InlineData("\n", 1025)]
        [InlineData("\r", 1024)]
        [InlineData("\r", 1025)]
        public void Materialize_WhenPathInsertionLineContentAtOrAboveBoundary_EnforcesContentByteLimit(string separator, int pathContentLength)
        {
            string fqdn = new string('a', pathContentLength - "Path: ".Length);
            NntpArticleParser parser = new(fqdn);
            byte[] article = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-insert-exact-line-content-boundary@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                ],
                "body\r\n",
                separator);

            NntpArticleParseResult parse = parser.Parse(article);
            Assert.True(parse.IsAccepted);

            if (pathContentLength == 1024)
            {
                using DownloadedArticleBuffer materialized = NntpArticleCanonicalMaterializer.Materialize(parse);
                string materializedText = Encoding.ASCII.GetString(materialized.Memory.Span);
                string pathLine = ExtractHeaderLine(materializedText, "Path:", separator);
                Assert.Equal(1024, Encoding.ASCII.GetByteCount(pathLine));
                Assert.Equal(separator.Length, DetermineLineTerminatorLength(materializedText, "Path:", separator));
            }
            else
            {
                NntpArticleCanonicalBoundaryException ex = Assert.Throws<NntpArticleCanonicalBoundaryException>(() => NntpArticleCanonicalMaterializer.Materialize(parse));
                Assert.Contains("Path insertion", ex.Message, StringComparison.Ordinal);
            }
        }

        [Fact]
        public void Materialize_WhenPathRewriteLineContentAt1024_SucceedsAnd1025Rejects()
        {
            string localPrefix = "bf";
            int localPrefixLengthWithDelimiter = localPrefix.Length + 1;
            int pathValueAt1024 = 1024 - "Path: ".Length;

            string upstreamAt1024 = new string('x', pathValueAt1024 - localPrefixLengthWithDelimiter);
            string upstreamAt1025 = new string('x', pathValueAt1024 - localPrefixLengthWithDelimiter + 1);

            NntpArticleParser parser = new(localPrefix);

            byte[] acceptedArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-rewrite-line-content-1024@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    $"Path: {upstreamAt1024}",
                ],
                "body\r\n");
            NntpArticleParseResult acceptedParse = parser.Parse(acceptedArticle);
            Assert.True(acceptedParse.IsAccepted);

            using DownloadedArticleBuffer accepted = NntpArticleCanonicalMaterializer.Materialize(acceptedParse);
            string acceptedHeaders = GetHeaderText(accepted.Memory.Span);
            string? acceptedPathLine = FindHeaderLine(acceptedHeaders, "Path:");
            Assert.NotNull(acceptedPathLine);
            Assert.Equal(1024, Encoding.ASCII.GetByteCount(acceptedPathLine));

            byte[] rejectedArticle = BuildArticle(
                [
                    "Date: Tue, 10 May 2011 13:48:50 -0500",
                    "Message-ID: <materialize-path-rewrite-line-content-1025@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    $"Path: {upstreamAt1025}",
                ],
                "body\r\n");
            NntpArticleParseResult rejectedParse = parser.Parse(rejectedArticle);
            Assert.True(rejectedParse.IsAccepted);

            NntpArticleCanonicalBoundaryException ex = Assert.Throws<NntpArticleCanonicalBoundaryException>(() => NntpArticleCanonicalMaterializer.Materialize(rejectedParse));
            Assert.Contains("Path rewrite", ex.Message, StringComparison.Ordinal);
        }

        [Fact]
        public void Materialize_WhenDateRewriteAt1024LineContentBoundary_SucceedsAnd1025Rejects()
        {
            string dateHeaderName = "Injection-Date";
            const string shortDateValue = "Wed, 1 Jun 2011 13:48:50 -0500";
            string canonicalDate = "Wed, 01 Jun 2011 18:48:50 +0000";
            int paddingForAccepted = 1023 - (dateHeaderName.Length + 2 + shortDateValue.Length);
            int paddingForRejected = 1024 - (dateHeaderName.Length + 2 + shortDateValue.Length);

            string BuildDateValue(int leadingPadding) => $"{new string(' ', leadingPadding)}{shortDateValue}";

            NntpArticleParser parser = new(LocalFqdn, NntpArticleParserOptions.Default with { MaxHeaderValueBytes = 200_000 });

            byte[] acceptedArticle = BuildArticle(
                [
                    $"{dateHeaderName}: {BuildDateValue(paddingForAccepted)}",
                    "Message-ID: <materialize-date-rewrite-line-content-1024@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: x",
                ],
                "body\r\n");
            NntpArticleParseResult acceptedParse = parser.Parse(acceptedArticle);
            Assert.True(acceptedParse.IsAccepted);
            Assert.Equal(canonicalDate, acceptedParse.CanonicalUtcDate);

            using DownloadedArticleBuffer accepted = NntpArticleCanonicalMaterializer.Materialize(acceptedParse);
            string acceptedHeaders = GetHeaderText(accepted.Memory.Span);
            string? acceptedDateLine = FindHeaderLine(acceptedHeaders, $"{dateHeaderName}:");
            Assert.NotNull(acceptedDateLine);
            Assert.Equal(1024, Encoding.ASCII.GetByteCount(acceptedDateLine));

            byte[] rejectedArticle = BuildArticle(
                [
                    $"{dateHeaderName}: {BuildDateValue(paddingForRejected)}",
                    "Message-ID: <materialize-date-rewrite-line-content-1025@example.test>",
                    "Newsgroups: alt.test",
                    "From: user@example.test",
                    "Path: x",
                ],
                "body\r\n");
            NntpArticleParseResult rejectedParse = parser.Parse(rejectedArticle);
            Assert.True(rejectedParse.IsAccepted);
            Assert.Equal(canonicalDate, rejectedParse.CanonicalUtcDate);

            NntpArticleCanonicalBoundaryException ex = Assert.Throws<NntpArticleCanonicalBoundaryException>(() => NntpArticleCanonicalMaterializer.Materialize(rejectedParse));
            Assert.Contains("date rewrite", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        private static byte[] BuildArticle(IReadOnlyList<string> headers, string body)
        {
            return BuildArticle(headers, Encoding.ASCII.GetBytes(body));
        }

        private static byte[] BuildArticleWithCustomBoundary(IReadOnlyList<(string Header, string Terminator)> headerLines, string boundaryTerminator, byte[] body)
        {
            StringBuilder builder = new();
            for (int i = 0; i < headerLines.Count; i++)
            {
                _ = builder.Append(headerLines[i].Header).Append(headerLines[i].Terminator);
            }

            _ = builder.Append(boundaryTerminator);
            byte[] headerBytes = Encoding.ASCII.GetBytes(builder.ToString());
            byte[] article = new byte[headerBytes.Length + body.Length];
            Buffer.BlockCopy(headerBytes, 0, article, 0, headerBytes.Length);
            Buffer.BlockCopy(body, 0, article, headerBytes.Length, body.Length);
            return article;
        }

        private static byte[] BuildSafeBody(int length)
        {
            if (length <= 0)
            {
                return Array.Empty<byte>();
            }

            byte[] body = new byte[length];
            Array.Fill(body, (byte)'A');

            for (int i = 100; i + 1 < body.Length; i += 102)
            {
                body[i] = (byte)'\r';
                body[i + 1] = (byte)'\n';
            }

            return body;
        }

        private static byte[] BuildArticle(IReadOnlyList<string> headers, string body, string separator)
        {
            return BuildArticle(headers, Encoding.ASCII.GetBytes(body), separator);
        }

        private static byte[] BuildArticle(IReadOnlyList<string> headers, byte[] body)
        {
            return BuildArticle(headers, body, "\r\n");
        }

        private static byte[] BuildArticle(IReadOnlyList<string> headers, byte[] body, string separator)
        {
            StringBuilder builder = new();
            for (int i = 0; i < headers.Count; i++)
            {
                _ = builder.Append(headers[i]).Append(separator);
            }

            _ = builder.Append(separator);
            byte[] headerBytes = Encoding.ASCII.GetBytes(builder.ToString());
            byte[] article = new byte[headerBytes.Length + body.Length];
            Buffer.BlockCopy(headerBytes, 0, article, 0, headerBytes.Length);
            Buffer.BlockCopy(body, 0, article, headerBytes.Length, body.Length);
            return article;
        }

        private static string GetHeaderText(ReadOnlySpan<byte> article)
        {
            int headerEnd = FindHeaderSeparator(article, out _);
            return Encoding.ASCII.GetString(article[..headerEnd]);
        }

        private static byte[] GetBodyBytes(ReadOnlySpan<byte> article)
        {
            return GetBodyBytes(article, "\r\n");
        }

        private static byte[] GetBodyBytes(ReadOnlySpan<byte> article, string separator)
        {
            int headerEnd = FindHeaderSeparator(article, out int separatorLength);
            if (separatorLength != separator.Length)
            {
                throw new InvalidOperationException($"Expected separator length {separator.Length} but found {separatorLength}.");
            }

            return article[(headerEnd + separatorLength + separatorLength)..].ToArray();
        }

        private static int FindHeaderSeparator(ReadOnlySpan<byte> article, out int separatorLength)
        {
            for (int i = 0; i <= article.Length - 4; i++)
            {
                if (article[i] == (byte)'\r'
                    && article[i + 1] == (byte)'\n'
                    && article[i + 2] == (byte)'\r'
                    && article[i + 3] == (byte)'\n')
                {
                    separatorLength = 2;
                    return i;
                }
            }

            for (int i = 0; i <= article.Length - 2; i++)
            {
                if (article[i] == (byte)'\n' && article[i + 1] == (byte)'\n')
                {
                    separatorLength = 1;
                    return i;
                }

                if (article[i] == (byte)'\r' && article[i + 1] == (byte)'\r')
                {
                    separatorLength = 1;
                    return i;
                }
            }

            throw new InvalidOperationException("Article did not contain an accepted header separator.");
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

        private static string? FindHeaderLine(string headers, string prefix)
        {
            string[] lines = headers.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < lines.Length; i++)
            {
                if (lines[i].StartsWith(prefix, StringComparison.Ordinal))
                {
                    return lines[i];
                }
            }

            return null;
        }

        private static string ExtractHeaderLine(string materializedArticle, string prefix, string separator)
        {
            string[] lines = materializedArticle.Split(separator, StringSplitOptions.None);
            for (int i = 0; i < lines.Length; i++)
            {
                if (lines[i].StartsWith(prefix, StringComparison.Ordinal))
                {
                    return lines[i];
                }
            }

            throw new InvalidOperationException($"Header line with prefix '{prefix}' was not found.");
        }

        private static int DetermineLineTerminatorLength(string materializedArticle, string prefix, string separator)
        {
            int index = materializedArticle.IndexOf(prefix, StringComparison.Ordinal);
            if (index < 0)
            {
                throw new InvalidOperationException($"Header line with prefix '{prefix}' was not found.");
            }

            int lineEnd = materializedArticle.IndexOf(separator, index, StringComparison.Ordinal);
            if (lineEnd < 0)
            {
                throw new InvalidOperationException($"Header line with prefix '{prefix}' did not contain separator '{separator}'.");
            }

            return separator.Length;
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
