// <copyright file="NntpArticleParserBenchmarksFixtureValidityTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Regression tests proving parser benchmark yEnc fixtures remain valid through the production parser/validator path.

using System.Reflection;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Verifies yEnc fixture validity for parser benchmarks against production parsing and yEnc validation contracts.
    /// </summary>
    public sealed class NntpArticleParserBenchmarksFixtureValidityTests
    {
        private const string LocalFqdn = "bf01.usenet.ninja";

        [Fact]
        public void BuildSyntheticSinglePartYEnc_WhenParsedByProductionParser_IsAcceptedAsValidYEnc()
        {
            byte[] body = InvokeBuildSyntheticSinglePartYEnc(payloadLength: 4096, name: "single.bin");
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-yenc-single@example.test>",
                    "Newsgroups: alt.binaries.test",
                    "From: user@example.test",
                ],
                bodyBytes: body);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult parseResult = parser.Parse(article);

            Assert.True(parseResult.IsAccepted);
            Assert.True(parseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, parseResult.ArticleType);
            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, parseResult.YEncValidation.Status);
            Assert.False(parseResult.YEncValidation.ShouldTreatAsYEncDecodingFailed);
        }

        [Fact]
        public void BuildSyntheticMultiPartYEnc_WhenParsedByProductionParser_IsAcceptedAsValidYEnc()
        {
            byte[] body = InvokeBuildSyntheticMultiPartYEnc(payloadLength: 8192, name: "multi.bin", partIndex: 1);
            byte[] article = BuildArticle(
                headers:
                [
                    "Date: Fri, 23 Aug 2024 07:30:10 +0000",
                    "Message-ID: <bench-yenc-multipart@example.test>",
                    "Newsgroups: alt.binaries.test",
                    "From: user@example.test",
                ],
                bodyBytes: body);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult parseResult = parser.Parse(article);

            Assert.True(parseResult.IsAccepted);
            Assert.True(parseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, parseResult.ArticleType);
            Assert.Equal(YEncArticleValidationStatus.ValidMultiPart, parseResult.YEncValidation.Status);
            Assert.False(parseResult.YEncValidation.ShouldTreatAsYEncDecodingFailed);
        }

        private static byte[] InvokeBuildSyntheticSinglePartYEnc(int payloadLength, string name)
        {
            MethodInfo method = typeof(NntpArticleParserBenchmarks).GetMethod(
                "BuildSyntheticSinglePartYEnc",
                BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException("BuildSyntheticSinglePartYEnc helper not found.");

            return (byte[])method.Invoke(null, [payloadLength, name])!;
        }

        private static byte[] InvokeBuildSyntheticMultiPartYEnc(int payloadLength, string name, int partIndex)
        {
            MethodInfo method = typeof(NntpArticleParserBenchmarks).GetMethod(
                "BuildSyntheticMultiPartYEnc",
                BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException("BuildSyntheticMultiPartYEnc helper not found.");

            return (byte[])method.Invoke(null, [payloadLength, name, partIndex])!;
        }

        private static byte[] BuildArticle(IEnumerable<string> headers, byte[] bodyBytes)
        {
            ArgumentNullException.ThrowIfNull(headers);
            ArgumentNullException.ThrowIfNull(bodyBytes);

            string prefix = string.Join("\r\n", headers) + "\r\n\r\n";
            byte[] headerBytes = System.Text.Encoding.ASCII.GetBytes(prefix);

            byte[] article = new byte[headerBytes.Length + bodyBytes.Length];
            Buffer.BlockCopy(headerBytes, 0, article, 0, headerBytes.Length);
            Buffer.BlockCopy(bodyBytes, 0, article, headerBytes.Length, bodyBytes.Length);
            return article;
        }
    }
}
