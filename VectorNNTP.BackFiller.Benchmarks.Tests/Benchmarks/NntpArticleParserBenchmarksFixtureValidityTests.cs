// <copyright file="NntpArticleParserBenchmarksFixtureValidityTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Regression tests proving parser benchmark yEnc fixtures remain valid through the production parser/validator path.

using System.Reflection;
using System.Text;
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
        public void Setup_WhenSinglePartFixtureParsedByProductionParser_IsAcceptedAsValidYEnc()
        {
            byte[] article = CreateAndGetSetupFixture("_yencSingle");
            byte[] expectedBody = InvokeBuildSyntheticSinglePartYEnc(payloadLength: 4096, name: "single.bin");

            AssertSetupFixtureUsesBytePreservingBody(article, expectedBody, YEncArticleValidationStatus.ValidSinglePart);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult parseResult = parser.Parse(article);

            Assert.True(parseResult.IsAccepted);
            Assert.True(parseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, parseResult.ArticleType);
            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, parseResult.YEncValidation.Status);
            Assert.False(parseResult.YEncValidation.ShouldTreatAsYEncDecodingFailed);
        }

        [Fact]
        public void Setup_WhenMultiPartFixtureParsedByProductionParser_IsAcceptedAsValidYEnc()
        {
            byte[] article = CreateAndGetSetupFixture("_yencMultipart");
            byte[] expectedBody = InvokeBuildSyntheticMultiPartYEnc(payloadLength: 8192, name: "multi.bin", partIndex: 1);

            AssertSetupFixtureUsesBytePreservingBody(article, expectedBody, YEncArticleValidationStatus.ValidMultiPart);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult parseResult = parser.Parse(article);

            Assert.True(parseResult.IsAccepted);
            Assert.True(parseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, parseResult.ArticleType);
            Assert.Equal(YEncArticleValidationStatus.ValidMultiPart, parseResult.YEncValidation.Status);
            Assert.False(parseResult.YEncValidation.ShouldTreatAsYEncDecodingFailed);
        }

        private static byte[] CreateAndGetSetupFixture(string fieldName)
        {
            NntpArticleParserBenchmarks benchmark = new();
            benchmark.Setup();

            FieldInfo field = typeof(NntpArticleParserBenchmarks).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException($"Benchmark fixture field '{fieldName}' was not found.");

            return field.GetValue(benchmark) as byte[]
                ?? throw new InvalidOperationException($"Benchmark fixture field '{fieldName}' was not initialized as bytes by Setup().");
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

        private static void AssertSetupFixtureUsesBytePreservingBody(byte[] article, byte[] expectedBody, YEncArticleValidationStatus expectedStatus)
        {
            ArgumentNullException.ThrowIfNull(article);
            ArgumentNullException.ThrowIfNull(expectedBody);

            int bodyOffset = FindBodyOffset(article);
            Assert.InRange(bodyOffset, 1, article.Length);

            ReadOnlySpan<byte> setupBody = article.AsSpan(bodyOffset);
            Assert.Equal(expectedBody.Length, setupBody.Length);
            Assert.True(setupBody.SequenceEqual(expectedBody), "Setup fixture body does not match the benchmark encoder output bytes. The byte-preserving fixture boundary regressed.");

            byte[] asciiRoundTrippedBody = Encoding.ASCII.GetBytes(Encoding.ASCII.GetString(setupBody));
            Assert.False(setupBody.SequenceEqual(asciiRoundTrippedBody), "Setup fixture body unexpectedly survives ASCII round-trip unchanged; expected non-ASCII yEnc payload bytes to be preserved.");

            byte[] corruptedArticle = new byte[article.Length];
            Buffer.BlockCopy(article, 0, corruptedArticle, 0, bodyOffset);
            Buffer.BlockCopy(asciiRoundTrippedBody, 0, corruptedArticle, bodyOffset, asciiRoundTrippedBody.Length);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult corruptedParseResult = parser.Parse(corruptedArticle);

            Assert.True(corruptedParseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, corruptedParseResult.ArticleType);
            Assert.NotEqual(expectedStatus, corruptedParseResult.YEncValidation.Status);
        }

        private static int FindBodyOffset(byte[] article)
        {
            for (int i = 0; i <= article.Length - 4; i++)
            {
                if (article[i] == (byte)'\r' &&
                    article[i + 1] == (byte)'\n' &&
                    article[i + 2] == (byte)'\r' &&
                    article[i + 3] == (byte)'\n')
                {
                    return i + 4;
                }
            }

            throw new InvalidOperationException("Article fixture does not contain an NNTP header/body separator.");
        }
    }
}
