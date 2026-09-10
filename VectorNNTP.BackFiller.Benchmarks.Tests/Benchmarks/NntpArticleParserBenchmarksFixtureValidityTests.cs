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

        private static readonly byte[] SinglePartDecodedPayload = [0x00, 0x13, 0xD6, 0xE0, 0xE3, 0x56];
        private static readonly byte[] SinglePartEncodedPayloadLine = [(byte)'*', (byte)'=', (byte)'}', (byte)'=', (byte)'@', (byte)'=', (byte)'J', (byte)'=', (byte)'M', 0x80, (byte)'\r', (byte)'\n'];
        private const string SinglePartExpectedCrc32 = "cb042efa";

        private static readonly byte[] MultiPartDecodedPayload = [0x41, 0x00, 0x13, 0xD6, 0xE0, 0xE3, 0x56, 0xFF];
        private static readonly byte[] MultiPartEncodedPayloadLine = [(byte)'k', (byte)'*', (byte)'=', (byte)'}', (byte)'=', (byte)'@', (byte)'=', (byte)'J', (byte)'=', (byte)'M', 0x80, (byte)')', (byte)'\r', (byte)'\n'];
        private const string MultiPartExpectedPcrc32 = "bfdadd48";

        [Fact]
        public void Setup_WhenSinglePartFixtureParsedByProductionParser_IsAcceptedAsValidYEnc()
        {
            byte[] independentBody = BuildIndependentSinglePartBody();
            byte[] article = CreateAndGetSetupFixture("_yencSingle", independentBody, null);

            AssertSetupFixtureUsesIndependentBody(article, independentBody, YEncArticleValidationStatus.ValidSinglePart);

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
            byte[] independentBody = BuildIndependentMultiPartBody();
            byte[] article = CreateAndGetSetupFixture("_yencMultipart", null, independentBody);

            AssertSetupFixtureUsesIndependentBody(article, independentBody, YEncArticleValidationStatus.ValidMultiPart);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult parseResult = parser.Parse(article);

            Assert.True(parseResult.IsAccepted);
            Assert.True(parseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, parseResult.ArticleType);
            Assert.Equal(YEncArticleValidationStatus.ValidMultiPart, parseResult.YEncValidation.Status);
            Assert.False(parseResult.YEncValidation.ShouldTreatAsYEncDecodingFailed);
        }

        [Fact]
        public void Setup_WhenDefaultFixturesAreGenerated_YEncFixturesRemainValidAndAsciiRoundTripChangesValidationOutcome()
        {
            NntpArticleParserBenchmarks benchmark = new();
            benchmark.Setup();

            byte[] singlePartArticle = GetSetupFixture(benchmark, "_yencSingle");
            byte[] multiPartArticle = GetSetupFixture(benchmark, "_yencMultipart");

            AssertDefaultGeneratedFixtureExhibitsB07RegressionMode(singlePartArticle, YEncArticleValidationStatus.ValidSinglePart);
            AssertDefaultGeneratedFixtureExhibitsB07RegressionMode(multiPartArticle, YEncArticleValidationStatus.ValidMultiPart);
        }

        private static byte[] CreateAndGetSetupFixture(string fieldName, byte[]? singlePartBodyOverride, byte[]? multiPartBodyOverride)
        {
            NntpArticleParserBenchmarks benchmark = new(singlePartBodyOverride, multiPartBodyOverride);
            benchmark.Setup();

            return GetSetupFixture(benchmark, fieldName);
        }

        private static byte[] GetSetupFixture(NntpArticleParserBenchmarks benchmark, string fieldName)
        {
            ArgumentNullException.ThrowIfNull(benchmark);

            FieldInfo field = typeof(NntpArticleParserBenchmarks).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException($"Benchmark fixture field '{fieldName}' was not found.");

            return field.GetValue(benchmark) as byte[]
                ?? throw new InvalidOperationException($"Benchmark fixture field '{fieldName}' was not initialized as bytes by Setup().");
        }

        private static byte[] BuildIndependentSinglePartBody()
        {
            Assert.Equal(6, SinglePartDecodedPayload.Length);

            return
            [
                ..Ascii("=ybegin line=128 size=6 name=single.bin\r\n"),
                ..SinglePartEncodedPayloadLine,
                ..Ascii($"=yend size=6 crc32={SinglePartExpectedCrc32}\r\n"),
            ];
        }

        private static byte[] BuildIndependentMultiPartBody()
        {
            Assert.Equal(8, MultiPartDecodedPayload.Length);

            return
            [
                ..Ascii("=ybegin part=1 line=128 size=8 name=multi.bin\r\n=ypart begin=1 end=8\r\n"),
                ..MultiPartEncodedPayloadLine,
                ..Ascii($"=yend size=8 pcrc32={MultiPartExpectedPcrc32}\r\n"),
            ];
        }

        private static void AssertSetupFixtureUsesIndependentBody(byte[] article, byte[] independentBody, YEncArticleValidationStatus expectedStatus)
        {
            ArgumentNullException.ThrowIfNull(article);
            ArgumentNullException.ThrowIfNull(independentBody);

            int bodyOffset = FindBodyOffset(article);
            Assert.InRange(bodyOffset, 1, article.Length);

            ReadOnlySpan<byte> setupBody = article.AsSpan(bodyOffset);
            Assert.Equal(independentBody.Length, setupBody.Length);
            Assert.True(setupBody.SequenceEqual(independentBody), "Setup fixture body does not match the independently established yEnc vector bytes.");

            AssertLossyAsciiRoundTripChangesValidationOutcome(article, expectedStatus, bodyOffset, setupBody);
        }

        private static void AssertDefaultGeneratedFixtureExhibitsB07RegressionMode(byte[] article, YEncArticleValidationStatus expectedStatus)
        {
            ArgumentNullException.ThrowIfNull(article);

            int bodyOffset = FindBodyOffset(article);
            Assert.InRange(bodyOffset, 1, article.Length);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult parseResult = parser.Parse(article);

            Assert.True(parseResult.IsAccepted);
            Assert.True(parseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, parseResult.ArticleType);
            Assert.Equal(expectedStatus, parseResult.YEncValidation.Status);
            Assert.False(parseResult.YEncValidation.ShouldTreatAsYEncDecodingFailed);

            ReadOnlySpan<byte> setupBody = article.AsSpan(bodyOffset);
            AssertLossyAsciiRoundTripChangesValidationOutcome(article, expectedStatus, bodyOffset, setupBody);
        }

        private static void AssertLossyAsciiRoundTripChangesValidationOutcome(
            byte[] article,
            YEncArticleValidationStatus expectedStatus,
            int bodyOffset,
            ReadOnlySpan<byte> setupBody)
        {
            byte[] asciiRoundTrippedBody = Encoding.ASCII.GetBytes(Encoding.ASCII.GetString(setupBody));
            Assert.False(setupBody.SequenceEqual(asciiRoundTrippedBody), "Setup fixture body unexpectedly survives ASCII round-trip unchanged; expected preserved non-ASCII yEnc bytes.");

            byte[] corruptedArticle = new byte[article.Length];
            Buffer.BlockCopy(article, 0, corruptedArticle, 0, bodyOffset);
            Buffer.BlockCopy(asciiRoundTrippedBody, 0, corruptedArticle, bodyOffset, asciiRoundTrippedBody.Length);

            NntpArticleParser parser = new(LocalFqdn);
            NntpArticleParseResult corruptedParseResult = parser.Parse(corruptedArticle);

            Assert.True(corruptedParseResult.YEncDetected);
            Assert.Equal(NntpArticleType.YEnc, corruptedParseResult.ArticleType);
            Assert.NotEqual(expectedStatus, corruptedParseResult.YEncValidation.Status);
        }

        private static byte[] Ascii(string text)
        {
            return Encoding.ASCII.GetBytes(text);
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
