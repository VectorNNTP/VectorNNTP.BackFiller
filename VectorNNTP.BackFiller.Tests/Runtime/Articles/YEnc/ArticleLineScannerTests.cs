// <copyright file="ArticleLineScannerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for article line scanner, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the article line scanner test suite.

using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.YEnc
{
    /// <summary>
    /// Verifies line terminator scanning semantics and SIMD/scalar equivalence for article line detection.
    /// </summary>
    public sealed class ArticleLineScannerTests
    {
        /// <summary>
        /// Verifies CRLF, LF, and CR-not-followed-by-LF behavior at representative offsets.
        /// </summary>
        [Theory]
        [InlineData("abc\r\ndef", 0, 3)]
        [InlineData("abc\ndef", 0, 3)]
        [InlineData("abc\rdef\nxyz", 0, 7)]
        [InlineData("\nxyz", 0, 0)]
        [InlineData("\r\nxyz", 0, 0)]
        [InlineData("abcdef", 0, -1)]
        public void IndexOfCrLf_WhenScanningMixedTerminators_ReturnsExpectedIndex(string text, int startOffset, int expected)
        {
            int index = ArticleLineScanner.IndexOfCrLf(System.Text.Encoding.ASCII.GetBytes(text), startOffset);
            Assert.Equal(expected, index);
        }

        /// <summary>
        /// Verifies scanning behavior around vector-size boundaries and middle-of-buffer starts.
        /// </summary>
        [Fact]
        public void IndexOfCrLf_WhenCrossingVectorBoundaries_MatchesExpectedResult()
        {
            byte[] buffer = System.Text.Encoding.ASCII.GetBytes(new string('a', 15) + "\r\n" + new string('b', 17) + "\n" + "tail");

            Assert.Equal(15, ArticleLineScanner.IndexOfCrLf(buffer, 0));
            Assert.Equal(34, ArticleLineScanner.IndexOfCrLf(buffer, 17));
            Assert.Equal(34, ArticleLineScanner.IndexOfCrLf(buffer, 33));
            Assert.Equal(-1, ArticleLineScanner.IndexOfCrLf(buffer, buffer.Length));
        }

        /// <summary>
        /// Verifies advancing past line terminators for CRLF, LF-only, and out-of-range indexes.
        /// </summary>
        [Fact]
        public void AdvancePastLineTerminator_WhenCalledWithDifferentTerminators_ReturnsExpectedOffset()
        {
            byte[] buffer = "a\r\nb\nc"u8.ToArray();

            Assert.Equal(3, ArticleLineScanner.AdvancePastLineTerminator(buffer, 1));
            Assert.Equal(5, ArticleLineScanner.AdvancePastLineTerminator(buffer, 4));
            Assert.Equal(buffer.Length, ArticleLineScanner.AdvancePastLineTerminator(buffer, 100));
        }

        /// <summary>
        /// Verifies prefix matching at line starts and non-matching when prefix appears inside line content.
        /// </summary>
        [Fact]
        public void FindLineStartingWith_WhenSearchingForAnchoredPrefix_OnlyMatchesLineStart()
        {
            byte[] buffer = "x=yend size=1 crc32=1\r\n=yend size=2 crc32=2\r\n"u8.ToArray();
            int match = ArticleLineScanner.FindLineStartingWith(buffer, 0, "=yend "u8);

            Assert.True(match > 0);
            Assert.Equal((byte)'=', buffer[match]);
            Assert.Equal(-1, ArticleLineScanner.FindLineStartingWith(buffer, match + 1, "=ybegin "u8));
        }

        /// <summary>
        /// Verifies SIMD scanner results match scalar reference logic for boundary-heavy buffers.
        /// </summary>
        [Fact]
        public void IndexOfCrLf_WhenComparedToScalarReference_ProducesIdenticalResults()
        {
            byte[] buffer = BuildBoundaryHeavyBuffer();

            for (int start = 0; start < buffer.Length + 3; start++)
            {
                int simd = ArticleLineScanner.IndexOfCrLf(buffer, start);
                int scalar = IndexOfCrLfScalarReference(buffer, start);
                Assert.Equal(scalar, simd);
            }
        }

        /// <summary>
        /// Verifies deterministic randomized parity between SIMD and scalar reference implementations over thousands of inputs.
        /// </summary>
        [Fact]
        public void IndexOfCrLf_WhenRandomizedAcrossThousandsOfInputs_MatchesScalarReference()
        {
            Random random = new(20260825);

            for (int caseIndex = 0; caseIndex < 4096; caseIndex++)
            {
                int length = random.Next(0, 1024);
                byte[] buffer = new byte[length];

                for (int i = 0; i < length; i++)
                {
                    int selector = random.Next(0, 16);
                    buffer[i] = selector switch
                    {
                        0 => (byte)'\r',
                        1 => (byte)'\n',
                        2 => (byte)'.',
                        3 => (byte)'=',
                        _ => (byte)random.Next(32, 127),
                    };
                }

                for (int startOffset = 0; startOffset < length + 2; startOffset++)
                {
                    int simd = ArticleLineScanner.IndexOfCrLf(buffer, startOffset);
                    int scalar = IndexOfCrLfScalarReference(buffer, startOffset);
                    Assert.Equal(scalar, simd);
                }
            }
        }

        /// <summary>
        /// Implements the expected scalar line-terminator behavior for parity validation.
        /// </summary>
        /// <param name="buffer">Input buffer to scan.</param>
        /// <param name="startOffset">Scan start offset.</param>
        /// <returns>Detected terminator index or -1 when no terminator is found.</returns>
        /// <summary>
        /// Confirms the index of cr lf scalar reference behavior.
        /// </summary>
        /// <returns>The value returned by the index of cr lf scalar reference helper.</returns>
        private static int IndexOfCrLfScalarReference(ReadOnlySpan<byte> buffer, int startOffset)
        {
            if ((uint)startOffset >= (uint)buffer.Length)
            {
                return -1;
            }

            for (int i = startOffset; i < buffer.Length; i++)
            {
                if (buffer[i] == (byte)'\r' && i + 1 < buffer.Length && buffer[i + 1] == (byte)'\n')
                {
                    return i;
                }

                if (buffer[i] == (byte)'\n' && (i == startOffset || buffer[i - 1] != (byte)'\r'))
                {
                    return i;
                }
            }

            return -1;
        }

        /// <summary>
        /// Builds deterministic input that stresses CR/LF patterns across SIMD block boundaries.
        /// </summary>
        /// <returns>Boundary-focused test buffer.</returns>
        /// <summary>
        /// Confirms the build boundary heavy buffer behavior.
        /// </summary>
        /// <returns>The value returned by the build boundary heavy buffer helper.</returns>
        private static byte[] BuildBoundaryHeavyBuffer()
        {
            List<byte> data = new(768);
            for (int i = 0; i < 384; i++)
            {
                data.Add((byte)('a' + (i % 26)));
                if (i % 16 == 15)
                {
                    data.Add((byte)'\r');
                    data.Add((byte)'\n');
                }
                else if (i % 17 == 0)
                {
                    data.Add((byte)'\n');
                }
                else if (i % 31 == 0)
                {
                    data.Add((byte)'\r');
                }
            }

            if (data[^1] != (byte)'\n')
            {
                data.Add((byte)'\n');
            }

            return [.. data];
        }
    }
}
