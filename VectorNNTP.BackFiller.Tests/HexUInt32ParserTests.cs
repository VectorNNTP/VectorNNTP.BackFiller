// <copyright file="HexUInt32ParserTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for hex u int32 parser.

using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies strict hexadecimal parsing behavior for yEnc CRC metadata values.
    /// </summary>
    public sealed class HexUInt32ParserTests
    {
        /// <summary>
        /// Verifies that one-to-eight hexadecimal digits parse successfully.
        /// </summary>
        [Theory]
        [InlineData("1", 0x1u)]
        [InlineData("ABCDEF12", 0xABCDEF12u)]
        [InlineData("abcdef12", 0xABCDEF12u)]
        [InlineData("00000000", 0x00000000u)]
        public void TryParseHexUInt32_WhenInputIsStrictHex_ReturnsExpectedValue(string input, uint expected)
        {
            bool parsed = HexUInt32Parser.TryParseHexUInt32(System.Text.Encoding.ASCII.GetBytes(input), out uint value);

            Assert.True(parsed);
            Assert.Equal(expected, value);
        }

        /// <summary>
        /// Verifies that empty, oversized, or partially hexadecimal values are rejected.
        /// </summary>
        [Theory]
        [InlineData("")]
        [InlineData("123456789")]
        [InlineData("ABCDEF12G")]
        [InlineData("12345678XYZ")]
        [InlineData("-1")]
        [InlineData("+")]
        [InlineData("G")]
        [InlineData(" ")]
        [InlineData("12 34")]
        [InlineData("12_34")]
        [InlineData("１２")]
        public void TryParseHexUInt32_WhenInputContainsGarbageOrIsOutOfBounds_ReturnsFalse(string input)
        {
            bool parsed = HexUInt32Parser.TryParseHexUInt32(System.Text.Encoding.ASCII.GetBytes(input), out uint value);

            Assert.False(parsed);
            Assert.Equal(0u, value);
        }
    }
}
