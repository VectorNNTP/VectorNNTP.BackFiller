// <copyright file="TransitProtocolParserTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit protocol parser, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit protocol parser test suite.

using System.IO.Pipelines;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Transit
{
    /// <summary>
    /// Tests NNTP protocol parsing semantics for greeting and CAPABILITIES handling.
    /// </summary>
    public sealed class TransitProtocolParserTests
    {
        /// <summary>
        /// Confirms the read nntp line async when cr lf line returns line without cr lf behavior.
        /// </summary>
        [Fact]
        public async Task ReadNntpLineAsync_WhenCrLfLine_ReturnsLineWithoutCrLf()
        {
            Pipe pipe = new();
            _ = await pipe.Writer.WriteAsync(Encoding.ASCII.GetBytes("200 transit ready\r\n"));

            string line = await TransitProtocolParser.ReadNntpLineAsync(pipe.Reader, CancellationToken.None);

            Assert.Equal("200 transit ready", line);
        }
        /// <summary>
        /// Confirms the validate greeting when200 or201 does not throw behavior.
        /// </summary>
        [Fact]
        public void ValidateGreeting_When200Or201_DoesNotThrow()
        {
            TransitProtocolParser.ValidateGreeting("200 transit posting allowed");
            TransitProtocolParser.ValidateGreeting("201 transit no posting");
        }
        /// <summary>
        /// Confirms the validate greeting when unexpected throws behavior.
        /// </summary>
        [Fact]
        public void ValidateGreeting_WhenUnexpected_Throws()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => TransitProtocolParser.ValidateGreeting("400 temporary failure"));

            Assert.Contains("Unexpected NNTP greeting response code", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the parse capabilities response when start tls compress streaming present ignores compression and detects supported features behavior.
        /// </summary>
        [Fact]
        public void ParseCapabilitiesResponse_WhenStartTlsCompressStreamingPresent_IgnoresCompressionAndDetectsSupportedFeatures()
        {
            string[] lines =
            [
                "101 Capability list:",
                "VERSION 2",
                "STARTTLS",
                "COMPRESS DEFLATE",
                "STREAMING",
                ".",
            ];

            TransitCapabilitySnapshot snapshot = TransitProtocolParser.ParseCapabilitiesResponse(lines);

            Assert.True(snapshot.SupportsStartTls);
            Assert.True(snapshot.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the parse capabilities response when malformed throws behavior.
        /// </summary>
        [Fact]
        public void ParseCapabilitiesResponse_WhenMalformed_Throws()
        {
            string[] lines =
            [
                "101 Capability list:",
                "VERSION 2",
                "STREAMING",
            ];

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => TransitProtocolParser.ParseCapabilitiesResponse(lines));

            Assert.Contains("missing multiline terminator", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Confirms the parse status line when valid parses code text and tokens behavior.
        /// </summary>
        [Fact]
        public void ParseStatusLine_WhenValid_ParsesCodeTextAndTokens()
        {
            (int code, string text, string[] tokens) = TransitProtocolParser.ParseStatusLine("203 streaming allowed");

            Assert.Equal(203, code);
            Assert.Equal("streaming allowed", text);
            Assert.Equal(["streaming", "allowed"], tokens);
        }
        /// <summary>
        /// Confirms the parse status line when missing separator after code throws behavior.
        /// </summary>
        [Fact]
        public void ParseStatusLine_WhenMissingSeparatorAfterCode_Throws()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => TransitProtocolParser.ParseStatusLine("239<id> transferred"));

            Assert.Contains("Malformed NNTP response", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Confirms the parse capabilities response when mixed case and parameters detects supported features behavior.
        /// </summary>
        [Fact]
        public void ParseCapabilitiesResponse_WhenMixedCaseAndParameters_DetectsSupportedFeatures()
        {
            string[] lines =
            [
                "101 Capability list:",
                "version 2",
                "starttls",
                "streaming posting",
                "x-feature custom",
                ".",
            ];

            TransitCapabilitySnapshot snapshot = TransitProtocolParser.ParseCapabilitiesResponse(lines);

            Assert.True(snapshot.SupportsStartTls);
            Assert.True(snapshot.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the parse capabilities response when stream token present detects streaming support behavior.
        /// </summary>
        [Fact]
        public void ParseCapabilitiesResponse_WhenStreamTokenPresent_DetectsStreamingSupport()
        {
            string[] lines =
            [
                "101 Capability list:",
                "VERSION 2",
                "STREAM",
                ".",
            ];

            TransitCapabilitySnapshot snapshot = TransitProtocolParser.ParseCapabilitiesResponse(lines);

            Assert.True(snapshot.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the parse capabilities response when status code not101 throws behavior.
        /// </summary>
        [Fact]
        public void ParseCapabilitiesResponse_WhenStatusCodeNot101_Throws()
        {
            string[] lines =
            [
                "500 command not recognized",
                ".",
            ];

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => TransitProtocolParser.ParseCapabilitiesResponse(lines));

            Assert.Contains("Unexpected CAPABILITIES response code", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the read nntp line with byte count async when cr lf line returns line and byte count behavior.
        /// </summary>
        [Fact]
        public async Task ReadNntpLineWithByteCountAsync_WhenCrLfLine_ReturnsLineAndByteCount()
        {
            Pipe pipe = new();
            _ = await pipe.Writer.WriteAsync(Encoding.ASCII.GetBytes("239 <id> ok\r\n"));

            (string line, int bytesRead) = await TransitProtocolParser.ReadNntpLineWithByteCountAsync(pipe.Reader, CancellationToken.None);

            Assert.Equal("239 <id> ok", line);
            Assert.Equal(13, bytesRead);
        }
        /// <summary>
        /// Confirms the read nntp line with byte count and completion async when completed without line returns completion marker behavior.
        /// </summary>
        [Fact]
        public async Task ReadNntpLineWithByteCountAndCompletionAsync_WhenCompletedWithoutLine_ReturnsCompletionMarker()
        {
            Pipe pipe = new();
            await pipe.Writer.CompleteAsync();

            (string? line, int bytesRead, bool completedWithoutLine) = await TransitProtocolParser.ReadNntpLineWithByteCountAndCompletionAsync(pipe.Reader, CancellationToken.None);

            Assert.Null(line);
            Assert.Equal(0, bytesRead);
            Assert.True(completedWithoutLine);
        }
        /// <summary>
        /// Confirms the read nntp line async when completed without line throws behavior.
        /// </summary>
        [Fact]
        public async Task ReadNntpLineAsync_WhenCompletedWithoutLine_Throws()
        {
            Pipe pipe = new();
            await pipe.Writer.CompleteAsync();

            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await TransitProtocolParser.ReadNntpLineAsync(pipe.Reader, CancellationToken.None));
        }
        /// <summary>
        /// Confirms the read nntp line async when line exceeds maximum without newline throws behavior.
        /// </summary>
        [Fact]
        public async Task ReadNntpLineAsync_WhenLineExceedsMaximumWithoutNewline_Throws()
        {
            Pipe pipe = new();
            byte[] oversizedLine = new byte[(16 * 1024) + 1];
            Array.Fill(oversizedLine, (byte)'A');

            _ = await pipe.Writer.WriteAsync(oversizedLine);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await TransitProtocolParser.ReadNntpLineAsync(pipe.Reader, CancellationToken.None));

            Assert.Contains("exceeded maximum length", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
