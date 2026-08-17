using System.IO.Pipelines;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests NNTP protocol parsing semantics for greeting and CAPABILITIES handling.
/// </summary>
public sealed class TransitProtocolParserTests
{
    [Fact]
    public async Task ReadNntpLineAsync_WhenCrLfLine_ReturnsLineWithoutCrLf()
    {
        Pipe pipe = new();
        await pipe.Writer.WriteAsync(Encoding.ASCII.GetBytes("200 transit ready\r\n"));

        string line = await TransitProtocolParser.ReadNntpLineAsync(pipe.Reader, CancellationToken.None);

        Assert.Equal("200 transit ready", line);
    }

    [Fact]
    public void ValidateGreeting_When200Or201_DoesNotThrow()
    {
        TransitProtocolParser.ValidateGreeting("200 transit posting allowed");
        TransitProtocolParser.ValidateGreeting("201 transit no posting");
    }

    [Fact]
    public void ValidateGreeting_WhenUnexpected_Throws()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => TransitProtocolParser.ValidateGreeting("400 temporary failure"));

        Assert.Contains("Unexpected NNTP greeting response code", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseCapabilitiesResponse_WhenStartTlsCompressStreamingPresent_DetectsAll()
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
        Assert.True(snapshot.SupportsCompressDeflate);
        Assert.True(snapshot.SupportsStreaming);
    }

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

    [Fact]
    public void ParseStatusLine_WhenValid_ParsesCodeTextAndTokens()
    {
        (int code, string text, string[] tokens) = TransitProtocolParser.ParseStatusLine("203 streaming allowed");

        Assert.Equal(203, code);
        Assert.Equal("streaming allowed", text);
        Assert.Equal(["streaming", "allowed"], tokens);
    }

    [Fact]
    public void ParseStatusLine_WhenMissingSeparatorAfterCode_Throws()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => TransitProtocolParser.ParseStatusLine("239<id> transferred"));

        Assert.Contains("Malformed NNTP response", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ParseCapabilitiesResponse_WhenMixedCaseAndParameters_DetectsSupportedFeatures()
    {
        string[] lines =
        [
            "101 Capability list:",
            "version 2",
            "starttls",
            "compress deflate level=6",
            "streaming posting",
            "x-feature custom",
            ".",
        ];

        TransitCapabilitySnapshot snapshot = TransitProtocolParser.ParseCapabilitiesResponse(lines);

        Assert.True(snapshot.SupportsStartTls);
        Assert.True(snapshot.SupportsCompressDeflate);
        Assert.True(snapshot.SupportsStreaming);
    }

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

    [Fact]
    public async Task ReadNntpLineWithByteCountAsync_WhenCrLfLine_ReturnsLineAndByteCount()
    {
        Pipe pipe = new();
        await pipe.Writer.WriteAsync(Encoding.ASCII.GetBytes("239 <id> ok\r\n"));

        (string line, int bytesRead) = await TransitProtocolParser.ReadNntpLineWithByteCountAsync(pipe.Reader, CancellationToken.None);

        Assert.Equal("239 <id> ok", line);
        Assert.Equal(13, bytesRead);
    }

    [Fact]
    public async Task ReadNntpLineAsync_WhenCompletedWithoutLine_Throws()
    {
        Pipe pipe = new();
        await pipe.Writer.CompleteAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await TransitProtocolParser.ReadNntpLineAsync(pipe.Reader, CancellationToken.None));
    }

    [Fact]
    public async Task ReadNntpLineAsync_WhenLineExceedsMaximumWithoutNewline_Throws()
    {
        Pipe pipe = new();
        byte[] oversizedLine = new byte[(16 * 1024) + 1];
        Array.Fill(oversizedLine, (byte)'A');

        await pipe.Writer.WriteAsync(oversizedLine);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await TransitProtocolParser.ReadNntpLineAsync(pipe.Reader, CancellationToken.None));

        Assert.Contains("exceeded maximum length", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
