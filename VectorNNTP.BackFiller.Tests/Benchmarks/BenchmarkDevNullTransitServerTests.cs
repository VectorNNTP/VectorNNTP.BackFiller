using Microsoft.Extensions.Logging.Abstractions;
using System.Net;
using System.Net.Sockets;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Validates benchmark-only dev/null fake transit server protocol and payload behavior.
/// </summary>
public sealed class BenchmarkDevNullTransitServerTests
{
    /// <summary>
    /// Verifies benchmark fake-server default startup binds to the fixed inspection port.
    /// </summary>
    [Fact]
    public async Task FakeServer_WhenStartedWithDefaults_BindsToPort1190()
    {
        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);

        Assert.Equal(BenchmarkDevNullTransitServer.DefaultListenPort, server.Port);
    }

    /// <summary>
    /// Ensures the fake server accepts TAKETHIS, consumes complete framed payload, and returns correlated 239 response.
    /// </summary>
    [Fact]
    public async Task FakeServer_WhenTakethisSubmitted_ConsumesPayloadAndReturnsCorrelated239()
    {
        string messageId = "<devnull-1@example.com>";
        byte[] payload = Encoding.ASCII.GetBytes("Header: value\r\n\r\nLine1\r\n.Line2\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitConnection>.Instance);

        await connection.InitializeAsync(CancellationToken.None);
        TransitPublishResult result = await connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L);

        Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        Assert.Equal(239, result.ResponseCode);
        Assert.Equal(messageId, result.MessageId);
        Assert.Equal(1, server.AcceptedArticles);
        Assert.True(server.ConsumedArticleBytes > 0);
    }

    /// <summary>
    /// Ensures multiple submissions are accepted and fully consumed by the dev/null sink.
    /// </summary>
    [Fact]
    public async Task FakeServer_WhenMultipleSubmissionsAreSent_ConsumesAllPayloadsAndReturnsAccepted()
    {
        const int submissionCount = 8;
        byte[] payload = Encoding.ASCII.GetBytes("X\r\nY\r\nZ\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitConnection>.Instance);

        await connection.InitializeAsync(CancellationToken.None);

        for (int i = 0; i < submissionCount; i++)
        {
            string messageId = $"<devnull-batch-{i + 1}@example.com>";
            TransitPublishResult result = await connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L);
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
            Assert.Equal(239, result.ResponseCode);
            Assert.Equal(messageId, result.MessageId);
        }

        Assert.Equal(submissionCount, server.AcceptedArticles);
        Assert.True(server.ConsumedArticleBytes > 0);
    }

    /// <summary>
    /// Ensures benchmark configuration load supports endpoint override and endpoint identity metadata for fake-server runs.
    /// </summary>
    [Fact]
    public async Task FakeServer_WhenTwoTakethisCommandsArePipelinedInSingleWrite_ConsumesBothAndReturnsTwoAccepted()
    {
        const string messageId1 = "<pipe-1@example.com>";
        const string messageId2 = "<pipe-2@example.com>";
        byte[] payload1 = Encoding.ASCII.GetBytes("A\r\nB\r\n");
        byte[] payload2 = Encoding.ASCII.GetBytes("C\r\nD\r\nE\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        using TcpClient client = new();
        await client.ConnectAsync(IPAddress.Loopback, server.Port);
        using NetworkStream stream = client.GetStream();

        await PerformStreamingHandshakeAsync(stream);

        string pipelinedCommands = BuildTakethisCommand(messageId1, payload1) + BuildTakethisCommand(messageId2, payload2);
        byte[] pipelinedBytes = Encoding.ASCII.GetBytes(pipelinedCommands);
        await stream.WriteAsync(pipelinedBytes);
        await stream.FlushAsync();

        string response1 = await ReadAsciiLineAsync(stream);
        string response2 = await ReadAsciiLineAsync(stream);
        Assert.StartsWith($"239 {messageId1}", response1, StringComparison.Ordinal);
        Assert.StartsWith($"239 {messageId2}", response2, StringComparison.Ordinal);

        await WriteAsciiLineAsync(stream, "QUIT");
        string quitResponse = await ReadAsciiLineAsync(stream);
        Assert.Equal("205 closing connection", quitResponse);

        Assert.Equal(2, server.AcceptedArticles);
        Assert.Equal(payload1.Length + payload2.Length, server.ConsumedArticleBytes);
    }

    [Fact]
    public async Task FakeServer_WhenTerminatorIsImmediatelyFollowedByNextTakethis_KeepsNextCommandReadable()
    {
        const string messageId1 = "<adjacent-1@example.com>";
        const string messageId2 = "<adjacent-2@example.com>";
        byte[] payload1 = Encoding.ASCII.GetBytes("BodyOne\r\n");
        byte[] payload2 = Encoding.ASCII.GetBytes("BodyTwo\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        using TcpClient client = new();
        await client.ConnectAsync(IPAddress.Loopback, server.Port);
        using NetworkStream stream = client.GetStream();

        await PerformStreamingHandshakeAsync(stream);

        string first = BuildTakethisCommand(messageId1, payload1);
        string second = BuildTakethisCommand(messageId2, payload2);
        byte[] bytes = Encoding.ASCII.GetBytes(first + second);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();

        string response1 = await ReadAsciiLineAsync(stream);
        string response2 = await ReadAsciiLineAsync(stream);
        Assert.StartsWith($"239 {messageId1}", response1, StringComparison.Ordinal);
        Assert.StartsWith($"239 {messageId2}", response2, StringComparison.Ordinal);

        await WriteAsciiLineAsync(stream, "QUIT");
        Assert.Equal("205 closing connection", await ReadAsciiLineAsync(stream));

        Assert.Equal(2, server.AcceptedArticles);
        Assert.Equal(payload1.Length + payload2.Length, server.ConsumedArticleBytes);
    }

    [Fact]
    public async Task FakeServer_WhenTerminatorAndNextCommandArriveInPartialChunks_ParsesWithoutOverflowOrDisconnect()
    {
        const string messageId1 = "<partial-1@example.com>";
        const string messageId2 = "<partial-2@example.com>";
        byte[] payload1 = Encoding.ASCII.GetBytes("LineOne\r\nLineTwo");
        byte[] payload2 = Encoding.ASCII.GetBytes("LineThree");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        using TcpClient client = new();
        await client.ConnectAsync(IPAddress.Loopback, server.Port);
        using NetworkStream stream = client.GetStream();

        await PerformStreamingHandshakeAsync(stream);

        string command1 = $"TAKETHIS {messageId1}\r\n";
        string payload1Body = Encoding.ASCII.GetString(payload1);

        await WriteAsciiAsync(stream, command1 + payload1Body + "\r\n");
        await WriteAsciiAsync(stream, ".\r");
        await WriteAsciiAsync(stream, "\nTAKETHIS ");
        await WriteAsciiAsync(stream, $"{messageId2}\r\n");
        await WriteAsciiAsync(stream, Encoding.ASCII.GetString(payload2) + "\r\n.\r\n");

        string response1 = await ReadAsciiLineAsync(stream);
        string response2 = await ReadAsciiLineAsync(stream);
        Assert.StartsWith($"239 {messageId1}", response1, StringComparison.Ordinal);
        Assert.StartsWith($"239 {messageId2}", response2, StringComparison.Ordinal);

        await WriteAsciiLineAsync(stream, "QUIT");
        Assert.Equal("205 closing connection", await ReadAsciiLineAsync(stream));

        Assert.Equal(2, server.AcceptedArticles);
        Assert.Equal(payload1.Length + payload2.Length, server.ConsumedArticleBytes);
    }

    [Fact]
    public async Task FakeServer_WhenCommandsArePipelinedWithMixedCaseAndCheck_ParsesAndRespondsByCommandKind()
    {
        const string checkMessageId = "<check-1@example.com>";
        const string takeMessageId = "<take-1@example.com>";
        byte[] payload = Encoding.ASCII.GetBytes("A\r\nB\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        using TcpClient client = new();
        await client.ConnectAsync(IPAddress.Loopback, server.Port);
        using NetworkStream stream = client.GetStream();

        await PerformStreamingHandshakeAsync(stream);

        string pipelined = $"check {checkMessageId}\r\ntAkEtHiS {takeMessageId}\r\n{Encoding.ASCII.GetString(payload)}\r\n.\r\n";
        await WriteAsciiAsync(stream, pipelined);

        string checkResponse = await ReadAsciiLineAsync(stream);
        string takethisResponse = await ReadAsciiLineAsync(stream);

        Assert.StartsWith($"238 {checkMessageId}", checkResponse, StringComparison.Ordinal);
        Assert.StartsWith($"239 {takeMessageId}", takethisResponse, StringComparison.Ordinal);

        await WriteAsciiLineAsync(stream, "QUIT");
        Assert.Equal("205 closing connection", await ReadAsciiLineAsync(stream));

        Assert.Equal(1, server.AcceptedArticles);
        Assert.Equal(payload.Length, server.ConsumedArticleBytes);
    }

    [Fact]
    public async Task FakeServer_WhenCommandIsFragmentedAcrossManyWrites_ParsesTakethisAndPayload()
    {
        const string messageId = "<fragmented-1@example.com>";
        byte[] payload = Encoding.ASCII.GetBytes("BodyLine1\r\nBodyLine2\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        using TcpClient client = new();
        await client.ConnectAsync(IPAddress.Loopback, server.Port);
        using NetworkStream stream = client.GetStream();

        await PerformStreamingHandshakeAsync(stream);

        await WriteAsciiAsync(stream, "TA");
        await WriteAsciiAsync(stream, "KETH");
        await WriteAsciiAsync(stream, "IS ");
        await WriteAsciiAsync(stream, messageId[..8]);
        await WriteAsciiAsync(stream, messageId[8..]);
        await WriteAsciiAsync(stream, "\r");
        await WriteAsciiAsync(stream, "\n");
        await WriteAsciiAsync(stream, Encoding.ASCII.GetString(payload));
        await WriteAsciiAsync(stream, "\r\n.\r\n");

        string response = await ReadAsciiLineAsync(stream);
        Assert.StartsWith($"239 {messageId}", response, StringComparison.Ordinal);

        await WriteAsciiLineAsync(stream, "QUIT");
        Assert.Equal("205 closing connection", await ReadAsciiLineAsync(stream));

        Assert.Equal(1, server.AcceptedArticles);
        Assert.Equal(payload.Length, server.ConsumedArticleBytes);
    }

    [Fact]
    public async Task FakeServer_WhenQuitSentAfterTakethisCompletion_ReturnsClosingResponse()
    {
        const string messageId = "<quit-after-complete@example.com>";
        byte[] payload = Encoding.ASCII.GetBytes("Q1\r\nQ2\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        using TcpClient client = new();
        await client.ConnectAsync(IPAddress.Loopback, server.Port);
        using NetworkStream stream = client.GetStream();

        await PerformStreamingHandshakeAsync(stream);

        await WriteAsciiAsync(stream, BuildTakethisCommand(messageId, payload));
        string takethisResponse = await ReadAsciiLineAsync(stream);
        Assert.StartsWith($"239 {messageId}", takethisResponse, StringComparison.Ordinal);

        await WriteAsciiLineAsync(stream, "QUIT");
        string quitResponse = await ReadAsciiLineAsync(stream);
        Assert.Equal("205 closing connection", quitResponse);

        Assert.Equal(1, server.AcceptedArticles);
    }

    [Fact]
    public async Task FakeServer_WhenQuitArrivesWithPendingTakethis_RespondsWithClosingAndStopsWithoutDrain()
    {
        const string messageId1 = "<quit-pending-1@example.com>";
        const string messageId2 = "<quit-pending-2@example.com>";
        byte[] payload = Encoding.ASCII.GetBytes("Body\r\n");

        await using BenchmarkDevNullTransitServer server = await BenchmarkDevNullTransitServer.StartAsync(IPAddress.Loopback);
        using TcpClient client = new();
        await client.ConnectAsync(IPAddress.Loopback, server.Port);
        using NetworkStream stream = client.GetStream();

        await PerformStreamingHandshakeAsync(stream);

        string pipelined = BuildTakethisCommand(messageId1, payload) + BuildTakethisCommand(messageId2, payload) + "QUIT\r\n";
        await WriteAsciiAsync(stream, pipelined);

        List<string> responses = new();
        for (int i = 0; i < 3; i++)
        {
            string line;
            try
            {
                line = await ReadAsciiLineAsync(stream);
            }
            catch (IOException)
            {
                break;
            }

            responses.Add(line);
            if (line.StartsWith("205 ", StringComparison.Ordinal))
            {
                break;
            }
        }

        Assert.Contains("205 closing connection", responses);
        Assert.True(server.AcceptedArticles >= 1);
    }

    [Fact]
    public void TransitBenchmarkConfigLoad_WhenFakeServerOverridesProvided_AppliesEndpointIdentityAndOverrides()
    {
        TransitBenchmarkCliOptions options = new(
            DurationSeconds: null,
            WarmupSeconds: 1,
            ConnectionPoolSize: 2,
            PipelineDepth: 2,
            DispatchWorkers: 4,
            QueueMegabytes: 128,
            QueueArticles: 128,
            ArticleKilobytes: 256,
            GeneratorWorkers: 1,
            WriteBatchCoalesceMicroseconds: 250,
            ExpectedAssemblyPath: "C:\\bench\\VectorNNTP.BackFiller.Benchmarks.dll",
            ExpectedAssemblyVersion: "1.0.0",
            ExpectedFileVersion: "1.0.0");

        TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(
            TimeSpan.FromSeconds(5),
            BenchmarkMode.Validation,
            options,
            endpointHostOverride: IPAddress.Loopback.ToString(),
            endpointPortOverride: 43210,
            endpointUseSslOverride: false,
            endpointType: BenchmarkDevNullTransitServer.EndpointTypeLabel,
            endpointIdentity: BenchmarkDevNullTransitServer.ServerIdentity);

        Assert.Equal(BenchmarkDevNullTransitServer.EndpointTypeLabel, config.EndpointType);
        Assert.Equal(BenchmarkDevNullTransitServer.ServerIdentity, config.EndpointIdentity);
        Assert.Equal(IPAddress.Loopback.ToString(), config.EndpointHost);
        Assert.Equal(43210, config.EndpointPort);
        Assert.False(config.EndpointUseSsl);
    }

    private static async Task PerformStreamingHandshakeAsync(NetworkStream stream)
    {
        string greeting = await ReadAsciiLineAsync(stream);
        Assert.StartsWith("200 ", greeting, StringComparison.Ordinal);

        await WriteAsciiLineAsync(stream, "CAPABILITIES");
        Assert.Equal("101 Capability list:", await ReadAsciiLineAsync(stream));
        Assert.Equal("STREAMING", await ReadAsciiLineAsync(stream));
        Assert.Equal(".", await ReadAsciiLineAsync(stream));

        await WriteAsciiLineAsync(stream, "MODE STREAM");
        Assert.Equal("203 Streaming permitted", await ReadAsciiLineAsync(stream));
    }

    private static string BuildTakethisCommand(string messageId, byte[] payload)
    {
        string body = Encoding.ASCII.GetString(payload);
        return $"TAKETHIS {messageId}\r\n{body}\r\n.\r\n";
    }

    private static async Task WriteAsciiLineAsync(NetworkStream stream, string line)
    {
        await WriteAsciiAsync(stream, line + "\r\n");
    }

    private static async Task WriteAsciiAsync(NetworkStream stream, string value)
    {
        byte[] bytes = Encoding.ASCII.GetBytes(value);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    private static async Task<string> ReadAsciiLineAsync(NetworkStream stream)
    {
        List<byte> buffer = new();
        byte[] single = new byte[1];

        while (true)
        {
            int read = await stream.ReadAsync(single);
            if (read == 0)
            {
                throw new IOException("Connection closed while reading line.");
            }

            buffer.Add(single[0]);
            if (buffer.Count >= 2
                && buffer[^2] == (byte)'\r'
                && buffer[^1] == (byte)'\n')
            {
                byte[] bytes = buffer.ToArray();
                return Encoding.ASCII.GetString(bytes, 0, bytes.Length - 2);
            }
        }
    }
}

