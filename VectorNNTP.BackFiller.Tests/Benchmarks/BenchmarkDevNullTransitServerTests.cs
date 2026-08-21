using Microsoft.Extensions.Logging.Abstractions;
using System.Net;
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

    }
