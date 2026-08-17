using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests TransitPublisher queue dispatch and publish outcome behavior.
/// </summary>
public sealed class TransitPublisherTests
{
    [Fact]
    public async Task PublishAsync_WhenInitialized_DispatchesQueuedSubmissionAndReturnsAccepted()
    {
        byte[] payload = [(byte)'P', (byte)'\n'];
        string messageId = "<publisher-accept@example.com>";

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageId}", takethisLine);

            byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(payload, receivedPayload);

            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1);

        await publisher.InitializeAsync(CancellationToken.None);
        TransitPublishResult result = await publisher.PublishAsync(messageId, payload, CancellationToken.None);

        Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        Assert.Equal(239, result.ResponseCode);
        Assert.Equal(messageId, result.MessageId);
    }

    [Fact]
    public async Task PublishAsync_WhenNotInitialized_ReturnsUnavailable()
    {
        await using TransitPublisher publisher = CreatePublisher(port: 19000, connectionPoolSize: 1);

        TransitPublishResult result = await publisher.PublishAsync(
            "<publisher-unavailable@example.com>",
            new byte[] { (byte)'U', (byte)'\n' },
            CancellationToken.None);

        Assert.Equal(TransitPublishStatus.Unavailable, result.Status);
        Assert.Null(result.ResponseCode);
    }

    [Fact]
    public async Task PublishAsync_WhenCanceledBeforeChannelAdmission_DoesNotLeakQueuedSubmissionCount()
    {
        const int queueCapacity = 2048;
        const int expectedAdmittedOutstanding = queueCapacity + 1;

        byte[] payload = [(byte)'Q', (byte)'\n'];
        int observedTakethisCount = 0;

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);
            _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Interlocked.Increment(ref observedTakethisCount);

            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] admittedSubmissions = new Task<TransitPublishResult>[expectedAdmittedOutstanding];
        for (int i = 0; i < expectedAdmittedOutstanding; i++)
        {
            string messageId = $"<fill-{i}@example.com>";
            admittedSubmissions[i] = publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask();
        }

        using CancellationTokenSource fillTimeout = new(TimeSpan.FromSeconds(10));
        while (GetQueuedSubmissionCount(publisher) < expectedAdmittedOutstanding)
        {
            await Task.Delay(10, fillTimeout.Token);
        }

        using CancellationTokenSource takethisObservedTimeout = new(TimeSpan.FromSeconds(10));
        while (Volatile.Read(ref observedTakethisCount) < 1)
        {
            await Task.Delay(10, takethisObservedTimeout.Token);
        }

        using CancellationTokenSource blockedCts = new();
        Task<TransitPublishResult> blockedAdmission = publisher.PublishAsync("<blocked-admission@example.com>", payload, blockedCts.Token).AsTask();

        await Task.Delay(100);
        Assert.False(blockedAdmission.IsCompleted);

        blockedCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blockedAdmission);

        Assert.Equal(expectedAdmittedOutstanding, GetQueuedSubmissionCount(publisher));
        Assert.Equal(1, Volatile.Read(ref observedTakethisCount));

        using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(10));
        await publisher.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);

        using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
        await Task.WhenAll(admittedSubmissions).WaitAsync(completionTimeout.Token);

        Assert.Equal(0, GetQueuedSubmissionCount(publisher));
    }

    [Fact]
    public async Task DisposeAsync_WhenInFlightTakethisResponseNeverArrives_CompletesAndFinalizesPublishTask()
    {
        string messageId = "<publisher-dispose-inflight@example.com>";
        byte[] payload = [(byte)'I', (byte)'\n'];

        TaskCompletionSource takethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageId}", takethisLine);
            _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            takethisObserved.TrySetResult();

            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult> publishTask = publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask();

        using CancellationTokenSource observeTimeout = new(TimeSpan.FromSeconds(10));
        await takethisObserved.Task.WaitAsync(observeTimeout.Token);

        using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(10));
        await publisher.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);

        using CancellationTokenSource publishTimeout = new(TimeSpan.FromSeconds(10));
        TransitPublishResult result = await publishTask.WaitAsync(publishTimeout.Token);

        Assert.Equal(TransitPublishStatus.Canceled, result.Status);
        Assert.Equal(messageId, result.MessageId);
        Assert.Null(result.ResponseCode);
        Assert.Equal(0, GetQueuedSubmissionCount(publisher));
    }

    [Fact]
    public async Task PublishAsync_WhenTwoConcurrentSubmissions_UsesTwoConnectionPool()
    {
        string firstMessageId = "<publisher-parallel-1@example.com>";
        string secondMessageId = "<publisher-parallel-2@example.com>";

        TaskCompletionSource firstSawTakethis = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondSawTakethis = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.True(
                    takethisLine == $"TAKETHIS {firstMessageId}" || takethisLine == $"TAKETHIS {secondMessageId}",
                    $"Unexpected TAKETHIS line '{takethisLine}'.");

                byte[] payload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.True(payload.SequenceEqual(new byte[] { (byte)'A', (byte)'\n' }) || payload.SequenceEqual(new byte[] { (byte)'B', (byte)'\n' }));

                firstSawTakethis.TrySetResult();
                await secondSawTakethis.Task.WaitAsync(cancellationToken);

                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
            },
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.True(
                    takethisLine == $"TAKETHIS {firstMessageId}" || takethisLine == $"TAKETHIS {secondMessageId}",
                    $"Unexpected TAKETHIS line '{takethisLine}'.");

                byte[] payload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.True(payload.SequenceEqual(new byte[] { (byte)'A', (byte)'\n' }) || payload.SequenceEqual(new byte[] { (byte)'B', (byte)'\n' }));

                secondSawTakethis.TrySetResult();
                await firstSawTakethis.Task.WaitAsync(cancellationToken);

                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
            },
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 2);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult> first = publisher.PublishAsync(firstMessageId, new byte[] { (byte)'A', (byte)'\n' }, CancellationToken.None).AsTask();
        Task<TransitPublishResult> second = publisher.PublishAsync(secondMessageId, new byte[] { (byte)'B', (byte)'\n' }, CancellationToken.None).AsTask();

        TransitPublishResult[] results = await Task.WhenAll(first, second);

        Assert.Contains(results, r => r.MessageId == firstMessageId && r.Status == TransitPublishStatus.Accepted);
        Assert.Contains(results, r => r.MessageId == secondMessageId && r.Status == TransitPublishStatus.Accepted);

        TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 2, outstandingSubmissions: 0);
        Assert.Equal(2, snapshot.TotalArticlesSubmitted);
        Assert.Equal(2, snapshot.TotalArticlesAccepted);
        Assert.Equal(0, snapshot.TotalReconnects);
    }

    [Fact]
    public async Task PublishAsync_WhenCallerCancelsAfterAdmission_StillLogsFinalOutcome()
    {
        string messageId = "<publisher-cancel-after-admission@example.com>";
        byte[] payload = [(byte)'C', (byte)'\n'];
        CapturingLoggerProvider provider = new();

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageId}", takethisLine);

            byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(payload, receivedPayload);

            await Task.Delay(80, cancellationToken);
            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
        });

        await using TransitPublisher publisher = CreatePublisherWithLogger(server.Port, connectionPoolSize: 1, provider.CreateLogger<TransitPublisher>());
        await publisher.InitializeAsync(CancellationToken.None);

        using CancellationTokenSource cts = new();
        ValueTask<TransitPublishResult> pending = publisher.PublishAsync(messageId, payload, cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.AsTask());

        await Task.Delay(150);

        Assert.Contains(provider.Entries, entry =>
            entry.EventId.Id == 2204
            && entry.Message.Contains(messageId, StringComparison.Ordinal)
            && entry.Message.Contains("Accepted", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task PublishAsync_WhenSingleConnectionPipelineDepthGreaterThanOne_SendsMultipleTakethisBeforeFirstResponse()
    {
        string[] messageIds =
        [
            "<publisher-pipeline-1@example.com>",
            "<publisher-pipeline-2@example.com>",
            "<publisher-pipeline-3@example.com>",
        ];

        TaskCompletionSource allTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            List<string> observed = [];
            for (int i = 0; i < messageIds.Length; i++)
            {
                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);

                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.Contains(messageId, messageIds);
                observed.Add(messageId);

                byte[] payload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(new byte[] { (byte)('1' + i), (byte)'\n' }, payload);
            }

            Assert.Equal(messageIds.Length, observed.Distinct(StringComparer.Ordinal).Count());
            allTakethisObserved.TrySetResult();

            await FakePublisherServer.WriteLineAsync(stream, $"239 {observed[0]} transferred");
            await FakePublisherServer.WriteLineAsync(stream, $"239 {observed[1]} transferred");
            await FakePublisherServer.WriteLineAsync(stream, $"239 {observed[2]} transferred");
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] submissions =
        [
            publisher.PublishAsync(messageIds[0], new byte[] { (byte)'1', (byte)'\n' }, CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[1], new byte[] { (byte)'2', (byte)'\n' }, CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[2], new byte[] { (byte)'3', (byte)'\n' }, CancellationToken.None).AsTask(),
        ];

        await allTakethisObserved.Task.WaitAsync(TimeSpan.FromSeconds(5));

        TransitPublishResult[] results = await Task.WhenAll(submissions);
        foreach (string messageId in messageIds)
        {
            Assert.Contains(results, r => r.MessageId == messageId && r.Status == TransitPublishStatus.Accepted && r.ResponseCode == 239);
        }
    }

    [Fact]
    public async Task PublishAsync_WhenSingleConnectionResponsesOutOfOrder_CorrelatesByMessageId()
    {
        string messageA = "<publisher-outoforder-a@example.com>";
        string messageB = "<publisher-outoforder-b@example.com>";
        string messageC = "<publisher-outoforder-c@example.com>";

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethisA = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageA}", takethisA);
            byte[] payloadA = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(new byte[] { (byte)'A', (byte)'\n' }, payloadA);

            string takethisB = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageB}", takethisB);
            byte[] payloadB = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(new byte[] { (byte)'B', (byte)'\n' }, payloadB);

            string takethisC = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageC}", takethisC);
            byte[] payloadC = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(new byte[] { (byte)'C', (byte)'\n' }, payloadC);

            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageC} transferred");
            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageA} transferred");
            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageB} transferred");
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult> first = publisher.PublishAsync(messageA, new byte[] { (byte)'A', (byte)'\n' }, CancellationToken.None).AsTask();
        Task<TransitPublishResult> second = publisher.PublishAsync(messageB, new byte[] { (byte)'B', (byte)'\n' }, CancellationToken.None).AsTask();
        Task<TransitPublishResult> third = publisher.PublishAsync(messageC, new byte[] { (byte)'C', (byte)'\n' }, CancellationToken.None).AsTask();

        TransitPublishResult[] results = await Task.WhenAll(first, second, third);

        Assert.Contains(results, r => r.MessageId == messageA && r.Status == TransitPublishStatus.Accepted);
        Assert.Contains(results, r => r.MessageId == messageB && r.Status == TransitPublishStatus.Accepted);
        Assert.Contains(results, r => r.MessageId == messageC && r.Status == TransitPublishStatus.Accepted);
    }

    [Fact]
    public async Task PublishAsync_WhenPipelineDepthTwo_DoesNotExceedTwoOutstandingSubmissions()
    {
        string[] messageIds =
        [
            "<publisher-depth-1@example.com>",
            "<publisher-depth-2@example.com>",
            "<publisher-depth-3@example.com>",
            "<publisher-depth-4@example.com>",
        ];

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            Dictionary<string, byte[]> expectedPayloadByMessageId = new(StringComparer.Ordinal)
            {
                [messageIds[0]] = new byte[] { (byte)'a', (byte)'\n' },
                [messageIds[1]] = new byte[] { (byte)'b', (byte)'\n' },
                [messageIds[2]] = new byte[] { (byte)'c', (byte)'\n' },
                [messageIds[3]] = new byte[] { (byte)'d', (byte)'\n' },
            };

            string takethis1 = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            string messageId1 = takethis1.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
            Assert.True(expectedPayloadByMessageId.TryGetValue(messageId1, out byte[]? expectedPayload1));
            byte[] payload1 = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(expectedPayload1, payload1);

            string takethis2 = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            string messageId2 = takethis2.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
            Assert.NotEqual(messageId1, messageId2);
            Assert.True(expectedPayloadByMessageId.TryGetValue(messageId2, out byte[]? expectedPayload2));
            byte[] payload2 = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(expectedPayload2, payload2);

            Task<string> pendingThirdTakethisRead = FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Task completedProbe = await Task.WhenAny(pendingThirdTakethisRead, Task.Delay(100, cancellationToken));
            Assert.NotSame(pendingThirdTakethisRead, completedProbe);

            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId1} transferred");

            string takethis3 = await pendingThirdTakethisRead;
            string messageId3 = takethis3.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
            Assert.NotEqual(messageId1, messageId3);
            Assert.NotEqual(messageId2, messageId3);
            Assert.True(expectedPayloadByMessageId.TryGetValue(messageId3, out byte[]? expectedPayload3));
            byte[] payload3 = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(expectedPayload3, payload3);

            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId2} transferred");

            string takethis4 = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            string messageId4 = takethis4.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
            Assert.NotEqual(messageId1, messageId4);
            Assert.NotEqual(messageId2, messageId4);
            Assert.NotEqual(messageId3, messageId4);
            Assert.True(expectedPayloadByMessageId.TryGetValue(messageId4, out byte[]? expectedPayload4));
            byte[] payload4 = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(expectedPayload4, payload4);

            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId3} transferred");
            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId4} transferred");
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 2);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] submissions =
        [
            publisher.PublishAsync(messageIds[0], new byte[] { (byte)'a', (byte)'\n' }, CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[1], new byte[] { (byte)'b', (byte)'\n' }, CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[2], new byte[] { (byte)'c', (byte)'\n' }, CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[3], new byte[] { (byte)'d', (byte)'\n' }, CancellationToken.None).AsTask(),
        ];

        TransitPublishResult[] results = await Task.WhenAll(submissions);

        Assert.All(results, result => Assert.Equal(TransitPublishStatus.Accepted, result.Status));
    }

    [Fact]
    public async Task PublishAsync_WhenPayloadContainsBinaryAndLeadingDots_PreservesByteIntegrity()
    {
        string messageId = "<publisher-binary@example.com>";
        byte[] payload =
        [
            0x00,
            0x7F,
            0x80,
            0xFF,
            (byte)'\r',
            (byte)'\n',
            (byte)'.',
            (byte)'d',
            (byte)'o',
            (byte)'t',
            (byte)'\n',
            (byte)'.',
            (byte)'\n',
            (byte)'X',
            (byte)'\n',
        ];

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageId}", takethisLine);

            byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(payload, receivedPayload);

            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        TransitPublishResult result = await publisher.PublishAsync(messageId, payload, CancellationToken.None);

        Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        Assert.Equal(239, result.ResponseCode);
        Assert.Equal(messageId, result.MessageId);
    }

    [Fact]
    public async Task PublishAsync_WhenMultipleOutstandingAndConnectionDrops_CompletesAllAsAmbiguous()
    {
        string[] messageIds =
        [
            "<publisher-drop-a@example.com>",
            "<publisher-drop-b@example.com>",
            "<publisher-drop-c@example.com>",
        ];

        Dictionary<string, byte[]> payloads = new(StringComparer.Ordinal)
        {
            [messageIds[0]] = new byte[] { (byte)'A', (byte)'\n' },
            [messageIds[1]] = new byte[] { (byte)'B', (byte)'\n' },
            [messageIds[2]] = new byte[] { (byte)'C', (byte)'\n' },
        };

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            HashSet<string> observed = new(StringComparer.Ordinal);

            for (int i = 0; i < messageIds.Length; i++)
            {
                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.True(observed.Add(messageId));
                Assert.True(payloads.TryGetValue(messageId, out byte[]? expectedPayload));

                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(expectedPayload, receivedPayload);
            }

            stream.Dispose();
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] submissions =
        [
            publisher.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
        ];

        TransitPublishResult[] results = await Task.WhenAll(submissions);

        Assert.All(results, result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));
        Assert.All(results, result => Assert.Null(result.ResponseCode));
    }

    [Fact]
    public async Task PublishAsync_WhenPartialResponseThenDisconnect_LeavesUnansweredSubmissionsAmbiguous()
    {
        string[] messageIds =
        [
            "<publisher-partial-a@example.com>",
            "<publisher-partial-b@example.com>",
            "<publisher-partial-c@example.com>",
        ];

        Dictionary<string, byte[]> payloads = new(StringComparer.Ordinal)
        {
            [messageIds[0]] = new byte[] { (byte)'1', (byte)'\n' },
            [messageIds[1]] = new byte[] { (byte)'2', (byte)'\n' },
            [messageIds[2]] = new byte[] { (byte)'3', (byte)'\n' },
        };

        string? firstAcceptedMessageId = null;

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            List<string> observed = [];

            for (int i = 0; i < messageIds.Length; i++)
            {
                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                observed.Add(messageId);
                Assert.True(payloads.TryGetValue(messageId, out byte[]? expectedPayload));

                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(expectedPayload, receivedPayload);
            }

            firstAcceptedMessageId = observed[0];
            await FakePublisherServer.WriteLineAsync(stream, $"239 {firstAcceptedMessageId} transferred");
            stream.Dispose();
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] submissions =
        [
            publisher.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
        ];

        TransitPublishResult[] results = await Task.WhenAll(submissions);

        Assert.NotNull(firstAcceptedMessageId);
        Assert.Contains(results, result => result.MessageId == firstAcceptedMessageId && result.Status == TransitPublishStatus.Accepted && result.ResponseCode == 239);

        TransitPublishResult[] ambiguous = results.Where(result => result.MessageId != firstAcceptedMessageId).ToArray();
        Assert.Equal(2, ambiguous.Length);
        Assert.All(ambiguous, result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));
    }

    [Fact]
    public async Task PublishAsync_WhenOutOfOrderPartialResponsesThenDisconnect_CompletesOnlyUnansweredSubmissionAsAmbiguous()
    {
        string[] messageIds =
        [
            "<publisher-oo-partial-a@example.com>",
            "<publisher-oo-partial-b@example.com>",
            "<publisher-oo-partial-c@example.com>",
        ];

        Dictionary<string, byte[]> payloads = new(StringComparer.Ordinal)
        {
            [messageIds[0]] = new byte[] { (byte)'a', (byte)'\n' },
            [messageIds[1]] = new byte[] { (byte)'b', (byte)'\n' },
            [messageIds[2]] = new byte[] { (byte)'c', (byte)'\n' },
        };

        string? firstResponseMessageId = null;
        string? secondResponseMessageId = null;
        string? remainingMessageId = null;

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            List<string> observed = [];

            for (int i = 0; i < messageIds.Length; i++)
            {
                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                observed.Add(messageId);
                Assert.True(payloads.TryGetValue(messageId, out byte[]? expectedPayload));

                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(expectedPayload, receivedPayload);
            }

            firstResponseMessageId = observed[1];
            secondResponseMessageId = observed[0];
            remainingMessageId = observed[2];

            await FakePublisherServer.WriteLineAsync(stream, $"239 {firstResponseMessageId} transferred");
            await FakePublisherServer.WriteLineAsync(stream, $"239 {secondResponseMessageId} transferred");
            stream.Dispose();
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] submissions =
        [
            publisher.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
        ];

        TransitPublishResult[] results = await Task.WhenAll(submissions);

        Assert.NotNull(firstResponseMessageId);
        Assert.NotNull(secondResponseMessageId);
        Assert.NotNull(remainingMessageId);

        Assert.Contains(results, result => result.MessageId == firstResponseMessageId && result.Status == TransitPublishStatus.Accepted && result.ResponseCode == 239);
        Assert.Contains(results, result => result.MessageId == secondResponseMessageId && result.Status == TransitPublishStatus.Accepted && result.ResponseCode == 239);
        Assert.Contains(results, result => result.MessageId == remainingMessageId && result.Status == TransitPublishStatus.Ambiguous);
    }

    [Fact]
    public async Task PublishAsync_WhenDisposedWithMultipleOutstanding_CompletesPendingSubmissions()
    {
        string[] messageIds =
        [
            "<publisher-cancel-a@example.com>",
            "<publisher-cancel-b@example.com>",
            "<publisher-cancel-c@example.com>",
        ];

        Dictionary<string, byte[]> payloads = new(StringComparer.Ordinal)
        {
            [messageIds[0]] = new byte[] { (byte)'x', (byte)'\n' },
            [messageIds[1]] = new byte[] { (byte)'y', (byte)'\n' },
            [messageIds[2]] = new byte[] { (byte)'z', (byte)'\n' },
        };

        TaskCompletionSource allTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            HashSet<string> observed = new(StringComparer.Ordinal);
            for (int i = 0; i < messageIds.Length; i++)
            {
                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.True(observed.Add(messageId));
                Assert.True(payloads.TryGetValue(messageId, out byte[]? expectedPayload));

                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(expectedPayload, receivedPayload);
            }

            allTakethisObserved.TrySetResult();
            await Task.Delay(5000, cancellationToken);
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] submissions =
        [
            publisher.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
            publisher.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
        ];

        await allTakethisObserved.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await publisher.DisposeAsync();

        TransitPublishResult[] results = await Task.WhenAll(submissions);
        Assert.All(results, result => Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous));
    }

    [Fact]
    public async Task PublishAsync_WhenConnectionReplaced_DoesNotRetryAmbiguousSubmissions()
    {
        string firstMessageId = "<publisher-replace-a@example.com>";
        string secondMessageId = "<publisher-replace-b@example.com>";
        string thirdMessageId = "<publisher-replace-c@example.com>";

        byte[] firstPayload = [(byte)'1', (byte)'\n'];
        byte[] secondPayload = [(byte)'2', (byte)'\n'];
        byte[] thirdPayload = [(byte)'3', (byte)'\n'];

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisOne = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                string messageIdOne = takethisOne.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.Contains(messageIdOne, new[] { firstMessageId, secondMessageId });
                byte[] payloadOne = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.True(payloadOne.SequenceEqual(firstPayload) || payloadOne.SequenceEqual(secondPayload));

                string takethisTwo = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                string messageIdTwo = takethisTwo.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.Contains(messageIdTwo, new[] { firstMessageId, secondMessageId });
                Assert.NotEqual(messageIdOne, messageIdTwo);
                byte[] payloadTwo = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.True(payloadTwo.SequenceEqual(firstPayload) || payloadTwo.SequenceEqual(secondPayload));

                stream.Dispose();
            },
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethis = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {thirdMessageId}", takethis);
                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(thirdPayload, receivedPayload);

                await FakePublisherServer.WriteLineAsync(stream, $"239 {thirdMessageId} transferred");

                using CancellationTokenSource noRetryCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                noRetryCts.CancelAfter(TimeSpan.FromMilliseconds(100));
                Exception ex = await Record.ExceptionAsync(() => FakePublisherServer.ReadLineAsync(stream, noRetryCts.Token));
                Assert.NotNull(ex);
                Assert.True(ex is OperationCanceledException or InvalidOperationException);
            },
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult> first = publisher.PublishAsync(firstMessageId, firstPayload, CancellationToken.None).AsTask();
        Task<TransitPublishResult> second = publisher.PublishAsync(secondMessageId, secondPayload, CancellationToken.None).AsTask();
        TransitPublishResult[] firstBatch = await Task.WhenAll(first, second);

        Assert.All(firstBatch, result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));

        TransitPublishResult third = await publisher.PublishAsync(thirdMessageId, thirdPayload, CancellationToken.None);
        Assert.Equal(TransitPublishStatus.Accepted, third.Status);
        Assert.Equal(239, third.ResponseCode);
        Assert.Equal(thirdMessageId, third.MessageId);
    }

    [Fact]
    public async Task PublishAsync_WhenConnectionCountFourAndPipelineDepthOne_UtilizesAllConnectionsConcurrently()
    {
        const int connectionCount = 4;
        const int pipelineDepth = 1;
        const int submissionCount = connectionCount;

        string[] messageIds = Enumerable.Range(0, submissionCount)
            .Select(static i => $"<publisher-multi-slot-{i:D2}@example.com>")
            .ToArray();

        Dictionary<string, byte[]> payloadsByMessageId = new(StringComparer.Ordinal);
        for (int i = 0; i < submissionCount; i++)
        {
            payloadsByMessageId[messageIds[i]] = new byte[] { (byte)i, (byte)'\n' };
        }

        int[] perSessionCounts = new int[connectionCount];
        int totalObserved = 0;
        TaskCompletionSource allObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseResponses = new(TaskCreationOptions.RunContinuationsAsynchronously);

        List<Func<NetworkStream, CancellationToken, Task>> sessions = [];
        for (int sessionIndex = 0; sessionIndex < connectionCount; sessionIndex++)
        {
            int currentSession = sessionIndex;
            sessions.Add(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.True(payloadsByMessageId.TryGetValue(messageId, out byte[]? expectedPayload));

                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(expectedPayload, receivedPayload);

                Interlocked.Increment(ref perSessionCounts[currentSession]);
                int observed = Interlocked.Increment(ref totalObserved);
                if (observed == submissionCount)
                {
                    allObserved.TrySetResult();
                }

                await releaseResponses.Task.WaitAsync(cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
            });
        }

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(sessions);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: connectionCount, perConnectionPipelineDepth: pipelineDepth);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] submissions = messageIds
            .Select(id => publisher.PublishAsync(id, payloadsByMessageId[id], CancellationToken.None).AsTask())
            .ToArray();

        await allObserved.Task.WaitAsync(TimeSpan.FromSeconds(5));

        for (int i = 0; i < connectionCount; i++)
        {
            Assert.Equal(1, Volatile.Read(ref perSessionCounts[i]));
        }

        releaseResponses.TrySetResult();

        TransitPublishResult[] results = await Task.WhenAll(submissions);
        foreach (string messageId in messageIds)
        {
            Assert.Contains(results, result => result.MessageId == messageId && result.Status == TransitPublishStatus.Accepted && result.ResponseCode == 239);
        }
    }

    [Fact]
    public async Task PublishAsync_AfterConnectionDrop_ReconnectsAndPublishesSubsequentSubmission()
    {
        string firstMessageId = "<publisher-first@example.com>";
        string secondMessageId = "<publisher-second@example.com>";

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {firstMessageId}", takethisLine);

                byte[] firstPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(new byte[] { (byte)'1', (byte)'\n' }, firstPayload);

                stream.Dispose();
            },
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {secondMessageId}", takethisLine);

                byte[] secondPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(new byte[] { (byte)'2', (byte)'\n' }, secondPayload);

                await FakePublisherServer.WriteLineAsync(stream, $"239 {secondMessageId} transferred");
            },
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        TransitPublishResult first = await publisher.PublishAsync(firstMessageId, new byte[] { (byte)'1', (byte)'\n' }, CancellationToken.None);
        TransitPublishResult second = await publisher.PublishAsync(secondMessageId, new byte[] { (byte)'2', (byte)'\n' }, CancellationToken.None);

        Assert.Equal(TransitPublishStatus.Ambiguous, first.Status);
        Assert.Equal(TransitPublishStatus.Accepted, second.Status);

        TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: 0);
        Assert.Equal(1, snapshot.TotalReconnects);
        Assert.Equal(2, snapshot.TotalArticlesSubmitted);
        Assert.Equal(1, snapshot.TotalArticlesAccepted);
        Assert.Equal(1, snapshot.TotalArticlesAmbiguous);
    }

    [Fact]
    public async Task InitializeAsync_WhenDisposeBeginsDuringConnectionSetup_DoesNotResurrectConnection()
    {
        using CancellationTokenSource testTimeout = new(TimeSpan.FromSeconds(10));

        TaskCompletionSource firstSessionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowGreeting = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            firstSessionAccepted.TrySetResult();
            await allowGreeting.Task.WaitAsync(cancellationToken);
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1);

        Task initTask = publisher.InitializeAsync(CancellationToken.None);

        await firstSessionAccepted.Task.WaitAsync(testTimeout.Token);

        Task disposeTask = publisher.DisposeAsync().AsTask();
        allowGreeting.TrySetResult();

        Exception? initFailure = await CaptureExceptionAsync(initTask, testTimeout.Token);

        await disposeTask.WaitAsync(testTimeout.Token);

        Assert.NotNull(initFailure);
        Assert.IsType<OperationCanceledException>(initFailure);
        Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        Assert.Equal(0, GetPrimaryConnectionCount(publisher));
    }

    [Fact]
    public async Task ReconnectAsync_WhenConcurrentRequestsTargetSameSlot_DoesNotReplaceFreshHealthyConnectionTwice()
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

        TaskCompletionSource thirdSessionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, _) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                stream.Dispose();
            },
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
            async (stream, cancellationToken) =>
            {
                thirdSessionAccepted.TrySetResult();
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 2);
        await publisher.InitializeAsync(CancellationToken.None);

        while (GetPrimaryConnectionState(publisher) is not TransitConnectionState.Faulted and not TransitConnectionState.Disconnected)
        {
            await Task.Delay(10, timeout.Token);
        }

        Task reconnectA = InvokeReconnectAsync(publisher, slotIndex: 0, CancellationToken.None);
        Task reconnectB = InvokeReconnectAsync(publisher, slotIndex: 0, CancellationToken.None);

        await Task.WhenAll(reconnectA, reconnectB).WaitAsync(timeout.Token);

        TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: 0);
        Assert.Equal(1, snapshot.TotalReconnects);
        Assert.Equal(1, GetPrimaryConnectionCount(publisher));

        await Task.Delay(200, timeout.Token);
        Assert.False(thirdSessionAccepted.Task.IsCompleted);
    }

    [Fact]
    public async Task PublishAsync_WhenPrimarySlotFaultedButSecondarySlotHealthy_StillAdmitsAndPublishes()
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

        string messageId = "<publisher-slot1-available@example.com>";
        byte[] payload = [(byte)'S', (byte)'2', (byte)'\n'];

        TaskCompletionSource firstSessionReady = new(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task RunPublishingSession(NetworkStream stream, CancellationToken cancellationToken)
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethis = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal($"TAKETHIS {messageId}", takethis);
            byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Assert.Equal(payload, receivedPayload);
            await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");

            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");
                firstSessionReady.TrySetResult();

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
            RunPublishingSession,
            RunPublishingSession,
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 2, perConnectionPipelineDepth: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        await firstSessionReady.Task.WaitAsync(timeout.Token);

        ForcePrimaryConnectionState(publisher, TransitConnectionState.Faulted);

        TransitPublishResult result = await publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask().WaitAsync(timeout.Token);

        Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        Assert.Equal(messageId, result.MessageId);
        Assert.Equal(239, result.ResponseCode);
    }

    [Fact]
    public async Task PublishAsync_WhenReconnectDisposesConnectionWhileSubmitWaitsWriteGate_DoesNotFaultPump()
    {
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

        string firstMessageId = "<publisher-replace-blocked-first@example.com>";
        string secondMessageId = "<publisher-replace-blocked-second@example.com>";

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
            async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                try
                {
                    string secondTakethis = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal($"TAKETHIS {secondMessageId}", secondTakethis);
                    _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, $"239 {secondMessageId} transferred");
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("Unexpected EOF", StringComparison.Ordinal))
                {
                }
            },
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        TransitConnection oldConnection = GetPrimaryConnection(publisher);
        SemaphoreSlim writeGate = GetConnectionWriteGate(oldConnection);
        await writeGate.WaitAsync(timeout.Token);

        Task<TransitPublishResult> firstPublish = publisher.PublishAsync(firstMessageId, new byte[] { (byte)'1', (byte)'\n' }, CancellationToken.None).AsTask();

        while (oldConnection.OutstandingSubmissionCount < 1)
        {
            await Task.Delay(10, timeout.Token);
        }

        SetConnectionState(oldConnection, TransitConnectionState.Faulted);
        Task reconnectTask = InvokeReconnectAsync(publisher, slotIndex: 0, CancellationToken.None);

        while (true)
        {
            TransitConnection? current = TryGetPrimaryConnection(publisher);
            if (current is not null && !ReferenceEquals(current, oldConnection))
            {
                break;
            }

            await Task.Delay(10, timeout.Token);
        }

        writeGate.Release();

        TransitPublishResult firstResult = await firstPublish.WaitAsync(timeout.Token);
        await reconnectTask.WaitAsync(timeout.Token);

        Assert.Equal(TransitPublishStatus.Ambiguous, firstResult.Status);

        while (publisher.CurrentState == TransitConnectionState.Connecting)
        {
            await Task.Delay(10, timeout.Token);
        }

        Assert.Equal(TransitConnectionState.Ready, publisher.CurrentState);

        TransitPublishResult secondResult = await publisher.PublishAsync(secondMessageId, new byte[] { (byte)'2', (byte)'\n' }, CancellationToken.None).AsTask().WaitAsync(timeout.Token);

        Assert.Equal(TransitPublishStatus.Accepted, secondResult.Status);
    }

    [Fact]
    public async Task PublishAsync_WhenShutdownBeginsDuringReconnectInitialization_DoesNotInstallReplacementConnection()
    {
        string messageId = "<publisher-reconnect-shutdown-first@example.com>";

        TaskCompletionSource secondSessionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowSecondGreeting = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, _) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                stream.Dispose();
            },
            async (stream, cancellationToken) =>
            {
                secondSessionAccepted.TrySetResult();
                await allowSecondGreeting.Task.WaitAsync(cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        using CancellationTokenSource faultedTimeout = new(TimeSpan.FromSeconds(10));
        while (GetPrimaryConnectionState(publisher) is not TransitConnectionState.Faulted and not TransitConnectionState.Disconnected)
        {
            await Task.Delay(10, faultedTimeout.Token);
        }

        Task<TransitPublishResult> publishTask = publisher.PublishAsync(messageId, new byte[] { (byte)'1', (byte)'\n' }, CancellationToken.None).AsTask();

        using CancellationTokenSource reconnectAcceptedTimeout = new(TimeSpan.FromSeconds(10));
        await secondSessionAccepted.Task.WaitAsync(reconnectAcceptedTimeout.Token);

        Task disposeTask = publisher.DisposeAsync().AsTask();
        allowSecondGreeting.TrySetResult();

        using CancellationTokenSource completeTimeout = new(TimeSpan.FromSeconds(10));
        TransitPublishResult result = await publishTask.WaitAsync(completeTimeout.Token);
        await disposeTask.WaitAsync(completeTimeout.Token);

        Assert.True(result.Status is TransitPublishStatus.Ambiguous or TransitPublishStatus.Canceled);
        Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        Assert.Equal(0, GetPrimaryConnectionCount(publisher));
    }

    [Fact]
    public async Task PublishAsync_WhenReconnectInitializationFails_CompletesSubmissionAmbiguousAndIncrementsAmbiguousMetric()
    {
        string messageId = "<publisher-reconnect-init-fail@example.com>";
        byte[] payload = [(byte)'R', (byte)'\n'];

        await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
        [
            async (stream, _) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                stream.Dispose();
            },
            async (stream, _) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "400 temporary failure");
            },
        ]);

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        using CancellationTokenSource faultedTimeout = new(TimeSpan.FromSeconds(10));
        while (GetPrimaryConnectionState(publisher) is not TransitConnectionState.Faulted and not TransitConnectionState.Disconnected)
        {
            await Task.Delay(10, faultedTimeout.Token);
        }

        using CancellationTokenSource publishTimeout = new(TimeSpan.FromSeconds(10));
        TransitPublishResult result = await publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask().WaitAsync(publishTimeout.Token);

        Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);

        TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 0, outstandingSubmissions: 0);
        Assert.Equal(1, snapshot.TotalArticlesSubmitted);
        Assert.Equal(1, snapshot.TotalArticlesAmbiguous);
    }

    [Fact]
    public async Task PublishAsync_WhenAdmissionCanceledBeforeEnqueue_DoesNotIncrementTotalSubmittedMetric()
    {
        const int queueCapacity = 2048;
        const int expectedAdmittedOutstanding = queueCapacity + 1;

        byte[] payload = [(byte)'M', (byte)'\n'];
        int observedTakethisCount = 0;

        await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
        {
            await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
            await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
            await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
            await FakePublisherServer.WriteLineAsync(stream, ".");
            await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM");
            await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

            string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
            Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);
            _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
            Interlocked.Increment(ref observedTakethisCount);

            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });

        await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
        await publisher.InitializeAsync(CancellationToken.None);

        Task<TransitPublishResult>[] admittedSubmissions = new Task<TransitPublishResult>[expectedAdmittedOutstanding];
        for (int i = 0; i < expectedAdmittedOutstanding; i++)
        {
            string messageId = $"<metric-fill-{i}@example.com>";
            admittedSubmissions[i] = publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask();
        }

        using CancellationTokenSource fillTimeout = new(TimeSpan.FromSeconds(10));
        while (GetQueuedSubmissionCount(publisher) < expectedAdmittedOutstanding)
        {
            await Task.Delay(10, fillTimeout.Token);
        }

        using CancellationTokenSource takethisObservedTimeout = new(TimeSpan.FromSeconds(10));
        while (Volatile.Read(ref observedTakethisCount) < 1)
        {
            await Task.Delay(10, takethisObservedTimeout.Token);
        }

        using CancellationTokenSource blockedCts = new();
        Task<TransitPublishResult> blockedAdmission = publisher.PublishAsync("<metric-blocked@example.com>", payload, blockedCts.Token).AsTask();

        await Task.Delay(100);
        Assert.False(blockedAdmission.IsCompleted);

        blockedCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blockedAdmission);

        TransitTransportSnapshot snapshotBeforeDispose = publisher.CaptureTransportSnapshot(
            activeConnections: 1,
            outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
        Assert.Equal(expectedAdmittedOutstanding, snapshotBeforeDispose.TotalArticlesSubmitted);

        using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(10));
        await publisher.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);

        using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
        await Task.WhenAll(admittedSubmissions).WaitAsync(completionTimeout.Token);

        Assert.Equal(0, GetQueuedSubmissionCount(publisher));
    }

    private static TransitPublisher CreatePublisher(int port, int connectionPoolSize, int perConnectionPipelineDepth = 8)
    {
        BackFillerRuntimeOptions options = new(
            CanonicalBackFillerFqdn: "bf.example.com",
            BackFillerId: 42,
            CanonicalDnsSuffix: "example.com",
            ValidatedLogDirectory: "C:\\logs",
            ValidatedCertificateDirectory: "C:\\certs",
            RabbitMqHosts: ["localhost"],
            RabbitMqPort: 5672,
            RabbitMqEnableSsl: false,
            TransitServerHost: IPAddress.Loopback.ToString(),
            TransitServerPort: port,
            TransitServerUseSsl: false,
            ShutdownGracePeriodSeconds: 60,
            ShutdownDrainQueuedWork: true,
            ShutdownFinishActiveArticles: true,
            RabbitMqMaximumShutdownDrainTimeoutSeconds: 120,
            WriteBatchCoalesceMicroseconds: 250);

        return new TransitPublisher(options, TimeProvider.System, NullLogger<TransitPublisher>.Instance, connectionPoolSize, perConnectionPipelineDepth);
    }

    private static long GetQueuedSubmissionCount(TransitPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        FieldInfo? field = typeof(TransitPublisher).GetField("_queuedSubmissionCount", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);

        object? raw = field.GetValue(publisher);
        Assert.IsType<long>(raw);
        return (long)raw;
    }

    private static int GetPrimaryConnectionCount(TransitPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        FieldInfo? slotsField = typeof(TransitPublisher).GetField("_connectionSlots", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(slotsField);

        object? slotsRaw = slotsField.GetValue(publisher);
        Assert.NotNull(slotsRaw);

        Array? slots = slotsRaw as Array;
        Assert.NotNull(slots);

        if (slots.Length == 0)
        {
            return 0;
        }

        object? firstSlot = slots.GetValue(0);
        Assert.NotNull(firstSlot);

        PropertyInfo? connectionProperty = firstSlot.GetType().GetProperty("Connection", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.NotNull(connectionProperty);

        object? connectionRaw = connectionProperty.GetValue(firstSlot);
        return connectionRaw is null ? 0 : 1;
    }

    private static async Task<Exception?> CaptureExceptionAsync(Task task, CancellationToken cancellationToken)
    {
        try
        {
            await task.WaitAsync(cancellationToken);
            return null;
        }
        catch (Exception ex)
        {
            if (ex is AggregateException aggregate)
            {
                return aggregate.InnerException;
            }

            return ex;
        }
    }

    private static Task InvokeReconnectAsync(TransitPublisher publisher, int slotIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        MethodInfo? reconnect = typeof(TransitPublisher).GetMethod("ReconnectAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(reconnect);

        object? invocation = reconnect.Invoke(publisher, [slotIndex, cancellationToken]);
        Task task = Assert.IsAssignableFrom<Task>(invocation);
        return task;
    }

    private static TransitConnection GetPrimaryConnection(TransitPublisher publisher)
    {
        TransitConnection? connection = TryGetPrimaryConnection(publisher);
        return Assert.IsType<TransitConnection>(connection);
    }

    private static TransitConnection? TryGetPrimaryConnection(TransitPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        FieldInfo? slotsField = typeof(TransitPublisher).GetField("_connectionSlots", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(slotsField);

        object? slotsRaw = slotsField.GetValue(publisher);
        Assert.NotNull(slotsRaw);

        Array slots = Assert.IsAssignableFrom<Array>(slotsRaw);
        Assert.True(slots.Length > 0);

        object? firstSlot = slots.GetValue(0);
        Assert.NotNull(firstSlot);
        PropertyInfo? connectionProperty = firstSlot.GetType().GetProperty("Connection", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.NotNull(connectionProperty);

        object? connectionRaw = connectionProperty.GetValue(firstSlot);
        return connectionRaw as TransitConnection;
    }

    private static SemaphoreSlim GetConnectionWriteGate(TransitConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);

        FieldInfo? writeGateField = typeof(TransitConnection).GetField("_writeGate", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(writeGateField);

        object? writeGateRaw = writeGateField.GetValue(connection);
        return Assert.IsType<SemaphoreSlim>(writeGateRaw);
    }

    private static void SetConnectionState(TransitConnection connection, TransitConnectionState state)
    {
        ArgumentNullException.ThrowIfNull(connection);

        FieldInfo? stateField = typeof(TransitConnection).GetField("_state", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(stateField);
        stateField.SetValue(connection, state);
    }

    private static TransitConnectionState GetPrimaryConnectionState(TransitPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        FieldInfo? slotsField = typeof(TransitPublisher).GetField("_connectionSlots", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(slotsField);

        object? slotsRaw = slotsField.GetValue(publisher);
        Assert.NotNull(slotsRaw);

        Array? slots = slotsRaw as Array;
        Assert.NotNull(slots);
        Assert.True(slots.Length > 0);

        object? firstSlot = slots.GetValue(0);
        Assert.NotNull(firstSlot);

        PropertyInfo? connectionProperty = firstSlot.GetType().GetProperty("Connection", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.NotNull(connectionProperty);

        object? connectionRaw = connectionProperty.GetValue(firstSlot);
        if (connectionRaw is null)
        {
            return TransitConnectionState.Disconnected;
        }

        TransitConnection connection = Assert.IsType<TransitConnection>(connectionRaw);
        return connection.CurrentState;
    }

    private static void ForcePrimaryConnectionState(TransitPublisher publisher, TransitConnectionState state)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        TransitConnection connection = GetPrimaryConnection(publisher);
        SetConnectionState(connection, state);
    }

    private static TransitPublisher CreatePublisherWithLogger(int port, int connectionPoolSize, ILogger<TransitPublisher> logger, int perConnectionPipelineDepth = 8)
    {
        BackFillerRuntimeOptions options = new(
            CanonicalBackFillerFqdn: "bf.example.com",
            BackFillerId: 42,
            CanonicalDnsSuffix: "example.com",
            ValidatedLogDirectory: "C:\\logs",
            ValidatedCertificateDirectory: "C:\\certs",
            RabbitMqHosts: ["localhost"],
            RabbitMqPort: 5672,
            RabbitMqEnableSsl: false,
            TransitServerHost: IPAddress.Loopback.ToString(),
            TransitServerPort: port,
            TransitServerUseSsl: false,
            ShutdownGracePeriodSeconds: 60,
            ShutdownDrainQueuedWork: true,
            ShutdownFinishActiveArticles: true,
            RabbitMqMaximumShutdownDrainTimeoutSeconds: 120,
            WriteBatchCoalesceMicroseconds: 250);

        return new TransitPublisher(options, TimeProvider.System, logger, connectionPoolSize, perConnectionPipelineDepth);
    }

    private sealed class CapturingLoggerProvider
    {
        private readonly object _gate = new();

        internal List<LogEntry> Entries { get; } = [];

        internal ILogger<T> CreateLogger<T>()
        {
            return new CapturingLogger<T>(Entries, _gate);
        }

        internal sealed record LogEntry(EventId EventId, string Message);

        private sealed class CapturingLogger<T>(List<LogEntry> entries, object gate) : ILogger<T>
        {
            private readonly List<LogEntry> _entries = entries;
            private readonly object _gate = gate;

            public IDisposable BeginScope<TState>(TState state) where TState : notnull
            {
                return NullScope.Instance;
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                string message = formatter(state, exception);
                lock (_gate)
                {
                    _entries.Add(new LogEntry(eventId, message));
                }
            }

            private sealed class NullScope : IDisposable
            {
                internal static readonly NullScope Instance = new();

                public void Dispose()
                {
                }
            }
        }
    }

    private sealed class FakePublisherServer : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> _sessions;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _acceptLoop;

        private FakePublisherServer(TcpListener listener, IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> sessions)
        {
            _listener = listener;
            _sessions = sessions;
            _acceptLoop = Task.Run(AcceptLoopAsync);
        }

        internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        internal static Task<FakePublisherServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
        {
            ArgumentNullException.ThrowIfNull(session);
            return StartSessionsAsync([session]);
        }

        internal static async Task<FakePublisherServer> StartSessionsAsync(IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> sessions)
        {
            ArgumentNullException.ThrowIfNull(sessions);

            if (sessions.Count == 0)
            {
                throw new ArgumentException("At least one fake transit session is required.", nameof(sessions));
            }

            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            FakePublisherServer server = new(listener, sessions);
            await Task.Delay(20);
            return server;
        }

        private async Task AcceptLoopAsync()
        {
            try
            {
                List<Task> sessionTasks = [];

                foreach (Func<NetworkStream, CancellationToken, Task> session in _sessions)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token);

                    sessionTasks.Add(Task.Run(async () =>
                    {
                        using (client)
                        using (NetworkStream stream = client.GetStream())
                        {
                            await session(stream, _cts.Token);
                        }
                    }, _cts.Token));
                }

                await Task.WhenAll(sessionTasks);
            }
            catch (OperationCanceledException)
            {
            }
        }

        internal static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
        {
            List<byte> buffer = [];

            while (true)
            {
                byte current = await ReadByteAsync(stream, cancellationToken);
                if (current == (byte)'\n')
                {
                    break;
                }

                buffer.Add(current);
            }

            if (buffer.Count > 0 && buffer[^1] == (byte)'\r')
            {
                buffer.RemoveAt(buffer.Count - 1);
            }

            return Encoding.ASCII.GetString(buffer.ToArray());
        }

        internal static async Task<byte[]> ReadTakethisPayloadAsync(Stream stream, CancellationToken cancellationToken)
        {
            using MemoryStream payload = new();
            bool atLineStart = true;

            while (true)
            {
                byte current = await ReadByteAsync(stream, cancellationToken);

                if (atLineStart && current == (byte)'.')
                {
                    byte next = await ReadByteAsync(stream, cancellationToken);
                    if (next == (byte)'\r')
                    {
                        byte nextNext = await ReadByteAsync(stream, cancellationToken);
                        if (nextNext == (byte)'\n')
                        {
                            break;
                        }

                        await payload.WriteAsync(new byte[] { current, next, nextNext }, cancellationToken);
                        atLineStart = false;
                        continue;
                    }

                    await payload.WriteAsync(new byte[] { next }, cancellationToken);
                    atLineStart = next == (byte)'\n';
                    continue;
                }

                await payload.WriteAsync(new byte[] { current }, cancellationToken);

                if (current == (byte)'\n')
                {
                    atLineStart = true;
                }
                else if (current != (byte)'\r')
                {
                    atLineStart = false;
                }
            }

            return payload.ToArray();
        }

        private static async ValueTask<byte> ReadByteAsync(Stream stream, CancellationToken cancellationToken)
        {
            byte[] single = new byte[1];
            int read = await stream.ReadAsync(single, cancellationToken);
            if (read == 0)
            {
                throw new InvalidOperationException("Unexpected EOF while reading stream data.");
            }

            return single[0];
        }

        internal static async Task ExpectCommandAsync(Stream stream, string expected)
        {
            string line = await ReadLineAsync(stream, CancellationToken.None);
            Assert.Equal(expected, line);
        }

        internal static Task WriteLineAsync(Stream stream, string line)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
            return stream.WriteAsync(bytes).AsTask();
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _listener.Stop();

            try
            {
                await _acceptLoop;
            }
            catch (OperationCanceledException)
            {
            }

            _cts.Dispose();
        }
    }
}
