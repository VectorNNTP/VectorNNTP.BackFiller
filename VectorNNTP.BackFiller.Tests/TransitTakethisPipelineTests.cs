// <copyright file="TransitTakethisPipelineTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit takethis pipeline, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit takethis pipeline test suite.

using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests TAKETHIS framing, byte integrity, and asynchronous response correlation.
    /// </summary>
    public sealed class TransitTakethisPipelineTests
    {
        /// <summary>
        /// Exercises submit takethis async  when accepted  preserves payload bytes and returns accepted behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenAccepted_PreservesPayloadBytesAndReturnsAccepted()
        {
            byte[] payload =
            [
                0x00, 0x01, 0x7F, 0x80, 0xFF,
                (byte)'y', (byte)'E', (byte)'n', (byte)'c',
                (byte)'\r', (byte)'\n', (byte)'.', (byte)'d', (byte)'o', (byte)'t',
                (byte)'\n', (byte)'L', (byte)'i', (byte)'n', (byte)'e', (byte)'\n',
            ];

            string messageId = "<msg-1@example.com>";

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);

                byte[] receivedPayload = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.Equal(payload, receivedPayload);

                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageId} Article transferred OK");
            });

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
        }
        /// <summary>
        /// Exercises submit takethis async  when server rejects  returns rejected behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenServerRejects_ReturnsRejected()
        {
            string messageId = "<msg-rejected@example.com>";
            byte[] payload = [(byte)'R', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);

                byte[] receivedPayload = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.Equal(payload, receivedPayload);

                await FakeTakethisServer.WriteLineAsync(stream, $"439 {messageId} Article not wanted");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L);

            Assert.Equal(TransitPublishStatus.Rejected, result.Status);
            Assert.Equal(439, result.ResponseCode);
            Assert.Equal(messageId, result.MessageId);
        }
        /// <summary>
        /// Exercises submit takethis async  when server returns400  marks ambiguous behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenServerReturns400_MarksAmbiguous()
        {
            string messageId = "<msg-400@example.com>";
            byte[] payload = [(byte)'A', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);

                byte[] receivedPayload = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.Equal(payload, receivedPayload);

                await FakeTakethisServer.WriteLineAsync(stream, $"400 {messageId} Deferred due to transient issue");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L);

            Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);
            Assert.Equal(400, result.ResponseCode);
            Assert.Equal(messageId, result.MessageId);
        }
        /// <summary>
        /// Exercises submit takethis async  when large binary payload  preserves bytes and returns accepted behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenLargeBinaryPayload_PreservesBytesAndReturnsAccepted()
        {
            byte[] payload = BuildLargePayload();
            string messageId = "<msg-large@example.com>";

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);

                byte[] receivedPayload = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.Equal(payload, receivedPayload);

                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageId} Article transferred OK");
            });

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
        }
        /// <summary>
        /// Exercises submit takethis async  when responses out of order  correlates by message id behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenResponsesOutOfOrder_CorrelatesByMessageId()
        {
            string messageA = "<msg-a@example.com>";
            string messageB = "<msg-b@example.com>";

            byte[] payloadA = [(byte)'A', (byte)'\n'];
            byte[] payloadB = [(byte)'B', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisA = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal($"TAKETHIS {messageA}", takethisA);
                byte[] ignoredPayloadA = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.NotNull(ignoredPayloadA);

                string takethisB = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal($"TAKETHIS {messageB}", takethisB);
                byte[] ignoredPayloadB = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.NotNull(ignoredPayloadB);

                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageB} transferred");
                await Task.Delay(10);
                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageA} transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> first = connection.SubmitTakethisAsync(messageA, payloadA, CancellationToken.None, 0L, 0L).AsTask();
            Task<TransitPublishResult> second = connection.SubmitTakethisAsync(messageB, payloadB, CancellationToken.None, 0L, 0L).AsTask();

            TransitPublishResult[] results = await Task.WhenAll(first, second);

            Assert.Contains(results, r => r.MessageId == messageA && r.Status == TransitPublishStatus.Accepted);
            Assert.Contains(results, r => r.MessageId == messageB && r.Status == TransitPublishStatus.Accepted);
        }
        /// <summary>
        /// Exercises submit takethis async  when sixteen concurrent submissions out of order  correlates all by message id behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenSixteenConcurrentSubmissionsOutOfOrder_CorrelatesAllByMessageId()
        {
            /// <summary>
            /// Supplies submission count for the fixture or scenario under test.
            /// </summary>
            const int SubmissionCount = 16;
            string[] messageIds = [.. Enumerable.Range(0, SubmissionCount).Select(static i => $"<msg-{i:D2}@example.com>")];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                Dictionary<string, byte[]> payloadsByMessageId = new(StringComparer.Ordinal);

                for (int i = 0; i < SubmissionCount; i++)
                {
                    string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    Assert.StartsWith("TAKETHIS <msg-", takethisLine, StringComparison.Ordinal);

                    string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                    byte[] payload = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    payloadsByMessageId.Add(messageId, payload);
                }

                for (int i = SubmissionCount - 1; i >= 0; i--)
                {
                    string messageId = messageIds[i];
                    Assert.True(payloadsByMessageId.TryGetValue(messageId, out byte[]? payload));
                    Assert.Equal(new byte[] { (byte)i, (byte)'\n' }, payload);
                    await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                }
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions = [.. messageIds.Select((id, index) => connection.SubmitTakethisAsync(id, new byte[] { (byte)index, (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask())];

            TransitPublishResult[] results = await Task.WhenAll(submissions);

            foreach (string messageId in messageIds)
            {
                Assert.Contains(results, r => r.MessageId == messageId && r.Status == TransitPublishStatus.Accepted && r.ResponseCode == 239);
            }
        }
        /// <summary>
        /// Exercises submit takethis async  when duplicate message id in flight  returns failed for second submission behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenDuplicateMessageIdInFlight_ReturnsFailedForSecondSubmission()
        {
            string messageId = "<msg-duplicate@example.com>";
            byte[] firstPayload = [(byte)'A', (byte)'\n'];
            byte[] secondPayload = [(byte)'B', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);

                byte[] receivedPayload = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.Equal(firstPayload, receivedPayload);

                await Task.Delay(50, cancellationToken);
                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageId} Article transferred OK");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> firstTask = connection.SubmitTakethisAsync(messageId, firstPayload, CancellationToken.None, 0L, 0L).AsTask();
            TransitPublishResult secondResult = await connection.SubmitTakethisAsync(messageId, secondPayload, CancellationToken.None, 0L, 0L);
            TransitPublishResult firstResult = await firstTask;

            Assert.Equal(TransitPublishStatus.Failed, secondResult.Status);
            Assert.Equal(messageId, secondResult.MessageId);
            Assert.Null(secondResult.ResponseCode);

            Assert.Equal(TransitPublishStatus.Accepted, firstResult.Status);
            Assert.Equal(239, firstResult.ResponseCode);
            Assert.Equal(messageId, firstResult.MessageId);
        }
        /// <summary>
        /// Exercises submit takethis async  when second caller canceled while waiting for write gate  does not cancel first in flight submission behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenSecondCallerCanceledWhileWaitingForWriteGate_DoesNotCancelFirstInFlightSubmission()
        {
            string firstMessageId = "<msg-gate-first@example.com>";
            string secondMessageId = "<msg-gate-second@example.com>";

            byte[] firstPayload = [(byte)'1', (byte)'\n'];
            byte[] secondPayload = [(byte)'2', (byte)'\n'];

            TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseFirstResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisOne = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {firstMessageId}", takethisOne);
                byte[] receivedOne = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(firstPayload, receivedOne);

                _ = firstTakethisObserved.TrySetResult();
                await releaseFirstResponse.Task.WaitAsync(cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, $"239 {firstMessageId} transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> firstTask = connection.SubmitTakethisAsync(firstMessageId, firstPayload, CancellationToken.None, 0L, 0L).AsTask();

            using CancellationTokenSource firstObservedTimeout = new(TimeSpan.FromSeconds(10));
            await firstTakethisObserved.Task.WaitAsync(firstObservedTimeout.Token);

            using CancellationTokenSource canceledAdmissionCts = new();
            Task<TransitPublishResult> secondTask = connection.SubmitTakethisAsync(secondMessageId, secondPayload, canceledAdmissionCts.Token, 0L, 0L).AsTask();
            canceledAdmissionCts.Cancel();

            TransitPublishResult secondResult = await secondTask;
            Assert.Equal(TransitPublishStatus.Canceled, secondResult.Status);
            Assert.Equal(secondMessageId, secondResult.MessageId);

            _ = releaseFirstResponse.TrySetResult();

            using CancellationTokenSource firstCompletionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult firstResult = await firstTask.WaitAsync(firstCompletionTimeout.Token);

            Assert.Equal(TransitPublishStatus.Accepted, firstResult.Status);
            Assert.Equal(firstMessageId, firstResult.MessageId);
            Assert.Equal(239, firstResult.ResponseCode);
        }
        /// <summary>
        /// Exercises submit takethis async  when payload does not end with lf  throws behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenPayloadDoesNotEndWithLf_Throws()
        {
            string messageId = "<msg-no-lf@example.com>";
            byte[] payload = [(byte)'X'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                await Task.Delay(100, cancellationToken);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L));

            Assert.Contains("must end with LF", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Exercises submit takethis async  when message id contains cr or lf  throws behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenMessageIdContainsCrOrLf_Throws()
        {
            byte[] payload = [(byte)'X', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                await Task.Delay(100, cancellationToken);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await connection.SubmitTakethisAsync("<bad\r\nmsg@example.com>", payload, CancellationToken.None, 0L, 0L));

            Assert.Contains("must not contain CR or LF", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Exercises submit takethis async  when connection drops  marks outstanding ambiguous behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenConnectionDrops_MarksOutstandingAmbiguous()
        {
            string messageId = "<msg-ambiguous@example.com>";
            byte[] payload = [(byte)'X', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string ignoredLine = await FakeTakethisServer.ReadLineAsync(stream, CancellationToken.None);
                byte[] ignoredPayload = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.False(string.IsNullOrWhiteSpace(ignoredLine));
                Assert.NotNull(ignoredPayload);

                stream.Dispose();
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L);

            Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);
            Assert.Equal(messageId, result.MessageId);
        }
        /// <summary>
        /// Exercises submit takethis async  when takethis response message id is not bracketed  fails connection and completes outstanding ambiguous behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenTakethisResponseMessageIdIsNotBracketed_FailsConnectionAndCompletesOutstandingAmbiguous()
        {
            string messageId = "<msg-malformed-response-id@example.com>";
            byte[] payload = [(byte)'X', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, "239 malformed-message-id transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult result = await connection
                .SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L)
                .AsTask()
                .WaitAsync(completionTimeout.Token);

            Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);
            Assert.Equal(messageId, result.MessageId);
            Assert.Null(result.ResponseCode);
        }
        /// <summary>
        /// Exercises submit takethis async  when server returns known tokenless239 with single outstanding  maps accepted behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenServerReturnsKnownTokenless239WithSingleOutstanding_MapsAccepted()
        {
            string messageId = "<msg-tokenless-239@example.com>";
            byte[] payload = [(byte)'T', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, "239 Article transferred OK");
            });

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
        }
        /// <summary>
        /// Exercises submit takethis async  when server returns tokenless239 with multiple outstanding  fails connection and marks outstanding ambiguous behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenServerReturnsTokenless239WithMultipleOutstanding_FailsConnectionAndMarksOutstandingAmbiguous()
        {
            string firstMessageId = "<msg-tokenless-multi-a@example.com>";
            string secondMessageId = "<msg-tokenless-multi-b@example.com>";
            byte[] firstPayload = [(byte)'A', (byte)'\n'];
            byte[] secondPayload = [(byte)'B', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string firstTakethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {firstMessageId}", firstTakethisLine);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                string secondTakethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {secondMessageId}", secondTakethisLine);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, "239 Article transferred OK");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> firstPublish = connection.SubmitTakethisAsync(firstMessageId, firstPayload, CancellationToken.None, 0L, 0L).AsTask();
            Task<TransitPublishResult> secondPublish = connection.SubmitTakethisAsync(secondMessageId, secondPayload, CancellationToken.None, 0L, 0L).AsTask();

            TransitPublishResult[] results = await Task.WhenAll(firstPublish, secondPublish);

            Assert.Contains(results, static result => result.MessageId == "<msg-tokenless-multi-a@example.com>" && result.Status == TransitPublishStatus.Ambiguous && result.ResponseCode is null);
            Assert.Contains(results, static result => result.MessageId == "<msg-tokenless-multi-b@example.com>" && result.Status == TransitPublishStatus.Ambiguous && result.ResponseCode is null);
        }
        /// <summary>
        /// Exercises submit takethis async  when server returns431 for submitted message  maps to rejected instead of hanging behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenServerReturns431ForSubmittedMessage_MapsToRejectedInsteadOfHanging()
        {
            string messageId = "<msg-431@example.com>";
            byte[] payload = [(byte)'R', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, $"431 {messageId} deferred due to temporary local issue");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(2));
            Task<TransitPublishResult> publishTask = connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L).AsTask();
            TransitPublishResult result = await publishTask.WaitAsync(completionTimeout.Token);

            Assert.Equal(TransitPublishStatus.Rejected, result.Status);
            Assert.Equal(431, result.ResponseCode);
            Assert.Equal(messageId, result.MessageId);
        }
        /// <summary>
        /// Exercises dispose async  when multiple outstanding takethis responses are withheld  terminalizes as ambiguous and completes behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenMultipleOutstandingTakethisResponsesAreWithheld_TerminalizesAsAmbiguousAndCompletes()
        {
            string[] messageIds =
            [
                "<msg-dispose-withheld-0@example.com>",
                "<msg-dispose-withheld-1@example.com>",
                "<msg-dispose-withheld-2@example.com>",
            ];

            TaskCompletionSource allTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource disposeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                for (int i = 0; i < messageIds.Length; i++)
                {
                    string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal($"TAKETHIS {messageIds[i]}", takethisLine);
                    _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                }

                allTakethisObserved.TrySetResult();
                await disposeStarted.Task.WaitAsync(cancellationToken);

                string quit = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("QUIT", quit);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] publishTasks =
            [
                connection.SubmitTakethisAsync(messageIds[0], new byte[] { (byte)'A', (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask(),
                connection.SubmitTakethisAsync(messageIds[1], new byte[] { (byte)'B', (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask(),
                connection.SubmitTakethisAsync(messageIds[2], new byte[] { (byte)'C', (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask(),
            ];

            using CancellationTokenSource observedTimeout = new(TimeSpan.FromSeconds(5));
            await allTakethisObserved.Task.WaitAsync(observedTimeout.Token);

            Task disposeTask = connection.DisposeAsync().AsTask();
            _ = disposeStarted.TrySetResult();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(5));
            TransitPublishResult[] results = await Task.WhenAll(publishTasks).WaitAsync(completionTimeout.Token);

            Assert.Equal(3, results.Length);
            Assert.All(results, static result =>
            {
                Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);
                Assert.Null(result.ResponseCode);
            });

            using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(5));
            await disposeTask.WaitAsync(disposeTimeout.Token);

            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = connection.CaptureDiagnosticsSnapshot();
            Assert.Equal(0, snapshot.CurrentConcurrentSubmissions);
            Assert.Empty(snapshot.OutstandingOperations);
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);
        }
        /// <summary>
        /// Exercises dispose async  when takethis response is correlated before shutdown  leaves definitive result behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenTakethisResponseIsCorrelatedBeforeShutdown_LeavesDefinitiveResult()
        {
            string messageId = "<msg-response-wins@example.com>";
            byte[] payload = [(byte)'R', (byte)'\n'];

            TaskCompletionSource responseCorrelated = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource disposeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethis = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethis);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                _ = responseCorrelated.TrySetResult();
                await disposeStarted.Task.WaitAsync(cancellationToken);

                string quit = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("QUIT", quit);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> publishTask = connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L).AsTask();

            using CancellationTokenSource responseTimeout = new(TimeSpan.FromSeconds(5));
            await responseCorrelated.Task.WaitAsync(responseTimeout.Token);

            Task disposeTask = connection.DisposeAsync().AsTask();
            _ = disposeStarted.TrySetResult();

            TransitPublishResult result = await publishTask;
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
            Assert.Equal(239, result.ResponseCode);

            using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(5));
            await disposeTask.WaitAsync(disposeTimeout.Token);
        }
        /// <summary>
        /// Exercises dispose async  when shutdown wins pending takethis and late response arrives  terminalizes once as ambiguous behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenShutdownWinsPendingTakethisAndLateResponseArrives_TerminalizesOnceAsAmbiguous()
        {
            string messageId = "<msg-shutdown-wins@example.com>";
            byte[] payload = [(byte)'S', (byte)'\n'];

            TaskCompletionSource takethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource disposeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource publishCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethis = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethis);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                _ = takethisObserved.TrySetResult();

                await disposeStarted.Task.WaitAsync(cancellationToken);
                await publishCompleted.Task.WaitAsync(cancellationToken);

                try
                {
                    await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                }
                catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
                {
                }

                try
                {
                    string quit = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal("QUIT", quit);
                }
                catch (Exception ex) when (ex is InvalidOperationException or IOException or SocketException or ObjectDisposedException)
                {
                }
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> publishTask = connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L).AsTask();

            using CancellationTokenSource observedTimeout = new(TimeSpan.FromSeconds(5));
            await takethisObserved.Task.WaitAsync(observedTimeout.Token);

            Task disposeTask = connection.DisposeAsync().AsTask();
            _ = disposeStarted.TrySetResult();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(5));
            TransitPublishResult result = await publishTask.WaitAsync(completionTimeout.Token);
            _ = publishCompleted.TrySetResult();

            Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);
            Assert.Null(result.ResponseCode);

            using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(5));
            await disposeTask.WaitAsync(disposeTimeout.Token);

            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = connection.CaptureDiagnosticsSnapshot();
            Assert.Equal(0, snapshot.CurrentConcurrentSubmissions);
            Assert.Empty(snapshot.OutstandingOperations);
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);
        }
        /// <summary>
        /// Exercises dispose async  when no outstanding takethis  sends quit before transport close behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenNoOutstandingTakethis_SendsQuitBeforeTransportClose()
        {
            TaskCompletionSource quitObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string quit = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("QUIT", quit);
                _ = quitObserved.TrySetResult();
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);
            await connection.DisposeAsync();

            using CancellationTokenSource observedTimeout = new(TimeSpan.FromSeconds(5));
            await quitObserved.Task.WaitAsync(observedTimeout.Token);
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);
        }
        /// <summary>
        /// Exercises dispose async  when outstanding takethis and shutdown begins  terminalizes and then sends quit behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenOutstandingTakethisAndShutdownBegins_TerminalizesAndThenSendsQuit()
        {
            string[] messageIds =
            [
                "<msg-quit-order-0@example.com>",
                "<msg-quit-order-1@example.com>",
                "<msg-quit-order-2@example.com>",
            ];

            TaskCompletionSource allTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource disposeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                for (int i = 0; i < messageIds.Length; i++)
                {
                    string takethis = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal($"TAKETHIS {messageIds[i]}", takethis);
                    _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                }

                _ = allTakethisObserved.TrySetResult();
                await disposeStarted.Task.WaitAsync(cancellationToken);

                string quit = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("QUIT", quit);

                try
                {
                    await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageIds[0]} transferred");
                    await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageIds[1]} transferred");
                    await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageIds[2]} transferred");
                }
                catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
                {
                }
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                connection.SubmitTakethisAsync(messageIds[0], new byte[] { (byte)'A', (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask(),
                connection.SubmitTakethisAsync(messageIds[1], new byte[] { (byte)'B', (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask(),
                connection.SubmitTakethisAsync(messageIds[2], new byte[] { (byte)'C', (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask(),
            ];

            using CancellationTokenSource observedTimeout = new(TimeSpan.FromSeconds(5));
            await allTakethisObserved.Task.WaitAsync(observedTimeout.Token);

            Task disposeTask = connection.DisposeAsync().AsTask();
            _ = disposeStarted.TrySetResult();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(5));
            TransitPublishResult[] results = await Task.WhenAll(submissions).WaitAsync(completionTimeout.Token);
            Assert.All(results, static result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));

            using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(5));
            await disposeTask.WaitAsync(disposeTimeout.Token);
        }
        /// <summary>
        /// Exercises dispose async  when transport already faulted  does not attempt quit behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenTransportAlreadyFaulted_DoesNotAttemptQuit()
        {
            TaskCompletionSource disconnectObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                await FakeTakethisServer.WriteLineAsync(stream, "malformed-status-line");

                byte[] single = new byte[1];
                using CancellationTokenSource readTimeout = new(TimeSpan.FromSeconds(5));
                int read = await stream.ReadAsync(single, readTimeout.Token);
                Assert.Equal(0, read);
                _ = disconnectObserved.TrySetResult();
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            using CancellationTokenSource faultTimeout = new(TimeSpan.FromSeconds(5));
            while (connection.CurrentState != TransitConnectionState.Faulted)
            {
                await Task.Delay(10, faultTimeout.Token);
            }

            await connection.DisposeAsync();

            using CancellationTokenSource disconnectTimeout = new(TimeSpan.FromSeconds(5));
            await disconnectObserved.Task.WaitAsync(disconnectTimeout.Token);
        }
        /// <summary>
        /// Exercises dispose async  when quit server closes immediately after quit  does not fault behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenQuitServerClosesImmediatelyAfterQuit_DoesNotFault()
        {
            string messageId = "<quit-immediate-close@example.com>";
            byte[] payload = [(byte)'Q', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethis = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethis);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageId} transferred");

                string quit = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("QUIT", quit);
                await FakeTakethisServer.WriteLineAsync(stream, "205 Connection closing");
                stream.Dispose();
            });

            TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L);
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
            Assert.Equal(239, result.ResponseCode);

            Exception? disposeException = await Record.ExceptionAsync(async () => await connection.DisposeAsync());
            Assert.Null(disposeException);

            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = connection.CaptureDiagnosticsSnapshot();
            Assert.Equal(0, snapshot.CurrentConcurrentSubmissions);
            Assert.Equal(0, snapshot.SubmissionsAmbiguous);
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);
        }
        /// <summary>
        /// Exercises dispose async  when quit server returns unexpected code  still disposes safely behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenQuitServerReturnsUnexpectedCode_StillDisposesSafely()
        {
            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string quit = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("QUIT", quit);
                await FakeTakethisServer.WriteLineAsync(stream, "500 Command not recognized");
                stream.Dispose();
            });

            TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Exception? disposeException = await Record.ExceptionAsync(async () => await connection.DisposeAsync());
            Assert.Null(disposeException);
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);
        }
        /// <summary>
        /// Exercises submit takethis async  when mixed accepted and rejected out of order  correlates each by message id behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenMixedAcceptedAndRejectedOutOfOrder_CorrelatesEachByMessageId()
        {
            string[] messageIds =
            [
                "<msg-mixed-0@example.com>",
                "<msg-mixed-1@example.com>",
                "<msg-mixed-2@example.com>",
                "<msg-mixed-3@example.com>",
            ];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                for (int i = 0; i < messageIds.Length; i++)
                {
                    string takethisLine = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal($"TAKETHIS {messageIds[i]}", takethisLine);
                    _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                }

                await FakeTakethisServer.WriteLineAsync(stream, $"439 {messageIds[2]} not wanted");
                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageIds[0]} transferred");
                await FakeTakethisServer.WriteLineAsync(stream, $"431 {messageIds[3]} temporary defer");
                await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageIds[1]} transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] tasks = [.. messageIds.Select((id, index) => connection.SubmitTakethisAsync(id, new byte[] { (byte)index, (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask())];

            TransitPublishResult[] results = await Task.WhenAll(tasks);

            Assert.Contains(results, static r => r.MessageId == "<msg-mixed-0@example.com>" && r.Status == TransitPublishStatus.Accepted && r.ResponseCode == 239);
            Assert.Contains(results, static r => r.MessageId == "<msg-mixed-1@example.com>" && r.Status == TransitPublishStatus.Accepted && r.ResponseCode == 239);
            Assert.Contains(results, static r => r.MessageId == "<msg-mixed-2@example.com>" && r.Status == TransitPublishStatus.Rejected && r.ResponseCode == 439);
            Assert.Contains(results, static r => r.MessageId == "<msg-mixed-3@example.com>" && r.Status == TransitPublishStatus.Rejected && r.ResponseCode == 431);
        }
        /// <summary>
        /// Exercises submit takethis async  when response message id is unknown  completes outstanding as ambiguous on connection failure behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenResponseMessageIdIsUnknown_CompletesOutstandingAsAmbiguousOnConnectionFailure()
        {
            string firstMessageId = "<msg-unknown-0@example.com>";
            string secondMessageId = "<msg-unknown-1@example.com>";
            byte[] payload = [(byte)'U', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                for (int i = 0; i < 2; i++)
                {
                    _ = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                }

                await FakeTakethisServer.WriteLineAsync(stream, "239 <msg-not-pending@example.com> transferred");
                stream.Dispose();
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> firstTask = connection.SubmitTakethisAsync(firstMessageId, payload, CancellationToken.None, 0L, 0L).AsTask();
            Task<TransitPublishResult> secondTask = connection.SubmitTakethisAsync(secondMessageId, payload, CancellationToken.None, 0L, 0L).AsTask();

            TransitPublishResult[] results = await Task.WhenAll(firstTask, secondTask);

            Assert.Contains(results, r => r.MessageId == firstMessageId && r.Status == TransitPublishStatus.Ambiguous && r.ResponseCode is null);
            Assert.Contains(results, r => r.MessageId == secondMessageId && r.Status == TransitPublishStatus.Ambiguous && r.ResponseCode is null);
        }
        /// <summary>
        /// Exercises submit takethis async  when duplicate server response arrives  later submission still completes correctly behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenDuplicateServerResponseArrives_LaterSubmissionStillCompletesCorrectly()
        {
            string firstMessageId = "<msg-duplicate-response-0@example.com>";
            string secondMessageId = "<msg-duplicate-response-1@example.com>";
            byte[] payload = [(byte)'X', (byte)'\n'];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                string firstTakethis = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {firstMessageId}", firstTakethis);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, $"239 {firstMessageId} transferred");
                await FakeTakethisServer.WriteLineAsync(stream, $"239 {firstMessageId} transferred");

                string secondTakethis = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {secondMessageId}", secondTakethis);
                _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakeTakethisServer.WriteLineAsync(stream, $"239 {secondMessageId} transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            TransitPublishResult first = await connection.SubmitTakethisAsync(firstMessageId, payload, CancellationToken.None, 0L, 0L);
            TransitPublishResult second = await connection.SubmitTakethisAsync(secondMessageId, payload, CancellationToken.None, 0L, 0L);

            Assert.Equal(TransitPublishStatus.Accepted, first.Status);
            Assert.Equal(TransitPublishStatus.Accepted, second.Status);
            Assert.Equal(239, first.ResponseCode);
            Assert.Equal(239, second.ResponseCode);
        }
        /// <summary>
        /// Exercises submit takethis async  when connection closes with multiple pending  completes all as ambiguous behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenConnectionClosesWithMultiplePending_CompletesAllAsAmbiguous()
        {
            string[] messageIds =
            [
                "<msg-close-0@example.com>",
                "<msg-close-1@example.com>",
                "<msg-close-2@example.com>",
            ];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                for (int i = 0; i < messageIds.Length; i++)
                {
                    _ = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                }

                stream.Dispose();
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] tasks = [.. messageIds.Select((id, index) => connection.SubmitTakethisAsync(id, new byte[] { (byte)index, (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask())];

            TransitPublishResult[] results = await Task.WhenAll(tasks);

            foreach (string messageId in messageIds)
            {
                Assert.Contains(results, result => result.MessageId == messageId && result.Status == TransitPublishStatus.Ambiguous && result.ResponseCode is null);
            }
        }
        /// <summary>
        /// Exercises submit takethis async  when sixteen concurrent token bearing responses  captures max outstanding at least sixteen behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task SubmitTakethisAsync_WhenSixteenConcurrentTokenBearingResponses_CapturesMaxOutstandingAtLeastSixteen()
        {
            /// <summary>
            /// Supplies submission count for the fixture or scenario under test.
            /// </summary>
            const int submissionCount = 16;
            string[] messageIds = [.. Enumerable.Range(0, submissionCount).Select(static i => $"<msg-depth-{i:D2}@example.com>")];

            await using FakeTakethisServer server = await FakeTakethisServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeTakethisServer.WriteLineAsync(stream, "200 transit ready");
                await FakeTakethisServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeTakethisServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeTakethisServer.WriteLineAsync(stream, "STREAMING");
                await FakeTakethisServer.WriteLineAsync(stream, ".");
                await FakeTakethisServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeTakethisServer.WriteLineAsync(stream, "203 Streaming permitted");

                for (int i = 0; i < submissionCount; i++)
                {
                    _ = await FakeTakethisServer.ReadLineAsync(stream, cancellationToken);
                    _ = await FakeTakethisServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                }

                for (int i = submissionCount - 1; i >= 0; i--)
                {
                    await FakeTakethisServer.WriteLineAsync(stream, $"239 {messageIds[i]} transferred");
                }
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitConnection>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions = [.. messageIds.Select((id, index) => connection.SubmitTakethisAsync(id, new byte[] { (byte)index, (byte)'\n' }, CancellationToken.None, 0L, 0L).AsTask())];

            TransitPublishResult[] results = await Task.WhenAll(submissions);
            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = connection.CaptureDiagnosticsSnapshot();

            Assert.Equal(submissionCount, results.Length);
            Assert.All(results, static result =>
            {
                Assert.Equal(TransitPublishStatus.Accepted, result.Status);
                Assert.Equal(239, result.ResponseCode);
            });

            Assert.True(snapshot.MaxConcurrentSubmissions >= submissionCount, $"Expected max outstanding >= {submissionCount}, observed {snapshot.MaxConcurrentSubmissions}.");
        }

        /// <summary>
        /// Verifies the build large payload behavior and expected contract.
        /// </summary>
        private static byte[] BuildLargePayload()
        {
            byte[] payload = new byte[262_145];
            for (int i = 0; i < payload.Length - 1; i++)
            {
                payload[i] = (byte)(i % 256);
            }

            payload[0] = (byte)'.';
            payload[128] = (byte)'\n';
            payload[129] = (byte)'.';
            payload[1024] = (byte)'\r';
            payload[1025] = (byte)'\n';
            payload[1026] = (byte)'.';
            payload[2048] = 0x00;
            payload[4096] = 0x80;
            payload[8192] = 0xFF;
            payload[^1] = (byte)'\n';

            return payload;
        }

        /// <summary>
        /// Covers fake takethis server behavior and invariants exercised by this test suite.
        /// </summary>
        private sealed class FakeTakethisServer : IAsyncDisposable
        {
            /// <summary>
            /// Supplies  listener for the fixture or scenario under test.
            /// </summary>
            private readonly TcpListener _listener;
            /// <summary>
            /// Supplies  session for the fixture or scenario under test.
            /// </summary>
            private readonly Func<NetworkStream, CancellationToken, Task> _session;
            /// <summary>
            /// Exercises  cts behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly CancellationTokenSource _cts = new();
            /// <summary>
            /// Supplies  accept loop for the fixture or scenario under test.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
        /// Verifies the fake takethis server behavior and expected contract.
            /// </summary>
            private FakeTakethisServer(TcpListener listener, Func<NetworkStream, CancellationToken, Task> session)
            {
                _listener = listener;
                _session = session;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Exercises port behavior, including the expected result and failure semantics.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
        /// Verifies the start async behavior and expected contract.
            /// </summary>
            internal static async Task<FakeTakethisServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                FakeTakethisServer server = new(listener, session);
                await Task.Delay(20);
                return server;
            }

            /// <summary>
        /// Verifies the accept loop async behavior and expected contract.
            /// </summary>
            private async Task AcceptLoopAsync()
            {
                try
                {
                    using TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token);
                    using NetworkStream stream = client.GetStream();
                    await _session(stream, _cts.Token);
                }
                catch (OperationCanceledException)
                {
                }
            }

            /// <summary>
        /// Verifies the read line async behavior and expected contract.
            /// </summary>
            internal static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> buffer = [];

                while (true)
                {
                    byte[] single = new byte[1];
                    int read = await stream.ReadAsync(single, cancellationToken);
                    if (read == 0)
                    {
                        throw new InvalidOperationException("Unexpected EOF while reading line.");
                    }

                    if (single[0] == (byte)'\n')
                    {
                        break;
                    }

                    buffer.Add(single[0]);
                }

                if (buffer.Count > 0 && buffer[^1] == (byte)'\r')
                {
                    buffer.RemoveAt(buffer.Count - 1);
                }

                return Encoding.ASCII.GetString([.. buffer]);
            }

            /// <summary>
        /// Verifies the read takethis payload async behavior and expected contract.
            /// </summary>
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

            /// <summary>
        /// Verifies the read byte async behavior and expected contract.
            /// </summary>
            private static async ValueTask<byte> ReadByteAsync(Stream stream, CancellationToken cancellationToken)
            {
                byte[] single = new byte[1];
                int read = await stream.ReadAsync(single, cancellationToken);
                return read == 0 ? throw new InvalidOperationException("Unexpected EOF while reading TAKETHIS payload.") : single[0];
            }

            /// <summary>
        /// Verifies the expect command async behavior and expected contract.
            /// </summary>
            internal static async Task ExpectCommandAsync(Stream stream, string expected)
            {
                string line = await ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal(expected, line);
            }

            /// <summary>
        /// Verifies the write line async behavior and expected contract.
            /// </summary>
            internal static Task WriteLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                return stream.WriteAsync(bytes).AsTask();
            }

            /// <summary>
        /// Verifies the dispose async behavior and expected contract.
            /// </summary>
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
}
