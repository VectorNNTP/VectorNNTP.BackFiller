// <copyright file="TransitPublisherTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit publisher, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit publisher test suite.

using System.Collections;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Transit
{
    /// <summary>
    /// Provides the comprehensive unit/integration-style regression suite for TransitPublisher queue dispatch, streaming protocol behavior, connection ownership, pipeline correlation, lifecycle transitions, shutdown, cancellation, preemption, reconnect handling, and terminal outcome semantics.
    /// </summary>
    /// <remarks>
    /// The suite intentionally combines direct publisher assertions with a deterministic fake transit server and diagnostic/reflection seams. The tests are written around externally meaningful lifecycle invariants while retaining targeted coverage for internal state-machine boundaries that are otherwise difficult to observe. The current validated baseline is 44 test cases: 41 <c>Fact</c> tests plus one <c>Theory</c> executed against three lifecycle exception data rows.
    /// </remarks>
    public sealed class TransitPublisherTests
    {
        /// <summary>
        /// Verifies that an initialized publisher admits a publish request, establishes the demand-driven transit connection, sends the article with the expected payload, and returns a definitive <c>239 Accepted</c> result correlated to the submitted Message-ID.
        /// </summary>
        [Fact]
        public async Task PublishAsync_WhenInitialized_DispatchesQueuedSubmissionAndReturnsAccepted()
        {
            byte[] payload = [(byte)'P', (byte)'\n'];
            string messageId = "<publisher-accept@example.com>";

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
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

        /// <summary>
        /// Verifies that publishing before publisher initialization is rejected as unavailable rather than attempting to create or use a transit connection.
        /// </summary>
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

        /// <summary>
        /// Confirms initialize async  when no queued work  defers connection until publish and returns to idle after quit behavior.
        /// </summary>
        /// <remarks>
        /// Initialization must not eagerly create a TCP session. A later publish must create the session, complete normally, and disposal must perform the protocol <c>QUIT</c> handshake and leave the publisher disconnected with no queued or in-flight work.
        /// </remarks>
        /// <summary>
        /// Confirms the initialize async when no queued work defers connection until publish and returns to idle after quit behavior.
        /// </summary>
        /// <returns>The value returned by the initialize async when no queued work defers connection until publish and returns to idle after quit helper.</returns>
        [Fact]
        public async Task InitializeAsync_WhenNoQueuedWork_DefersConnectionUntilPublishAndReturnsToIdleAfterQuit()
        {
            string messageId = "<publisher-idle-on-demand@example.com>";
            byte[] payload = [(byte)'I', (byte)'D', (byte)'L', (byte)'E', (byte)'\n'];

            TaskCompletionSource sessionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource takethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource quitObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                _ = sessionAccepted.TrySetResult();

                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);

                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(payload, receivedPayload);
                _ = takethisObserved.TrySetResult();

                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");

                string teardownLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("QUIT", teardownLine);
                _ = quitObserved.TrySetResult();
                await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);

            await publisher.InitializeAsync(CancellationToken.None);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot initial = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(0, initial.QueueSnapshot.QueuedItemCount);
            Assert.Equal(0, initial.QueueSnapshot.RetryPendingCount);
            Assert.Equal(0, initial.QueueSnapshot.InFlightCount);
            Assert.Equal(0, initial.TotalReconnects);
            Assert.Equal(0, GetPrimaryConnectionCount(publisher));
            Assert.False(sessionAccepted.Task.IsCompleted);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));

            Task<TransitPublishResult> publishTask = publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask();
            await takethisObserved.Task.WaitAsync(completionTimeout.Token);

            TransitPublishResult result = await publishTask.WaitAsync(completionTimeout.Token);
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
            Assert.Equal(239, result.ResponseCode);
            Assert.Equal(messageId, result.MessageId);

            await publisher.DisposeAsync().AsTask().WaitAsync(completionTimeout.Token);
            await quitObserved.Task.WaitAsync(completionTimeout.Token);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot final = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(0, final.QueueSnapshot.QueuedItemCount);
            Assert.Equal(0, final.QueueSnapshot.RetryPendingCount);
            Assert.Equal(0, final.QueueSnapshot.InFlightCount);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        }

        /// <summary>
        /// Verifies that the fake transit server cancels a session that is blocked waiting for its initial handshake command when the server is disposed.
        /// </summary>
        /// <remarks>
        /// The test explicitly creates a TCP client so the session delegate actually reaches the blocked <c>CAPABILITIES</c> read, then proves that <c>DisposeAsync</c> propagates the server cancellation token into that read.
        /// </remarks>
        /// <summary>
        /// Confirms the fake publisher server dispose async cancels session blocked in handshake read behavior.
        /// </summary>
        /// <returns>The value returned by the fake publisher server dispose async cancels session blocked in handshake read helper.</returns>
        [Fact]
        public async Task FakePublisherServer_DisposeAsync_CancelsSessionBlockedInHandshakeRead()
        {
            TaskCompletionSource handshakeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource handshakeCanceled = new(TaskCreationOptions.RunContinuationsAsynchronously);

            FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                try
                {
                    _ = handshakeEntered.TrySetResult();
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _ = handshakeCanceled.TrySetResult();
                    throw;
                }
            });

            bool disposed = false;
            try
            {
                using TcpClient client = new();
                using CancellationTokenSource connectTimeout = new(TimeSpan.FromSeconds(2));
                await client.ConnectAsync(IPAddress.Loopback, server.Port, connectTimeout.Token);

                using CancellationTokenSource enteredTimeout = new(TimeSpan.FromSeconds(2));
                await handshakeEntered.Task.WaitAsync(enteredTimeout.Token);

                using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(2));
                await server.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);
                disposed = true;
                await handshakeCanceled.Task.WaitAsync(disposeTimeout.Token);
            }
            finally
            {
                if (!disposed)
                {
                    await server.DisposeAsync();
                }
            }
        }

        /// <summary>
        /// Verifies that caller cancellation while admission is blocked does not leave a phantom queued-submission count.
        /// </summary>
        /// <remarks>
        /// The test fills the admission path with real publish demand, proves the additional publish is waiting for admission, cancels that blocked request, and then compares the queued count with the pre-cancellation baseline before disposing the publisher.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when canceled before channel admission does not leak queued submission count behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when canceled before channel admission does not leak queued submission count helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenCanceledBeforeChannelAdmission_DoesNotLeakQueuedSubmissionCount()
        {
            const int QueueCapacity = 2048;
            const int ExpectedAdmittedOutstanding = QueueCapacity + 1;

            byte[] payload = [(byte)'Q', (byte)'\n'];
            int observedTakethisCount = 0;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                _ = Interlocked.Increment(ref observedTakethisCount);

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] admittedSubmissions = new Task<TransitPublishResult>[ExpectedAdmittedOutstanding];
            for (int i = 0; i < ExpectedAdmittedOutstanding; i++)
            {
                string messageId = $"<fill-{i}@example.com>";
                admittedSubmissions[i] = publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask();
            }

            using CancellationTokenSource fillTimeout = new(TimeSpan.FromSeconds(10));
            while (Volatile.Read(ref observedTakethisCount) < 1)
            {
                await Task.Delay(10, fillTimeout.Token);
            }

            TransitTransportSnapshot fillSnapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            while (fillSnapshot.TotalArticlesSubmitted < ExpectedAdmittedOutstanding)
            {
                await Task.Delay(10, fillTimeout.Token);
                fillSnapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            }

            long baselineQueuedSubmissionCount = GetQueuedSubmissionCount(publisher);
            long baselineAdmissionWaitCount = publisher.CaptureConnectionDiagnosticsSnapshot().QueueSnapshot.AdmissionWaitCount;

            using CancellationTokenSource blockedCts = new();
            Task<TransitPublishResult> blockedAdmission = publisher.PublishAsync("<blocked-admission@example.com>", payload, blockedCts.Token).AsTask();

            using CancellationTokenSource blockedObservedTimeout = new(TimeSpan.FromSeconds(10));
            while (publisher.CaptureConnectionDiagnosticsSnapshot().QueueSnapshot.AdmissionWaitCount <= baselineAdmissionWaitCount)
            {
                Assert.False(blockedAdmission.IsCompleted);
                await Task.Delay(10, blockedObservedTimeout.Token);
            }

            blockedCts.Cancel();
            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blockedAdmission);

            Assert.Equal(baselineQueuedSubmissionCount, GetQueuedSubmissionCount(publisher));
            Assert.Equal(1, Volatile.Read(ref observedTakethisCount));

            using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            _ = await Task.WhenAll(admittedSubmissions).WaitAsync(completionTimeout.Token);

            Assert.Equal(0, GetQueuedSubmissionCount(publisher));
        }

        /// <summary>
        /// Verifies that disposing the publisher while an article is already in-flight completes the publish task with an uncertainty-safe terminal outcome.
        /// </summary>
        /// <remarks>
        /// The fake server deliberately withholds the <c>239</c> response. Disposal must therefore complete the publish as <c>Ambiguous</c> and clear queued, retry-pending, and in-flight accounting.
        /// </remarks>
        /// <summary>
        /// Confirms the dispose async when in flight takethis response never arrives completes and finalizes publish task behavior.
        /// </summary>
        /// <returns>The value returned by the dispose async when in flight takethis response never arrives completes and finalizes publish task helper.</returns>
        [Fact]
        public async Task DisposeAsync_WhenInFlightTakethisResponseNeverArrives_CompletesAndFinalizesPublishTask()
        {
            string messageId = "<publisher-dispose-inflight@example.com>";
            byte[] payload = [(byte)'I', (byte)'\n'];

            TaskCompletionSource takethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                _ = takethisObserved.TrySetResult();

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

            Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);
            Assert.Equal(messageId, result.MessageId);
            Assert.Null(result.ResponseCode);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(0, snapshot.QueueSnapshot.QueuedItemCount);
            Assert.Equal(0, snapshot.QueueSnapshot.RetryPendingCount);
            Assert.Equal(0, snapshot.QueueSnapshot.InFlightCount);
        }

        /// <summary>
        /// Verifies that two concurrent submissions can be dispatched concurrently across a two-connection publisher pool.
        /// </summary>
        /// <remarks>
        /// The fake server records the two independent sessions, validates each Message-ID and payload, and the test requires both publishes to be accepted without reconnects.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when two concurrent submissions uses two connection pool behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when two concurrent submissions uses two connection pool helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenTwoConcurrentSubmissions_UsesTwoConnectionPool()
        {
            string firstMessageId = "<publisher-parallel-1@example.com>";
            string secondMessageId = "<publisher-parallel-2@example.com>";

            byte[] firstPayload = [(byte)'A', (byte)'\n'];
            byte[] secondPayload = [(byte)'B', (byte)'\n'];

            Dictionary<string, byte[]> expectedPayloadByMessageId = new(StringComparer.Ordinal)
            {
                [firstMessageId] = firstPayload,
                [secondMessageId] = secondPayload,
            };

            TaskCompletionSource firstSawTakethis = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource secondSawTakethis = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowResponses = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<string> firstSessionObservedMessageIds = [];
            List<string> secondSessionObservedMessageIds = [];

            async Task HandleSessionAsync(
                NetworkStream stream,
                CancellationToken cancellationToken,
                TaskCompletionSource observedSignal,
                List<string> observedMessageIds)
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                while (true)
                {
                    string commandLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(commandLine, "QUIT", StringComparison.Ordinal))
                    {
                        await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                        return;
                    }

                    Assert.StartsWith("TAKETHIS ", commandLine, StringComparison.Ordinal);

                    string messageId = commandLine["TAKETHIS ".Length..];
                    Assert.True(expectedPayloadByMessageId.TryGetValue(messageId, out byte[]? expectedPayload), $"Unexpected Message-ID {messageId}");

                    byte[] payload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.Equal(expectedPayload, payload);

                    observedMessageIds.Add(messageId);
                    _ = observedSignal.TrySetResult();

                    await allowResponses.Task.WaitAsync(cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                }
            }

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                (stream, cancellationToken) => HandleSessionAsync(stream, cancellationToken, firstSawTakethis, firstSessionObservedMessageIds),
                (stream, cancellationToken) => HandleSessionAsync(stream, cancellationToken, secondSawTakethis, secondSessionObservedMessageIds),
            ]);

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 2, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> first = publisher.PublishAsync(firstMessageId, firstPayload, CancellationToken.None).AsTask();
            Task<TransitPublishResult> second = publisher.PublishAsync(secondMessageId, secondPayload, CancellationToken.None).AsTask();

            using CancellationTokenSource observedTimeout = new(TimeSpan.FromSeconds(10));
            await WaitForOutstandingAwaitingResponsesAsync(publisher, minimumAwaitingResponses: 2, observedTimeout.Token);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot observedSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            TransitPublisher.ConnectionDiagnosticsEntry[] participatingConnections = observedSnapshot.Connections
                .Where(static entry => entry.Snapshot.OutstandingOperations.Any(static operation => operation.WaitingFor239Response))
                .ToArray();
            Assert.Equal(2, participatingConnections.Length);

            string[] awaitingMessageIds = participatingConnections
                .SelectMany(static entry => entry.Snapshot.OutstandingOperations)
                .Where(static operation => operation.WaitingFor239Response)
                .Select(static operation => operation.MessageId)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            Assert.Contains(firstMessageId, awaitingMessageIds);
            Assert.Contains(secondMessageId, awaitingMessageIds);

            await Task.WhenAll(
                firstSawTakethis.Task.WaitAsync(observedTimeout.Token),
                secondSawTakethis.Task.WaitAsync(observedTimeout.Token));

            allowResponses.TrySetResult();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult[] results = await Task.WhenAll(first, second).WaitAsync(completionTimeout.Token);

            Assert.Contains(results, r => r.MessageId == firstMessageId && r.Status == TransitPublishStatus.Accepted);
            Assert.Contains(results, r => r.MessageId == secondMessageId && r.Status == TransitPublishStatus.Accepted);

            Assert.NotEmpty(firstSessionObservedMessageIds);
            Assert.NotEmpty(secondSessionObservedMessageIds);

            TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 2, outstandingSubmissions: 0);
            Assert.Equal(2, snapshot.TotalArticlesSubmitted);
            Assert.Equal(2, snapshot.TotalArticlesAccepted);
            Assert.Equal(0, snapshot.TotalReconnects);
        }

        /// <summary>
        /// Verifies that caller cancellation after admission cancels the caller-facing wait without preventing the admitted article from reaching its definitive server outcome.
        /// </summary>
        /// <remarks>
        /// The test also verifies that the publisher eventually records the accepted article and clears its active and in-flight bookkeeping, demonstrating that caller cancellation does not strand admitted work.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when caller cancels after admission still logs final outcome behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when caller cancels after admission still logs final outcome helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenCallerCancelsAfterAdmission_StillLogsFinalOutcome()
        {
            string messageId = "<publisher-cancel-after-admission@example.com>";
            byte[] payload = [(byte)'C', (byte)'\n'];
            CapturingLoggerProvider provider = new();
            TaskCompletionSource<bool> response239Sent = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);

                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(payload, receivedPayload);

                await Task.Delay(80, cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                _ = response239Sent.TrySetResult(true);
            });

            await using TransitPublisher publisher = CreatePublisherWithLogger(server.Port, connectionPoolSize: 1, provider.CreateLogger<TransitPublisher>());
            await publisher.InitializeAsync(CancellationToken.None);

            using CancellationTokenSource cts = new();
            ValueTask<TransitPublishResult> pending = publisher.PublishAsync(messageId, payload, cts.Token);

            using CancellationTokenSource admissionTimeout = new(TimeSpan.FromSeconds(5));
            await WaitForOutstandingAwaitingResponsesAsync(publisher, minimumAwaitingResponses: 1, admissionTimeout.Token);

            cts.Cancel();
            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(pending.AsTask);

            using CancellationTokenSource responseTimeout = new(TimeSpan.FromSeconds(5));
            _ = await response239Sent.Task.WaitAsync(responseTimeout.Token);

            using CancellationTokenSource terminalizationTimeout = new(TimeSpan.FromSeconds(5));
            TransitTransportSnapshot? finalSnapshot = null;
            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot? finalDiagnostics = null;
            while (true)
            {
                terminalizationTimeout.Token.ThrowIfCancellationRequested();
                finalSnapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
                finalDiagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();

                if (finalSnapshot.TotalArticlesAccepted >= 1
                    && finalDiagnostics.QueueSnapshot.InFlightCount == 0
                    && GetActiveSubmissionCount(publisher) == 0)
                {
                    break;
                }

                await Task.Yield();
            }

            Assert.NotNull(finalSnapshot);
            Assert.NotNull(finalDiagnostics);
            Assert.Equal(1, finalSnapshot.TotalArticlesAccepted);
            Assert.Equal(0, finalDiagnostics.QueueSnapshot.InFlightCount);
            Assert.Equal(0, GetActiveSubmissionCount(publisher));
        }

        /// <summary>
        /// Verifies that a single transit connection can materialize multiple <c>TAKETHIS</c> submissions before any <c>239</c> response is received when its pipeline depth permits it.
        /// </summary>
        /// <remarks>
        /// The test proves two distinct submissions are concurrently awaiting responses, validates their payloads, then releases both responses and verifies the third queued submission progresses.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when single connection pipeline depth greater than one sends multiple takethis before first response behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when single connection pipeline depth greater than one sends multiple takethis before first response helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenSingleConnectionPipelineDepthGreaterThanOne_SendsMultipleTakethisBeforeFirstResponse()
        {
            string[] messageIds =
            [
                "<publisher-pipeline-1@example.com>",
                "<publisher-pipeline-2@example.com>",
                "<publisher-pipeline-3@example.com>",
            ];

            TransitPublisher? publisher = null;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 2, cancellationToken);

                List<string> observed = [];
                Dictionary<string, byte[]> payloadByMessageId = new(StringComparer.Ordinal)
                {
                    [messageIds[0]] = [(byte)'1', (byte)'\n'],
                    [messageIds[1]] = [(byte)'2', (byte)'\n'],
                    [messageIds[2]] = [(byte)'3', (byte)'\n'],
                };

                for (int i = 0; i < 2; i++)
                {
                    string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);

                    string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                    Assert.Contains(messageId, messageIds);
                    observed.Add(messageId);

                    byte[] payload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.True(payloadByMessageId.TryGetValue(messageId, out byte[]? expectedPayload));
                    Assert.Equal(expectedPayload, payload);
                }

                Assert.Equal(2, observed.Distinct(StringComparer.Ordinal).Count());
                await FakePublisherServer.WriteLineAsync(stream, $"239 {observed[0]} transferred");
                await FakePublisherServer.WriteLineAsync(stream, $"239 {observed[1]} transferred");

                string thirdTakethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.StartsWith("TAKETHIS ", thirdTakethisLine, StringComparison.Ordinal);
                string thirdMessageId = thirdTakethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.Contains(thirdMessageId, messageIds);
                Assert.DoesNotContain(thirdMessageId, observed);

                byte[] thirdPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.True(payloadByMessageId.TryGetValue(thirdMessageId, out byte[]? expectedThirdPayload));
                Assert.Equal(expectedThirdPayload, thirdPayload);

                await FakePublisherServer.WriteLineAsync(stream, $"239 {thirdMessageId} transferred");
            });

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                publisherInstance.PublishAsync(messageIds[0], new byte[] { (byte)'1', (byte)'\n' }, CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[1], new byte[] { (byte)'2', (byte)'\n' }, CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[2], new byte[] { (byte)'3', (byte)'\n' }, CancellationToken.None).AsTask(),
            ];

            TransitPublishResult[] results = await Task.WhenAll(submissions);
            foreach (string messageId in messageIds)
            {
                Assert.Contains(results, r => r.MessageId == messageId && r.Status == TransitPublishStatus.Accepted && r.ResponseCode == 239);
            }
        }

        /// <summary>
        /// Verifies that pipelined <c>239</c> responses are correlated by Message-ID rather than by response arrival order.
        /// </summary>
        /// <remarks>
        /// The server deliberately responds to the third, first, and second articles in a different order from transmission. Each publish must still complete against its own Message-ID.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when single connection responses out of order correlates by message id behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when single connection responses out of order correlates by message id helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenSingleConnectionResponsesOutOfOrder_CorrelatesByMessageId()
        {
            string messageA = "<publisher-outoforder-a@example.com>";
            string messageB = "<publisher-outoforder-b@example.com>";
            string messageC = "<publisher-outoforder-c@example.com>";

            TransitPublisher? publisher = null;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 3, cancellationToken);

                string takethisA = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageA}", takethisA);
                byte[] payloadA = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal([(byte)'A', (byte)'\n'], payloadA);

                string takethisB = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageB}", takethisB);
                byte[] payloadB = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal([(byte)'B', (byte)'\n'], payloadB);

                string takethisC = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageC}", takethisC);
                byte[] payloadC = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal([(byte)'C', (byte)'\n'], payloadC);

                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageC} transferred");
                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageA} transferred");
                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageB} transferred");
            });

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> first = publisherInstance.PublishAsync(messageA, new byte[] { (byte)'A', (byte)'\n' }, CancellationToken.None).AsTask();
            Task<TransitPublishResult> second = publisherInstance.PublishAsync(messageB, new byte[] { (byte)'B', (byte)'\n' }, CancellationToken.None).AsTask();
            Task<TransitPublishResult> third = publisherInstance.PublishAsync(messageC, new byte[] { (byte)'C', (byte)'\n' }, CancellationToken.None).AsTask();

            TransitPublishResult[] results = await Task.WhenAll(first, second, third);

            Assert.Contains(results, r => r.MessageId == messageA && r.Status == TransitPublishStatus.Accepted);
            Assert.Contains(results, r => r.MessageId == messageB && r.Status == TransitPublishStatus.Accepted);
            Assert.Contains(results, r => r.MessageId == messageC && r.Status == TransitPublishStatus.Accepted);
        }

        /// <summary>
        /// Verifies depth-two pipeline saturation and forward progress when additional demand exists beyond the current claimed batch.
        /// </summary>
        /// <remarks>
        /// The first two submissions must occupy the two available pipeline slots while the third remains queued rather than awaiting a response. Releasing the first batch responses must allow subsequent demand to materialize and all four publishes to complete without stranded work.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when pipeline depth two does not deadlock when third intent cannot yet materialize behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when pipeline depth two does not deadlock when third intent cannot yet materialize helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenPipelineDepthTwo_DoesNotDeadlockWhenThirdIntentCannotYetMaterialize()
        {
            string[] messageIds =
            [
                "<publisher-depth-1@example.com>",
                "<publisher-depth-2@example.com>",
                "<publisher-depth-3@example.com>",
                "<publisher-depth-4@example.com>",
            ];

            byte[][] payloads =
            [
                [(byte)'a', (byte)'\n'],
                [(byte)'b', (byte)'\n'],
                [(byte)'c', (byte)'\n'],
                [(byte)'d', (byte)'\n'],
            ];

            TimeSpan testTimeout = TimeSpan.FromSeconds(15);
            TimeSpan serverReadTimeout = TimeSpan.FromSeconds(5);
            List<string> observedTakethisMessageIds = [];
            TaskCompletionSource firstTwoObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string[]> firstTwoMessageIdsCaptured = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowFirstResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource firstBatchResponsesSent = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource subsequentTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            string diagnostics = string.Empty;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                static bool TryExtractTakethisMessageId(string commandLine, out string? messageId)
                {
                    if (commandLine.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        messageId = commandLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                        return true;
                    }

                    messageId = null;
                    return false;
                }

                string BuildServerDiagnostics(string expectedStage, Exception? ex = null)
                {
                    StringBuilder builder = new();
                    builder.Append("stage=").Append(expectedStage)
                        .Append(" pipelineDepth=2")
                        .Append(" observedTakethisCount=").Append(observedTakethisMessageIds.Count)
                        .Append(" observedMessageIds=").Append(string.Join(",", observedTakethisMessageIds));

                    if (ex is not null)
                    {
                        builder.Append(" exception=").Append(ex.GetType().Name)
                            .Append(": ").Append(ex.Message);
                    }

                    return builder.ToString();
                }

                async Task<string> ReadCommandWithTimeoutAsync(string expectedStage)
                {
                    using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeout.CancelAfter(serverReadTimeout);

                    try
                    {
                        return await FakePublisherServer.ReadLineAsync(stream, timeout.Token);
                    }
                    catch (OperationCanceledException ex) when (timeout.IsCancellationRequested)
                    {
                        throw new Xunit.Sdk.XunitException($"Timed out waiting for {expectedStage}. {BuildServerDiagnostics(expectedStage, ex)}");
                    }
                }

                async Task<byte[]> ReadTakethisPayloadWithTimeoutAsync(string expectedStage)
                {
                    using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeout.CancelAfter(serverReadTimeout);

                    try
                    {
                        return await FakePublisherServer.ReadTakethisPayloadAsync(stream, timeout.Token);
                    }
                    catch (OperationCanceledException ex) when (timeout.IsCancellationRequested)
                    {
                        throw new Xunit.Sdk.XunitException($"Timed out waiting for payload at {expectedStage}. {BuildServerDiagnostics(expectedStage, ex)}");
                    }
                }

                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                Dictionary<string, byte[]> expectedPayloadByMessageId = new(StringComparer.Ordinal)
                {
                    [messageIds[0]] = payloads[0],
                    [messageIds[1]] = payloads[1],
                    [messageIds[2]] = payloads[2],
                    [messageIds[3]] = payloads[3],
                };

                HashSet<string> responded = new(StringComparer.Ordinal);
                List<string> firstTwo = [];

                while (firstTwo.Count < 2)
                {
                    string commandLine = await ReadCommandWithTimeoutAsync($"initial TAKETHIS #{firstTwo.Count + 1}");
                    if (!TryExtractTakethisMessageId(commandLine, out string? messageId) || messageId is null)
                    {
                        throw new InvalidOperationException($"Unexpected protocol command in {nameof(PublishAsync_WhenPipelineDepthTwo_DoesNotDeadlockWhenThirdIntentCannotYetMaterialize)} before initial saturation: '{commandLine}'");
                    }

                    observedTakethisMessageIds.Add(messageId);
                    firstTwo.Add(messageId);
                    Assert.True(expectedPayloadByMessageId.TryGetValue(messageId, out byte[]? expectedPayload));
                    byte[] payload = await ReadTakethisPayloadWithTimeoutAsync($"payload for {messageId}");
                    Assert.Equal(expectedPayload, payload);
                }

                firstTwoMessageIdsCaptured.TrySetResult([firstTwo[0], firstTwo[1]]);
                firstTwoObserved.TrySetResult();

                await allowFirstResponse.Task.WaitAsync(cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, $"239 {firstTwo[0]} transferred");
                responded.Add(firstTwo[0]);
                await FakePublisherServer.WriteLineAsync(stream, $"239 {firstTwo[1]} transferred");
                responded.Add(firstTwo[1]);
                firstBatchResponsesSent.TrySetResult();

                while (responded.Count < messageIds.Length)
                {
                    string commandLine;
                    try
                    {
                        commandLine = await ReadCommandWithTimeoutAsync("remaining TAKETHIS or QUIT");
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("Unexpected EOF while reading stream data.", StringComparison.Ordinal) && responded.Count == messageIds.Length)
                    {
                        diagnostics = BuildServerDiagnostics("completed-eof", ex);
                        return;
                    }

                    if (string.Equals(commandLine, "QUIT", StringComparison.Ordinal))
                    {
                        if (responded.Count == messageIds.Length)
                        {
                            diagnostics = BuildServerDiagnostics("completed-quit");
                            await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                            return;
                        }

                        throw new InvalidOperationException($"Unexpected QUIT before all definitive responses in {nameof(PublishAsync_WhenPipelineDepthTwo_DoesNotDeadlockWhenThirdIntentCannotYetMaterialize)}. {BuildServerDiagnostics("quit-before-complete")}");
                    }

                    if (!TryExtractTakethisMessageId(commandLine, out string? messageId) || messageId is null)
                    {
                        throw new InvalidOperationException($"Unexpected protocol command in {nameof(PublishAsync_WhenPipelineDepthTwo_DoesNotDeadlockWhenThirdIntentCannotYetMaterialize)}: '{commandLine}'");
                    }

                    observedTakethisMessageIds.Add(messageId);
                    Assert.True(expectedPayloadByMessageId.TryGetValue(messageId, out byte[]? expectedPayload));
                    byte[] payload = await ReadTakethisPayloadWithTimeoutAsync($"payload for {messageId}");
                    Assert.Equal(expectedPayload, payload);

                    if (!firstTwo.Contains(messageId, StringComparer.Ordinal))
                    {
                        subsequentTakethisObserved.TrySetResult();
                    }

                    if (responded.Add(messageId))
                    {
                        await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                    }
                }

                diagnostics = BuildServerDiagnostics("completed-all-responses");
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 2);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                publisher.PublishAsync(messageIds[0], payloads[0], CancellationToken.None).AsTask(),
                publisher.PublishAsync(messageIds[1], payloads[1], CancellationToken.None).AsTask(),
                publisher.PublishAsync(messageIds[2], payloads[2], CancellationToken.None).AsTask(),
                publisher.PublishAsync(messageIds[3], payloads[3], CancellationToken.None).AsTask(),
            ];

            using CancellationTokenSource completionTimeout = new(testTimeout);

            await firstTwoObserved.Task.WaitAsync(completionTimeout.Token);
            string[] firstTwoMessageIds = await firstTwoMessageIdsCaptured.Task.WaitAsync(completionTimeout.Token);

            while (true)
            {
                completionTimeout.Token.ThrowIfCancellationRequested();

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                bool firstAwaiting = snapshot.Connections.Any(entry => entry.Snapshot.OutstandingOperations.Any(op => string.Equals(op.MessageId, firstTwoMessageIds[0], StringComparison.Ordinal) && op.WaitingFor239Response));
                bool secondAwaiting = snapshot.Connections.Any(entry => entry.Snapshot.OutstandingOperations.Any(op => string.Equals(op.MessageId, firstTwoMessageIds[1], StringComparison.Ordinal) && op.WaitingFor239Response));
                bool thirdAwaiting = snapshot.Connections.Any(entry => entry.Snapshot.OutstandingOperations.Any(op => string.Equals(op.MessageId, messageIds[2], StringComparison.Ordinal) && op.WaitingFor239Response));

                if (firstAwaiting && secondAwaiting && !thirdAwaiting)
                {
                    break;
                }

                await Task.Yield();
            }

            allowFirstResponse.TrySetResult();
            await firstBatchResponsesSent.Task.WaitAsync(completionTimeout.Token);

            while (true)
            {
                completionTimeout.Token.ThrowIfCancellationRequested();

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                bool firstStillOutstanding = snapshot.Connections.Any(entry => entry.Snapshot.OutstandingOperations.Any(op => string.Equals(op.MessageId, firstTwoMessageIds[0], StringComparison.Ordinal)));
                bool secondStillOutstanding = snapshot.Connections.Any(entry => entry.Snapshot.OutstandingOperations.Any(op => string.Equals(op.MessageId, firstTwoMessageIds[1], StringComparison.Ordinal)));

                if (!firstStillOutstanding && !secondStillOutstanding)
                {
                    break;
                }

                await Task.Yield();
            }

            await subsequentTakethisObserved.Task.WaitAsync(completionTimeout.Token);

            TransitPublishResult[] results;
            try
            {
                results = await Task.WhenAll(submissions).WaitAsync(completionTimeout.Token);
            }
            catch (OperationCanceledException ex) when (completionTimeout.IsCancellationRequested)
            {
                TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
                string message = $"Timed out waiting for PublishAsync completion. pipelineDepth=2 diagnostics={diagnostics} queuedSubmissions={GetQueuedSubmissionCount(publisher)} activeSubmissions={GetActiveSubmissionCount(publisher)} snapshotActiveConnections={snapshot.ActiveConnections} snapshotOutstandingSubmissions={snapshot.OutstandingSubmissions} totalSubmitted={snapshot.TotalArticlesSubmitted} totalAccepted={snapshot.TotalArticlesAccepted} totalAmbiguous={snapshot.TotalArticlesAmbiguous} totalReconnects={snapshot.TotalReconnects} primaryConnectionState={GetPrimaryConnectionState(publisher)} exception={ex.Message}";
                throw new Xunit.Sdk.XunitException(message);
            }

            Assert.Equal(4, observedTakethisMessageIds.Count);
            Assert.Equal(4, observedTakethisMessageIds.Distinct(StringComparer.Ordinal).Count());
            Assert.All(messageIds, messageId => Assert.Contains(messageId, observedTakethisMessageIds));

            Assert.All(results, result =>
            {
                Assert.Equal(TransitPublishStatus.Accepted, result.Status);
                Assert.Equal(239, result.ResponseCode);
            });

            TransitTransportSnapshot finalSnapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: 0);
            Assert.Equal(4, finalSnapshot.TotalArticlesSubmitted);
            Assert.Equal(4, finalSnapshot.TotalArticlesAccepted);
            Assert.Equal(0, finalSnapshot.TotalArticlesRejected);
            Assert.Equal(0, finalSnapshot.TotalArticlesAmbiguous);
            Assert.Equal(0, finalSnapshot.OutstandingSubmissions);
            Assert.Equal(0, GetActiveSubmissionCount(publisher));
        }

        /// <summary>
        /// Verifies that transit payload framing preserves binary bytes and lines beginning with dots across a <c>TAKETHIS</c> submission.
        /// </summary>
        /// <remarks>
        /// The fake server decodes the dot-stuffed article framing and compares the resulting bytes with the original payload before returning a definitive acceptance.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when payload contains binary and leading dots preserves byte integrity behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when payload contains binary and leading dots preserves byte integrity helper.</returns>
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
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
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

        /// <summary>
        /// Verifies that when a connection drops while multiple submissions remain unresolved, every affected submission receives the uncertainty-safe <c>Ambiguous</c> outcome.
        /// </summary>
        /// <remarks>
        /// The test is specifically checking terminalization and cleanup of all outstanding operations rather than retrying work whose definitive server outcome is unknown.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when multiple outstanding and connection drops completes all as ambiguous behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when multiple outstanding and connection drops completes all as ambiguous helper.</returns>
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

            TransitPublisher? publisher = null;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 2, cancellationToken);

                HashSet<string> observed = new(StringComparer.Ordinal);

                for (int i = 0; i < 2; i++)
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

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                publisherInstance.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
            ];

            TransitPublishResult[] results = await Task.WhenAll(submissions);

            Assert.All(results, result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));
            Assert.All(results, result => Assert.Null(result.ResponseCode));
        }

        /// <summary>
        /// Verifies mixed definitive and uncertain outcomes when a connection responds to one pipelined article and then disconnects.
        /// </summary>
        /// <remarks>
        /// The acknowledged article must remain <c>Accepted</c>, while every article without a definitive response must become <c>Ambiguous</c>.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when partial response then disconnect leaves unanswered submissions ambiguous behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when partial response then disconnect leaves unanswered submissions ambiguous helper.</returns>
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
            TransitPublisher? publisher = null;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 1, cancellationToken);

                string protocolLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                if (!protocolLine.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Unexpected protocol command in {nameof(PublishAsync_WhenPartialResponseThenDisconnect_LeavesUnansweredSubmissionsAmbiguous)}: '{protocolLine}'");
                }

                string messageId = protocolLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                Assert.True(payloads.TryGetValue(messageId, out byte[]? expectedPayload));
                byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(expectedPayload, receivedPayload);

                firstAcceptedMessageId = messageId;
                await FakePublisherServer.WriteLineAsync(stream, $"239 {firstAcceptedMessageId} transferred");
                stream.Dispose();
            });

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                publisherInstance.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
            ];

            TransitPublishResult[] results = await Task.WhenAll(submissions);

            Assert.NotNull(firstAcceptedMessageId);
            Assert.Contains(results, result => result.MessageId == firstAcceptedMessageId && result.Status == TransitPublishStatus.Accepted && result.ResponseCode == 239);

            TransitPublishResult[] ambiguous = results.Where(result => result.MessageId != firstAcceptedMessageId).ToArray();
            Assert.Equal(2, ambiguous.Length);
            Assert.All(ambiguous, result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));
        }

        /// <summary>
        /// Verifies Message-ID correlation when a subset of pipelined responses arrives out of order before the connection disconnects.
        /// </summary>
        /// <remarks>
        /// Two specifically answered Message-IDs must be accepted even though their responses arrive out of order; only the unanswered submission may become <c>Ambiguous</c>.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when out of order partial responses then disconnect completes only unanswered submission as ambiguous behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when out of order partial responses then disconnect completes only unanswered submission as ambiguous helper.</returns>
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
            TransitPublisher? publisher = null;

            Task<TransitPublishResult>[] submissions = [];

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 2, cancellationToken);

                List<string> observed = [];

                while (observed.Count < 2)
                {
                    string protocolLine;
                    try
                    {
                        protocolLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("Unexpected EOF while reading stream data.", StringComparison.Ordinal) && observed.Count > 0)
                    {
                        break;
                    }

                    if (string.Equals(protocolLine, "QUIT", StringComparison.Ordinal))
                    {
                        break;
                    }

                    if (!protocolLine.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException($"Unexpected protocol command in {nameof(PublishAsync_WhenOutOfOrderPartialResponsesThenDisconnect_CompletesOnlyUnansweredSubmissionAsAmbiguous)}: '{protocolLine}'");
                    }

                    string messageId = protocolLine["TAKETHIS ".Length..];
                    observed.Add(messageId);
                    Assert.True(payloads.TryGetValue(messageId, out byte[]? expectedPayload));

                    byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.Equal(expectedPayload, receivedPayload);
                }

                if (observed.Count < 2)
                {
                    throw new InvalidOperationException($"Expected two TAKETHIS commands before teardown, observed {observed.Count}.");
                }

                Assert.Equal(2, observed.Count);
                firstResponseMessageId = observed[1];
                secondResponseMessageId = observed[0];
                remainingMessageId = messageIds.Single(messageId => !observed.Contains(messageId, StringComparer.Ordinal));

                await FakePublisherServer.WriteLineAsync(stream, $"239 {firstResponseMessageId} transferred");
                await FakePublisherServer.WriteLineAsync(stream, $"239 {secondResponseMessageId} transferred");

                await WaitForAcceptedCountAsync(submissions, expectedAcceptedCount: 2, cancellationToken);
                stream.Dispose();
            });

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            submissions =
            [
                publisherInstance.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
            ];

            TransitPublishResult[] results = await Task.WhenAll(submissions);

            Assert.NotNull(firstResponseMessageId);
            Assert.NotNull(secondResponseMessageId);
            Assert.NotNull(remainingMessageId);

            Assert.Contains(results, result => result.MessageId == firstResponseMessageId && result.Status == TransitPublishStatus.Accepted && result.ResponseCode == 239);
            Assert.Contains(results, result => result.MessageId == secondResponseMessageId && result.Status == TransitPublishStatus.Accepted && result.ResponseCode == 239);
            Assert.Contains(results, result => result.MessageId == remainingMessageId && result.Status == TransitPublishStatus.Ambiguous);
        }

        /// <summary>
        /// Verifies that publisher disposal terminalizes multiple pending submissions rather than leaving their publish tasks unresolved.
        /// </summary>
        /// <remarks>
        /// Because the fake server withholds definitive responses, each pending operation may complete as <c>Canceled</c> or <c>Ambiguous</c>, but all tasks and queue bookkeeping must reach a terminal state.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when disposed with multiple outstanding completes pending submissions behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when disposed with multiple outstanding completes pending submissions helper.</returns>
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

            TransitPublisher? publisher = null;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 2, cancellationToken);

                HashSet<string> observed = new(StringComparer.Ordinal);
                for (int i = 0; i < 2; i++)
                {
                    string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    string messageId = takethisLine.Split(' ', 2, StringSplitOptions.TrimEntries)[1];
                    Assert.True(observed.Add(messageId));
                    Assert.True(payloads.TryGetValue(messageId, out byte[]? expectedPayload));

                    byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.Equal(expectedPayload, receivedPayload);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                publisherInstance.PublishAsync(messageIds[0], payloads[messageIds[0]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[1], payloads[messageIds[1]], CancellationToken.None).AsTask(),
                publisherInstance.PublishAsync(messageIds[2], payloads[messageIds[2]], CancellationToken.None).AsTask(),
            ];

            await WaitForOutstandingAwaitingResponsesAsync(publisherInstance, minimumAwaitingResponses: 2, CancellationToken.None);
            await publisherInstance.DisposeAsync();

            TransitPublishResult[] results = await Task.WhenAll(submissions);
            Assert.All(results, result => Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous));
        }

        /// <summary>
        /// Verifies that submissions whose outcome becomes ambiguous on a failed connection are not retransmitted on the replacement connection.
        /// </summary>
        /// <remarks>
        /// The replacement session must accept only genuinely new demand. The original batch must remain <c>Ambiguous</c>, while the later submission is accepted exactly once.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when connection replaced does not retry ambiguous submissions behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when connection replaced does not retry ambiguous submissions helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenConnectionReplaced_DoesNotRetryAmbiguousSubmissions()
        {
            string firstMessageId = "<publisher-replace-a@example.com>";
            string secondMessageId = "<publisher-replace-b@example.com>";
            string thirdMessageId = "<publisher-replace-c@example.com>";

            byte[] firstPayload = [(byte)'1', (byte)'\n'];
            byte[] secondPayload = [(byte)'2', (byte)'\n'];
            byte[] thirdPayload = [(byte)'3', (byte)'\n'];
            TransitPublisher? publisher = null;

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                async (stream, cancellationToken) =>
                {
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                    string commandOne = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(commandOne, "QUIT", StringComparison.Ordinal))
                    {
                        return;
                    }

                    if (!commandOne.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException($"Unexpected command on initial connection: '{commandOne}'.");
                    }

                    string messageIdOne = commandOne["TAKETHIS ".Length..];
                    Assert.Contains(messageIdOne, new[] { firstMessageId, secondMessageId });
                    byte[] payloadOne = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.True(payloadOne.SequenceEqual(firstPayload) || payloadOne.SequenceEqual(secondPayload));

                    TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                    await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 2, cancellationToken);

                    string commandTwo = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(commandTwo, "QUIT", StringComparison.Ordinal))
                    {
                        return;
                    }

                    if (!commandTwo.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException($"Unexpected command on initial connection: '{commandTwo}'.");
                    }

                    string messageIdTwo = commandTwo["TAKETHIS ".Length..];
                    Assert.Contains(messageIdTwo, new[] { firstMessageId, secondMessageId });
                    Assert.NotEqual(messageIdOne, messageIdTwo);
                    byte[] payloadTwo = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.True(payloadTwo.SequenceEqual(firstPayload) || payloadTwo.SequenceEqual(secondPayload));

                    stream.Dispose();
                },
                async (stream, cancellationToken) =>
                {
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                    string takethis = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal($"TAKETHIS {thirdMessageId}", takethis);
                    byte[] receivedPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.Equal(thirdPayload, receivedPayload);

                    await FakePublisherServer.WriteLineAsync(stream, $"239 {thirdMessageId} transferred");

                    using CancellationTokenSource noRetryCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    noRetryCts.CancelAfter(TimeSpan.FromMilliseconds(100));

                    string? nextLine = null;
                    Exception? ex = await Record.ExceptionAsync(async () => nextLine = await FakePublisherServer.ReadLineAsync(stream, noRetryCts.Token));
                    if (ex is null)
                    {
                        Assert.Equal("QUIT", nextLine);
                    }
                    else
                    {
                        Assert.True(ex is OperationCanceledException or InvalidOperationException);
                    }
                },
            ]);

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 8);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> first = publisherInstance.PublishAsync(firstMessageId, firstPayload, CancellationToken.None).AsTask();
            Task<TransitPublishResult> second = publisherInstance.PublishAsync(secondMessageId, secondPayload, CancellationToken.None).AsTask();
            TransitPublishResult[] firstBatch = await Task.WhenAll(first, second);

            Assert.All(firstBatch, result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));

            TransitPublishResult third = await publisherInstance.PublishAsync(thirdMessageId, thirdPayload, CancellationToken.None);
            Assert.Equal(TransitPublishStatus.Accepted, third.Status);
            Assert.Equal(239, third.ResponseCode);
            Assert.Equal(thirdMessageId, third.MessageId);
        }

        /// <summary>
        /// Verifies that a four-connection pool can utilize all available connections concurrently when each connection has a pipeline depth of one.
        /// </summary>
        /// <remarks>
        /// The test uses independent fake sessions and requires each submitted article to be observed and accepted, validating pool-wide concurrency rather than only two-connection behavior.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when connection count four and pipeline depth one utilizes all connections concurrently behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when connection count four and pipeline depth one utilizes all connections concurrently helper.</returns>
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
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
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

        /// <summary>
        /// Verifies watchdog recovery when a claimed batch makes no response progress and the connection is faulted.
        /// </summary>
        /// <remarks>
        /// The unresolved first batch must terminate as <c>Ambiguous</c> under the current uncertainty policy, while a later submission must be able to use replacement demand and receive a definitive <c>239</c> without connection-affinity assumptions.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when connection watchdog times out requeues outstanding and subsequent connection completes without affinity behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when connection watchdog times out requeues outstanding and subsequent connection completes without affinity helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenConnectionWatchdogTimesOut_RequeuesOutstandingAndSubsequentConnectionCompletesWithoutAffinity()
        {
            string firstMessageId = "<publisher-watchdog-first@example.com>";
            string secondMessageId = "<publisher-watchdog-second@example.com>";
            string thirdMessageId = "<publisher-watchdog-third@example.com>";
            int sessionAcceptCount = 0;

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                async (stream, cancellationToken) =>
                {
                    Interlocked.Increment(ref sessionAcceptCount);
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                    HashSet<string> unresolved = new(StringComparer.Ordinal);
                    while (unresolved.Count < 2)
                    {
                        string commandLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                        if (string.Equals(commandLine, "QUIT", StringComparison.Ordinal))
                        {
                            await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                            return;
                        }

                        Assert.StartsWith("TAKETHIS <", commandLine, StringComparison.Ordinal);
                        string messageId = commandLine["TAKETHIS ".Length..];
                        _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                        Assert.True(
                            string.Equals(messageId, firstMessageId, StringComparison.Ordinal) || string.Equals(messageId, secondMessageId, StringComparison.Ordinal),
                            $"Unexpected message id on watchdog-stalled session: {messageId}");
                        unresolved.Add(messageId);
                    }

                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                },
                async (stream, cancellationToken) =>
                {
                    Interlocked.Increment(ref sessionAcceptCount);
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                    while (true)
                    {
                        string commandLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                        if (string.Equals(commandLine, "QUIT", StringComparison.Ordinal))
                        {
                            await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                            return;
                        }

                        Assert.StartsWith("TAKETHIS <", commandLine, StringComparison.Ordinal);
                        string messageId = commandLine["TAKETHIS ".Length..];
                        _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                        Assert.Equal(thirdMessageId, messageId);
                        await FakePublisherServer.WriteLineAsync(stream, $"239 {thirdMessageId} transferred");
                        return;
                    }
                },
            ]);

            TransitPublisher publisher = CreatePublisher(
                server.Port,
                connectionPoolSize: 1,
                perConnectionPipelineDepth: 2,
                connectionResponseProgressTimeout: TimeSpan.FromMilliseconds(200),
                connectionResponseProgressCheckInterval: TimeSpan.FromMilliseconds(20));

            try
            {
                await publisher.InitializeAsync(CancellationToken.None);

                Task<TransitPublishResult> first = publisher.PublishAsync(firstMessageId, new byte[] { (byte)'1', (byte)'\n' }, CancellationToken.None).AsTask();
                Task<TransitPublishResult> second = publisher.PublishAsync(secondMessageId, new byte[] { (byte)'2', (byte)'\n' }, CancellationToken.None).AsTask();

                using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
                TransitPublishResult[] firstBatchResults = await Task.WhenAll(first, second).WaitAsync(completionTimeout.Token);

                Assert.All(firstBatchResults, result => Assert.Equal(TransitPublishStatus.Ambiguous, result.Status));

                TransitPublishResult thirdResult = await publisher.PublishAsync(thirdMessageId, new byte[] { (byte)'3', (byte)'\n' }, CancellationToken.None)
                    .AsTask()
                    .WaitAsync(completionTimeout.Token);

                Assert.Equal(TransitPublishStatus.Accepted, thirdResult.Status);
                Assert.Equal(239, thirdResult.ResponseCode);
                Assert.Equal(thirdMessageId, thirdResult.MessageId);

                TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: 0);
                Assert.Equal(3, snapshot.TotalArticlesSubmitted);
                Assert.Equal(1, snapshot.TotalArticlesAccepted);
                Assert.Equal(2, snapshot.TotalArticlesAmbiguous);
                Assert.Equal(0, snapshot.OutstandingSubmissions);
                Assert.Equal(2, Volatile.Read(ref sessionAcceptCount));
            }
            finally
            {
                using CancellationTokenSource teardownTimeout = new(TimeSpan.FromSeconds(10));
                try
                {
                    await publisher.DisposeAsync().AsTask().WaitAsync(teardownTimeout.Token);
                }
                catch (OperationCanceledException ex) when (teardownTimeout.IsCancellationRequested)
                {
                    throw new Xunit.Sdk.XunitException($"Publisher disposal exceeded test teardown timeout (10s) in {nameof(PublishAsync_WhenConnectionWatchdogTimesOut_RequeuesOutstandingAndSubsequentConnectionCompletesWithoutAffinity)}. {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Verifies that disposing an initialized publisher with no outstanding submission work does not create a reconnect loop.
        /// </summary>
        /// <remarks>
        /// Note that the current implementation is demand-driven; with no publish demand this test intentionally exercises the idle lifecycle rather than a live connection. Its strongest value is guarding the no-work shutdown path.
        /// </remarks>
        /// <summary>
        /// Confirms the dispose async when no outstanding work watchdog does not trigger reconnect loop behavior.
        /// </summary>
        /// <returns>The value returned by the dispose async when no outstanding work watchdog does not trigger reconnect loop helper.</returns>
        [Fact]
        public async Task DisposeAsync_WhenNoOutstandingWork_WatchdogDoesNotTriggerReconnectLoop()
        {
            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisher = CreatePublisher(
                server.Port,
                connectionPoolSize: 1,
                perConnectionPipelineDepth: 2,
                connectionResponseProgressTimeout: TimeSpan.FromMilliseconds(150),
                connectionResponseProgressCheckInterval: TimeSpan.FromMilliseconds(20));
            await publisher.InitializeAsync(CancellationToken.None);

            await Task.Delay(300);

            using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);

            TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 0, outstandingSubmissions: 0);
            Assert.Equal(0, snapshot.TotalArticlesSubmitted);
            Assert.Equal(0, snapshot.TotalReconnects);
            Assert.Equal(0, snapshot.OutstandingSubmissions);
        }

        /// <summary>
        /// Verifies recovery after the first connection drops: the unresolved first publish becomes <c>Ambiguous</c>, while a subsequent publish is admitted on recovery and completes with <c>239</c>.
        /// </summary>
        /// <remarks>
        /// The test intentionally avoids asserting an exact reconnect-counter value and focuses on externally observable outcome correctness.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async after connection drop reconnects and publishes subsequent submission behavior.
        /// </summary>
        /// <returns>The value returned by the publish async after connection drop reconnects and publishes subsequent submission helper.</returns>
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
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
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
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
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
            Assert.Equal(2, snapshot.TotalArticlesSubmitted);
            Assert.Equal(1, snapshot.TotalArticlesAccepted);
            Assert.Equal(1, snapshot.TotalArticlesAmbiguous);
        }

        /// <summary>
        /// Verifies that disposal racing with connection setup cannot allow an initialization task to resurrect a live connection after shutdown has begun.
        /// </summary>
        /// <remarks>
        /// The test creates explicit publish demand to enter connection setup, blocks the server before completing the greeting, starts disposal, and then verifies disconnected state plus zero residual queue, retry, active, and outstanding work.
        /// </remarks>
        /// <summary>
        /// Confirms the initialize async when dispose begins during connection setup does not resurrect connection behavior.
        /// </summary>
        /// <returns>The value returned by the initialize async when dispose begins during connection setup does not resurrect connection helper.</returns>
        [Fact]
        public async Task InitializeAsync_WhenDisposeBeginsDuringConnectionSetup_DoesNotResurrectConnection()
        {
            using CancellationTokenSource testTimeout = new(TimeSpan.FromSeconds(10));

            string messageId = "<init-dispose-setup-race@example.com>";
            byte[] payload = [(byte)'I', (byte)'\n'];

            TaskCompletionSource connectionSetupInProgress = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowServerGreeting = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                connectionSetupInProgress.TrySetResult();
                await allowServerGreeting.Task.WaitAsync(cancellationToken);

                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");

                try
                {
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("Unexpected EOF while reading stream data.", StringComparison.Ordinal))
                {
                    return;
                }

                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Assert.Equal(0, GetPrimaryConnectionCount(publisher));

            Task<TransitPublishResult> submission = publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask();

            await connectionSetupInProgress.Task.WaitAsync(testTimeout.Token);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot setupSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            string? setupConnectionId = setupSnapshot.Slots.Length > 0 ? setupSnapshot.Slots[0].CurrentConnectionId : null;

            Task disposeTask = publisher.DisposeAsync().AsTask();
            allowServerGreeting.TrySetResult();

            await disposeTask.WaitAsync(testTimeout.Token);

            TransitPublishResult result = await submission.WaitAsync(testTimeout.Token);
            Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot finalSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
            Assert.Equal(0, GetPrimaryConnectionCount(publisher));
            Assert.Equal(0, finalSnapshot.QueueSnapshot.QueuedItemCount);
            Assert.Equal(0, finalSnapshot.QueueSnapshot.InFlightCount);
            Assert.Equal(0, finalSnapshot.QueueSnapshot.RetryPendingCount);
            Assert.Equal(0, GetActiveSubmissionCount(publisher));
            Assert.DoesNotContain(
                finalSnapshot.Connections.SelectMany(static entry => entry.Snapshot.OutstandingOperations),
                operation => string.Equals(operation.MessageId, messageId, StringComparison.Ordinal));

            if (!string.IsNullOrWhiteSpace(setupConnectionId))
            {
                Assert.DoesNotContain(
                    finalSnapshot.Connections,
                    entry => string.Equals(entry.ConnectionId, setupConnectionId, StringComparison.Ordinal));
            }
        }

        /// <summary>
        /// Verifies that a worker outliving the bounded disposal wait does not race with disposal of the queue semaphore.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenWorkerOutlivesBoundedWait_DoesNotSurfaceQueueSemaphoreDisposalRace()
        {
            using CancellationTokenSource testTimeout = new(TimeSpan.FromSeconds(10));

            TaskCompletionSource allowCapabilities = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseSecondResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);

            string firstMessageId = "<bounded-dispose-prime@example.com>";
            string secondMessageId = "<bounded-dispose-race@example.com>";

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");

                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await allowCapabilities.Task.WaitAsync(cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string firstTakethis = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {firstMessageId}", firstTakethis);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, $"239 {firstMessageId} transferred");

                string secondCommand = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                if (string.Equals(secondCommand, $"TAKETHIS {secondMessageId}", StringComparison.Ordinal))
                {
                    _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    await releaseSecondResponse.Task.WaitAsync(cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, $"239 {secondMessageId} transferred");
                    return;
                }

                Assert.Equal("QUIT", secondCommand);
                await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
            });

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
                TransitServerPort: server.Port,
                TransitServerUseSsl: false,
                ShutdownGracePeriodSeconds: 60,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 120,
                WriteBatchCoalesceMicroseconds: 250,
                TransitShutdownDrainGracePeriod: TimeSpan.Zero,
                TransitShutdownDrainInactivityWatchdog: TimeSpan.Zero,
                TransitShutdownAbsoluteMaximum: TimeSpan.Zero);

            TaskCompletionSource<bool> claimBoundarySignal = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Action claimBoundaryObserved = () => _ = claimBoundarySignal.TrySetResult(true);

            await using TransitPublisher publisher = new(
                options,
                TimeProvider.System,
                NullLogger<TransitPublisher>.Instance,
                connectionPoolSize: 1,
                perConnectionPipelineDepth: 1,
                claimBoundaryObserved: claimBoundaryObserved);

            await publisher.InitializeAsync(CancellationToken.None);
            allowCapabilities.TrySetResult();

            TransitPublishResult prime = await publisher.PublishAsync(
                firstMessageId,
                new byte[] { (byte)'p', (byte)'\n' },
                CancellationToken.None);
            Assert.Equal(TransitPublishStatus.Accepted, prime.Status);

            GlobalTransitWorkQueue queue = GetGlobalQueue(publisher);
            object claimGate = GetClaimGate(queue);

            TaskCompletionSource claimGateHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
            using ManualResetEventSlim releaseClaimGate = new(false);

            Task claimGateHolder = Task.Run(() =>
            {
                Monitor.Enter(claimGate);
                try
                {
                    claimGateHeld.TrySetResult();
                    releaseClaimGate.Wait(testTimeout.Token);
                }
                finally
                {
                    Monitor.Exit(claimGate);
                }
            }, CancellationToken.None);

            await claimGateHeld.Task.WaitAsync(testTimeout.Token);

            try
            {
                Task<TransitPublishResult> secondPublish = publisher.PublishAsync(
                    secondMessageId,
                    new byte[] { (byte)'x', (byte)'\n' },
                    CancellationToken.None).AsTask();

                await claimBoundarySignal.Task.WaitAsync(testTimeout.Token);

                Task disposeTask = publisher.DisposeAsync().AsTask();

                releaseSecondResponse.TrySetResult();
                TransitPublishResult secondResult = await secondPublish.WaitAsync(testTimeout.Token);
                Assert.True(secondResult.Status is TransitPublishStatus.Accepted or TransitPublishStatus.Ambiguous or TransitPublishStatus.Canceled);

                releaseClaimGate.Set();
                await claimGateHolder.WaitAsync(testTimeout.Token);

                await disposeTask.WaitAsync(testTimeout.Token);
                await WaitForRemainingConnectionWorkerCountAsync(publisher, expectedCount: 0, testTimeout.Token);
            }
            finally
            {
                releaseClaimGate.Set();
                await claimGateHolder.WaitAsync(testTimeout.Token);
            }
        }

        /// <summary>
        /// Verifies same-slot reconnect deduplication when multiple reconnect triggers race.
        /// </summary>
        /// <remarks>
        /// The test captures the original and replacement connection identities from diagnostics, correlates them to fake-server endpoints, proves the replacement becomes ready, and then verifies that the same replacement identity remains installed rather than being replaced a second time.
        /// </remarks>
        /// <summary>
        /// Confirms the reconnect async when concurrent requests target same slot does not replace fresh healthy connection twice behavior.
        /// </summary>
        /// <returns>The value returned by the reconnect async when concurrent requests target same slot does not replace fresh healthy connection twice helper.</returns>
        [Fact]
        public async Task ReconnectAsync_WhenConcurrentRequestsTargetSameSlot_DoesNotReplaceFreshHealthyConnectionTwice()
        {
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

            string setupMessageId = "<reconnect-concurrent-setup@example.com>";
            string firstMessageId = "<reconnect-concurrent-a@example.com>";
            string secondMessageId = "<reconnect-concurrent-b@example.com>";
            string postReconnectMessageId = "<reconnect-concurrent-post@example.com>";
            byte[] payload = [(byte)'R', (byte)'\n'];

            TaskCompletionSource setupTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string> setupTakethisEndpoint = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowSetupResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseFirstSessionDisconnect = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource firstSessionDisconnected = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseSecondSessionHandshake = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string> secondSessionEndpoint = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string> postReconnectTakethisEndpoint = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource thirdSessionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            FakePublisherServer? fakeServer = null;

            static bool EndpointsMatch(string firstEndpoint, string secondEndpoint)
            {
                if (string.Equals(firstEndpoint, secondEndpoint, StringComparison.Ordinal))
                {
                    return true;
                }

                if (!IPEndPoint.TryParse(firstEndpoint, out IPEndPoint? first)
                    || !IPEndPoint.TryParse(secondEndpoint, out IPEndPoint? second))
                {
                    return false;
                }

                IPAddress firstAddress = first.Address.IsIPv4MappedToIPv6 ? first.Address.MapToIPv4() : first.Address;
                IPAddress secondAddress = second.Address.IsIPv4MappedToIPv6 ? second.Address.MapToIPv4() : second.Address;
                return first.Port == second.Port && firstAddress.Equals(secondAddress);
            }

            static TransitPublisher.ConnectionDiagnosticsEntry? ResolvePrimaryEntry(TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot)
            {
                if (snapshot.Slots.Length == 0)
                {
                    return null;
                }

                TransitPublisher.ConnectionSlotSnapshot slot = snapshot.Slots[0];
                if (!slot.HasCurrentConnection || string.IsNullOrWhiteSpace(slot.CurrentConnectionId))
                {
                    return null;
                }

                return snapshot.Connections.FirstOrDefault(
                    entry => entry.SlotIndex == 0
                        && string.Equals(entry.ConnectionId, slot.CurrentConnectionId, StringComparison.Ordinal));
            }

            static async Task<(TransitPublisher.ConnectionDiagnosticsEntry Owner, TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot Snapshot)> WaitForOutstandingOwnerAsync(
                TransitPublisher publisher,
                string messageId,
                CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                    TransitPublisher.ConnectionDiagnosticsEntry? owner = snapshot.Connections.FirstOrDefault(
                        entry => entry.Snapshot.OutstandingOperations.Any(op => string.Equals(op.MessageId, messageId, StringComparison.Ordinal) && op.WaitingFor239Response));

                    if (owner is not null)
                    {
                        return (owner, snapshot);
                    }

                    await Task.Yield();
                }
            }

            static async Task<(string ConnectionId, string LocalEndpoint)> WaitForPrimaryReadyConnectionAsync(
                TransitPublisher publisher,
                string excludedConnectionId,
                CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                    if (snapshot.Slots.Length > 0)
                    {
                        TransitPublisher.ConnectionSlotSnapshot primarySlot = snapshot.Slots[0];
                        string? candidateConnectionId = primarySlot.CurrentConnectionId;
                        if (primarySlot.HasCurrentConnection
                            && !string.IsNullOrWhiteSpace(candidateConnectionId)
                            && !string.Equals(candidateConnectionId, excludedConnectionId, StringComparison.Ordinal))
                        {
                            string currentConnectionId = candidateConnectionId;
                            TransitPublisher.ConnectionDiagnosticsEntry? entry = ResolvePrimaryEntry(snapshot);
                            string? candidateLocalEndpoint = entry?.Snapshot.LocalEndpoint;
                            if (entry is not null
                                && entry.Snapshot.CurrentState == TransitConnectionState.Ready
                                && !string.IsNullOrWhiteSpace(candidateLocalEndpoint))
                            {
                                string localEndpoint = candidateLocalEndpoint;
                                return (currentConnectionId, localEndpoint);
                            }
                        }
                    }

                    await Task.Yield();
                }
            }

            static async Task<TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot> WaitForPrimaryReconnectPendingAsync(
                TransitPublisher publisher,
                string originalConnectionId,
                CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                    if (snapshot.TotalReconnects >= 1
                        && snapshot.Slots.Length > 0
                        && !snapshot.Slots[0].HasCurrentConnection
                        && snapshot.Connections.All(entry => !string.Equals(entry.ConnectionId, originalConnectionId, StringComparison.Ordinal)))
                    {
                        return snapshot;
                    }

                    await Task.Yield();
                }
            }

            static async Task WaitForPrimaryTerminalFaultAsync(TransitPublisher publisher, CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitConnectionState state = GetPrimaryConnectionState(publisher);
                    if (state is TransitConnectionState.Faulted or TransitConnectionState.Disconnected)
                    {
                        return;
                    }

                    await Task.Yield();
                }
            }

            static async Task WaitForPrimaryReadyWithConnectionIdAsync(
                TransitPublisher publisher,
                string expectedConnectionId,
                CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                    TransitPublisher.ConnectionDiagnosticsEntry? entry = ResolvePrimaryEntry(snapshot);
                    if (snapshot.Slots.Length > 0
                        && snapshot.Slots[0].HasCurrentConnection
                        && string.Equals(snapshot.Slots[0].CurrentConnectionId, expectedConnectionId, StringComparison.Ordinal)
                        && entry is not null
                        && entry.Snapshot.CurrentState == TransitConnectionState.Ready)
                    {
                        return;
                    }

                    if (snapshot.Slots.Length > 0
                        && snapshot.Slots[0].HasCurrentConnection
                        && !string.IsNullOrWhiteSpace(snapshot.Slots[0].CurrentConnectionId)
                        && !string.Equals(snapshot.Slots[0].CurrentConnectionId, expectedConnectionId, StringComparison.Ordinal))
                    {
                        throw new Xunit.Sdk.XunitException(
                            $"Primary slot changed from expected replacement connection '{expectedConnectionId}' to '{snapshot.Slots[0].CurrentConnectionId}'.");
                    }

                    await Task.Yield();
                }
            }

            async Task RunFirstSessionAsync(NetworkStream stream, CancellationToken cancellationToken)
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                FakePublisherServer serverInstance = Assert.IsType<FakePublisherServer>(fakeServer);
                string acceptedRemoteEndpoint = serverInstance.GetRemoteEndpoint(stream);

                while (true)
                {
                    string protocolLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(protocolLine, "QUIT", StringComparison.Ordinal))
                    {
                        await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                        return;
                    }

                    if (!protocolLine.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException($"Unexpected protocol command in {nameof(ReconnectAsync_WhenConcurrentRequestsTargetSameSlot_DoesNotReplaceFreshHealthyConnectionTwice)}: '{protocolLine}'");
                    }

                    string messageId = protocolLine["TAKETHIS ".Length..];
                    if (!string.Equals(messageId, setupMessageId, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException($"Unexpected message id on original session in {nameof(ReconnectAsync_WhenConcurrentRequestsTargetSameSlot_DoesNotReplaceFreshHealthyConnectionTwice)}: '{messageId}'");
                    }

                    byte[] readPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.Equal(payload, readPayload);

                    setupTakethisEndpoint.TrySetResult(acceptedRemoteEndpoint);
                    setupTakethisObserved.TrySetResult();

                    await allowSetupResponse.Task.WaitAsync(cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");

                    await releaseFirstSessionDisconnect.Task.WaitAsync(cancellationToken);
                    stream.Dispose();
                    firstSessionDisconnected.TrySetResult();
                    return;
                }
            }

            async Task RunSecondSessionAsync(NetworkStream stream, CancellationToken cancellationToken)
            {
                await releaseSecondSessionHandshake.Task.WaitAsync(cancellationToken);

                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                FakePublisherServer serverInstance = Assert.IsType<FakePublisherServer>(fakeServer);
                string acceptedRemoteEndpoint = serverInstance.GetRemoteEndpoint(stream);
                secondSessionEndpoint.TrySetResult(acceptedRemoteEndpoint);

                while (true)
                {
                    string protocolLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(protocolLine, "QUIT", StringComparison.Ordinal))
                    {
                        await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                        return;
                    }

                    if (!protocolLine.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException($"Unexpected protocol command in {nameof(ReconnectAsync_WhenConcurrentRequestsTargetSameSlot_DoesNotReplaceFreshHealthyConnectionTwice)}: '{protocolLine}'");
                    }

                    string messageId = protocolLine["TAKETHIS ".Length..];
                    Assert.True(
                        string.Equals(messageId, firstMessageId, StringComparison.Ordinal)
                        || string.Equals(messageId, secondMessageId, StringComparison.Ordinal)
                        || string.Equals(messageId, postReconnectMessageId, StringComparison.Ordinal));

                    byte[] readPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.Equal(payload, readPayload);

                    if (string.Equals(messageId, postReconnectMessageId, StringComparison.Ordinal))
                    {
                        postReconnectTakethisEndpoint.TrySetResult(acceptedRemoteEndpoint);
                    }

                    await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                }
            }

            async Task RunThirdSessionAsync(NetworkStream stream, CancellationToken cancellationToken)
            {
                thirdSessionAccepted.TrySetResult();
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            fakeServer = await FakePublisherServer.StartSessionsAsync([
                RunFirstSessionAsync,
                RunSecondSessionAsync,
                RunThirdSessionAsync,
            ]);

            await using FakePublisherServer server = fakeServer;
            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 2);
            await publisher.InitializeAsync(CancellationToken.None);

            Assert.Equal(0, GetPrimaryConnectionCount(publisher));

            Task<TransitPublishResult> setupSubmission = publisher.PublishAsync(setupMessageId, payload, CancellationToken.None).AsTask();

            await setupTakethisObserved.Task.WaitAsync(timeout.Token);
            (TransitPublisher.ConnectionDiagnosticsEntry setupOwner, TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot _) = await WaitForOutstandingOwnerAsync(
                publisher,
                setupMessageId,
                timeout.Token);

            int ownerSlotIndex = setupOwner.SlotIndex;
            string originalConnectionId = setupOwner.ConnectionId;
            Assert.False(string.IsNullOrWhiteSpace(setupOwner.Snapshot.LocalEndpoint));
            string originalLocalEndpoint = setupOwner.Snapshot.LocalEndpoint!;

            string firstAcceptedEndpoint = await setupTakethisEndpoint.Task.WaitAsync(timeout.Token);
            Assert.True(EndpointsMatch(originalLocalEndpoint, firstAcceptedEndpoint));

            allowSetupResponse.TrySetResult();
            TransitPublishResult setupResult = await setupSubmission.WaitAsync(timeout.Token);

            Assert.Equal(0, ownerSlotIndex);
            await WaitForPrimaryReadyWithConnectionIdAsync(publisher, originalConnectionId, timeout.Token);

            TransitTransportSnapshot transportBeforeReplacement = publisher.CaptureTransportSnapshot(
                activeConnections: 1,
                outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));

            releaseFirstSessionDisconnect.TrySetResult();
            await firstSessionDisconnected.Task.WaitAsync(timeout.Token);
            await WaitForPrimaryTerminalFaultAsync(publisher, timeout.Token);

            Task<TransitPublishResult> submissionA = publisher.PublishAsync(firstMessageId, payload, CancellationToken.None).AsTask();
            Task<TransitPublishResult> submissionB = publisher.PublishAsync(secondMessageId, payload, CancellationToken.None).AsTask();

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot reconnectPendingSnapshot = await WaitForPrimaryReconnectPendingAsync(
                publisher,
                originalConnectionId,
                timeout.Token);
            Assert.False(reconnectPendingSnapshot.Slots[0].HasCurrentConnection);
            Assert.Null(reconnectPendingSnapshot.Slots[0].CurrentConnectionId);
            Assert.DoesNotContain(
                reconnectPendingSnapshot.Connections,
                entry => string.Equals(entry.ConnectionId, originalConnectionId, StringComparison.Ordinal));
            Assert.Equal(1, reconnectPendingSnapshot.TotalReconnects);

            TransitTransportSnapshot reconnectPendingTransport = publisher.CaptureTransportSnapshot(
                activeConnections: 1,
                outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            Assert.True(reconnectPendingTransport.TotalBytesTransmitted >= transportBeforeReplacement.TotalBytesTransmitted);
            Assert.True(reconnectPendingTransport.TotalBytesReceived >= transportBeforeReplacement.TotalBytesReceived);

            releaseSecondSessionHandshake.TrySetResult();

            (string replacementConnectionId, string replacementLocalEndpoint) = await WaitForPrimaryReadyConnectionAsync(
                publisher,
                originalConnectionId,
                timeout.Token);

            string secondAcceptedEndpoint = await secondSessionEndpoint.Task.WaitAsync(timeout.Token);
            Assert.True(EndpointsMatch(replacementLocalEndpoint, secondAcceptedEndpoint));

            TransitPublishResult[] reconnectWindowResults = await Task.WhenAll(submissionA, submissionB).WaitAsync(timeout.Token);
            Assert.All(
                reconnectWindowResults,
                result => Assert.True(
                    result.Status is TransitPublishStatus.Accepted or TransitPublishStatus.Ambiguous,
                    $"Unexpected status for reconnect-window submission {result.MessageId}: {result.Status}"));

            await WaitForPrimaryReadyWithConnectionIdAsync(publisher, replacementConnectionId, timeout.Token);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot postRaceSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            TransitPublisher.ConnectionDiagnosticsEntry? postRaceEntry = ResolvePrimaryEntry(postRaceSnapshot);
            Assert.NotNull(postRaceEntry);
            Assert.Equal(replacementConnectionId, postRaceEntry.ConnectionId);
            Assert.False(string.IsNullOrWhiteSpace(postRaceEntry.Snapshot.LocalEndpoint));
            Assert.True(EndpointsMatch(replacementLocalEndpoint, postRaceEntry.Snapshot.LocalEndpoint!));
            Assert.DoesNotContain(postRaceSnapshot.Connections, entry => string.Equals(entry.ConnectionId, originalConnectionId, StringComparison.Ordinal));
            Assert.Equal(1, postRaceSnapshot.TotalReconnects);
            Assert.False(thirdSessionAccepted.Task.IsCompleted);

            TransitTransportSnapshot postRaceTransportSnapshot = publisher.CaptureTransportSnapshot(
                activeConnections: 1,
                outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            Assert.True(transportBeforeReplacement.TotalBytesTransmitted > 0);
            Assert.True(transportBeforeReplacement.TotalBytesReceived > 0);
            Assert.True(postRaceTransportSnapshot.TotalBytesTransmitted >= transportBeforeReplacement.TotalBytesTransmitted);
            Assert.True(postRaceTransportSnapshot.TotalBytesReceived >= transportBeforeReplacement.TotalBytesReceived);

            TransitPublishResult postReconnectResult = await publisher.PublishAsync(postReconnectMessageId, payload, CancellationToken.None)
                .AsTask()
                .WaitAsync(timeout.Token);

            Assert.Equal(TransitPublishStatus.Accepted, postReconnectResult.Status);
            Assert.Equal(239, postReconnectResult.ResponseCode);
            Assert.Equal(postReconnectMessageId, postReconnectResult.MessageId);

            string postReconnectEndpoint = await postReconnectTakethisEndpoint.Task.WaitAsync(timeout.Token);
            Assert.True(EndpointsMatch(replacementLocalEndpoint, postReconnectEndpoint));

            TransitTransportSnapshot finalTransportSnapshot = publisher.CaptureTransportSnapshot(
                activeConnections: 1,
                outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            Assert.True(finalTransportSnapshot.TotalBytesTransmitted >= postRaceTransportSnapshot.TotalBytesTransmitted);
            Assert.True(finalTransportSnapshot.TotalBytesReceived >= postRaceTransportSnapshot.TotalBytesReceived);

            await WaitForPrimaryReadyWithConnectionIdAsync(publisher, replacementConnectionId, timeout.Token);
            Assert.False(thirdSessionAccepted.Task.IsCompleted);

            _ = setupResult;
        }

        /// <summary>
        /// Verifies that a healthy secondary connection can continue serving new publish demand when the primary slot is faulted.
        /// </summary>
        /// <remarks>
        /// The test first establishes and identifies both slot connections, deliberately faults slot zero, then proves the target submission is transmitted and accepted on slot one without requiring recovery of the faulted primary.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when primary slot faulted but secondary slot healthy still admits and publishes behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when primary slot faulted but secondary slot healthy still admits and publishes helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenPrimarySlotFaultedButSecondarySlotHealthy_StillAdmitsAndPublishes()
        {
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

            string setupMessageIdA = "<publisher-slot-setup-a@example.com>";
            string setupMessageIdB = "<publisher-slot-setup-b@example.com>";
            string messageId = "<publisher-slot1-available@example.com>";
            byte[] payload = [(byte)'S', (byte)'2', (byte)'\n'];

            TaskCompletionSource<string> slot0SessionMapped = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string> slot1SessionMapped = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource slot0SessionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource targetTakethisReceivedOnSlot1 = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource targetResponseSentOnSlot1 = new(TaskCreationOptions.RunContinuationsAsynchronously);

            FakePublisherServer? fakeServer = null;
            TransitPublisher? publisher = null;

            static TransitConnectionState ResolveSlotState(
                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot,
                int slotIndex)
            {
                if ((uint)slotIndex >= (uint)snapshot.Slots.Length)
                {
                    return TransitConnectionState.Disconnected;
                }

                TransitPublisher.ConnectionSlotSnapshot slot = snapshot.Slots[slotIndex];
                if (!slot.HasCurrentConnection || string.IsNullOrWhiteSpace(slot.CurrentConnectionId))
                {
                    return TransitConnectionState.Disconnected;
                }

                TransitPublisher.ConnectionDiagnosticsEntry? entry = snapshot.Connections.FirstOrDefault(
                    candidate => candidate.SlotIndex == slotIndex
                        && string.Equals(candidate.ConnectionId, slot.CurrentConnectionId, StringComparison.Ordinal));
                return entry?.Snapshot.CurrentState ?? TransitConnectionState.Disconnected;
            }

            static string? ResolveSlotLocalEndpoint(
                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot,
                int slotIndex)
            {
                if ((uint)slotIndex >= (uint)snapshot.Slots.Length)
                {
                    return null;
                }

                TransitPublisher.ConnectionSlotSnapshot slot = snapshot.Slots[slotIndex];
                if (!slot.HasCurrentConnection || string.IsNullOrWhiteSpace(slot.CurrentConnectionId))
                {
                    return null;
                }

                TransitPublisher.ConnectionDiagnosticsEntry? entry = snapshot.Connections.FirstOrDefault(
                    candidate => candidate.SlotIndex == slotIndex
                        && string.Equals(candidate.ConnectionId, slot.CurrentConnectionId, StringComparison.Ordinal));
                return entry?.Snapshot.LocalEndpoint;
            }

            static bool EndpointsMatch(string firstEndpoint, string secondEndpoint)
            {
                if (string.Equals(firstEndpoint, secondEndpoint, StringComparison.Ordinal))
                {
                    return true;
                }

                if (!IPEndPoint.TryParse(firstEndpoint, out IPEndPoint? first)
                    || !IPEndPoint.TryParse(secondEndpoint, out IPEndPoint? second))
                {
                    return false;
                }

                IPAddress firstAddress = first.Address.IsIPv4MappedToIPv6 ? first.Address.MapToIPv4() : first.Address;
                IPAddress secondAddress = second.Address.IsIPv4MappedToIPv6 ? second.Address.MapToIPv4() : second.Address;
                return first.Port == second.Port && firstAddress.Equals(secondAddress);
            }

            static TransitConnection GetConnectionBySlot(TransitPublisher publisherInstance, int slotIndex)
            {
                FieldInfo? connectionsField = typeof(TransitPublisher).GetField("_connections", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(connectionsField);

                object? connectionsRaw = connectionsField.GetValue(publisherInstance);
                TransitConnection[] connections = Assert.IsType<TransitConnection[]>(connectionsRaw);
                Assert.InRange(slotIndex, 0, connections.Length - 1);
                return Assert.IsType<TransitConnection>(connections[slotIndex]);
            }

            static async Task<TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot> WaitForBothSlotsReadyAsync(
                TransitPublisher publisherInstance,
                CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisherInstance.CaptureConnectionDiagnosticsSnapshot();
                    if (snapshot.Slots.Length >= 2
                        && snapshot.Slots[0].HasCurrentConnection
                        && !string.IsNullOrWhiteSpace(snapshot.Slots[0].CurrentConnectionId)
                        && snapshot.Slots[1].HasCurrentConnection
                        && !string.IsNullOrWhiteSpace(snapshot.Slots[1].CurrentConnectionId)
                        && ResolveSlotState(snapshot, slotIndex: 0) == TransitConnectionState.Ready
                        && ResolveSlotState(snapshot, slotIndex: 1) == TransitConnectionState.Ready
                        && !string.IsNullOrWhiteSpace(ResolveSlotLocalEndpoint(snapshot, slotIndex: 0))
                        && !string.IsNullOrWhiteSpace(ResolveSlotLocalEndpoint(snapshot, slotIndex: 1)))
                    {
                        return snapshot;
                    }

                    await Task.Yield();
                }
            }

            async Task RunRoleAgnosticSession(NetworkStream stream, CancellationToken cancellationToken)
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                FakePublisherServer serverInstance = Assert.IsType<FakePublisherServer>(fakeServer);
                TransitPublisher publisherInstance = Assert.IsType<TransitPublisher>(publisher);

                string acceptedRemoteEndpoint = serverInstance.GetRemoteEndpoint(stream);
                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot readySnapshot = await WaitForBothSlotsReadyAsync(publisherInstance, cancellationToken);
                string slot0Endpoint = Assert.IsType<string>(ResolveSlotLocalEndpoint(readySnapshot, slotIndex: 0));
                string slot1Endpoint = Assert.IsType<string>(ResolveSlotLocalEndpoint(readySnapshot, slotIndex: 1));

                if (EndpointsMatch(acceptedRemoteEndpoint, slot0Endpoint))
                {
                    slot0SessionMapped.TrySetResult(acceptedRemoteEndpoint);

                    string commandLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.StartsWith("TAKETHIS ", commandLine, StringComparison.Ordinal);
                    string setupMessageId = commandLine["TAKETHIS ".Length..];
                    Assert.True(
                        string.Equals(setupMessageIdA, setupMessageId, StringComparison.Ordinal)
                        || string.Equals(setupMessageIdB, setupMessageId, StringComparison.Ordinal),
                        $"Unexpected setup Message-ID on slot-0 session: {setupMessageId}");

                    byte[] setupPayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    Assert.Equal(payload, setupPayload);

                    slot0SessionBlocked.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                    return;
                }

                if (EndpointsMatch(acceptedRemoteEndpoint, slot1Endpoint))
                {
                    slot1SessionMapped.TrySetResult(acceptedRemoteEndpoint);

                    while (true)
                    {
                        string commandLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                        if (string.Equals(commandLine, "QUIT", StringComparison.Ordinal))
                        {
                            await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                            return;
                        }

                        Assert.StartsWith("TAKETHIS ", commandLine, StringComparison.Ordinal);
                        string publishedMessageId = commandLine["TAKETHIS ".Length..];

                        byte[] articlePayload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                        Assert.Equal(payload, articlePayload);

                        if (string.Equals(publishedMessageId, messageId, StringComparison.Ordinal))
                        {
                            targetTakethisReceivedOnSlot1.TrySetResult();
                        }

                        await FakePublisherServer.WriteLineAsync(stream, $"239 {publishedMessageId} transferred");

                        if (string.Equals(publishedMessageId, messageId, StringComparison.Ordinal))
                        {
                            targetResponseSentOnSlot1.TrySetResult();
                            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                        }
                    }
                }

                Assert.Fail($"Session endpoint '{acceptedRemoteEndpoint}' did not match slot endpoints '{slot0Endpoint}' or '{slot1Endpoint}'.");
            }

            async Task RunFallbackSession(NetworkStream stream, CancellationToken cancellationToken)
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                while (true)
                {
                    string commandLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(commandLine, "QUIT", StringComparison.Ordinal))
                    {
                        await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                        return;
                    }

                    Assert.StartsWith("TAKETHIS ", commandLine, StringComparison.Ordinal);
                    _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                    string publishedMessageId = commandLine["TAKETHIS ".Length..];
                    await FakePublisherServer.WriteLineAsync(stream, $"239 {publishedMessageId} transferred");
                }
            }

            await using FakePublisherServer ownedServer = await FakePublisherServer.StartSessionsAsync(
            [
                RunRoleAgnosticSession,
                RunRoleAgnosticSession,
                RunFallbackSession,
            ]);
            fakeServer = ownedServer;

            await using TransitPublisher ownedPublisher = CreatePublisher(ownedServer.Port, connectionPoolSize: 2, perConnectionPipelineDepth: 1);
            publisher = ownedPublisher;
            await ownedPublisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> setupPublishA = ownedPublisher.PublishAsync(setupMessageIdA, payload, CancellationToken.None).AsTask();
            Task<TransitPublishResult> setupPublishB = ownedPublisher.PublishAsync(setupMessageIdB, payload, CancellationToken.None).AsTask();

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot beforeFaultSnapshot = await WaitForBothSlotsReadyAsync(ownedPublisher, timeout.Token);
            await slot0SessionMapped.Task.WaitAsync(timeout.Token);
            await slot1SessionMapped.Task.WaitAsync(timeout.Token);
            await slot0SessionBlocked.Task.WaitAsync(timeout.Token);

            TransitConnection primaryBeforeFault = GetConnectionBySlot(ownedPublisher, slotIndex: 0);
            TransitConnection secondaryBeforeFault = GetConnectionBySlot(ownedPublisher, slotIndex: 1);
            Assert.NotNull(primaryBeforeFault);
            Assert.NotNull(secondaryBeforeFault);

            Assert.Equal(TransitConnectionState.Ready, ResolveSlotState(beforeFaultSnapshot, slotIndex: 0));
            Assert.Equal(TransitConnectionState.Ready, ResolveSlotState(beforeFaultSnapshot, slotIndex: 1));

            string slot0Endpoint = Assert.IsType<string>(ResolveSlotLocalEndpoint(beforeFaultSnapshot, slotIndex: 0));
            string slot1Endpoint = Assert.IsType<string>(ResolveSlotLocalEndpoint(beforeFaultSnapshot, slotIndex: 1));

            Assert.True(EndpointsMatch(slot0Endpoint, await slot0SessionMapped.Task.WaitAsync(timeout.Token)));
            Assert.True(EndpointsMatch(slot1Endpoint, await slot1SessionMapped.Task.WaitAsync(timeout.Token)));

            ForcePrimaryConnectionState(ownedPublisher, TransitConnectionState.Faulted);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot afterFaultSnapshot = ownedPublisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(TransitConnectionState.Faulted, ResolveSlotState(afterFaultSnapshot, slotIndex: 0));
            Assert.Equal(TransitConnectionState.Ready, ResolveSlotState(afterFaultSnapshot, slotIndex: 1));

            TransitPublishResult result = await ownedPublisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask().WaitAsync(timeout.Token);

            await targetTakethisReceivedOnSlot1.Task.WaitAsync(timeout.Token);
            await targetResponseSentOnSlot1.Task.WaitAsync(timeout.Token);

            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
            Assert.Equal(messageId, result.MessageId);
            Assert.Equal(239, result.ResponseCode);

            _ = setupPublishA;
            _ = setupPublishB;
        }

        /// <summary>
        /// Verifies that disposing a connection during a submission's write-gate wait does not fault the publisher's submission pump.
        /// </summary>
        /// <remarks>
        /// The first submission is deliberately made uncertain by closing its owning session. The publisher must cleanly terminalize that work, remain usable, and later accept a new submission through a different connection.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when reconnect disposes connection while submit waits write gate does not fault pump behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when reconnect disposes connection while submit waits write gate does not fault pump helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenReconnectDisposesConnectionWhileSubmitWaitsWriteGate_DoesNotFaultPump()
        {
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

            string firstMessageId = "<publisher-replace-blocked-first@example.com>";
            string secondMessageId = "<publisher-replace-blocked-second@example.com>";
            byte[] firstPayload = [(byte)'1', (byte)'\n'];
            byte[] secondPayload = [(byte)'2', (byte)'\n'];

            TaskCompletionSource<string> firstTakethisEndpoint = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowFirstSessionClose = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource firstSessionClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string> secondTakethisEndpoint = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource secondTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowSecondResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource secondResponseSent = new(TaskCreationOptions.RunContinuationsAsynchronously);

            FakePublisherServer? fakeServer = null;

            static bool EndpointsMatch(string firstEndpoint, string secondEndpoint)
            {
                if (string.Equals(firstEndpoint, secondEndpoint, StringComparison.Ordinal))
                {
                    return true;
                }

                if (!IPEndPoint.TryParse(firstEndpoint, out IPEndPoint? first)
                    || !IPEndPoint.TryParse(secondEndpoint, out IPEndPoint? second))
                {
                    return false;
                }

                IPAddress firstAddress = first.Address.IsIPv4MappedToIPv6 ? first.Address.MapToIPv4() : first.Address;
                IPAddress secondAddress = second.Address.IsIPv4MappedToIPv6 ? second.Address.MapToIPv4() : second.Address;
                return first.Port == second.Port && firstAddress.Equals(secondAddress);
            }

            static async Task<(TransitPublisher.ConnectionDiagnosticsEntry Owner, TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot Snapshot)> WaitForOutstandingOwnerAsync(
                TransitPublisher publisher,
                string messageId,
                CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                    TransitPublisher.ConnectionDiagnosticsEntry? owner = snapshot.Connections.FirstOrDefault(
                        entry => entry.Snapshot.OutstandingOperations.Any(op => string.Equals(op.MessageId, messageId, StringComparison.Ordinal) && op.WaitingFor239Response));

                    if (owner is not null)
                    {
                        return (owner, snapshot);
                    }

                    await Task.Yield();
                }
            }

            async Task RunSessionAsync(NetworkStream stream, CancellationToken cancellationToken)
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                FakePublisherServer serverInstance = Assert.IsType<FakePublisherServer>(fakeServer);
                string acceptedRemoteEndpoint = serverInstance.GetRemoteEndpoint(stream);

                while (true)
                {
                    string commandLine;
                    try
                    {
                        commandLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("Unexpected EOF while reading stream data.", StringComparison.Ordinal))
                    {
                        return;
                    }

                    if (string.Equals(commandLine, "QUIT", StringComparison.Ordinal))
                    {
                        await FakePublisherServer.WriteLineAsync(stream, "205 closing connection");
                        return;
                    }

                    if (!commandLine.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException($"Unexpected protocol command in {nameof(PublishAsync_WhenReconnectDisposesConnectionWhileSubmitWaitsWriteGate_DoesNotFaultPump)}: '{commandLine}'");
                    }

                    string messageId = commandLine["TAKETHIS ".Length..];
                    byte[] payload = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                    if (string.Equals(messageId, firstMessageId, StringComparison.Ordinal))
                    {
                        Assert.Equal(firstPayload, payload);
                        firstTakethisEndpoint.TrySetResult(acceptedRemoteEndpoint);
                        firstTakethisObserved.TrySetResult();
                        await allowFirstSessionClose.Task.WaitAsync(cancellationToken);
                        stream.Dispose();
                        firstSessionClosed.TrySetResult();
                        return;
                    }

                    if (string.Equals(messageId, secondMessageId, StringComparison.Ordinal))
                    {
                        Assert.Equal(secondPayload, payload);
                        secondTakethisEndpoint.TrySetResult(acceptedRemoteEndpoint);
                        secondTakethisObserved.TrySetResult();
                        await allowSecondResponse.Task.WaitAsync(cancellationToken);
                        await FakePublisherServer.WriteLineAsync(stream, $"239 {secondMessageId} transferred");
                        secondResponseSent.TrySetResult();
                        continue;
                    }

                    throw new InvalidOperationException($"Unexpected TAKETHIS message-id in {nameof(PublishAsync_WhenReconnectDisposesConnectionWhileSubmitWaitsWriteGate_DoesNotFaultPump)}: '{messageId}'");
                }
            }

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                RunSessionAsync,
                RunSessionAsync,
            ]);
            fakeServer = server;

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> firstPublish = publisher.PublishAsync(firstMessageId, firstPayload, CancellationToken.None).AsTask();

            await firstTakethisObserved.Task.WaitAsync(timeout.Token);
            await WaitForOutstandingAwaitingResponsesAsync(publisher, minimumAwaitingResponses: 1, timeout.Token);

            (TransitPublisher.ConnectionDiagnosticsEntry ownerConnection, TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot ownerSnapshot) =
                await WaitForOutstandingOwnerAsync(publisher, firstMessageId, timeout.Token);

            string ownerLocalEndpoint = Assert.IsType<string>(ownerConnection.Snapshot.LocalEndpoint);
            string firstObservedEndpoint = await firstTakethisEndpoint.Task.WaitAsync(timeout.Token);
            Assert.True(EndpointsMatch(ownerLocalEndpoint, firstObservedEndpoint));
            Assert.InRange(ownerConnection.SlotIndex, 0, ownerSnapshot.Slots.Length - 1);

            allowFirstSessionClose.TrySetResult();
            await firstSessionClosed.Task.WaitAsync(timeout.Token);

            TransitPublishResult firstResult = await firstPublish.WaitAsync(timeout.Token);
            Assert.Equal(TransitPublishStatus.Ambiguous, firstResult.Status);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot afterFirstSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.DoesNotContain(afterFirstSnapshot.Connections.SelectMany(static entry => entry.Snapshot.OutstandingOperations),
                operation => string.Equals(operation.MessageId, firstMessageId, StringComparison.Ordinal));
            Assert.Equal(0, GetActiveSubmissionCount(publisher));
            Assert.Equal(0, afterFirstSnapshot.QueueSnapshot.QueuedItemCount);
            Assert.Equal(0, afterFirstSnapshot.QueueSnapshot.InFlightCount);
            Assert.Equal(0, afterFirstSnapshot.QueueSnapshot.RetryPendingCount);

            Task<TransitPublishResult> secondPublish = publisher.PublishAsync(secondMessageId, secondPayload, CancellationToken.None).AsTask();
            await secondTakethisObserved.Task.WaitAsync(timeout.Token);

            (TransitPublisher.ConnectionDiagnosticsEntry secondOwnerConnection, _) =
                await WaitForOutstandingOwnerAsync(publisher, secondMessageId, timeout.Token);

            string secondObservedEndpoint = await secondTakethisEndpoint.Task.WaitAsync(timeout.Token);
            string secondOwnerLocalEndpoint = Assert.IsType<string>(secondOwnerConnection.Snapshot.LocalEndpoint);
            Assert.True(EndpointsMatch(secondOwnerLocalEndpoint, secondObservedEndpoint));
            Assert.False(EndpointsMatch(firstObservedEndpoint, secondObservedEndpoint));
            Assert.NotEqual(ownerConnection.ConnectionId, secondOwnerConnection.ConnectionId);

            allowSecondResponse.TrySetResult();
            await secondResponseSent.Task.WaitAsync(timeout.Token);
            TransitPublishResult secondResult = await secondPublish.WaitAsync(timeout.Token);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot finalSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(TransitPublishStatus.Accepted, secondResult.Status);
            Assert.Equal(239, secondResult.ResponseCode);
            Assert.Equal(secondMessageId, secondResult.MessageId);
            Assert.Equal(TransitConnectionState.Ready, publisher.CurrentState);
        }

        /// <summary>
        /// Verifies that shutdown racing the reconnect-initialization phase prevents a replacement connection from being committed.
        /// </summary>
        /// <remarks>
        /// The test reaches a deterministic primary fault, begins disposal before the replacement handshake can complete, and verifies disconnected state with no active slot replacement and no completed replacement-session handshake.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when shutdown begins during reconnect initialization does not install replacement connection behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when shutdown begins during reconnect initialization does not install replacement connection helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenShutdownBeginsDuringReconnectInitialization_DoesNotInstallReplacementConnection()
        {
            string messageId = "<publisher-reconnect-shutdown-first@example.com>";

            TaskCompletionSource firstSessionTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowFirstSessionClose = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource secondSessionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowSecondGreeting = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource secondSessionCapabilitiesObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                async (stream, cancellationToken) =>
                {
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                    string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal($"TAKETHIS {messageId}", takethisLine);
                    _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    firstSessionTakethisObserved.TrySetResult();

                    await allowFirstSessionClose.Task.WaitAsync(cancellationToken);
                    stream.Dispose();
                },
                async (stream, cancellationToken) =>
                {
                    secondSessionAccepted.TrySetResult();
                    await allowSecondGreeting.Task.WaitAsync(cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    secondSessionCapabilitiesObserved.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                },
            ]);

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> publishTask = publisher.PublishAsync(messageId, new byte[] { (byte)'1', (byte)'\n' }, CancellationToken.None).AsTask();

            using CancellationTokenSource workObservedTimeout = new(TimeSpan.FromSeconds(10));
            await firstSessionTakethisObserved.Task.WaitAsync(workObservedTimeout.Token);
            await WaitForOutstandingAwaitingResponsesAsync(publisher, minimumAwaitingResponses: 1, workObservedTimeout.Token);

            allowFirstSessionClose.TrySetResult();

            using CancellationTokenSource faultObservedTimeout = new(TimeSpan.FromSeconds(10));
            while (true)
            {
                TransitConnectionState state = GetPrimaryConnectionState(publisher);
                if (state is TransitConnectionState.Faulted or TransitConnectionState.Disconnected)
                {
                    break;
                }

                await Task.Delay(10, faultObservedTimeout.Token);
            }

            Task disposeTask = publisher.DisposeAsync().AsTask();
            allowSecondGreeting.TrySetResult();
            using CancellationTokenSource completeTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult result = await publishTask.WaitAsync(completeTimeout.Token);
            await disposeTask.WaitAsync(completeTimeout.Token);

            Assert.True(result.Status is TransitPublishStatus.Ambiguous or TransitPublishStatus.Canceled);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            Assert.True(snapshot.Slots.Length > 0);
            Assert.False(snapshot.Slots[0].HasCurrentConnection);
            Assert.False(secondSessionCapabilitiesObserved.Task.IsCompleted);
        }

        /// <summary>
        /// Verifies that a reconnect attempt which fails during initialization completes the affected publish as <c>Ambiguous</c> and records the corresponding ambiguous article metric.
        /// </summary>
        /// <remarks>
        /// The test uses a first session that disconnects and a replacement session that returns a temporary failure before normal streaming is established.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when reconnect initialization fails completes submission ambiguous and increments ambiguous metric behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when reconnect initialization fails completes submission ambiguous and increments ambiguous metric helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenReconnectInitializationFails_CompletesSubmissionAmbiguousAndIncrementsAmbiguousMetric()
        {
            string messageId = "<publisher-reconnect-init-fail@example.com>";
            byte[] payload = [(byte)'R', (byte)'\n'];

            TaskCompletionSource firstSessionReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowFirstSessionClose = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource firstSessionClosed = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource secondSessionAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowSecondGreeting = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                async (stream, cancellationToken) =>
                {
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");
                    firstSessionReady.TrySetResult();

                    await allowFirstSessionClose.Task.WaitAsync(cancellationToken);
                    stream.Dispose();
                    firstSessionClosed.TrySetResult();
                },
                async (stream, cancellationToken) =>
                {
                    secondSessionAccepted.TrySetResult();
                    await allowSecondGreeting.Task.WaitAsync(cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "400 temporary failure");
                },
            ]);

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1, transitRetryMaxAttempts: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            GlobalTransitWorkQueue queue = GetGlobalQueue(publisher);
            object claimGate = GetClaimGate(queue);
            TaskCompletionSource claimGateHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
            using ManualResetEventSlim releaseClaimGate = new(false);
            Task claimGateHolder = Task.Run(() =>
            {
                Monitor.Enter(claimGate);
                try
                {
                    claimGateHeld.TrySetResult();
                    releaseClaimGate.Wait();
                }
                finally
                {
                    Monitor.Exit(claimGate);
                }
            }, CancellationToken.None);

            try
            {
                using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
                await claimGateHeld.Task.WaitAsync(timeout.Token);
                Task<TransitPublishResult> publishTask = publisher.PublishAsync(messageId, payload, CancellationToken.None).AsTask();

                await firstSessionReady.Task.WaitAsync(timeout.Token);

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot pendingSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                Assert.Equal(1, pendingSnapshot.QueueSnapshot.QueuedItemCount);
                Assert.Equal(1, GetActiveSubmissionCount(publisher));
                Assert.False(secondSessionAccepted.Task.IsCompleted);

                allowFirstSessionClose.TrySetResult();
                await firstSessionClosed.Task.WaitAsync(timeout.Token);

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot afterFirstCloseSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                Assert.Equal(1, afterFirstCloseSnapshot.QueueSnapshot.QueuedItemCount);
                Assert.Equal(1, GetActiveSubmissionCount(publisher));

                releaseClaimGate.Set();

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot beforeReplacementFailureSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                Assert.Equal(1, beforeReplacementFailureSnapshot.QueueSnapshot.QueuedItemCount);
                Assert.Equal(1, GetActiveSubmissionCount(publisher));
                Assert.False(publishTask.IsCompleted);
                Assert.False(allowSecondGreeting.Task.IsCompleted);

                allowSecondGreeting.TrySetResult();
                TransitPublishResult result = await publishTask.WaitAsync(timeout.Token);

                Assert.Equal(TransitPublishStatus.Ambiguous, result.Status);
                TransitTransportSnapshot snapshot = publisher.CaptureTransportSnapshot(activeConnections: 0, outstandingSubmissions: 0);
                Assert.Equal(1, snapshot.TotalArticlesSubmitted);
                Assert.Equal(1, snapshot.TotalArticlesAmbiguous);
            }
            finally
            {
                releaseClaimGate.Set();
                await claimGateHolder.WaitAsync(CancellationToken.None);
            }
        }

        /// <summary>
        /// Verifies that cancellation while a publish is waiting for channel admission does not increment the total-submitted metric.
        /// </summary>
        /// <remarks>
        /// The test distinguishes admission waiting from actual enqueue/ownership and checks that the canceled request does not appear as a submitted article.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when admission canceled before enqueue does not increment total submitted metric behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when admission canceled before enqueue does not increment total submitted metric helper.</returns>
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
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
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
            while (Volatile.Read(ref observedTakethisCount) < 1)
            {
                await Task.Delay(10, fillTimeout.Token);
            }

            TransitTransportSnapshot fillSnapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            while (fillSnapshot.TotalArticlesSubmitted < expectedAdmittedOutstanding)
            {
                await Task.Delay(10, fillTimeout.Token);
                fillSnapshot = publisher.CaptureTransportSnapshot(activeConnections: 1, outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            }

            long baselineTotalArticlesSubmitted = fillSnapshot.TotalArticlesSubmitted;
            long baselineAdmissionWaitCount = publisher.CaptureConnectionDiagnosticsSnapshot().QueueSnapshot.AdmissionWaitCount;

            using CancellationTokenSource blockedCts = new();
            Task<TransitPublishResult> blockedAdmission = publisher.PublishAsync("<metric-blocked@example.com>", payload, blockedCts.Token).AsTask();

            using CancellationTokenSource blockedObservedTimeout = new(TimeSpan.FromSeconds(10));
            while (publisher.CaptureConnectionDiagnosticsSnapshot().QueueSnapshot.AdmissionWaitCount <= baselineAdmissionWaitCount)
            {
                Assert.False(blockedAdmission.IsCompleted);
                await Task.Delay(10, blockedObservedTimeout.Token);
            }

            Assert.False(blockedAdmission.IsCompleted);

            blockedCts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blockedAdmission);

            TransitTransportSnapshot snapshotBeforeDispose = publisher.CaptureTransportSnapshot(
                activeConnections: 1,
                outstandingSubmissions: checked((int)GetQueuedSubmissionCount(publisher)));
            Assert.Equal(baselineTotalArticlesSubmitted, snapshotBeforeDispose.TotalArticlesSubmitted);

            using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            await Task.WhenAll(admittedSubmissions).WaitAsync(completionTimeout.Token);
        }

        /// <summary>
        /// Verifies that preempting submission processing while the first pipeline lane is stalled terminalizes all already-admitted publishes.
        /// </summary>
        /// <remarks>
        /// The test focuses on queue draining and task completion when the active lane cannot make response progress.
        /// </remarks>
        /// <summary>
        /// Confirms the preempt submission processing async when first pipeline lane stalls completes all admitted publish tasks and clears queued count behavior.
        /// </summary>
        /// <returns>The value returned by the preempt submission processing async when first pipeline lane stalls completes all admitted publish tasks and clears queued count helper.</returns>
        [Fact]
        public async Task PreemptSubmissionProcessingAsync_WhenFirstPipelineLaneStalls_CompletesAllAdmittedPublishTasksAndClearsQueuedCount()
        {
            byte[] payload = [(byte)'S', (byte)'\n'];
            TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                firstTakethisObserved.TrySetResult();

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                publisher.PublishAsync("<stall-1@example.com>", payload, CancellationToken.None).AsTask(),
                publisher.PublishAsync("<stall-2@example.com>", payload, CancellationToken.None).AsTask(),
                publisher.PublishAsync("<stall-3@example.com>", payload, CancellationToken.None).AsTask(),
            ];

            using CancellationTokenSource firstObserveTimeout = new(TimeSpan.FromSeconds(10));
            await firstTakethisObserved.Task.WaitAsync(firstObserveTimeout.Token);

            using CancellationTokenSource preemptTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.PreemptSubmissionProcessingAsync(preemptTimeout.Token);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult[] results = await Task.WhenAll(submissions).WaitAsync(completionTimeout.Token);

            Assert.All(results, result => Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous));
            Assert.Equal(0, GetQueuedSubmissionCount(publisher));
        }

        /// <summary>
        /// Confirms preempt submission processing async  when connection pending is zero and publisher outstanding is positive  terminalizes publisher backlog behavior.
        /// </summary>
        /// <remarks>
        /// Preemption must find and terminalize the publisher-level backlog rather than relying solely on connection-local pending state.
        /// </remarks>
        /// <summary>
        /// Confirms the preempt submission processing async when connection pending is zero and publisher outstanding is positive terminalizes publisher backlog behavior.
        /// </summary>
        /// <returns>The value returned by the preempt submission processing async when connection pending is zero and publisher outstanding is positive terminalizes publisher backlog helper.</returns>
        [Fact]
        public async Task PreemptSubmissionProcessingAsync_WhenConnectionPendingIsZeroAndPublisherOutstandingIsPositive_TerminalizesPublisherBacklog()
        {
            byte[] payload = [(byte)'Z', (byte)'\n'];
            TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource reconnectCapabilitiesObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseReconnectSession = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                async (stream, cancellationToken) =>
                {
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    Assert.Equal("CAPABILITIES", await FakePublisherServer.ReadLineAsync(stream, cancellationToken));
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    Assert.Equal("MODE STREAM", await FakePublisherServer.ReadLineAsync(stream, cancellationToken));
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                    string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);
                    _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                    firstTakethisObserved.TrySetResult();
                },
                async (stream, cancellationToken) =>
                {
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    string capabilitiesCommand = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal("CAPABILITIES", capabilitiesCommand);
                    reconnectCapabilitiesObserved.TrySetResult();
                    await releaseReconnectSession.Task.WaitAsync(cancellationToken);
                },
            ]);

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] admittedSubmissions =
            [
                publisher.PublishAsync("<post-init-1@example.com>", payload, CancellationToken.None).AsTask(),
                publisher.PublishAsync("<post-init-2@example.com>", payload, CancellationToken.None).AsTask(),
                publisher.PublishAsync("<post-init-3@example.com>", payload, CancellationToken.None).AsTask(),
            ];

            using CancellationTokenSource firstObserveTimeout = new(TimeSpan.FromSeconds(10));
            await firstTakethisObserved.Task.WaitAsync(firstObserveTimeout.Token);

            using CancellationTokenSource reconnectObserveTimeout = new(TimeSpan.FromSeconds(10));
            await reconnectCapabilitiesObserved.Task.WaitAsync(reconnectObserveTimeout.Token);

            using CancellationTokenSource stateTimeout = new(TimeSpan.FromSeconds(10));
            while (true)
            {
                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                int pendingMessageIds = snapshot.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
                long queuedSubmissions = snapshot.QueuedSubmissionCount;

                if (pendingMessageIds == 0 && queuedSubmissions > 0)
                {
                    break;
                }

                await Task.Delay(10, stateTimeout.Token);
            }

            using CancellationTokenSource preemptTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.PreemptSubmissionProcessingAsync(preemptTimeout.Token);

            releaseReconnectSession.TrySetResult();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult[] results = await Task.WhenAll(admittedSubmissions).WaitAsync(completionTimeout.Token);

            Assert.All(results, result => Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous));
            Assert.Equal(0, GetQueuedSubmissionCount(publisher));
        }

        /// <summary>
        /// Verifies that preemption drains a queued backlog larger than the connection pipeline depth.
        /// </summary>
        /// <remarks>
        /// All admitted publish tasks must reach a lifecycle-valid terminal outcome and the queued/active accounting must return to zero.
        /// </remarks>
        /// <summary>
        /// Confirms the preempt submission processing async when queued backlog exceeds pipeline depth terminalizes all admitted submissions and clears tracking behavior.
        /// </summary>
        /// <returns>The value returned by the preempt submission processing async when queued backlog exceeds pipeline depth terminalizes all admitted submissions and clears tracking helper.</returns>
        [Fact]
        public async Task PreemptSubmissionProcessingAsync_WhenQueuedBacklogExceedsPipelineDepth_TerminalizesAllAdmittedSubmissionsAndClearsTracking()
        {
            const int submissionCount = 12;
            byte[] payload = [(byte)'Q', (byte)'\n'];
            TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                firstTakethisObserved.TrySetResult();

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 2);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions = Enumerable.Range(0, submissionCount)
                .Select(index => publisher.PublishAsync($"<queued-preempt-{index}@example.com>", payload, CancellationToken.None).AsTask())
                .ToArray();

            using CancellationTokenSource firstObserveTimeout = new(TimeSpan.FromSeconds(10));
            await firstTakethisObserved.Task.WaitAsync(firstObserveTimeout.Token);

            using CancellationTokenSource preemptTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.PreemptSubmissionProcessingAsync(preemptTimeout.Token);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult[] results = await Task.WhenAll(submissions).WaitAsync(completionTimeout.Token);

            Assert.Equal(submissionCount, results.Length);
            Assert.All(results, result => Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous));
            Assert.Equal(0, GetQueuedSubmissionCount(publisher));
            Assert.Equal(0, GetActiveSubmissionCount(publisher));
        }

        /// <summary>
        /// Verifies exactly-once terminalization of a submission that is already in flight when preemption begins.
        /// </summary>
        /// <remarks>
        /// The test guards against both stranded completion and duplicate terminal completion while also checking active bookkeeping cleanup.
        /// </remarks>
        /// <summary>
        /// Confirms the preempt submission processing async when submission is in flight terminalizes exactly once and clears tracking behavior.
        /// </summary>
        /// <returns>The value returned by the preempt submission processing async when submission is in flight terminalizes exactly once and clears tracking helper.</returns>
        [Fact]
        public async Task PreemptSubmissionProcessingAsync_WhenSubmissionIsInFlight_TerminalizesExactlyOnceAndClearsTracking()
        {
            byte[] payload = [(byte)'I', (byte)'\n'];
            TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.StartsWith("TAKETHIS ", takethisLine, StringComparison.Ordinal);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                firstTakethisObserved.TrySetResult();

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> submission = publisher.PublishAsync("<inflight-preempt@example.com>", payload, CancellationToken.None).AsTask();

            using CancellationTokenSource firstObserveTimeout = new(TimeSpan.FromSeconds(10));
            await firstTakethisObserved.Task.WaitAsync(firstObserveTimeout.Token);

            using CancellationTokenSource preemptTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.PreemptSubmissionProcessingAsync(preemptTimeout.Token);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult result = await submission.WaitAsync(completionTimeout.Token);

            Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous);
            Assert.Equal(0, GetQueuedSubmissionCount(publisher));
            Assert.Equal(0, GetActiveSubmissionCount(publisher));
        }

        /// <summary>
        /// Confirms preempt submission processing async  when raced with ownership transition across repeated runs  does not strand submission behavior.
        /// </summary>
        /// <remarks>
        /// Each iteration creates real demand, proves ownership/awaiting state, invokes preemption, and requires every submission to terminalize with no queue, active-work, or outstanding-operation residue.
        /// </remarks>
        /// <summary>
        /// Confirms the preempt submission processing async when raced with ownership transition across repeated runs does not strand submission behavior.
        /// </summary>
        /// <returns>The value returned by the preempt submission processing async when raced with ownership transition across repeated runs does not strand submission helper.</returns>
        [Fact]
        public async Task PreemptSubmissionProcessingAsync_WhenRacedWithOwnershipTransitionAcrossRepeatedRuns_DoesNotStrandSubmission()
        {
            const int iterations = 20;

            for (int iteration = 0; iteration < iterations; iteration++)
            {
                byte[] payload = [(byte)'R', (byte)'\n'];
                string[] messageIds =
                [
                    $"<transition-{iteration}-1@example.com>",
                    $"<transition-{iteration}-2@example.com>",
                    $"<transition-{iteration}-3@example.com>",
                ];

                HashSet<string> expectedMessageIds = [.. messageIds];
                TaskCompletionSource firstTakethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

                await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
                {
                    await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                    await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                    await FakePublisherServer.WriteLineAsync(stream, ".");
                    await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                    await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                    while (true)
                    {
                        string protocolLine;
                        try
                        {
                            protocolLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                        }
                        catch (InvalidOperationException ex) when (ex.Message.Contains("Unexpected EOF while reading stream data.", StringComparison.Ordinal))
                        {
                            return;
                        }

                        if (string.Equals(protocolLine, "QUIT", StringComparison.Ordinal))
                        {
                            return;
                        }

                        if (!protocolLine.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                        {
                            throw new InvalidOperationException($"Unexpected protocol command in {nameof(PreemptSubmissionProcessingAsync_WhenRacedWithOwnershipTransitionAcrossRepeatedRuns_DoesNotStrandSubmission)}: '{protocolLine}'");
                        }

                        string messageId = protocolLine["TAKETHIS ".Length..];
                        Assert.Contains(messageId, expectedMessageIds);
                        _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                        firstTakethisObserved.TrySetResult();
                    }
                });

                await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
                await publisher.InitializeAsync(CancellationToken.None);

                Task<TransitPublishResult>[] submissions =
                [
                    publisher.PublishAsync(messageIds[0], payload, CancellationToken.None).AsTask(),
                    publisher.PublishAsync(messageIds[1], payload, CancellationToken.None).AsTask(),
                    publisher.PublishAsync(messageIds[2], payload, CancellationToken.None).AsTask(),
                ];

                using CancellationTokenSource ownershipObservedTimeout = new(TimeSpan.FromSeconds(10));
                await firstTakethisObserved.Task.WaitAsync(ownershipObservedTimeout.Token);
                await WaitForOutstandingAwaitingResponsesAsync(publisher, minimumAwaitingResponses: 1, ownershipObservedTimeout.Token);

                using CancellationTokenSource preemptTimeout = new(TimeSpan.FromSeconds(10));
                await publisher.PreemptSubmissionProcessingAsync(preemptTimeout.Token);

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot postPreemptSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                Assert.True(postPreemptSnapshot.QueueSnapshot.IsAdmissionFrozen);

                using CancellationTokenSource disposeTimeout = new(TimeSpan.FromSeconds(10));
                await publisher.DisposeAsync().AsTask().WaitAsync(disposeTimeout.Token);

                using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
                TransitPublishResult[] results = await Task.WhenAll(submissions).WaitAsync(completionTimeout.Token);

                Assert.Equal(3, results.Length);
                Assert.Equal(3, results.Select(result => result.MessageId).Distinct(StringComparer.Ordinal).Count());
                Assert.All(results, result =>
                {
                    Assert.True(result.Status is TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous or TransitPublishStatus.Accepted or TransitPublishStatus.Rejected);
                });

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot finalSnapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                Assert.Equal(0, finalSnapshot.QueueSnapshot.QueuedItemCount);
                Assert.Equal(0, finalSnapshot.QueueSnapshot.InFlightCount);
                Assert.Equal(0, finalSnapshot.QueueSnapshot.RetryPendingCount);
                Assert.Equal(0, GetActiveSubmissionCount(publisher));
                Assert.DoesNotContain(
                    finalSnapshot.Connections.SelectMany(static entry => entry.Snapshot.OutstandingOperations),
                    operation => expectedMessageIds.Contains(operation.MessageId));
            }
        }

        /// <summary>
        /// Verifies that preemption racing a definitive completion does not strand or double-complete any admitted submission.
        /// </summary>
        /// <remarks>
        /// The first article is deliberately accepted immediately before preemption, while additional submissions remain subject to the preemption path.
        /// </remarks>
        /// <summary>
        /// Confirms the preempt submission processing async when completion races with preemption completes all admitted submissions with single terminal outcome per submission behavior.
        /// </summary>
        /// <returns>The value returned by the preempt submission processing async when completion races with preemption completes all admitted submissions with single terminal outcome per submission helper.</returns>
        [Fact]
        public async Task PreemptSubmissionProcessingAsync_WhenCompletionRacesWithPreemption_CompletesAllAdmittedSubmissionsWithSingleTerminalOutcomePerSubmission()
        {
            byte[] payload = [(byte)'N', (byte)'\n'];
            TaskCompletionSource firstAcceptedObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string firstTakethis = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("TAKETHIS <race-preempt-1@example.com>", firstTakethis);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "239 <race-preempt-1@example.com> transferred");
                firstAcceptedObserved.TrySetResult();

                string nextCommand = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                if (nextCommand.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                {
                    _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                }
                else
                {
                    Assert.Equal("QUIT", nextCommand);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisher = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 1);
            await publisher.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult>[] submissions =
            [
                publisher.PublishAsync("<race-preempt-1@example.com>", payload, CancellationToken.None).AsTask(),
                publisher.PublishAsync("<race-preempt-2@example.com>", payload, CancellationToken.None).AsTask(),
                publisher.PublishAsync("<race-preempt-3@example.com>", payload, CancellationToken.None).AsTask(),
            ];

            using CancellationTokenSource acceptedTimeout = new(TimeSpan.FromSeconds(10));
            await firstAcceptedObserved.Task.WaitAsync(acceptedTimeout.Token);

            using CancellationTokenSource preemptTimeout = new(TimeSpan.FromSeconds(10));
            await publisher.PreemptSubmissionProcessingAsync(preemptTimeout.Token);

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult[] results = await Task.WhenAll(submissions).WaitAsync(completionTimeout.Token);

            Assert.Equal(3, results.Length);
            Assert.Equal(3, results.Select(result => result.MessageId).Distinct(StringComparer.Ordinal).Count());
            Assert.Contains(results, result => result.MessageId == "<race-preempt-1@example.com>" && result.Status is TransitPublishStatus.Accepted or TransitPublishStatus.Ambiguous);
            Assert.All(results, result => Assert.True(result.Status is TransitPublishStatus.Accepted or TransitPublishStatus.Canceled or TransitPublishStatus.Ambiguous));
            Assert.Equal(0, GetQueuedSubmissionCount(publisher));
            Assert.Equal(0, GetActiveSubmissionCount(publisher));
        }

        /// <summary>
        /// Confirms publish async  when pipeline depth two receives two takethis before any response  completes both and clears in flight correlation behavior.
        /// </summary>
        /// <remarks>
        /// The test checks both definitive results and the absence of residual in-flight correlation state before issuing the follow-up publish.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when pipeline depth two receives two takethis before any response completes both and clears in flight correlation behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when pipeline depth two receives two takethis before any response completes both and clears in flight correlation helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenPipelineDepthTwoReceivesTwoTakethisBeforeAnyResponse_CompletesBothAndClearsInFlightCorrelation()
        {
            byte[] payload = [(byte)'P', (byte)'\n'];
            string messageIdA = "<pipeline-two-a@example.com>";
            string messageIdB = "<pipeline-two-b@example.com>";
            string messageIdC = "<pipeline-two-c@example.com>";
            TransitPublisher? publisher = null;

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 2, cancellationToken);

                string firstCommand = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                if (string.Equals(firstCommand, "QUIT", StringComparison.Ordinal))
                {
                    return;
                }

                if (!firstCommand.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Unexpected command while awaiting first TAKETHIS: '{firstCommand}'.");
                }

                string firstMessageId = firstCommand["TAKETHIS ".Length..];
                Assert.Contains(firstMessageId, new[] { messageIdA, messageIdB });
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                string secondCommand = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                if (string.Equals(secondCommand, "QUIT", StringComparison.Ordinal))
                {
                    return;
                }

                if (!secondCommand.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Unexpected command while awaiting second TAKETHIS: '{secondCommand}'.");
                }

                string secondMessageId = secondCommand["TAKETHIS ".Length..];
                Assert.Contains(secondMessageId, new[] { messageIdA, messageIdB });
                Assert.NotEqual(firstMessageId, secondMessageId);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                await FakePublisherServer.WriteLineAsync(stream, $"239 {firstMessageId} transferred");
                await FakePublisherServer.WriteLineAsync(stream, $"239 {secondMessageId} transferred");

                string thirdCommand = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                if (string.Equals(thirdCommand, "QUIT", StringComparison.Ordinal))
                {
                    return;
                }

                if (!thirdCommand.StartsWith("TAKETHIS ", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Unexpected command while awaiting third TAKETHIS: '{thirdCommand}'.");
                }

                string thirdMessageId = thirdCommand["TAKETHIS ".Length..];
                Assert.Equal(messageIdC, thirdMessageId);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, $"239 {thirdMessageId} transferred");

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 2);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> publishA = publisherInstance.PublishAsync(messageIdA, payload, CancellationToken.None).AsTask();
            Task<TransitPublishResult> publishB = publisherInstance.PublishAsync(messageIdB, payload, CancellationToken.None).AsTask();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult[] firstPair = await Task.WhenAll(publishA, publishB).WaitAsync(completionTimeout.Token);

            Assert.Equal(2, firstPair.Length);
            Assert.All(firstPair, result =>
            {
                Assert.Equal(TransitPublishStatus.Accepted, result.Status);
                Assert.Equal(239, result.ResponseCode);
            });
            Assert.Contains(firstPair, result => result.MessageId == messageIdA);
            Assert.Contains(firstPair, result => result.MessageId == messageIdB);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics = publisherInstance.CaptureConnectionDiagnosticsSnapshot();
            Assert.Equal(0, diagnostics.QueueSnapshot.InFlightCount);
            Assert.True(diagnostics.Connections.All(static entry => entry.Snapshot.OutstandingOperations.Length == 0));

            TransitPublishResult followUp = await publisherInstance.PublishAsync(messageIdC, payload, CancellationToken.None).AsTask().WaitAsync(completionTimeout.Token);
            Assert.Equal(TransitPublishStatus.Accepted, followUp.Status);
            Assert.Equal(239, followUp.ResponseCode);
            Assert.Equal(TransitConnectionState.Ready, publisherInstance.CurrentState);
        }

        /// <summary>
        /// Verifies that two concurrently claimed submissions carrying the same Message-ID both reach lifecycle-terminal outcomes without stranded queue or connection correlation state.
        /// </summary>
        /// <remarks>
        /// The fake server deliberately supplies one acceptance and one duplicate response. The invariant is completion and cleanup rather than a specific outcome assignment to either duplicate request.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when pipeline depth two claims duplicate message id completes without stranding claimed work behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when pipeline depth two claims duplicate message id completes without stranding claimed work helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenPipelineDepthTwoClaimsDuplicateMessageId_CompletesWithoutStrandingClaimedWork()
        {
            byte[] payload = [(byte)'D', (byte)'\n'];
            string duplicateMessageId = "<duplicate-inflight@example.com>";

            TransitPublisher? publisher = null;

            static bool IsLifecycleTerminalStatus(TransitPublishStatus status)
            {
                return status is TransitPublishStatus.Accepted
                    or TransitPublishStatus.Rejected
                    or TransitPublishStatus.Ambiguous;
            }

            async Task WaitForNoStrandedWorkAsync(TransitPublisher publisherInstance, CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisherInstance.CaptureConnectionDiagnosticsSnapshot();
                    int outstandingOperations = snapshot.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
                    int awaitingResponses = snapshot.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Count(static op => op.WaitingFor239Response));

                    if (snapshot.QueueSnapshot.QueuedItemCount == 0
                        && snapshot.QueueSnapshot.InFlightCount == 0
                        && snapshot.QueueSnapshot.RetryPendingCount == 0
                        && snapshot.QueuedSubmissionCount == 0
                        && GetActiveSubmissionCount(publisherInstance) == 0
                        && outstandingOperations == 0
                        && awaitingResponses == 0)
                    {
                        return;
                    }

                    await Task.Yield();
                }
            }

            async Task HandleResponsiveSessionAsync(NetworkStream stream, CancellationToken cancellationToken)
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                TransitPublisher activePublisher = publisher ?? throw new InvalidOperationException("Publisher is not available for synchronization.");
                await WaitForOutstandingAwaitingResponsesAsync(activePublisher, minimumAwaitingResponses: 2, cancellationToken);

                int responsesSent = 0;
                while (responsesSent < 2)
                {
                    string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    Assert.Equal($"TAKETHIS {duplicateMessageId}", takethisLine);
                    _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);

                    bool accepted = (responsesSent & 1) == 0;
                    await FakePublisherServer.WriteLineAsync(
                        stream,
                        accepted
                            ? $"239 {duplicateMessageId} transferred"
                            : $"439 {duplicateMessageId} duplicate");

                    responsesSent++;
                }

                while (true)
                {
                    string protocolLine;
                    try
                    {
                        protocolLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("Unexpected EOF while reading stream data.", StringComparison.Ordinal))
                    {
                        return;
                    }

                    if (string.Equals(protocolLine, "QUIT", StringComparison.Ordinal))
                    {
                        return;
                    }

                    throw new InvalidOperationException($"Unexpected protocol command in {nameof(PublishAsync_WhenPipelineDepthTwoClaimsDuplicateMessageId_CompletesWithoutStrandingClaimedWork)}: '{protocolLine}'");
                }
            }

            await using FakePublisherServer server = await FakePublisherServer.StartSessionsAsync(
            [
                HandleResponsiveSessionAsync,
                HandleResponsiveSessionAsync,
                HandleResponsiveSessionAsync,
            ]);

            await using TransitPublisher publisherInstance = CreatePublisher(server.Port, connectionPoolSize: 1, perConnectionPipelineDepth: 2);
            publisher = publisherInstance;
            await publisherInstance.InitializeAsync(CancellationToken.None);

            Task<TransitPublishResult> first = publisherInstance.PublishAsync(duplicateMessageId, payload, CancellationToken.None).AsTask();
            Task<TransitPublishResult> second = publisherInstance.PublishAsync(duplicateMessageId, payload, CancellationToken.None).AsTask();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult[] results = await Task.WhenAll(first, second).WaitAsync(completionTimeout.Token);

            Assert.Equal(2, results.Length);
            Assert.All(results, r => Assert.Equal(duplicateMessageId, r.MessageId));
            Assert.All(results, result => Assert.True(IsLifecycleTerminalStatus(result.Status), $"Unexpected terminal status for duplicate submission {result.MessageId}: {result.Status}"));

            await WaitForNoStrandedWorkAsync(publisherInstance, completionTimeout.Token);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics = publisherInstance.CaptureConnectionDiagnosticsSnapshot();
            int outstandingOperations = diagnostics.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
            int awaitingResponses = diagnostics.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Count(static op => op.WaitingFor239Response));

            Assert.Equal(0, diagnostics.QueueSnapshot.QueuedItemCount);
            Assert.Equal(0, diagnostics.QueueSnapshot.InFlightCount);
            Assert.Equal(0, diagnostics.QueueSnapshot.RetryPendingCount);
            Assert.Equal(0, diagnostics.QueuedSubmissionCount);
            Assert.Equal(0, GetActiveSubmissionCount(publisherInstance));
            Assert.Equal(0, outstandingOperations);
            Assert.Equal(0, awaitingResponses);
        }

        /// <summary>
        /// Verifies that a logger exception raised during cancellation/outcome continuation processing does not escape to the caller or fault the publisher.
        /// </summary>
        /// <remarks>
        /// The caller must receive its cancellation, the admitted submission must still reach terminal cleanup, and the test intentionally injects a throwing logger without depending on a stale event-ID contract.
        /// </remarks>
        /// <summary>
        /// Confirms the publish async when cancellation outcome logging throws logs continuation failure without faulting caller behavior.
        /// </summary>
        /// <returns>The value returned by the publish async when cancellation outcome logging throws logs continuation failure without faulting caller helper.</returns>
        [Fact]
        public async Task PublishAsync_WhenCancellationOutcomeLoggingThrows_LogsContinuationFailureWithoutFaultingCaller()
        {
            string messageId = "<publisher-cancel-logger-failure@example.com>";
            byte[] payload = [(byte)'C', (byte)'\n'];
            ThrowOnOutcomeCapturingLoggerProvider provider = new();
            TaskCompletionSource takethisObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowResponse239 = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakePublisherServer server = await FakePublisherServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakePublisherServer.WriteLineAsync(stream, "200 transit ready");
                await FakePublisherServer.ExpectCommandAsync(stream, "CAPABILITIES", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "101 Capability list:");
                await FakePublisherServer.WriteLineAsync(stream, "STREAMING");
                await FakePublisherServer.WriteLineAsync(stream, ".");
                await FakePublisherServer.ExpectCommandAsync(stream, "MODE STREAM", cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethisLine = await FakePublisherServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethisLine);
                _ = await FakePublisherServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                takethisObserved.TrySetResult();

                await allowResponse239.Task.WaitAsync(cancellationToken);
                await FakePublisherServer.WriteLineAsync(stream, $"239 {messageId} transferred");
            });

            await using TransitPublisher publisher = CreatePublisherWithLogger(server.Port, connectionPoolSize: 1, provider.CreateLogger<TransitPublisher>());
            await publisher.InitializeAsync(CancellationToken.None);

            using CancellationTokenSource cts = new();
            ValueTask<TransitPublishResult> pending = publisher.PublishAsync(messageId, payload, cts.Token);

            using CancellationTokenSource admissionTimeout = new(TimeSpan.FromSeconds(5));
            await takethisObserved.Task.WaitAsync(admissionTimeout.Token);
            await WaitForOutstandingAwaitingResponsesAsync(publisher, minimumAwaitingResponses: 1, admissionTimeout.Token);

            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.AsTask());

            allowResponse239.TrySetResult();

            using CancellationTokenSource cleanupTimeout = new(TimeSpan.FromSeconds(5));
            while (true)
            {
                cleanupTimeout.Token.ThrowIfCancellationRequested();

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                int outstandingOperations = snapshot.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);

                if (snapshot.QueueSnapshot.QueuedItemCount == 0
                    && snapshot.QueueSnapshot.InFlightCount == 0
                    && snapshot.QueueSnapshot.RetryPendingCount == 0
                    && snapshot.QueuedSubmissionCount == 0
                    && outstandingOperations == 0
                    && GetActiveSubmissionCount(publisher) == 0)
                {
                    break;
                }

                await Task.Yield();
            }

            IReadOnlyList<CapturingLoggerProvider.LogEntry> logEntries = provider.CaptureEntriesSnapshot();
            Assert.DoesNotContain(logEntries, entry => entry.EventId.Id == 2209);
            Assert.NotEqual(TransitConnectionState.Faulted, publisher.CurrentState);
        }

        /// <summary>
        /// Verifies that the private lifecycle-failure classifier recognizes a writer-not-initialized lifecycle exception as a connection-lifecycle submission failure.
        /// </summary>
        [Fact]
        public void IsConnectionLifecycleSubmitFailure_WhenLifecycleExceptionWriterNotInitialized_ReturnsTrue()
        {
            TransitConnection connection = new("localhost", 119, useSsl: false, NullLogger<TransitPublisher>.Instance);

            bool classified = InvokeIsConnectionLifecycleSubmitFailure(
                connection,
                new TransitConnection.TransitConnectionLifecycleException(TransitConnection.TransitConnectionLifecycleFailure.WriterNotInitialized));

            Assert.True(classified);
        }

        /// <summary>
        /// Verifies that the private lifecycle-failure classifier recognizes a writer-completed-during-submission lifecycle exception as a connection-lifecycle submission failure.
        /// </summary>
        [Fact]
        public void IsConnectionLifecycleSubmitFailure_WhenLifecycleExceptionWriterCompleted_ReturnsTrue()
        {
            TransitConnection connection = new("localhost", 119, useSsl: false, NullLogger<TransitPublisher>.Instance);

            bool classified = InvokeIsConnectionLifecycleSubmitFailure(
                connection,
                new TransitConnection.TransitConnectionLifecycleException(TransitConnection.TransitConnectionLifecycleFailure.WriterCompletedDuringTakethisSubmission));

            Assert.True(classified);
        }

        /// <summary>
        /// Verifies that an arbitrary <c>InvalidOperationException</c> is not classified as a lifecycle submission failure merely because the connection is faulted.
        /// </summary>
        [Fact]
        public void IsConnectionLifecycleSubmitFailure_WhenArbitraryInvalidOperationAndConnectionFaulted_ReturnsFalse()
        {
            TransitConnection connection = new("localhost", 119, useSsl: false, NullLogger<TransitPublisher>.Instance);
            SetConnectionState(connection, TransitConnectionState.Faulted);

            bool classified = InvokeIsConnectionLifecycleSubmitFailure(connection, new InvalidOperationException("arbitrary"));

            Assert.False(classified);
        }

        /// <summary>
        /// Verifies that an arbitrary <c>InvalidOperationException</c> is not classified as a lifecycle submission failure on a healthy ready connection.
        /// </summary>
        [Fact]
        public void IsConnectionLifecycleSubmitFailure_WhenInvalidOperationAndConnectionReady_ReturnsFalse()
        {
            TransitConnection connection = new("localhost", 119, useSsl: false, NullLogger<TransitPublisher>.Instance);
            SetConnectionState(connection, TransitConnectionState.Ready);

            bool classified = InvokeIsConnectionLifecycleSubmitFailure(connection, new InvalidOperationException("arbitrary"));

            Assert.False(classified);
        }

        /// <summary>
        /// Verifies that the lifecycle-failure classifier recognizes the transport exception types that are always treated as lifecycle submission failures.
        /// </summary>
        /// <remarks>
        /// The member data covers disposed transport, I/O failure, and connection-reset socket failure cases.
        /// </remarks>
        /// <summary>
        /// Confirms the is connection lifecycle submit failure when known lifecycle exception type returns true behavior.
        /// </summary>
        /// <param name="exception">The exception used by this test scenario.</param>
        [Theory]
        [MemberData(nameof(GetAlwaysClassifiedLifecycleExceptions))]
        public void IsConnectionLifecycleSubmitFailure_WhenKnownLifecycleExceptionType_ReturnsTrue(Exception exception)
        {
            TransitConnection connection = new("localhost", 119, useSsl: false, NullLogger<TransitPublisher>.Instance);

            bool classified = InvokeIsConnectionLifecycleSubmitFailure(connection, exception);

            Assert.True(classified);
        }

        /// <summary>
        /// Supplies exception instances that the lifecycle-failure classifier must always recognize as transport/lifecycle failures.
        /// </summary>
        /// <returns>An enumerable containing disposed-transport, I/O, and connection-reset exception cases.</returns>
        /// <summary>
        /// Confirms the get always classified lifecycle exceptions behavior.
        /// </summary>
        /// <returns>The value returned by the get always classified lifecycle exceptions helper.</returns>
        public static IEnumerable<object[]> GetAlwaysClassifiedLifecycleExceptions()
        {
            yield return [new ObjectDisposedException("transport")];
            yield return [new IOException("io")];
            yield return [new SocketException((int)SocketError.ConnectionReset)];
        }

        /// <summary>
        /// Invokes the private <c>TransitPublisher.IsConnectionLifecycleSubmitFailure</c> classifier through reflection so its boundary cases can be tested without changing production visibility.
        /// </summary>
        /// <param name="connection">The transit connection whose lifecycle state participates in classification.</param>
        /// <param name="exception">The exception to classify.</param>
        /// <returns>Returns <c>true</c> when the production classifier recognizes the exception as a lifecycle submission failure; otherwise <c>false</c>.</returns>
        /// <summary>
        /// Confirms the invoke is connection lifecycle submit failure behavior.
        /// </summary>
        /// <returns>The value returned by the invoke is connection lifecycle submit failure helper.</returns>
        private static bool InvokeIsConnectionLifecycleSubmitFailure(TransitConnection connection, Exception exception)
        {
            MethodInfo? method = typeof(TransitPublisher).GetMethod("IsConnectionLifecycleSubmitFailure", BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(method);

            object? raw = method.Invoke(null, [connection, exception]);
            return Assert.IsType<bool>(raw);
        }

        /// <summary>
        /// Creates a <see cref="TransitPublisher"/> configured for the fake transit server used by the tests.
        /// </summary>
        /// <param name="port">The fake server TCP port.</param>
        /// <param name="connectionPoolSize">The number of publisher connection slots.</param>
        /// <param name="perConnectionPipelineDepth">The maximum claimed pipeline depth per connection.</param>
        /// <param name="connectionResponseProgressTimeout">Optional response-progress watchdog timeout.</param>
        /// <param name="connectionResponseProgressCheckInterval">Optional response-progress watchdog polling interval.</param>
        /// <returns>Returns a configured but uninitialized publisher instance.</returns>
        private static TransitPublisher CreatePublisher(
            int port,
            int connectionPoolSize,
            int perConnectionPipelineDepth = 8,
            int transitRetryMaxAttempts = 3,
            TimeSpan? connectionResponseProgressTimeout = null,
            TimeSpan? connectionResponseProgressCheckInterval = null,
            Action? claimBoundaryObserved = null)
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
                WriteBatchCoalesceMicroseconds: 250,
                TransitRetryMaxAttempts: transitRetryMaxAttempts);

            return new TransitPublisher(
                options,
                TimeProvider.System,
                NullLogger<TransitPublisher>.Instance,
                connectionPoolSize,
                perConnectionPipelineDepth,
                connectionResponseProgressTimeout,
                connectionResponseProgressCheckInterval,
                timingCollector: null,
                claimBoundaryObserved: claimBoundaryObserved);
        }

        /// <summary>
        /// Reads the publisher's queue diagnostics and returns the number of currently queued submission items.
        /// </summary>
        /// <param name="publisher">The publisher whose queue state is inspected.</param>
        /// <returns>Returns the current queued-item count.</returns>
        /// <summary>
        /// Confirms the get queued submission count behavior.
        /// </summary>
        /// <returns>The value returned by the get queued submission count helper.</returns>
        private static long GetQueuedSubmissionCount(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            return snapshot.QueueSnapshot.QueuedItemCount;
        }

        /// <summary>
        /// Reads the publisher's private active-work dictionary through reflection for assertions about submission lifecycle cleanup.
        /// </summary>
        /// <param name="publisher">The publisher whose active-work tracking is inspected.</param>
        /// <returns>Returns the number of active work items currently tracked by the publisher.</returns>
        /// <summary>
        /// Confirms the get active submission count behavior.
        /// </summary>
        /// <returns>The value returned by the get active submission count helper.</returns>
        private static int GetActiveSubmissionCount(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            FieldInfo? field = typeof(TransitPublisher).GetField("_activeWorkItems", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);

            object? raw = field.GetValue(publisher);
            Assert.NotNull(raw);

            IDictionary activeSubmissions = Assert.IsAssignableFrom<IDictionary>(raw);
            return activeSubmissions.Count;
        }

        /// <summary>
        /// Determines whether slot zero currently has a connection installed.
        /// </summary>
        /// <param name="publisher">The publisher whose connection-slot diagnostics are inspected.</param>
        /// <returns>Returns <c>1</c> when the primary slot has a current connection; otherwise <c>0</c>.</returns>
        /// <summary>
        /// Confirms the get primary connection count behavior.
        /// </summary>
        /// <returns>The value returned by the get primary connection count helper.</returns>
        private static int GetPrimaryConnectionCount(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            if (snapshot.Slots.Length == 0)
            {
                return 0;
            }

            return snapshot.Slots[0].HasCurrentConnection ? 1 : 0;
        }

        /// <summary>
        /// Waits until publisher diagnostics show at least the requested number of outstanding operations awaiting definitive <c>239</c> responses.
        /// </summary>
        /// <param name="publisher">The publisher being observed.</param>
        /// <param name="minimumAwaitingResponses">The minimum number of awaiting-response operations required before returning.</param>
        /// <param name="cancellationToken">Cancels the wait if the expected state cannot be reached.</param>
        /// <returns>Completes when the required awaiting-response count is observed.</returns>
        private static async Task WaitForOutstandingAwaitingResponsesAsync(
            TransitPublisher publisher,
            int minimumAwaitingResponses,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            if (minimumAwaitingResponses <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(minimumAwaitingResponses), minimumAwaitingResponses, "Minimum awaiting response count must be greater than zero.");
            }

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
                int awaitingResponses = snapshot.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Count(static operation => operation.WaitingFor239Response));
                if (awaitingResponses >= minimumAwaitingResponses)
                {
                    return;
                }

                await Task.Yield();
            }
        }

        /// <summary>
        /// Waits until the supplied publish tasks contain at least the requested number of successfully accepted results.
        /// </summary>
        /// <param name="submissions">The publish tasks to inspect.</param>
        /// <param name="expectedAcceptedCount">The minimum number of completed tasks whose status is <c>Accepted</c>.</param>
        /// <param name="cancellationToken">Cancels the wait while awaiting accepted results.</param>
        /// <returns>Completes when the accepted-result threshold is reached.</returns>
        private static async Task WaitForAcceptedCountAsync(
            Task<TransitPublishResult>[] submissions,
            int expectedAcceptedCount,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(submissions);

            if (expectedAcceptedCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(expectedAcceptedCount), expectedAcceptedCount, "Expected accepted count must be non-negative.");
            }

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int acceptedCount = 0;
                foreach (Task<TransitPublishResult> submission in submissions)
                {
                    if (!submission.IsCompletedSuccessfully)
                    {
                        continue;
                    }

                    if (submission.Result.Status == TransitPublishStatus.Accepted)
                    {
                        acceptedCount++;
                    }
                }

                if (acceptedCount >= expectedAcceptedCount)
                {
                    return;
                }

                await Task.Yield();
            }
        }

        /// <summary>
        /// Waits until slot zero exposes a concrete primary connection instance.
        /// </summary>
        /// <param name="publisher">The publisher whose primary slot is observed.</param>
        /// <param name="cancellationToken">Cancels the wait if no connection appears.</param>
        /// <returns>Returns the current primary <see cref="TransitConnection"/>.</returns>
        /// <summary>
        /// Confirms the wait for primary connection async behavior.
        /// </summary>
        /// <returns>The value returned by the wait for primary connection async helper.</returns>
        private static async Task<TransitConnection> WaitForPrimaryConnectionAsync(TransitPublisher publisher, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                TransitConnection? connection = TryGetPrimaryConnection(publisher);
                if (connection is not null)
                {
                    return connection;
                }

                await Task.Yield();
            }
        }

        /// <summary>
        /// Waits until the publisher reaches a specified top-level connection state.
        /// </summary>
        /// <param name="publisher">The publisher whose state is observed.</param>
        /// <param name="expectedState">The state that must be observed.</param>
        /// <param name="cancellationToken">Cancels the wait if the expected state is not reached.</param>
        /// <returns>Completes when the publisher reaches the requested state.</returns>
        private static async Task WaitForPublisherStateAsync(
            TransitPublisher publisher,
            TransitConnectionState expectedState,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (publisher.CurrentState == expectedState)
                {
                    return;
                }

                await Task.Yield();
            }
        }

        /// <summary>
        /// Awaits a task and converts its completion or failure into an exception value for tests that need to inspect asynchronous failures without immediately throwing.
        /// </summary>
        /// <param name="task">The task whose completion is observed.</param>
        /// <param name="cancellationToken">Cancels the wait for task completion.</param>
        /// <returns>Returns <c>null</c> when the task completes successfully; otherwise returns the observed exception.</returns>
        /// <summary>
        /// Confirms the capture exception async behavior.
        /// </summary>
        /// <returns>The value returned by the capture exception async helper.</returns>
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

        /// <summary>
        /// Invokes the publisher's private reconnect operation through reflection for tests that need to exercise reconnect concurrency directly.
        /// </summary>
        /// <param name="publisher">The publisher on which reconnect is invoked.</param>
        /// <param name="slotIndex">The connection slot targeted by reconnect.</param>
        /// <param name="cancellationToken">Cancels the reconnect operation.</param>
        /// <returns>Returns the task representing the reconnect operation.</returns>
        /// <summary>
        /// Confirms the invoke reconnect async behavior.
        /// </summary>
        /// <returns>The value returned by the invoke reconnect async helper.</returns>
        private static Task InvokeReconnectAsync(TransitPublisher publisher, int slotIndex, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            MethodInfo? reconnect = typeof(TransitPublisher).GetMethod("ReconnectAsync", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(reconnect);

            object? invocation = reconnect.Invoke(publisher, [slotIndex, cancellationToken]);
            Task task = Assert.IsAssignableFrom<Task>(invocation);
            return task;
        }

        /// <summary>
        /// Invokes private connection creation for a specific slot and installs the resulting connection into the publisher's private connection array for targeted lifecycle tests.
        /// </summary>
        /// <param name="publisher">The publisher receiving the connection.</param>
        /// <param name="slotIndex">The target connection slot.</param>
        /// <param name="cancellationToken">Cancels connection creation.</param>
        /// <returns>Returns the initialized connection installed for the requested slot.</returns>
        /// <summary>
        /// Confirms the create connection for slot async behavior.
        /// </summary>
        /// <returns>The value returned by the create connection for slot async helper.</returns>
        private static async Task<TransitConnection> CreateConnectionForSlotAsync(TransitPublisher publisher, int slotIndex, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            MethodInfo? createAndInitialize = typeof(TransitPublisher).GetMethod("CreateAndInitializeConnectionAsync", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(createAndInitialize);

            object? invocation = createAndInitialize.Invoke(publisher, [slotIndex, false, cancellationToken]);
            Task<TransitConnection> task = Assert.IsAssignableFrom<Task<TransitConnection>>(invocation);
            TransitConnection connection = await task;

            FieldInfo? connectionsField = typeof(TransitPublisher).GetField("_connections", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(connectionsField);

            object? connectionsRaw = connectionsField.GetValue(publisher);
            TransitConnection[] connections = Assert.IsType<TransitConnection[]>(connectionsRaw);
            Assert.InRange(slotIndex, 0, connections.Length - 1);
            connections[slotIndex] = connection;

            return connection;
        }

        /// <summary>
        /// Returns the concrete connection currently stored in the publisher's primary slot.
        /// </summary>
        /// <param name="publisher">The publisher whose primary connection is required.</param>
        /// <returns>Returns the primary <see cref="TransitConnection"/>; the test fails if none is installed.</returns>
        /// <summary>
        /// Confirms the get primary connection behavior.
        /// </summary>
        /// <returns>The value returned by the get primary connection helper.</returns>
        private static TransitConnection GetPrimaryConnection(TransitPublisher publisher)
        {
            TransitConnection? connection = TryGetPrimaryConnection(publisher);
            return Assert.IsType<TransitConnection>(connection);
        }

        /// <summary>
        /// Reads the publisher's private primary connection slot for tests that need to inspect or manipulate connection state.
        /// </summary>
        /// <param name="publisher">The publisher whose primary slot is inspected.</param>
        /// <returns>Returns the primary <see cref="TransitConnection"/> when installed; otherwise <c>null</c>.</returns>
        /// <summary>
        /// Confirms the try get primary connection behavior.
        /// </summary>
        /// <returns>The value returned by the try get primary connection helper.</returns>
        private static TransitConnection? TryGetPrimaryConnection(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            FieldInfo? connectionsField = typeof(TransitPublisher).GetField("_connections", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(connectionsField);

            object? connectionsRaw = connectionsField.GetValue(publisher);
            TransitConnection[] connections = Assert.IsType<TransitConnection[]>(connectionsRaw);
            if (connections.Length == 0)
            {
                return null;
            }

            return connections[0];
        }

        /// <summary>
        /// Retrieves the private write gate from a transit connection for tests that need to synchronize against connection write ownership.
        /// </summary>
        /// <param name="connection">The connection whose write gate is inspected.</param>
        /// <returns>Returns the connection's <see cref="SemaphoreSlim"/> write gate.</returns>
        /// <summary>
        /// Confirms the get connection write gate behavior.
        /// </summary>
        /// <returns>The value returned by the get connection write gate helper.</returns>
        private static SemaphoreSlim GetConnectionWriteGate(TransitConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);

            FieldInfo? writeGateField = typeof(TransitConnection).GetField("_writeGate", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(writeGateField);

            object? writeGateRaw = writeGateField.GetValue(connection);
            return Assert.IsType<SemaphoreSlim>(writeGateRaw);
        }

        /// <summary>
        /// Sets a transit connection's current lifecycle state through the compiled property backing storage for targeted state-machine tests.
        /// </summary>
        /// <param name="connection">The connection whose state is changed.</param>
        /// <param name="state">The state to assign.</param>
        /// <summary>
        /// Confirms the set connection state behavior.
        /// </summary>
        private static void SetConnectionState(TransitConnection connection, TransitConnectionState state)
        {
            ArgumentNullException.ThrowIfNull(connection);

            PropertyInfo? currentStateProperty = typeof(TransitConnection).GetProperty(nameof(TransitConnection.CurrentState), BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(currentStateProperty);

            MethodInfo? setter = currentStateProperty.SetMethod;
            Assert.NotNull(setter);
            setter.Invoke(connection, [state]);
        }

        /// <summary>
        /// Resolves the current state of the connection installed in slot zero from publisher diagnostics.
        /// </summary>
        /// <param name="publisher">The publisher whose primary slot is observed.</param>
        /// <returns>Returns the primary connection state, or <see cref="TransitConnectionState.Disconnected"/> when no current primary connection exists.</returns>
        /// <summary>
        /// Confirms the get primary connection state behavior.
        /// </summary>
        /// <returns>The value returned by the get primary connection state helper.</returns>
        private static TransitConnectionState GetPrimaryConnectionState(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot snapshot = publisher.CaptureConnectionDiagnosticsSnapshot();
            if (snapshot.Slots.Length == 0)
            {
                return TransitConnectionState.Disconnected;
            }

            TransitPublisher.ConnectionSlotSnapshot primarySlot = snapshot.Slots[0];
            string? candidatePrimaryConnectionId = primarySlot.CurrentConnectionId;
            if (!primarySlot.HasCurrentConnection || string.IsNullOrWhiteSpace(candidatePrimaryConnectionId))
            {
                return TransitConnectionState.Disconnected;
            }

            string primaryConnectionId = candidatePrimaryConnectionId;
            TransitPublisher.ConnectionDiagnosticsEntry? entry = snapshot.Connections.FirstOrDefault(candidate => string.Equals(candidate.ConnectionId, primaryConnectionId, StringComparison.Ordinal));
            return entry?.Snapshot.CurrentState ?? TransitConnectionState.Disconnected;
        }

        /// <summary>
        /// Gets the remaining worker count tracked by the publisher for shutdown-lifecycle assertions.
        /// </summary>
        /// <param name="publisher">Publisher whose worker count is inspected.</param>
        /// <returns>The current count of connection workers that have not yet exited.</returns>
        private static int GetRemainingConnectionWorkerCount(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            FieldInfo? field = typeof(TransitPublisher).GetField("_remainingConnectionWorkers", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);

            object? value = field.GetValue(publisher);
            return Assert.IsType<int>(value);
        }

        /// <summary>
        /// Waits until the publisher worker counter reaches the requested value.
        /// </summary>
        /// <param name="publisher">Publisher whose worker count is observed.</param>
        /// <param name="expectedCount">Expected remaining worker count.</param>
        /// <param name="cancellationToken">Cancellation token for bounded waiting.</param>
        /// <returns>A task that completes once the expected worker count is observed.</returns>
        private static async Task WaitForRemainingConnectionWorkerCountAsync(TransitPublisher publisher, int expectedCount, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            while (GetRemainingConnectionWorkerCount(publisher) != expectedCount)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
            }
        }

        /// <summary>
        /// Reads the publisher's owned global queue through reflection for lifecycle assertions.
        /// </summary>
        /// <param name="publisher">Publisher whose queue reference is inspected.</param>
        /// <returns>The current global transit work queue instance owned by the publisher.</returns>
        private static GlobalTransitWorkQueue GetGlobalQueue(TransitPublisher publisher)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            FieldInfo? field = typeof(TransitPublisher).GetField("_globalQueue", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);

            object? value = field.GetValue(publisher);
            return Assert.IsType<GlobalTransitWorkQueue>(value);
        }

        /// <summary>
        /// Reads the queue-owned retry-signal semaphore through reflection for lifecycle assertions.
        /// </summary>
        /// <param name="queue">Queue whose retry signal is inspected.</param>
        /// <returns>The queue-owned retry scheduling semaphore.</returns>
        private static SemaphoreSlim GetRetryScheduledSignal(GlobalTransitWorkQueue queue)
        {
            ArgumentNullException.ThrowIfNull(queue);

            FieldInfo? field = typeof(GlobalTransitWorkQueue).GetField("_retryScheduledSignal", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);

            object? value = field.GetValue(queue);
            return Assert.IsType<SemaphoreSlim>(value);
        }

        /// <summary>
        /// Reads the queue claim gate object through reflection so tests can deterministically block worker claim progress.
        /// </summary>
        /// <param name="queue">Queue whose claim gate is inspected.</param>
        /// <returns>The queue claim synchronization object.</returns>
        private static object GetClaimGate(GlobalTransitWorkQueue queue)
        {
            ArgumentNullException.ThrowIfNull(queue);

            FieldInfo? field = typeof(GlobalTransitWorkQueue).GetField("_claimGate", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);

            object? value = field.GetValue(queue);
            return Assert.IsType<object>(value);
        }

        /// <summary>
        /// Forces the primary connection into a specified state through the test reflection seam.
        /// </summary>
        /// <param name="publisher">The publisher whose primary connection is changed.</param>
        /// <param name="state">The state to assign to the primary connection.</param>
        /// <summary>
        /// Confirms the force primary connection state behavior.
        /// </summary>
        private static void ForcePrimaryConnectionState(TransitPublisher publisher, TransitConnectionState state)
        {
            ArgumentNullException.ThrowIfNull(publisher);

            TransitConnection connection = GetPrimaryConnection(publisher);
            SetConnectionState(connection, state);
        }

        /// <summary>
        /// Creates a test publisher using a caller-supplied <see cref="ILogger{TCategoryName}"/> implementation.
        /// </summary>
        /// <param name="port">The fake server TCP port.</param>
        /// <param name="connectionPoolSize">The number of publisher connection slots.</param>
        /// <param name="logger">The logger supplied to the publisher.</param>
        /// <param name="perConnectionPipelineDepth">The maximum pipeline depth per connection.</param>
        /// <returns>Returns a configured but uninitialized publisher instance using the supplied logger.</returns>
        /// <summary>
        /// Confirms the create publisher with logger behavior.
        /// </summary>
        /// <returns>The value returned by the create publisher with logger helper.</returns>
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

        /// <summary>
        /// Captures structured log entries emitted by the publisher so tests can inspect logging side effects without requiring an external logging backend.
        /// </summary>
        /// <remarks>
        /// The provider serializes access to its shared entry list because publisher logging may occur concurrently with the test thread.
        /// </remarks>
        private sealed class CapturingLoggerProvider
        {
            /// <summary>Protects the shared captured-entry list from concurrent logger writes and test reads.</summary>
            private readonly object _gate = new();

            /// <summary>Contains the structured log entries captured by this provider.</summary>
            internal List<LogEntry> Entries { get; } = [];

            /// <summary>
            /// Creates a logger instance for the requested logging category.
            /// </summary>
            /// <remarks>
            /// </remarks>
            internal ILogger<T> CreateLogger<T>()
            {
                return new CapturingLogger<T>(Entries, _gate);
            }

            /// <summary>
            /// Represents one captured logging event, including its event identity, severity, rendered message, exception, and structured state values.
            /// </summary>
            /// <remarks>
            /// <param name="EventId">The event identifier supplied by the logger.</param>
            /// <param name="LogLevel">The severity associated with the event.</param>
            /// <param name="Message">The rendered log message.</param>
            /// <param name="Exception">The exception associated with the event, if any.</param>
            /// <param name="StateValues">Structured state values supplied with the event.</param>
            /// </remarks>
            /// <summary>
            /// Confirms the log entry behavior.
            /// </summary>
            /// <returns>The value returned by the log entry helper.</returns>
            internal sealed record LogEntry(EventId EventId, LogLevel LogLevel, string Message, Exception? Exception, IReadOnlyDictionary<string, object?> StateValues);

            /// <summary>
            /// Provides an <see cref="ILogger{TCategoryName}"/> implementation that captures log records in a shared list.
            /// </summary>
            /// <remarks>
            /// <param name="entries">The shared list receiving captured entries.</param>
            /// <param name="gate">The synchronization object protecting the shared list.</param>
            /// </remarks>
            private sealed class CapturingLogger<T>(List<LogEntry> entries, object gate) : ILogger<T>
            {
                /// <summary>References the shared list into which this logger writes captured entries.</summary>
                private readonly List<LogEntry> _entries = entries;
                /// <summary>Protects the shared captured-entry list during concurrent logging.</summary>
                private readonly object _gate = gate;

                /// <summary>
                /// Creates a no-op logging scope because the tests do not require scope state.
                /// </summary>
                /// <remarks>
                /// <param name="state">The scope state supplied by the caller.</param><returns>A disposable no-op scope.</returns>
                /// </remarks>
                public IDisposable BeginScope<TState>(TState state) where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
                /// Reports that logging is enabled for every requested log level.
                /// </summary>
                /// <remarks>
                /// <param name="logLevel">The log level being queried.</param><returns><c>true</c> for every log level.</returns>
                /// </remarks>
                /// <summary>
                /// Confirms the is enabled behavior.
                /// </summary>
                /// <returns>The value returned by the is enabled helper.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return true;
                }

                /// <summary>
                /// Captures a structured log event and appends it to the provider's synchronized entry list.
                /// </summary>
                /// <remarks>
                /// <param name="logLevel">The event severity.</param>
                /// <param name="eventId">The event identifier.</param>
                /// <param name="state">The structured logging state.</param>
                /// <param name="exception">The associated exception, if any.</param>
                /// <param name="formatter">The formatter used to render the state and exception.</param>
                /// </remarks>
                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    Dictionary<string, object?> stateValues = [];
                    if (state is IEnumerable<KeyValuePair<string, object?>> structuredState)
                    {
                        foreach (KeyValuePair<string, object?> item in structuredState)
                        {
                            stateValues[item.Key] = item.Value;
                        }
                    }

                    lock (_gate)
                    {
                        _entries.Add(new LogEntry(eventId, logLevel, message, exception, stateValues));
                    }
                }

                /// <summary>
                /// Provides the shared no-op disposable scope returned by the test logger.
                /// </summary>
                private sealed class NullScope : IDisposable
                {
                    /// <summary>The singleton no-op logging scope instance.</summary>
                    internal static readonly NullScope Instance = new();

                    /// <summary>Disposes the no-op scope; no state is held and no action is required.</summary>
                    /// <summary>
                    /// Confirms the dispose behavior.
                    /// </summary>
                    public void Dispose()
                    {
                    }
                }
            }
        }

        /// <summary>
        /// Captures publisher log entries while deliberately throwing for the outcome-continuation event used by the logging-failure regression test.
        /// </summary>
        /// <remarks>
        /// The provider first records the event and then throws for event ID <c>2204</c>, allowing the test to verify that a logger failure does not escape through the publisher's cancellation continuation.
        /// </remarks>
        private sealed class ThrowOnOutcomeCapturingLoggerProvider
        {
            /// <summary>Protects the shared captured-entry list from concurrent logger writes and snapshot reads.</summary>
            private readonly object _gate = new();

            /// <summary>Contains captured log entries shared with the throwing logger instances.</summary>
            internal List<CapturingLoggerProvider.LogEntry> Entries { get; } = [];

            /// <summary>
            /// Returns a stable snapshot of the captured log entries.
            /// </summary>
            /// <remarks>
            /// <returns>A point-in-time array containing all entries captured so far.</returns>
            /// </remarks>
            /// <summary>
            /// Confirms the capture entries snapshot behavior.
            /// </summary>
            /// <returns>The value returned by the capture entries snapshot helper.</returns>
            internal IReadOnlyList<CapturingLoggerProvider.LogEntry> CaptureEntriesSnapshot()
            {
                lock (_gate)
                {
                    return Entries.ToArray();
                }
            }

            /// <summary>
            /// Creates a throwing logger instance for the requested logging category.
            /// </summary>
            /// <remarks>
            /// </remarks>
            internal ILogger<T> CreateLogger<T>()
            {
                return new ThrowOnOutcomeCapturingLogger<T>(Entries, _gate);
            }

            /// <summary>
            /// Provides an <see cref="ILogger{TCategoryName}"/> implementation that records events and deliberately throws for the targeted outcome event.
            /// </summary>
            /// <remarks>
            /// <param name="entries">The shared entry list receiving captured events.</param>
            /// <param name="gate">The synchronization object protecting the shared list.</param>
            /// </remarks>
            private sealed class ThrowOnOutcomeCapturingLogger<T>(List<CapturingLoggerProvider.LogEntry> entries, object gate) : ILogger<T>
            {
                /// <summary>References the shared captured-entry list.</summary>
                private readonly List<CapturingLoggerProvider.LogEntry> _entries = entries;
                /// <summary>Protects the shared captured-entry list during concurrent logging.</summary>
                private readonly object _gate = gate;

                /// <summary>
                /// Creates a no-op logging scope because the test logger does not require scope state.
                /// </summary>
                /// <remarks>
                /// <param name="state">The scope state supplied by the caller.</param><returns>A disposable no-op scope.</returns>
                /// </remarks>
                public IDisposable BeginScope<TState>(TState state) where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
                /// Reports that logging is enabled for every requested log level.
                /// </summary>
                /// <remarks>
                /// <param name="logLevel">The log level being queried.</param><returns><c>true</c> for every log level.</returns>
                /// </remarks>
                /// <summary>
                /// Confirms the is enabled behavior.
                /// </summary>
                /// <returns>The value returned by the is enabled helper.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return true;
                }

                /// <summary>
                /// Captures a structured log event and throws after recording event ID <c>2204</c> to simulate a logger failure.
                /// </summary>
                /// <remarks>
                /// <param name="logLevel">The event severity.</param>
                /// <param name="eventId">The event identifier.</param>
                /// <param name="state">The structured logging state.</param>
                /// <param name="exception">The associated exception, if any.</param>
                /// <param name="formatter">The formatter used to render the state and exception.</param>
                /// </remarks>
                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    Dictionary<string, object?> stateValues = [];
                    if (state is IEnumerable<KeyValuePair<string, object?>> structuredState)
                    {
                        foreach (KeyValuePair<string, object?> item in structuredState)
                        {
                            stateValues[item.Key] = item.Value;
                        }
                    }

                    lock (_gate)
                    {
                        _entries.Add(new CapturingLoggerProvider.LogEntry(eventId, logLevel, message, exception, stateValues));
                    }

                    if (eventId.Id == 2204)
                    {
                        throw new InvalidOperationException("Simulated logging failure for delayed outcome continuation.");
                    }
                }

                /// <summary>
                /// Provides the shared no-op disposable scope returned by the throwing test logger.
                /// </summary>
                private sealed class NullScope : IDisposable
                {
                    /// <summary>The singleton no-op logging scope instance.</summary>
                    internal static readonly NullScope Instance = new();

                    /// <summary>Disposes the no-op scope; no state is held and no action is required.</summary>
                    /// <summary>
                    /// Confirms the dispose behavior.
                    /// </summary>
                    public void Dispose()
                    {
                    }
                }
            }
        }

        /// <summary>
        /// Provides a deterministic in-process TCP server used by the publisher tests to emulate transit protocol sessions.
        /// </summary>
        /// <remarks>
        /// The server owns the listener, cancellation source, accepted-session tasks, and endpoint mapping required by tests that correlate publisher connection diagnostics with fake-server sessions.
        /// </remarks>
        private sealed class FakePublisherServer : IAsyncDisposable
        {
            /// <summary>The TCP listener accepting fake transit connections.</summary>
            private readonly TcpListener _listener;
            /// <summary>The ordered session handlers assigned to accepted connections.</summary>
            private readonly IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> _sessions;
            /// <summary>Cancels the accept loop and every active fake-server session.</summary>
            private readonly CancellationTokenSource _cts = new();
            /// <summary>The asynchronous task running the server accept loop.</summary>
            private readonly Task _acceptLoop;
            /// <summary>Tracks every session task so disposal can await all accepted sessions.</summary>
            private readonly ConcurrentBag<Task> _sessionTasks = [];
            /// <summary>Maps each accepted stream to the remote endpoint observed by the fake server.</summary>
            private readonly ConcurrentDictionary<NetworkStream, string> _sessionRemoteEndpoints = new();

            /// <summary>
            /// Creates a fake publisher server around an already-started TCP listener.
            /// </summary>
            /// <remarks>
            /// <param name="listener">The listener that accepts fake publisher connections.</param>
            /// <param name="sessions">The ordered session handlers assigned to accepted connections.</param>
            /// </remarks>
            /// <summary>
            /// Confirms the r behavior.
            /// </summary>
            /// <returns>The value returned by the r helper.</returns>
            private FakePublisherServer(TcpListener listener, IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> sessions)
            {
                _listener = listener;
                _sessions = sessions;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>Gets the dynamically allocated TCP port on which the fake server is listening.</summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Returns the remote endpoint recorded for an accepted fake-server session.
            /// </summary>
            /// <param name="stream">The accepted session stream.</param>
            /// <returns>Returns the endpoint string associated with the stream.</returns>
            /// <summary>
            /// Confirms the get remote endpoint behavior.
            /// </summary>
            /// <returns>The value returned by the get remote endpoint helper.</returns>
            internal string GetRemoteEndpoint(NetworkStream stream)
            {
                ArgumentNullException.ThrowIfNull(stream);

                bool found = _sessionRemoteEndpoints.TryGetValue(stream, out string? endpoint);
                Assert.True(found, "Accepted session remote endpoint was not recorded for this stream.");
                return Assert.IsType<string>(endpoint);
            }

            /// <summary>
            /// Starts a fake transit server with a single session handler.
            /// </summary>
            /// <param name="session">The handler used for the first accepted session.</param>
            /// <returns>Returns a started <see cref="FakePublisherServer"/>.</returns>
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <returns>The value returned by the start async helper.</returns>
            internal static Task<FakePublisherServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
            {
                ArgumentNullException.ThrowIfNull(session);
                return StartSessionsAsync([session]);
            }

            /// <summary>
            /// Starts a fake transit server that consumes a predefined sequence of session handlers, one handler per accepted TCP connection.
            /// </summary>
            /// <param name="sessions">The ordered session handlers to invoke for accepted connections.</param>
            /// <returns>Returns a started <see cref="FakePublisherServer"/>.</returns>
            /// <summary>
            /// Confirms the start sessions async behavior.
            /// </summary>
            /// <returns>The value returned by the start sessions async helper.</returns>
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

            /// <summary>
            /// Accepts fake transit TCP connections, assigns each accepted connection to the next configured session handler, and waits for all tracked sessions to finish.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            /// <summary>
            /// Confirms the accept loop async behavior.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            private async Task AcceptLoopAsync()
            {
                foreach (Func<NetworkStream, CancellationToken, Task> session in _sessions)
                {
                    TcpClient client;
                    try
                    {
                        client = await _listener.AcceptTcpClientAsync(_cts.Token);
                    }
                    catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                    {
                        break;
                    }

                    Task sessionTask = Task.Run(async () =>
                    {
                        using (client)
                        using (NetworkStream stream = client.GetStream())
                        {
                            string remoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? string.Empty;
                            _sessionRemoteEndpoints[stream] = remoteEndpoint;
                            try
                            {
                                await session(stream, _cts.Token);
                            }
                            catch (IOException)
                            {
                            }
                            catch (SocketException)
                            {
                            }
                            finally
                            {
                                _sessionRemoteEndpoints.TryRemove(stream, out _);
                            }
                        }
                    }, CancellationToken.None);

                    _sessionTasks.Add(sessionTask);
                }

                Task[] tracked = _sessionTasks.ToArray();
                if (tracked.Length == 0)
                {
                    return;
                }

                await Task.WhenAll(tracked).ConfigureAwait(false);
            }

            /// <summary>
            /// Reads one CRLF-terminated ASCII line from a fake-server stream while honoring cancellation.
            /// </summary>
            /// <param name="stream">The stream to read.</param>
            /// <param name="cancellationToken">Cancels the read.</param>
            /// <returns>Returns the decoded line without the CRLF terminator.</returns>
            /// <summary>
            /// Confirms the read line async behavior.
            /// </summary>
            /// <returns>The value returned by the read line async helper.</returns>
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

            /// <summary>
            /// Reads and decodes one dot-stuffed <c>TAKETHIS</c> article payload from a fake-server stream.
            /// </summary>
            /// <param name="stream">The stream carrying the article payload.</param>
            /// <param name="cancellationToken">Cancels the payload read.</param>
            /// <returns>Returns the reconstructed article payload bytes.</returns>
            /// <summary>
            /// Confirms the read takethis payload async behavior.
            /// </summary>
            /// <returns>The value returned by the read takethis payload async helper.</returns>
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
            /// Reads exactly one byte from a stream for the fake-server protocol parser.
            /// </summary>
            /// <param name="stream">The stream to read.</param>
            /// <param name="cancellationToken">Cancels the read.</param>
            /// <returns>Returns the next byte, or throws when the stream reaches EOF.</returns>
            /// <summary>
            /// Confirms the read byte async behavior.
            /// </summary>
            /// <returns>The value returned by the read byte async helper.</returns>
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

            /// <summary>
            /// Reads one protocol line and asserts that it exactly matches the expected command.
            /// </summary>
            /// <param name="stream">The stream carrying the command.</param>
            /// <param name="expected">The exact command line expected from the publisher.</param>
            /// <param name="cancellationToken">Cancels the read.</param>
            /// <returns>Completes after the expected command has been observed.</returns>
            /// <summary>
            /// Confirms the expect command async behavior.
            /// </summary>
            /// <returns>The value returned by the expect command async helper.</returns>
            internal static async Task ExpectCommandAsync(Stream stream, string expected, CancellationToken cancellationToken)
            {
                string line = await ReadLineAsync(stream, cancellationToken);
                Assert.Equal(expected, line);
            }

            /// <summary>
            /// Writes one ASCII protocol line followed by CRLF to a fake-server stream.
            /// </summary>
            /// <param name="stream">The destination stream.</param>
            /// <param name="line">The protocol line to write without its CRLF terminator.</param>
            /// <returns>Returns the asynchronous write operation.</returns>
            /// <summary>
            /// Confirms the write line async behavior.
            /// </summary>
            /// <returns>The value returned by the write line async helper.</returns>
            internal static Task WriteLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                return stream.WriteAsync(bytes).AsTask();
            }

            /// <summary>
            /// Stops the fake server, cancels all accepted sessions, waits for the accept loop and tracked session tasks, and finally releases the server cancellation source.
            /// </summary>
            /// <returns>Completes when the fake server and all tracked session activity have been shut down.</returns>
            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            public async ValueTask DisposeAsync()
            {
                _cts.Cancel();
                _listener.Stop();

                try
                {
                    await _acceptLoop.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                {
                }

                Task[] tracked = _sessionTasks.ToArray();
                foreach (Task sessionTask in tracked)
                {
                    try
                    {
                        await sessionTask.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                    {
                    }
                }

                _cts.Dispose();
            }
        }
    }
}

