// <copyright file="ListenerProtocolSessionTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Listener
// Deterministic tests for per-connection listener protocol session multiplexing, bounds, serialization, and lifecycle behavior.

using System.Buffers;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.Listener;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Listener
{
    /// <summary>
    /// Verifies per-connection listener protocol session behavior and ownership boundaries.
    /// </summary>
    public sealed class ListenerProtocolSessionTests
    {
        [Fact]
        public async Task RunAsync_WhenSingleGetRequest_ProcessesAndWritesNotFound()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(1, "30edc94157aa16fe644a45a1f1ffe160");
            TestTransport transport = new([request], 4096);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            await session.RunAsync(CancellationToken.None);

            Assert.Single(handler.SeenRequestIds);
            Assert.Equal<uint>(1, handler.SeenRequestIds[0]);

            byte[] writes = transport.GetWrittenBytes();
            int expectedLength = checked((int)(ListenerProtocol.HeaderLengthBytes + ListenerProtocol.GetResponseNotFoundPayloadLength));
            Assert.Equal(expectedLength, writes.Length);

            ListenerFrameParseResult frame = ListenerProtocolParser.ParseOneFrame(writes);
            Assert.Equal(ListenerFrameParseStatus.Success, frame.Status);
            Assert.Equal(ListenerOpcode.GetResponseNotFound, frame.Frame!.Value.Header.Opcode);
            Assert.Equal<uint>(1, frame.Frame.Value.Header.RequestId);
            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_WhenRequestsCompleteSynchronously_DoesNotRetainCompletedRequestTasks()
        {
            List<byte[]> frames = [];
            for (uint i = 1; i <= 3; i++)
            {
                frames.Add(ListenerProtocolEncoder.EncodeGetRequest(i, "30edc94157aa16fe644a45a1f1ffe160"));
            }

            TestTransport transport = new(frames, 4096);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            await session.RunAsync(CancellationToken.None);

            Assert.Equal(3, handler.SeenRequestIds.Count);
            Assert.Equal(0, session.OutstandingRequestCount);
            Assert.Equal(0, GetTrackedRequestTaskCount(session));
        }

        [Fact]
        public async Task RunAsync_CompletesThenDisposeAsync_DoesNotThrow()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(901, "30edc94157aa16fe644a45a1f1ffe160");
            LifecycleProbeTransport transport = new([request], 4096, disposeException: null, throwOnSecondDispose: true);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            await session.RunAsync(CancellationToken.None);

            Exception? disposeException = await Record.ExceptionAsync(() => session.DisposeAsync().AsTask());

            Assert.Null(disposeException);
            Assert.Equal(1, transport.DisposeCallCount);
        }

        [Fact]
        public async Task DisposeAsync_WhileRunAsyncActive_UsesSingleDrainAndSingleCleanupOwnership()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(902, "30edc94157aa16fe644a45a1f1ffe160");
            LifecycleProbeTransport transport = new([request], 4096, disposeException: null, throwOnSecondDispose: true);
            BlockingAllRequestsHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            Task runTask = session.RunAsync(CancellationToken.None);
            await handler.WaitForInvocationsAtLeastAsync(1);

            Task disposeTask = session.DisposeAsync().AsTask();
            handler.ReleaseAll(ListenerSessionRequestDispatchResult.NotFound());

            Exception? lifecycleException = await Record.ExceptionAsync(async () => await Task.WhenAll(runTask, disposeTask));

            Assert.Null(lifecycleException);
            Assert.Equal(1, transport.DisposeCallCount);
        }

        [Fact]
        public async Task RunAsync_CleanupFaultLeavesDisposedRunCts_DisposeAsyncDoesNotCancelDisposedCts()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(903, "30edc94157aa16fe644a45a1f1ffe160");
            InvalidOperationException injectedDisposeFailure = new("Injected transport dispose failure.");
            LifecycleProbeTransport transport = new([request], 4096, injectedDisposeFailure, throwOnSecondDispose: true);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            Exception? runException = await Record.ExceptionAsync(() => session.RunAsync(CancellationToken.None));
            Assert.NotNull(runException);
            Assert.IsType<InvalidOperationException>(runException);
            Assert.Equal(injectedDisposeFailure.Message, runException.Message);

            Exception? disposeException = await Record.ExceptionAsync(() => session.DisposeAsync().AsTask());
            Assert.NotNull(disposeException);
            Assert.IsType<InvalidOperationException>(disposeException);
            Assert.Equal(injectedDisposeFailure.Message, disposeException.Message);
            Assert.IsNotType<ObjectDisposedException>(disposeException);
            Assert.Equal(1, transport.DisposeCallCount);
        }

        [Fact]
        public async Task RunAsync_WhenDisposeWinsBeforeStartup_IsRejectedWithoutPublishingRuntimeState()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(904, "30edc94157aa16fe644a45a1f1ffe160");
            LifecycleProbeTransport transport = new([request], 4096, disposeException: null, throwOnSecondDispose: true);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            await session.DisposeAsync();

            Exception? runException = await Record.ExceptionAsync(() => session.RunAsync(CancellationToken.None));

            Assert.NotNull(runException);
            Assert.IsType<InvalidOperationException>(runException);
            Assert.Equal("Session is already running.", runException.Message);
            Assert.Null(GetPrivateFieldValue<CancellationTokenSource>(session, "_runCts"));
            Assert.Null(GetPrivateFieldValue<Task>(session, "_runTask"));
            Assert.Null(GetPrivateFieldValue<Task>(session, "_writerTask"));
            Assert.Equal(0, transport.ReadCallCount);
            Assert.Equal(0, transport.WriteCallCount);
            Assert.Equal(1, transport.DisposeCallCount);
            Assert.Equal(ListenerProtocolSessionState.Completed, session.State);
        }

        [Fact]
        public async Task RunAsync_WhenContendingWithDispose_HasSingleLifecycleOutcome()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(905, "30edc94157aa16fe644a45a1f1ffe160");
            LifecycleProbeTransport transport = new([request], 4096, disposeException: null, throwOnSecondDispose: true);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);
            object stateGate = GetPrivateFieldValue<object>(session, "_stateGate")
                ?? throw new InvalidOperationException("Session state gate was not available.");

            Task runTask;
            Task disposeTask;
            lock (stateGate)
            {
                runTask = Task.Run(() => session.RunAsync(CancellationToken.None));
                disposeTask = Task.Run(() => session.DisposeAsync().AsTask());
            }

            Exception? runException = await Record.ExceptionAsync(() => runTask);
            Exception? disposeException = await Record.ExceptionAsync(() => disposeTask);

            Assert.Null(disposeException);
            if (runException is not null)
            {
                Assert.IsType<InvalidOperationException>(runException);
                Assert.Equal("Session is already running.", runException.Message);
            }

            Assert.Equal(1, transport.DisposeCallCount);
            Assert.Equal(ListenerProtocolSessionState.Completed, session.State);
        }

        [Fact]
        public async Task RunAsync_WhenDuplicateOutstandingRequestId_EmitsDuplicateErrorAndKeepsOriginalActive()
        {
            byte[] first = ListenerProtocolEncoder.EncodeGetRequest(7, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] duplicate = ListenerProtocolEncoder.EncodeGetRequest(7, "30edc94157aa16fe644a45a1f1ffe161");

            BlockingFirstRequestHandler handler = new();
            TestTransport transport = new([first, duplicate], 4096);
            ListenerProtocolSession session = new(transport, handler);

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await handler.WaitForFirstRequestAsync();
                await transport.WaitForWritesAtLeastAsync(checked((int)(ListenerProtocol.HeaderLengthBytes + ListenerProtocol.GetResponseErrorPayloadLength)));

                IReadOnlyList<ListenerFrameParseResult> framesBeforeRelease = ParseAllFrames(transport.GetWrittenBytes());
                Assert.Single(framesBeforeRelease);
                Assert.Equal(ListenerOpcode.GetResponseError, framesBeforeRelease[0].Frame!.Value.Header.Opcode);
                Assert.Equal(ListenerProtocolErrorCode.DuplicateRequestId, ReadErrorCode(framesBeforeRelease[0].Frame.Value.Payload));
                Assert.Equal<uint>(7, handler.SeenRequestIds.Single());
                Assert.Equal(1, session.OutstandingRequestCount);
            }
            finally
            {
                handler.ReleaseFirst(ListenerSessionRequestDispatchResult.NotFound());
                await runTask;
            }

            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_WhenOutstandingLimitReached_RejectsAdditionalRequestsWithOverflow()
        {
            List<byte[]> frames = [];
            for (uint i = 1; i <= 65; i++)
            {
                frames.Add(ListenerProtocolEncoder.EncodeGetRequest(i, "30edc94157aa16fe644a45a1f1ffe160"));
            }

            BlockingAllRequestsHandler handler = new();
            TestTransport transport = new(frames, 4096);
            ListenerProtocolSession session = new(transport, handler);

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await handler.WaitForInvocationsAtLeastAsync(8);
                await transport.WaitForWritesAtLeastAsync(checked((int)(ListenerProtocol.HeaderLengthBytes + ListenerProtocol.GetResponseErrorPayloadLength)));

                IReadOnlyList<ListenerFrameParseResult> outbound = ParseAllFrames(transport.GetWrittenBytes());
                Assert.Contains(
                    outbound,
                    result => result.Frame!.Value.Header.RequestId == 65
                        && result.Frame.Value.Header.Opcode == ListenerOpcode.GetResponseError
                        && ReadErrorCode(result.Frame.Value.Payload) == ListenerProtocolErrorCode.RequestTableOverflow);
            }
            finally
            {
                handler.ReleaseAll(ListenerSessionRequestDispatchResult.NotFound());
                await runTask;
            }

            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_EnforcesMaximumConcurrentProcessingOf8()
        {
            List<byte[]> frames = [];
            for (uint i = 1; i <= 16; i++)
            {
                frames.Add(ListenerProtocolEncoder.EncodeGetRequest(i, "30edc94157aa16fe644a45a1f1ffe160"));
            }

            ConcurrencyTrackingHandler handler = new();
            TestTransport transport = new(frames, 4096);
            ListenerProtocolSession session = new(transport, handler);

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await handler.WaitForActiveCountAtLeastAsync(8);
                Assert.Equal(8, handler.MaxActive);
            }
            finally
            {
                handler.ReleaseAll(ListenerSessionRequestDispatchResult.NotFound());
                await runTask;
            }
        }

        [Fact]
        public async Task RunAsync_AllowsOutOfOrderResponseCompletionForMultiplexing()
        {
            byte[] req1 = ListenerProtocolEncoder.EncodeGetRequest(1, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] req2 = ListenerProtocolEncoder.EncodeGetRequest(2, "30edc94157aa16fe644a45a1f1ffe161");

            TaskCompletionSource<bool> request2Terminalized = new(TaskCreationOptions.RunContinuationsAsynchronously);
            OrderedCompletionHandler handler = new();
            TestTransport transport = new([req1, req2], 4096);
            ListenerProtocolSession session = new(
                transport,
                handler,
                onAwaitingReceiptAck: null,
                onTerminalized: requestId =>
                {
                    if (requestId == 2)
                    {
                        request2Terminalized.TrySetResult(true);
                    }
                });

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await handler.WaitForRegistrationAsync(1);
                await handler.WaitForRegistrationAsync(2);
                await transport.WaitForEofAsync();

                Assert.Equal(2, session.OutstandingRequestCount);
                Assert.False(runTask.IsCompleted);

                handler.Complete(2, ListenerSessionRequestDispatchResult.NotFound());
                await transport.WaitForWritesAtLeastAsync(checked((int)(ListenerProtocol.HeaderLengthBytes + ListenerProtocol.GetResponseNotFoundPayloadLength)));
                await request2Terminalized.Task;

                Assert.Equal(1, session.OutstandingRequestCount);
                Assert.False(runTask.IsCompleted);

                handler.Complete(1, ListenerSessionRequestDispatchResult.NotFound());

                await runTask;
            }
            finally
            {
                handler.Complete(1, ListenerSessionRequestDispatchResult.NotFound());
                handler.Complete(2, ListenerSessionRequestDispatchResult.NotFound());
                await runTask;
            }

            Assert.True(runTask.IsCompletedSuccessfully);
            Assert.Equal(0, session.OutstandingRequestCount);

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Equal(2, frames.Count);
            Assert.All(frames, static frame => Assert.Equal(ListenerFrameParseStatus.Success, frame.Status));
            Assert.Equal<uint>(2, frames[0].Frame!.Value.Header.RequestId);
            Assert.Equal<uint>(1, frames[1].Frame!.Value.Header.RequestId);
        }

        [Fact]
        public async Task RunAsync_SerializesOutboundWritesWithoutInterleavingUnderConcurrentCompletion()
        {
            byte[] req1 = ListenerProtocolEncoder.EncodeGetRequest(1, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] req2 = ListenerProtocolEncoder.EncodeGetRequest(2, "30edc94157aa16fe644a45a1f1ffe161");

            OrderedCompletionHandler handler = new();
            TestTransport transport = new([req1, req2], 1);
            ListenerProtocolSession session = new(transport, handler);

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await handler.WaitForRegistrationAsync(1);
                await handler.WaitForRegistrationAsync(2);

                handler.Complete(1, ListenerSessionRequestDispatchResult.NotFound());
                handler.Complete(2, ListenerSessionRequestDispatchResult.NotFound());

                await runTask;
            }
            finally
            {
                handler.Complete(1, ListenerSessionRequestDispatchResult.NotFound());
                handler.Complete(2, ListenerSessionRequestDispatchResult.NotFound());
                await runTask;
            }

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Equal(2, frames.Count);
            Assert.All(frames, static frame => Assert.Equal(ListenerFrameParseStatus.Success, frame.Status));
        }

        [Fact]
        public async Task RunAsync_ParsesFragmentedAndCoalescedInputFrames()
        {
            byte[] req1 = ListenerProtocolEncoder.EncodeGetRequest(1, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] req2 = ListenerProtocolEncoder.EncodeGetRequest(2, "30edc94157aa16fe644a45a1f1ffe161");

            byte[] aggregate = new byte[req1.Length + req2.Length];
            Buffer.BlockCopy(req1, 0, aggregate, 0, req1.Length);
            Buffer.BlockCopy(req2, 0, aggregate, req1.Length, req2.Length);

            byte[] fragmentA = aggregate[..12];
            byte[] fragmentB = aggregate[12..43];
            byte[] fragmentC = aggregate[43..];

            ImmediateNotFoundHandler handler = new();
            TestTransport transport = new([fragmentA, fragmentB, fragmentC], 4096);
            ListenerProtocolSession session = new(transport, handler);

            await session.RunAsync(CancellationToken.None);

            Assert.Equal(2, handler.SeenRequestIds.Count);
            Assert.Contains<uint>(1, handler.SeenRequestIds);
            Assert.Contains<uint>(2, handler.SeenRequestIds);
        }

        [Fact]
        public async Task RunAsync_WhenHeaderDeclaresFrameLargerThanAccumulationCap_ForcesShutdownBeforeAdditionalReads()
        {
            const int parserAccumulationMaxBytes = 32768;
            uint payloadLength = checked((uint)((parserAccumulationMaxBytes - ListenerProtocol.HeaderLengthBytes) + 1));
            byte[] oversizedHeader = CreateRawHeader(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetRequest,
                requestId: 41,
                payloadLength: payloadLength,
                reserved: 0);
            byte[] trailing = Enumerable.Repeat((byte)'z', 64).ToArray();

            ImmediateNotFoundHandler handler = new();
            TestTransport transport = new([oversizedHeader, trailing], 4096);
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(parserAccumulationMaxBytes: parserAccumulationMaxBytes));

            await session.RunAsync(CancellationToken.None);

            Assert.Empty(handler.SeenRequestIds);
            Assert.Equal(0, session.OutstandingRequestCount);
            Assert.Equal(1, transport.ReadCallCount);
            Assert.Equal(1, transport.RemainingReadFragments);
            Assert.Empty(transport.GetWrittenBytes());
        }

        [Fact]
        public async Task RunAsync_WhenDeclaredFrameEqualsAccumulationCap_IsNotRejectedEarly()
        {
            const int parserAccumulationMaxBytes = 32768;
            int payloadLength = parserAccumulationMaxBytes - ListenerProtocol.HeaderLengthBytes;
            byte[] payload = Enumerable.Repeat((byte)'f', payloadLength).ToArray();
            byte[] frame = CreateRawFrame(
                version: ListenerProtocol.Version1,
                opcode: ListenerOpcode.GetResponseFound,
                requestId: 51,
                payload: payload);

            byte[] headerFragment = frame[..ListenerProtocol.HeaderLengthBytes];
            byte[] payloadFragment = frame[ListenerProtocol.HeaderLengthBytes..];

            ImmediateNotFoundHandler handler = new();
            TestTransport transport = new([headerFragment, payloadFragment], 4096);
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(parserAccumulationMaxBytes: parserAccumulationMaxBytes));

            await session.RunAsync(CancellationToken.None);

            Assert.Empty(handler.SeenRequestIds);
            Assert.Equal(3, transport.ReadCallCount);

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Single(frames);
            Assert.Equal(ListenerOpcode.GetResponseError, frames[0].Frame!.Value.Header.Opcode);
            Assert.Equal(ListenerProtocolErrorCode.UnsupportedOpcode, ReadErrorCode(frames[0].Frame.Value.Payload));
        }

        [Fact]
        public async Task RunAsync_WhenValidFrameWithinAccumulationCapArrivesFragmented_ParsesSuccessfully()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(61, "30edc94157aa16fe644a45a1f1ffe160");
            const int parserAccumulationMaxBytes = 32768;
            byte[] fragmentA = request[..8];
            byte[] fragmentB = request[8..20];
            byte[] fragmentC = request[20..];

            ImmediateNotFoundHandler handler = new();
            TestTransport transport = new([fragmentA, fragmentB, fragmentC], 4096);
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(parserAccumulationMaxBytes: parserAccumulationMaxBytes));

            await session.RunAsync(CancellationToken.None);

            Assert.Single(handler.SeenRequestIds);
            Assert.Equal<uint>(61, handler.SeenRequestIds[0]);
        }

        [Fact]
        public async Task RunAsync_WhenHeaderIsIncomplete_WaitsForRemainingHeaderBytes()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(71, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] first = request[..8];
            byte[] second = request[8..];

            ImmediateNotFoundHandler handler = new();
            TestTransport transport = new([first, second], 4096);
            ListenerProtocolSession session = new(transport, handler);

            await session.RunAsync(CancellationToken.None);

            Assert.Equal(3, transport.ReadCallCount);
            Assert.Single(handler.SeenRequestIds);
            Assert.Equal<uint>(71, handler.SeenRequestIds[0]);
        }

        [Fact]
        public async Task RunAsync_WhenUnsupportedVersion_StopsWithoutDispatchingRequests()
        {
            byte[] payload = Encoding.ASCII.GetBytes("30edc94157aa16fe644a45a1f1ffe160");
            byte[] invalid = CreateRawFrame(version: 2, opcode: ListenerOpcode.GetRequest, requestId: 1, payload: payload);

            ImmediateNotFoundHandler handler = new();
            TestTransport transport = new([invalid], 4096);
            ListenerProtocolSession session = new(transport, handler);

            await session.RunAsync(CancellationToken.None);

            Assert.Empty(handler.SeenRequestIds);
            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_WhenFoundThenAck_CorrelatesAndTerminalizesRequest()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(5, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] ack = ListenerProtocolEncoder.EncodeGetReceiptAck(5);

            {
                byte[] duplicateAck = ListenerProtocolEncoder.EncodeGetReceiptAck(5);
                byte[] coalesced = new byte[request.Length + ack.Length + duplicateAck.Length];
                Buffer.BlockCopy(request, 0, coalesced, 0, request.Length);
                Buffer.BlockCopy(ack, 0, coalesced, request.Length, ack.Length);
                Buffer.BlockCopy(duplicateAck, 0, coalesced, request.Length + ack.Length, duplicateAck.Length);

                FoundHandler earlyHandler = new();
                TestTransport earlyTransport = new([coalesced], 4096);
                ListenerProtocolSession earlySession = new(earlyTransport, earlyHandler);

                await earlySession.RunAsync(CancellationToken.None);

                Assert.Equal(0, earlySession.OutstandingRequestCount);
                Assert.Equal(0, earlySession.AwaitingReceiptAckCount);

                IReadOnlyList<ListenerFrameParseResult> earlyFrames = ParseAllFrames(earlyTransport.GetWrittenBytes());
                Assert.Single(earlyFrames);
                Assert.Equal(ListenerOpcode.GetResponseFound, earlyFrames[0].Frame!.Value.Header.Opcode);
                Assert.Equal<uint>(5, earlyFrames[0].Frame.Value.Header.RequestId);
            }

            {
                TaskCompletionSource<uint> awaitingReceiptAck = new(TaskCreationOptions.RunContinuationsAsynchronously);
                FoundHandler normalHandler = new();
                GatedAckReadTransport normalTransport = new(request, ack, 4096);
                ListenerProtocolSession normalSession = new(
                    normalTransport,
                    normalHandler,
                    listenerOptions: null,
                    onAwaitingReceiptAck: requestId => awaitingReceiptAck.TrySetResult(requestId));
                Task normalRunTask = normalSession.RunAsync(CancellationToken.None);

                try
                {
                    await normalTransport.WaitForWritesAtLeastAsync(checked((int)(ListenerProtocol.HeaderLengthBytes + FoundHandler.PayloadLength)));
                    uint awaitingRequestId = await awaitingReceiptAck.Task;
                    Assert.Equal<uint>(5, awaitingRequestId);
                    Assert.Equal(1, normalSession.AwaitingReceiptAckCount);
                    Assert.Equal(1, normalSession.OutstandingRequestCount);

                    normalTransport.ReleaseAcknowledgementRead();
                    await normalRunTask;
                }
                finally
                {
                    normalTransport.ReleaseAcknowledgementRead();
                    await normalRunTask;
                }

                Assert.Equal(0, normalSession.OutstandingRequestCount);
                Assert.Equal(0, normalSession.AwaitingReceiptAckCount);

                IReadOnlyList<ListenerFrameParseResult> normalFrames = ParseAllFrames(normalTransport.GetWrittenBytes());
                Assert.Single(normalFrames);
                Assert.Equal(ListenerOpcode.GetResponseFound, normalFrames[0].Frame!.Value.Header.Opcode);
                Assert.Equal<uint>(5, normalFrames[0].Frame.Value.Header.RequestId);
            }
        }

        [Fact]
        public async Task RunAsync_WhenAckUnknownRequestId_EmitsInvalidRequestIdError()
        {
            byte[] ack = ListenerProtocolEncoder.EncodeGetReceiptAck(41);
            ImmediateNotFoundHandler handler = new();
            TestTransport transport = new([ack], 4096);
            ListenerProtocolSession session = new(transport, handler);

            await session.RunAsync(CancellationToken.None);

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Single(frames);
            Assert.Equal(ListenerOpcode.GetResponseError, frames[0].Frame!.Value.Header.Opcode);
            Assert.Equal(ListenerProtocolErrorCode.InvalidRequestId, ReadErrorCode(frames[0].Frame.Value.Payload));
        }

        [Fact]
        public async Task RunAsync_WhenParserAccumulationAtConfiguredCap_AcceptsFragment()
        {
            byte[] fragment = Enumerable.Repeat((byte)'a', 32768).ToArray();
            TestTransport transport = new([fragment], 65536);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(parserAccumulationMaxBytes: 32768));

            await session.RunAsync(CancellationToken.None);

            Assert.Empty(handler.SeenRequestIds);
            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_WhenParserAccumulationExceedsConfiguredCap_ForcesSessionShutdownWithoutDispatch()
        {
            byte[] fragmentOne = Enumerable.Repeat((byte)'a', 20000).ToArray();
            byte[] fragmentTwo = Enumerable.Repeat((byte)'b', 20000).ToArray();
            TestTransport transport = new([fragmentOne, fragmentTwo], 65536);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(parserAccumulationMaxBytes: 32768));

            await session.RunAsync(CancellationToken.None);

            Assert.Empty(handler.SeenRequestIds);
            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_WithRetentionBackedHandler_WhenAwaitingAckTimeoutExpires_ReleasesLeaseWithoutCompletion()
        {
            const string messageId = "<listener-await-timeout@example.com>";
            const string payloadText = "await-timeout-payload";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string messageIdMd5 = RetainArticle(authority, messageId, payloadText);

            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(90, messageIdMd5);
            TestTransport transport = new([request], 4096);
            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(awaitingReceiptAckTimeoutSeconds: 1),
                onAwaitingReceiptAck: null,
                onTerminalized: handler.OnRequestTerminalized,
                onFoundTransferTerminal: handler.OnFoundTransferTerminal,
                onReceiptAcknowledged: handler.OnReceiptAcknowledged);

            await session.RunAsync(CancellationToken.None);

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
            Assert.Equal(0, snapshot.ListenerCompletionCount);
            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_WhenFoundByteBudgetExactlyMatchesPayload_AllowsReservationAndReleasesAfterTransfer()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(99, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] payload = Encoding.ASCII.GetBytes("budget-fit-payload");
            TestTransport transport = new([request], 4096);
            BlockingAllRequestsHandler handler = new();
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(maxQueuedFoundPayloadBytes: payload.Length));

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await handler.WaitForInvocationsAtLeastAsync(1);
                handler.ReleaseAll(ListenerSessionRequestDispatchResult.Found(payload));
                await runTask;
            }
            finally
            {
                handler.ReleaseAll(ListenerSessionRequestDispatchResult.Found(payload));
                await runTask;
            }

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Single(frames);
            Assert.Equal(ListenerOpcode.GetResponseFound, frames[0].Frame!.Value.Header.Opcode);
            Assert.Equal(0, session.ReservedFoundPayloadBytes);
        }

        [Fact]
        public async Task StreamListenerProtocolSessionTransport_WhenReadStalls_TimesOutAndDisposes()
        {
            BlockingProgressTimeoutStream stream = new();
            await using StreamListenerProtocolSessionTransport transport = new(stream, TimeSpan.FromMilliseconds(200));

            Task<int> readTask = transport.ReadAsync(new byte[8], CancellationToken.None).AsTask();
            await stream.WaitForReadEnteredAsync().ConfigureAwait(false);

            TimeoutException timeout = await Assert.ThrowsAsync<TimeoutException>(async () => await readTask.ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Contains("read exceeded no-progress timeout", timeout.Message, StringComparison.OrdinalIgnoreCase);

            await transport.DisposeAsync().ConfigureAwait(false);
            await stream.WaitForDisposeAsync().ConfigureAwait(false);
        }

        [Fact]
        public async Task StreamListenerProtocolSessionTransport_WhenWriteStalls_TimesOutAndDisposes()
        {
            BlockingProgressTimeoutStream stream = new();
            await using StreamListenerProtocolSessionTransport transport = new(stream, TimeSpan.FromMilliseconds(200));

            Task<int> writeTask = transport.WriteAsync(Encoding.ASCII.GetBytes("payload"), CancellationToken.None).AsTask();
            await stream.WaitForWriteEnteredAsync().ConfigureAwait(false);

            TimeoutException timeout = await Assert.ThrowsAsync<TimeoutException>(async () => await writeTask.ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Contains("write exceeded no-progress timeout", timeout.Message, StringComparison.OrdinalIgnoreCase);

            await transport.DisposeAsync().ConfigureAwait(false);
            await stream.WaitForDisposeAsync().ConfigureAwait(false);
        }

        [Fact]
        public async Task RunAsync_WhenWriterFaultsWhileReaderRemainsActive_ForcesSessionTermination()
        {
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(501, "30edc94157aa16fe644a45a1f1ffe160");
            WriterFaultWhileReaderActiveTransport transport = new(request);
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            Task runTask = session.RunAsync(CancellationToken.None);
            await transport.WaitForReadBlockedAsync().ConfigureAwait(false);
            await transport.WaitForWriteAttemptAsync().ConfigureAwait(false);

            InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await runTask.ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Equal("Listener protocol session terminated due to writer failure.", exception.Message);
            Assert.IsType<TimeoutException>(exception.InnerException);
            Assert.Equal(ListenerProtocolSessionState.Completed, session.State);
        }

        [Fact]
        public async Task RunAsync_WhenHostCancellationRequested_StopsWithoutFaulting()
        {
            IdleCancelableTransport transport = new();
            ImmediateNotFoundHandler handler = new();
            ListenerProtocolSession session = new(transport, handler);

            using CancellationTokenSource cancellation = new();
            Task runTask = session.RunAsync(cancellation.Token);
            await transport.WaitForReadStartedAsync().ConfigureAwait(false);

            cancellation.Cancel();
            await runTask.ConfigureAwait(false);

            Assert.Equal(ListenerProtocolSessionState.Completed, session.State);
        }

        [Fact]
        public void ExceedsParserAccumulationLimit_WhenBufferedAndReadWouldOverflowInt_ReturnsTrue()
        {
            Assert.True(ListenerProtocolSession.ExceedsParserAccumulationLimit(int.MaxValue, 1, int.MaxValue));
        }

        [Fact]
        public void ExceedsParserAccumulationLimit_WhenReadFitsConfiguredLimit_ReturnsFalse()
        {
            Assert.False(ListenerProtocolSession.ExceedsParserAccumulationLimit(32767, 1, 32768));
        }

        [Fact]
        public void ExceedsFoundReservationLimit_WhenCurrentPlusPayloadWouldOverflowLong_ReturnsTrue()
        {
            Assert.True(ListenerProtocolSession.ExceedsFoundReservationLimit(long.MaxValue, 1, int.MaxValue));
        }

        [Fact]
        public void ExceedsFoundReservationLimit_WhenCurrentPlusPayloadEqualsBudget_ReturnsFalse()
        {
            Assert.False(ListenerProtocolSession.ExceedsFoundReservationLimit(15, 5, 20));
        }

        [Fact]
        public async Task RunAsync_WhenFoundByteBudgetExceeded_EmitsErrorAndDoesNotRetainFoundReservation()
        {
            byte[] requestOne = ListenerProtocolEncoder.EncodeGetRequest(100, "30edc94157aa16fe644a45a1f1ffe160");
            byte[] requestTwo = ListenerProtocolEncoder.EncodeGetRequest(101, "30edc94157aa16fe644a45a1f1ffe161");
            byte[] coalesced = new byte[requestOne.Length + requestTwo.Length];
            Buffer.BlockCopy(requestOne, 0, coalesced, 0, requestOne.Length);
            Buffer.BlockCopy(requestTwo, 0, coalesced, requestOne.Length, requestTwo.Length);

            BlockingAllRequestsHandler handler = new();
            TestTransport transport = new([coalesced], 4096);
            ListenerProtocolSession session = new(
                transport,
                handler,
                CreateListenerOptions(maxQueuedFoundPayloadBytes: 10));

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await handler.WaitForInvocationsAtLeastAsync(2);
                handler.ReleaseAll(ListenerSessionRequestDispatchResult.Found(Encoding.ASCII.GetBytes("payload-exceeds-budget")));
                await runTask;
            }
            finally
            {
                handler.ReleaseAll(ListenerSessionRequestDispatchResult.Found(Encoding.ASCII.GetBytes("payload-exceeds-budget")));
                await runTask;
            }

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Equal(2, frames.Count);
            Assert.All(frames, static frame => Assert.Equal(ListenerOpcode.GetResponseError, frame.Frame!.Value.Header.Opcode));
            Assert.Equal(0, session.ReservedFoundPayloadBytes);
            Assert.Equal(0, session.OutstandingRequestCount);
        }

        [Fact]
        public async Task RunAsync_WithRetentionBackedHandler_WhenRetainedArticleExists_WritesFoundAndMarksListenerCompletedAfterAck()
        {
            const string messageId = "<listener-found@example.com>";
            const string payloadText = "found-from-retention";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string messageIdMd5 = RetainArticle(authority, messageId, payloadText);

            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(51, messageIdMd5);
            byte[] ack = ListenerProtocolEncoder.EncodeGetReceiptAck(51);
            GatedAckReadTransport transport = new(request, ack, 4096);
            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerProtocolSession session = new(
                transport,
                handler,
                onAwaitingReceiptAck: null,
                onTerminalized: handler.OnRequestTerminalized,
                onFoundTransferTerminal: handler.OnFoundTransferTerminal,
                onReceiptAcknowledged: handler.OnReceiptAcknowledged);

            Task runTask = session.RunAsync(CancellationToken.None);
            try
            {
                await transport.WaitForWritesAtLeastAsync(checked((int)(ListenerProtocol.HeaderLengthBytes + payloadText.Length)));

                ArticleRetentionSnapshot beforeAck = authority.GetSnapshot();
                Assert.Equal(1, beforeAck.ActiveReaderCount);
                Assert.Equal(0, beforeAck.ListenerCompletionCount);

                transport.ReleaseAcknowledgementRead();
                await runTask;
            }
            finally
            {
                transport.ReleaseAcknowledgementRead();
                await runTask;
            }

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Single(frames);
            Assert.Equal(ListenerOpcode.GetResponseFound, frames[0].Frame!.Value.Header.Opcode);
            Assert.Equal<uint>(51, frames[0].Frame.Value.Header.RequestId);
            Assert.Equal(payloadText, Encoding.ASCII.GetString(frames[0].Frame.Value.Payload.ToArray()));

            ArticleRetentionSnapshot afterAck = authority.GetSnapshot();
            Assert.Equal(0, afterAck.ActiveReaderCount);
            Assert.Equal(1, afterAck.ListenerCompletionCount);
        }

        [Fact]
        public async Task RunAsync_WithRetentionBackedHandler_WhenMd5Missing_WritesNotFoundAndDoesNotMarkListenerCompleted()
        {
            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(63, "30edc94157aa16fe644a45a1f1ffe160");
            TestTransport transport = new([request], 4096);
            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerProtocolSession session = new(
                transport,
                handler,
                onAwaitingReceiptAck: null,
                onTerminalized: handler.OnRequestTerminalized,
                onFoundTransferTerminal: handler.OnFoundTransferTerminal,
                onReceiptAcknowledged: handler.OnReceiptAcknowledged);

            await session.RunAsync(CancellationToken.None);

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Single(frames);
            Assert.Equal(ListenerOpcode.GetResponseNotFound, frames[0].Frame!.Value.Header.Opcode);
            Assert.Equal<uint>(63, frames[0].Frame.Value.Header.RequestId);

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
            Assert.Equal(0, snapshot.ListenerCompletionCount);
        }

        [Fact]
        public async Task RunAsync_WithRetentionBackedHandler_WhenFoundWithoutAck_DoesNotMarkListenerCompletedAndReleasesLease()
        {
            const string messageId = "<listener-no-ack@example.com>";
            const string payloadText = "payload-no-ack";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string messageIdMd5 = RetainArticle(authority, messageId, payloadText);

            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(77, messageIdMd5);
            TestTransport transport = new([request], 4096);
            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerProtocolSession session = new(
                transport,
                handler,
                onAwaitingReceiptAck: null,
                onTerminalized: handler.OnRequestTerminalized,
                onFoundTransferTerminal: handler.OnFoundTransferTerminal,
                onReceiptAcknowledged: handler.OnReceiptAcknowledged);

            await session.RunAsync(CancellationToken.None);

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
            Assert.Equal(0, snapshot.ListenerCompletionCount);
        }

        [Fact]
        public async Task RunAsync_WithRetentionBackedHandler_WhenFoundTransferFails_DoesNotMarkListenerCompletedAndReleasesLease()
        {
            const string messageId = "<listener-transfer-fail@example.com>";
            const string payloadText = "payload-transfer-fail";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string messageIdMd5 = RetainArticle(authority, messageId, payloadText);

            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(88, messageIdMd5);
            FailingFoundPayloadTransport transport = new(request);
            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerProtocolSession session = new(
                transport,
                handler,
                onAwaitingReceiptAck: null,
                onTerminalized: handler.OnRequestTerminalized,
                onFoundTransferTerminal: handler.OnFoundTransferTerminal,
                onReceiptAcknowledged: handler.OnReceiptAcknowledged);

            await session.RunAsync(CancellationToken.None);

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
            Assert.Equal(0, snapshot.ListenerCompletionCount);
        }

        [Fact]
        public async Task RunAsync_WithRetentionBackedHandler_WhenFoundTransferCancelled_DoesNotMarkListenerCompletedAndReleasesLease()
        {
            const string messageId = "<listener-transfer-cancel@example.com>";
            const string payloadText = "payload-transfer-cancel";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string messageIdMd5 = RetainArticle(authority, messageId, payloadText);

            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(89, messageIdMd5);
            CancellingFoundPayloadTransport transport = new(request);
            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerProtocolSession session = new(
                transport,
                handler,
                onAwaitingReceiptAck: null,
                onTerminalized: handler.OnRequestTerminalized,
                onFoundTransferTerminal: handler.OnFoundTransferTerminal,
                onReceiptAcknowledged: handler.OnReceiptAcknowledged);

            await session.RunAsync(CancellationToken.None);

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
            Assert.Equal(0, snapshot.ListenerCompletionCount);
        }

        [Fact]
        public async Task RunAsync_WithRetentionBackedHandler_WhenMultipleRequestsFound_ResolvesEachRequestIndependently()
        {
            const string messageIdOne = "<listener-multi-1@example.com>";
            const string messageIdTwo = "<listener-multi-2@example.com>";
            const string payloadOne = "payload-one";
            const string payloadTwo = "payload-two";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string md5One = RetainArticle(authority, messageIdOne, payloadOne);
            string md5Two = RetainArticle(authority, messageIdTwo, payloadTwo);

            byte[] req1 = ListenerProtocolEncoder.EncodeGetRequest(101, md5One);
            byte[] req2 = ListenerProtocolEncoder.EncodeGetRequest(102, md5Two);
            byte[] ack2 = ListenerProtocolEncoder.EncodeGetReceiptAck(102);
            byte[] ack1 = ListenerProtocolEncoder.EncodeGetReceiptAck(101);
            byte[] coalesced = new byte[req1.Length + req2.Length + ack2.Length + ack1.Length];
            Buffer.BlockCopy(req1, 0, coalesced, 0, req1.Length);
            Buffer.BlockCopy(req2, 0, coalesced, req1.Length, req2.Length);
            Buffer.BlockCopy(ack2, 0, coalesced, req1.Length + req2.Length, ack2.Length);
            Buffer.BlockCopy(ack1, 0, coalesced, req1.Length + req2.Length + ack2.Length, ack1.Length);

            TestTransport transport = new([coalesced], 4096);
            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerProtocolSession session = new(
                transport,
                handler,
                onAwaitingReceiptAck: null,
                onTerminalized: handler.OnRequestTerminalized,
                onFoundTransferTerminal: handler.OnFoundTransferTerminal,
                onReceiptAcknowledged: handler.OnReceiptAcknowledged);

            await session.RunAsync(CancellationToken.None);

            IReadOnlyList<ListenerFrameParseResult> frames = ParseAllFrames(transport.GetWrittenBytes());
            Assert.Equal(2, frames.Count);
            Assert.All(frames, static frame => Assert.Equal(ListenerOpcode.GetResponseFound, frame.Frame!.Value.Header.Opcode));

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
            Assert.Equal(2, snapshot.ListenerCompletionCount);
        }

        [Fact]
        public async Task HandleGetRequestAsync_WithRetentionBackedHandler_RequiresExactLowercaseCanonicalMd5()
        {
            const string messageId = "<listener-md5-case@example.com>";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string messageIdMd5 = RetainArticle(authority, messageId, "payload-case");
            ListenerProtocolRetentionRequestHandler handler = new(authority);

            ListenerSessionRequestDispatchResult uppercase = await handler.HandleGetRequestAsync(
                201,
                Encoding.ASCII.GetBytes(messageIdMd5.ToUpperInvariant()),
                CancellationToken.None);
            ListenerSessionRequestDispatchResult lowercase = await handler.HandleGetRequestAsync(
                202,
                Encoding.ASCII.GetBytes(messageIdMd5),
                CancellationToken.None);

            Assert.Equal(ListenerSessionRequestDispatchKind.NotFound, uppercase.Kind);
            Assert.Equal(ListenerSessionRequestDispatchKind.Found, lowercase.Kind);

            handler.OnRequestTerminalized(202);
            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
        }

        [Fact]
        public async Task HandleGetRequestAsync_WithRetentionBackedHandler_UsesLeasePayloadMemoryWithoutCopy()
        {
            const string messageId = "<listener-no-copy@example.com>";

            await using ArticleRetentionAuthority authority = CreateRetentionAuthority();
            string messageIdMd5 = RetainArticle(authority, messageId, "payload-no-copy");

            ArticleRetentionReadLeaseResult baselineLeaseResult = authority.TryAcquireReadLeaseByMessageIdMd5(messageIdMd5);
            Assert.True(baselineLeaseResult.IsAcquired);
            using IArticleRetentionReadLease baselineLease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(baselineLeaseResult.Lease);

            ListenerProtocolRetentionRequestHandler handler = new(authority);
            ListenerSessionRequestDispatchResult dispatch = await handler.HandleGetRequestAsync(
                301,
                Encoding.ASCII.GetBytes(messageIdMd5),
                CancellationToken.None);

            Assert.Equal(ListenerSessionRequestDispatchKind.Found, dispatch.Kind);
            Assert.True(MemoryMarshal.TryGetArray(baselineLease.Payload, out ArraySegment<byte> baselineSegment));
            Assert.True(MemoryMarshal.TryGetArray(dispatch.FoundPayload, out ArraySegment<byte> foundSegment));
            Assert.Same(baselineSegment.Array, foundSegment.Array);

            baselineLease.Dispose();
            handler.OnRequestTerminalized(301);

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.ActiveReaderCount);
        }

        private static ArticleRetentionAuthority CreateRetentionAuthority()
        {
            return new ArticleRetentionAuthority(CreateRuntimeOptions(capacityBytes: 128 * 1024));
        }

        private static ListenerRuntimeOptions CreateListenerOptions(
            int parserAccumulationMaxBytes = 262144,
            int tlsHandshakeTimeoutSeconds = 30,
            int ioProgressTimeoutSeconds = 60,
            int awaitingReceiptAckTimeoutSeconds = 30,
            int maxQueuedFoundPayloadBytes = 67108864,
            int maxActiveConnections = 1024)
        {
            return new ListenerRuntimeOptions(
                ParserAccumulationMaxBytes: parserAccumulationMaxBytes,
                TlsHandshakeTimeout: TimeSpan.FromSeconds(tlsHandshakeTimeoutSeconds),
                IoProgressTimeout: TimeSpan.FromSeconds(ioProgressTimeoutSeconds),
                AwaitingReceiptAckTimeout: TimeSpan.FromSeconds(awaitingReceiptAckTimeoutSeconds),
                MaxQueuedFoundPayloadBytes: maxQueuedFoundPayloadBytes,
                MaxActiveConnections: maxActiveConnections);
        }

        private static string RetainArticle(ArticleRetentionAuthority authority, string messageId, string payloadText)
        {
            DownloadedArticleBuffer payload = CreateBuffer(payloadText);
            ArticleRetentionAdmissionResult admission = authority.TryRetainSuccessArticle(messageId, payload);
            Assert.Equal(ArticleRetentionAdmissionStatus.Admitted, admission.Status);
            Assert.NotNull(admission.MessageIdMd5);
            return admission.MessageIdMd5!;
        }

        private static BackFillerRuntimeOptions CreateRuntimeOptions(long capacityBytes)
        {
            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "backfiller01.usenet.ninja",
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
                ValidatedLogDirectory: "C:\\logs",
                ValidatedCertificateDirectory: "C:\\certs",
                RabbitMqHosts: ["rabbit01.usenet.ninja"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: true,
                TransitServerHost: "transit01.usenet.ninja",
                TransitServerPort: 563,
                TransitServerUseSsl: true,
                BindPort: 119,
                ArticleRetention: new ArticleRetentionRuntimeOptions(
                    MaximumRetainedPayloadBytes: capacityBytes,
                    RetentionTtl: TimeSpan.FromSeconds(60),
                    SweepInterval: TimeSpan.FromSeconds(1)));
        }

        private static DownloadedArticleBuffer CreateBuffer(string payload)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(payload);
            byte[] rented = ArrayPool<byte>.Shared.Rent(bytes.Length);
            Array.Copy(bytes, rented, bytes.Length);
            return new DownloadedArticleBuffer(rented, bytes.Length);
        }

        private static ListenerProtocolErrorCode ReadErrorCode(ReadOnlySequence<byte> payload)
        {
            byte[] bytes = payload.ToArray();
            return (ListenerProtocolErrorCode)((bytes[0] << 8) | bytes[1]);
        }

        private static int GetTrackedRequestTaskCount(ListenerProtocolSession session)
        {
            FieldInfo field = typeof(ListenerProtocolSession).GetField("_requestTasks", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("ListenerProtocolSession request-task field was not found.");

            if (field.GetValue(session) is not ConcurrentDictionary<uint, Task> requestTasks)
            {
                throw new InvalidOperationException("ListenerProtocolSession request-task field did not expose the expected dictionary type.");
            }

            return requestTasks.Count;
        }

        private static TField? GetPrivateFieldValue<TField>(ListenerProtocolSession session, string fieldName)
            where TField : class
        {
            ArgumentNullException.ThrowIfNull(session);
            if (string.IsNullOrWhiteSpace(fieldName))
            {
                throw new ArgumentException("Field name must be provided.", nameof(fieldName));
            }

            FieldInfo field = typeof(ListenerProtocolSession).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException($"ListenerProtocolSession field '{fieldName}' was not found.");

            object? value = field.GetValue(session);
            if (value is null)
            {
                return null;
            }

            if (value is not TField typed)
            {
                throw new InvalidOperationException($"ListenerProtocolSession field '{fieldName}' did not expose expected type '{typeof(TField).Name}'.");
            }

            return typed;
        }

        private static IReadOnlyList<ListenerFrameParseResult> ParseAllFrames(byte[] outbound)
        {
            List<ListenerFrameParseResult> result = [];
            int offset = 0;
            while (offset < outbound.Length)
            {
                ReadOnlySequence<byte> input = new(outbound, offset, outbound.Length - offset);
                ListenerFrameParseResult parsed = ListenerProtocolParser.ParseOneFrame(in input);
                Assert.Equal(ListenerFrameParseStatus.Success, parsed.Status);
                result.Add(parsed);
                offset += checked((int)parsed.ConsumedBytes);
            }

            return result;
        }

        private static byte[] CreateRawFrame(byte version, ListenerOpcode opcode, uint requestId, byte[] payload)
        {
            byte[] frame = new byte[ListenerProtocol.HeaderLengthBytes + payload.Length];
            ListenerFrameHeader header = new(
                Version: version,
                Opcode: opcode,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: requestId,
                PayloadLength: checked((uint)payload.Length),
                Reserved: 0);

            header.WriteTo(frame);
            Buffer.BlockCopy(payload, 0, frame, ListenerProtocol.HeaderLengthBytes, payload.Length);
            return frame;
        }

        private static byte[] CreateRawHeader(byte version, ListenerOpcode opcode, uint requestId, uint payloadLength, uint reserved)
        {
            byte[] headerBytes = new byte[ListenerProtocol.HeaderLengthBytes];
            ListenerFrameHeader header = new(
                Version: version,
                Opcode: opcode,
                HeaderLength: ListenerProtocol.HeaderLengthBytes,
                RequestId: requestId,
                PayloadLength: payloadLength,
                Reserved: reserved);
            header.WriteTo(headerBytes);
            return headerBytes;
        }

        private sealed class ImmediateNotFoundHandler : IListenerProtocolRequestHandler
        {
            internal List<uint> SeenRequestIds { get; } = [];

            public ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
                uint requestId,
                ReadOnlyMemory<byte> messageIdMd5Payload,
                CancellationToken cancellationToken)
            {
                _ = messageIdMd5Payload;
                _ = cancellationToken;
                SeenRequestIds.Add(requestId);
                return ValueTask.FromResult(ListenerSessionRequestDispatchResult.NotFound());
            }
        }

        private sealed class BlockingFirstRequestHandler : IListenerProtocolRequestHandler
        {
            private readonly TaskCompletionSource<ListenerSessionRequestDispatchResult> _firstGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _firstRequestSeen = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _invocations;

            internal List<uint> SeenRequestIds { get; } = [];

            public async ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
                uint requestId,
                ReadOnlyMemory<byte> messageIdMd5Payload,
                CancellationToken cancellationToken)
            {
                _ = messageIdMd5Payload;
                SeenRequestIds.Add(requestId);
                int count = Interlocked.Increment(ref _invocations);
                if (count == 1)
                {
                    _firstRequestSeen.TrySetResult(true);
                    return await _firstGate.Task.WaitAsync(cancellationToken);
                }

                return ListenerSessionRequestDispatchResult.NotFound();
            }

            internal Task WaitForFirstRequestAsync()
            {
                return _firstRequestSeen.Task;
            }

            internal void ReleaseFirst(ListenerSessionRequestDispatchResult result)
            {
                _firstGate.TrySetResult(result);
            }
        }

        private sealed class BlockingAllRequestsHandler : IListenerProtocolRequestHandler
        {
            private readonly ConcurrentDictionary<uint, TaskCompletionSource<ListenerSessionRequestDispatchResult>> _gates = new();
            private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _invocationThresholds = new();
            private readonly object _gateSync = new();
            private int _invocations;
            private bool _releaseAllRequested;
            private ListenerSessionRequestDispatchResult _releaseResult;

            public async ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
                uint requestId,
                ReadOnlyMemory<byte> messageIdMd5Payload,
                CancellationToken cancellationToken)
            {
                _ = messageIdMd5Payload;
                int invocations = Interlocked.Increment(ref _invocations);
                SignalInvocationThresholds(invocations);

                TaskCompletionSource<ListenerSessionRequestDispatchResult> gate;
                bool releaseNow;
                ListenerSessionRequestDispatchResult releaseResult;
                lock (_gateSync)
                {
                    gate = _gates.GetOrAdd(
                        requestId,
                        static _ => new TaskCompletionSource<ListenerSessionRequestDispatchResult>(TaskCreationOptions.RunContinuationsAsynchronously));
                    releaseNow = _releaseAllRequested;
                    releaseResult = _releaseResult;
                }

                if (releaseNow)
                {
                    gate.TrySetResult(releaseResult);
                }

                return await gate.Task.WaitAsync(cancellationToken);
            }

            internal Task WaitForInvocationsAtLeastAsync(int expected)
            {
                if (Volatile.Read(ref _invocations) >= expected)
                {
                    return Task.CompletedTask;
                }

                TaskCompletionSource<bool> threshold = _invocationThresholds.GetOrAdd(
                    expected,
                    static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

                if (Volatile.Read(ref _invocations) >= expected)
                {
                    threshold.TrySetResult(true);
                }

                return threshold.Task;
            }

            internal void ReleaseAll(ListenerSessionRequestDispatchResult result)
            {
                List<TaskCompletionSource<ListenerSessionRequestDispatchResult>> gatesToRelease;
                lock (_gateSync)
                {
                    _releaseAllRequested = true;
                    _releaseResult = result;
                    gatesToRelease = [.. _gates.Values];
                }

                for (int i = 0; i < gatesToRelease.Count; i++)
                {
                    gatesToRelease[i].TrySetResult(result);
                }
            }

            private void SignalInvocationThresholds(int currentInvocations)
            {
                foreach (KeyValuePair<int, TaskCompletionSource<bool>> threshold in _invocationThresholds)
                {
                    if (threshold.Key <= currentInvocations)
                    {
                        threshold.Value.TrySetResult(true);
                    }
                }
            }
        }

        private sealed class ConcurrencyTrackingHandler : IListenerProtocolRequestHandler
        {
            private readonly ConcurrentDictionary<uint, TaskCompletionSource<ListenerSessionRequestDispatchResult>> _gates = new();
            private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _activeThresholds = new();
            private readonly object _gateSync = new();
            private int _active;
            private int _maxActive;
            private bool _releaseAllRequested;
            private ListenerSessionRequestDispatchResult _releaseResult;

            internal int MaxActive => Volatile.Read(ref _maxActive);

            public async ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
                uint requestId,
                ReadOnlyMemory<byte> messageIdMd5Payload,
                CancellationToken cancellationToken)
            {
                _ = messageIdMd5Payload;
                int activeNow = Interlocked.Increment(ref _active);
                UpdateMaximum(activeNow);
                SignalActiveThresholds(activeNow);

                TaskCompletionSource<ListenerSessionRequestDispatchResult> gate;
                bool releaseNow;
                ListenerSessionRequestDispatchResult releaseResult;
                lock (_gateSync)
                {
                    gate = _gates.GetOrAdd(
                        requestId,
                        static _ => new TaskCompletionSource<ListenerSessionRequestDispatchResult>(TaskCreationOptions.RunContinuationsAsynchronously));
                    releaseNow = _releaseAllRequested;
                    releaseResult = _releaseResult;
                }

                if (releaseNow)
                {
                    gate.TrySetResult(releaseResult);
                }

                try
                {
                    return await gate.Task.WaitAsync(cancellationToken);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref _active);
                }
            }

            internal Task WaitForActiveCountAtLeastAsync(int minimum)
            {
                if (Volatile.Read(ref _active) >= minimum)
                {
                    return Task.CompletedTask;
                }

                TaskCompletionSource<bool> threshold = _activeThresholds.GetOrAdd(
                    minimum,
                    static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

                if (Volatile.Read(ref _active) >= minimum)
                {
                    threshold.TrySetResult(true);
                }

                return threshold.Task;
            }

            internal void ReleaseAll(ListenerSessionRequestDispatchResult result)
            {
                List<TaskCompletionSource<ListenerSessionRequestDispatchResult>> gatesToRelease;
                lock (_gateSync)
                {
                    _releaseAllRequested = true;
                    _releaseResult = result;
                    gatesToRelease = [.. _gates.Values];
                }

                for (int i = 0; i < gatesToRelease.Count; i++)
                {
                    gatesToRelease[i].TrySetResult(result);
                }
            }

            private void SignalActiveThresholds(int currentActive)
            {
                foreach (KeyValuePair<int, TaskCompletionSource<bool>> threshold in _activeThresholds)
                {
                    if (threshold.Key <= currentActive)
                    {
                        threshold.Value.TrySetResult(true);
                    }
                }
            }

            private void UpdateMaximum(int candidate)
            {
                while (true)
                {
                    int current = Volatile.Read(ref _maxActive);
                    if (candidate <= current)
                    {
                        return;
                    }

                    if (Interlocked.CompareExchange(ref _maxActive, candidate, current) == current)
                    {
                        return;
                    }
                }
            }
        }

        private sealed class OrderedCompletionHandler : IListenerProtocolRequestHandler
        {
            private readonly ConcurrentDictionary<uint, TaskCompletionSource<ListenerSessionRequestDispatchResult>> _pending = new();
            private readonly ConcurrentDictionary<uint, TaskCompletionSource<bool>> _registrations = new();

            public async ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
                uint requestId,
                ReadOnlyMemory<byte> messageIdMd5Payload,
                CancellationToken cancellationToken)
            {
                _ = messageIdMd5Payload;
                TaskCompletionSource<ListenerSessionRequestDispatchResult> gate = _pending.GetOrAdd(
                    requestId,
                    static _ => new TaskCompletionSource<ListenerSessionRequestDispatchResult>(TaskCreationOptions.RunContinuationsAsynchronously));
                TaskCompletionSource<bool> registration = _registrations.GetOrAdd(
                    requestId,
                    static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
                registration.TrySetResult(true);
                return await gate.Task.WaitAsync(cancellationToken);
            }

            internal void Complete(uint requestId, ListenerSessionRequestDispatchResult result)
            {
                if (_pending.TryGetValue(requestId, out TaskCompletionSource<ListenerSessionRequestDispatchResult>? gate))
                {
                    gate.TrySetResult(result);
                }
            }

            internal Task WaitForRegistrationAsync(uint requestId)
            {
                TaskCompletionSource<bool> registration = _registrations.GetOrAdd(
                    requestId,
                    static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
                return registration.Task;
            }
        }

        private sealed class FoundHandler : IListenerProtocolRequestHandler
        {
            private static readonly byte[] Payload = Encoding.ASCII.GetBytes("found-payload");

            internal static int PayloadLength => Payload.Length;

            public ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
                uint requestId,
                ReadOnlyMemory<byte> messageIdMd5Payload,
                CancellationToken cancellationToken)
            {
                _ = requestId;
                _ = messageIdMd5Payload;
                _ = cancellationToken;
                return ValueTask.FromResult(ListenerSessionRequestDispatchResult.Found(Payload));
            }
        }

        private sealed class GatedAckReadTransport : IListenerProtocolSessionTransport
        {
            private readonly byte[] _request;
            private readonly byte[] _ack;
            private readonly TaskCompletionSource<bool> _ackRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _writeThresholds = new();
            private readonly object _writeGate = new();
            private readonly List<byte> _writes = [];
            private readonly int _maxWriteChunkLength;
            private int _readStep;
            private int _writtenBytes;
            private bool _disposed;

            internal GatedAckReadTransport(byte[] request, byte[] ack, int maxWriteChunkLength)
            {
                _request = request;
                _ack = ack;
                _maxWriteChunkLength = maxWriteChunkLength;
            }

            public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(GatedAckReadTransport));
                }

                int step = Volatile.Read(ref _readStep);
                if (step == 0)
                {
                    if (_request.Length > buffer.Length)
                    {
                        throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                    }

                    _request.CopyTo(buffer);
                    _ = Interlocked.Exchange(ref _readStep, 1);
                    return _request.Length;
                }

                if (step == 1)
                {
                    await _ackRelease.Task.WaitAsync(cancellationToken);
                    if (_ack.Length > buffer.Length)
                    {
                        throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                    }

                    _ack.CopyTo(buffer);
                    _ = Interlocked.Exchange(ref _readStep, 2);
                    return _ack.Length;
                }

                return 0;
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(GatedAckReadTransport));
                }

                int chunkLength = Math.Min(buffer.Length, _maxWriteChunkLength);
                ReadOnlySpan<byte> span = buffer.Span[..chunkLength];
                lock (_writeGate)
                {
                    for (int i = 0; i < span.Length; i++)
                    {
                        _writes.Add(span[i]);
                    }
                }

                int written = Interlocked.Add(ref _writtenBytes, chunkLength);
                SignalWriteThresholds(written);
                return ValueTask.FromResult(chunkLength);
            }

            public ValueTask DisposeAsync()
            {
                _disposed = true;
                return ValueTask.CompletedTask;
            }

            internal void ReleaseAcknowledgementRead()
            {
                _ackRelease.TrySetResult(true);
            }

            internal byte[] GetWrittenBytes()
            {
                lock (_writeGate)
                {
                    return _writes.ToArray();
                }
            }

            internal Task WaitForWritesAtLeastAsync(int bytes)
            {
                if (Volatile.Read(ref _writtenBytes) >= bytes)
                {
                    return Task.CompletedTask;
                }

                TaskCompletionSource<bool> threshold = _writeThresholds.GetOrAdd(
                    bytes,
                    static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

                if (Volatile.Read(ref _writtenBytes) >= bytes)
                {
                    threshold.TrySetResult(true);
                }

                return threshold.Task;
            }

            private void SignalWriteThresholds(int currentWritten)
            {
                foreach (KeyValuePair<int, TaskCompletionSource<bool>> threshold in _writeThresholds)
                {
                    if (threshold.Key <= currentWritten)
                    {
                        threshold.Value.TrySetResult(true);
                    }
                }
            }
        }

        private sealed class FailingFoundPayloadTransport : IListenerProtocolSessionTransport
        {
            private readonly byte[] _request;
            private int _readStep;
            private int _writeCalls;
            private bool _disposed;

            internal FailingFoundPayloadTransport(byte[] request)
            {
                _request = request;
            }

            public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(FailingFoundPayloadTransport));
                }

                if (Volatile.Read(ref _readStep) == 0)
                {
                    if (_request.Length > buffer.Length)
                    {
                        throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                    }

                    _request.CopyTo(buffer);
                    _ = Interlocked.Exchange(ref _readStep, 1);
                    return ValueTask.FromResult(_request.Length);
                }

                return ValueTask.FromResult(0);
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(FailingFoundPayloadTransport));
                }

                int call = Interlocked.Increment(ref _writeCalls);
                if (call == 1)
                {
                    return ValueTask.FromResult(buffer.Length);
                }

                throw new IOException("Simulated payload transfer failure.");
            }

            public ValueTask DisposeAsync()
            {
                _disposed = true;
                return ValueTask.CompletedTask;
            }
        }

        private sealed class CancellingFoundPayloadTransport : IListenerProtocolSessionTransport
        {
            private readonly byte[] _request;
            private int _readStep;
            private int _writeCalls;
            private bool _disposed;

            internal CancellingFoundPayloadTransport(byte[] request)
            {
                _request = request;
            }

            public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(CancellingFoundPayloadTransport));
                }

                if (Volatile.Read(ref _readStep) == 0)
                {
                    if (_request.Length > buffer.Length)
                    {
                        throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                    }

                    _request.CopyTo(buffer);
                    _ = Interlocked.Exchange(ref _readStep, 1);
                    return ValueTask.FromResult(_request.Length);
                }

                return ValueTask.FromResult(0);
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(CancellingFoundPayloadTransport));
                }

                int call = Interlocked.Increment(ref _writeCalls);
                if (call == 1)
                {
                    return ValueTask.FromResult(buffer.Length);
                }

                throw new OperationCanceledException(cancellationToken);
            }

            public ValueTask DisposeAsync()
            {
                _disposed = true;
                return ValueTask.CompletedTask;
            }
        }

        private sealed class LifecycleProbeTransport : IListenerProtocolSessionTransport
        {
            private readonly Queue<byte[]> _readFragments;
            private readonly object _writeGate = new();
            private readonly List<byte> _writes = [];
            private readonly int _maxWriteChunkLength;
            private readonly Exception? _disposeException;
            private readonly bool _throwOnSecondDispose;
            private int _disposeCalls;
            private int _readCalls;
            private int _writeCalls;
            private bool _disposed;

            internal LifecycleProbeTransport(IEnumerable<byte[]> readFragments, int maxWriteChunkLength, Exception? disposeException, bool throwOnSecondDispose)
            {
                _readFragments = new Queue<byte[]>(readFragments);
                _maxWriteChunkLength = maxWriteChunkLength;
                _disposeException = disposeException;
                _throwOnSecondDispose = throwOnSecondDispose;
            }

            internal int DisposeCallCount => Volatile.Read(ref _disposeCalls);

            internal int ReadCallCount => Volatile.Read(ref _readCalls);

            internal int WriteCallCount => Volatile.Read(ref _writeCalls);

            public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(LifecycleProbeTransport));
                }

                _ = Interlocked.Increment(ref _readCalls);
                if (_readFragments.Count == 0)
                {
                    return ValueTask.FromResult(0);
                }

                byte[] next = _readFragments.Dequeue();
                if (next.Length > buffer.Length)
                {
                    throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                }

                next.CopyTo(buffer);
                return ValueTask.FromResult(next.Length);
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(LifecycleProbeTransport));
                }

                _ = Interlocked.Increment(ref _writeCalls);
                int chunkLength = Math.Min(buffer.Length, _maxWriteChunkLength);
                ReadOnlySpan<byte> span = buffer.Span[..chunkLength];
                lock (_writeGate)
                {
                    for (int i = 0; i < span.Length; i++)
                    {
                        _writes.Add(span[i]);
                    }
                }

                return ValueTask.FromResult(chunkLength);
            }

            public ValueTask DisposeAsync()
            {
                int disposeCalls = Interlocked.Increment(ref _disposeCalls);
                if (disposeCalls > 1 && _throwOnSecondDispose)
                {
                    throw new InvalidOperationException("LifecycleProbeTransport disposed more than once.");
                }

                _disposed = true;
                if (_disposeException is not null)
                {
                    throw _disposeException;
                }

                return ValueTask.CompletedTask;
            }
        }

        private sealed class BlockingProgressTimeoutStream : Stream
        {
            private readonly TaskCompletionSource<bool> _disposeSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _readEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _writeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly object _sync = new();
            private bool _disposed;

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public Task WaitForDisposeAsync() => _disposeSignal.Task;

            public Task WaitForReadEnteredAsync() => _readEntered.Task;

            public Task WaitForWriteEnteredAsync() => _writeEntered.Task;

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                _readEntered.TrySetResult(true);
                return AwaitReadCancellationAsync(cancellationToken);
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                _writeEntered.TrySetResult(true);
                return AwaitWriteCancellationAsync(cancellationToken);
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
            }

            public override void Flush()
            {
            }

            public override Task FlushAsync(CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            protected override void Dispose(bool disposing)
            {
                lock (_sync)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    _disposed = true;
                }

                _disposeSignal.TrySetResult(true);
                base.Dispose(disposing);
            }

            public override ValueTask DisposeAsync()
            {
                lock (_sync)
                {
                    if (_disposed)
                    {
                        return ValueTask.CompletedTask;
                    }

                    _disposed = true;
                }

                _disposeSignal.TrySetResult(true);
                return ValueTask.CompletedTask;
            }

            private static async ValueTask<int> AwaitReadCancellationAsync(CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return 0;
            }

            private static async ValueTask AwaitWriteCancellationAsync(CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }
        }

        private sealed class WriterFaultWhileReaderActiveTransport : IListenerProtocolSessionTransport
        {
            private readonly byte[] _request;
            private readonly TaskCompletionSource<bool> _writeAttempted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _readerBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _readStep;
            private bool _disposed;

            internal WriterFaultWhileReaderActiveTransport(byte[] request)
            {
                _request = request;
            }

            public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(WriterFaultWhileReaderActiveTransport));
                }

                if (Interlocked.CompareExchange(ref _readStep, 1, 0) == 0)
                {
                    if (_request.Length > buffer.Length)
                    {
                        throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                    }

                    _request.CopyTo(buffer);
                    return _request.Length;
                }

                _readerBlocked.TrySetResult(true);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return 0;
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(WriterFaultWhileReaderActiveTransport));
                }

                _ = buffer;
                _writeAttempted.TrySetResult(true);
                throw new TimeoutException("Simulated writer timeout while reader remains active.");
            }

            public ValueTask DisposeAsync()
            {
                _disposed = true;
                return ValueTask.CompletedTask;
            }

            internal Task WaitForWriteAttemptAsync() => _writeAttempted.Task;

            internal Task WaitForReadBlockedAsync() => _readerBlocked.Task;
        }

        private sealed class IdleCancelableTransport : IListenerProtocolSessionTransport
        {
            private readonly TaskCompletionSource<bool> _readStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private bool _disposed;

            public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(IdleCancelableTransport));
                }

                _ = buffer;
                _readStarted.TrySetResult(true);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                return 0;
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(IdleCancelableTransport));
                }

                return ValueTask.FromResult(buffer.Length);
            }

            public ValueTask DisposeAsync()
            {
                _disposed = true;
                return ValueTask.CompletedTask;
            }

            internal Task WaitForReadStartedAsync() => _readStarted.Task;
        }

        private sealed class TestTransport : IListenerProtocolSessionTransport
        {
            private readonly Queue<byte[]> _readFragments;
            private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _writeThresholds = new();
            private readonly TaskCompletionSource<bool> _eofReached = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly object _writeGate = new();
            private readonly List<byte> _writes = [];
            private readonly int _maxWriteChunkLength;
            private int _writtenBytes;
            private int _readCalls;
            private bool _disposed;

            internal TestTransport(IEnumerable<byte[]> readFragments, int maxWriteChunkLength)
            {
                _readFragments = new Queue<byte[]>(readFragments);
                _maxWriteChunkLength = maxWriteChunkLength;
            }

            public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(TestTransport));
                }

                _ = Interlocked.Increment(ref _readCalls);
                if (_readFragments.Count == 0)
                {
                    _eofReached.TrySetResult(true);
                    return ValueTask.FromResult(0);
                }

                byte[] next = _readFragments.Dequeue();
                if (next.Length > buffer.Length)
                {
                    throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                }

                next.CopyTo(buffer);
                return ValueTask.FromResult(next.Length);
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(TestTransport));
                }

                int chunkLength = Math.Min(buffer.Length, _maxWriteChunkLength);
                ReadOnlySpan<byte> span = buffer.Span[..chunkLength];
                lock (_writeGate)
                {
                    for (int i = 0; i < span.Length; i++)
                    {
                        _writes.Add(span[i]);
                    }
                }

                int written = Interlocked.Add(ref _writtenBytes, chunkLength);
                SignalWriteThresholds(written);
                return ValueTask.FromResult(chunkLength);
            }

            public ValueTask DisposeAsync()
            {
                _disposed = true;
                return ValueTask.CompletedTask;
            }

            internal int ReadCallCount => Volatile.Read(ref _readCalls);

            internal int RemainingReadFragments
            {
                get
                {
                    return _readFragments.Count;
                }
            }

            internal byte[] GetWrittenBytes()
            {
                lock (_writeGate)
                {
                    return _writes.ToArray();
                }
            }

            internal Task WaitForEofAsync()
            {
                return _eofReached.Task;
            }

            internal Task WaitForWritesAtLeastAsync(int bytes)
            {
                if (Volatile.Read(ref _writtenBytes) >= bytes)
                {
                    return Task.CompletedTask;
                }

                TaskCompletionSource<bool> threshold = _writeThresholds.GetOrAdd(
                    bytes,
                    static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

                if (Volatile.Read(ref _writtenBytes) >= bytes)
                {
                    threshold.TrySetResult(true);
                }

                return threshold.Task;
            }

            private void SignalWriteThresholds(int currentWritten)
            {
                foreach (KeyValuePair<int, TaskCompletionSource<bool>> threshold in _writeThresholds)
                {
                    if (threshold.Key <= currentWritten)
                    {
                        threshold.Value.TrySetResult(true);
                    }
                }
            }
        }
    }
}
