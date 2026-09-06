// <copyright file="ListenerProtocolSessionTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Listener
// Deterministic tests for per-connection listener protocol session multiplexing, bounds, serialization, and lifecycle behavior.

using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
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
                    requestId => awaitingReceiptAck.TrySetResult(requestId));
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

        private static ListenerProtocolErrorCode ReadErrorCode(ReadOnlySequence<byte> payload)
        {
            byte[] bytes = payload.ToArray();
            return (ListenerProtocolErrorCode)((bytes[0] << 8) | bytes[1]);
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

        private sealed class TestTransport : IListenerProtocolSessionTransport
        {
            private readonly Queue<byte[]> _readFragments;
            private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _writeThresholds = new();
            private readonly TaskCompletionSource<bool> _eofReached = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly object _writeGate = new();
            private readonly List<byte> _writes = [];
            private readonly int _maxWriteChunkLength;
            private int _writtenBytes;
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
