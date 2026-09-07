// <copyright file="ListenerProtocolSession.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Per-connection protocol session runtime with bounded request tracking, bounded dispatch concurrency, and serialized outbound writes.

using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Channels;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Runs one established Listener protocol connection over an owned connected transport.
    /// </summary>
    /// <remarks>
    /// <para>This session is the per-connection concurrency and lifecycle boundary for Listener protocol processing.</para>
    /// <para>It owns incremental parse/read loop, bounded outstanding RequestId tracking, bounded handler concurrency,
    /// serialized outbound writes, and receipt-ack correlation state.</para>
    /// <para>Outstanding request bound is 64, concurrent request-processing bound is 8, and outbound response queue bound is 64.</para>
    /// <para>Responses may be emitted in completion order; arrival-order preservation is not required.</para>
    /// </remarks>
    internal sealed class ListenerProtocolSession : IAsyncDisposable
    {
        internal const int MaxOutstandingRequests = 64;
        internal const int MaxConcurrentProcessingRequests = 8;
        internal const int MaxOutboundResponses = 64;

        private readonly IListenerProtocolSessionTransport _transport;
        private readonly IListenerProtocolRequestHandler _requestHandler;
        private readonly Channel<OutboundResponseDescriptor> _outbound;
        private readonly SemaphoreSlim _processingLimiter;
        private readonly ConcurrentDictionary<uint, RequestContext> _requests = new();
        private readonly ConcurrentDictionary<uint, Task> _requestTasks = new();
        private readonly object _stateGate = new();
        private readonly Action<uint>? _onAwaitingReceiptAck;
        private readonly Action<uint>? _onTerminalized;
        private readonly Action<ListenerFoundTransferEvent>? _onFoundTransferTerminal;
        private readonly Action<ListenerReceiptAckEvent>? _onReceiptAcknowledged;
        private readonly ConcurrentDictionary<uint, CancellationTokenSource> _awaitingReceiptAckTimeouts = new();
        private readonly int _parserAccumulationMaxBytes;
        private readonly TimeSpan _awaitingReceiptAckTimeout;
        private readonly int _maxQueuedFoundPayloadBytes;
        private long _reservedFoundPayloadBytes;
        private CancellationTokenSource? _runCts;
        private Task? _writerTask;
        private Task? _runTask;
        private Task? _drainTask;
        private int _disposeStarted;
        private int _outboundCompletionSignaled;
        private ListenerProtocolSessionState _state = ListenerProtocolSessionState.Running;

        /// <summary>
        /// Initializes a new per-connection listener protocol session.
        /// </summary>
        /// <param name="transport">Connected transport owned by the session for the session lifetime.</param>
        /// <param name="requestHandler">Handler used to execute validated GetRequest operations.</param>
        /// <param name="listenerOptions">Per-session listener safety limits for parser accumulation, receipt-ack lifetime, Found payload pressure, and active connection bounds.</param>
        /// <param name="onAwaitingReceiptAck">Optional callback invoked when a Found response has completed transport write and request transitions to AwaitingReceiptAck.</param>
        /// <param name="onTerminalized">Optional callback invoked when a request is terminalized and removed from outstanding tracking.</param>
        /// <param name="onFoundTransferTerminal">Optional callback invoked when a Found response write reaches a terminal transfer status.</param>
        /// <param name="onReceiptAcknowledged">Optional callback invoked when a receipt acknowledgement frame is accepted for a tracked request.</param>
        internal ListenerProtocolSession(
            IListenerProtocolSessionTransport transport,
            IListenerProtocolRequestHandler requestHandler,
            ListenerRuntimeOptions? listenerOptions = null,
            Action<uint>? onAwaitingReceiptAck = null,
            Action<uint>? onTerminalized = null,
            Action<ListenerFoundTransferEvent>? onFoundTransferTerminal = null,
            Action<ListenerReceiptAckEvent>? onReceiptAcknowledged = null)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _requestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
            ListenerRuntimeOptions resolvedListenerOptions = listenerOptions
                ?? new ListenerRuntimeOptions(
                    ParserAccumulationMaxBytes: 262144,
                    AwaitingReceiptAckTimeout: TimeSpan.FromSeconds(30),
                    MaxQueuedFoundPayloadBytes: 67108864,
                    MaxActiveConnections: 1024);
            if (resolvedListenerOptions.ParserAccumulationMaxBytes < 32 * 1024)
            {
                throw new ArgumentOutOfRangeException(nameof(listenerOptions), "Parser accumulation max bytes must be at least 32768.");
            }

            if (resolvedListenerOptions.AwaitingReceiptAckTimeout <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(listenerOptions), "Awaiting receipt-ack timeout must be greater than zero.");
            }

            if (resolvedListenerOptions.MaxQueuedFoundPayloadBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(listenerOptions), "Maximum queued Found payload bytes must be greater than zero.");
            }

            _parserAccumulationMaxBytes = resolvedListenerOptions.ParserAccumulationMaxBytes;
            _awaitingReceiptAckTimeout = resolvedListenerOptions.AwaitingReceiptAckTimeout;
            _maxQueuedFoundPayloadBytes = resolvedListenerOptions.MaxQueuedFoundPayloadBytes;
            _onAwaitingReceiptAck = onAwaitingReceiptAck;
            _onTerminalized = onTerminalized;
            _onFoundTransferTerminal = onFoundTransferTerminal;
            _onReceiptAcknowledged = onReceiptAcknowledged;
            _processingLimiter = new SemaphoreSlim(MaxConcurrentProcessingRequests, MaxConcurrentProcessingRequests);
            _outbound = Channel.CreateBounded<OutboundResponseDescriptor>(new BoundedChannelOptions(MaxOutboundResponses)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });
        }

        /// <summary>
        /// Gets the current observable session lifecycle state.
        /// </summary>
        internal ListenerProtocolSessionState State
        {
            get
            {
                lock (_stateGate)
                {
                    return _state;
                }
            }
        }

        /// <summary>
        /// Gets the currently tracked outstanding request count.
        /// </summary>
        internal int OutstandingRequestCount => _requests.Count;

        /// <summary>
        /// Gets the number of requests awaiting receipt acknowledgement after complete Found transfer.
        /// </summary>
        internal int AwaitingReceiptAckCount => _requests.Values.Count(static value => value.State == ListenerRequestState.AwaitingReceiptAck);

        /// <summary>
        /// Gets currently reserved queued/in-flight Found payload bytes for this connection session.
        /// </summary>
        internal long ReservedFoundPayloadBytes => Volatile.Read(ref _reservedFoundPayloadBytes);

        /// <summary>
        /// Runs the session until remote close, cancellation, protocol fatal violation, or shutdown.
        /// </summary>
        /// <param name="cancellationToken">Host cancellation token for session termination.</param>
        internal async Task RunAsync(CancellationToken cancellationToken)
        {
            Task runTask;
            CancellationToken sessionToken;

            lock (_stateGate)
            {
                if (_runTask is not null
                    || Volatile.Read(ref _disposeStarted) != 0
                    || _drainTask is not null
                    || _state is ListenerProtocolSessionState.ForcedShutdown or ListenerProtocolSessionState.Completed)
                {
                    throw new InvalidOperationException("Session is already running.");
                }

                CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _runCts = linked;
                sessionToken = linked.Token;
                _writerTask = RunWriterAsync(sessionToken);
                _runTask = RunReadLoopAsync(sessionToken);
                runTask = _runTask;
            }

            Exception? readFault = null;
            try
            {
                await runTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (sessionToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                readFault = ex;
            }

            if (readFault is not null)
            {
                BeginForcedShutdown();
            }
            else if (State != ListenerProtocolSessionState.ForcedShutdown)
            {
                BeginGracefulShutdown();
            }

            await EnsureDrainAndStopAsync().ConfigureAwait(false);

            if (readFault is not null)
            {
                throw new InvalidOperationException("Listener protocol session terminated due to reader failure.", readFault);
            }
        }

        /// <summary>
        /// Starts graceful shutdown by refusing new request admissions while allowing admitted work to finish.
        /// </summary>
        internal void BeginGracefulShutdown()
        {
            lock (_stateGate)
            {
                if (_state is ListenerProtocolSessionState.Completed or ListenerProtocolSessionState.ForcedShutdown)
                {
                    return;
                }

                _state = ListenerProtocolSessionState.GracefulShutdown;
            }
        }

        /// <summary>
        /// Starts forced shutdown by canceling active work, queued writes, and parser input processing.
        /// </summary>
        internal void BeginForcedShutdown()
        {
            lock (_stateGate)
            {
                if (_state == ListenerProtocolSessionState.Completed)
                {
                    return;
                }

                _state = ListenerProtocolSessionState.ForcedShutdown;
                _runCts?.Cancel();
            }

            CompleteOutboundWriter();
        }

        private void CompleteOutboundWriter()
        {
            if (Interlocked.Exchange(ref _outboundCompletionSignaled, 1) == 0)
            {
                _ = _outbound.Writer.TryComplete();
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            {
                return;
            }

            BeginForcedShutdown();
            await EnsureDrainAndStopAsync().ConfigureAwait(false);
        }

        private Task EnsureDrainAndStopAsync()
        {
            lock (_stateGate)
            {
                _drainTask ??= DrainAndStopAsync(CancellationToken.None);

                return _drainTask;
            }
        }

        private async Task DrainAndStopAsync(CancellationToken cancellationToken)
        {
            Task? writerTask = _writerTask;
            List<Task> requestTasks = [.. _requestTasks.Values];
            ListenerProtocolSessionState state = State;

            try
            {
                if (state == ListenerProtocolSessionState.ForcedShutdown)
                {
                    List<Task> tasksToAwait = [];
                    if (writerTask is not null)
                    {
                        tasksToAwait.Add(writerTask);
                    }

                    tasksToAwait.AddRange(requestTasks);
                    await Task.WhenAll(tasksToAwait).WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await Task.WhenAll(requestTasks).WaitAsync(cancellationToken).ConfigureAwait(false);
                    CompleteOutboundWriter();

                    if (writerTask is not null)
                    {
                        await writerTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                BeginForcedShutdown();
            }

            CancellationTokenSource? runCtsToDispose;
            lock (_stateGate)
            {
                runCtsToDispose = _runCts;
                _runCts = null;
            }

            runCtsToDispose?.Dispose();

            CancelAwaitingReceiptAckTimeouts();
            TerminalizeOutstandingRequests();
            _requestTasks.Clear();
            _processingLimiter.Dispose();
            await _transport.DisposeAsync().ConfigureAwait(false);

            lock (_stateGate)
            {
                _state = ListenerProtocolSessionState.Completed;
            }
        }

        private async Task RunReadLoopAsync(CancellationToken cancellationToken)
        {
            byte[] readBuffer = ArrayPool<byte>.Shared.Rent(32 * 1024);
            byte[] parseBuffer = ArrayPool<byte>.Shared.Rent(32 * 1024);
            int buffered = 0;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    int bytesRead = await _transport.ReadAsync(readBuffer.AsMemory(0, readBuffer.Length), cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        break;
                    }

                    if (ExceedsParserAccumulationLimit(buffered, bytesRead, _parserAccumulationMaxBytes))
                    {
                        BeginForcedShutdown();
                        return;
                    }

                    int requiredBuffered = buffered + bytesRead;

                    if (requiredBuffered > parseBuffer.Length)
                    {
                        byte[] bigger = ArrayPool<byte>.Shared.Rent(Math.Min(_parserAccumulationMaxBytes, Math.Max(parseBuffer.Length * 2, requiredBuffered)));
                        parseBuffer.AsSpan(0, buffered).CopyTo(bigger);
                        ArrayPool<byte>.Shared.Return(parseBuffer);
                        parseBuffer = bigger;
                    }

                    readBuffer.AsSpan(0, bytesRead).CopyTo(parseBuffer.AsSpan(buffered));
                    buffered += bytesRead;

                    int consumed = 0;
                    while (consumed < buffered)
                    {
                        int candidateLength = buffered - consumed;
                        if (TryGetDeclaredFrameLength(parseBuffer, consumed, candidateLength, out long declaredFrameLength)
                            && DeclaredFrameExceedsAccumulationLimit(declaredFrameLength, _parserAccumulationMaxBytes))
                        {
                            BeginForcedShutdown();
                            return;
                        }

                        ReadOnlySequence<byte> candidate = new(parseBuffer, consumed, candidateLength);
                        ListenerFrameParseResult parseResult = ListenerProtocolParser.ParseOneFrame(in candidate);
                        if (parseResult.Status == ListenerFrameParseStatus.Incomplete)
                        {
                            break;
                        }

                        if (parseResult.Status == ListenerFrameParseStatus.Invalid)
                        {
                            bool fatal = await HandleInvalidParseAsync(parseResult, parseBuffer, consumed, cancellationToken).ConfigureAwait(false);
                            consumed += checked((int)parseResult.ConsumedBytes);
                            if (fatal)
                            {
                                return;
                            }

                            continue;
                        }

                        ListenerParsedFrame frame = parseResult.Frame!.Value;
                        consumed += checked((int)parseResult.ConsumedBytes);
                        await HandleParsedFrameAsync(frame, cancellationToken).ConfigureAwait(false);
                    }

                    if (consumed > 0)
                    {
                        parseBuffer.AsSpan(consumed, buffered - consumed).CopyTo(parseBuffer);
                        buffered -= consumed;
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(readBuffer);
                ArrayPool<byte>.Shared.Return(parseBuffer);
            }
        }

        private async Task<bool> HandleInvalidParseAsync(ListenerFrameParseResult parseResult, byte[] parseBuffer, int frameOffset, CancellationToken cancellationToken)
        {
            ListenerProtocolErrorCode errorCode = parseResult.Error switch
            {
                ListenerFrameParseError.UnsupportedVersion => ListenerProtocolErrorCode.UnsupportedVersion,
                ListenerFrameParseError.UnsupportedOpcode => ListenerProtocolErrorCode.UnsupportedOpcode,
                ListenerFrameParseError.InvalidHeaderLength => ListenerProtocolErrorCode.InvalidHeaderLength,
                ListenerFrameParseError.InvalidReserved => ListenerProtocolErrorCode.InvalidFrameLength,
                ListenerFrameParseError.InvalidFrameLength => ListenerProtocolErrorCode.InvalidFrameLength,
                ListenerFrameParseError.InvalidRequestId => ListenerProtocolErrorCode.InvalidRequestId,
                ListenerFrameParseError.InvalidMessageIdMd5 => ListenerProtocolErrorCode.InvalidMessageIdMd5,
                ListenerFrameParseError.None => ListenerProtocolErrorCode.InvalidFrameLength,
                _ => ListenerProtocolErrorCode.InvalidFrameLength,
            };

            bool fatal = parseResult.Error is ListenerFrameParseError.UnsupportedVersion
                or ListenerFrameParseError.InvalidHeaderLength
                or ListenerFrameParseError.InvalidFrameLength;

            if (!fatal)
            {
                uint requestId = TryReadRequestId(parseBuffer.AsSpan(frameOffset));
                await QueueErrorResponseAsync(requestId, errorCode, shouldTerminalizeRequest: false, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                BeginForcedShutdown();
            }

            return fatal;
        }

        private static uint TryReadRequestId(ReadOnlySpan<byte> candidate)
        {
            return candidate.Length >= ListenerProtocol.HeaderLengthBytes
                ? ListenerFrameHeader.ReadFrom(candidate[..ListenerProtocol.HeaderLengthBytes]).RequestId
                : 0;
        }

        private async Task HandleParsedFrameAsync(ListenerParsedFrame frame, CancellationToken cancellationToken)
        {
            if (frame.Header.Opcode == ListenerOpcode.GetRequest)
            {
                await HandleGetRequestAsync(frame, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (frame.Header.Opcode == ListenerOpcode.GetReceiptAck)
            {
                await HandleGetReceiptAckAsync(frame, cancellationToken).ConfigureAwait(false);
                return;
            }

            await QueueErrorResponseAsync(frame.Header.RequestId, ListenerProtocolErrorCode.UnsupportedOpcode, shouldTerminalizeRequest: false, cancellationToken).ConfigureAwait(false);
        }

        private async Task HandleGetRequestAsync(ListenerParsedFrame frame, CancellationToken cancellationToken)
        {
            uint requestId = frame.Header.RequestId;

            if (State != ListenerProtocolSessionState.Running)
            {
                await QueueErrorResponseAsync(requestId, ListenerProtocolErrorCode.ServerShuttingDown, shouldTerminalizeRequest: false, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (_requests.Count >= MaxOutstandingRequests)
            {
                await QueueErrorResponseAsync(requestId, ListenerProtocolErrorCode.RequestTableOverflow, shouldTerminalizeRequest: false, cancellationToken).ConfigureAwait(false);
                return;
            }

            byte[] md5Payload = frame.Payload.ToArray();
            RequestContext context = new(requestId, md5Payload);
            if (!_requests.TryAdd(requestId, context))
            {
                await QueueErrorResponseAsync(requestId, ListenerProtocolErrorCode.DuplicateRequestId, shouldTerminalizeRequest: false, cancellationToken).ConfigureAwait(false);
                return;
            }

            Task processingTask = ProcessRequestAsync(context, cancellationToken);
            _requestTasks[requestId] = processingTask;
            if (processingTask.IsCompleted)
            {
                _ = _requestTasks.TryRemove(requestId, out _);
            }
        }

        private async Task ProcessRequestAsync(RequestContext context, CancellationToken callerToken)
        {
            CancellationToken sessionToken = _runCts?.Token ?? callerToken;
            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(callerToken, sessionToken);
            CancellationToken token = linked.Token;

            try
            {
                await _processingLimiter.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Terminalize(context.RequestId);
                return;
            }

            int reservedFoundPayloadBytes = 0;
            bool descriptorQueued = false;
            try
            {
                context.MarkProcessing();
                ListenerSessionRequestDispatchResult dispatch = await _requestHandler
                    .HandleGetRequestAsync(context.RequestId, context.MessageIdMd5Payload, token)
                    .ConfigureAwait(false);

                OutboundResponseDescriptor descriptor;
                if (dispatch.Kind == ListenerSessionRequestDispatchKind.Found)
                {
                    if (!TryReserveFoundPayloadBytes(dispatch.FoundPayload.Length))
                    {
                        descriptor = OutboundResponseDescriptor.FromFrame(
                            context.RequestId,
                            ListenerProtocolEncoder.EncodeGetResponseError(context.RequestId, ListenerProtocolErrorCode.InternalError),
                            ResponseOutcome.Error,
                            shouldTerminalizeRequest: true,
                            reservedFoundPayloadBytes: 0);
                    }
                    else
                    {
                        reservedFoundPayloadBytes = dispatch.FoundPayload.Length;
                        descriptor = OutboundResponseDescriptor.FromFound(context.RequestId, dispatch.FoundPayload);
                    }
                }
                else
                {
                    descriptor = dispatch.Kind switch
                    {
                        ListenerSessionRequestDispatchKind.Found => OutboundResponseDescriptor.FromFrame(context.RequestId, ListenerProtocolEncoder.EncodeGetResponseError(context.RequestId, ListenerProtocolErrorCode.InternalError), ResponseOutcome.Error, shouldTerminalizeRequest: true, reservedFoundPayloadBytes: 0),
                        ListenerSessionRequestDispatchKind.NotFound => OutboundResponseDescriptor.FromFrame(context.RequestId, ListenerProtocolEncoder.EncodeGetResponseNotFound(context.RequestId), ResponseOutcome.NotFound, shouldTerminalizeRequest: true, reservedFoundPayloadBytes: 0),
                        ListenerSessionRequestDispatchKind.Error => OutboundResponseDescriptor.FromFrame(context.RequestId, ListenerProtocolEncoder.EncodeGetResponseError(context.RequestId, dispatch.ErrorCode), ResponseOutcome.Error, shouldTerminalizeRequest: true, reservedFoundPayloadBytes: 0),
                        _ => OutboundResponseDescriptor.FromFrame(context.RequestId, ListenerProtocolEncoder.EncodeGetResponseError(context.RequestId, ListenerProtocolErrorCode.InternalError), ResponseOutcome.Error, shouldTerminalizeRequest: true, reservedFoundPayloadBytes: 0),
                    };
                }

                await _outbound.Writer.WriteAsync(descriptor, token).ConfigureAwait(false);
                descriptorQueued = true;
            }
            catch (OperationCanceledException)
            {
                Terminalize(context.RequestId);
            }
            catch
            {
                await QueueErrorResponseAsync(context.RequestId, ListenerProtocolErrorCode.InternalError, shouldTerminalizeRequest: true, CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                if (!descriptorQueued && reservedFoundPayloadBytes > 0)
                {
                    ReleaseFoundPayloadBytes(reservedFoundPayloadBytes);
                }

                _ = _requestTasks.TryRemove(context.RequestId, out _);
                _ = _processingLimiter.Release();
            }
        }

        private async Task HandleGetReceiptAckAsync(ListenerParsedFrame frame, CancellationToken cancellationToken)
        {
            uint requestId = frame.Header.RequestId;
            if (!_requests.TryGetValue(requestId, out RequestContext? context))
            {
                await QueueErrorResponseAsync(requestId, ListenerProtocolErrorCode.InvalidRequestId, shouldTerminalizeRequest: false, cancellationToken).ConfigureAwait(false);
                return;
            }

            ListenerReceiptAckTransition transition = context.RegisterReceiptAcknowledgement();
            if (transition == ListenerReceiptAckTransition.AlreadyAcknowledged)
            {
                return;
            }

            if (transition is ListenerReceiptAckTransition.PendingFoundWrite or ListenerReceiptAckTransition.Terminalize)
            {
                _onReceiptAcknowledged?.Invoke(new ListenerReceiptAckEvent(requestId));
            }

            if (transition == ListenerReceiptAckTransition.PendingFoundWrite)
            {
                return;
            }

            if (transition == ListenerReceiptAckTransition.Terminalize)
            {
                CancelAwaitingReceiptAckTimeout(requestId);
                Terminalize(requestId);
                return;
            }

            await QueueErrorResponseAsync(requestId, ListenerProtocolErrorCode.InvalidRequestId, shouldTerminalizeRequest: false, cancellationToken).ConfigureAwait(false);
        }

        private async Task QueueErrorResponseAsync(
            uint requestId,
            ListenerProtocolErrorCode errorCode,
            bool shouldTerminalizeRequest,
            CancellationToken cancellationToken)
        {
            uint normalizedRequestId = requestId == 0 ? 1u : requestId;
            byte[] frame = ListenerProtocolEncoder.EncodeGetResponseError(normalizedRequestId, errorCode);
            OutboundResponseDescriptor descriptor = OutboundResponseDescriptor.FromFrame(normalizedRequestId, frame, ResponseOutcome.Error, shouldTerminalizeRequest, reservedFoundPayloadBytes: 0);
            await _outbound.Writer.WriteAsync(descriptor, cancellationToken).ConfigureAwait(false);
        }

        private async Task RunWriterAsync(CancellationToken cancellationToken)
        {
            await foreach (OutboundResponseDescriptor descriptor in _outbound.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                ListenerTransferCompletionStatus status = await WriteDescriptorAsync(descriptor, cancellationToken).ConfigureAwait(false);
                ApplyCompletion(descriptor, status);
            }
        }

        private async Task<ListenerTransferCompletionStatus> WriteDescriptorAsync(OutboundResponseDescriptor descriptor, CancellationToken cancellationToken)
        {
            try
            {
                if (descriptor.Kind == OutboundDescriptorKind.Frame)
                {
                    await WriteFullyAsync(descriptor.Frame, cancellationToken).ConfigureAwait(false);
                    return ListenerTransferCompletionStatus.Completed;
                }

                await WriteFullyAsync(descriptor.FoundHeader, cancellationToken).ConfigureAwait(false);
                await WriteFullyAsync(descriptor.FoundPayload, cancellationToken).ConfigureAwait(false);
                return ListenerTransferCompletionStatus.Completed;
            }
            catch (OperationCanceledException)
            {
                return ListenerTransferCompletionStatus.Canceled;
            }
            catch (IOException)
            {
                return ListenerTransferCompletionStatus.Failed;
            }
            catch (ObjectDisposedException)
            {
                return ListenerTransferCompletionStatus.Incomplete;
            }
        }

        private async Task WriteFullyAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        {
            int written = 0;
            while (written < payload.Length)
            {
                int accepted = await _transport.WriteAsync(payload[written..], cancellationToken).ConfigureAwait(false);
                if (accepted <= 0)
                {
                    throw new IOException("Transport returned zero accepted bytes.");
                }

                written += accepted;
            }
        }

        private void ApplyCompletion(OutboundResponseDescriptor descriptor, ListenerTransferCompletionStatus status)
        {
            if (!_requests.TryGetValue(descriptor.RequestId, out RequestContext? context))
            {
                return;
            }

            if (descriptor.ReservedFoundPayloadBytes > 0)
            {
                ReleaseFoundPayloadBytes(descriptor.ReservedFoundPayloadBytes);
            }

            if (descriptor.Outcome == ResponseOutcome.Found)
            {
                _onFoundTransferTerminal?.Invoke(new ListenerFoundTransferEvent(descriptor.RequestId, status));
            }

            if (status != ListenerTransferCompletionStatus.Completed)
            {
                Terminalize(descriptor.RequestId);
                return;
            }

            if (descriptor.Outcome == ResponseOutcome.Found)
            {
                if (context.MarkFoundWriteCompletedAndTryFinalize())
                {
                    Terminalize(descriptor.RequestId);
                    return;
                }

                if (context.State == ListenerRequestState.AwaitingReceiptAck)
                {
                    StartAwaitingReceiptAckTimeout(descriptor.RequestId);
                    _onAwaitingReceiptAck?.Invoke(descriptor.RequestId);
                }

                return;
            }

            if (descriptor.ShouldTerminalizeRequest)
            {
                Terminalize(descriptor.RequestId);
            }
        }

        private void Terminalize(uint requestId)
        {
            CancelAwaitingReceiptAckTimeout(requestId);
            if (_requests.TryRemove(requestId, out RequestContext? removed))
            {
                removed.MarkTerminal();
                _onTerminalized?.Invoke(requestId);
            }
        }

        private void TerminalizeOutstandingRequests()
        {
            List<uint> requestIds = [.. _requests.Keys];
            for (int i = 0; i < requestIds.Count; i++)
            {
                Terminalize(requestIds[i]);
            }
        }

        private bool TryReserveFoundPayloadBytes(int payloadBytes)
        {
            while (true)
            {
                long currentReserved = Volatile.Read(ref _reservedFoundPayloadBytes);
                if (ExceedsFoundReservationLimit(currentReserved, payloadBytes, _maxQueuedFoundPayloadBytes))
                {
                    return false;
                }

                long nextReserved = currentReserved + payloadBytes;

                if (Interlocked.CompareExchange(ref _reservedFoundPayloadBytes, nextReserved, currentReserved) == currentReserved)
                {
                    return true;
                }
            }
        }

        private void ReleaseFoundPayloadBytes(int payloadBytes)
        {
            _ = Interlocked.Add(ref _reservedFoundPayloadBytes, -payloadBytes);
        }

        internal static bool ExceedsParserAccumulationLimit(int buffered, int bytesRead, int parserAccumulationMaxBytes)
        {
            return buffered > parserAccumulationMaxBytes - bytesRead;
        }

        internal static bool DeclaredFrameExceedsAccumulationLimit(long declaredFrameLength, int parserAccumulationMaxBytes)
        {
            return declaredFrameLength > parserAccumulationMaxBytes;
        }

        internal static bool TryGetDeclaredFrameLength(byte[] parseBuffer, int frameOffset, int candidateLength, out long declaredFrameLength)
        {
            declaredFrameLength = 0;

            if (candidateLength < ListenerProtocol.HeaderLengthBytes)
            {
                return false;
            }

            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(parseBuffer.AsSpan(frameOffset, ListenerProtocol.HeaderLengthBytes));
            declaredFrameLength = checked((long)ListenerProtocol.HeaderLengthBytes + header.PayloadLength);
            return true;
        }

        internal static bool ExceedsFoundReservationLimit(long currentReserved, int payloadBytes, int maxQueuedFoundPayloadBytes)
        {
            return currentReserved > (long)maxQueuedFoundPayloadBytes - payloadBytes;
        }

        private void StartAwaitingReceiptAckTimeout(uint requestId)
        {
            CancellationTokenSource timeoutCts = new();
            if (!_awaitingReceiptAckTimeouts.TryAdd(requestId, timeoutCts))
            {
                timeoutCts.Dispose();
                return;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(_awaitingReceiptAckTimeout, timeoutCts.Token).ConfigureAwait(false);
                    Terminalize(requestId);
                }
                catch (OperationCanceledException)
                {
                }
            });
        }

        private void CancelAwaitingReceiptAckTimeout(uint requestId)
        {
            if (_awaitingReceiptAckTimeouts.TryRemove(requestId, out CancellationTokenSource? timeoutCts))
            {
                timeoutCts.Cancel();
                timeoutCts.Dispose();
            }
        }

        private void CancelAwaitingReceiptAckTimeouts()
        {
            List<CancellationTokenSource> timeouts = [.. _awaitingReceiptAckTimeouts.Values];
            _awaitingReceiptAckTimeouts.Clear();
            for (int i = 0; i < timeouts.Count; i++)
            {
                timeouts[i].Cancel();
                timeouts[i].Dispose();
            }
        }

        private sealed class RequestContext
        {
            private readonly object _sync = new();
            private int _state;
            private bool _receiptAcknowledged;

            internal RequestContext(uint requestId, ReadOnlyMemory<byte> messageIdMd5Payload)
            {
                RequestId = requestId;
                MessageIdMd5Payload = messageIdMd5Payload;
                _state = (int)ListenerRequestState.Queued;
            }

            internal uint RequestId { get; }

            internal ReadOnlyMemory<byte> MessageIdMd5Payload { get; }

            internal ListenerRequestState State => (ListenerRequestState)Volatile.Read(ref _state);

            internal void MarkProcessing()
            {
                _ = Interlocked.CompareExchange(ref _state, (int)ListenerRequestState.Processing, (int)ListenerRequestState.Queued);
            }

            internal ListenerReceiptAckTransition RegisterReceiptAcknowledgement()
            {
                lock (_sync)
                {
                    if (_state == (int)ListenerRequestState.Terminal)
                    {
                        return ListenerReceiptAckTransition.Invalid;
                    }

                    if (_receiptAcknowledged)
                    {
                        return ListenerReceiptAckTransition.AlreadyAcknowledged;
                    }

                    _receiptAcknowledged = true;

                    if (_state == (int)ListenerRequestState.AwaitingReceiptAck)
                    {
                        _state = (int)ListenerRequestState.Terminal;
                        return ListenerReceiptAckTransition.Terminalize;
                    }

                    return _state is (int)ListenerRequestState.Queued or (int)ListenerRequestState.Processing
                        ? ListenerReceiptAckTransition.PendingFoundWrite
                        : ListenerReceiptAckTransition.Invalid;
                }
            }

            internal bool MarkFoundWriteCompletedAndTryFinalize()
            {
                lock (_sync)
                {
                    if (_state == (int)ListenerRequestState.Terminal)
                    {
                        return false;
                    }

                    if (_receiptAcknowledged)
                    {
                        _state = (int)ListenerRequestState.Terminal;
                        return true;
                    }

                    _state = (int)ListenerRequestState.AwaitingReceiptAck;
                    return false;
                }
            }

            internal void MarkTerminal()
            {
                Volatile.Write(ref _state, (int)ListenerRequestState.Terminal);
            }
        }

        private enum ListenerReceiptAckTransition
        {
            Invalid = 0,
            PendingFoundWrite = 1,
            Terminalize = 2,
            AlreadyAcknowledged = 3,
        }

        private enum ResponseOutcome
        {
            Error = 0,
            NotFound = 1,
            Found = 2,
        }

        private enum OutboundDescriptorKind
        {
            Frame = 0,
            Found = 1,
        }

        private readonly record struct OutboundResponseDescriptor(
            uint RequestId,
            OutboundDescriptorKind Kind,
            ResponseOutcome Outcome,
            bool ShouldTerminalizeRequest,
            int ReservedFoundPayloadBytes,
            ReadOnlyMemory<byte> Frame,
            ReadOnlyMemory<byte> FoundHeader,
            ReadOnlyMemory<byte> FoundPayload)
        {
            internal static OutboundResponseDescriptor FromFrame(uint requestId, ReadOnlyMemory<byte> frame, ResponseOutcome outcome, bool shouldTerminalizeRequest, int reservedFoundPayloadBytes)
            {
                return new OutboundResponseDescriptor(requestId, OutboundDescriptorKind.Frame, outcome, shouldTerminalizeRequest, reservedFoundPayloadBytes, frame, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
            }

            internal static OutboundResponseDescriptor FromFound(uint requestId, ReadOnlyMemory<byte> payload)
            {
                ListenerFoundResponseFrame encoded = ListenerProtocolEncoder.EncodeGetResponseFound(requestId, payload);
                return new OutboundResponseDescriptor(requestId, OutboundDescriptorKind.Found, ResponseOutcome.Found, false, payload.Length, ReadOnlyMemory<byte>.Empty, encoded.Header, encoded.Payload);
            }
        }
    }
}
