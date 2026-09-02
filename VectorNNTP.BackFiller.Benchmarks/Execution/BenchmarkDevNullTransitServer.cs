// <copyright file="BenchmarkDevNullTransitServer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/BenchmarkDevNullTransitServer: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Provides a benchmark-only NNTP STREAMING sink that consumes article bytes and replies with accepted status.
/// </summary>
internal sealed class BenchmarkDevNullTransitServer : IAsyncDisposable
{
    /// <summary>
    /// Implements the greeting Bytes contract.
    /// </summary>
    private static readonly ReadOnlyMemory<byte> GreetingBytes = Encoding.ASCII.GetBytes("200 benchmark dev-null sink ready\r\n");
    /// <summary>
    /// Implements the capabilities HeaderBytes contract.
    /// </summary>
    private static readonly ReadOnlyMemory<byte> CapabilitiesHeaderBytes = Encoding.ASCII.GetBytes("101 Capability list:\r\n");
    /// <summary>
    /// Implements the capabilities StreamingBytes contract.
    /// </summary>
    private static readonly ReadOnlyMemory<byte> CapabilitiesStreamingBytes = Encoding.ASCII.GetBytes("STREAMING\r\n");
    /// <summary>
    /// Implements the dot LineBytes contract.
    /// </summary>
    private static readonly ReadOnlyMemory<byte> DotLineBytes = Encoding.ASCII.GetBytes(".\r\n");
    /// <summary>
    /// Implements the streaming PermittedBytes contract.
    /// </summary>
    private static readonly ReadOnlyMemory<byte> StreamingPermittedBytes = Encoding.ASCII.GetBytes("203 Streaming permitted\r\n");
    /// <summary>
    /// Implements the quit ResponseBytes contract.
    /// </summary>
    private static readonly ReadOnlyMemory<byte> QuitResponseBytes = Encoding.ASCII.GetBytes("205 closing connection\r\n");
    /// <summary>
    /// Implements the unknown CommandBytes contract.
    /// </summary>
    private static readonly ReadOnlyMemory<byte> UnknownCommandBytes = Encoding.ASCII.GetBytes("500 unknown command\r\n");
    /// <summary>
    /// Gets or sets the capabilities CommandBytes.
    /// </summary>
    private static ReadOnlySpan<byte> CapabilitiesCommandBytes => "CAPABILITIES"u8;
    /// <summary>
    /// Gets or sets the mode StreamCommandBytes.
    /// </summary>
    private static ReadOnlySpan<byte> ModeStreamCommandBytes => "MODE STREAM"u8;
    /// <summary>
    /// Gets or sets the quit CommandBytes.
    /// </summary>
    private static ReadOnlySpan<byte> QuitCommandBytes => "QUIT"u8;
    /// <summary>
    /// Gets or sets the check PrefixBytes.
    /// </summary>
    private static ReadOnlySpan<byte> CheckPrefixBytes => "CHECK "u8;
    /// <summary>
    /// Gets or sets the takethis PrefixBytes.
    /// </summary>
    private static ReadOnlySpan<byte> TakethisPrefixBytes => "TAKETHIS "u8;
    /// <summary>
    /// Implements the article Terminator contract.
    /// </summary>
    private static readonly byte[] ArticleTerminator = "\r\n.\r\n"u8.ToArray();

    /// <summary>
    /// Gets or sets the _listener.
    /// </summary>
    private readonly TcpListener _listener;
    /// <summary>
    /// Runs the _clientTasks benchmark scenario.
    /// </summary>
    private readonly ConcurrentDictionary<int, Task> _clientTasks = new();
    /// <summary>
    /// Runs the _shutdown benchmark scenario.
    /// </summary>
    private readonly CancellationTokenSource _shutdown = new();

    /// <summary>
    /// Gets or sets the _acceptLoopTask.
    /// </summary>
    private Task? _acceptLoopTask;
    /// <summary>
    /// Gets or sets the _clientTaskId.
    /// </summary>
    private int _clientTaskId;

    /// <summary>
    /// Gets or sets the _acceptedArticles.
    /// </summary>
    private long _acceptedArticles;
    /// <summary>
    /// Gets or sets the _consumedArticleBytes.
    /// </summary>
    private long _consumedArticleBytes;
    /// <summary>
    /// Gets or sets the _totalConnections.
    /// </summary>
    private long _totalConnections;

    /// <summary>
    /// Implements the benchmark DevNullTransitServer contract.
    /// </summary>
    private BenchmarkDevNullTransitServer(IPAddress listenAddress, int port)
    {
        ArgumentNullException.ThrowIfNull(listenAddress);

        _listener = new TcpListener(listenAddress, port);
        ListenAddress = listenAddress;
    }

    /// <summary>
    /// Gets the default fixed TCP listen port for benchmark fake-server runs.
    /// </summary>
    internal const int DefaultListenPort = 1190;

    /// <summary>
    /// Gets the benchmark endpoint type label emitted by this sink.
    /// </summary>
    internal const string EndpointTypeLabel = "BENCHMARK FAKE SERVER / DEV NULL";

    /// <summary>
    /// Gets a stable implementation identity string for benchmark artifacts.
    /// </summary>
    internal const string ServerIdentity = "BenchmarkDevNullTransitServer/v1";

    /// <summary>
    /// Gets the local IP address the sink is listening on.
    /// </summary>
    internal IPAddress ListenAddress { get; }

    /// <summary>
    /// Gets the local TCP port the sink is listening on.
    /// </summary>
    internal int Port
    {
        get
        {
            return (_listener.LocalEndpoint as IPEndPoint)?.Port ?? 0;
        }
    }

    /// <summary>
    /// Gets the total number of accepted TAKETHIS submissions.
    /// </summary>
    internal long AcceptedArticles => Interlocked.Read(ref _acceptedArticles);

    /// <summary>
    /// Gets the total opaque payload bytes consumed prior to framing terminators.
    /// </summary>
    internal long ConsumedArticleBytes => Interlocked.Read(ref _consumedArticleBytes);

    /// <summary>
    /// Gets the total number of accepted TCP connections.
    /// </summary>
    internal long TotalConnections => Interlocked.Read(ref _totalConnections);

    /// <summary>
    /// Starts the benchmark sink server and returns the running instance.
    /// </summary>
    /// <param name="listenAddress">The local address to bind.</param>
    /// <param name="port">The local port to bind. Defaults to fixed benchmark port 1190.</param>
    /// <param name="cancellationToken">A cancellation token used while starting.</param>
    /// <returns>A running sink server instance.</returns>
    internal static Task<BenchmarkDevNullTransitServer> StartAsync(IPAddress listenAddress, int port = DefaultListenPort, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        BenchmarkDevNullTransitServer server = new(listenAddress, port);
        server._listener.Start(backlog: 1024);
        server._acceptLoopTask = Task.Run(() => server.AcceptLoopAsync(server._shutdown.Token), CancellationToken.None);
        return Task.FromResult(server);
    }

    /// <summary>
    /// Stops the benchmark sink server and all active client handlers.
    /// </summary>
    /// <returns>A task that completes when shutdown is finished.</returns>
    public async ValueTask DisposeAsync()
    {
        _shutdown.Cancel();

        try
        {
            _listener.Stop();
        }
        catch (SocketException)
        {
            // Listener may already be closed during coordinated shutdown.
        }

        if (_acceptLoopTask is not null)
        {
            await AwaitNoThrowAsync(_acceptLoopTask).ConfigureAwait(false);
        }

        Task[] handlers = _clientTasks.Values.ToArray();
        if (handlers.Length > 0)
        {
            foreach (Task handler in handlers)
            {
                await AwaitNoThrowAsync(handler).ConfigureAwait(false);
            }
        }

        _shutdown.Dispose();
    }

    /// <summary>
    /// Continuously accepts and dispatches inbound TCP clients.
    /// </summary>
    /// <param name="cancellationToken">The shutdown cancellation token.</param>
    /// <returns>A task representing the accept loop lifetime.</returns>
    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (SocketException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            Interlocked.Increment(ref _totalConnections);
            int taskId = Interlocked.Increment(ref _clientTaskId);
            string remoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? "<unknown>";
            string localEndpoint = client.Client.LocalEndPoint?.ToString() ?? "<unknown>";
            Console.WriteLine($"[FAKE-LC] ACCEPT taskId={taskId} remote={remoteEndpoint} local={localEndpoint}");

            Task clientTask = Task.Run(() => HandleClientAsync(client, taskId, cancellationToken), CancellationToken.None);
            _clientTasks[taskId] = clientTask;
            _ = clientTask.ContinueWith(
                _ => _clientTasks.TryRemove(taskId, out _),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Runs NNTP command handling for a single client connection.
    /// </summary>
    /// <param name="client">The connected client.</param>
    /// <param name="taskId">Identifier assigned to the client session.</param>
    /// <param name="cancellationToken">The server shutdown token.</param>
    /// <returns>A task representing the client session.</returns>
    private async Task HandleClientAsync(TcpClient client, int taskId, CancellationToken cancellationToken)
    {
        string remoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? "<unknown>";
        string sessionId = $"{taskId:D4}-{Guid.NewGuid():N}";
        long commandsReceived = 0;
        long bytesConsumed = 0;
        long responsesSent = 0;
        string lastProtocolEvent = "Accepted";
        string closeReason = "Unspecified";

        try
        {
            using (client)
            {
                using NetworkStream stream = client.GetStream();
                PipeReader reader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
                PipeWriter writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
                Channel<ResponseWorkItem> responseQueue = Channel.CreateUnbounded<ResponseWorkItem>(new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = true,
                    AllowSynchronousContinuations = true,
                });

                lastProtocolEvent = "WriteGreeting";
                await WriteAsync(writer, GreetingBytes, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref responsesSent);

                lastProtocolEvent = "ExpectCapabilities";
                (bool capabilitiesMatched, string? capabilitiesFailureReason) = await ExpectCommandAndReplyAsync(reader, writer, ExpectedCommand.Capabilities, cancellationToken).ConfigureAwait(false);
                if (!capabilitiesMatched)
                {
                    closeReason = $"CapabilitiesNegotiationFailed:{capabilitiesFailureReason}";
                    return;
                }

                lastProtocolEvent = "WriteCapabilities";
                await WriteAsync(writer, CapabilitiesHeaderBytes, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref responsesSent);
                await WriteAsync(writer, CapabilitiesStreamingBytes, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref responsesSent);
                await WriteAsync(writer, DotLineBytes, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref responsesSent);

                lastProtocolEvent = "ExpectModeStream";
                (bool modeStreamMatched, string? modeStreamFailureReason) = await ExpectCommandAndReplyAsync(reader, writer, ExpectedCommand.ModeStream, cancellationToken).ConfigureAwait(false);
                if (!modeStreamMatched)
                {
                    closeReason = $"ModeStreamNegotiationFailed:{modeStreamFailureReason}";
                    return;
                }

                lastProtocolEvent = "WriteModeStreamPermitted";
                await WriteAsync(writer, StreamingPermittedBytes, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref responsesSent);

                TransmitLoopMetrics txMetrics = new();
                TransmitLoopControl txControl = new();
                Task txTask = RunTransmitLoopAsync(writer, responseQueue.Reader, txControl, txMetrics, () => Interlocked.Increment(ref responsesSent));

                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        lastProtocolEvent = "ReadCommand";
                        (ParsedCommand? command, string? parserFailureReason) = await ReadCommandAsync(reader, cancellationToken).ConfigureAwait(false);
                        if (command is null)
                        {
                            closeReason = parserFailureReason is null
                                ? "PeerDisconnectOrProtocolReadFailure"
                                : $"ParserInvalidOperation:{parserFailureReason}";
                            break;
                        }

                        commandsReceived++;

                        if (command.Value.Kind == CommandKind.Quit)
                        {
                            lastProtocolEvent = "SignalQuit";
                            closeReason = "PeerQuit";
                            txControl.RequestQuit();
                            responseQueue.Writer.TryComplete();
                            break;
                        }

                        if (command.Value.Kind == CommandKind.Takethis)
                        {
                            lastProtocolEvent = "ConsumeTakethisPayload";
                            long payloadLength = await ConsumeArticlePayloadAsync(reader, cancellationToken).ConfigureAwait(false);
                            bytesConsumed += payloadLength;
                            Interlocked.Add(ref _consumedArticleBytes, payloadLength);
                            Interlocked.Increment(ref _acceptedArticles);

                            lastProtocolEvent = "QueueTakethisAccepted";
                            EnqueueResponse(responseQueue.Writer, ResponseWorkItem.Takethis(command.Value.MessageId));
                            continue;
                        }

                        if (command.Value.Kind == CommandKind.Check)
                        {
                            lastProtocolEvent = "QueueCheckResponse";
                            EnqueueResponse(responseQueue.Writer, ResponseWorkItem.Check(command.Value.MessageId));
                            continue;
                        }

                        lastProtocolEvent = "QueueUnknownCommand";
                        EnqueueResponse(responseQueue.Writer, ResponseWorkItem.Unknown());
                    }
                }
                finally
                {
                    responseQueue.Writer.TryComplete();
                    await AwaitNoThrowAsync(txTask).ConfigureAwait(false);
                }

                Console.WriteLine($"[FAKE-TX] sessionId={sessionId} iterations={txMetrics.Iterations} flushes={txMetrics.FlushCount} responses={txMetrics.TotalResponsesSent} avgResponsesPerFlush={txMetrics.AverageResponsesPerFlush:F2} maxResponsesPerFlush={txMetrics.MaxResponsesPerFlush} quitRequested={txControl.IsQuitRequested}");

                if (closeReason == "Unspecified")
                {
                    closeReason = "ServerShutdownCancellation";
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            closeReason = "ServerShutdownCancellation";
        }
        catch (TaskCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            closeReason = "ServerShutdownCancellation";
        }
        catch (Exception ex)
        {
            closeReason = $"Exception:{ex.GetType().Name}";
            Console.WriteLine($"BenchmarkDevNullTransitServer client handler error: {ex}");
        }
        finally
        {
            Console.WriteLine($"[FAKE-LC] CLOSE sessionId={sessionId} taskId={taskId} remote={remoteEndpoint} commands={commandsReceived} bytesConsumed={bytesConsumed} responses={responsesSent} lastEvent={lastProtocolEvent} reason={closeReason} socketState={DescribeSocketState(client)}");
        }
    }

    /// <summary>
    /// Reads and verifies an expected command line.
    /// </summary>
    /// <param name="reader">The command reader.</param>
    /// <param name="writer">The response writer.</param>
    /// <param name="expectedCommand">The expected command discriminator.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns><see langword="true"/> when the command matched; otherwise <see langword="false"/>.</returns>
    private static async Task<(bool Matched, string? FailureReason)> ExpectCommandAndReplyAsync(PipeReader reader, PipeWriter writer, ExpectedCommand expectedCommand, CancellationToken cancellationToken)
    {
        (ParsedCommand? command, string? parserFailureReason) = await ReadCommandAsync(reader, cancellationToken).ConfigureAwait(false);
        if (command is null)
        {
            string? failureReason = parserFailureReason is null
                ? "ReadLineReturnedNull"
                : $"ParserInvalidOperation:{parserFailureReason}";
            return (false, failureReason);
        }

        bool matched = expectedCommand switch
        {
            ExpectedCommand.Capabilities => command.Value.Kind == CommandKind.Capabilities,
            ExpectedCommand.ModeStream => command.Value.Kind == CommandKind.ModeStream,
            _ => false,
        };

        if (!matched)
        {
            await WriteAsync(writer, UnknownCommandBytes, cancellationToken).ConfigureAwait(false);
            return (false, $"UnexpectedCommand:{command.Value.Kind}");
        }

        return (true, null);
    }

    /// <summary>
    /// Reads one CRLF-terminated command from the connection and parses it using byte-oriented dispatch.
    /// </summary>
    /// <param name="reader">The pipe reader.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>The parsed command, or <see langword="null"/> when disconnected or parse failed.</returns>
    private static async Task<(ParsedCommand? Command, string? ParserFailureReason)> ReadCommandAsync(PipeReader reader, CancellationToken cancellationToken)
    {
        while (true)
        {
            ReadResult result;
            try
            {
                result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidOperationException ex)
            {
                return (null, ex.Message);
            }

            ReadOnlySequence<byte> buffer = result.Buffer;
            SequenceReader<byte> sequenceReader = new(buffer);
            if (!sequenceReader.TryReadTo(out ReadOnlySequence<byte> line, (byte)'\n', advancePastDelimiter: true))
            {
                if (result.IsCompleted)
                {
                    reader.AdvanceTo(buffer.End);
                    return (null, "NNTP connection closed while awaiting line response.");
                }

                if (buffer.Length > 16 * 1024)
                {
                    reader.AdvanceTo(buffer.End);
                    return (null, "NNTP response line exceeded maximum length of 16384 bytes.");
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            if (TryGetLastByte(line, out byte lastByte) && lastByte == (byte)'\r')
            {
                line = line.Slice(0, line.Length - 1);
            }

            ParsedCommand parsedCommand = TryParseCommand(line, out ParsedCommand parsed)
                ? parsed
                : new ParsedCommand(CommandKind.Unknown, string.Empty);

            SequencePosition consumed = sequenceReader.Position;
            reader.AdvanceTo(consumed, consumed);
            return (parsedCommand, null);
        }
    }

    /// <summary>
    /// Implements the try ParseCommand contract.
    /// </summary>
    private static bool TryParseCommand(in ReadOnlySequence<byte> line, out ParsedCommand command)
    {
        if (line.IsEmpty)
        {
            command = default;
            return false;
        }

        if (AsciiEqualsIgnoreCase(line, CapabilitiesCommandBytes))
        {
            command = new ParsedCommand(CommandKind.Capabilities, string.Empty);
            return true;
        }

        if (AsciiEqualsIgnoreCase(line, ModeStreamCommandBytes))
        {
            command = new ParsedCommand(CommandKind.ModeStream, string.Empty);
            return true;
        }

        if (AsciiEqualsIgnoreCase(line, QuitCommandBytes))
        {
            command = new ParsedCommand(CommandKind.Quit, string.Empty);
            return true;
        }

        if (AsciiStartsWithIgnoreCase(line, TakethisPrefixBytes))
        {
            ReadOnlySequence<byte> messageIdBytes = line.Slice(TakethisPrefixBytes.Length);
            string messageId = DecodeMessageIdOrDefault(messageIdBytes);
            command = new ParsedCommand(CommandKind.Takethis, messageId);
            return true;
        }

        if (AsciiStartsWithIgnoreCase(line, CheckPrefixBytes))
        {
            ReadOnlySequence<byte> messageIdBytes = line.Slice(CheckPrefixBytes.Length);
            string messageId = DecodeMessageIdOrDefault(messageIdBytes);
            command = new ParsedCommand(CommandKind.Check, messageId);
            return true;
        }

        command = new ParsedCommand(CommandKind.Unknown, string.Empty);
        return true;
    }

    /// <summary>
    /// Implements the decode MessageIdOrDefault contract.
    /// </summary>
    private static string DecodeMessageIdOrDefault(in ReadOnlySequence<byte> messageIdBytes)
    {
        string raw = Encoding.ASCII.GetString(messageIdBytes.ToArray()).Trim();
        return string.IsNullOrWhiteSpace(raw)
            ? "<missing-message-id@benchmark.devnull>"
            : raw;
    }

    /// <summary>
    /// Implements the ascii EqualsIgnoreCase contract.
    /// </summary>
    private static bool AsciiEqualsIgnoreCase(in ReadOnlySequence<byte> value, ReadOnlySpan<byte> expected)
    {
        if (value.Length != expected.Length)
        {
            return false;
        }

        int index = 0;
        foreach (ReadOnlyMemory<byte> segment in value)
        {
            ReadOnlySpan<byte> span = segment.Span;
            for (int i = 0; i < span.Length; i++)
            {
                if (index >= expected.Length || ToUpperAsciiInvariant(span[i]) != expected[index])
                {
                    return false;
                }

                index++;
            }
        }

        return index == expected.Length;
    }

    /// <summary>
    /// Implements the ascii StartsWithIgnoreCase contract.
    /// </summary>
    private static bool AsciiStartsWithIgnoreCase(in ReadOnlySequence<byte> value, ReadOnlySpan<byte> prefix)
    {
        if (value.Length < prefix.Length)
        {
            return false;
        }

        int index = 0;
        foreach (ReadOnlyMemory<byte> segment in value)
        {
            ReadOnlySpan<byte> span = segment.Span;
            for (int i = 0; i < span.Length && index < prefix.Length; i++)
            {
                if (ToUpperAsciiInvariant(span[i]) != prefix[index])
                {
                    return false;
                }

                index++;
            }

            if (index == prefix.Length)
            {
                return true;
            }
        }

        return index == prefix.Length;
    }

    /// <summary>
    /// Implements the try GetLastByte contract.
    /// </summary>
    private static bool TryGetLastByte(in ReadOnlySequence<byte> sequence, out byte value)
    {
        value = 0;
        if (sequence.IsEmpty)
        {
            return false;
        }

        foreach (ReadOnlyMemory<byte> segment in sequence)
        {
            ReadOnlySpan<byte> span = segment.Span;
            if (!span.IsEmpty)
            {
                value = span[^1];
            }
        }

        return true;
    }

    /// <summary>
    /// Converts to UpperAsciiInvariant.
    /// </summary>
    private static byte ToUpperAsciiInvariant(byte value)
    {
        return value is >= (byte)'a' and <= (byte)'z'
            ? (byte)(value - 32)
            : value;
    }

    /// <summary>
    /// Consumes and discards opaque TAKETHIS payload bytes until article framing terminator is observed.
    /// </summary>
    /// <param name="reader">The pipe reader.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>The number of payload bytes consumed before framing terminator.</returns>
    private static async Task<long> ConsumeArticlePayloadAsync(PipeReader reader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        const int terminatorLength = 5; // "\r\n.\r\n"
        byte[] trailingWindow = new byte[terminatorLength];
        int trailingCount = 0;
        long payloadBytes = 0;

        while (true)
        {
            ReadResult result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            ReadOnlySequence<byte> buffer = result.Buffer;
            SequenceReader<byte> sequenceReader = new(buffer);

            while (!sequenceReader.End)
            {
                byte current = sequenceReader.CurrentSpan[sequenceReader.CurrentSpanIndex];
                sequenceReader.Advance(1);
                payloadBytes++;

                if (trailingCount < terminatorLength)
                {
                    trailingWindow[trailingCount++] = current;
                }
                else
                {
                    trailingWindow[0] = trailingWindow[1];
                    trailingWindow[1] = trailingWindow[2];
                    trailingWindow[2] = trailingWindow[3];
                    trailingWindow[3] = trailingWindow[4];
                    trailingWindow[4] = current;
                }

                if (trailingCount == terminatorLength
                    && trailingWindow[0] == (byte)'\r'
                    && trailingWindow[1] == (byte)'\n'
                    && trailingWindow[2] == (byte)'.'
                    && trailingWindow[3] == (byte)'\r'
                    && trailingWindow[4] == (byte)'\n')
                {
                    SequencePosition consumedUpTo = sequenceReader.Position;
                    reader.AdvanceTo(consumedUpTo, consumedUpTo);
                    return payloadBytes - terminatorLength;
                }
            }

            if (result.IsCompleted)
            {
                reader.AdvanceTo(buffer.End);
                throw new IOException("TAKETHIS payload terminated before NNTP article delimiter was received.");
            }

            reader.AdvanceTo(buffer.End);
        }
    }

    /// <summary>
    /// Runs the single-writer response path for a client connection.
    /// </summary>
    /// <param name="writer">The socket-bound protocol writer.</param>
    /// <param name="queueReader">The FIFO response queue reader.</param>
    /// <param name="control">Shared control state for quit and connection completion.</param>
    /// <param name="metrics">Counters updated while responses are transmitted.</param>
    /// <param name="onResponseWritten">Callback invoked after each response is staged.</param>
    /// <returns>A task representing the transmit loop.</returns>
    private static async Task RunTransmitLoopAsync(PipeWriter writer, ChannelReader<ResponseWorkItem> queueReader, TransmitLoopControl control, TransmitLoopMetrics metrics, Action onResponseWritten)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(queueReader);
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(metrics);
        ArgumentNullException.ThrowIfNull(onResponseWritten);

        while (true)
        {
            if (control.IsQuitRequested)
            {
                await SendQuitAndStopAsync(writer, queueReader, metrics, onResponseWritten).ConfigureAwait(false);
                return;
            }

            bool canRead = await queueReader.WaitToReadAsync().ConfigureAwait(false);
            if (!canRead)
            {
                if (control.IsQuitRequested)
                {
                    await SendQuitAndStopAsync(writer, queueReader, metrics, onResponseWritten).ConfigureAwait(false);
                }

                return;
            }

            if (control.IsQuitRequested)
            {
                await SendQuitAndStopAsync(writer, queueReader, metrics, onResponseWritten).ConfigureAwait(false);
                return;
            }

            if (!queueReader.TryRead(out ResponseWorkItem firstResponse))
            {
                continue;
            }

            int stagedCount = 0;
            metrics.Iterations++;

            WriteResponse(writer, firstResponse);
            onResponseWritten();
            stagedCount++;

            while (stagedCount < 11 && queueReader.TryRead(out ResponseWorkItem response))
            {
                WriteResponse(writer, response);
                onResponseWritten();
                stagedCount++;
            }

            metrics.RecordFlush(stagedCount);
            FlushResult flushResult = await writer.FlushAsync().ConfigureAwait(false);
            if (flushResult.IsCompleted)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Implements the send QuitAndStopAsync contract.
    /// </summary>
    private static async Task SendQuitAndStopAsync(PipeWriter writer, ChannelReader<ResponseWorkItem> queueReader, TransmitLoopMetrics metrics, Action onResponseWritten)
    {
        while (queueReader.TryRead(out _))
        {
        }

        metrics.Iterations++;
        writer.Write(QuitResponseBytes.Span);
        onResponseWritten();
        metrics.RecordFlush(1);

        FlushResult flushResult = await writer.FlushAsync().ConfigureAwait(false);
        if (flushResult.IsCompleted)
        {
            return;
        }
    }

    /// <summary>
    /// Implements the enqueue Response contract.
    /// </summary>
    private static void EnqueueResponse(ChannelWriter<ResponseWorkItem> writer, in ResponseWorkItem workItem)
    {
        ArgumentNullException.ThrowIfNull(writer);

        if (writer.TryWrite(workItem))
        {
            return;
        }

        ValueTask writeTask = writer.WriteAsync(workItem);
        if (writeTask.IsCompletedSuccessfully)
        {
            writeTask.GetAwaiter().GetResult();
            return;
        }

        writeTask.AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Writes Response.
    /// </summary>
    private static void WriteResponse(PipeWriter writer, in ResponseWorkItem response)
    {
        switch (response.Kind)
        {
            case ResponseKind.TakethisAccepted:
                WriteTakethisAccepted(writer, response.MessageId);
                return;
            case ResponseKind.CheckSend:
                WriteCheckSend(writer, response.MessageId);
                return;
            case ResponseKind.UnknownCommand:
                writer.Write(UnknownCommandBytes.Span);
                return;
            default:
                throw new InvalidOperationException($"Unexpected response kind '{response.Kind}'.");
        }
    }

    /// <summary>
    /// Represents the transmit LoopControl class used by the benchmark or regression gate.
    /// </summary>
    private sealed class TransmitLoopControl
    {
        /// <summary>
        /// Gets or sets the _quitRequested.
        /// </summary>
        private int _quitRequested;

        /// <summary>
        /// Implements the is QuitRequested contract.
        /// </summary>
        internal bool IsQuitRequested => Volatile.Read(ref _quitRequested) == 1;

        /// <summary>
        /// Implements the request Quit contract.
        /// </summary>
        internal void RequestQuit()
        {
            Interlocked.Exchange(ref _quitRequested, 1);
        }
    }

    /// <summary>
    /// Represents the transmit LoopMetrics class used by the benchmark or regression gate.
    /// </summary>
    private sealed class TransmitLoopMetrics
    {
        /// <summary>
        /// Gets or sets the _flushCount.
        /// </summary>
        private long _flushCount;
        /// <summary>
        /// Gets or sets the _totalResponsesSent.
        /// </summary>
        private long _totalResponsesSent;
        /// <summary>
        /// Gets or sets the _maxResponsesPerFlush.
        /// </summary>
        private int _maxResponsesPerFlush;

        /// <summary>
        /// Gets or sets the iterations.
        /// </summary>
        internal long Iterations { get; set; }

        /// <summary>
        /// Implements the flush Count contract.
        /// </summary>
        internal long FlushCount => Interlocked.Read(ref _flushCount);

        /// <summary>
        /// Implements the total ResponsesSent contract.
        /// </summary>
        internal long TotalResponsesSent => Interlocked.Read(ref _totalResponsesSent);

        /// <summary>
        /// Implements the max ResponsesPerFlush contract.
        /// </summary>
        internal int MaxResponsesPerFlush => Volatile.Read(ref _maxResponsesPerFlush);

        /// <summary>
        /// Gets or sets the average ResponsesPerFlush.
        /// </summary>
        internal double AverageResponsesPerFlush => FlushCount == 0
            ? 0d
            : (double)TotalResponsesSent / FlushCount;

        /// <summary>
        /// Implements the record Flush contract.
        /// </summary>
        internal void RecordFlush(int responsesInFlush)
        {
            Interlocked.Increment(ref _flushCount);
            Interlocked.Add(ref _totalResponsesSent, responsesInFlush);

            while (true)
            {
                int current = Volatile.Read(ref _maxResponsesPerFlush);
                if (responsesInFlush <= current)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _maxResponsesPerFlush, responsesInFlush, current) == current)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// Represents the expected Command enum used by the benchmark or regression gate.
    /// </summary>
    private enum ExpectedCommand
    {
        Capabilities,
        ModeStream,
    }

    /// <summary>
    /// Represents the command Kind enum used by the benchmark or regression gate.
    /// </summary>
    private enum CommandKind
    {
        Unknown,
        Capabilities,
        ModeStream,
        Takethis,
        Check,
        Quit,
    }

    /// <summary>
    /// Represents the parsed Command record struct used by the benchmark or regression gate.
    /// </summary>
    private readonly record struct ParsedCommand(CommandKind Kind, string MessageId);

    /// <summary>
    /// Represents the response Kind enum used by the benchmark or regression gate.
    /// </summary>
    private enum ResponseKind
    {
        TakethisAccepted,
        CheckSend,
        UnknownCommand,
    }

    /// <summary>
    /// Represents the response WorkItem record struct used by the benchmark or regression gate.
    /// </summary>
    private readonly record struct ResponseWorkItem(ResponseKind Kind, string MessageId)
    {
        /// <summary>
        /// Runs the takethis benchmark scenario.
        /// </summary>
        public static ResponseWorkItem Takethis(string messageId) => new(ResponseKind.TakethisAccepted, messageId);

        /// <summary>
        /// Runs the check benchmark scenario.
        /// </summary>
        public static ResponseWorkItem Check(string messageId) => new(ResponseKind.CheckSend, messageId);

        /// <summary>
        /// Runs the unknown benchmark scenario.
        /// </summary>
        public static ResponseWorkItem Unknown() => new(ResponseKind.UnknownCommand, string.Empty);
    }

    /// <summary>
    /// Stages a 238 CHECK success response preserving message-id correlation.
    /// </summary>
    /// <param name="writer">The response writer.</param>
    /// <param name="messageId">The message-id from the CHECK command line.</param>
    private static void WriteCheckSend(PipeWriter writer, string messageId)
    {
        ArgumentNullException.ThrowIfNull(messageId);

        const string responsePrefix = "238 ";
        const string responseSuffix = " send article to be transferred\r\n";

        int maxBytes = responsePrefix.Length + messageId.Length + responseSuffix.Length;
        Span<byte> span = writer.GetSpan(maxBytes);
        int written = 0;
        written += Encoding.ASCII.GetBytes(responsePrefix, span[written..]);
        written += Encoding.ASCII.GetBytes(messageId, span[written..]);
        written += Encoding.ASCII.GetBytes(responseSuffix, span[written..]);
        writer.Advance(written);
    }

    /// <summary>
    /// Stages a 239 TAKETHIS success response preserving message-id correlation.
    /// </summary>
    /// <param name="writer">The response writer.</param>
    /// <param name="messageId">The message-id from the TAKETHIS command line.</param>
    private static void WriteTakethisAccepted(PipeWriter writer, string messageId)
    {
        ArgumentNullException.ThrowIfNull(messageId);

        const string responsePrefix = "239 ";
        const string responseSuffix = " Article transferred OK\r\n";

        int maxBytes = responsePrefix.Length + messageId.Length + responseSuffix.Length;
        Span<byte> span = writer.GetSpan(maxBytes);
        int written = 0;
        written += Encoding.ASCII.GetBytes(responsePrefix, span[written..]);
        written += Encoding.ASCII.GetBytes(messageId, span[written..]);
        written += Encoding.ASCII.GetBytes(responseSuffix, span[written..]);
        writer.Advance(written);
    }

    /// <summary>
    /// Writes bytes to the client and flushes buffered output.
    /// </summary>
    /// <param name="writer">The output writer.</param>
    /// <param name="payload">The bytes to write.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>A task that completes when bytes are flushed.</returns>
    private static async Task WriteAsync(PipeWriter writer, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        await writer.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        FlushResult flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        if (flushResult.IsCanceled)
        {
            throw new OperationCanceledException(cancellationToken);
        }
    }

    /// <summary>
    /// Awaits a task while suppressing expected shutdown exceptions.
    /// </summary>
    /// <param name="task">The task to await.</param>
    /// <returns>A task representing the await operation.</returns>
    private static async Task AwaitNoThrowAsync(Task task)
    {
        ArgumentNullException.ThrowIfNull(task);

        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (SocketException)
        {
        }
    }

    /// <summary>
    /// Implements the describe SocketState contract.
    /// </summary>
    private static string DescribeSocketState(TcpClient client)
    {
        ArgumentNullException.ThrowIfNull(client);

        try
        {
            Socket? socket = client.Client;
            if (socket is null)
            {
                return "socket-null";
            }

            bool pollRead = socket.Poll(0, SelectMode.SelectRead);
            bool pollWrite = socket.Poll(0, SelectMode.SelectWrite);
            bool pollError = socket.Poll(0, SelectMode.SelectError);
            return $"connected={socket.Connected},available={socket.Available},pollRead={pollRead},pollWrite={pollWrite},pollError={pollError}";
        }
        catch (ObjectDisposedException)
        {
            return "disposed";
        }
        catch (SocketException ex)
        {
            return $"socketException:{ex.SocketErrorCode}";
        }
        catch (NullReferenceException)
        {
            return "socket-unavailable";
        }
    }
}
