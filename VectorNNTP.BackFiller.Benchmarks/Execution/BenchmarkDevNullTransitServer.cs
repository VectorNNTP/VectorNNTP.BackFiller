using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Provides a benchmark-only NNTP STREAMING sink that consumes article bytes and replies with accepted status.
/// </summary>
internal sealed class BenchmarkDevNullTransitServer : IAsyncDisposable
{
    private static readonly ReadOnlyMemory<byte> GreetingBytes = Encoding.ASCII.GetBytes("200 benchmark dev-null sink ready\r\n");
    private static readonly ReadOnlyMemory<byte> CapabilitiesHeaderBytes = Encoding.ASCII.GetBytes("101 Capability list:\r\n");
    private static readonly ReadOnlyMemory<byte> CapabilitiesStreamingBytes = Encoding.ASCII.GetBytes("STREAMING\r\n");
    private static readonly ReadOnlyMemory<byte> DotLineBytes = Encoding.ASCII.GetBytes(".\r\n");
    private static readonly ReadOnlyMemory<byte> StreamingPermittedBytes = Encoding.ASCII.GetBytes("203 Streaming permitted\r\n");
    private static readonly ReadOnlyMemory<byte> QuitResponseBytes = Encoding.ASCII.GetBytes("205 closing connection\r\n");
    private static readonly ReadOnlyMemory<byte> UnknownCommandBytes = Encoding.ASCII.GetBytes("500 unknown command\r\n");
    private static readonly byte[] ArticleTerminator = "\r\n.\r\n"u8.ToArray();

    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<int, Task> _clientTasks = new();
    private readonly CancellationTokenSource _shutdown = new();

    private Task? _acceptLoopTask;
    private int _clientTaskId;

    private long _acceptedArticles;
    private long _consumedArticleBytes;
    private long _totalConnections;

    private BenchmarkDevNullTransitServer(IPAddress listenAddress, int port)
    {
        ArgumentNullException.ThrowIfNull(listenAddress);

        _listener = new TcpListener(listenAddress, port);
        ListenAddress = listenAddress;
    }

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
    /// <param name="port">The local port to bind, or zero for ephemeral.</param>
    /// <param name="cancellationToken">A cancellation token used while starting.</param>
    /// <returns>A running sink server instance.</returns>
    internal static Task<BenchmarkDevNullTransitServer> StartAsync(IPAddress listenAddress, int port = 0, CancellationToken cancellationToken = default)
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
            Task clientTask = Task.Run(() => HandleClientAsync(client, cancellationToken), CancellationToken.None);
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
    /// <param name="cancellationToken">The server shutdown token.</param>
    /// <returns>A task representing the client session.</returns>
    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        try
        {
            using (client)
            {
                using NetworkStream stream = client.GetStream();
                PipeReader reader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
                PipeWriter writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));

                await WriteAsync(writer, GreetingBytes, cancellationToken).ConfigureAwait(false);

                if (!await ExpectCommandAndReplyAsync(reader, writer, expectedCommand: "CAPABILITIES", cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                await WriteAsync(writer, CapabilitiesHeaderBytes, cancellationToken).ConfigureAwait(false);
                await WriteAsync(writer, CapabilitiesStreamingBytes, cancellationToken).ConfigureAwait(false);
                await WriteAsync(writer, DotLineBytes, cancellationToken).ConfigureAwait(false);

                if (!await ExpectCommandAndReplyAsync(reader, writer, expectedCommand: "MODE STREAM", cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                await WriteAsync(writer, StreamingPermittedBytes, cancellationToken).ConfigureAwait(false);

                while (!cancellationToken.IsCancellationRequested)
                {
                    string? commandLine = await ReadLineAsync(reader, cancellationToken).ConfigureAwait(false);
                    if (commandLine is null)
                    {
                        return;
                    }

                    if (string.Equals(commandLine, "QUIT", StringComparison.OrdinalIgnoreCase))
                    {
                        await WriteAsync(writer, QuitResponseBytes, cancellationToken).ConfigureAwait(false);
                        return;
                    }

                    if (TryParseTakethisMessageId(commandLine, out string? messageId))
                    {
                        long payloadLength = await ConsumeArticlePayloadAsync(reader, cancellationToken).ConfigureAwait(false);
                        Interlocked.Add(ref _consumedArticleBytes, payloadLength);
                        Interlocked.Increment(ref _acceptedArticles);
                        await WriteTakethisAcceptedAsync(writer, messageId, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    await WriteAsync(writer, UnknownCommandBytes, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not TaskCanceledException)
        {
            Console.WriteLine($"BenchmarkDevNullTransitServer client handler error: {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Reads and verifies an expected command line.
    /// </summary>
    /// <param name="reader">The command reader.</param>
    /// <param name="writer">The response writer.</param>
    /// <param name="expectedCommand">The expected exact command.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns><see langword="true"/> when the command matched; otherwise <see langword="false"/>.</returns>
    private static async Task<bool> ExpectCommandAndReplyAsync(PipeReader reader, PipeWriter writer, string expectedCommand, CancellationToken cancellationToken)
    {
        string? line = await ReadLineAsync(reader, cancellationToken).ConfigureAwait(false);
        if (line is null)
        {
            return false;
        }

        if (!string.Equals(line, expectedCommand, StringComparison.OrdinalIgnoreCase))
        {
            await WriteAsync(writer, UnknownCommandBytes, cancellationToken).ConfigureAwait(false);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Reads one CRLF-terminated command line from the connection.
    /// </summary>
    /// <param name="reader">The pipe reader.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>The decoded command line without CRLF, or <see langword="null"/> when disconnected.</returns>
    private static async Task<string?> ReadLineAsync(PipeReader reader, CancellationToken cancellationToken)
    {
        try
        {
            return await TransitProtocolParser.ReadNntpLineAsync(reader, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            return null;
        }
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

            foreach (ReadOnlyMemory<byte> segment in buffer)
            {
                ReadOnlySpan<byte> span = segment.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    byte current = span[i];
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
                        reader.AdvanceTo(buffer.End);
                        return payloadBytes - terminatorLength;
                    }
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
    /// Parses the message-id from a TAKETHIS command line.
    /// </summary>
    /// <param name="commandLine">The received command line.</param>
    /// <param name="messageId">The parsed message-id when successful.</param>
    /// <returns><see langword="true"/> when TAKETHIS command syntax is present.</returns>
    private static bool TryParseTakethisMessageId(string commandLine, out string messageId)
    {
        ArgumentNullException.ThrowIfNull(commandLine);

        const string prefix = "TAKETHIS ";
        if (!commandLine.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            messageId = string.Empty;
            return false;
        }

        string rawMessageId = commandLine[prefix.Length..].Trim();
        if (string.IsNullOrWhiteSpace(rawMessageId))
        {
            messageId = "<missing-message-id@benchmark.devnull>";
            return true;
        }

        messageId = rawMessageId;
        return true;
    }

    /// <summary>
    /// Writes a 239 TAKETHIS success response preserving message-id correlation.
    /// </summary>
    /// <param name="writer">The response writer.</param>
    /// <param name="messageId">The message-id from the TAKETHIS command line.</param>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    /// <returns>A task that completes when the response is flushed.</returns>
    private static async Task WriteTakethisAcceptedAsync(PipeWriter writer, string messageId, CancellationToken cancellationToken)
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

        FlushResult flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        if (flushResult.IsCanceled)
        {
            throw new OperationCanceledException(cancellationToken);
        }
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
}
