using System.Net;
using System.Net.Sockets;
using System.Text;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

public sealed class TransitServerDependencyProbeTests
{
    [Fact]
    public async Task ValidateTransitServerConnectivityAsync_WhenStartTlsAdvertisedButRejected_FailsValidation()
    {
        using CancellationTokenSource testTimeout = new(TimeSpan.FromSeconds(10));

        await using ProbeNntpServer server = await ProbeNntpServer.StartAsync(async (stream, cancellationToken) =>
        {
            await ProbeNntpServer.WriteLineAsync(stream, "200 transit ready", cancellationToken);

            string firstCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken);
            if (string.Equals(firstCommand, "CAPABILITIES", StringComparison.Ordinal))
            {
                await ProbeNntpServer.WriteLineAsync(stream, "101 Capability list:", cancellationToken);
                await ProbeNntpServer.WriteLineAsync(stream, "STARTTLS", cancellationToken);
                await ProbeNntpServer.WriteLineAsync(stream, "STREAMING", cancellationToken);
                await ProbeNntpServer.WriteLineAsync(stream, ".", cancellationToken);

                string nextCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken);
                if (string.Equals(nextCommand, "STARTTLS", StringComparison.Ordinal))
                {
                    await ProbeNntpServer.WriteLineAsync(stream, "580 TLS not available", cancellationToken);
                    return;
                }

                if (string.Equals(nextCommand, "MODE STREAM", StringComparison.Ordinal))
                {
                    await ProbeNntpServer.WriteLineAsync(stream, "203 Streaming permitted", cancellationToken);
                    string quitCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken);
                    if (string.Equals(quitCommand, "QUIT", StringComparison.Ordinal))
                    {
                        await ProbeNntpServer.WriteLineAsync(stream, "205 closing connection", cancellationToken);
                    }
                }

                return;
            }

            if (string.Equals(firstCommand, "MODE STREAM", StringComparison.Ordinal))
            {
                await ProbeNntpServer.WriteLineAsync(stream, "203 Streaming permitted", cancellationToken);
                string quitCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken);
                if (string.Equals(quitCommand, "QUIT", StringComparison.Ordinal))
                {
                    await ProbeNntpServer.WriteLineAsync(stream, "205 closing connection", cancellationToken);
                }
            }
        });

        BackFillerOptions options = new()
        {
            TransitServer = new TransitServerOptions
            {
                Host = IPAddress.Loopback.ToString(),
                Port = server.Port,
                UseSsl = false,
            },
        };

        DependencyValidationResult result = await TransitServerDependencyProbe.ValidateTransitServerConnectivityAsync(
            options,
            TimeSpan.FromSeconds(3),
            testTimeout.Token);

        Assert.False(result.IsValid);
        Assert.Contains(result.FailedDependencies, static failure =>
            failure.Dependency == "TransitServer"
            && failure.Reason.Contains("STARTTLS negotiation rejected", StringComparison.Ordinal));
    }

    private sealed class ProbeNntpServer : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly Func<NetworkStream, CancellationToken, Task> _session;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _acceptLoopTask;

        private ProbeNntpServer(TcpListener listener, Func<NetworkStream, CancellationToken, Task> session)
        {
            _listener = listener;
            _session = session;
            _acceptLoopTask = Task.Run(AcceptLoopAsync);
        }

        internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        internal static Task<ProbeNntpServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
        {
            ArgumentNullException.ThrowIfNull(session);

            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();

            ProbeNntpServer server = new(listener, session);
            return Task.FromResult(server);
        }

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
            catch (ObjectDisposedException)
            {
            }
        }

        internal static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
        {
            List<byte> buffer = [];

            while (true)
            {
                byte[] one = new byte[1];
                int read = await stream.ReadAsync(one, cancellationToken);
                if (read == 0)
                {
                    throw new InvalidOperationException("Unexpected EOF while reading line.");
                }

                byte current = one[0];
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

        internal static async Task WriteLineAsync(Stream stream, string line, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(line);

            byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
            await stream.WriteAsync(bytes, cancellationToken);
            await stream.FlushAsync(cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _listener.Stop();

            try
            {
                await _acceptLoopTask;
            }
            catch (OperationCanceledException)
            {
            }

            _cts.Dispose();
        }
    }
}
