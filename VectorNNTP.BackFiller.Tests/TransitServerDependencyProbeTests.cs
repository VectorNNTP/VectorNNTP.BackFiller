// <copyright file="TransitServerDependencyProbeTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit server dependency probe, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit server dependency probe test suite.

using System.Net;
using System.Net.Sockets;
using System.Text;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
        /// Verifies the transit server dependency probe tests scenario and its documented contract.
    /// </summary>
    public sealed class TransitServerDependencyProbeTests
    {
        /// <summary>
        /// Verifies the validate transit server connectivity async when start tls advertised but rejected fails validation scenario and its documented contract.
        /// </summary>
        [Fact]
        public async Task ValidateTransitServerConnectivityAsync_WhenStartTlsAdvertisedButRejected_FailsValidation()
        {
            using CancellationTokenSource testTimeout = new(TimeSpan.FromSeconds(10));

            ProbeNntpServer serverInstance = await ProbeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                await ProbeNntpServer.WriteLineAsync(stream, "200 transit ready", cancellationToken).ConfigureAwait(false);

                string firstCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken).ConfigureAwait(false);
                if (string.Equals(firstCommand, "CAPABILITIES", StringComparison.Ordinal))
                {
                    await ProbeNntpServer.WriteLineAsync(stream, "101 Capability list:", cancellationToken).ConfigureAwait(false);
                    await ProbeNntpServer.WriteLineAsync(stream, "STARTTLS", cancellationToken).ConfigureAwait(false);
                    await ProbeNntpServer.WriteLineAsync(stream, "STREAMING", cancellationToken).ConfigureAwait(false);
                    await ProbeNntpServer.WriteLineAsync(stream, ".", cancellationToken).ConfigureAwait(false);

                    string nextCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken).ConfigureAwait(false);
                    if (string.Equals(nextCommand, "STARTTLS", StringComparison.Ordinal))
                    {
                        await ProbeNntpServer.WriteLineAsync(stream, "580 TLS not available", cancellationToken).ConfigureAwait(false);
                        return;
                    }

                    if (string.Equals(nextCommand, "MODE STREAM", StringComparison.Ordinal))
                    {
                        await ProbeNntpServer.WriteLineAsync(stream, "203 Streaming permitted", cancellationToken).ConfigureAwait(false);
                        string quitCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken).ConfigureAwait(false);
                        if (string.Equals(quitCommand, "QUIT", StringComparison.Ordinal))
                        {
                            await ProbeNntpServer.WriteLineAsync(stream, "205 closing connection", cancellationToken).ConfigureAwait(false);
                        }
                    }

                    return;
                }

                if (string.Equals(firstCommand, "MODE STREAM", StringComparison.Ordinal))
                {
                    await ProbeNntpServer.WriteLineAsync(stream, "203 Streaming permitted", cancellationToken).ConfigureAwait(false);
                    string quitCommand = await ProbeNntpServer.ReadLineAsync(stream, cancellationToken).ConfigureAwait(false);
                    if (string.Equals(quitCommand, "QUIT", StringComparison.Ordinal))
                    {
                        await ProbeNntpServer.WriteLineAsync(stream, "205 closing connection", cancellationToken).ConfigureAwait(false);
                    }
                }
            });

            await using (serverInstance.ConfigureAwait(false))
            {
                BackFillerOptions options = new()
                {
                    TransitServer = new TransitServerOptions
                    {
                        Host = IPAddress.Loopback.ToString(),
                        Port = serverInstance.Port,
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
        }

        /// <summary>
        /// Verifies the probe nntp server scenario and its documented contract.
        /// </summary>
        private sealed class ProbeNntpServer : IAsyncDisposable
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
            /// Supplies  accept loop task for the fixture or scenario under test.
            /// </summary>
            private readonly Task _acceptLoopTask;

            /// <summary>
        /// Verifies the probe nntp server scenario and its documented contract.
            /// </summary>
            private ProbeNntpServer(TcpListener listener, Func<NetworkStream, CancellationToken, Task> session)
            {
                _listener = listener;
                _session = session;
                _acceptLoopTask = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Exercises port behavior, including the expected result and failure semantics.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
        /// Verifies the start async scenario and its documented contract.
            /// </summary>
        /// <returns>The start async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the start async scenario and its documented contract.
        /// </summary>
        /// <param name="NetworkStream">The network stream supplied to the helper.</param>
        /// <param name="CancellationToken">The cancellation token supplied to the helper.</param>
        /// <param name="session">The session supplied to the helper.</param>
        /// <returns>The start async value produced for the requested scenario.</returns>
            internal static Task<ProbeNntpServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
            {
                ArgumentNullException.ThrowIfNull(session);

                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();

                ProbeNntpServer server = new(listener, session);
                return Task.FromResult(server);
            }

            /// <summary>
        /// Verifies the accept loop async scenario and its documented contract.
            /// </summary>
        /// <returns>The accept loop async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the accept loop async scenario and its documented contract.
        /// </summary>
        /// <returns>The accept loop async value produced for the requested scenario.</returns>
            private async Task AcceptLoopAsync()
            {
                try
                {
                    using TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                    using NetworkStream stream = client.GetStream();
                    await _session(stream, _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }

            /// <summary>
        /// Verifies the read line async scenario and its documented contract.
            /// </summary>
        /// <returns>The read line async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the read line async scenario and its documented contract.
        /// </summary>
        /// <param name="stream">The stream supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The read line async value produced for the requested scenario.</returns>
            internal static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> buffer = [];

                while (true)
                {
                    byte[] one = new byte[1];
                    int read = await stream.ReadAsync(one, cancellationToken).ConfigureAwait(false);
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

                return Encoding.ASCII.GetString([.. buffer]);
            }

            /// <summary>
        /// Verifies the write line async scenario and its documented contract.
            /// </summary>
        /// <returns>The write line async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the write line async scenario and its documented contract.
        /// </summary>
        /// <param name="stream">The stream supplied to the helper.</param>
        /// <param name="line">The line supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The write line async value produced for the requested scenario.</returns>
            internal static async Task WriteLineAsync(Stream stream, string line, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(stream);
                ArgumentException.ThrowIfNullOrWhiteSpace(line);

                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
            /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
        /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
            public async ValueTask DisposeAsync()
            {
                _cts.Cancel();
                _listener.Stop();

                try
                {
                    await _acceptLoopTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                _cts.Dispose();
            }
        }
    }
}
