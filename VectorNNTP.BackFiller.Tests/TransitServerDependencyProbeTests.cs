// <copyright file="TransitServerDependencyProbeTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for transit server dependency probe.

using System.Net;
using System.Net.Sockets;
using System.Text;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Documents the TransitServerDependencyProbeTests test type and its protected contract.
    /// </summary>
    public sealed class TransitServerDependencyProbeTests
    {
        /// <summary>
        /// Verifies the ValidateTransitServerConnectivityAsync_WhenStartTlsAdvertisedButRejected_FailsValidation scenario and expected contract.
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
        /// Documents the ProbeNntpServer test type and its protected contract.
        /// </summary>
        private sealed class ProbeNntpServer : IAsyncDisposable
        {
            /// <summary>
            /// Stores the _listener fixture value used by these tests.
            /// </summary>
            private readonly TcpListener _listener;
            /// <summary>
            /// Documents the _session member and its test-supporting contract.
            /// </summary>
            private readonly Func<NetworkStream, CancellationToken, Task> _session;
            /// <summary>
            /// Stores the _cts fixture value used by these tests.
            /// </summary>
            private readonly CancellationTokenSource _cts = new();
            /// <summary>
            /// Stores the _acceptLoopTask fixture value used by these tests.
            /// </summary>
            private readonly Task _acceptLoopTask;

            /// <summary>
            /// Verifies the ProbeNntpServer scenario and expected contract.
            /// </summary>
            private ProbeNntpServer(TcpListener listener, Func<NetworkStream, CancellationToken, Task> session)
            {
                _listener = listener;
                _session = session;
                _acceptLoopTask = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Stores the Port value used by this test fixture.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Verifies the StartAsync scenario and expected contract.
            /// </summary>
            internal static Task<ProbeNntpServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
            {
                ArgumentNullException.ThrowIfNull(session);

                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();

                ProbeNntpServer server = new(listener, session);
                return Task.FromResult(server);
            }

            /// <summary>
            /// Verifies the AcceptLoopAsync scenario and expected contract.
            /// </summary>
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
            /// Verifies the ReadLineAsync scenario and expected contract.
            /// </summary>
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
            /// Verifies the WriteLineAsync scenario and expected contract.
            /// </summary>
            internal static async Task WriteLineAsync(Stream stream, string line, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(stream);
                ArgumentException.ThrowIfNullOrWhiteSpace(line);

                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
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
