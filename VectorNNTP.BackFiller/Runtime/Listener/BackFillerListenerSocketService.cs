// <copyright file="BackFillerListenerSocketService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Listener
// Implements the inbound BackFiller TLS listener for the future data-plane protocol.

using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using VectorNNTP.Backfiller.Runtime.Shutdown;

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Inbound BackFiller TLS listener for the future BackFiller/NNRPD data plane.
    /// </summary>
    /// <remarks>
    /// This service is distinct from the outbound NNTP acquisition client and from the fake NNTP test server.
    /// It binds the configured inbound endpoints, negotiates server-side TLS using the active listener certificate,
    /// and waits for the remote peer to disconnect. The application-level binary protocol has not yet been defined,
    /// so this service does not attempt to interpret any post-handshake payload.
    /// </remarks>
    internal sealed partial class BackFillerListenerSocketService(
        BackFillerRuntimeOptions runtimeOptions,
        BackFillerCertificateState certificateState,
        ShutdownCoordinator shutdownCoordinator,
        ILogger<BackFillerListenerSocketService> logger) : BackgroundService
    {
        /// <summary>
        /// Pending connection backlog requested for each bound listener socket.
        /// </summary>
        private const int ListenBacklog = 512;

        /// <summary>
        /// Validated runtime snapshot that defines listener enablement, bind addresses, and port selection.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
        /// <summary>
        /// Certificate state that supplies disposable clones of the currently active listener certificate.
        /// </summary>
        private readonly BackFillerCertificateState _certificateState = certificateState ?? throw new ArgumentNullException(nameof(certificateState));
        /// <summary>
        /// Shutdown coordinator whose tokens stop accept loops and active connection handling.
        /// </summary>
        private readonly ShutdownCoordinator _shutdownCoordinator = shutdownCoordinator ?? throw new ArgumentNullException(nameof(shutdownCoordinator));
        /// <summary>
        /// Logger receiving listener lifecycle, bind, handshake, and shutdown diagnostics.
        /// </summary>
        private readonly ILogger<BackFillerListenerSocketService> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Serializes mutations of the active client set during connection open and shutdown.
        /// </summary>
        private readonly object _connectionsGate = new();
        /// <summary>
        /// Tracks accepted clients so shutdown can dispose every live connection.
        /// </summary>
        private readonly HashSet<TcpClient> _activeClients = [];
        /// <summary>
        /// Owns the bound listener sockets created for the configured endpoint set.
        /// </summary>
        private readonly List<Socket> _listenSockets = [];

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_runtimeOptions.EffectiveLetsEncrypt.Enabled)
            {
                LogListenerDisabled(_logger);
                return;
            }

            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken,
                _shutdownCoordinator.GracefulShutdownStartedToken,
                _shutdownCoordinator.ForcedShutdownToken);

            CancellationToken token = linked.Token;

            IReadOnlyList<IPEndPoint> endpoints = BuildListenEndpoints(_runtimeOptions);
            if (endpoints.Count == 0)
            {
                throw new InvalidOperationException("No inbound listener endpoints were resolved from runtime configuration.");
            }

            try
            {
                BindAllEndpoints(endpoints);
                LogListenerStarted(_logger, _listenSockets.Count, _runtimeOptions.BindPort);

                List<Task> acceptLoops = [.. _listenSockets.Select(socket => RunAcceptLoopAsync(socket, token))];
                await Task.WhenAll(acceptLoops).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                LogListenerStoppingByCancellation(_logger);
            }
            finally
            {
                CloseListenSockets();
                CloseActiveClients();
                LogListenerStopped(_logger);
            }
        }

        /// <summary>
        /// Accepts inbound TCP connections on one bound listener until shutdown or socket disposal stops the loop.
        /// </summary>
        /// <param name="listenSocket">Bound listener socket owned by this service.</param>
        /// <param name="cancellationToken">Token that stops accepting and converts expected socket shutdown into a quiet exit.</param>
        /// <returns>A task that completes when the accept loop exits for this listener.</returns>
        /// <remarks>
        /// Each accepted socket is handed to <see cref="ProcessAcceptedSocketAsync(Socket, CancellationToken)"/> without awaiting it.
        /// That per-client routine contains its own exception handling so detached tasks do not fault the host.
        /// </remarks>
        private async Task RunAcceptLoopAsync(Socket listenSocket, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Socket acceptedSocket;
                try
                {
                    acceptedSocket = await listenSocket.AcceptAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (SocketException ex) when (IsExpectedStoppingSocketError(ex.SocketErrorCode, cancellationToken))
                {
                    break;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Inbound listener accept loop failed for endpoint {listenSocket.LocalEndPoint}.", ex);
                }

                _ = ProcessAcceptedSocketAsync(acceptedSocket, cancellationToken);
            }
        }

        /// <summary>
        /// Wraps one accepted socket in <see cref="TcpClient"/>, performs the TLS handshake, and then idles until disconnect or shutdown.
        /// </summary>
        /// <param name="acceptedSocket">Freshly accepted socket whose ownership transfers to this routine.</param>
        /// <param name="cancellationToken">Shutdown-aware token that aborts handshake or idle waiting.</param>
        /// <returns>A task that completes after the connection has been closed and unregistered.</returns>
        private async Task ProcessAcceptedSocketAsync(Socket acceptedSocket, CancellationToken cancellationToken)
        {
            TcpClient? client = null;
            try
            {
                client = new TcpClient { Client = acceptedSocket };
                RegisterClient(client);

                EndPoint? remote = acceptedSocket.RemoteEndPoint;
                string remoteEndpoint = remote?.ToString() ?? "<unknown>";
                LogClientAccepted(_logger, remoteEndpoint);

                using NetworkStream networkStream = client.GetStream();
                using SslStream sslStream = new(networkStream, leaveInnerStreamOpen: false);

                using X509Certificate2 serverCertificate = GetCurrentServerCertificateOrThrow();
                SslServerAuthenticationOptions tlsOptions = new()
                {
                    ServerCertificate = serverCertificate,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    ClientCertificateRequired = false,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                };

                await sslStream.AuthenticateAsServerAsync(tlsOptions, cancellationToken).ConfigureAwait(false);
                string thumbprint = serverCertificate.Thumbprint ?? string.Empty;
                LogTlsHandshakeSucceeded(_logger, remoteEndpoint, thumbprint);

                await WaitForClientDisconnectAsync(sslStream, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (IOException ex) when (cancellationToken.IsCancellationRequested)
            {
                LogConnectionClosedDuringShutdown(_logger, ex);
            }
            catch (AuthenticationException ex)
            {
                LogTlsHandshakeFailed(_logger, ex);
            }
            catch (Exception ex)
            {
                LogClientProcessingFault(_logger, ex);
            }
            finally
            {
                if (client is not null)
                {
                    UnregisterClient(client);
                    client.Dispose();
                }
                else
                {
                    acceptedSocket.Dispose();
                }
            }
        }

        /// <summary>
        /// Creates, binds, and tracks one listening socket for each resolved endpoint.
        /// </summary>
        /// <param name="endpoints">Validated endpoint set to bind for inbound listener startup.</param>
        private void BindAllEndpoints(IReadOnlyList<IPEndPoint> endpoints)
        {
            foreach (IPEndPoint endpoint in endpoints)
            {
                Socket listenSocket = CreateBoundListenSocket(endpoint);
                _listenSockets.Add(listenSocket);
                string endpointText = endpoint.ToString();
                string addressFamily = endpoint.AddressFamily.ToString();
                LogEndpointBound(_logger, endpointText, addressFamily);
            }
        }

        /// <summary>
        /// Expands configured bind-address semantics into the concrete endpoint set the listener should bind.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options supplying port and preserved bind-address tokens.</param>
        /// <returns>Deduplicated endpoint set for listener startup.</returns>
        /// <remarks>
        /// An empty configured token set maps to separate IPv4 and IPv6 wildcard endpoints. Explicit wildcard tokens
        /// (<c>*</c>, <c>Any</c>, <c>0.0.0.0</c>, and <c>::</c>) preserve those same listener semantics while
        /// non-wildcard tokens bind only their parsed address family.
        /// </remarks>
        private static IReadOnlyList<IPEndPoint> BuildListenEndpoints(BackFillerRuntimeOptions runtimeOptions)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            int port = runtimeOptions.BindPort;
            if (port is <= 0 or > 65535)
            {
                throw new InvalidOperationException($"Configured bind port is invalid: {port}");
            }

            HashSet<IPEndPoint> endpoints = new(new IPEndPointComparer());
            IReadOnlyList<string> configuredTokens = runtimeOptions.EffectiveConfiguredBindAddressTokens;

            if (configuredTokens.Count == 0)
            {
                _ = endpoints.Add(new IPEndPoint(IPAddress.Any, port));
                _ = endpoints.Add(new IPEndPoint(IPAddress.IPv6Any, port));
                return [.. endpoints];
            }

            for (int index = 0; index < configuredTokens.Count; index++)
            {
                string token = configuredTokens[index];
                if (string.IsNullOrWhiteSpace(token))
                {
                    continue;
                }

                string trimmed = token.Trim();
                if (BindAddressDnsAddressDeriver.IsWildcardBindAddressToken(trimmed))
                {
                    _ = endpoints.Add(new IPEndPoint(IPAddress.Any, port));
                    _ = endpoints.Add(new IPEndPoint(IPAddress.IPv6Any, port));
                    continue;
                }

                if (!IPAddress.TryParse(trimmed, out IPAddress? address))
                {
                    throw new InvalidOperationException($"Configured bind address token is invalid and cannot be parsed: '{trimmed}'.");
                }

                if (IPAddress.Any.Equals(address))
                {
                    _ = endpoints.Add(new IPEndPoint(IPAddress.Any, port));
                    continue;
                }

                if (IPAddress.IPv6Any.Equals(address))
                {
                    _ = endpoints.Add(new IPEndPoint(IPAddress.IPv6Any, port));
                    continue;
                }

                _ = endpoints.Add(new IPEndPoint(address, port));
            }

            return [.. endpoints];
        }

        /// <summary>
        /// Creates one TCP listening socket, applies address-family-specific options, and binds it to the requested endpoint.
        /// </summary>
        /// <param name="endpoint">Endpoint to bind and start listening on.</param>
        /// <returns>Bound listening socket whose ownership transfers to the caller.</returns>
        /// <remarks>
        /// IPv6 listeners are forced to IPv6-only mode so an explicit IPv4 wildcard listener remains independent.
        /// If binding or <see cref="Socket.Listen(int)"/> fails, the created socket is disposed before the exception escapes.
        /// </remarks>
        private static Socket CreateBoundListenSocket(IPEndPoint endpoint)
        {
            ArgumentNullException.ThrowIfNull(endpoint);

            Socket listenSocket = new(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
            };

            if (endpoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                listenSocket.DualMode = false;
            }

            try
            {
                listenSocket.Bind(endpoint);
                listenSocket.Listen(ListenBacklog);
                return listenSocket;
            }
            catch
            {
                listenSocket.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Retrieves a disposable clone of the current listener certificate and verifies that it can perform server authentication.
        /// </summary>
        /// <returns>Certificate clone that the caller must dispose after completing the handshake.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no active certificate exists or the active certificate lacks a private key.</exception>
        private X509Certificate2 GetCurrentServerCertificateOrThrow()
        {
            X509Certificate2? certificate = _certificateState.GetCurrentCertificateClone() ?? throw new InvalidOperationException("BackFiller listener cannot accept TLS connections because no active certificate is available.");
            if (!certificate.HasPrivateKey)
            {
                certificate.Dispose();
                throw new InvalidOperationException("BackFiller listener cannot accept TLS connections because the active certificate has no private key.");
            }

            return certificate;
        }

        /// <summary>
        /// Reads and discards post-handshake traffic until the peer disconnects or shutdown cancellation is observed.
        /// </summary>
        /// <param name="sslStream">Authenticated TLS stream for one client connection.</param>
        /// <param name="cancellationToken">Token that aborts the idle wait during shutdown.</param>
        /// <returns>A task that completes when the stream reaches EOF or cancellation is requested.</returns>
        /// <remarks>
        /// The BackFiller inbound application protocol is not yet defined, so payload bytes are intentionally ignored.
        /// </remarks>
        private static async Task WaitForClientDisconnectAsync(SslStream sslStream, CancellationToken cancellationToken)
        {
            byte[] buffer = GC.AllocateUninitializedArray<byte>(512);

            while (!cancellationToken.IsCancellationRequested)
            {
                int bytesRead = await sslStream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Adds one accepted client to the active-connection set so coordinated shutdown can dispose it later.
        /// </summary>
        /// <param name="client">Accepted client now owned by the listener.</param>
        private void RegisterClient(TcpClient client)
        {
            lock (_connectionsGate)
            {
                _ = _activeClients.Add(client);
            }
        }

        /// <summary>
        /// Removes one client from the active-connection set after its connection handling has completed.
        /// </summary>
        /// <param name="client">Client to remove from shutdown tracking.</param>
        private void UnregisterClient(TcpClient client)
        {
            lock (_connectionsGate)
            {
                _ = _activeClients.Remove(client);
            }
        }

        /// <summary>
        /// Disposes every bound listener socket and clears the owned listener collection.
        /// </summary>
        /// <remarks>
        /// Individual disposal failures are intentionally suppressed because shutdown should continue closing the remaining sockets.
        /// </remarks>
        private void CloseListenSockets()
        {
            for (int i = 0; i < _listenSockets.Count; i++)
            {
                try
                {
                    _listenSockets[i].Dispose();
                }
                catch
                {
                }
            }

            _listenSockets.Clear();
        }

        /// <summary>
        /// Snapshots and disposes every active client connection still tracked by the listener.
        /// </summary>
        /// <remarks>
        /// The active set is cleared under the connection gate before disposals run so shutdown does not race repeated cleanup.
        /// Individual disposal failures are intentionally suppressed.
        /// </remarks>
        private void CloseActiveClients()
        {
            List<TcpClient> clients;
            lock (_connectionsGate)
            {
                clients = [.. _activeClients];
                _activeClients.Clear();
            }

            for (int i = 0; i < clients.Count; i++)
            {
                try
                {
                    clients[i].Dispose();
                }
                catch
                {
                }
            }
        }

        /// <summary>
        /// Classifies socket errors that are expected when shutdown interrupts a blocked accept call.
        /// </summary>
        /// <param name="socketError">Socket error observed from the failed accept operation.</param>
        /// <param name="cancellationToken">Cancellation token that indicates the listener is stopping.</param>
        /// <returns><see langword="true"/> when the error is a shutdown side effect that should quietly terminate the loop.</returns>
        private static bool IsExpectedStoppingSocketError(SocketError socketError, CancellationToken cancellationToken)
        {
            return cancellationToken.IsCancellationRequested && socketError is SocketError.OperationAborted or SocketError.Interrupted or SocketError.NotSocket or SocketError.InvalidArgument;
        }

        /// <summary>
        /// Compares endpoints by address and port so listen-endpoint construction can deduplicate equivalent bindings.
        /// </summary>
        private sealed class IPEndPointComparer : IEqualityComparer<IPEndPoint>
        {
            /// <summary>
            /// Determines whether two endpoints bind the same address and port pair.
            /// </summary>
            /// <param name="x">First endpoint to compare.</param>
            /// <param name="y">Second endpoint to compare.</param>
            /// <returns><see langword="true"/> when both endpoints are null or share the same address and port.</returns>
            public bool Equals(IPEndPoint? x, IPEndPoint? y)
            {
                return ReferenceEquals(x, y) || (x is not null && y is not null && x.Port == y.Port && x.Address.Equals(y.Address));
            }

            /// <summary>
            /// Produces a hash code compatible with <see cref="Equals(IPEndPoint?, IPEndPoint?)"/>.
            /// </summary>
            /// <param name="obj">Endpoint whose address and port identify one binding target.</param>
            /// <returns>Hash code derived from the endpoint address and port.</returns>
            public int GetHashCode(IPEndPoint obj)
            {
                return HashCode.Combine(obj.Address, obj.Port);
            }
        }

        /// <summary>
        /// Logs that listener startup completed and records how many socket bindings were created for the configured port.
        /// </summary>
        /// <param name="logger">Logger receiving the startup event.</param>
        /// <param name="listenerCount">Number of distinct listen sockets activated for the current configuration.</param>
        /// <param name="port">TCP port shared by every activated listener.</param>
        [LoggerMessage(EventId = 2700, Level = LogLevel.Information, Message = "Inbound BackFiller listener started; ListenerCount={ListenerCount}; Port={Port}")]
        private static partial void LogListenerStarted(ILogger logger, int listenerCount, int port);

        /// <summary>
        /// Logs one concrete endpoint binding created during listener startup.
        /// </summary>
        /// <param name="logger">Logger receiving the bound-endpoint event.</param>
        /// <param name="endpoint">Local endpoint string accepted by the socket bind.</param>
        /// <param name="addressFamily">Address family of the bound socket.</param>
        [LoggerMessage(EventId = 2701, Level = LogLevel.Information, Message = "Inbound BackFiller listener bound endpoint {Endpoint} ({AddressFamily})")]
        private static partial void LogEndpointBound(ILogger logger, string endpoint, string addressFamily);

        /// <summary>
        /// Logs that the accept loop admitted one inbound TCP client for TLS processing.
        /// </summary>
        /// <param name="logger">Logger receiving the accepted-client event.</param>
        /// <param name="remoteEndpoint">Remote endpoint reported by the accepted client socket.</param>
        [LoggerMessage(EventId = 2702, Level = LogLevel.Debug, Message = "Inbound BackFiller listener accepted connection from {RemoteEndpoint}")]
        private static partial void LogClientAccepted(ILogger logger, string remoteEndpoint);

        /// <summary>
        /// Logs that one accepted client completed the inbound TLS handshake successfully.
        /// </summary>
        /// <param name="logger">Logger receiving the handshake-success event.</param>
        /// <param name="remoteEndpoint">Remote endpoint associated with the negotiated TLS session.</param>
        /// <param name="thumbprint">Thumbprint of the certificate presented by the listener.</param>
        [LoggerMessage(EventId = 2703, Level = LogLevel.Information, Message = "Inbound BackFiller TLS handshake succeeded for {RemoteEndpoint}; Thumbprint={Thumbprint}")]
        private static partial void LogTlsHandshakeSucceeded(ILogger logger, string remoteEndpoint, string thumbprint);

        /// <summary>
        /// Logs that one accepted client failed the inbound TLS handshake and will be disconnected.
        /// </summary>
        /// <param name="logger">Logger receiving the handshake-failure event.</param>
        /// <param name="exception">Handshake exception captured for diagnostics.</param>
        [LoggerMessage(EventId = 2704, Level = LogLevel.Warning, Message = "Inbound BackFiller TLS handshake failed")]
        private static partial void LogTlsHandshakeFailed(ILogger logger, Exception exception);

        /// <summary>
        /// Logs that post-accept client processing failed outside the expected shutdown path.
        /// </summary>
        /// <param name="logger">Logger receiving the client-processing fault event.</param>
        /// <param name="exception">Unhandled processing exception captured from the client task.</param>
        [LoggerMessage(EventId = 2705, Level = LogLevel.Warning, Message = "Inbound BackFiller client connection processing faulted")]
        private static partial void LogClientProcessingFault(ILogger logger, Exception exception);

        /// <summary>
        /// Logs that a client connection closed while shutdown cancellation was already in progress.
        /// </summary>
        /// <param name="logger">Logger receiving the shutdown-close event.</param>
        /// <param name="exception">I/O exception observed while waiting for the connection to close.</param>
        [LoggerMessage(EventId = 2706, Level = LogLevel.Debug, Message = "Inbound BackFiller listener connection closed during shutdown")]
        private static partial void LogConnectionClosedDuringShutdown(ILogger logger, Exception exception);

        /// <summary>
        /// Logs that the accept loop is stopping because host or shutdown cancellation was signaled.
        /// </summary>
        /// <param name="logger">Logger receiving the cancellation-stop event.</param>
        [LoggerMessage(EventId = 2707, Level = LogLevel.Information, Message = "Inbound BackFiller listener stopping due to shutdown/cancellation")]
        private static partial void LogListenerStoppingByCancellation(ILogger logger);

        /// <summary>
        /// Logs that listener shutdown completed after sockets and tracked clients were closed.
        /// </summary>
        /// <param name="logger">Logger receiving the stop-complete event.</param>
        [LoggerMessage(EventId = 2708, Level = LogLevel.Information, Message = "Inbound BackFiller listener stopped")]
        private static partial void LogListenerStopped(ILogger logger);

        /// <summary>
        /// Logs that the inbound listener remains disabled because certificate provisioning is not enabled.
        /// </summary>
        /// <param name="logger">Logger receiving the disabled-listener event.</param>
        [LoggerMessage(EventId = 2709, Level = LogLevel.Information, Message = "Inbound BackFiller listener is disabled because Let's Encrypt is not enabled")]
        private static partial void LogListenerDisabled(ILogger logger);
    }
}
