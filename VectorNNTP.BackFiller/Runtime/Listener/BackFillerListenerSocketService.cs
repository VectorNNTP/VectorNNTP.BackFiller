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
        /// Stores listen backlog used by back filler listener socket service.
        /// </summary>
        private const int ListenBacklog = 512;

        /// <summary>
        /// Stores runtime options used by back filler listener socket service.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
        /// <summary>
        /// Stores certificate state used by back filler listener socket service.
        /// </summary>
        private readonly BackFillerCertificateState _certificateState = certificateState ?? throw new ArgumentNullException(nameof(certificateState));
        /// <summary>
        /// Stores shutdown coordinator used by back filler listener socket service.
        /// </summary>
        private readonly ShutdownCoordinator _shutdownCoordinator = shutdownCoordinator ?? throw new ArgumentNullException(nameof(shutdownCoordinator));
        /// <summary>
        /// Supplies the logger used by back filler listener socket service.
        /// </summary>
        private readonly ILogger<BackFillerListenerSocketService> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Stores connections gate used by back filler listener socket service.
        /// </summary>
        private readonly object _connectionsGate = new();
        /// <summary>
        /// Stores active clients used by back filler listener socket service.
        /// </summary>
        private readonly HashSet<TcpClient> _activeClients = [];
        /// <summary>
        /// Stores listen sockets used by back filler listener socket service.
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
        /// Handles run accept loop async for back filler listener socket service.
        /// </summary>
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
        /// Handles process accepted socket async for back filler listener socket service.
        /// </summary>
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
        /// Handles bind all endpoints for back filler listener socket service.
        /// </summary>
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
        /// Handles build listen endpoints for back filler listener socket service.
        /// </summary>
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
        /// Handles create bound listen socket for back filler listener socket service.
        /// </summary>
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
        /// Handles get current server certificate or throw for back filler listener socket service.
        /// </summary>
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
        /// Handles wait for client disconnect async for back filler listener socket service.
        /// </summary>
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
        /// Handles register client for back filler listener socket service.
        /// </summary>
        private void RegisterClient(TcpClient client)
        {
            lock (_connectionsGate)
            {
                _ = _activeClients.Add(client);
            }
        }

        /// <summary>
        /// Handles unregister client for back filler listener socket service.
        /// </summary>
        private void UnregisterClient(TcpClient client)
        {
            lock (_connectionsGate)
            {
                _ = _activeClients.Remove(client);
            }
        }

        /// <summary>
        /// Handles close listen sockets for back filler listener socket service.
        /// </summary>
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
        /// Handles close active clients for back filler listener socket service.
        /// </summary>
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
        /// Handles is expected stopping socket error for back filler listener socket service.
        /// </summary>
        private static bool IsExpectedStoppingSocketError(SocketError socketError, CancellationToken cancellationToken)
        {
            return cancellationToken.IsCancellationRequested && socketError is SocketError.OperationAborted or SocketError.Interrupted or SocketError.NotSocket or SocketError.InvalidArgument;
        }

        /// <summary>
        /// Defines ipend point comparer and its back filler listener socket service contract.
        /// </summary>
        private sealed class IPEndPointComparer : IEqualityComparer<IPEndPoint>
        {
            /// <summary>
            /// Handles equals for back filler listener socket service.
            /// </summary>
            /// <param name="x">The x value.</param>
            /// <param name="y">The y value.</param>
            /// <returns>true when the operation succeeds; otherwise false.</returns>
            public bool Equals(IPEndPoint? x, IPEndPoint? y)
            {
                return ReferenceEquals(x, y) || (x is not null && y is not null && x.Port == y.Port && x.Address.Equals(y.Address));
            }

            /// <summary>
            /// Handles get hash code for back filler listener socket service.
            /// </summary>
            /// <param name="obj">The obj value.</param>
            /// <returns>The operation result.</returns>
            public int GetHashCode(IPEndPoint obj)
            {
                return HashCode.Combine(obj.Address, obj.Port);
            }
        }

        /// <summary>
        /// Emits the listener started log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2700, Level = LogLevel.Information, Message = "Inbound BackFiller listener started; ListenerCount={ListenerCount}; Port={Port}")]
        private static partial void LogListenerStarted(ILogger logger, int listenerCount, int port);

        /// <summary>
        /// Emits the endpoint bound log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2701, Level = LogLevel.Information, Message = "Inbound BackFiller listener bound endpoint {Endpoint} ({AddressFamily})")]
        private static partial void LogEndpointBound(ILogger logger, string endpoint, string addressFamily);

        /// <summary>
        /// Emits the client accepted log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2702, Level = LogLevel.Debug, Message = "Inbound BackFiller listener accepted connection from {RemoteEndpoint}")]
        private static partial void LogClientAccepted(ILogger logger, string remoteEndpoint);

        /// <summary>
        /// Emits the tls handshake succeeded log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2703, Level = LogLevel.Information, Message = "Inbound BackFiller TLS handshake succeeded for {RemoteEndpoint}; Thumbprint={Thumbprint}")]
        private static partial void LogTlsHandshakeSucceeded(ILogger logger, string remoteEndpoint, string thumbprint);

        /// <summary>
        /// Emits the tls handshake failed log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2704, Level = LogLevel.Warning, Message = "Inbound BackFiller TLS handshake failed")]
        private static partial void LogTlsHandshakeFailed(ILogger logger, Exception exception);

        /// <summary>
        /// Emits the client processing fault log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2705, Level = LogLevel.Warning, Message = "Inbound BackFiller client connection processing faulted")]
        private static partial void LogClientProcessingFault(ILogger logger, Exception exception);

        /// <summary>
        /// Emits the connection closed during shutdown log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2706, Level = LogLevel.Debug, Message = "Inbound BackFiller listener connection closed during shutdown")]
        private static partial void LogConnectionClosedDuringShutdown(ILogger logger, Exception exception);

        /// <summary>
        /// Emits the listener stopping by cancellation log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2707, Level = LogLevel.Information, Message = "Inbound BackFiller listener stopping due to shutdown/cancellation")]
        private static partial void LogListenerStoppingByCancellation(ILogger logger);

        /// <summary>
        /// Emits the listener stopped log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2708, Level = LogLevel.Information, Message = "Inbound BackFiller listener stopped")]
        private static partial void LogListenerStopped(ILogger logger);

        /// <summary>
        /// Emits the listener disabled log event for back filler listener socket service.
        /// </summary>
        [LoggerMessage(EventId = 2709, Level = LogLevel.Information, Message = "Inbound BackFiller listener is disabled because Let's Encrypt is not enabled")]
        private static partial void LogListenerDisabled(ILogger logger);
    }
}
