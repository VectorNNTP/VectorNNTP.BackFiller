// <copyright file="RabbitMqDependencyProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Validation
// Implements the rabbit mq dependency probe behavior.

using System.Net.Sockets;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Serilog;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Executes RabbitMQ startup dependency probes and converts connectivity outcomes into aggregated dependency-validation diagnostics.
    /// </summary>
    /// <remarks>
    /// The probe attempts host-by-host AMQP connectivity using runtime-validated RabbitMQ settings, logs successful
    /// connections for operator visibility, and records probe failures in <see cref="DependencyValidationResult"/>
    /// instead of failing fast on the first host error.
    /// </remarks>
    internal static class RabbitMqDependencyProbe
    {
        /// <summary>
        /// Probes RabbitMQ connectivity for each configured host by opening an AMQP connection and channel within the configured timeout.
        /// </summary>
        /// <param name="runtimeOptions">
        /// Validated runtime options snapshot that supplies RabbitMQ endpoint settings and canonical identity used to build the connection name.
        /// </param>
        /// <param name="timeout">Per-host probe timeout applied via a linked cancellation token source.</param>
        /// <param name="cancellationToken">Startup cancellation token; when canceled, probing stops and cancellation is rethrown.</param>
        /// <returns>
        /// A task that completes with an aggregated <see cref="DependencyValidationResult"/> containing one failure entry per host-level
        /// connectivity/authentication/protocol issue. Unexpected probe exceptions are logged at debug level and represented as sanitized failures.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="runtimeOptions"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Validated RabbitMQ runtime settings are missing from <paramref name="runtimeOptions"/>.</exception>
        /// <exception cref="OperationCanceledException">The outer <paramref name="cancellationToken"/> is canceled.</exception>
        /// <remarks>
        /// On successful host probes, an informational structured log is emitted with <c>Host</c>, <c>Port</c>, <c>VirtualHost</c>,
        /// <c>ConnectionName</c>, and <c>EnableSsl</c>. Probe failures are accumulated into the returned result rather than thrown,
        /// allowing startup validation to report all failed RabbitMQ hosts in one pass.
        /// </remarks>
        internal static async Task<DependencyValidationResult> ValidateRabbitMqConnectivityAsync(
            BackFillerRuntimeOptions runtimeOptions,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            List<(string Dependency, string Reason)> failures = [];
            List<(string Category, string Message)> warnings = [];
            List<(string Category, string Message)> errors = [];

            RabbitMqRuntimeOptions rabbitMq = runtimeOptions.RabbitMq
                ?? throw new InvalidOperationException("Validated runtime RabbitMQ settings are required for dependency probing.");

            IReadOnlyList<string> hosts = RabbitMqConnectionFactoryBuilder.BuildHostList(rabbitMq);
            if (hosts.Count == 0 || rabbitMq.Port <= 0)
            {
                return new DependencyValidationResult(failures, warnings, errors);
            }

            string connectionName = RabbitMqRuntimeOptions.GetDefaultConnectionName(runtimeOptions.CanonicalBackFillerFqdn);
            ConnectionFactory connectionFactory = RabbitMqConnectionFactoryBuilder.BuildConnectionFactory(rabbitMq, connectionName);

            foreach (string host in hosts)
            {
                try
                {
                    using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    cts.CancelAfter(timeout);

                    using IConnection connection = await connectionFactory
                        .CreateConnectionAsync([host], cts.Token)
                        .ConfigureAwait(false);

                    using IChannel channel = await connection
                        .CreateChannelAsync(options: default, cancellationToken: cts.Token)
                        .ConfigureAwait(false);

                    Log.Information(
                        "RabbitMQ connectivity validated successfully (Host: {Host}, Port: {Port}, VirtualHost: {VirtualHost}, ConnectionName: {ConnectionName}, EnableSsl: {EnableSsl})",
                        host,
                        rabbitMq.Port,
                        rabbitMq.VirtualHost,
                        connectionName,
                        rabbitMq.EnableSsl);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (OperationCanceledException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{rabbitMq.Port} connection timed out after {timeout.TotalSeconds:F1}s"));
                }
                catch (AuthenticationFailureException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{rabbitMq.Port} authentication failed"));
                }
                catch (PossibleAuthenticationFailureException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{rabbitMq.Port} authentication failed"));
                }
                catch (BrokerUnreachableException ex) when (ex.InnerException is SocketException socketEx)
                {
                    failures.Add(("RabbitMQ", $"{host}:{rabbitMq.Port} {GetSanitizedSocketFailureReason(socketEx.SocketErrorCode)}"));
                }
                catch (BrokerUnreachableException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{rabbitMq.Port} unable to establish AMQP connection"));
                }
                catch (Exception ex)
                {
                    Log.Debug(ex, "RabbitMQ connectivity validation threw an exception during startup dependency validation.");
                    failures.Add(("RabbitMQ", $"{host}:{rabbitMq.Port} unexpected connectivity failure"));
                }
            }

            return new DependencyValidationResult(failures, warnings, errors);
        }

        /// <summary>
        /// Translates socket error codes into sanitized connectivity reason text suitable for startup diagnostics.
        /// </summary>
        /// <param name="socketError">Socket error code observed from dependency connectivity failure handling.</param>
        /// <returns>A non-sensitive human-readable reason string used in dependency failure messages.</returns>
        /// <remarks>
        /// Unknown socket error values intentionally collapse to a generic message to avoid exposing low-level platform
        /// details in operator-facing startup output.
        /// </remarks>
        internal static string GetSanitizedSocketFailureReason(SocketError socketError)
        {
            return socketError switch
            {
                SocketError.TimedOut
                    => "Connection timed out",

                SocketError.ConnectionRefused
                    => "Connection refused by remote endpoint",

                SocketError.HostNotFound
                    or SocketError.NoData
                    or SocketError.TryAgain
                    or SocketError.NoRecovery
                    => "DNS host resolution failed",

                SocketError.NetworkUnreachable
                    or SocketError.NetworkDown
                    => "Network is unreachable",

                SocketError.HostUnreachable
                    or SocketError.HostDown
                    => "Host is unreachable",

                SocketError.ConnectionAborted
                    or SocketError.ConnectionReset
                    or SocketError.Shutdown
                    or SocketError.Disconnecting
                    => "Connection was terminated before the operation completed",

                SocketError.NotConnected
                    or SocketError.NotSocket
                    => "Socket is not connected",

                SocketError.AddressAlreadyInUse
                    => "Local endpoint address is already in use",

                SocketError.AddressNotAvailable
                    => "Requested local or remote address is not available",

                SocketError.AccessDenied
                    => "Socket operation was denied by the operating system",

                SocketError.NoBufferSpaceAvailable
                    => "Insufficient socket buffer resources",

                SocketError.TooManyOpenSockets
                    or SocketError.ProcessLimit
                    => "Socket resource limit was reached",

                SocketError.WouldBlock
                    or SocketError.InProgress
                    or SocketError.AlreadyInProgress
                    => "Socket operation is already in progress",

                SocketError.OperationAborted
                    => "Socket operation was aborted",

                SocketError.IOPending
                    => "Socket I/O operation is pending",

                SocketError.Interrupted
                    => "Socket operation was interrupted",

                SocketError.Fault
                    => "Socket operation failed due to a local system fault",

                SocketError.InvalidArgument
                    => "Invalid argument supplied to socket operation",

                SocketError.DestinationAddressRequired
                    => "Destination address is required for the socket operation",

                SocketError.MessageSize
                    => "Socket message exceeds the supported size",

                SocketError.ProtocolType
                    or SocketError.ProtocolOption
                    or SocketError.ProtocolNotSupported
                    or SocketError.SocketNotSupported
                    or SocketError.OperationNotSupported
                    => "Requested socket protocol or operation is not supported",

                SocketError.ProtocolFamilyNotSupported
                    or SocketError.AddressFamilyNotSupported
                    => "Requested socket protocol or address family is not supported",

                SocketError.NetworkReset
                    => "Network connection was reset",

                SocketError.IsConnected
                    => "Socket is already connected",

                SocketError.SystemNotReady
                    => "Networking subsystem is not ready",

                SocketError.VersionNotSupported
                    => "Requested socket API version is not supported",

                SocketError.NotInitialized
                    => "Socket subsystem has not been initialized",

                SocketError.TypeNotFound
                    => "Requested socket type was not found",

                SocketError.Success
                    => "Socket operation completed successfully",

                SocketError.SocketError
                    => "Socket operation failed",

                _ => "Unable to reach endpoint",
            };
        }
    }
}
