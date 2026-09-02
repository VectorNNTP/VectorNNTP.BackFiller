// <copyright file="RabbitMqDependencyProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: rabbit mq dependency probe in the startup validation subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="RabbitMqDependencyProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using System.Net.Sockets;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Serilog;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Defines the rabbit mq dependency probe component and its contracts for this subsystem.
    /// </summary>
    internal static class RabbitMqDependencyProbe
    {
        /// <summary>
        /// Validates RabbitMQ dependency health by opening AMQP connections/channels for configured runtime hosts.
        /// </summary>
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
        /// Maps socket-level connectivity failures to sanitized startup diagnostics.
        /// </summary>
        internal static string GetSanitizedSocketFailureReason(SocketError socketError)
        {
#pragma warning disable IDE0072 // Add missing cases
            return socketError switch
            {
                SocketError.TimedOut => "Connection timed out",
                SocketError.ConnectionRefused => "Connection refused by remote endpoint",
                SocketError.HostNotFound or SocketError.NoData => "DNS host resolution failed",
                SocketError.NetworkUnreachable => "Network is unreachable",
                SocketError.HostUnreachable => "Host is unreachable",
                _ => "Unable to reach endpoint",
            };
#pragma warning restore IDE0072 // Add missing cases
        }
    }
}
