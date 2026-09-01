// <copyright file="RabbitMqConnectionFactoryBuilder.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using System.Security.Authentication;
using RabbitMQ.Client;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Builds RabbitMQ client connection factories from validated runtime options.
    /// </summary>
    internal static class RabbitMqConnectionFactoryBuilder
    {
        /// <summary>
        /// Creates a configured RabbitMQ <see cref="ConnectionFactory"/> from validated runtime options.
        /// </summary>
        /// <param name="options">Validated immutable RabbitMQ runtime options.</param>
        /// <param name="clientProvidedConnectionName">Client-provided connection name.</param>
        /// <returns>Configured RabbitMQ connection factory.</returns>
        internal static ConnectionFactory BuildConnectionFactory(
            RabbitMqRuntimeOptions options,
            string clientProvidedConnectionName)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentException.ThrowIfNullOrWhiteSpace(clientProvidedConnectionName);

            ConnectionFactory factory = new()
            {
                Port = options.Port,
                VirtualHost = options.VirtualHost,
                ClientProvidedName = clientProvidedConnectionName,
                AutomaticRecoveryEnabled = false,
                TopologyRecoveryEnabled = false,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(options.NetworkRecoveryIntervalSeconds),
                RequestedHeartbeat = TimeSpan.FromSeconds(options.RequestedHeartbeatSeconds),
                RequestedConnectionTimeout = TimeSpan.FromSeconds(options.ConnectionBlockedTimeoutSeconds),
                ContinuationTimeout = TimeSpan.FromSeconds(options.RpcTimeoutSeconds),
                HandshakeContinuationTimeout = TimeSpan.FromSeconds(options.RpcTimeoutSeconds),
                SocketReadTimeout = TimeSpan.FromSeconds(options.SocketTimeoutSeconds),
                SocketWriteTimeout = TimeSpan.FromSeconds(options.SocketTimeoutSeconds),
                RequestedChannelMax = (ushort)options.RequestedChannelMax,
            };

            if (!string.IsNullOrWhiteSpace(options.Username))
            {
                factory.UserName = options.Username;
            }

            if (!string.IsNullOrWhiteSpace(options.Password))
            {
                factory.Password = options.Password;
            }

            factory.Ssl.Enabled = options.EnableSsl;
            if (options.EnableSsl)
            {
                factory.Ssl.Version = SslProtocols.Tls12 | SslProtocols.Tls13;
                factory.Ssl.ServerName = options.Hosts.Count > 0 ? options.Hosts[0] : string.Empty;
            }

            return factory;
        }

        /// <summary>
        /// Creates endpoint host list consumed by RabbitMQ connection-creation APIs.
        /// </summary>
        /// <param name="options">Validated immutable RabbitMQ runtime options.</param>
        /// <returns>Distinct host list in connection preference order.</returns>
        internal static IReadOnlyList<string> BuildHostList(RabbitMqRuntimeOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            return options.Hosts;
        }

        /// <summary>
        /// Creates a sanitized configuration snapshot safe for logs and diagnostics.
        /// </summary>
        /// <param name="options">Validated immutable RabbitMQ runtime options.</param>
        /// <param name="clientProvidedConnectionName">Client-provided connection name.</param>
        /// <returns>Sanitized runtime snapshot.</returns>
        internal static RabbitMqConnectionFactorySnapshot BuildSanitizedSnapshot(
            RabbitMqRuntimeOptions options,
            string clientProvidedConnectionName)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentException.ThrowIfNullOrWhiteSpace(clientProvidedConnectionName);

            return new RabbitMqConnectionFactorySnapshot(
                Hosts: [.. options.Hosts],
                Port: options.Port,
                VirtualHost: options.VirtualHost,
                EnableSsl: options.EnableSsl,
                RequestedHeartbeatSeconds: options.RequestedHeartbeatSeconds,
                RequestedConnectionTimeoutSeconds: options.ConnectionBlockedTimeoutSeconds,
                RpcTimeoutSeconds: options.RpcTimeoutSeconds,
                SocketTimeoutSeconds: options.SocketTimeoutSeconds,
                RequestedChannelMax: options.RequestedChannelMax,
                AutomaticRecoveryEnabled: false,
                TopologyRecoveryEnabled: false,
                NetworkRecoveryIntervalSeconds: options.NetworkRecoveryIntervalSeconds,
                ClientProvidedConnectionName: clientProvidedConnectionName,
                UsesUsernameAuthentication: !string.IsNullOrWhiteSpace(options.Username),
                HasPassword: !string.IsNullOrWhiteSpace(options.Password));
        }
    }

    /// <summary>
    /// Sanitized RabbitMQ connection-factory settings safe for structured logs.
    /// </summary>
    /// <param name="Hosts">Configured broker host list.</param>
    /// <param name="Port">Configured broker port.</param>
    /// <param name="VirtualHost">Configured virtual host.</param>
    /// <param name="EnableSsl">Configured TLS mode.</param>
    /// <param name="RequestedHeartbeatSeconds">Requested heartbeat timeout.</param>
    /// <param name="RequestedConnectionTimeoutSeconds">Requested AMQP connection timeout.</param>
    /// <param name="RpcTimeoutSeconds">Requested AMQP continuation timeout.</param>
    /// <param name="SocketTimeoutSeconds">Socket read/write timeout.</param>
    /// <param name="RequestedChannelMax">Requested channel max.</param>
    /// <param name="AutomaticRecoveryEnabled">Whether RabbitMQ automatic recovery is enabled.</param>
    /// <param name="TopologyRecoveryEnabled">Whether RabbitMQ topology recovery is enabled.</param>
    /// <param name="NetworkRecoveryIntervalSeconds">Network recovery interval in seconds.</param>
    /// <param name="ClientProvidedConnectionName">Client-provided connection name.</param>
    /// <param name="UsesUsernameAuthentication">Whether username auth is configured.</param>
    /// <param name="HasPassword">Whether password is configured.</param>
    internal sealed record RabbitMqConnectionFactorySnapshot(
        IReadOnlyList<string> Hosts,
        int Port,
        string VirtualHost,
        bool EnableSsl,
        int RequestedHeartbeatSeconds,
        int RequestedConnectionTimeoutSeconds,
        int RpcTimeoutSeconds,
        int SocketTimeoutSeconds,
        int RequestedChannelMax,
        bool AutomaticRecoveryEnabled,
        bool TopologyRecoveryEnabled,
        int NetworkRecoveryIntervalSeconds,
        string ClientProvidedConnectionName,
        bool UsesUsernameAuthentication,
        bool HasPassword);
}
