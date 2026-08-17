using System.Net.Security;
using System.Net.Sockets;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Serilog;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    internal static class RabbitMqDependencyProbe
    {
        /// <summary>
        /// Validates RabbitMQ dependency health by opening AMQP connections/channels for each configured host.
        /// </summary>
        internal static async Task<DependencyValidationResult> ValidateRabbitMqConnectivityAsync(
            BackFillerOptions? backFiller,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            List<(string Dependency, string Reason)> failures = [];
            List<(string Category, string Message)> warnings = [];
            List<(string Category, string Message)> errors = [];

            RabbitMqOptions? rabbitMq = backFiller?.RabbitMQ;
            string[] hosts = [.. (rabbitMq?.Hosts ?? [])
                .Where(static x => !string.IsNullOrWhiteSpace(x))
                .Select(static x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)];
            int? port = rabbitMq?.Port;

            if (hosts.Length == 0 || port is null or <= 0)
            {
                // Configuration validation should catch this, but guard defensively.
                return new DependencyValidationResult(failures, warnings, errors);
            }

            ConnectionFactory connectionFactory = new()
            {
                Port = port.Value,
                AutomaticRecoveryEnabled = false,
            };

            if (!string.IsNullOrWhiteSpace(rabbitMq?.Username))
            {
                connectionFactory.UserName = rabbitMq.Username;
            }

            if (rabbitMq?.Password != null)
            {
                connectionFactory.Password = rabbitMq.Password;
            }

            if (!string.IsNullOrWhiteSpace(rabbitMq?.VirtualHost))
            {
                connectionFactory.VirtualHost = rabbitMq.VirtualHost;
            }

            connectionFactory.Ssl.Enabled = rabbitMq?.EnableSsl ?? false;

            foreach (string host in hosts)
            {
                try
                {
                    using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    cts.CancelAfter(timeout);

                    using RabbitMQ.Client.IConnection connection = await connectionFactory
                        .CreateConnectionAsync([host], cts.Token)
                        .ConfigureAwait(false);

                    using IChannel channel = await connection
                        .CreateChannelAsync(options: default, cancellationToken: cts.Token)
                        .ConfigureAwait(false);

                    Log.Information(
                        "RabbitMQ connectivity validated successfully (Host: {Host}, Port: {Port})",
                        host,
                        port.Value);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (OperationCanceledException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{port.Value} connection timed out after {timeout.TotalSeconds:F1}s"));
                }
                catch (AuthenticationFailureException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{port.Value} authentication failed"));
                }
                catch (PossibleAuthenticationFailureException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{port.Value} authentication failed"));
                }
                catch (BrokerUnreachableException ex) when (ex.InnerException is SocketException socketEx)
                {
                    failures.Add(("RabbitMQ", $"{host}:{port.Value} {GetSanitizedSocketFailureReason(socketEx.SocketErrorCode)}"));
                }
                catch (BrokerUnreachableException)
                {
                    failures.Add(("RabbitMQ", $"{host}:{port.Value} unable to establish AMQP connection"));
                }
                catch (Exception ex)
                {
                    Log.Debug(ex, "RabbitMQ connectivity validation threw an exception during startup dependency validation.");
                    failures.Add(("RabbitMQ", $"{host}:{port.Value} unexpected connectivity failure"));
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
