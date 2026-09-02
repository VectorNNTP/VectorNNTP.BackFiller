// <copyright file="TransitServerDependencyProbe.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Validation
// Implements the transit server dependency probe behavior.

using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Serilog;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.Backfiller.Startup.Validation
{
    /// <summary>
    /// Defines transit server dependency probe and its transit server dependency probe contract.
    /// </summary>
    internal static class TransitServerDependencyProbe
    {
        /// <summary>
        /// Validates TransitServer endpoint reachability, NNTP greeting semantics, and streaming capability.
        /// </summary>
        /// <param name="backFiller">The backFiller value.</param>
        /// <param name="timeout">The timeout value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <typeparam name="DependencyValidationResult">The DependencyValidationResult type parameter.</typeparam>
        internal static async Task<DependencyValidationResult> ValidateTransitServerConnectivityAsync(
            BackFillerOptions? backFiller,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            List<(string Dependency, string Reason)> failures = [];
            List<(string Category, string Message)> warnings = [];
            List<(string Category, string Message)> errors = [];

            string? host = backFiller?.TransitServer?.Host?.Trim();
            int port = backFiller?.TransitServer?.Port ?? 0;
            bool useSsl = backFiller?.TransitServer?.UseSsl ?? false;

            if (string.IsNullOrWhiteSpace(host) || port <= 0)
            {
                return new DependencyValidationResult(failures, warnings, errors);
            }

            try
            {
                using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(timeout);

                using TcpClient client = new();
                await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);

                using NetworkStream networkStream = client.GetStream();

                if (useSsl)
                {
                    using SslStream sslStream = CreateStrictTlsStream(networkStream, leaveInnerStreamOpen: false);
                    await AuthenticateTlsAsync(sslStream, host, cts.Token).ConfigureAwait(false);
                    await ValidateTransitServerStreamingSessionAsync(sslStream, host, negotiateStartTls: false, cts.Token).ConfigureAwait(false);
                }
                else
                {
                    await ValidateTransitServerStreamingSessionAsync(networkStream, host, negotiateStartTls: true, cts.Token).ConfigureAwait(false);
                }

                Log.Information(
                    "TransitServer connectivity and stream-mode validation succeeded (Host: {Host}, Port: {Port}, UseSsl: {UseSsl})",
                    host,
                    port,
                    useSsl);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                failures.Add(("TransitServer", $"Connectivity validation timed out after {timeout.TotalSeconds:F1}s"));
            }
            catch (AuthenticationException)
            {
                failures.Add(("TransitServer", "Connectivity validation failed: TLS certificate or handshake validation failed"));
            }
            catch (SocketException ex)
            {
                failures.Add(("TransitServer", $"Connectivity validation failed: {RabbitMqDependencyProbe.GetSanitizedSocketFailureReason(ex.SocketErrorCode)}"));
            }
            catch (InvalidOperationException ex)
            {
                failures.Add(("TransitServer", $"Connectivity validation failed: {ex.Message}"));
            }
            catch (Exception ex)
            {
                Log.Debug(ex, "TransitServer connectivity validation threw an exception during startup dependency validation.");
                failures.Add(("TransitServer", "Connectivity validation failed: Unexpected connectivity failure"));
            }

            return new DependencyValidationResult(failures, warnings, errors);
        }

        /// <summary>
        /// Validates NNTP stream-mode semantics for a connected TransitServer session.
        /// </summary>
        /// <param name="stream">Connected transport stream.</param>
        /// <param name="host">Transit host for TLS target validation when STARTTLS is negotiated.</param>
        /// <param name="negotiateStartTls">Whether STARTTLS should be negotiated when advertised by CAPABILITIES.</param>
        /// <param name="cancellationToken">Cancellation token for network operations.</param>
        /// <returns>A task that completes when greeting, capability checks, MODE STREAM, and QUIT exchange succeed.</returns>
        internal static async Task ValidateTransitServerStreamingSessionAsync(
            Stream stream,
            string host,
            bool negotiateStartTls,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(host);

            Stream activeStream = stream;
            SslStream? startTlsStream = null;
            StreamReader reader = CreateReader(activeStream);
            StreamWriter writer = CreateWriter(activeStream);

            try
            {
                string greetingLine = await ReadNntpLineAsync(reader, cancellationToken).ConfigureAwait(false);
                TransitProtocolParser.ValidateGreeting(greetingLine);

                TransitCapabilitySnapshot capabilities = await ReadCapabilitiesAsync(reader, writer, cancellationToken).ConfigureAwait(false);

                if (negotiateStartTls && capabilities.SupportsStartTls)
                {
                    await WriteNntpCommandAsync(writer, "STARTTLS", cancellationToken).ConfigureAwait(false);
                    string startTlsResponse = await ReadNntpLineAsync(reader, cancellationToken).ConfigureAwait(false);
                    (int startTlsCode, string startTlsText) = TransitProtocolParser.ParseStatusCodeAndText(startTlsResponse);

                    if (startTlsCode != 382)
                    {
                        throw new InvalidOperationException($"STARTTLS negotiation rejected ({startTlsCode}): {startTlsText}");
                    }

                    startTlsStream = CreateStrictTlsStream(activeStream, leaveInnerStreamOpen: true);
                    await AuthenticateTlsAsync(startTlsStream, host, cancellationToken).ConfigureAwait(false);

                    reader.Dispose();
                    writer.Dispose();

                    activeStream = startTlsStream;
                    reader = CreateReader(activeStream);
                    writer = CreateWriter(activeStream);
                    capabilities = await ReadCapabilitiesAsync(reader, writer, cancellationToken).ConfigureAwait(false);
                }

                if (!capabilities.SupportsStreaming)
                {
                    throw new InvalidOperationException("Transit server does not advertise STREAM/STREAMING capability required for TAKETHIS publishing.");
                }

                await WriteNntpCommandAsync(writer, "MODE STREAM", cancellationToken).ConfigureAwait(false);
                string modeStreamResponse = await ReadNntpLineAsync(reader, cancellationToken).ConfigureAwait(false);
                (int modeStreamCode, string modeStreamText) = TransitProtocolParser.ParseStatusCodeAndText(modeStreamResponse);

                if (modeStreamCode != 203)
                {
                    throw new InvalidOperationException($"MODE STREAM rejected by transit server ({modeStreamCode}): {modeStreamText}");
                }

                await WriteNntpCommandAsync(writer, "QUIT", cancellationToken).ConfigureAwait(false);
                string quitResponse = await ReadNntpLineAsync(reader, cancellationToken).ConfigureAwait(false);
                (int quitCode, string quitText) = TransitProtocolParser.ParseStatusCodeAndText(quitResponse);

                if (quitCode != 205)
                {
                    throw new InvalidOperationException($"QUIT rejected by transit server ({quitCode}): {quitText}");
                }
            }
            finally
            {
                writer.Dispose();
                reader.Dispose();
                startTlsStream?.Dispose();
            }
        }

        /// <summary>
        /// Handles read capabilities async for transit server dependency probe.
        /// </summary>
        private static async Task<TransitCapabilitySnapshot> ReadCapabilitiesAsync(
            StreamReader reader,
            StreamWriter writer,
            CancellationToken cancellationToken)
        {
            await WriteNntpCommandAsync(writer, "CAPABILITIES", cancellationToken).ConfigureAwait(false);

            List<string> capabilityLines = [];
            while (true)
            {
                string line = await ReadNntpLineAsync(reader, cancellationToken).ConfigureAwait(false);
                capabilityLines.Add(line);

                if (line == ".")
                {
                    break;
                }
            }

            return TransitProtocolParser.ParseCapabilitiesResponse(capabilityLines);
        }

        /// <summary>
        /// Handles write nntp command async for transit server dependency probe.
        /// </summary>
        private static async Task WriteNntpCommandAsync(
            StreamWriter writer,
            string command,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(writer);
            ArgumentException.ThrowIfNullOrWhiteSpace(command);

            cancellationToken.ThrowIfCancellationRequested();
            await writer.WriteAsync(command.AsMemory(), cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync("\r\n".AsMemory(), cancellationToken).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Handles create reader for transit server dependency probe.
        /// </summary>
        private static StreamReader CreateReader(Stream stream)
        {
            return new StreamReader(stream, Encoding.ASCII, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        }

        /// <summary>
        /// Handles create writer for transit server dependency probe.
        /// </summary>
        private static StreamWriter CreateWriter(Stream stream)
        {
            return new StreamWriter(stream, Encoding.ASCII, leaveOpen: true)
            {
                NewLine = "\r\n",
                AutoFlush = false,
            };
        }

        /// <summary>
        /// Handles create strict tls stream for transit server dependency probe.
        /// </summary>
        private static SslStream CreateStrictTlsStream(Stream innerStream, bool leaveInnerStreamOpen)
        {
            return new SslStream(
                innerStream,
                leaveInnerStreamOpen,
                static (_, certificate, _, sslPolicyErrors) =>
                {
                    _ = certificate;
                    return sslPolicyErrors == SslPolicyErrors.None;
                });
        }

        /// <summary>
        /// Handles authenticate tls async for transit server dependency probe.
        /// </summary>
        private static async Task AuthenticateTlsAsync(SslStream sslStream, string host, CancellationToken cancellationToken)
        {
            SslClientAuthenticationOptions authOptions = new()
            {
                TargetHost = host,
                EnabledSslProtocols = SslProtocols.None,
            };

            await sslStream.AuthenticateAsClientAsync(authOptions, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads a single NNTP response line from the server with cancellation support.
        /// </summary>
        private static async Task<string> ReadNntpLineAsync(
            StreamReader reader,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(reader);

            string? line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            return line ?? throw new InvalidOperationException("Server closed connection unexpectedly during NNTP negotiation");
        }
    }
}
