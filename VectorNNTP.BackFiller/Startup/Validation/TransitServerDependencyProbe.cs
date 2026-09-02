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
    /// Performs startup dependency checks against the configured transit server endpoint.
    /// </summary>
    /// <remarks>
    /// The probe validates TCP reachability, optional TLS client authentication, NNTP greeting parsing,
    /// CAPABILITIES negotiation, STREAM/STREAMING availability for TAKETHIS publishing, MODE STREAM acceptance,
    /// and graceful QUIT handling.
    /// </remarks>
    internal static class TransitServerDependencyProbe
    {
        /// <summary>
        /// Executes the transit-server startup probe and returns categorized validation failures.
        /// </summary>
        /// <param name="backFiller">Application configuration containing transit server host, port, and TLS mode.</param>
        /// <param name="timeout">Maximum probe duration before the linked timeout token cancels the operation.</param>
        /// <param name="cancellationToken">External cancellation token for startup shutdown/abort.</param>
        /// <returns>
        /// A validation result containing startup dependency failures. When host/port are not configured,
        /// the probe returns without adding failures.
        /// </returns>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken"/> is canceled.</exception>
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
        /// Validates NNTP streaming-session behavior over an already connected transport stream.
        /// </summary>
        /// <param name="stream">Connected network or TLS transport stream used for NNTP command exchange.</param>
        /// <param name="host">Target host name used when STARTTLS upgrades the session to TLS.</param>
        /// <param name="negotiateStartTls">
        /// <see langword="true"/> to negotiate STARTTLS when advertised by CAPABILITIES; otherwise validation continues
        /// on the current stream mode.
        /// </param>
        /// <param name="cancellationToken">Cancellation token for line reads, command writes, and optional TLS upgrade.</param>
        /// <returns>A task that completes after greeting, capability checks, MODE STREAM, and QUIT all succeed.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="stream"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="host"/> is null, empty, or whitespace.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when STARTTLS, STREAM/STREAMING capability requirements, MODE STREAM, or QUIT responses are invalid.
        /// </exception>
        /// <remarks>
        /// When STARTTLS is negotiated successfully, this method rebuilds its reader/writer over the upgraded TLS stream
        /// and re-issues CAPABILITIES to validate post-upgrade server capabilities.
        /// </remarks>
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
            StreamReader? reader = null;
            StreamWriter? writer = null;

            try
            {
                reader = CreateReader(activeStream);
                writer = CreateWriter(activeStream);

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
                writer?.Dispose();
                reader?.Dispose();
                startTlsStream?.Dispose();
            }
        }

        /// <summary>
        /// Sends CAPABILITIES and parses the multi-line response into a capability snapshot.
        /// </summary>
        /// <param name="reader">Reader used to consume capability response lines.</param>
        /// <param name="writer">Writer used to send the CAPABILITIES command.</param>
        /// <param name="cancellationToken">Cancellation token for command and response I/O.</param>
        /// <returns>The parsed transit capability snapshot.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the server does not terminate the capability listing within the configured safety bound.
        /// </exception>
        private static async Task<TransitCapabilitySnapshot> ReadCapabilitiesAsync(
            StreamReader reader,
            StreamWriter writer,
            CancellationToken cancellationToken)
        {
            const int MaxCapabilityLines = 1024;

            await WriteNntpCommandAsync(writer, "CAPABILITIES", cancellationToken).ConfigureAwait(false);

            List<string> capabilityLines = [];
            for (int i = 0; i < MaxCapabilityLines; i++)
            {
                string line = await ReadNntpLineAsync(reader, cancellationToken).ConfigureAwait(false);
                capabilityLines.Add(line);

                if (line == ".")
                {
                    return TransitProtocolParser.ParseCapabilitiesResponse(capabilityLines);
                }
            }

            throw new InvalidOperationException(
                $"Transit server returned more than {MaxCapabilityLines} capability lines without terminating '.'.");
        }

        /// <summary>
        /// Writes a single NNTP command line using ASCII encoding and CRLF termination.
        /// </summary>
        /// <param name="writer">Writer bound to the active transport stream.</param>
        /// <param name="command">NNTP command verb and arguments without line termination.</param>
        /// <param name="cancellationToken">Cancellation token for write and flush operations.</param>
        /// <returns>A task that completes after the command line has been flushed to the stream.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="writer"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="command"/> is null, empty, or whitespace.</exception>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken"/> is canceled.</exception>
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
        /// Creates an ASCII stream reader for NNTP response parsing without BOM detection.
        /// </summary>
        /// <param name="stream">Underlying transport stream to read from.</param>
        /// <returns>A reader that leaves the underlying stream open when disposed.</returns>
        private static StreamReader CreateReader(Stream stream)
        {
            return new StreamReader(stream, Encoding.ASCII, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        }

        /// <summary>
        /// Creates an ASCII stream writer configured for explicit flush control and CRLF line semantics.
        /// </summary>
        /// <param name="stream">Underlying transport stream to write to.</param>
        /// <returns>A writer that leaves the underlying stream open when disposed.</returns>
        private static StreamWriter CreateWriter(Stream stream)
        {
            return new StreamWriter(stream, Encoding.ASCII, leaveOpen: true)
            {
                NewLine = "\r\n",
                AutoFlush = false,
            };
        }

        /// <summary>
        /// Wraps a transport stream in an <see cref="SslStream"/> that enforces strict policy-error validation.
        /// </summary>
        /// <param name="innerStream">Transport stream to wrap for TLS client authentication.</param>
        /// <param name="leaveInnerStreamOpen">
        /// Whether disposing the returned <see cref="SslStream"/> should keep <paramref name="innerStream"/> open.
        /// </param>
        /// <returns>An SSL stream configured to reject certificates with any <see cref="SslPolicyErrors"/> value.</returns>
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
        /// Authenticates the client side of the TLS session using this probe's transport security policy.
        /// </summary>
        /// <param name="sslStream">TLS stream to authenticate as a client.</param>
        /// <param name="host">Target host name used for SNI and certificate host validation.</param>
        /// <param name="cancellationToken">Cancellation token for the TLS handshake operation.</param>
        /// <returns>A task that completes after TLS client authentication succeeds.</returns>
        private static async Task AuthenticateTlsAsync(SslStream sslStream, string host, CancellationToken cancellationToken)
        {
            SslClientAuthenticationOptions authOptions = CreateTlsClientAuthenticationOptions(host);
            await sslStream.AuthenticateAsClientAsync(authOptions, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Builds TLS client-authentication options for transit-server probe handshakes.
        /// </summary>
        /// <param name="host">Target host name used by TLS client authentication.</param>
        /// <returns>Authentication options restricted to TLS 1.2 and TLS 1.3.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="host"/> is null, empty, or whitespace.</exception>
        internal static SslClientAuthenticationOptions CreateTlsClientAuthenticationOptions(string host)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(host);

            return new SslClientAuthenticationOptions
            {
                TargetHost = host,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            };
        }

        /// <summary>
        /// Reads one NNTP response line and fails fast if the remote endpoint closes unexpectedly.
        /// </summary>
        /// <param name="reader">Reader bound to the active transit session stream.</param>
        /// <param name="cancellationToken">Cancellation token for asynchronous line read.</param>
        /// <returns>The next NNTP response line from the server.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="reader"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the server closes the connection before returning a line.</exception>
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
