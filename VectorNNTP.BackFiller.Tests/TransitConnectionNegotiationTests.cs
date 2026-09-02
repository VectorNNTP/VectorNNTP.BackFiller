// <copyright file="TransitConnectionNegotiationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit connection negotiation, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit connection negotiation test suite.

using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.Backfiller.Tests.TestInfrastructure;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests transit connection protocol negotiation behavior.
    /// </summary>
    public sealed class TransitConnectionNegotiationTests : IClassFixture<TestTlsCertificateFixture>
    {
        /// <summary>
        /// Supplies  tls fixture for the fixture or scenario under test.
        /// </summary>
        private readonly TestTlsCertificateFixture _tlsFixture;

        /// <summary>
        /// Confirms the transit connection negotiation tests behavior.
        /// </summary>
        public TransitConnectionNegotiationTests(TestTlsCertificateFixture tlsFixture)
        {
            ArgumentNullException.ThrowIfNull(tlsFixture);
            _tlsFixture = tlsFixture;
        }
        /// <summary>
        /// Confirms the initialize async when plain streaming capabilities reaches ready behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenPlainStreamingCapabilities_ReachesReady()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "VERSION 2");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");
                await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                await ServeGracefulShutdownAsync(stream, _);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
            Assert.False(connection.IsTlsActive);
            Assert.True(connection.Capabilities.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the initialize async when plain stream capability alias reaches ready behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenPlainStreamCapabilityAlias_ReachesReady()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "VERSION 2");
                await FakeNntpServer.WriteLineAsync(stream, "STREAM");
                await FakeNntpServer.WriteLineAsync(stream, ".");
                await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                await ServeGracefulShutdownAsync(stream, _);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            await connection.InitializeAsync(CancellationToken.None);

            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
            Assert.True(connection.Capabilities.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the dispose async when disposed immediately after ready does not throw from write loop startup race behavior.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenDisposedImmediatelyAfterReady_DoesNotThrowFromWriteLoopStartupRace()
        {
            /// <summary>
            /// Supplies iterations for the fixture or scenario under test.
            /// </summary>
            const int Iterations = 100;

            for (int iteration = 0; iteration < Iterations; iteration++)
            {
                await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
                {
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakeNntpServer.WriteLineAsync(stream, "VERSION 2");
                    await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                    await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                    await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                    await ServeGracefulShutdownAsync(stream, cancellationToken);
                });

                await using TransitConnection connection = new(
                    host: IPAddress.Loopback.ToString(),
                    port: server.Port,
                    useSsl: false,
                    NullLogger<TransitPublisher>.Instance);

                await connection.InitializeAsync(CancellationToken.None);
                Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);

                Exception? disposeException = await Record.ExceptionAsync(async () => await connection.DisposeAsync());
                Assert.Null(disposeException);
                Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);
            }
        }
        /// <summary>
        /// Confirms the initialize async when initialization token canceled after ready response loop continues processing behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenInitializationTokenCanceledAfterReady_ResponseLoopContinuesProcessing()
        {
            string messageId = "<token-cancel-after-ready@example.com>";
            byte[] payload = [(byte)'R', (byte)'\n'];

            TaskCompletionSource publishObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");
                await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethis = await FakeNntpServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal($"TAKETHIS {messageId}", takethis);
                byte[] receivedPayload = await FakeNntpServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(payload, receivedPayload);

                _ = publishObserved.TrySetResult();
                using CancellationTokenSource serverTimeout = new(TimeSpan.FromSeconds(10));
                using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, serverTimeout.Token);
                await allowResponse.Task.WaitAsync(linked.Token);

                await FakeNntpServer.WriteLineAsync(stream, $"239 {messageId} transferred");
                await ServeGracefulShutdownAsync(stream, cancellationToken);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            using CancellationTokenSource initCts = new();
            await connection.InitializeAsync(initCts.Token);
            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
            initCts.Cancel();
            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);

            Task<TransitPublishResult> publishTask = connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L).AsTask();

            using CancellationTokenSource observeTimeout = new(TimeSpan.FromSeconds(10));
            await publishObserved.Task.WaitAsync(observeTimeout.Token);

            _ = allowResponse.TrySetResult();

            using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
            TransitPublishResult result = await publishTask.WaitAsync(completionTimeout.Token);

            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
            Assert.Equal(239, result.ResponseCode);
            Assert.Equal(messageId, result.MessageId);
            Assert.NotEqual(TransitConnectionState.Faulted, connection.CurrentState);
            Assert.NotEqual(TransitConnectionState.Disconnected, connection.CurrentState);
        }
        /// <summary>
        /// Confirms the initialize async when start tls advertised upgrades to tls and renegotiates capabilities behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenStartTlsAdvertised_UpgradesToTlsAndRenegotiatesCapabilities()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "STARTTLS");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "STARTTLS");
                await FakeNntpServer.WriteLineAsync(stream, "382 Continue with TLS negotiation");

                SslStream sslStream = new(stream, leaveInnerStreamOpen: false);
                SslServerAuthenticationOptions serverOptions = new()
                {
                    ServerCertificate = _tlsFixture.ServerCertificate,
                    ClientCertificateRequired = false,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                };

                await sslStream.AuthenticateAsServerAsync(serverOptions, cancellationToken);

                await FakeNntpServer.ExpectCommandAsync(sslStream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(sslStream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(sslStream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(sslStream, ".");

                await FakeNntpServer.ExpectCommandAsync(sslStream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(sslStream, "203 Streaming permitted");
                await ServeGracefulShutdownAsync(sslStream, cancellationToken);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance,
                _tlsFixture.ServerCertificateValidationCallback);

            await connection.InitializeAsync(CancellationToken.None);

            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
            Assert.True(connection.IsTlsActive);
            Assert.True(connection.Capabilities.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the initialize async when streaming not advertised throws behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenStreamingNotAdvertised_Throws()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "VERSION 2");
                await FakeNntpServer.WriteLineAsync(stream, ".");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));

            Assert.Contains("STREAMING capability", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the initialize async when capabilities response code unexpected throws behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenCapabilitiesResponseCodeUnexpected_Throws()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "500 command not recognized");
                await FakeNntpServer.WriteLineAsync(stream, ".");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Contains("Unexpected CAPABILITIES response code", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the initialize async when mode stream rejected throws behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenModeStreamRejected_Throws()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");
                await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(stream, "501 streaming unavailable");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Contains("Unexpected MODE STREAM response code", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the initialize async when compress deflate advertised does not negotiate compression behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenCompressDeflateAdvertised_DoesNotNegotiateCompression()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");
                await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethis = await FakeNntpServer.ReadLineAsync(stream, cancellationToken);
                Assert.Equal("TAKETHIS <compress-ignored@example.com>", takethis);
                byte[] payload = await FakeNntpServer.ReadTakethisPayloadAsync(stream, cancellationToken);
                Assert.Equal(new byte[] { (byte)'N', (byte)'\n' }, payload);
                await FakeNntpServer.WriteLineAsync(stream, "239 <compress-ignored@example.com> transferred");
                await ServeGracefulShutdownAsync(stream, cancellationToken);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync("<compress-ignored@example.com>", new byte[] { (byte)'N', (byte)'\n' }, CancellationToken.None, 0L, 0L);

            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
            Assert.True(connection.Capabilities.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the initialize async when greeting unexpected throws behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenGreetingUnexpected_Throws()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "400 temporary unavailable");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Contains("Unexpected NNTP greeting response code", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the initialize async when server closes during greeting throws behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenServerClosesDuringGreeting_Throws()
        {
            TaskCompletionSource<(string? ServerLocalEndpoint, string? ClientLocalEndpoint)> endpointCapture = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeNntpServer server = await FakeNntpServer.StartAsync((stream, cancellationToken) =>
            {
                _ = endpointCapture.TrySetResult(
                    (stream.Socket.LocalEndPoint?.ToString(), stream.Socket.RemoteEndPoint?.ToString()));
                stream.Dispose();
                return Task.CompletedTask;
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Contains("closed while awaiting line response", ex.Message, StringComparison.OrdinalIgnoreCase);

            (string? serverLocalEndpoint, string? clientLocalEndpoint) = await endpointCapture.Task;
            string listenerLocalEndpoint = GetListenerLocalEndpoint(server) ?? string.Empty;
            string clientRemoteEndpoint = serverLocalEndpoint ?? listenerLocalEndpoint;
            string clientEndpoint = clientLocalEndpoint ?? string.Empty;

            string diagnosticLine = $"P1-TCP-ENDPOINT: listener={listenerLocalEndpoint} clientLocal={clientEndpoint} clientRemote={clientRemoteEndpoint} tuple={clientEndpoint}->{clientRemoteEndpoint}";
            Console.WriteLine(diagnosticLine);

            string artifactsDirectory = ResolveArtifactsDirectory();
            _ = Directory.CreateDirectory(artifactsDirectory);
            string diagnosticPath = Path.Combine(artifactsDirectory, "phase2-p1-greeting-test-endpoint-diag.txt");
            File.WriteAllText(diagnosticPath, diagnosticLine + Environment.NewLine);

            /// <summary>
            /// Confirms get listener local endpoint behavior.
            /// </summary>
            static string? GetListenerLocalEndpoint(FakeNntpServer serverInstance)
            {
                FieldInfo? listenerField = typeof(FakeNntpServer).GetField("_listener", BindingFlags.Instance | BindingFlags.NonPublic);
                TcpListener? listener = listenerField?.GetValue(serverInstance) as TcpListener;
                return listener?.LocalEndpoint?.ToString();
            }

            /// <summary>
            /// Confirms resolve artifacts directory behavior.
            /// </summary>
            static string ResolveArtifactsDirectory()
            {
                DirectoryInfo? current = new(AppContext.BaseDirectory);
                while (current is not null)
                {
                    string candidate = Path.Combine(current.FullName, "artifacts");
                    if (Directory.Exists(candidate))
                    {
                        return candidate;
                    }

                    current = current.Parent;
                }

                return Path.Combine(AppContext.BaseDirectory, "artifacts");
            }
        }
        /// <summary>
        /// Confirms the initialize async when server closes during capabilities throws behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenServerClosesDuringCapabilities_Throws()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                stream.Dispose();
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Contains("closed while awaiting line response", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Confirms the initialize async when use ssl true negotiation runs over tls behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenUseSslTrue_NegotiationRunsOverTls()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                SslStream sslStream = new(stream, leaveInnerStreamOpen: false);
                SslServerAuthenticationOptions serverOptions = new()
                {
                    ServerCertificate = _tlsFixture.ServerCertificate,
                    ClientCertificateRequired = false,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                };

                await sslStream.AuthenticateAsServerAsync(serverOptions, cancellationToken);

                await FakeNntpServer.WriteLineAsync(sslStream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(sslStream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(sslStream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(sslStream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(sslStream, ".");
                await FakeNntpServer.ExpectCommandAsync(sslStream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(sslStream, "203 Streaming permitted");
                await ServeGracefulShutdownAsync(sslStream, cancellationToken);
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: true,
                NullLogger<TransitPublisher>.Instance,
                _tlsFixture.ServerCertificateValidationCallback);

            await connection.InitializeAsync(CancellationToken.None);

            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
            Assert.True(connection.IsTlsActive);
            Assert.True(connection.Capabilities.SupportsStreaming);
        }
        /// <summary>
        /// Confirms the initialize async when use ssl true and compression advertised uses tls without compression behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenUseSslTrueAndCompressionAdvertised_UsesTlsWithoutCompression()
        {
            byte[] payload =
            [
                0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF,
                (byte)'\r', (byte)'\n', (byte)'.', (byte)'.', (byte)'X', (byte)'\n',
                (byte)'.', (byte)'L', (byte)'e', (byte)'a', (byte)'d', (byte)'\n',
            ];

            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                SslStream sslStream = new(stream, leaveInnerStreamOpen: false);
                SslServerAuthenticationOptions serverOptions = new()
                {
                    ServerCertificate = _tlsFixture.ServerCertificate,
                    ClientCertificateRequired = false,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                };

                await sslStream.AuthenticateAsServerAsync(serverOptions, cancellationToken);

                await FakeNntpServer.WriteLineAsync(sslStream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(sslStream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(sslStream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(sslStream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(sslStream, ".");

                await FakeNntpServer.ExpectCommandAsync(sslStream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(sslStream, "203 Streaming permitted");

                string takethis = await FakeNntpServer.ReadLineAsync(sslStream, cancellationToken);
                Assert.Equal("TAKETHIS <ssl-uncompressed@example.com>", takethis);
                byte[] receivedPayload = await FakeNntpServer.ReadTakethisPayloadAsync(sslStream, cancellationToken);
                Assert.Equal(payload, receivedPayload);
                await FakeNntpServer.WriteLineAsync(sslStream, "239 <ssl-uncompressed@example.com> transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: true,
                NullLogger<TransitPublisher>.Instance,
                _tlsFixture.ServerCertificateValidationCallback);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync("<ssl-uncompressed@example.com>", payload, CancellationToken.None, 0L, 0L);

            Assert.True(connection.IsTlsActive);
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        }
        /// <summary>
        /// Confirms the initialize async when use ssl true and streaming not advertised throws behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenUseSslTrueAndStreamingNotAdvertised_Throws()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                SslStream sslStream = new(stream, leaveInnerStreamOpen: false);
                SslServerAuthenticationOptions serverOptions = new()
                {
                    ServerCertificate = _tlsFixture.ServerCertificate,
                    ClientCertificateRequired = false,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                };

                await sslStream.AuthenticateAsServerAsync(serverOptions, cancellationToken);

                await FakeNntpServer.WriteLineAsync(sslStream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(sslStream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(sslStream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(sslStream, "VERSION 2");
                await FakeNntpServer.WriteLineAsync(sslStream, ".");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: true,
                NullLogger<TransitPublisher>.Instance,
                _tlsFixture.ServerCertificateValidationCallback);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Contains("STREAMING capability", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the initialize async when start tls advertised with compression upgrades to tls without compression behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenStartTlsAdvertisedWithCompression_UpgradesToTlsWithoutCompression()
        {
            byte[] payload =
            [
                0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF,
                (byte)'\r', (byte)'\n', (byte)'.', (byte)'.', (byte)'Y', (byte)'\n',
                (byte)'.', (byte)'D', (byte)'o', (byte)'t', (byte)'\n',
            ];

            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "STARTTLS");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "STARTTLS");
                await FakeNntpServer.WriteLineAsync(stream, "382 Continue with TLS negotiation");

                SslStream sslStream = new(stream, leaveInnerStreamOpen: false);
                SslServerAuthenticationOptions serverOptions = new()
                {
                    ServerCertificate = _tlsFixture.ServerCertificate,
                    ClientCertificateRequired = false,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                };

                await sslStream.AuthenticateAsServerAsync(serverOptions, cancellationToken);

                await FakeNntpServer.ExpectCommandAsync(sslStream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(sslStream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(sslStream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(sslStream, ".");

                await FakeNntpServer.ExpectCommandAsync(sslStream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(sslStream, "203 Streaming permitted");

                string takethis = await FakeNntpServer.ReadLineAsync(sslStream, cancellationToken);
                Assert.Equal("TAKETHIS <starttls-uncompressed@example.com>", takethis);
                byte[] receivedPayload = await FakeNntpServer.ReadTakethisPayloadAsync(sslStream, cancellationToken);
                Assert.Equal(payload, receivedPayload);
                await FakeNntpServer.WriteLineAsync(sslStream, "239 <starttls-uncompressed@example.com> transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance,
                _tlsFixture.ServerCertificateValidationCallback);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync("<starttls-uncompressed@example.com>", payload, CancellationToken.None, 0L, 0L);

            Assert.True(connection.IsTlsActive);
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        }
        /// <summary>
        /// Confirms the initialize async when server closes during tls handshake throws authentication or io exception behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenServerClosesDuringTlsHandshake_ThrowsAuthenticationOrIoException()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, cancellationToken) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "STARTTLS");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "STARTTLS");
                await FakeNntpServer.WriteLineAsync(stream, "382 Continue with TLS negotiation");

                stream.Dispose();
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance,
                _tlsFixture.ServerCertificateValidationCallback);

            Exception ex = await Record.ExceptionAsync(() => connection.InitializeAsync(CancellationToken.None));
            Assert.NotNull(ex);
            Assert.True(ex is AuthenticationException or IOException);
        }
        /// <summary>
        /// Confirms the initialize async when use ssl true and server closes during tls handshake throws authentication or io exception behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenUseSslTrueAndServerClosesDuringTlsHandshake_ThrowsAuthenticationOrIoException()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync((stream, cancellationToken) =>
            {
                stream.Dispose();
                return Task.CompletedTask;
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: true,
                NullLogger<TransitPublisher>.Instance,
                _tlsFixture.ServerCertificateValidationCallback);

            Exception ex = await Record.ExceptionAsync(() => connection.InitializeAsync(CancellationToken.None));
            Assert.NotNull(ex);
            Assert.True(ex is AuthenticationException or IOException);
        }
        /// <summary>
        /// Confirms the initialize async when compression advertised publishes over uncompressed transport behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenCompressionAdvertised_PublishesOverUncompressedTransport()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");

                string takethis = await FakeNntpServer.ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal("TAKETHIS <uncompressed@example.com>", takethis);

                byte[] payload = await FakeNntpServer.ReadTakethisPayloadAsync(stream, CancellationToken.None);
                Assert.Equal([(byte)'Z', (byte)'\n'], payload);

                await FakeNntpServer.WriteLineAsync(stream, "239 <uncompressed@example.com> transferred");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            await connection.InitializeAsync(CancellationToken.None);
            TransitPublishResult result = await connection.SubmitTakethisAsync("<uncompressed@example.com>", new byte[] { (byte)'Z', (byte)'\n' }, CancellationToken.None, 0L, 0L);

            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
            Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        }
        /// <summary>
        /// Confirms the initialize async when capabilities failure then retry on same instance succeeds with fresh transport behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenCapabilitiesFailureThenRetryOnSameInstance_SucceedsWithFreshTransport()
        {
            int sessionCount = 0;

            await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
            [
                async (stream, _) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "500 command not recognized");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                },
                async (stream, cancellationToken) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                    await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                    await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                    await ServeGracefulShutdownAsync(stream, cancellationToken);
                },
            ]);

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            _ = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);

            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
            await connection.InitializeAsync(timeout.Token);

            Assert.Equal(2, sessionCount);
            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
        }
        /// <summary>
        /// Confirms the initialize async when mode stream rejected then retry on same instance succeeds with fresh transport behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenModeStreamRejectedThenRetryOnSameInstance_SucceedsWithFreshTransport()
        {
            int sessionCount = 0;

            await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
            [
                async (stream, _) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                    await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                    await FakeNntpServer.WriteLineAsync(stream, "501 streaming unavailable");
                },
                async (stream, cancellationToken) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                    await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                    await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                    await ServeGracefulShutdownAsync(stream, cancellationToken);
                },
            ]);

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            _ = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);

            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
            await connection.InitializeAsync(timeout.Token);

            Assert.Equal(2, sessionCount);
            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
        }
        /// <summary>
        /// Confirms the initialize async when only compress advertised with no streaming throws streaming required behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenOnlyCompressAdvertisedWithNoStreaming_ThrowsStreamingRequired()
        {
            await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
            {
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, ".");
            });

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
            Assert.Contains("does not advertise STREAMING capability", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the initialize async when start tls handshake fails then retry on same instance succeeds with fresh transport behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenStartTlsHandshakeFailsThenRetryOnSameInstance_SucceedsWithFreshTransport()
        {
            int sessionCount = 0;

            await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
            [
                async (stream, _) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakeNntpServer.WriteLineAsync(stream, "STARTTLS");
                    await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                    await FakeNntpServer.ExpectCommandAsync(stream, "STARTTLS");
                    await FakeNntpServer.WriteLineAsync(stream, "382 Continue with TLS negotiation");
                    stream.Dispose();
                },
                async (stream, cancellationToken) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                    await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                    await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                    await ServeGracefulShutdownAsync(stream, cancellationToken);
                },
            ]);

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            Exception firstAttempt = await Record.ExceptionAsync(() => connection.InitializeAsync(CancellationToken.None));
            Assert.NotNull(firstAttempt);
            Assert.True(firstAttempt is IOException or AuthenticationException);
            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);

            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
            await connection.InitializeAsync(timeout.Token);

            Assert.Equal(2, sessionCount);
            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
        }
        /// <summary>
        /// Confirms the initialize async when canceled during greeting then retry on same instance succeeds with fresh transport behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenCanceledDuringGreetingThenRetryOnSameInstance_SucceedsWithFreshTransport()
        {
            int sessionCount = 0;

            await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
            [
                async (stream, cancellationToken) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    byte[] buffer = new byte[1];
                    _ = await stream.ReadAsync(buffer, cancellationToken);
                },
                async (stream, cancellationToken) =>
                {
                    Interlocked.Increment(ref sessionCount);
                    await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                    await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                    await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                    await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                    await FakeNntpServer.WriteLineAsync(stream, ".");
                    await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                    await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                    await ServeGracefulShutdownAsync(stream, cancellationToken);
                },
            ]);

            await using TransitConnection connection = new(
                host: IPAddress.Loopback.ToString(),
                port: server.Port,
                useSsl: false,
                NullLogger<TransitPublisher>.Instance);

            using (CancellationTokenSource canceledInit = new(TimeSpan.FromMilliseconds(500)))
            {
                _ = await Assert.ThrowsAsync<OperationCanceledException>(() => connection.InitializeAsync(canceledInit.Token));
            }

            Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);

            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
            await connection.InitializeAsync(timeout.Token);

            Assert.Equal(2, sessionCount);
            Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
        }

        /// <summary>
        /// Continues servicing a ready fake NNTP session until the client performs graceful QUIT shutdown.
        /// </summary>
        /// <returns>The value returned by the serve graceful shutdown async helper.</returns>
        /// <summary>
        /// Confirms the serve graceful shutdown async behavior.
        /// </summary>
        /// <param name="stream">The stream used by this test scenario.</param>
        /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
        /// <returns>The value returned by the serve graceful shutdown async helper.</returns>
        private static async Task ServeGracefulShutdownAsync(Stream stream, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(stream);

            string command = await FakeNntpServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal("QUIT", command);
            await FakeNntpServer.WriteLineAsync(stream, "205 connection closing");
        }


        /// <summary>
        /// Confirms the fake nntp server behavior.
        /// </summary>
        private sealed class FakeNntpServer : IAsyncDisposable
        {
            /// <summary>
            /// Supplies  listener for the fixture or scenario under test.
            /// </summary>
            private readonly TcpListener _listener;
            /// <summary>
            /// Supplies  sessions for the fixture or scenario under test.
            /// </summary>
            private readonly IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> _sessions;
            /// <summary>
            /// Confirms  cts behavior.
            /// </summary>
            private readonly CancellationTokenSource _cts = new();
            /// <summary>
            /// Supplies  accept loop for the fixture or scenario under test.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
            /// Confirms the fake nntp server behavior.
            /// </summary>
            private FakeNntpServer(TcpListener listener, IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> sessions)
            {
                _listener = listener;
                _sessions = sessions;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Confirms port behavior.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <returns>The value returned by the start async helper.</returns>
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <param name="NetworkStream">The network stream used by this test scenario.</param>
            /// <param name="CancellationToken">The cancellation token used by this test scenario.</param>
            /// <param name="session">The session used by this test scenario.</param>
            /// <returns>The value returned by the start async helper.</returns>
            internal static async Task<FakeNntpServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
            {
                ArgumentNullException.ThrowIfNull(session);
                return await StartSessionsAsync([session]);
            }

            /// <summary>
            /// Confirms the start sessions async behavior.
            /// </summary>
            /// <returns>The value returned by the start sessions async helper.</returns>
            /// <summary>
            /// Confirms the start sessions async behavior.
            /// </summary>
            /// <param name="NetworkStream">The network stream used by this test scenario.</param>
            /// <param name="CancellationToken">The cancellation token used by this test scenario.</param>
            /// <param name="sessions">The sessions used by this test scenario.</param>
            /// <returns>The value returned by the start sessions async helper.</returns>
            internal static async Task<FakeNntpServer> StartSessionsAsync(IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> sessions)
            {
                ArgumentNullException.ThrowIfNull(sessions);

                if (sessions.Count == 0)
                {
                    throw new ArgumentException("At least one fake NNTP session is required.", nameof(sessions));
                }

                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                FakeNntpServer server = new(listener, sessions);
                await Task.Delay(20);
                return server;
            }

            /// <summary>
            /// Confirms the accept loop async behavior.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            /// <summary>
            /// Confirms the accept loop async behavior.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            private async Task AcceptLoopAsync()
            {
                try
                {
                    for (int i = 0; i < _sessions.Count; i++)
                    {
                        using TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token);
                        using NetworkStream stream = client.GetStream();
                        await _sessions[i](stream, _cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                }
            }

            /// <summary>
            /// Confirms the read line async behavior.
            /// </summary>
            /// <returns>The value returned by the read line async helper.</returns>
            /// <summary>
            /// Confirms the read line async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the read line async helper.</returns>
            internal static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> buffer = [];

                while (true)
                {
                    byte current = await ReadByteAsync(stream, cancellationToken);
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

                return System.Text.Encoding.ASCII.GetString([.. buffer]);
            }

            /// <summary>
            /// Confirms the read byte async behavior.
            /// </summary>
            /// <returns>The value returned by the read byte async helper.</returns>
            /// <summary>
            /// Confirms the read byte async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the read byte async helper.</returns>
            private static async ValueTask<byte> ReadByteAsync(Stream stream, CancellationToken cancellationToken)
            {
                byte[] single = new byte[1];
                int read = await stream.ReadAsync(single, cancellationToken);
                return read == 0 ? throw new InvalidOperationException("Unexpected EOF while reading stream data.") : single[0];
            }

            /// <summary>
            /// Confirms the read takethis payload async behavior.
            /// </summary>
            /// <returns>The value returned by the read takethis payload async helper.</returns>
            /// <summary>
            /// Confirms the read takethis payload async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the read takethis payload async helper.</returns>
            internal static async Task<byte[]> ReadTakethisPayloadAsync(Stream stream, CancellationToken cancellationToken)
            {
                using MemoryStream payload = new();
                bool atLineStart = true;

                while (true)
                {
                    byte current = await ReadByteAsync(stream, cancellationToken);

                    if (atLineStart && current == (byte)'.')
                    {
                        byte next = await ReadByteAsync(stream, cancellationToken);
                        if (next == (byte)'\r')
                        {
                            byte nextNext = await ReadByteAsync(stream, cancellationToken);
                            if (nextNext == (byte)'\n')
                            {
                                break;
                            }

                            await payload.WriteAsync(new byte[] { current, next, nextNext }, cancellationToken);
                            atLineStart = false;
                            continue;
                        }

                        await payload.WriteAsync(new byte[] { next }, cancellationToken);
                        atLineStart = next == (byte)'\n';
                        continue;
                    }

                    await payload.WriteAsync(new byte[] { current }, cancellationToken);

                    if (current == (byte)'\n')
                    {
                        atLineStart = true;
                    }
                    else if (current != (byte)'\r')
                    {
                        atLineStart = false;
                    }
                }

                return payload.ToArray();
            }

            /// <summary>
            /// Confirms the expect command async behavior.
            /// </summary>
            /// <returns>The value returned by the expect command async helper.</returns>
            /// <summary>
            /// Confirms the expect command async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="expected">The expected used by this test scenario.</param>
            /// <returns>The value returned by the expect command async helper.</returns>
            internal static async Task ExpectCommandAsync(Stream stream, string expected)
            {
                string line = await ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal(expected, line);
            }

            /// <summary>
            /// Confirms the write line async behavior.
            /// </summary>
            /// <returns>The value returned by the write line async helper.</returns>
            /// <summary>
            /// Confirms the write line async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="line">The line used by this test scenario.</param>
            /// <returns>The value returned by the write line async helper.</returns>
            internal static async Task WriteLineAsync(Stream stream, string line)
            {
                byte[] bytes = System.Text.Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, CancellationToken.None);
                await stream.FlushAsync(CancellationToken.None);
            }

            /// <summary>
            /// Confirms the capturing logger provider behavior.
            /// </summary>
            internal sealed class CapturingLoggerProvider
            {
                /// <summary>
                /// Confirms  gate behavior.
                /// </summary>
                private readonly object _gate = new();

                /// <summary>
                /// Supplies entries for the fixture or scenario under test.
                /// </summary>
                internal List<LogEntry> Entries { get; } = [];

                internal ILogger<T> CreateLogger<T>()
                {
                    return new CapturingLogger<T>(Entries, _gate);
                }

                /// <summary>
                /// Confirms the log entry behavior.
                /// </summary>
                /// <returns>The value returned by the log entry helper.</returns>
                /// <summary>
                /// Confirms the log entry behavior.
                /// </summary>
                /// <param name="EventId">The event id used by this test scenario.</param>
                /// <param name="Message">The message used by this test scenario.</param>
                /// <returns>The value returned by the log entry helper.</returns>
                internal sealed record LogEntry(EventId EventId, string Message);

                /// <summary>
                /// Confirms the capturing logger behavior.
                /// </summary>
                private sealed class CapturingLogger<T>(List<LogEntry> entries, object gate) : ILogger<T>
                {
                    /// <summary>
                    /// Supplies  entries for the fixture or scenario under test.
                    /// </summary>
                    private readonly List<LogEntry> _entries = entries;
                    /// <summary>
                    /// Supplies  gate for the fixture or scenario under test.
                    /// </summary>
                    private readonly object _gate = gate;

                    public IDisposable BeginScope<TState>(TState state) where TState : notnull
                    {
                        return NullScope.Instance;
                    }

                    /// <summary>
                    /// Confirms the is enabled behavior.
                    /// </summary>
                    /// <returns>The value returned by the is enabled helper.</returns>
                    /// <summary>
                    /// Confirms the is enabled behavior.
                    /// </summary>
                    /// <param name="logLevel">The log level used by this test scenario.</param>
                    /// <returns>The value returned by the is enabled helper.</returns>
                    public bool IsEnabled(LogLevel logLevel)
                    {
                        return true;
                    }

                    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                    {
                        string message = formatter(state, exception);
                        lock (_gate)
                        {
                            _entries.Add(new LogEntry(eventId, message));
                        }
                    }

                    /// <summary>
                    /// Confirms the null scope behavior.
                    /// </summary>
                    private sealed class NullScope : IDisposable
                    {
                        /// <summary>
                        /// Confirms instance behavior.
                        /// </summary>
                        internal static readonly NullScope Instance = new();

                        /// <summary>
                        /// Confirms the dispose behavior.
                        /// </summary>
                        public void Dispose()
                        {
                        }
                    }
                }
            }

            /// <summary>
            /// Confirms the write lines async behavior.
            /// </summary>
            /// <returns>The value returned by the write lines async helper.</returns>
            /// <summary>
            /// Confirms the write lines async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="lines">The lines used by this test scenario.</param>
            /// <returns>The value returned by the write lines async helper.</returns>
            internal static async Task WriteLinesAsync(Stream stream, IReadOnlyList<string> lines)
            {
                ArgumentNullException.ThrowIfNull(stream);
                ArgumentNullException.ThrowIfNull(lines);

                if (lines.Count == 0)
                {
                    return;
                }

                int totalBytes = 0;
                foreach (string line in lines)
                {
                    ArgumentException.ThrowIfNullOrWhiteSpace(line);
                    totalBytes += System.Text.Encoding.ASCII.GetByteCount(line) + 2;
                }

                byte[] bytes = new byte[totalBytes];
                int offset = 0;

                foreach (string line in lines)
                {
                    offset += System.Text.Encoding.ASCII.GetBytes(line.AsSpan(), bytes.AsSpan(offset));
                    bytes[offset++] = (byte)'\r';
                    bytes[offset++] = (byte)'\n';
                }

                await stream.WriteAsync(bytes, CancellationToken.None);
                await stream.FlushAsync(CancellationToken.None);
            }

            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            public async ValueTask DisposeAsync()
            {
                _cts.Cancel();
                _listener.Stop();

                try
                {
                    await _acceptLoop;
                }
                catch (OperationCanceledException)
                {
                }

                _cts.Dispose();
            }
        }
    }
}
