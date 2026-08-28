// <copyright file="TransitConnectionNegotiationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using System.Net;
using System.Net.Security;
using System.Reflection;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;
using VectorNNTP.Backfiller.Tests.TestInfrastructure;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests transit connection protocol negotiation behavior.
    /// </summary>
    public sealed class TransitConnectionNegotiationTests : IClassFixture<TestTlsCertificateFixture>
    {
        private readonly TestTlsCertificateFixture _tlsFixture;

        public TransitConnectionNegotiationTests(TestTlsCertificateFixture tlsFixture)
        {
            ArgumentNullException.ThrowIfNull(tlsFixture);
            _tlsFixture = tlsFixture;
        }

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

        [Fact]
        public async Task DisposeAsync_WhenDisposedImmediatelyAfterReady_DoesNotThrowFromWriteLoopStartupRace()
        {
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

            static string? GetListenerLocalEndpoint(FakeNntpServer serverInstance)
            {
                FieldInfo? listenerField = typeof(FakeNntpServer).GetField("_listener", BindingFlags.Instance | BindingFlags.NonPublic);
                TcpListener? listener = listenerField?.GetValue(serverInstance) as TcpListener;
                return listener?.LocalEndpoint?.ToString();
            }

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
        private static async Task ServeGracefulShutdownAsync(Stream stream, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(stream);

            string command = await FakeNntpServer.ReadLineAsync(stream, cancellationToken);
            Assert.Equal("QUIT", command);
            await FakeNntpServer.WriteLineAsync(stream, "205 connection closing");
        }


        private sealed class FakeNntpServer : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> _sessions;
            private readonly CancellationTokenSource _cts = new();
            private readonly Task _acceptLoop;

            private FakeNntpServer(TcpListener listener, IReadOnlyList<Func<NetworkStream, CancellationToken, Task>> sessions)
            {
                _listener = listener;
                _sessions = sessions;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            internal static async Task<FakeNntpServer> StartAsync(Func<NetworkStream, CancellationToken, Task> session)
            {
                ArgumentNullException.ThrowIfNull(session);
                return await StartSessionsAsync([session]);
            }

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

            private static async ValueTask<byte> ReadByteAsync(Stream stream, CancellationToken cancellationToken)
            {
                byte[] single = new byte[1];
                int read = await stream.ReadAsync(single, cancellationToken);
                return read == 0 ? throw new InvalidOperationException("Unexpected EOF while reading stream data.") : single[0];
            }

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

            internal static async Task ExpectCommandAsync(Stream stream, string expected)
            {
                string line = await ReadLineAsync(stream, CancellationToken.None);
                Assert.Equal(expected, line);
            }

            internal static async Task WriteLineAsync(Stream stream, string line)
            {
                byte[] bytes = System.Text.Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, CancellationToken.None);
                await stream.FlushAsync(CancellationToken.None);
            }

            internal sealed class CapturingLoggerProvider
            {
                private readonly object _gate = new();

                internal List<LogEntry> Entries { get; } = [];

                internal ILogger<T> CreateLogger<T>()
                {
                    return new CapturingLogger<T>(Entries, _gate);
                }

                internal sealed record LogEntry(EventId EventId, string Message);

                private sealed class CapturingLogger<T>(List<LogEntry> entries, object gate) : ILogger<T>
                {
                    private readonly List<LogEntry> _entries = entries;
                    private readonly object _gate = gate;

                    public IDisposable BeginScope<TState>(TState state) where TState : notnull
                    {
                        return NullScope.Instance;
                    }

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

                    private sealed class NullScope : IDisposable
                    {
                        internal static readonly NullScope Instance = new();

                        public void Dispose()
                        {
                        }
                    }
                }
            }

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
