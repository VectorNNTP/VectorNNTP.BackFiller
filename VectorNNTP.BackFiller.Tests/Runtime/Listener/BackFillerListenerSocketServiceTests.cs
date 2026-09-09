// <copyright file="BackFillerListenerSocketServiceTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for back filler listener socket service, covering configuration, runtime, and failure-handling contracts exercised by the tests.
// Primary responsibility: documents the executable contracts covered by the back filler listener socket service test suite.

using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.Certificates;
using VectorNNTP.Backfiller.Runtime.Listener;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Listener
{
    /// <summary>
    /// Confirms the back filler listener socket service tests behavior.
    /// </summary>
    public sealed class BackFillerListenerSocketServiceTests
    {
        /// <summary>
        /// Confirms the start async with loopback bind and certificate accepts tls connection behavior.
        /// </summary>
        [Fact]
        public async Task StartAsync_WithLoopbackBindAndCertificate_AcceptsTlsConnection()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 certA = CreateServerCertificate("bf-listener-a.example.com");

            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(certA), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5));

            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port);

            using SslStream sslStream = new(
                client.GetStream(),
                leaveInnerStreamOpen: false,
                static (sender, certificate, chain, errors) => true);

            await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = "localhost",
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            });

            Assert.True(sslStream.IsAuthenticated);

            await service.StopAsync(CancellationToken.None);
            await runTask;

            state.Dispose();
            shutdown.Dispose();
        }
        /// <summary>
        /// Confirms the start async when certificate state replaced new connections use new certificate behavior.
        /// </summary>
        [Fact]
        public async Task StartAsync_WhenCertificateStateReplaced_NewConnectionsUseNewCertificate()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 certA = CreateServerCertificate("bf-listener-a.example.com");
            using X509Certificate2 certB = CreateServerCertificate("bf-listener-b.example.com");

            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(certA), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5));

            string thumbprintA = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port);
            Assert.Equal(certA.GetCertHashString(HashAlgorithmName.SHA256), thumbprintA, ignoreCase: true);

            state.Publish(new BackFillerCertificateBundle(CloneForState(certB), "memory", DateTimeOffset.UtcNow));

            string thumbprintB = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port);
            Assert.Equal(certB.GetCertHashString(HashAlgorithmName.SHA256), thumbprintB, ignoreCase: true);

            await service.StopAsync(CancellationToken.None);
            await runTask;

            state.Dispose();
            shutdown.Dispose();
        }
        /// <summary>
        /// Confirms the start async when certificate missing handshake fails but listener stays alive behavior.
        /// </summary>
        [Fact]
        public async Task StartAsync_WhenCertificateMissing_HandshakeFailsButListenerStaysAlive()
        {
            int port = ReserveEphemeralTcpPort();
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

            using SslStream sslStream = new(
                client.GetStream(),
                leaveInnerStreamOpen: false,
                static (sender, certificate, chain, errors) => true);

            Exception handshakeFailure = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                {
                    TargetHost = "localhost",
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                }).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.True(handshakeFailure is AuthenticationException or IOException);

            using X509Certificate2 cert = CreateServerCertificate("bf-listener-recovery.example.com");
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));

            string thumbprint = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port).ConfigureAwait(false);
            Assert.Equal(cert.GetCertHashString(HashAlgorithmName.SHA256), thumbprint, ignoreCase: true);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await runTask.ConfigureAwait(false);

            state.Dispose();
            shutdown.Dispose();
        }
        /// <summary>
        /// Confirms the start async with wildcard bind listens on loopback behavior.
        /// </summary>
        [Fact]
        public async Task StartAsync_WithWildcardBindListensOnLoopback()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 certA = CreateServerCertificate("bf-listener-wildcard.example.com");

            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["*"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(certA), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            string thumbprint = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port).ConfigureAwait(false);
            Assert.Equal(certA.GetCertHashString(HashAlgorithmName.SHA256), thumbprint, ignoreCase: true);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await runTask.ConfigureAwait(false);

            state.Dispose();
            shutdown.Dispose();
        }

        [Fact]
        public void BuildListenEndpoints_WhenBindAddressOmitted_UsesIpv4AndIpv6WildcardEndpoints()
        {
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(bindPort: 119, bindTokens: []);

            IReadOnlyList<IPEndPoint> endpoints = InvokeBuildListenEndpointsForTesting(runtime);

            Assert.Contains(endpoints, endpoint => endpoint.Address.Equals(IPAddress.Any) && endpoint.Port == 119);
            Assert.Contains(endpoints, endpoint => endpoint.Address.Equals(IPAddress.IPv6Any) && endpoint.Port == 119);
            Assert.Equal(2, endpoints.Count);
        }

        [Fact]
        public async Task StartAsync_WhenMaxActiveConnectionsReached_RejectsAdditionalConnection()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-cap.example.com");

            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"], maxActiveConnections: 1);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);
            TaskCompletionSource<bool> acceptedSlotReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
            service.OnConnectionSlotReleasedForTesting = () => acceptedSlotReleased.TrySetResult(true);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                using TcpClient accepted = new();
                await accepted.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
                using SslStream acceptedSsl = await AuthenticateClientAsync(accepted).ConfigureAwait(false);
                Assert.True(acceptedSsl.IsAuthenticated);

                using TcpClient rejected = new();
                await rejected.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
                using SslStream rejectedSsl = new(
                    rejected.GetStream(),
                    leaveInnerStreamOpen: false,
                    static (sender, certificate, chain, errors) => true);

                await Assert.ThrowsAsync<IOException>(async () =>
                {
                    await rejectedSsl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                    {
                        TargetHost = "localhost",
                        EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                        CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                    }).ConfigureAwait(false);
                }).ConfigureAwait(false);

                rejected.Dispose();

                accepted.Client.Shutdown(SocketShutdown.Send);
                await AwaitRemoteClosureAsync(acceptedSsl).ConfigureAwait(false);
                await acceptedSlotReleased.Task.ConfigureAwait(false);

                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
            finally
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WhenTlsHandshakeStalls_TimesOutAndReleasesCapacityForNextConnection()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-h10-handshake-timeout.example.com");

            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(
                port,
                ["127.0.0.1"],
                maxActiveConnections: 1,
                tlsHandshakeTimeoutSeconds: 3,
                ioProgressTimeoutSeconds: 30,
                awaitingReceiptAckTimeoutSeconds: 30);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            CapturingLoggerProvider loggerProvider = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                loggerProvider.CreateLogger<BackFillerListenerSocketService>());

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            try
            {
                ConnectionSlotReleasePhaseObserver releaseObserver = new();
                service.OnConnectionSlotReleasedForTesting = releaseObserver.OnConnectionSlotReleased;

                Task readinessProbeReleaseObserved = releaseObserver.BeginNextPhaseAndGetTask();
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                await readinessProbeReleaseObserved.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                Task timeoutReleaseObserved = releaseObserver.BeginNextPhaseAndGetTask();
                int scenarioLogStartIndex = loggerProvider.Entries.Count;

                Stopwatch timeoutStopwatch = Stopwatch.StartNew();

                using TcpClient stalled = new();
                await stalled.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

                byte[] closureProbe = new byte[1];
                Task<int> closureReadTask = stalled.GetStream().ReadAsync(closureProbe).AsTask();
                int read = await closureReadTask.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                await timeoutReleaseObserved.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                timeoutStopwatch.Stop();

                Assert.Equal(0, read);
                Assert.InRange(timeoutStopwatch.Elapsed, TimeSpan.Zero, TimeSpan.FromSeconds(8));
                int scenarioLogCount = loggerProvider.Entries.Count - scenarioLogStartIndex;
                IReadOnlyList<CapturingLoggerProvider.LogEntry> scenarioLogs = loggerProvider.Entries.GetRange(scenarioLogStartIndex, scenarioLogCount);
                Assert.Contains(
                    scenarioLogs,
                    static entry => entry.EventId.Id == 2709
                        && entry.StateValues.TryGetValue("Reason", out object? reason)
                        && reason is string reasonText
                        && reasonText == "tls-handshake");
                Assert.DoesNotContain(scenarioLogs, static entry => entry.EventId.Id == 2705);

                using TcpClient recovered = new();
                await recovered.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
                using SslStream recoveredSsl = await AuthenticateClientAsync(recovered).ConfigureAwait(false);
                Assert.True(recoveredSsl.IsAuthenticated);

                recovered.Client.Shutdown(SocketShutdown.Send);
                await AwaitRemoteClosureAsync(recoveredSsl).ConfigureAwait(false);

                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
            finally
            {
                shutdown.SignalForcedShutdown();
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WithTlsProtocolGetRequest_WhenRetainedArticleAndAck_RespondsFoundAndMarksListenerCompleted()
        {
            const string messageId = "<stage6d-found-ack@example.com>";
            const string payloadText = "stage6d-found-payload";

            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-protocol.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            string messageIdMd5 = RetainArticle(retentionAuthority, messageId, payloadText);

            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                using TcpClient client = new();
                await client.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

                using SslStream sslStream = await AuthenticateClientAsync(client).ConfigureAwait(false);

                byte[] request = ListenerProtocolEncoder.EncodeGetRequest(1001, messageIdMd5);
                byte[] ack = ListenerProtocolEncoder.EncodeGetReceiptAck(1001);
                byte[] requestAndAck = new byte[request.Length + ack.Length];
                Buffer.BlockCopy(request, 0, requestAndAck, 0, request.Length);
                Buffer.BlockCopy(ack, 0, requestAndAck, request.Length, ack.Length);
                await sslStream.WriteAsync(requestAndAck).ConfigureAwait(false);

                ListenerParsedFrame found = await ReadSingleFrameAsync(sslStream, maxPayloadBytes: payloadText.Length + 1024).ConfigureAwait(false);
                Assert.Equal(ListenerOpcode.GetResponseFound, found.Header.Opcode);
                Assert.Equal<uint>(1001, found.Header.RequestId);
                Assert.Equal(payloadText, Encoding.ASCII.GetString(found.Payload.ToArray()));

                client.Client.Shutdown(SocketShutdown.Send);
                await AwaitRemoteClosureAsync(sslStream).ConfigureAwait(false);
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);

                ArticleRetentionSnapshot after = retentionAuthority.GetSnapshot();
                Assert.Equal(0, after.ActiveReaderCount);
                Assert.Equal(1, after.ListenerCompletionCount);
            }
            finally
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WithTlsProtocolGetRequest_WhenArticleMissing_RespondsNotFoundAndDoesNotMarkCompletion()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-notfound.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);

            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                using TcpClient client = new();
                await client.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

                using SslStream sslStream = await AuthenticateClientAsync(client).ConfigureAwait(false);

                byte[] request = ListenerProtocolEncoder.EncodeGetRequest(1101, "30edc94157aa16fe644a45a1f1ffe160");
                await sslStream.WriteAsync(request).ConfigureAwait(false);

                ListenerParsedFrame response = await ReadSingleFrameAsync(sslStream, maxPayloadBytes: 8).ConfigureAwait(false);
                Assert.Equal(ListenerOpcode.GetResponseNotFound, response.Header.Opcode);
                Assert.Equal<uint>(1101, response.Header.RequestId);
                Assert.Equal(1u, response.Header.PayloadLength);
                Assert.Equal(ListenerProtocol.NotFoundReasonUnavailable, response.Payload.First.Span[0]);

                client.Client.Shutdown(SocketShutdown.Send);
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);

                ArticleRetentionSnapshot snapshot = retentionAuthority.GetSnapshot();
                Assert.Equal(0, snapshot.ListenerCompletionCount);
                Assert.Equal(0, snapshot.ActiveReaderCount);
            }
            finally
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WithTlsProtocolGetRequest_WhenClientDisconnectsBeforeAck_DoesNotMarkCompletionAndReleasesLease()
        {
            const string messageId = "<stage6d-no-ack-disconnect@example.com>";
            const string payloadText = "stage6d-no-ack-payload";

            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-no-ack.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            string messageIdMd5 = RetainArticle(retentionAuthority, messageId, payloadText);

            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                using TcpClient client = new();
                await client.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

                using SslStream sslStream = await AuthenticateClientAsync(client).ConfigureAwait(false);
                byte[] request = ListenerProtocolEncoder.EncodeGetRequest(1201, messageIdMd5);
                await sslStream.WriteAsync(request).ConfigureAwait(false);

                ListenerParsedFrame found = await ReadSingleFrameAsync(sslStream, maxPayloadBytes: payloadText.Length + 1024).ConfigureAwait(false);
                Assert.Equal(ListenerOpcode.GetResponseFound, found.Header.Opcode);
                Assert.Equal<uint>(1201, found.Header.RequestId);
                Assert.Equal(payloadText, Encoding.ASCII.GetString(found.Payload.ToArray()));

                ArticleRetentionSnapshot beforeDisconnect = retentionAuthority.GetSnapshot();
                Assert.Equal(1, beforeDisconnect.ActiveReaderCount);
                Assert.Equal(0, beforeDisconnect.ListenerCompletionCount);

                client.Client.Shutdown(SocketShutdown.Send);
                await AwaitRemoteClosureAsync(sslStream).ConfigureAwait(false);
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);

                ArticleRetentionSnapshot after = retentionAuthority.GetSnapshot();
                Assert.Equal(0, after.ActiveReaderCount);
                Assert.Equal(0, after.ListenerCompletionCount);
            }
            finally
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WithTlsProtocolGetRequests_WhenMultiplexedOutOfOrderAck_CorrelatesAndCompletesIndependently()
        {
            const string messageIdOne = "<stage6d-multiplex-1@example.com>";
            const string messageIdTwo = "<stage6d-multiplex-2@example.com>";
            const string payloadOne = "stage6d-payload-one";
            const string payloadTwo = "stage6d-payload-two";

            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-multiplex.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            string md5One = RetainArticle(retentionAuthority, messageIdOne, payloadOne);
            string md5Two = RetainArticle(retentionAuthority, messageIdTwo, payloadTwo);

            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                using TcpClient client = new();
                await client.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

                using SslStream sslStream = await AuthenticateClientAsync(client).ConfigureAwait(false);

                byte[] reqOne = ListenerProtocolEncoder.EncodeGetRequest(1301, md5One);
                byte[] reqTwo = ListenerProtocolEncoder.EncodeGetRequest(1302, md5Two);
                byte[] ackTwo = ListenerProtocolEncoder.EncodeGetReceiptAck(1302);
                byte[] ackOne = ListenerProtocolEncoder.EncodeGetReceiptAck(1301);

                byte[] coalescedFrames = new byte[reqOne.Length + reqTwo.Length + ackTwo.Length + ackOne.Length];
                Buffer.BlockCopy(reqOne, 0, coalescedFrames, 0, reqOne.Length);
                Buffer.BlockCopy(reqTwo, 0, coalescedFrames, reqOne.Length, reqTwo.Length);
                Buffer.BlockCopy(ackTwo, 0, coalescedFrames, reqOne.Length + reqTwo.Length, ackTwo.Length);
                Buffer.BlockCopy(ackOne, 0, coalescedFrames, reqOne.Length + reqTwo.Length + ackTwo.Length, ackOne.Length);
                await sslStream.WriteAsync(coalescedFrames).ConfigureAwait(false);

                ListenerParsedFrame responseA = await ReadSingleFrameAsync(sslStream, maxPayloadBytes: payloadOne.Length + payloadTwo.Length + 2048).ConfigureAwait(false);
                ListenerParsedFrame responseB = await ReadSingleFrameAsync(sslStream, maxPayloadBytes: payloadOne.Length + payloadTwo.Length + 2048).ConfigureAwait(false);

                Assert.Equal(ListenerOpcode.GetResponseFound, responseA.Header.Opcode);
                Assert.Equal(ListenerOpcode.GetResponseFound, responseB.Header.Opcode);
                Assert.NotEqual(responseA.Header.RequestId, responseB.Header.RequestId);

                Dictionary<uint, string> payloadByRequestId = new()
                {
                    [responseA.Header.RequestId] = Encoding.ASCII.GetString(responseA.Payload.ToArray()),
                    [responseB.Header.RequestId] = Encoding.ASCII.GetString(responseB.Payload.ToArray()),
                };

                Assert.Equal(payloadOne, payloadByRequestId[1301]);
                Assert.Equal(payloadTwo, payloadByRequestId[1302]);

                client.Client.Shutdown(SocketShutdown.Send);
                await AwaitRemoteClosureAsync(sslStream).ConfigureAwait(false);
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);

                ArticleRetentionSnapshot snapshot = retentionAuthority.GetSnapshot();
                Assert.Equal(0, snapshot.ActiveReaderCount);
                Assert.Equal(2, snapshot.ListenerCompletionCount);
            }
            finally
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WithTlsProtocolGetRequest_WhenForcedShutdownBeforeAck_DoesNotMarkCompletionAndReleasesLease()
        {
            const string messageId = "<stage6d-forced-shutdown@example.com>";
            const string payloadText = "stage6d-forced-payload";

            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-forced.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            string messageIdMd5 = RetainArticle(retentionAuthority, messageId, payloadText);

            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                using TcpClient client = new();
                await client.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

                using SslStream sslStream = await AuthenticateClientAsync(client).ConfigureAwait(false);
                byte[] request = ListenerProtocolEncoder.EncodeGetRequest(1401, messageIdMd5);
                await sslStream.WriteAsync(request).ConfigureAwait(false);

                ListenerParsedFrame found = await ReadSingleFrameAsync(sslStream, maxPayloadBytes: payloadText.Length + 1024).ConfigureAwait(false);
                Assert.Equal(ListenerOpcode.GetResponseFound, found.Header.Opcode);
                Assert.Equal<uint>(1401, found.Header.RequestId);

                ArticleRetentionSnapshot beforeStop = retentionAuthority.GetSnapshot();
                Assert.Equal(1, beforeStop.ActiveReaderCount);
                Assert.Equal(0, beforeStop.ListenerCompletionCount);

                TaskCompletionSource<bool> requestConnectionSlotReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
                service.OnConnectionSlotReleasedForTesting = () => requestConnectionSlotReleased.TrySetResult(true);

                shutdown.SignalForcedShutdown();
                await AwaitRemoteClosureAsync(sslStream).ConfigureAwait(false);
                await requestConnectionSlotReleased.Task.ConfigureAwait(false);
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);

                ArticleRetentionSnapshot afterStop = retentionAuthority.GetSnapshot();
                Assert.Equal(0, afterStop.ActiveReaderCount);
                Assert.Equal(0, afterStop.ListenerCompletionCount);
            }
            finally
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StopAsync_WhenGracefulShutdownSignaled_AllowsFinalWaitToDrainAfterActiveConnectionCompletes()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-graceful-drain.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            TaskCompletionSource<bool> blockedConnectionGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            InjectActiveConnectionTaskForTesting(service, blockedConnectionGate.Task);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                shutdown.SignalGracefulShutdown(TimeSpan.FromMinutes(5));

                Task stopTask = service.StopAsync(CancellationToken.None);
                await AssertTaskRemainsIncompleteAsync(stopTask).ConfigureAwait(false);

                blockedConnectionGate.TrySetResult(true);
                await stopTask.ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
            finally
            {
                blockedConnectionGate.TrySetResult(true);
                shutdown.SignalForcedShutdown();
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WhenEstablishedClientIsIdle_TerminatesSessionAfterIoProgressTimeoutAndLogsConnectionTimedOut()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-h10-idle-timeout.example.com");

            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(
                port,
                ["127.0.0.1"],
                maxActiveConnections: 1,
                tlsHandshakeTimeoutSeconds: 30,
                ioProgressTimeoutSeconds: 3,
                awaitingReceiptAckTimeoutSeconds: 30);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            CapturingLoggerProvider loggerProvider = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                loggerProvider.CreateLogger<BackFillerListenerSocketService>());

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                ConnectionSlotReleasePhaseObserver releaseObserver = new();
                service.OnConnectionSlotReleasedForTesting = releaseObserver.OnConnectionSlotReleased;

                Task readinessProbeReleaseObserved = releaseObserver.BeginNextPhaseAndGetTask();
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                await readinessProbeReleaseObserved.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                Task idleReleaseObserved = releaseObserver.BeginNextPhaseAndGetTask();
                int scenarioLogStartIndex = loggerProvider.Entries.Count;

                using TcpClient idleClient = new();
                await idleClient.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
                using SslStream idleSsl = await AuthenticateClientAsync(idleClient).ConfigureAwait(false);

                Stopwatch timeoutStopwatch = Stopwatch.StartNew();

                byte[] closureProbe = new byte[1];
                Task<int> closureReadTask = idleSsl.ReadAsync(closureProbe).AsTask();
                int read = await closureReadTask.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                await idleReleaseObserved.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                timeoutStopwatch.Stop();

                Assert.Equal(0, read);
                Assert.InRange(timeoutStopwatch.Elapsed, TimeSpan.Zero, TimeSpan.FromSeconds(8));
                int scenarioLogCount = loggerProvider.Entries.Count - scenarioLogStartIndex;
                IReadOnlyList<CapturingLoggerProvider.LogEntry> scenarioLogs = loggerProvider.Entries.GetRange(scenarioLogStartIndex, scenarioLogCount);
                Assert.Contains(
                    scenarioLogs,
                    static entry => entry.EventId.Id == 2709
                        && entry.StateValues.TryGetValue("Reason", out object? reason)
                        && reason is string reasonText
                        && reasonText == "io-progress");
                Assert.DoesNotContain(scenarioLogs, static entry => entry.EventId.Id == 2705);

                using TcpClient recovered = new();
                await recovered.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
                using SslStream recoveredSsl = await AuthenticateClientAsync(recovered).ConfigureAwait(false);
                Assert.True(recoveredSsl.IsAuthenticated);

                recovered.Client.Shutdown(SocketShutdown.Send);
                await AwaitRemoteClosureAsync(recoveredSsl).ConfigureAwait(false);

                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
            finally
            {
                shutdown.SignalForcedShutdown();
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StartAsync_WhenClientStopsReadingButKeepsSending_WriterTimeoutTerminatesSessionAndLogsConnectionTimedOut()
        {
            const string messageId = "<stage6d-h10-writer-timeout@example.com>";

            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-h10-writer-timeout.example.com");

            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(
                port,
                ["127.0.0.1"],
                maxActiveConnections: 1,
                tlsHandshakeTimeoutSeconds: 30,
                ioProgressTimeoutSeconds: 3,
                awaitingReceiptAckTimeoutSeconds: 30);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            string messageIdMd5 = RetainArticle(retentionAuthority, messageId, new string('p', 16 * 1024 * 1024));

            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            CapturingLoggerProvider loggerProvider = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                loggerProvider.CreateLogger<BackFillerListenerSocketService>());

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                ConnectionSlotReleasePhaseObserver releaseObserver = new();
                service.OnConnectionSlotReleasedForTesting = releaseObserver.OnConnectionSlotReleased;

                Task readinessProbeReleaseObserved = releaseObserver.BeginNextPhaseAndGetTask();
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                await readinessProbeReleaseObserved.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                Task writerTimeoutReleaseObserved = releaseObserver.BeginNextPhaseAndGetTask();
                int scenarioLogStartIndex = loggerProvider.Entries.Count;

                using TcpClient stalled = new();
                await stalled.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
                using SslStream stalledSsl = await AuthenticateClientAsync(stalled).ConfigureAwait(false);

                byte[] foundRequest = ListenerProtocolEncoder.EncodeGetRequest(1701, messageIdMd5);
                await stalledSsl.WriteAsync(foundRequest).ConfigureAwait(false);

                TaskCompletionSource<bool> keepReaderAliveWriteObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
                TaskCompletionSource<bool> keepReaderAliveWriteObservedAfterTimeoutWait = new(TaskCreationOptions.RunContinuationsAsynchronously);
                using CancellationTokenSource keepReaderAliveProducerCts = new();
                int keepReaderAliveWritesSucceeded = 0;
                int observeWritesAfterTimeoutWait = 0;
                Task keepReaderAliveProducerTask = Task.Run(async () =>
                {
                    uint requestId = 1702;
                    while (true)
                    {
                        try
                        {
                            keepReaderAliveProducerCts.Token.ThrowIfCancellationRequested();
                            byte[] keepReaderAliveRequest = ListenerProtocolEncoder.EncodeGetRequest(requestId, "30edc94157aa16fe644a45a1f1ffe160");
                            await stalledSsl.WriteAsync(keepReaderAliveRequest, keepReaderAliveProducerCts.Token).ConfigureAwait(false);
                            _ = Interlocked.Increment(ref keepReaderAliveWritesSucceeded);
                            keepReaderAliveWriteObserved.TrySetResult(true);
                            if (Volatile.Read(ref observeWritesAfterTimeoutWait) != 0)
                            {
                                keepReaderAliveWriteObservedAfterTimeoutWait.TrySetResult(true);
                            }

                            requestId++;
                        }
                        catch (OperationCanceledException) when (keepReaderAliveProducerCts.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (IOException ex) when (IsExpectedTlsAbruptClosure(ex))
                        {
                            return;
                        }
                    }
                });

                await keepReaderAliveWriteObserved.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                Stopwatch timeoutStopwatch = Stopwatch.StartNew();
                try
                {
                    Volatile.Write(ref observeWritesAfterTimeoutWait, 1);
                    await keepReaderAliveWriteObservedAfterTimeoutWait.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                    await writerTimeoutReleaseObserved.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                    await AwaitRemoteClosureAsync(stalledSsl).WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                }
                finally
                {
                    keepReaderAliveProducerCts.Cancel();
                    await keepReaderAliveProducerTask.ConfigureAwait(false);
                    timeoutStopwatch.Stop();
                }

                Assert.True(Volatile.Read(ref keepReaderAliveWritesSucceeded) >= 2);

                Assert.InRange(timeoutStopwatch.Elapsed, TimeSpan.Zero, TimeSpan.FromSeconds(12));
                int scenarioLogCount = loggerProvider.Entries.Count - scenarioLogStartIndex;
                IReadOnlyList<CapturingLoggerProvider.LogEntry> scenarioLogs = loggerProvider.Entries.GetRange(scenarioLogStartIndex, scenarioLogCount);
                Assert.Contains(
                    scenarioLogs,
                    static entry => entry.EventId.Id == 2709
                        && entry.StateValues.TryGetValue("Reason", out object? reason)
                        && reason is string reasonText
                        && reasonText == "io-progress");
                Assert.DoesNotContain(scenarioLogs, static entry => entry.EventId.Id == 2705);

                using TcpClient recovered = new();
                await recovered.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
                using SslStream recoveredSsl = await AuthenticateClientAsync(recovered).ConfigureAwait(false);
                Assert.True(recoveredSsl.IsAuthenticated);

                recovered.Client.Shutdown(SocketShutdown.Send);
                await AwaitRemoteClosureAsync(recoveredSsl).ConfigureAwait(false);

                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
            finally
            {
                shutdown.SignalForcedShutdown();
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StopAsync_WhenForcedEscalationOccursWhileWaiting_CancelsFinalWaitAndCompletesShutdown()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-forced-escalation.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            TaskCompletionSource<bool> blockedConnectionGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            InjectActiveConnectionTaskForTesting(service, blockedConnectionGate.Task);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                shutdown.SignalGracefulShutdown(TimeSpan.FromMinutes(5));

                Task stopTask = service.StopAsync(CancellationToken.None);
                await AssertTaskRemainsIncompleteAsync(stopTask).ConfigureAwait(false);

                shutdown.SignalForcedShutdown();
                await stopTask.ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
            finally
            {
                blockedConnectionGate.TrySetResult(true);
                shutdown.SignalForcedShutdown();
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        [Fact]
        public async Task StopAsync_WhenForcedShutdownAlreadySignaledBeforeFinalWait_CompletesWithoutWaitingForBlockedActiveTask()
        {
            int port = ReserveEphemeralTcpPort();
            using X509Certificate2 cert = CreateServerCertificate("bf-listener-forced-already-signaled.example.com");
            BackFillerRuntimeOptions runtime = CreateRuntimeOptions(port, ["127.0.0.1"]);
            await using ArticleRetentionAuthority retentionAuthority = new(runtime);
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(CloneForState(cert), "memory", DateTimeOffset.UtcNow));
            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                retentionAuthority,
                NullLogger<BackFillerListenerSocketService>.Instance);

            TaskCompletionSource<bool> blockedConnectionGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            InjectActiveConnectionTaskForTesting(service, blockedConnectionGate.Task);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);
            try
            {
                await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                shutdown.SignalForcedShutdown();

                Task stopTask = service.StopAsync(CancellationToken.None);
                await stopTask.ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
            finally
            {
                blockedConnectionGate.TrySetResult(true);
                shutdown.SignalForcedShutdown();
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
                state.Dispose();
                shutdown.Dispose();
            }
        }

        /// <summary>
        /// Confirms the connect and get server thumbprint async behavior.
        /// </summary>
        /// <returns>The value returned by the connect and get server thumbprint async helper.</returns>
        /// <summary>
        /// Confirms the connect and get server thumbprint async behavior.
        /// </summary>
        /// <param name="address">The address used by this test scenario.</param>
        /// <param name="port">The port used by this test scenario.</param>
        /// <returns>The value returned by the connect and get server thumbprint async helper.</returns>
        private static async Task<string> ConnectAndGetServerThumbprintAsync(IPAddress address, int port)
        {
            using TcpClient client = new();
            await client.ConnectAsync(address, port).ConfigureAwait(false);

            using SslStream sslStream = new(
                client.GetStream(),
                leaveInnerStreamOpen: false,
                static (sender, certificate, chain, errors) => true);

            await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = "localhost",
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            }).ConfigureAwait(false);

            X509Certificate? remote = sslStream.RemoteCertificate;
            Assert.NotNull(remote);
            return remote.GetCertHashString(HashAlgorithmName.SHA256);
        }

        private static async Task<SslStream> AuthenticateClientAsync(TcpClient client)
        {
            SslStream sslStream = new(
                client.GetStream(),
                leaveInnerStreamOpen: false,
                static (sender, certificate, chain, errors) => true);

            await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = "localhost",
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            }).ConfigureAwait(false);

            return sslStream;
        }

        private static async Task<ListenerParsedFrame> ReadSingleFrameAsync(SslStream sslStream, int maxPayloadBytes)
        {
            byte[] headerBytes = await ReadExactlyAsync(sslStream, ListenerProtocol.HeaderLengthBytes).ConfigureAwait(false);
            ListenerFrameHeader header = ListenerFrameHeader.ReadFrom(headerBytes);
            Assert.True(header.PayloadLength <= (uint)maxPayloadBytes);

            byte[] payload = header.PayloadLength == 0
                ? []
                : await ReadExactlyAsync(sslStream, checked((int)header.PayloadLength)).ConfigureAwait(false);

            return new ListenerParsedFrame(header, new ReadOnlySequence<byte>(payload));
        }

        private static async Task<byte[]> ReadExactlyAsync(SslStream sslStream, int length)
        {
            byte[] buffer = new byte[length];
            int read = 0;
            while (read < length)
            {
                int received = await sslStream.ReadAsync(buffer.AsMemory(read, length - read)).ConfigureAwait(false);
                if (received == 0)
                {
                    throw new IOException("Unexpected EOF while reading protocol frame.");
                }

                read += received;
            }

            return buffer;
        }

        private static async Task AwaitRemoteClosureAsync(SslStream sslStream)
        {
            byte[] single = new byte[1];
            while (true)
            {
                try
                {
                    int read = await sslStream.ReadAsync(single.AsMemory(0, 1)).ConfigureAwait(false);
                    if (read == 0)
                    {
                        return;
                    }
                }
                catch (IOException ex) when (IsExpectedTlsAbruptClosure(ex))
                {
                    return;
                }
            }
        }

        private static bool IsExpectedTlsAbruptClosure(IOException exception)
        {
            ArgumentNullException.ThrowIfNull(exception);

            if (exception.Message.Contains("unexpected EOF", StringComparison.OrdinalIgnoreCase)
                || exception.Message.Contains("0 bytes", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            SocketException? socketException = exception.InnerException as SocketException;
            return socketException?.SocketErrorCode is SocketError.ConnectionAborted or SocketError.ConnectionReset;
        }

        private static async Task AssertTaskRemainsIncompleteAsync(Task task)
        {
            ArgumentNullException.ThrowIfNull(task);

            await Task.Yield();
            Assert.False(task.IsCompleted);
        }

        private static IReadOnlyList<IPEndPoint> InvokeBuildListenEndpointsForTesting(BackFillerRuntimeOptions runtimeOptions)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            MethodInfo method = typeof(BackFillerListenerSocketService).GetMethod(
                "BuildListenEndpoints",
                BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException("BuildListenEndpoints method was not found for listener endpoint contract tests.");

            object? rawResult = method.Invoke(null, [runtimeOptions]);
            return rawResult as IReadOnlyList<IPEndPoint>
                ?? throw new InvalidOperationException("BuildListenEndpoints returned an unexpected result type.");
        }

        private static void InjectActiveConnectionTaskForTesting(BackFillerListenerSocketService service, Task connectionTask)
        {
            ArgumentNullException.ThrowIfNull(service);
            ArgumentNullException.ThrowIfNull(connectionTask);

            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;

            FieldInfo gateField = typeof(BackFillerListenerSocketService).GetField("_connectionsGate", flags)
                ?? throw new InvalidOperationException("Listener test seam field '_connectionsGate' was not found.");
            FieldInfo tasksField = typeof(BackFillerListenerSocketService).GetField("_activeConnectionTasks", flags)
                ?? throw new InvalidOperationException("Listener test seam field '_activeConnectionTasks' was not found.");

            object gate = gateField.GetValue(service)
                ?? throw new InvalidOperationException("Listener connection gate was null.");
            HashSet<Task> tasks = (HashSet<Task>?)tasksField.GetValue(service)
                ?? throw new InvalidOperationException("Listener active connection task set was null.");

            lock (gate)
            {
                _ = tasks.Add(connectionTask);
            }
        }

        private static string RetainArticle(ArticleRetentionAuthority authority, string messageId, string payloadText)
        {
            DownloadedArticleBuffer payload = CreateBuffer(payloadText);
            ArticleRetentionAdmissionResult admission = authority.TryRetainSuccessArticle(messageId, payload);
            Assert.Equal(ArticleRetentionAdmissionStatus.Admitted, admission.Status);
            Assert.NotNull(admission.MessageIdMd5);
            return admission.MessageIdMd5!;
        }

        private static DownloadedArticleBuffer CreateBuffer(string payload)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(payload);
            byte[] rented = ArrayPool<byte>.Shared.Rent(bytes.Length);
            Array.Copy(bytes, rented, bytes.Length);
            return new DownloadedArticleBuffer(rented, bytes.Length);
        }

        /// <summary>
        /// Confirms the wait for port ready async behavior.
        /// </summary>
        /// <returns>The value returned by the wait for port ready async helper.</returns>
        /// <summary>
        /// Confirms the wait for port ready async behavior.
        /// </summary>
        /// <param name="address">The address used by this test scenario.</param>
        /// <param name="port">The port used by this test scenario.</param>
        /// <param name="timeout">The timeout used by this test scenario.</param>
        /// <returns>The value returned by the wait for port ready async helper.</returns>
        private static async Task WaitForPortReadyAsync(IPAddress address, int port, TimeSpan timeout)
        {
            DateTime deadline = DateTime.UtcNow.Add(timeout);

            while (DateTime.UtcNow <= deadline)
            {
                using TcpClient probe = new();
                try
                {
                    using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(150));
                    await probe.ConnectAsync(address, port, cts.Token).ConfigureAwait(false);
                    return;
                }
                catch
                {
                    await Task.Delay(25).ConfigureAwait(false);
                }
            }

            throw new TimeoutException($"Timed out waiting for listener readiness at {address}:{port}.");
        }

        private sealed class ConnectionSlotReleasePhaseObserver
        {
            private readonly object _gate = new();
            private int _releasedCount;
            private int _releaseTarget = int.MaxValue;
            private TaskCompletionSource<bool> _releaseObserved = CreatePhaseTaskCompletionSource();

            internal void OnConnectionSlotReleased()
            {
                TaskCompletionSource<bool>? observed = null;

                lock (_gate)
                {
                    _releasedCount++;
                    if (_releasedCount >= _releaseTarget)
                    {
                        observed = _releaseObserved;
                    }
                }

                observed?.TrySetResult(true);
            }

            internal Task BeginNextPhaseAndGetTask()
            {
                lock (_gate)
                {
                    TaskCompletionSource<bool> nextObserved = CreatePhaseTaskCompletionSource();
                    _releaseTarget = _releasedCount + 1;
                    _releaseObserved = nextObserved;
                    return nextObserved.Task;
                }
            }

            private static TaskCompletionSource<bool> CreatePhaseTaskCompletionSource()
            {
                return new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        private sealed class CapturingLoggerProvider
        {
            private readonly object _gate = new();

            internal List<LogEntry> Entries { get; } = [];

            internal ILogger<T> CreateLogger<T>()
            {
                return new CapturingLogger<T>(Entries, _gate);
            }

            internal sealed record LogEntry(EventId EventId, LogLevel LogLevel, string Message, Exception? Exception, IReadOnlyDictionary<string, object?> StateValues);

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
                    _ = logLevel;
                    return true;
                }

                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    Dictionary<string, object?> stateValues = [];
                    if (state is IEnumerable<KeyValuePair<string, object?>> structuredState)
                    {
                        foreach (KeyValuePair<string, object?> item in structuredState)
                        {
                            stateValues[item.Key] = item.Value;
                        }
                    }

                    lock (_gate)
                    {
                        _entries.Add(new LogEntry(eventId, logLevel, message, exception, stateValues));
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

        /// <summary>
        /// Confirms the reserve ephemeral tcp port behavior.
        /// </summary>
        /// <returns>The value returned by the reserve ephemeral tcp port helper.</returns>
        /// <summary>
        /// Confirms the reserve ephemeral tcp port behavior.
        /// </summary>
        /// <returns>The value returned by the reserve ephemeral tcp port helper.</returns>
        private static int ReserveEphemeralTcpPort()
        {
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        /// <summary>
        /// Confirms the create server certificate behavior.
        /// </summary>
        /// <returns>The value returned by the create server certificate helper.</returns>
        /// <summary>
        /// Confirms the create server certificate behavior.
        /// </summary>
        /// <param name="dnsName">The dns name used by this test scenario.</param>
        /// <returns>The value returned by the create server certificate helper.</returns>
        private static X509Certificate2 CreateServerCertificate(string dnsName)
        {
            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new(
                $"CN={dnsName}",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);

            SubjectAlternativeNameBuilder sanBuilder = new();
            sanBuilder.AddDnsName("localhost");
            sanBuilder.AddDnsName(dnsName);
            sanBuilder.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(sanBuilder.Build());
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: true));
            OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, critical: true));

            string pfxPassword = Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));
            using X509Certificate2 cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(7));
            byte[] pfx = cert.Export(X509ContentType.Pkcs12, pfxPassword);
            return new X509Certificate2(
                pfx,
                pfxPassword,
                X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
        }

        /// <summary>
        /// Creates a state-owned listener certificate clone whose private key remains exportable for runtime cloning.
        /// </summary>
        /// <param name="certificate">Source certificate to clone for certificate state publication.</param>
        /// <returns>A cloned certificate suitable for listener runtime use.</returns>
        private static X509Certificate2 CloneForState(X509Certificate2 certificate)
        {
            ArgumentNullException.ThrowIfNull(certificate);

            string pfxPassword = Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));
            byte[] pfx = certificate.Export(X509ContentType.Pkcs12, pfxPassword);
            return new X509Certificate2(
                pfx,
                pfxPassword,
                X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
        }

        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create runtime options helper.</returns>
        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <param name="bindPort">The bind port used by this test scenario.</param>
        /// <param name="bindTokens">The bind tokens used by this test scenario.</param>
        /// <returns>The value returned by the create runtime options helper.</returns>
        private static BackFillerRuntimeOptions CreateRuntimeOptions(
            int bindPort,
            IReadOnlyList<string> bindTokens,
            int maxActiveConnections = 1024,
            int tlsHandshakeTimeoutSeconds = 30,
            int ioProgressTimeoutSeconds = 60,
            int awaitingReceiptAckTimeoutSeconds = 30)
        {
            BackFillerLetsEncryptRuntimeOptions letsEncrypt = new(
                CanonicalCertificateSubjectName: "bf-listener.example.com",
                AcmeAccountEmail: "security@example.com",
                AcmeAccountKeyPemPath: Path.Combine(Path.GetTempPath(), "listener-account.key"),
                CertificatePfxPath: Path.Combine(Path.GetTempPath(), "listener-test.pfx"),
                CertificatePrivateKeyPemPath: Path.Combine(Path.GetTempPath(), "listener-test.key"),
                PfxExportPassword: "UnitTest-PfxPassword-123!",
                RenewBeforeExpiryDays: 7,
                RenewalCheckIntervalHours: 6,
                RenewalJitterRatio: 0.1,
                UseStagingDirectory: true,
                AcmeTransientRetryMaxAttempts: 5,
                DnsPropagationDelaySeconds: 0,
                DnsTxtPollIntervalSeconds: 1,
                DnsTxtPollTimeoutSeconds: 10,
                DnsAuthoritativeNsCacheMinutes: 1,
                DnsAuthoritativeQuorumRatio: 0.7,
                CloudFlareApiToken: "token",
                CloudFlareZoneId: "zone");

            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "bf-listener.example.com",
                BackFillerId: 1,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: bindPort,
                ConfiguredBindAddressTokens: bindTokens,
                ShutdownGracePeriodSeconds: 30,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                Listener: new ListenerRuntimeOptions(
                    ParserAccumulationMaxBytes: 262144,
                    TlsHandshakeTimeout: TimeSpan.FromSeconds(tlsHandshakeTimeoutSeconds),
                    IoProgressTimeout: TimeSpan.FromSeconds(ioProgressTimeoutSeconds),
                    AwaitingReceiptAckTimeout: TimeSpan.FromSeconds(awaitingReceiptAckTimeoutSeconds),
                    MaxQueuedFoundPayloadBytes: 67108864,
                    MaxActiveConnections: maxActiveConnections),
                LetsEncrypt: letsEncrypt);
        }
    }
}
