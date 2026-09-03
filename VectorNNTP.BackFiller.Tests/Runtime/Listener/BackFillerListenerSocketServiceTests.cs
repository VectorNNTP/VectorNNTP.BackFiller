// <copyright file="BackFillerListenerSocketServiceTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for back filler listener socket service, covering configuration, runtime, and failure-handling contracts exercised by the tests.
// Primary responsibility: documents the executable contracts covered by the back filler listener socket service test suite.

using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
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
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(new X509Certificate2(certA.Export(X509ContentType.Pkcs12)), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
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
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(new X509Certificate2(certA.Export(X509ContentType.Pkcs12)), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5));

            string thumbprintA = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port);
            Assert.Equal(certA.Thumbprint, thumbprintA, ignoreCase: true);

            state.Publish(new BackFillerCertificateBundle(new X509Certificate2(certB.Export(X509ContentType.Pkcs12)), "memory", DateTimeOffset.UtcNow));

            string thumbprintB = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port);
            Assert.Equal(certB.Thumbprint, thumbprintB, ignoreCase: true);

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
            BackFillerCertificateState state = new();

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
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

            _ = await Assert.ThrowsAnyAsync<AuthenticationException>(async () =>
                await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                {
                    TargetHost = "localhost",
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                }).ConfigureAwait(false)).ConfigureAwait(false);

            using X509Certificate2 cert = CreateServerCertificate("bf-listener-recovery.example.com");
            state.Publish(new BackFillerCertificateBundle(new X509Certificate2(cert.Export(X509ContentType.Pkcs12)), "memory", DateTimeOffset.UtcNow));

            string thumbprint = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port).ConfigureAwait(false);
            Assert.Equal(cert.Thumbprint, thumbprint, ignoreCase: true);

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
            BackFillerCertificateState state = new();
            state.Publish(new BackFillerCertificateBundle(new X509Certificate2(certA.Export(X509ContentType.Pkcs12)), "memory", DateTimeOffset.UtcNow));

            ShutdownCoordinator shutdown = new();
            BackFillerListenerSocketService service = new(
                runtime,
                state,
                shutdown,
                NullLogger<BackFillerListenerSocketService>.Instance);

            using CancellationTokenSource runCts = new();
            Task runTask = service.StartAsync(runCts.Token);

            await WaitForPortReadyAsync(IPAddress.Loopback, port, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            string thumbprint = await ConnectAndGetServerThumbprintAsync(IPAddress.Loopback, port).ConfigureAwait(false);
            Assert.Equal(certA.Thumbprint, thumbprint, ignoreCase: true);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await runTask.ConfigureAwait(false);

            state.Dispose();
            shutdown.Dispose();
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

            using X509Certificate2 cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(7));
            return new X509Certificate2(cert.Export(X509ContentType.Pkcs12));
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
        private static BackFillerRuntimeOptions CreateRuntimeOptions(int bindPort, IReadOnlyList<string> bindTokens)
        {
            BackFillerLetsEncryptRuntimeOptions letsEncrypt = new(
                Enabled: true,
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
                LetsEncrypt: letsEncrypt);
        }
    }
}
