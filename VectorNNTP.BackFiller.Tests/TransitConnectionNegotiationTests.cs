using System.IO.Compression;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests transit connection protocol negotiation behavior.
/// </summary>
public sealed class TransitConnectionNegotiationTests : IClassFixture<TransitConnectionNegotiationTests.TlsCertificateFixture>
{
    private readonly TlsCertificateFixture _tlsFixture;

    public TransitConnectionNegotiationTests(TlsCertificateFixture tlsFixture)
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
            await WaitUntilCanceledAsync(cancellationToken: _);
        });

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        await connection.InitializeAsync(CancellationToken.None);

        Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
        Assert.False(connection.IsTlsActive);
        Assert.False(connection.IsCompressionActive);
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
            await WaitUntilCanceledAsync(cancellationToken: _);
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

            publishObserved.TrySetResult();
            using CancellationTokenSource serverTimeout = new(TimeSpan.FromSeconds(10));
            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, serverTimeout.Token);
            await allowResponse.Task.WaitAsync(linked.Token);

            await FakeNntpServer.WriteLineAsync(stream, $"239 {messageId} transferred");
            await WaitUntilCanceledAsync(cancellationToken);
        });

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        using CancellationTokenSource initCts = new();
        await connection.InitializeAsync(initCts.Token);
        initCts.Cancel();

        Task<TransitPublishResult> publishTask = connection.SubmitTakethisAsync(messageId, payload, CancellationToken.None, 0L, 0L).AsTask();

        using CancellationTokenSource observeTimeout = new(TimeSpan.FromSeconds(10));
        await publishObserved.Task.WaitAsync(observeTimeout.Token);

        allowResponse.TrySetResult();

        using CancellationTokenSource completionTimeout = new(TimeSpan.FromSeconds(10));
        TransitPublishResult result = await publishTask.WaitAsync(completionTimeout.Token);

        Assert.Equal(TransitPublishStatus.Accepted, result.Status);
        Assert.Equal(239, result.ResponseCode);
        Assert.Equal(messageId, result.MessageId);
        Assert.Equal(TransitConnectionState.Publishing, connection.CurrentState);
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
            await WaitUntilCanceledAsync(cancellationToken);
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
        Assert.False(connection.IsCompressionActive);
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
        Assert.Contains("MODE STREAM rejected", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InitializeAsync_WhenCompressNegotiationRejected_Throws()
    {
        await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
        {
            await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
            await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
            await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
            await FakeNntpServer.WriteLineAsync(stream, ".");
            await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(stream, "503 compression unavailable");
        });

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
        Assert.Contains("COMPRESS DEFLATE negotiation failed", ex.Message, StringComparison.Ordinal);
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
        await using FakeNntpServer server = await FakeNntpServer.StartAsync(static (stream, cancellationToken) =>
        {
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
            await WaitUntilCanceledAsync(cancellationToken);
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
        Assert.False(connection.IsCompressionActive);
        Assert.True(connection.Capabilities.SupportsStreaming);
    }

    [Fact]
    public async Task InitializeAsync_WhenUseSslTrueAndCompressionAdvertised_UsesTlsThenCompression()
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
            await FakeNntpServer.WriteLineAsync(sslStream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(sslStream, "STREAMING");
            await FakeNntpServer.WriteLineAsync(sslStream, ".");

            await FakeNntpServer.ExpectCommandAsync(sslStream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(sslStream, "206 Compression enabled");

            using DeflateStream compressedRead = new(sslStream, CompressionMode.Decompress, leaveOpen: true);
            using DeflateStream compressedWrite = new(sslStream, CompressionMode.Compress, leaveOpen: true);

            await FakeNntpServer.ExpectCommandAsync(compressedRead, "MODE STREAM");
            await FakeNntpServer.WriteLineAsync(compressedWrite, "203 Streaming permitted");

            string takethis = await FakeNntpServer.ReadLineAsync(compressedRead, cancellationToken);
            Assert.Equal("TAKETHIS <ssl-compressed@example.com>", takethis);
            byte[] receivedPayload = await FakeNntpServer.ReadTakethisPayloadAsync(compressedRead, cancellationToken);
            Assert.Equal(payload, receivedPayload);
            await FakeNntpServer.WriteLineAsync(compressedWrite, "239 <ssl-compressed@example.com> transferred");
        });

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: true,
            NullLogger<TransitPublisher>.Instance,
            _tlsFixture.ServerCertificateValidationCallback);

        await connection.InitializeAsync(CancellationToken.None);
        TransitPublishResult result = await connection.SubmitTakethisAsync("<ssl-compressed@example.com>", payload, CancellationToken.None, 0L, 0L);

        Assert.True(connection.IsTlsActive);
        Assert.True(connection.IsCompressionActive);
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
    public async Task InitializeAsync_WhenStartTlsAdvertisedWithCompression_UpgradesToTlsThenEnablesCompression()
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
            await FakeNntpServer.WriteLineAsync(sslStream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(sslStream, "STREAMING");
            await FakeNntpServer.WriteLineAsync(sslStream, ".");

            await FakeNntpServer.ExpectCommandAsync(sslStream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(sslStream, "206 Compression enabled");

            using DeflateStream compressedRead = new(sslStream, CompressionMode.Decompress, leaveOpen: true);
            using DeflateStream compressedWrite = new(sslStream, CompressionMode.Compress, leaveOpen: true);

            await FakeNntpServer.ExpectCommandAsync(compressedRead, "MODE STREAM");
            await FakeNntpServer.WriteLineAsync(compressedWrite, "203 Streaming permitted");

            string takethis = await FakeNntpServer.ReadLineAsync(compressedRead, cancellationToken);
            Assert.Equal("TAKETHIS <starttls-compressed@example.com>", takethis);
            byte[] receivedPayload = await FakeNntpServer.ReadTakethisPayloadAsync(compressedRead, cancellationToken);
            Assert.Equal(payload, receivedPayload);

            await FakeNntpServer.WriteLineAsync(compressedWrite, "239 <starttls-compressed@example.com> transferred");
        });

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance,
            _tlsFixture.ServerCertificateValidationCallback);

        await connection.InitializeAsync(CancellationToken.None);
        TransitPublishResult result = await connection.SubmitTakethisAsync("<starttls-compressed@example.com>", payload, CancellationToken.None, 0L, 0L);

        Assert.True(connection.IsTlsActive);
        Assert.True(connection.IsCompressionActive);
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
    public async Task InitializeAsync_WhenCompressionAdvertised_EnablesCompressionAndPublishes()
    {
        await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
        {
            await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
            await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
            await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
            await FakeNntpServer.WriteLineAsync(stream, ".");

            await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(stream, "206 Compression enabled");

            using DeflateStream compressedRead = new(stream, CompressionMode.Decompress, leaveOpen: true);
            using DeflateStream compressedWrite = new(stream, CompressionMode.Compress, leaveOpen: true);

            await FakeNntpServer.ExpectCommandAsync(compressedRead, "MODE STREAM");
            await FakeNntpServer.WriteLineAsync(compressedWrite, "203 Streaming permitted");

            string takethis = await FakeNntpServer.ReadLineAsync(compressedRead, CancellationToken.None);
            Assert.Equal("TAKETHIS <compressed@example.com>", takethis);

            byte[] payload = await FakeNntpServer.ReadTakethisPayloadAsync(compressedRead, CancellationToken.None);
            Assert.Equal(new byte[] { (byte)'Z', (byte)'\n' }, payload);

            await FakeNntpServer.WriteLineAsync(compressedWrite, "239 <compressed@example.com> transferred");
        });

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        await connection.InitializeAsync(CancellationToken.None);
        TransitPublishResult result = await connection.SubmitTakethisAsync("<compressed@example.com>", new byte[] { (byte)'Z', (byte)'\n' }, CancellationToken.None, 0L, 0L);

        Assert.Equal(TransitConnectionState.Publishing, connection.CurrentState);
        Assert.True(connection.IsCompressionActive);
        Assert.Equal(TransitPublishStatus.Accepted, result.Status);
    }

    [Fact]
    public async Task InitializeAsync_WhenCompressionResponseAndExtraPlaintextBuffered_ThrowsTransitionSafetyError()
    {
        await using FakeNntpServer server = await FakeNntpServer.StartAsync(async (stream, _) =>
        {
            await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
            await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
            await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
            await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
            await FakeNntpServer.WriteLineAsync(stream, ".");

            await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
            await FakeNntpServer.WriteLinesAsync(stream,
            [
                "206 Compression enabled",
                "203 Unexpected plaintext line",
            ]);
        });

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
        Assert.Contains("Buffered NNTP data remained in PipeReader", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InitializeAsync_WhenCompressInteroperabilityFails_FallsBackToUncompressedReconnect()
    {
        FakeNntpServer.CapturingLoggerProvider loggerProvider = new();
        int sessionCount = 0;

        await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
        [
            async (stream, cancellationToken) =>
            {
                Interlocked.Increment(ref sessionCount);

                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "206 Compression enabled");

                using DeflateStream compressedRead = new(stream, CompressionMode.Decompress, leaveOpen: true);
                await FakeNntpServer.ExpectCommandAsync(compressedRead, "MODE STREAM");

                using ZLibStream zlibWrite = new(stream, CompressionMode.Compress, leaveOpen: true);
                await FakeNntpServer.WriteLineAsync(zlibWrite, "203 Streaming is OK");
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
            },
            async (stream, cancellationToken) =>
            {
                Interlocked.Increment(ref sessionCount);

                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "MODE STREAM");
                await FakeNntpServer.WriteLineAsync(stream, "203 Streaming permitted");
                await WaitUntilCanceledAsync(cancellationToken);
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            loggerProvider.CreateLogger<TransitPublisher>());

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        await connection.InitializeAsync(timeout.Token);

        Assert.Equal(2, sessionCount);
        Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
        Assert.False(connection.IsCompressionActive);
        Assert.Contains(loggerProvider.Entries, entry => entry.EventId.Id == 2214);
        Assert.Single(loggerProvider.Entries.Where(entry => entry.EventId.Id == 2214));
    }

    [Fact]
    public async Task InitializeAsync_WhenCancellationDuringCompressionNegotiation_ThrowsOperationCanceledWithoutFallback()
    {
        FakeNntpServer.CapturingLoggerProvider loggerProvider = new();
        int sessionCount = 0;

        await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
        [
            async (stream, cancellationToken) =>
            {
                Interlocked.Increment(ref sessionCount);

                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "206 Compression enabled");

                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            async (stream, _) =>
            {
                Interlocked.Increment(ref sessionCount);
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            loggerProvider.CreateLogger<TransitPublisher>());

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<OperationCanceledException>(() => connection.InitializeAsync(timeout.Token));
        Assert.Equal(1, sessionCount);
        Assert.DoesNotContain(loggerProvider.Entries, entry => entry.EventId.Id == 2214);
    }

    [Fact]
    public async Task InitializeAsync_WhenCompressedModeStreamReadGetsConnectionReset_DoesNotFallback()
    {
        FakeNntpServer.CapturingLoggerProvider loggerProvider = new();
        int sessionCount = 0;

        await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
        [
            async (stream, _) =>
            {
                Interlocked.Increment(ref sessionCount);

                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "206 Compression enabled");
                stream.Socket.Shutdown(SocketShutdown.Both);
                stream.Socket.Close();
            },
            async (stream, _) =>
            {
                Interlocked.Increment(ref sessionCount);
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            loggerProvider.CreateLogger<TransitPublisher>());

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAnyAsync<IOException>(() => connection.InitializeAsync(timeout.Token));

        Assert.Equal(1, sessionCount);
        Assert.DoesNotContain(loggerProvider.Entries, entry => entry.EventId.Id == 2214);
    }

    [Fact]
    public async Task InitializeAsync_WhenCompressedModeStreamReadGetsConnectionAborted_DoesNotFallback()
    {
        FakeNntpServer.CapturingLoggerProvider loggerProvider = new();
        int sessionCount = 0;

        await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
        [
            async (stream, _) =>
            {
                Interlocked.Increment(ref sessionCount);

                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "206 Compression enabled");
                stream.Socket.LingerState = new LingerOption(true, 0);
                stream.Socket.Close();
            },
            async (stream, _) =>
            {
                Interlocked.Increment(ref sessionCount);
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            loggerProvider.CreateLogger<TransitPublisher>());

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAnyAsync<IOException>(() => connection.InitializeAsync(timeout.Token));

        Assert.Equal(1, sessionCount);
        Assert.DoesNotContain(loggerProvider.Entries, entry => entry.EventId.Id == 2214);
    }

    [Fact]
    public async Task InitializeAsync_WhenCompressedModeStreamReadGetsEof_DoesNotFallback()
    {
        FakeNntpServer.CapturingLoggerProvider loggerProvider = new();
        int sessionCount = 0;

        await using FakeNntpServer server = await FakeNntpServer.StartSessionsAsync(
        [
            async (stream, _) =>
            {
                Interlocked.Increment(ref sessionCount);

                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
                await FakeNntpServer.ExpectCommandAsync(stream, "CAPABILITIES");
                await FakeNntpServer.WriteLineAsync(stream, "101 Capability list:");
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");

                await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "206 Compression enabled");
                stream.Close();
            },
            async (stream, _) =>
            {
                Interlocked.Increment(ref sessionCount);
                await FakeNntpServer.WriteLineAsync(stream, "200 transit ready");
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            loggerProvider.CreateLogger<TransitPublisher>());

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        Exception ex = await Record.ExceptionAsync(() => connection.InitializeAsync(timeout.Token));

        Assert.NotNull(ex);
        Assert.True(ex is InvalidOperationException or IOException);
        Assert.Equal(1, sessionCount);
        Assert.DoesNotContain(loggerProvider.Entries, entry => entry.EventId.Id == 2214);
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
                await WaitUntilCanceledAsync(cancellationToken);
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
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
                await WaitUntilCanceledAsync(cancellationToken);
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
        Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        await connection.InitializeAsync(timeout.Token);

        Assert.Equal(2, sessionCount);
        Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
    }

    [Fact]
    public async Task InitializeAsync_WhenCompressRejectedThenRetryOnSameInstance_SucceedsWithFreshTransport()
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
                await FakeNntpServer.WriteLineAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "STREAMING");
                await FakeNntpServer.WriteLineAsync(stream, ".");
                await FakeNntpServer.ExpectCommandAsync(stream, "COMPRESS DEFLATE");
                await FakeNntpServer.WriteLineAsync(stream, "503 compression unavailable");
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
                await WaitUntilCanceledAsync(cancellationToken);
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => connection.InitializeAsync(CancellationToken.None));
        Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        await connection.InitializeAsync(timeout.Token);

        Assert.Equal(2, sessionCount);
        Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
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
                await WaitUntilCanceledAsync(cancellationToken);
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
                await WaitUntilCanceledAsync(cancellationToken);
            },
        ]);

        await using TransitConnection connection = new(
            host: IPAddress.Loopback.ToString(),
            port: server.Port,
            useSsl: false,
            NullLogger<TransitPublisher>.Instance);

        using (CancellationTokenSource canceledInit = new(TimeSpan.FromMilliseconds(500)))
        {
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.InitializeAsync(canceledInit.Token));
        }

        Assert.Equal(TransitConnectionState.Disconnected, connection.CurrentState);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));
        await connection.InitializeAsync(timeout.Token);

        Assert.Equal(2, sessionCount);
        Assert.Equal(TransitConnectionState.Ready, connection.CurrentState);
    }

    private static async Task WaitUntilCanceledAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    public sealed class TlsCertificateFixture : IDisposable
    {
        private readonly X509Certificate2 _serverCertificate;
        private readonly string _serverCertificateThumbprint;

        public TlsCertificateFixture()
        {
            string pfxPassword = Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));

            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new("CN=127.0.0.1", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: false));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, critical: false));
            request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));

            SubjectAlternativeNameBuilder san = new();
            san.AddDnsName("localhost");
            san.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(san.Build());

            DateTimeOffset now = DateTimeOffset.UtcNow;
            using X509Certificate2 ephemeralCertificate = request.CreateSelfSigned(now.AddDays(-1), now.AddDays(7));
            byte[] pfx = ephemeralCertificate.Export(X509ContentType.Pkcs12, pfxPassword);

            _serverCertificate = new X509Certificate2(
                pfx,
                pfxPassword,
                X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable);

            _serverCertificateThumbprint = _serverCertificate.Thumbprint;
        }

        internal X509Certificate2 ServerCertificate => _serverCertificate;

        internal RemoteCertificateValidationCallback ServerCertificateValidationCallback => ValidateServerCertificate;

        private bool ValidateServerCertificate(object? sender, X509Certificate? certificate, X509Chain? chain, SslPolicyErrors sslPolicyErrors)
        {
            if (certificate is null)
            {
                return false;
            }

            string? thumbprint = certificate.GetCertHashString(HashAlgorithmName.SHA256);
            string? expectedThumbprint = _serverCertificate.GetCertHashString(HashAlgorithmName.SHA256);

            return string.Equals(thumbprint, expectedThumbprint, StringComparison.OrdinalIgnoreCase);
        }

        public void Dispose()
        {
            _serverCertificate.Dispose();
        }
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

            return System.Text.Encoding.ASCII.GetString(buffer.ToArray());
        }

        private static async ValueTask<byte> ReadByteAsync(Stream stream, CancellationToken cancellationToken)
        {
            byte[] single = new byte[1];
            int read = await stream.ReadAsync(single, cancellationToken);
            if (read == 0)
            {
                throw new InvalidOperationException("Unexpected EOF while reading stream data.");
            }

            return single[0];
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
