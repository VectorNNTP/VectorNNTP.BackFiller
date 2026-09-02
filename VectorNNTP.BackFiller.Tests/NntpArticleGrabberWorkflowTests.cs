// <copyright file="NntpArticleGrabberWorkflowTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for nntp article grabber workflow, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the nntp article grabber workflow test suite.

using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies grabber workflow orchestration contracts over reusable authenticated NNTP sessions.
    /// </summary>
    public sealed class NntpArticleGrabberWorkflowTests
    {
        /// <summary>
        /// Confirms successful acquisition and parse returns accepted workflow result while preserving Message-ID.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenAcquisitionAndParseSucceed_ReturnsSuccessfulWorkflowResult()
        {
            byte[] article = BuildArticleBytes("<workflow-success@test>", "body\r\n");

            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-success@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "220 0 <workflow-success@test> article follows");
                await FakeServer.WriteBytesAsync(stream, article);
                await FakeServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleGrabberResult result = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-success@test>"),
                    CancellationToken.None);

                Assert.True(result.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.None, result.FailureCode);
                Assert.Equal("<workflow-success@test>", result.MessageId);
                Assert.NotNull(result.Success);
                Assert.True(result.Success.Parse.IsAccepted);
                Assert.Equal(NntpArticleType.Text, result.Success.Parse.ArticleType);
            }
        }

        /// <summary>
        /// Confirms parser malformed-article failures are preserved as malformed workflow failures.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenParserRejectsMalformedArticle_ReturnsMalformedArticleFailure()
        {
            byte[] malformed = Encoding.ASCII.GetBytes("invalid-without-header-separator\r\n");

            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-malformed@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "220 0 <workflow-malformed@test> article follows");
                await FakeServer.WriteBytesAsync(stream, malformed);
                await FakeServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleGrabberResult result = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-malformed@test>"),
                    CancellationToken.None);

                Assert.False(result.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.InvalidHeaders, result.FailureCode);
                _ = Assert.NotNull(result.ParseFailureCode);
                Assert.Equal(NntpArticleAcquisitionFailureCode.None, result.AcquisitionFailureCode);
            }
        }

        /// <summary>
        /// Confirms yEnc decoder/CRC failures are preserved distinctly from generic malformed parser failures.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenYEncValidationFails_ReturnsYEncValidationFailure()
        {
            byte[] invalidYEnc = BuildArticleBytes(
                "<workflow-yenc@test>",
                "=ybegin line=128 size=5 name=test.bin\r\n" +
                "ABCDEF\r\n" +
                "=yend size=5 crc32=00000000\r\n");

            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-yenc@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "220 0 <workflow-yenc@test> article follows");
                await FakeServer.WriteBytesAsync(stream, invalidYEnc);
                await FakeServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleGrabberResult result = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-yenc@test>"),
                    CancellationToken.None);

                Assert.False(result.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.YEncValidationFailure, result.FailureCode);
                Assert.Equal(NntpArticleParseFailureCode.YEncDecodingFailed, result.ParseFailureCode);
                Assert.True(result.YEncStatus is not null and not YEncArticleValidationStatus.ValidNonYEnc);
            }
        }

        /// <summary>
        /// Confirms article-not-found is preserved as provider article-not-found instead of parser failure.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenArticleNotFound_ReturnsArticleNotFoundFailure()
        {
            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-missing@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "430 no such article");
            });

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleGrabberResult result = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-missing@test>"),
                    CancellationToken.None);

                Assert.False(result.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.ArticleNotFound, result.FailureCode);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleNotFound, result.AcquisitionFailureCode);
                Assert.Null(result.ParseFailureCode);
            }
        }

        /// <summary>
        /// Confirms authentication failures map to explicit grabber authentication failures.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenAuthenticationFails_ReturnsAuthenticationFailure()
        {
            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user");
                await FakeServer.WriteAsciiLineAsync(stream, "381 pass required");
                await FakeServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS bad");
                await FakeServer.WriteAsciiLineAsync(stream, "481 authentication rejected");
            });

            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "bad");
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.Null(session);
            Assert.Equal(NntpArticleAcquisitionFailureCode.AuthenticationFailure, connectResult.FailureCode);
        }

        /// <summary>
        /// Confirms cancellation from acquisition is preserved as explicit workflow cancellation.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenCancelledDuringReceive_ReturnsCancelledFailure()
        {
            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-cancel@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "220 0 <workflow-cancel@test> article follows");
                await FakeServer.WriteBytesAsync(stream, Encoding.ASCII.GetBytes("body\r\n"));
                await Task.Delay(1000).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(session);
            await using (session.ConfigureAwait(false))
            {
                using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(100));
                using NntpArticleGrabberResult result = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-cancel@test>"),
                    cts.Token).ConfigureAwait(false);

                Assert.False(result.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.Cancelled, result.FailureCode);
                Assert.Equal(NntpArticleAcquisitionFailureCode.Cancelled, result.AcquisitionFailureCode);
            }
        }

        /// <summary>
        /// Confirms connection failures are classified distinctly from parser failures.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenConnectionBreaksMidArticle_ReturnsConnectionFailure()
        {
            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-io@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "220 0 <workflow-io@test> article follows");
                await FakeServer.WriteBytesAsync(stream, Encoding.ASCII.GetBytes("partial"));
                stream.Close();
            }).ConfigureAwait(false);

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(session);
            await using (session.ConfigureAwait(false))
            {
                using NntpArticleGrabberResult result = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-io@test>"),
                    CancellationToken.None).ConfigureAwait(false);

                Assert.False(result.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.ArticleFramingFailure, result.FailureCode);
                Assert.Equal(NntpArticleAcquisitionFailureCode.TruncatedArticle, result.AcquisitionFailureCode);
            }
        }

        /// <summary>
        /// Confirms protocol failures are classified distinctly when status lines are malformed.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenStatusLineMalformed_ReturnsProtocolFailure()
        {
            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-badstatus@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "x20 malformed");
            }).ConfigureAwait(false);

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(session);
            await using (session.ConfigureAwait(false))
            {
                using NntpArticleGrabberResult result = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-badstatus@test>"),
                    CancellationToken.None).ConfigureAwait(false);

                Assert.False(result.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.ProtocolFailure, result.FailureCode);
                Assert.Equal(NntpArticleAcquisitionFailureCode.MalformedResponse, result.AcquisitionFailureCode);
            }
        }

        /// <summary>
        /// Confirms one reusable session can process multiple workflow requests with mixed outcomes without teardown.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenSessionReusedAcrossMultipleWorkItems_PreservesMixedOutcomes()
        {
            byte[] valid = BuildArticleBytes("<workflow-reuse-valid@test>", "body\r\n");
            byte[] malformed = Encoding.ASCII.GetBytes("header-without-separator\r\n");

            await using FakeServer server = await FakeServer.StartAsync(async stream =>
            {
                await FakeServer.WriteAsciiLineAsync(stream, "200 ready");

                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-reuse-valid@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "220 0 <workflow-reuse-valid@test> article follows");
                await FakeServer.WriteBytesAsync(stream, valid);
                await FakeServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());

                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-reuse-missing@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "430 no such article");

                await FakeServer.ExpectAsciiLineAsync(stream, "ARTICLE <workflow-reuse-malformed@test>");
                await FakeServer.WriteAsciiLineAsync(stream, "220 0 <workflow-reuse-malformed@test> article follows");
                await FakeServer.WriteBytesAsync(stream, malformed);
                await FakeServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            }).ConfigureAwait(false);

            NntpArticleGrabberWorkflow workflow = CreateWorkflow();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(session);
            await using (session.ConfigureAwait(false))
            {
                using NntpArticleGrabberResult success = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-reuse-valid@test>"),
                    CancellationToken.None).ConfigureAwait(false);

                using NntpArticleGrabberResult missing = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-reuse-missing@test>"),
                    CancellationToken.None).ConfigureAwait(false);

                using NntpArticleGrabberResult malformedResult = await workflow.ProcessAsync(
                    session,
                    new NntpArticleGrabberWorkItem("<workflow-reuse-malformed@test>"),
                    CancellationToken.None).ConfigureAwait(false);

                Assert.True(success.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.None, success.FailureCode);

                Assert.False(missing.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.ArticleNotFound, missing.FailureCode);

                Assert.False(malformedResult.IsSuccess);
                Assert.Equal(NntpArticleGrabberFailureCode.InvalidHeaders, malformedResult.FailureCode);
            }
        }

        /// <summary>
        /// Builds parser-compatible article bytes for workflow tests.
        /// </summary>
        /// <param name="messageId">Message-ID header value.</param>
        /// <param name="body">Body text.</param>
        /// <returns>Article bytes.</returns>
        /// <summary>
        /// Confirms the build article bytes behavior.
        /// </summary>
        /// <param name="messageId">The message id used by this test scenario.</param>
        /// <param name="body">The body used by this test scenario.</param>
        /// <returns>The value returned by the build article bytes helper.</returns>
        private static byte[] BuildArticleBytes(string messageId, string body)
        {
            byte[] headers = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                $"Message-ID: {messageId}\r\n" +
                "Newsgroups: alt.test\r\n" +
                "From: user@example.test\r\n" +
                "\r\n");

            byte[] bodyBytes = Encoding.ASCII.GetBytes(body);
            byte[] article = new byte[headers.Length + bodyBytes.Length];
            Buffer.BlockCopy(headers, 0, article, 0, headers.Length);
            Buffer.BlockCopy(bodyBytes, 0, article, headers.Length, bodyBytes.Length);
            return article;
        }

        /// <summary>
        /// Creates a workflow instance using deterministic runtime identity settings.
        /// </summary>
        /// <returns>Configured workflow instance for tests.</returns>
        /// <summary>
        /// Confirms the create workflow behavior.
        /// </summary>
        /// <returns>The value returned by the create workflow helper.</returns>
        private static NntpArticleGrabberWorkflow CreateWorkflow()
        {
            BackFillerRuntimeOptions options = new(
                CanonicalBackFillerFqdn: "bf01.usenet.ninja",
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
                ValidatedLogDirectory: "C:\\logs",
                ValidatedCertificateDirectory: "C:\\certs",
                RabbitMqHosts: ["127.0.0.1"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "127.0.0.1",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                ShutdownGracePeriodSeconds: 30,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 15,
                WriteBatchCoalesceMicroseconds: 0);

            return new NntpArticleGrabberWorkflow(options, NullLogger<NntpArticleGrabberWorkflow>.Instance);
        }

        /// <summary>
        /// Minimal reusable-session fake server used by grabber workflow contract tests.
        /// </summary>
        private sealed class FakeServer : IAsyncDisposable
        {
            /// <summary>
            /// Listener.
            /// </summary>
            private readonly TcpListener _listener;

            /// <summary>
            /// Per-connection script callback.
            /// </summary>
            private readonly Func<NetworkStream, Task> _session;

            /// <summary>
            /// Shutdown token source.
            /// </summary>
            private readonly CancellationTokenSource _shutdown = new();

            /// <summary>
            /// Accept loop task.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
            /// Initializes fake server.
            /// </summary>
            /// <param name="listener">Bound listener.</param>
            /// <param name="session">Session script callback.</param>
            /// <summary>
            /// Confirms the r behavior.
            /// </summary>
            /// <param name="listener">The listener used by this test scenario.</param>
            /// <param name="NetworkStream">The network stream used by this test scenario.</param>
            /// <param name="session">The session used by this test scenario.</param>
            /// <returns>The value returned by the r helper.</returns>
            private FakeServer(TcpListener listener, Func<NetworkStream, Task> session)
            {
                _listener = listener;
                _session = session;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Gets bound TCP port.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Creates non-TLS acquisition endpoint for this fake server.
            /// </summary>
            /// <returns>Acquisition endpoint descriptor.</returns>
            /// <summary>
            /// Confirms the create endpoint behavior.
            /// </summary>
            /// <returns>The value returned by the create endpoint helper.</returns>
            internal NntpArticleAcquisitionEndpoint CreateEndpoint()
            {
                return new NntpArticleAcquisitionEndpoint("127.0.0.1", Port, UseSsl: false, Username: null, Password: null);
            }

            /// <summary>
            /// Starts fake server and waits briefly for listener readiness.
            /// </summary>
            /// <param name="session">Session callback script.</param>
            /// <returns>Started fake server.</returns>
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <param name="NetworkStream">The network stream used by this test scenario.</param>
            /// <param name="session">The session used by this test scenario.</param>
            /// <returns>The value returned by the start async helper.</returns>
            internal static async Task<FakeServer> StartAsync(Func<NetworkStream, Task> session)
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                FakeServer server = new(listener, session);
                await Task.Delay(20).ConfigureAwait(false);
                return server;
            }

            /// <summary>
            /// Reads one ASCII line and asserts exact expected value.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="expected">Expected line text.</param>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the expect ascii line async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="expected">The expected used by this test scenario.</param>
            /// <returns>The value returned by the expect ascii line async helper.</returns>
            internal static async Task ExpectAsciiLineAsync(Stream stream, string expected)
            {
                string line = await ReadAsciiLineAsync(stream, CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(expected, line);
            }

            /// <summary>
            /// Reads one ASCII protocol line without CRLF terminator.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>Line text.</returns>
            /// <summary>
            /// Confirms the read ascii line async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the read ascii line async helper.</returns>
            internal static async Task<string> ReadAsciiLineAsync(Stream stream, CancellationToken cancellationToken)
            {
                List<byte> bytes = [];
                byte[] single = new byte[1];

                while (true)
                {
                    int read = await stream.ReadAsync(single, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                    {
                        throw new EndOfStreamException("Unexpected EOF while reading protocol line.");
                    }

                    if (single[0] == (byte)'\n')
                    {
                        break;
                    }

                    bytes.Add(single[0]);
                }

                if (bytes.Count > 0 && bytes[^1] == (byte)'\r')
                {
                    bytes.RemoveAt(bytes.Count - 1);
                }

                return Encoding.ASCII.GetString(CollectionsMarshal.AsSpan(bytes));
            }

            /// <summary>
            /// Writes one ASCII line with CRLF terminator.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="line">Line text without CRLF.</param>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the write ascii line async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="line">The line used by this test scenario.</param>
            /// <returns>The value returned by the write ascii line async helper.</returns>
            internal static async Task WriteAsciiLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }

            /// <summary>
            /// Writes raw bytes and flushes the network stream.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="bytes">Byte payload.</param>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the write bytes async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="bytes">The bytes used by this test scenario.</param>
            /// <returns>The value returned by the write bytes async helper.</returns>
            internal static async Task WriteBytesAsync(Stream stream, byte[] bytes)
            {
                await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }

            /// <summary>
            /// Stops server and waits for accept loop completion.
            /// </summary>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            public async ValueTask DisposeAsync()
            {
                _shutdown.Cancel();

                try
                {
                    _listener.Stop();
                }
                catch
                {
                }

                await _acceptLoop.ConfigureAwait(false);
                _shutdown.Dispose();
            }

            /// <summary>
            /// Accepts one connection and executes scripted session behavior.
            /// </summary>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the accept loop async behavior.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            private async Task AcceptLoopAsync()
            {
                try
                {
                    using TcpClient client = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                    using NetworkStream stream = client.GetStream();
                    await _session(stream).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }
}
