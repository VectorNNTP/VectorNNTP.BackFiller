// <copyright file="NntpArticleAcquisitionTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for nntp article acquisition, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the nntp article acquisition test suite.

using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.Articles.YEnc;
using VectorNNTP.BackFiller.Tests.TestInfrastructure.Certificates;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Verifies acquisition contract semantics, session reuse behavior, and logging requirements.
    /// </summary>
    public sealed class NntpArticleAcquisitionTests
    {
        /// <summary>
        /// Validates representative valid Message-ID grammar forms.
        /// </summary>
        [Fact]
        public void MessageIdValidation_WhenRepresentativeValidForms_ReturnsTrue()
        {
            Assert.True(NntpMessageIdValidation.IsValidMessageId("<abc@example.com>"));
            Assert.True(NntpMessageIdValidation.IsValidMessageId("<part.one+tag@news-server.example.net>"));
            Assert.True(NntpMessageIdValidation.IsValidMessageId("<name@[127.0.0.1]>"));
        }

        /// <summary>
        /// Validates representative invalid Message-ID grammar forms.
        /// </summary>
        [Fact]
        public void MessageIdValidation_WhenRepresentativeInvalidForms_ReturnsFalse()
        {
            Assert.False(NntpMessageIdValidation.IsValidMessageId("abc@example.com"));
            Assert.False(NntpMessageIdValidation.IsValidMessageId("<double..dot@example.com>"));
            Assert.False(NntpMessageIdValidation.IsValidMessageId("<\"quoted\"@example.com>"));
            Assert.False(NntpMessageIdValidation.IsValidMessageId("<toolong" + new string('a', 400) + "@example.com>"));
        }

        /// <summary>
        /// Confirms session reuse executes multiple ARTICLE requests over one authenticated connection.
        /// </summary>
        [Fact]
        public async Task SessionReuse_WhenMultipleArticlesRequested_AuthenticatesOnceAndReusesConnection()
        {
            byte[] firstArticle = BuildArticleBytes("<first@test>", "first-body\r\n");
            byte[] secondArticle = BuildArticleBytes("<second@test>", "second-body\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "381 pass required");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS pass");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "281 auth accepted");

                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <first@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <first@test> article follows");
                await FakeArticleServer.WriteBytesAsync(stream, firstArticle);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());

                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <second@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <second@test> article follows");
                await FakeArticleServer.WriteBytesAsync(stream, secondArticle);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "pass");
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            Assert.Equal(NntpArticleAcquisitionFailureCode.None, connectResult.FailureCode);
            Assert.Null(connectResult.ArticleBuffer);
            Assert.False(connectResult.IsSuccess);

            await using (session)
            {
                using NntpArticleAcquisitionResult first = await session.DownloadArticleAsync("<first@test>", CancellationToken.None);
                using NntpArticleAcquisitionResult second = await session.DownloadArticleAsync("<second@test>", CancellationToken.None);

                Assert.True(first.IsSuccess);
                Assert.True(second.IsSuccess);
                Assert.Equal(firstArticle, first.ArticleBytes.ToArray());
                Assert.Equal(secondArticle, second.ArticleBytes.ToArray());
            }
        }

        /// <summary>
        /// Confirms article-not-found does not force reconnect and subsequent article can succeed.
        /// </summary>
        [Fact]
        public async Task SessionReuse_When430ThenSuccess_SubsequentArticleSucceedsWithoutReconnect()
        {
            byte[] secondArticle = BuildArticleBytes("<exists@test>", "body\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <missing@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "430 no such article");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <exists@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <exists@test> article follows");
                await FakeArticleServer.WriteBytesAsync(stream, secondArticle);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult missing = await session.DownloadArticleAsync("<missing@test>", CancellationToken.None);
                using NntpArticleAcquisitionResult exists = await session.DownloadArticleAsync("<exists@test>", CancellationToken.None);

                Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleNotFound, missing.FailureCode);
                Assert.True(exists.IsSuccess);
                Assert.Equal(secondArticle, exists.ArticleBytes.ToArray());
            }
        }

        /// <summary>
        /// Confirms status-only success factory preserves protocol status details without creating a payload-bearing success.
        /// </summary>
        [Fact]
        public void StatusSuccess_WhenCreated_UsesNoneWithoutPayloadAndKeepsIsSuccessFalse()
        {
            using NntpArticleAcquisitionResult result = NntpArticleAcquisitionResult.StatusSuccess(111, "20260826010101");
            Assert.Equal(NntpArticleAcquisitionFailureCode.None, result.FailureCode);
            Assert.Equal(111, result.ResponseCode);
            Assert.Equal("20260826010101", result.ResponseText);
            Assert.Null(result.ArticleBuffer);
            Assert.False(result.IsSuccess);
        }

        /// <summary>
        /// Confirms ARTICLE status classification reports command acceptance as status-only success before payload acquisition.
        /// </summary>
        [Fact]
        public void ClassifyArticleStatus_WhenStatus220_ReturnsStatusSuccessWithoutPayload()
        {
            MethodInfo? classify = typeof(NntpArticleAcquisitionSession).GetMethod("ClassifyArticleStatus", BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(classify);

            object? raw = classify.Invoke(null, [220, "article follows"]);
            NntpArticleAcquisitionResult result = Assert.IsType<NntpArticleAcquisitionResult>(raw);
            using (result)
            {
                Assert.Equal(NntpArticleAcquisitionFailureCode.None, result.FailureCode);
                Assert.Equal(220, result.ResponseCode);
                Assert.Equal("article follows", result.ResponseText);
                Assert.Null(result.ArticleBuffer);
                Assert.False(result.IsSuccess);
            }
        }

        /// <summary>
        /// Confirms DATE status classification reports command success as status-only success without article payload.
        /// </summary>
        [Fact]
        public void ClassifyDateStatus_WhenStatus111_ReturnsStatusSuccessWithoutPayload()
        {
            MethodInfo? classify = typeof(NntpArticleAcquisitionSession).GetMethod("ClassifyDateStatus", BindingFlags.NonPublic | BindingFlags.Static);
            Assert.NotNull(classify);

            object? raw = classify.Invoke(null, [111, "20260826010101"]);
            NntpArticleAcquisitionResult result = Assert.IsType<NntpArticleAcquisitionResult>(raw);
            using (result)
            {
                Assert.Equal(NntpArticleAcquisitionFailureCode.None, result.FailureCode);
                Assert.Equal(111, result.ResponseCode);
                Assert.Equal("20260826010101", result.ResponseText);
                Assert.Null(result.ArticleBuffer);
                Assert.False(result.IsSuccess);
            }
        }

        /// <summary>
        /// Confirms attach is rejected after terminal disposal and failed attach ownership remains with the caller.
        /// </summary>
        [Fact]
        public void TryAttachArticleBuffer_WhenDisposed_ReturnsFalseAndCallerRetainsOwnership()
        {
            NntpArticleAcquisitionResult result = NntpArticleAcquisitionResult.StatusSuccess(111, "status");
            result.Dispose();

            DownloadedArticleBuffer detachedOwner = CreateDownloadedArticleBuffer("payload-after-dispose");
            bool attached = result.TryAttachArticleBuffer(detachedOwner);

            Assert.False(attached);
            Assert.Null(result.TryDetachArticleBuffer());

            detachedOwner.Dispose();
            _ = Assert.Throws<ObjectDisposedException>(() =>
            {
                _ = detachedOwner.Memory;
            });
        }

        /// <summary>
        /// Confirms detached ownership can be reattached and detached again while the result remains valid.
        /// </summary>
        [Fact]
        public void TryAttachArticleBuffer_WhenValid_SupportsDetachAttachDetachLifecycle()
        {
            using NntpArticleAcquisitionResult result = NntpArticleAcquisitionResult.StatusSuccess(111, "status");
            using DownloadedArticleBuffer firstOwner = CreateDownloadedArticleBuffer("first-payload");
            using DownloadedArticleBuffer secondOwner = CreateDownloadedArticleBuffer("second-payload");

            Assert.True(result.TryAttachArticleBuffer(firstOwner));
            DownloadedArticleBuffer detachedFirst = Assert.IsType<DownloadedArticleBuffer>(result.TryDetachArticleBuffer());
            Assert.Same(firstOwner, detachedFirst);

            Assert.True(result.TryAttachArticleBuffer(secondOwner));
            DownloadedArticleBuffer detachedSecond = Assert.IsType<DownloadedArticleBuffer>(result.TryDetachArticleBuffer());
            Assert.Same(secondOwner, detachedSecond);
        }

        /// <summary>
        /// Confirms concurrent attach and dispose produce one terminal ownership outcome without payload leaks.
        /// </summary>
        [Fact]
        public async Task TryAttachArticleBuffer_WhenConcurrentWithDispose_PreservesTerminalOwnershipContractAsync()
        {
            NntpArticleAcquisitionResult result = NntpArticleAcquisitionResult.StatusSuccess(111, "status");
            DownloadedArticleBuffer candidateOwner = CreateDownloadedArticleBuffer("candidate-payload");
            Barrier barrier = new(2);

            Task disposeTask = Task.Run(() =>
            {
                barrier.SignalAndWait();
                result.Dispose();
            });

            Task<bool> attachTask = Task.Run(() =>
            {
                barrier.SignalAndWait();
                return result.TryAttachArticleBuffer(candidateOwner);
            });

            await Task.WhenAll(disposeTask, attachTask).ConfigureAwait(false);

            bool attached = attachTask.Result;
            Assert.Null(result.TryDetachArticleBuffer());

            if (!attached)
            {
                candidateOwner.Dispose();
            }

            _ = Assert.Throws<ObjectDisposedException>(() =>
            {
                _ = candidateOwner.Memory;
            });
        }

        /// <summary>
        /// Confirms invalid Message-ID is rejected before protocol command emission.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenMessageIdInvalid_ReturnsInvalidMessageIdWithoutSendingCommand()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await Task.Delay(100).ConfigureAwait(false);
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("invalid-message-id", CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.InvalidMessageId, result.FailureCode);
            }
        }

        /// <summary>
        /// Confirms malformed status lines are classified as malformed response failures.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenStatusMalformed_ReturnsMalformedResponse()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <badstatus@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "x20 malformed");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<badstatus@test>", CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.MalformedResponse, result.FailureCode);
            }
        }

        /// <summary>
        /// Confirms ARTICLE command-unavailable responses are classified as remote rejection with raw status preserved.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenArticleCommandUnavailable_ReturnsRemoteRejectedWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <unavailable@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "500 command not understood");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<unavailable@test>", CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.RemoteRejected, result.FailureCode);
                Assert.Equal(500, result.ResponseCode);
                Assert.Equal("command not understood", result.ResponseText);
            }
        }

        /// <summary>
        /// Confirms unexpected but syntactically valid ARTICLE responses are treated as protocol failures.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenArticleUnexpectedStatus_ReturnsProtocolFailureWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <unexpected@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "111 20260826010101");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<unexpected@test>", CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ProtocolFailure, result.FailureCode);
                Assert.Equal(111, result.ResponseCode);
                Assert.Equal("20260826010101", result.ResponseText);
            }
        }

        /// <summary>
        /// Confirms AUTHINFO PASS authentication rejection retains raw NNTP status and deterministic authentication failure classification.
        /// </summary>
        [Fact]
        public async Task ConnectAsync_WhenAuthInfoPassRejected_ReturnsAuthenticationFailureWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "381 pass required");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS bad");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "481 authentication rejected");
            });

            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "bad");
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.Null(session);
            Assert.Equal(NntpArticleAcquisitionFailureCode.AuthenticationFailure, connectResult.FailureCode);
            Assert.Equal(481, connectResult.ResponseCode);
            Assert.Equal("authentication rejected", connectResult.ResponseText);
        }

        /// <summary>
        /// Confirms AUTHINFO USER authentication rejection retains raw NNTP status and deterministic authentication failure classification.
        /// </summary>
        [Fact]
        public async Task ConnectAsync_WhenAuthInfoUserRejected_ReturnsAuthenticationFailureWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "481 authentication rejected");
            });

            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "bad");
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.Null(session);
            Assert.Equal(NntpArticleAcquisitionFailureCode.AuthenticationFailure, connectResult.FailureCode);
            Assert.Equal(481, connectResult.ResponseCode);
            Assert.Equal("authentication rejected", connectResult.ResponseText);
        }

        /// <summary>
        /// Confirms AUTHINFO USER protocol-level unexpected responses are not treated as authentication failures.
        /// </summary>
        [Fact]
        public async Task ConnectAsync_WhenAuthInfoUserUnexpectedStatus_ReturnsProtocolFailureWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "211 list follows");
            });

            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "pass");
            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.Null(session);
            Assert.Equal(NntpArticleAcquisitionFailureCode.ProtocolFailure, connectResult.FailureCode);
            Assert.Equal(211, connectResult.ResponseCode);
            Assert.Equal("list follows", connectResult.ResponseText);
        }

        /// <summary>
        /// Confirms DATE keepalive accepts only the command-specific 111 status as success.
        /// </summary>
        [Fact]
        public async Task KeepAliveWithDateAsync_WhenStatus111_ReturnsSuccessWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "DATE");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "111 20260826010101");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.KeepAliveWithDateAsync(CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.None, result.FailureCode);
                Assert.Equal(111, result.ResponseCode);
                Assert.Equal("20260826010101", result.ResponseText);
                Assert.Null(result.ArticleBuffer);
                Assert.False(result.IsSuccess);
            }
        }

        /// <summary>
        /// Confirms DATE keepalive classifies unsupported command responses as remote rejection and preserves raw status.
        /// </summary>
        [Fact]
        public async Task KeepAliveWithDateAsync_WhenStatus500_ReturnsRemoteRejectedWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "DATE");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "500 command not understood");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.KeepAliveWithDateAsync(CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.RemoteRejected, result.FailureCode);
                Assert.Equal(500, result.ResponseCode);
                Assert.Equal("command not understood", result.ResponseText);
            }
        }

        /// <summary>
        /// Confirms DATE keepalive treats syntactically valid but command-unexpected statuses as protocol failures.
        /// </summary>
        [Fact]
        public async Task KeepAliveWithDateAsync_WhenUnexpectedStatus_ReturnsProtocolFailureWithRawStatus()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "DATE");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 article follows");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.KeepAliveWithDateAsync(CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ProtocolFailure, result.FailureCode);
                Assert.Equal(220, result.ResponseCode);
                Assert.Equal("article follows", result.ResponseText);
            }
        }

        /// <summary>
        /// Confirms malformed DATE status lines are classified as malformed responses.
        /// </summary>
        [Fact]
        public async Task KeepAliveWithDateAsync_WhenStatusMalformed_ReturnsMalformedResponse()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "DATE");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "x11 malformed");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.KeepAliveWithDateAsync(CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.MalformedResponse, result.FailureCode);
            }
        }

        /// <summary>
        /// Confirms acquisition removes one transport dot only for line-start dot-stuffed payload lines.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenDotStuffed_UnstuffsExactlyOneLeadingTransportDot()
        {
            byte[] wireArticle = BuildArticleBytes("<dot@test>", ".begins\r\n..double\r\n...triple\r\nmiddle.dot\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <dot@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <dot@test> article follows");
                await FakeArticleServer.WriteBytesAsync(stream, wireArticle);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<dot@test>", CancellationToken.None);
                Assert.True(result.IsSuccess);
                string text = Encoding.ASCII.GetString(result.ArticleBytes.Span);
                Assert.Contains("\r\n.begins\r\n", text, StringComparison.Ordinal);
                Assert.Contains("\r\n.double\r\n", text, StringComparison.Ordinal);
                Assert.Contains("\r\n..triple\r\n", text, StringComparison.Ordinal);
                Assert.Contains("\r\nmiddle.dot\r\n", text, StringComparison.Ordinal);
                Assert.DoesNotContain("\r\n...triple\r\n", text, StringComparison.Ordinal);
            }
        }

        /// <summary>
        /// Confirms fragmented multiline payload across multiple socket writes is reconstructed correctly.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenPayloadFragmentedAcrossWrites_ReconstructsArticle()
        {
            byte[] article = BuildArticleBytes("<fragment@test>", "line1\r\nline2\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <fragment@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <fragment@test> article follows");

                for (int i = 0; i < article.Length; i += 3)
                {
                    int take = Math.Min(3, article.Length - i);
                    byte[] slice = article.AsSpan(i, take).ToArray();
                    await FakeArticleServer.WriteBytesAsync(stream, slice).ConfigureAwait(false);
                }

                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<fragment@test>", CancellationToken.None);
                Assert.True(result.IsSuccess);
                Assert.Equal(article, result.ArticleBytes.ToArray());
            }
        }

        /// <summary>
        /// Proves standalone validator and real acquisition-parser-canonical path agree for dot-prefixed yEnc payload lines.
        /// </summary>
        [Fact]
        public async Task DotStuffingContract_WhenValidatedStandaloneAndThroughAcquisition_MatchesDecodedBytesCrcAndCanonicalBody()
        {
            const string messageId = "<issue63-dot-contract@test>";

            byte[] firstLineEncodedTailBytes = [0x21, 0x13];
            byte[] secondLineEncodedTailBytes = [0x2F, 0x10];
            byte[] thirdLineEncodedTailBytes = [0x2A, 0x01];

            string logicalLineStartingWithSingleDot = "." + EncodeYEncLine(firstLineEncodedTailBytes.AsSpan(0, 1)) + "." + EncodeYEncLine(firstLineEncodedTailBytes.AsSpan(1, 1));
            string logicalLineStartingWithDoubleDot = ".." + EncodeYEncLine(secondLineEncodedTailBytes);
            string logicalLineStartingWithTripleDot = "..." + EncodeYEncLine(thirdLineEncodedTailBytes);

            Assert.StartsWith(".", logicalLineStartingWithSingleDot, StringComparison.Ordinal);
            Assert.StartsWith("..", logicalLineStartingWithDoubleDot, StringComparison.Ordinal);
            Assert.StartsWith("...", logicalLineStartingWithTripleDot, StringComparison.Ordinal);
            Assert.Contains(".", logicalLineStartingWithSingleDot.AsSpan(1).ToString(), StringComparison.Ordinal);
            Assert.Contains("=", logicalLineStartingWithSingleDot, StringComparison.Ordinal);

            byte[] expectedDecodedBytes = [
                0x04,
                0x21,
                0x04,
                0x13,
                0x04,
                0x04,
                0x2F,
                0x10,
                0x04,
                0x04,
                0x04,
                0x2A,
                0x01,
            ];

            uint expectedCrc = Crc32(expectedDecodedBytes);
            string logicalEncodedPayload = string.Join(
                "\r\n",
                logicalLineStartingWithSingleDot,
                logicalLineStartingWithDoubleDot,
                logicalLineStartingWithTripleDot) + "\r\n";

            byte[] logicalPayloadBytes = Encoding.ASCII.GetBytes(logicalEncodedPayload);
            byte[] wirePayloadBytes = DotStuffLineStarts(logicalPayloadBytes);
            string wireEncodedPayload = Encoding.ASCII.GetString(wirePayloadBytes);

            byte[] logicalBody = Encoding.ASCII.GetBytes($"=ybegin line=128 size={expectedDecodedBytes.Length} name=issue63.bin\r\n{logicalEncodedPayload}=yend size={expectedDecodedBytes.Length} crc32={expectedCrc:x8}\r\n");
            byte[] wireArticle = BuildArticleBytes(messageId, Encoding.ASCII.GetBytes($"=ybegin line=128 size={expectedDecodedBytes.Length} name=issue63.bin\r\n{wireEncodedPayload}=yend size={expectedDecodedBytes.Length} crc32={expectedCrc:x8}\r\n"));

            YEncArticleValidationResult standaloneValidation = YEncArticleValidator.Validate(logicalBody);
            Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, standaloneValidation.Status);

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, $"ARTICLE {messageId}");
                await FakeArticleServer.WriteAsciiLineAsync(stream, $"220 0 {messageId} article follows");
                await FakeArticleServer.WriteBytesAsync(stream, wireArticle);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult acquisition = await session.DownloadArticleAsync(messageId, CancellationToken.None);
                Assert.True(acquisition.IsSuccess);

                ReadOnlyMemory<byte> acquiredArticle = acquisition.ArticleBytes;
                string acquiredText = Encoding.ASCII.GetString(acquiredArticle.Span);
                Assert.Contains($"\r\n{logicalEncodedPayload}=yend", acquiredText, StringComparison.Ordinal);
                Assert.DoesNotContain($"\r\n{wireEncodedPayload}=yend", acquiredText, StringComparison.Ordinal);
                Assert.DoesNotContain("\r\n.\r\n", acquiredText, StringComparison.Ordinal);

                NntpArticleParser parser = new("bf01.usenet.ninja");
                NntpArticleParseResult parseResult = parser.Parse(acquiredArticle);
                Assert.True(parseResult.IsAccepted);
                Assert.True(parseResult.YEncDetected);
                Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, parseResult.YEncValidation.Status);

                YEncArticleValidationResult reparsedBodyValidation = YEncArticleValidator.Validate(parseResult.BodyBytes.Span);
                Assert.Equal(YEncArticleValidationStatus.ValidSinglePart, reparsedBodyValidation.Status);

                byte[] standaloneDecoded = DecodeSinglePartYEncBody(logicalBody);
                byte[] acquiredDecoded = DecodeSinglePartYEncBody(parseResult.BodyBytes.Span.ToArray());
                Assert.Equal(expectedDecodedBytes, standaloneDecoded);
                Assert.Equal(expectedDecodedBytes, acquiredDecoded);
                Assert.Equal(expectedCrc, Crc32(standaloneDecoded));
                Assert.Equal(expectedCrc, Crc32(acquiredDecoded));
                Assert.Equal(expectedDecodedBytes.Length, standaloneDecoded.Length);
                Assert.Equal(expectedDecodedBytes.Length, acquiredDecoded.Length);

                using DownloadedArticleBuffer canonical = NntpArticleCanonicalMaterializer.Materialize(parseResult);
                byte[] canonicalBody = GetBodyBytes(canonical.Memory.Span);
                Assert.Equal(parseResult.BodyBytes.ToArray(), canonicalBody);
                Assert.Equal(logicalBody, canonicalBody);
            }
        }

        /// <summary>
        /// Confirms article-size guardrails are enforced deterministically.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenArticleTooLarge_ReturnsArticleTooLarge()
        {
            byte[] article = BuildArticleBytes("<huge@test>", new string('A', 16_384) + "\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <huge@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <huge@test> article follows");
                await FakeArticleServer.WriteBytesAsync(stream, article);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            NntpArticleAcquisitionOptions options = NntpArticleAcquisitionOptions.Default with { MaxArticleBytes = 4096 };
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                options,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<huge@test>", CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleTooLarge, result.FailureCode);
            }
        }

        /// <summary>
        /// Confirms cancellation while receiving article is classified as cancelled.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenCancelledDuringReceive_ReturnsCancelled()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <cancel@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <cancel@test> article follows");
                await FakeArticleServer.WriteBytesAsync(stream, Encoding.ASCII.GetBytes("line\r\n"));
                await Task.Delay(1000).ConfigureAwait(false);
            }).ConfigureAwait(false);

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(session);
            await using (session.ConfigureAwait(false))
            {
                using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(100));
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<cancel@test>", cts.Token).ConfigureAwait(false);
                Assert.Equal(NntpArticleAcquisitionFailureCode.Cancelled, result.FailureCode);
            }
        }

        /// <summary>
        /// Confirms timeout while waiting for payload progress is classified as timeout.
        /// </summary>
        [Fact]
        public async Task DownloadArticleAsync_WhenReceiveTimeout_ReturnsTimeout()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <timeout@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <timeout@test> article follows");
                await Task.Delay(600).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpArticleAcquisitionOptions options = NntpArticleAcquisitionOptions.Default with { ReceiveTimeout = TimeSpan.FromMilliseconds(100) };
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                options,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(session);
            await using (session.ConfigureAwait(false))
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<timeout@test>", CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(NntpArticleAcquisitionFailureCode.Timeout, result.FailureCode);
            }
        }

        /// <summary>
        /// Confirms parser bridge only accepts successful acquisition results.
        /// </summary>
        [Fact]
        public async Task ParserBridge_WhenAcquisitionFails_ThrowsInsteadOfFabricatingParserFailure()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <missing@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "430 no such article");
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<missing@test>", CancellationToken.None);
                NntpArticleParser parser = new("bf01.usenet.ninja");
                InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => NntpArticleAcquisitionParserBridge.ParseSuccessfulArticle(parser, result));
                Assert.Contains("unsuccessful acquisition", ex.Message, StringComparison.OrdinalIgnoreCase);
            }
        }

        /// <summary>
        /// Confirms protocol command/response logging redacts credentials and correlates Message-ID only for ARTICLE workflow operations.
        /// </summary>
        [Fact]
        public async Task Logging_WhenAuthAndArticleFlow_RedactsCredentialsAndIncludesMessageIdOnlyForArticleOperations()
        {
            byte[] article = BuildArticleBytes("<log@test>", "body\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "381 pass required");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS secret");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "281 auth accepted");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <log@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <log@test> article follows");
                await FakeArticleServer.WriteBytesAsync(stream, article);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray());
            });

            CapturingLoggerProvider loggerProvider = new();
            ILogger<NntpArticleAcquisitionSession> logger = loggerProvider.CreateLogger<NntpArticleAcquisitionSession>();
            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "secret");
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                logger,
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<log@test>", CancellationToken.None);
                Assert.True(result.IsSuccess);
            }

            string logs = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.Contains("RX: 200 ready", logs, StringComparison.Ordinal);
            Assert.Contains("TX: AUTHINFO USER ***", logs, StringComparison.Ordinal);
            Assert.Contains("RX: 381 pass required", logs, StringComparison.Ordinal);
            Assert.Contains("TX: AUTHINFO PASS ***", logs, StringComparison.Ordinal);
            Assert.Contains("RX: 281 auth accepted", logs, StringComparison.Ordinal);

            Assert.Contains("TX: ARTICLE <log@test>", logs, StringComparison.Ordinal);
            Assert.Contains("RX: 220 0 <log@test> article follows", logs, StringComparison.Ordinal);
            Assert.Contains("MessageId=<log@test>", logs, StringComparison.Ordinal);

            Assert.DoesNotContain("AUTHINFO USER *** MessageId=", logs, StringComparison.Ordinal);
            Assert.DoesNotContain("AUTHINFO PASS *** MessageId=", logs, StringComparison.Ordinal);
            Assert.DoesNotContain("RX: 200 ready MessageId=", logs, StringComparison.Ordinal);
            Assert.DoesNotContain("RX: 381 pass required MessageId=", logs, StringComparison.Ordinal);
            Assert.DoesNotContain("RX: 281 auth accepted MessageId=", logs, StringComparison.Ordinal);

            Assert.DoesNotContain("secret", logs, StringComparison.Ordinal);
            Assert.DoesNotContain("body", logs, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms DATE keepalive protocol command/response logging omits MessageId correlation because no article scope exists.
        /// </summary>
        [Fact]
        public async Task Logging_WhenDateKeepAliveFlow_OmitsMessageIdCorrelation()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "381 pass required");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS pass");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "281 auth accepted");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "DATE");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "111 20260826010101");
            });

            CapturingLoggerProvider loggerProvider = new();
            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "pass");
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                loggerProvider.CreateLogger<NntpArticleAcquisitionSession>(),
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.KeepAliveWithDateAsync(CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.None, result.FailureCode);
                Assert.Equal(111, result.ResponseCode);
                Assert.Equal("20260826010101", result.ResponseText);
            }

            string logs = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.Contains("TX: DATE", logs, StringComparison.Ordinal);
            Assert.Contains("RX: 111 20260826010101", logs, StringComparison.Ordinal);
            Assert.DoesNotContain("TX: DATE MessageId=", logs, StringComparison.Ordinal);
            Assert.DoesNotContain("RX: 111 20260826010101 MessageId=", logs, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms graceful disposal of a connected session sends QUIT and consumes the server 205 response before transport teardown.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenSessionEstablished_SendsQuitAndReceives205()
        {
            CapturingLoggerProvider loggerProvider = new();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "QUIT").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "205 closing connection").ConfigureAwait(false);
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                loggerProvider.CreateLogger<NntpArticleAcquisitionSession>(),
                CancellationToken.None);

            Assert.NotNull(session);

            await session.DisposeAsync();

            string logs = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.Contains("TX: QUIT", logs, StringComparison.Ordinal);
            Assert.Contains("RX: 205 closing connection", logs, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms disposal skips QUIT when transport is no longer usable after connection failure.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenConnectionAlreadyFailed_DoesNotAttemptQuit()
        {
            CapturingLoggerProvider loggerProvider = new();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <connection-failure@test>").ConfigureAwait(false);
            });

            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                loggerProvider.CreateLogger<NntpArticleAcquisitionSession>(),
                CancellationToken.None);

            Assert.NotNull(session);

            using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<connection-failure@test>", CancellationToken.None);
            Assert.Equal(NntpArticleAcquisitionFailureCode.ConnectionFailure, result.FailureCode);

            await session.DisposeAsync();

            string logs = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.DoesNotContain("TX: QUIT", logs, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms startup cancellation during AUTHINFO does not emit QUIT because protocol-ready state was never reached.
        /// </summary>
        [Fact]
        public async Task ConnectAsync_WhenCancelledDuringAuthentication_DoesNotAttemptQuit()
        {
            CapturingLoggerProvider loggerProvider = new();
            TaskCompletionSource authUserObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowAuthFlow = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user").ConfigureAwait(false);
                _ = authUserObserved.TrySetResult();
                await allowAuthFlow.Task.ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "381 pass required").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS pass").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "281 auth accepted").ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpArticleAcquisitionEndpoint endpoint = new("127.0.0.1", server.Port, UseSsl: false, Username: "user", Password: "pass");
            using CancellationTokenSource connectCancellation = new();

            Task<(NntpArticleAcquisitionSession? Session, NntpArticleAcquisitionResult Result)> connectTask = NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                loggerProvider.CreateLogger<NntpArticleAcquisitionSession>(),
                connectCancellation.Token).AsTask();

            using CancellationTokenSource waitTimeout = new(TimeSpan.FromSeconds(10));
            await authUserObserved.Task.WaitAsync(waitTimeout.Token).ConfigureAwait(false);

            connectCancellation.Cancel();
            _ = allowAuthFlow.TrySetResult();

            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await connectTask.ConfigureAwait(false);
            using (connectResult)
            {
                Assert.Null(session);
                Assert.Equal(NntpArticleAcquisitionFailureCode.Cancelled, connectResult.FailureCode);
            }

            string logs = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.DoesNotContain("TX: QUIT", logs, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms lifecycle information logging includes message-id and invariant elapsed formatting.
        /// </summary>
        [Fact]
        public async Task Logging_WhenArticleMissing_ContainsLifecycleOutcomeWithInvariantElapsed()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready");
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <missing-lifecycle@test>");
                await FakeArticleServer.WriteAsciiLineAsync(stream, "430 no such article");
            });

            CapturingLoggerProvider loggerProvider = new();
            (NntpArticleAcquisitionSession? session, _) = await NntpArticleAcquisitionSession.ConnectAsync(
                server.CreateEndpoint(),
                NntpArticleAcquisitionOptions.Default,
                loggerProvider.CreateLogger<NntpArticleAcquisitionSession>(),
                CancellationToken.None);

            Assert.NotNull(session);
            await using (session)
            {
                using NntpArticleAcquisitionResult result = await session.DownloadArticleAsync("<missing-lifecycle@test>", CancellationToken.None);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleNotFound, result.FailureCode);
            }

            CapturedLogEntry lifecycle = Assert.Single(
                loggerProvider.Entries,
                static entry => entry.Message.Contains("Article <missing-lifecycle@test> not found in ", StringComparison.Ordinal));

            string suffix = lifecycle.Message.Split(" in ", StringSplitOptions.None)[1];
            string durationToken = suffix.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries)[0];
            Assert.Matches("^[0-9]+\\.[0-9]{2}s$", durationToken);
            Assert.DoesNotContain(",", durationToken, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms acquisition TLS succeeds when a per-session strict certificate callback is supplied.
        /// </summary>
        [Fact]
        public async Task ConnectAsync_WhenUseSslTrueAndCallbackSupplied_Succeeds()
        {
            using TestTlsCertificateFixture tlsFixture = new();

            await using FakeArticleServer server = await FakeArticleServer.StartWithTransportAsync(
                async stream =>
                {
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                    await FakeArticleServer.ExpectAsciiLineAsync(stream, "QUIT").ConfigureAwait(false);
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "205 closing connection").ConfigureAwait(false);
                },
                FakeArticleServer.ConnectionTransport.ImplicitTls,
                tlsFixture.ServerCertificate);

            NntpArticleAcquisitionEndpoint endpoint = server.CreateEndpoint(useSsl: true, host: "localhost");

            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None,
                tlsFixture.ServerCertificateValidationCallback);

            using (connectResult)
            {
                Assert.NotNull(session);
                Assert.Equal(NntpArticleAcquisitionFailureCode.None, connectResult.FailureCode);
            }

            await using (session)
            {
            }
        }

        /// <summary>
        /// Confirms acquisition TLS without a callback keeps platform-default certificate validation semantics.
        /// </summary>
        [Fact]
        public async Task ConnectAsync_WhenUseSslTrueWithoutCallback_UsesDefaultValidationAndFailsForSelfSignedServer()
        {
            using TestTlsCertificateFixture tlsFixture = new();

            await using FakeArticleServer server = await FakeArticleServer.StartWithTransportAsync(
                static async stream =>
                {
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                },
                FakeArticleServer.ConnectionTransport.ImplicitTls,
                tlsFixture.ServerCertificate);

            NntpArticleAcquisitionEndpoint endpoint = server.CreateEndpoint(useSsl: true, host: "localhost");

            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None);

            using (connectResult)
            {
                Assert.Null(session);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ConnectionFailure, connectResult.FailureCode);
            }
        }

        /// <summary>
        /// Confirms the strict fixture callback rejects non-matching certificates.
        /// </summary>
        [Fact]
        public async Task FixtureCallback_WhenServerCertificateDoesNotMatch_RejectsConnection()
        {
            using TestTlsCertificateFixture trustedFixture = new();
            using TestTlsCertificateFixture serverFixture = new();

            await using FakeArticleServer server = await FakeArticleServer.StartWithTransportAsync(
                static async stream =>
                {
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                },
                FakeArticleServer.ConnectionTransport.ImplicitTls,
                serverFixture.ServerCertificate);

            NntpArticleAcquisitionEndpoint endpoint = server.CreateEndpoint(useSsl: true, host: "localhost");

            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                NntpArticleAcquisitionOptions.Default,
                NullLogger<NntpArticleAcquisitionSession>.Instance,
                CancellationToken.None,
                trustedFixture.ServerCertificateValidationCallback);

            using (connectResult)
            {
                Assert.Null(session);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ConnectionFailure, connectResult.FailureCode);
            }
        }

        /// <summary>
        /// Encodes decoded payload bytes into a single yEnc payload line for contract verification fixtures.
        /// </summary>
        /// <param name="decoded">Decoded payload bytes to encode using yEnc escaping rules.</param>
        /// <returns>ASCII yEnc payload text for one logical line without trailing CRLF.</returns>
        private static string EncodeYEncLine(ReadOnlySpan<byte> decoded)
        {
            byte[] encoded = EncodeYEncPayload(decoded);
            return Encoding.ASCII.GetString(encoded).TrimEnd('\r', '\n');
        }

        private static byte[] EncodeYEncPayload(ReadOnlySpan<byte> decoded)
        {
            List<byte> output = new(decoded.Length + (decoded.Length / 32));
            int lineCount = 0;

            for (int i = 0; i < decoded.Length; i++)
            {
                byte encoded = unchecked((byte)(decoded[i] + 42));
                bool mustEscape = encoded is 0 or 9 or 10 or 13 or 32 or 46 or 61;

                if (mustEscape)
                {
                    output.Add((byte)'=');
                    output.Add(unchecked((byte)(encoded + 64)));
                    lineCount += 2;
                }
                else
                {
                    output.Add(encoded);
                    lineCount++;
                }

                if (lineCount >= 128)
                {
                    output.Add((byte)'\r');
                    output.Add((byte)'\n');
                    lineCount = 0;
                }
            }

            if (output.Count == 0 || output[^1] != (byte)'\n')
            {
                output.Add((byte)'\r');
                output.Add((byte)'\n');
            }

            return [.. output];
        }

        private static byte[] DotStuffLineStarts(ReadOnlySpan<byte> payload)
        {
            List<byte> output = new(payload.Length + 32);
            bool atLineStart = true;

            for (int i = 0; i < payload.Length; i++)
            {
                byte current = payload[i];
                if (atLineStart && current == (byte)'.')
                {
                    output.Add((byte)'.');
                }

                output.Add(current);
                atLineStart = current == (byte)'\n';
            }

            return [.. output];
        }

        private static byte[] DecodeSinglePartYEncBody(ReadOnlySpan<byte> body)
        {
            int beginLineStart = body.IndexOf("=ybegin "u8);
            Assert.True(beginLineStart >= 0, "Expected =ybegin marker.");

            int beginLineEnd = body[beginLineStart..].IndexOf("\r\n"u8);
            Assert.True(beginLineEnd >= 0, "Expected CRLF-terminated =ybegin line.");
            int payloadStart = beginLineStart + beginLineEnd + 2;

            int endLineStart = body[payloadStart..].IndexOf("\r\n=yend "u8);
            Assert.True(endLineStart >= 0, "Expected =yend marker after payload.");
            int payloadEndExclusive = payloadStart + endLineStart;

            ReadOnlySpan<byte> encodedPayload = body[payloadStart..payloadEndExclusive];
            byte[] decodedBuffer = new byte[encodedPayload.Length];
            int decodedCount = 0;
            int lineStart = 0;

            while (lineStart < encodedPayload.Length)
            {
                int lineEnd = encodedPayload[lineStart..].IndexOf("\r\n"u8);
                bool finalLine = lineEnd < 0;
                int lineLength = finalLine ? encodedPayload.Length - lineStart : lineEnd;
                ReadOnlySpan<byte> line = encodedPayload.Slice(lineStart, lineLength);

                for (int i = 0; i < line.Length; i++)
                {
                    byte current = line[i];
                    if (current == (byte)'=')
                    {
                        Assert.True(i + 1 < line.Length, "Expected escaped payload byte after '='.");
                        decodedBuffer[decodedCount++] = unchecked((byte)(line[i + 1] - 42 - 64));
                        i++;
                    }
                    else
                    {
                        decodedBuffer[decodedCount++] = unchecked((byte)(current - 42));
                    }
                }

                lineStart = finalLine ? encodedPayload.Length : lineStart + lineEnd + 2;
            }

            byte[] decoded = new byte[decodedCount];
            Buffer.BlockCopy(decodedBuffer, 0, decoded, 0, decodedCount);
            return decoded;
        }

        /// <summary>
        /// Computes CRC-32 for deterministic decoded-byte assertions.
        /// </summary>
        /// <param name="payload">Decoded payload bytes.</param>
        /// <returns>CRC-32 value.</returns>
        private static uint Crc32(ReadOnlySpan<byte> payload)
        {
            uint crc = 0xFFFFFFFFu;
            for (int i = 0; i < payload.Length; i++)
            {
                crc = (crc >> 8) ^ CrcTable[(int)((crc ^ payload[i]) & 0xFF)];
            }

            return crc ^ 0xFFFFFFFFu;
        }

        /// <summary>
        /// Gets the CRLF-separated body bytes from a complete article payload.
        /// </summary>
        /// <param name="article">Complete article bytes.</param>
        /// <returns>Body bytes following the first CRLFCRLF separator.</returns>
        private static byte[] GetBodyBytes(ReadOnlySpan<byte> article)
        {
            int separator = article.IndexOf("\r\n\r\n"u8);
            Assert.True(separator >= 0, "Expected CRLFCRLF header/body separator.");
            return article[(separator + 4)..].ToArray();
        }

        /// <summary>
        /// CRC-32 lookup table for deterministic decoded-byte assertions.
        /// </summary>
        private static readonly uint[] CrcTable = CreateCrcTable();

        /// <summary>
        /// Creates the CRC-32 lookup table used by <see cref="Crc32(ReadOnlySpan{byte})"/>.
        /// </summary>
        /// <returns>Initialized 256-entry lookup table.</returns>
        private static uint[] CreateCrcTable()
        {
            uint[] table = new uint[256];
            for (uint i = 0; i < table.Length; i++)
            {
                uint value = i;
                for (int bit = 0; bit < 8; bit++)
                {
                    value = (value & 1) == 0 ? value >> 1 : (value >> 1) ^ 0xEDB88320u;
                }

                table[i] = value;
            }

            return table;
        }

        /// <summary>
        /// Builds parser-compatible article bytes for test cases.
        /// </summary>
        /// <param name="messageId">Message-ID value.</param>
        /// <param name="body">Body text.</param>
        /// <returns>Article bytes.</returns>
        /// <summary>
        /// Confirms the build article bytes behavior.
        /// </summary>
        /// <returns>The value returned by the build article bytes helper.</returns>
        private static byte[] BuildArticleBytes(string messageId, string body)
        {
            return BuildArticleBytes(messageId, Encoding.ASCII.GetBytes(body));
        }

        private static DownloadedArticleBuffer CreateDownloadedArticleBuffer(string payload)
        {
            byte[] payloadBytes = Encoding.ASCII.GetBytes(payload);
            byte[] rented = ArrayPool<byte>.Shared.Rent(payloadBytes.Length);
            Buffer.BlockCopy(payloadBytes, 0, rented, 0, payloadBytes.Length);
            return new DownloadedArticleBuffer(rented, payloadBytes.Length);
        }

        /// <summary>
        /// Builds parser-compatible article bytes for test cases.
        /// </summary>
        /// <param name="messageId">Message-ID value.</param>
        /// <param name="body">Body bytes.</param>
        /// <returns>Article bytes.</returns>
        /// <summary>
        /// Confirms the build article bytes behavior.
        /// </summary>
        /// <returns>The value returned by the build article bytes helper.</returns>
        private static byte[] BuildArticleBytes(string messageId, byte[] body)
        {
            byte[] headers = Encoding.ASCII.GetBytes(
                "Date: Fri, 23 Aug 2024 07:30:10 +0000\r\n" +
                $"Message-ID: {messageId}\r\n" +
                "Newsgroups: alt.test\r\n" +
                "From: user@example.test\r\n" +
                "\r\n");

            byte[] article = new byte[headers.Length + body.Length];
            Buffer.BlockCopy(headers, 0, article, 0, headers.Length);
            Buffer.BlockCopy(body, 0, article, headers.Length, body.Length);
            return article;
        }

        /// <summary>
        /// Minimal in-process fake NNTP server for acquisition contract tests.
        /// </summary>
        private sealed class FakeArticleServer : IAsyncDisposable
        {
            /// <summary>
            /// Server transport mode.
            /// </summary>
            internal enum ConnectionTransport
            {
                /// <summary>
                /// Plain TCP transport.
                /// </summary>
                Plaintext,

                /// <summary>
                /// Implicit TLS transport.
                /// </summary>
                ImplicitTls,
            }

            /// <summary>
            /// Listener.
            /// </summary>
            private readonly TcpListener _listener;

            /// <summary>
            /// Session callback.
            /// </summary>
            private readonly Func<Stream, Task> _session;

            /// <summary>
            /// Transport mode.
            /// </summary>
            private readonly ConnectionTransport _transport;

            /// <summary>
            /// TLS server certificate for implicit TLS transport.
            /// </summary>
            private readonly X509Certificate2? _serverCertificate;

            /// <summary>
            /// Cancellation source.
            /// </summary>
            private readonly CancellationTokenSource _shutdown = new();

            /// <summary>
            /// Accept loop task.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
            /// Initializes fake server.
            /// </summary>
            /// <param name="listener">Listener.</param>
            /// <param name="session">Session callback.</param>
            /// <param name="transport">Transport mode.</param>
            /// <param name="serverCertificate">TLS server certificate for implicit TLS mode.</param>
            /// <summary>
            /// Confirms the r behavior.
            /// </summary>
            /// <returns>The value returned by the r helper.</returns>
            private FakeArticleServer(TcpListener listener, Func<Stream, Task> session, ConnectionTransport transport, X509Certificate2? serverCertificate)
            {
                _listener = listener;
                _session = session;
                _transport = transport;
                _serverCertificate = serverCertificate;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Gets bound port.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Starts fake server.
            /// </summary>
            /// <param name="session">Session callback.</param>
            /// <returns>Started server.</returns>
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <returns>The value returned by the start async helper.</returns>
            internal static async Task<FakeArticleServer> StartAsync(Func<NetworkStream, Task> session)
            {
                ArgumentNullException.ThrowIfNull(session);

                return await StartWithTransportAsync(
                    stream => session((NetworkStream)stream),
                    ConnectionTransport.Plaintext,
                    serverCertificate: null).ConfigureAwait(false);
            }

            /// <summary>
            /// Starts fake server with explicit transport mode.
            /// </summary>
            /// <param name="session">Session callback.</param>
            /// <param name="transport">Transport mode.</param>
            /// <param name="serverCertificate">TLS server certificate required for implicit TLS mode.</param>
            /// <returns>Started server.</returns>
            /// <summary>
            /// Confirms the start with transport async behavior.
            /// </summary>
            /// <returns>The value returned by the start with transport async helper.</returns>
            internal static async Task<FakeArticleServer> StartWithTransportAsync(Func<Stream, Task> session, ConnectionTransport transport, X509Certificate2? serverCertificate)
            {
                ArgumentNullException.ThrowIfNull(session);

                if (transport == ConnectionTransport.ImplicitTls && serverCertificate is null)
                {
                    throw new ArgumentNullException(nameof(serverCertificate), "TLS transport requires a server certificate.");
                }

                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                FakeArticleServer server = new(listener, session, transport, serverCertificate);
                await Task.Delay(20).ConfigureAwait(false);
                return server;
            }

            /// <summary>
            /// Creates acquisition endpoint for this server.
            /// </summary>
            /// <returns>Endpoint descriptor.</returns>
            /// <summary>
            /// Confirms the create endpoint behavior.
            /// </summary>
            /// <param name="useSsl">The use ssl used by this test scenario.</param>
            /// <param name="host">The host used by this test scenario.</param>
            /// <returns>The value returned by the create endpoint helper.</returns>
            internal NntpArticleAcquisitionEndpoint CreateEndpoint(bool useSsl = false, string host = "127.0.0.1")
            {
                return new NntpArticleAcquisitionEndpoint(host, Port, UseSsl: useSsl, Username: null, Password: null);
            }

            /// <summary>
            /// Reads one ASCII line and validates expected text.
            /// </summary>
            /// <param name="stream">Stream.</param>
            /// <param name="expected">Expected line.</param>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the expect ascii line async behavior.
            /// </summary>
            /// <returns>The value returned by the expect ascii line async helper.</returns>
            internal static async Task ExpectAsciiLineAsync(Stream stream, string expected)
            {
                string line = await ReadAsciiLineAsync(stream, CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(expected, line);
            }

            /// <summary>
            /// Reads one ASCII line without CRLF terminator.
            /// </summary>
            /// <param name="stream">Stream.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>Line text.</returns>
            /// <summary>
            /// Confirms the read ascii line async behavior.
            /// </summary>
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
            /// Writes ASCII line with CRLF.
            /// </summary>
            /// <param name="stream">Stream.</param>
            /// <param name="line">Line text.</param>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the write ascii line async behavior.
            /// </summary>
            /// <returns>The value returned by the write ascii line async helper.</returns>
            internal static async Task WriteAsciiLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }

            /// <summary>
            /// Writes raw bytes and flushes.
            /// </summary>
            /// <param name="stream">Stream.</param>
            /// <param name="bytes">Bytes.</param>
            /// <returns>Completion task.</returns>
            /// <summary>
            /// Confirms the write bytes async behavior.
            /// </summary>
            /// <returns>The value returned by the write bytes async helper.</returns>
            internal static async Task WriteBytesAsync(Stream stream, byte[] bytes)
            {
                await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }

            /// <summary>
            /// Disposes fake server.
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
            /// Accept loop body.
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
                    using NetworkStream networkStream = client.GetStream();
                    Stream protocolStream = networkStream;

                    if (_transport == ConnectionTransport.ImplicitTls)
                    {
                        X509Certificate2 serverCertificate = _serverCertificate ?? throw new InvalidOperationException("TLS transport requires a server certificate.");
                        SslStream sslStream = new(networkStream, leaveInnerStreamOpen: false);
                        await sslStream.AuthenticateAsServerAsync(
                            new SslServerAuthenticationOptions
                            {
                                ServerCertificate = serverCertificate,
                                ClientCertificateRequired = false,
                                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                            },
                            _shutdown.Token).ConfigureAwait(false);

                        protocolStream = sslStream;
                    }

                    await _session(protocolStream).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
                catch (IOException)
                {
                }
                catch (AuthenticationException)
                {
                }
            }
        }

        /// <summary>
        /// Captured log entry snapshot.
        /// </summary>
        /// <param name="Level">Log level.</param>
        /// <param name="Message">Rendered message text.</param>
        /// <summary>
        /// Confirms the captured log entry behavior.
        /// </summary>
        /// <returns>The value returned by the captured log entry helper.</returns>
        private sealed record CapturedLogEntry(LogLevel Level, string Message);

        /// <summary>
        /// In-memory logger provider for protocol/lifecycle logging assertions.
        /// </summary>
        private sealed class CapturingLoggerProvider
        {
            /// <summary>
            /// Shared lock.
            /// </summary>
            private readonly object _gate = new();

            /// <summary>
            /// Captured entries.
            /// </summary>
            internal List<CapturedLogEntry> Entries { get; } = [];

            /// <summary>
            /// Creates logger instance.
            /// </summary>
            /// <typeparam name="T">Category type.</typeparam>
            /// <returns>Logger.</returns>
            internal ILogger<T> CreateLogger<T>()
            {
                return new CapturingLogger<T>(Entries, _gate);
            }

            /// <summary>
            /// Capturing logger implementation.
            /// </summary>
            /// <typeparam name="T">Category type.</typeparam>
            private sealed class CapturingLogger<T> : ILogger<T>
            {
                /// <summary>
                /// Backing entry list.
                /// </summary>
                private readonly List<CapturedLogEntry> _entries;

                /// <summary>
                /// Synchronization gate.
                /// </summary>
                private readonly object _gate;

                /// <summary>
                /// Initializes logger.
                /// </summary>
                /// <param name="entries">Entry list.</param>
                /// <param name="gate">Lock gate.</param>
                /// <summary>
                /// Confirms the r behavior.
                /// </summary>
                /// <returns>The value returned by the r helper.</returns>
                internal CapturingLogger(List<CapturedLogEntry> entries, object gate)
                {
                    _entries = entries;
                    _gate = gate;
                }

                /// <summary>
                /// Begins scope.
                /// </summary>
                /// <typeparam name="TState">Scope state type.</typeparam>
                /// <param name="state">State.</param>
                /// <returns>Scope disposable.</returns>
                public IDisposable BeginScope<TState>(TState state)
                    where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
                /// Gets value indicating whether level is enabled.
                /// </summary>
                /// <param name="logLevel">Log level.</param>
                /// <returns>Always true for tests.</returns>
                /// <summary>
                /// Confirms the is enabled behavior.
                /// </summary>
                /// <returns>The value returned by the is enabled helper.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return true;
                }

                /// <summary>
                /// Records log entry.
                /// </summary>
                /// <typeparam name="TState">State type.</typeparam>
                /// <param name="logLevel">Level.</param>
                /// <param name="eventId">Event id.</param>
                /// <param name="state">State.</param>
                /// <param name="exception">Exception.</param>
                /// <param name="formatter">Formatter.</param>
                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    lock (_gate)
                    {
                        _entries.Add(new CapturedLogEntry(logLevel, message));
                    }
                }

                /// <summary>
                /// Null scope singleton.
                /// </summary>
                private sealed class NullScope : IDisposable
                {
                    /// <summary>
                    /// Singleton instance.
                    /// </summary>
                    internal static readonly NullScope Instance = new();

                    /// <summary>
                    /// Disposes scope.
                    /// </summary>
                    public void Dispose()
                    {
                    }
                }
            }
        }
    }
}
