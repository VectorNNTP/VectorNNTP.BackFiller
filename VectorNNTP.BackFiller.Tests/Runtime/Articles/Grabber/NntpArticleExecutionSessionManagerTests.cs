// <copyright file="NntpArticleExecutionSessionManagerTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for nntp article execution session manager, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the nntp article execution session manager test suite.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Grabber
{
    /// <summary>
    /// Verifies foundational grabber session-manager ownership boundaries and lease semantics.
    /// </summary>
    public sealed class NntpArticleExecutionSessionManagerTests
    {
        /// <summary>
        /// Confirms one account with max-connections one yields one connected reusable session slot.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenSingleAccountWithOneConnection_CreatesOneReadySession()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <single@test>").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <single@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, BuildArticleBytes("<single@test>", "body\r\n")).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null);

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, manager.TotalSessionCount);

            await using NntpArticleSessionLease lease = await manager.AcquireAsync("<single@test>", CancellationToken.None).ConfigureAwait(false);
            using NntpArticleAcquisitionResult result = await lease.Session.DownloadArticleAsync("<single@test>", CancellationToken.None).ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);
            Assert.True(result.IsSuccess);
        }

        /// <summary>
        /// Confirms one configured session permits exactly one active lease and additional acquisition waits until release.
        /// </summary>
        [Fact]
        public async Task AcquireAsync_WhenSingleConnectionConfigured_SecondAcquireWaitsUntilFirstRelease()
        {
            byte[] article = BuildArticleBytes("<serialized@test>", "serialized\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <serialized@test>").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <serialized@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <serialized@test>").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <serialized@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null);

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            await using NntpArticleSessionLease firstLease = await manager.AcquireAsync("<serialized@test>", CancellationToken.None).ConfigureAwait(false);

            Task<NntpArticleSessionLease> secondAcquireTask = manager.AcquireAsync("<serialized@test>", CancellationToken.None).AsTask();
            await Task.Delay(100).ConfigureAwait(false);
            Assert.False(secondAcquireTask.IsCompleted);

            using NntpArticleAcquisitionResult firstResult = await firstLease.Session.DownloadArticleAsync("<serialized@test>", CancellationToken.None).ConfigureAwait(false);
            firstLease.ReportAcquisitionOutcome(firstResult.FailureCode);

            await firstLease.DisposeAsync().ConfigureAwait(false);

            await using NntpArticleSessionLease secondLease = await secondAcquireTask.ConfigureAwait(false);
            using NntpArticleAcquisitionResult secondResult = await secondLease.Session.DownloadArticleAsync("<serialized@test>", CancellationToken.None).ConfigureAwait(false);
            secondLease.ReportAcquisitionOutcome(secondResult.FailureCode);

            Assert.True(firstResult.IsSuccess);
            Assert.True(secondResult.IsSuccess);
        }

        /// <summary>
        /// Confirms a 430 article-not-found outcome does not destroy the reusable authenticated session.
        /// </summary>
        [Fact]
        public async Task SessionHealth_WhenArticleNotFound_LeaseReuseSucceedsWithoutReconnect()
        {
            byte[] existingArticle = BuildArticleBytes("<exists@test>", "body\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <missing@test>").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "430 no such article").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <exists@test>").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <exists@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, existingArticle).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null);

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            await using (NntpArticleSessionLease missingLease = await manager.AcquireAsync("<missing@test>", CancellationToken.None).ConfigureAwait(false))
            {
                using NntpArticleAcquisitionResult missing = await missingLease.Session.DownloadArticleAsync("<missing@test>", CancellationToken.None).ConfigureAwait(false);
                missingLease.ReportAcquisitionOutcome(missing.FailureCode);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleNotFound, missing.FailureCode);
            }

            await using NntpArticleSessionLease successLease = await manager.AcquireAsync("<exists@test>", CancellationToken.None).ConfigureAwait(false);
            using NntpArticleAcquisitionResult success = await successLease.Session.DownloadArticleAsync("<exists@test>", CancellationToken.None).ConfigureAwait(false);
            successLease.ReportAcquisitionOutcome(success.FailureCode);
            Assert.True(success.IsSuccess);
            Assert.Equal(existingArticle.Length, success.ArticleLength);
        }

        /// <summary>
        /// Confirms deterministic session-health classification marks connection failures as non-reusable.
        /// </summary>
        [Fact]
        public void SessionHealthClassifier_WhenConnectionFailure_ReturnsNotReusable()
        {
            bool reusable = NntpArticleSessionHealthClassifier.IsSessionReusable(NntpArticleAcquisitionFailureCode.ConnectionFailure);

            Assert.False(reusable);
        }

        /// <summary>
        /// Confirms deterministic session-health classification keeps article-not-found outcomes reusable.
        /// </summary>
        [Fact]
        public void SessionHealthClassifier_WhenArticleNotFound_ReturnsReusable()
        {
            bool reusable = NntpArticleSessionHealthClassifier.IsSessionReusable(NntpArticleAcquisitionFailureCode.ArticleNotFound);

            Assert.True(reusable);
        }

        /// <summary>
        /// Confirms unknown acquisition failure enum values are treated conservatively as non-reusable.
        /// </summary>
        [Fact]
        public void SessionHealthClassifier_WhenFailureCodeUnknown_ReturnsNotReusable()
        {
            bool reusable = NntpArticleSessionHealthClassifier.IsSessionReusable((NntpArticleAcquisitionFailureCode)12);

            Assert.False(reusable);
        }

        /// <summary>
        /// Confirms authentication failure during initialization leaves no ready sessions and surfaces deterministic failure.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenAuthenticationRejected_ThrowsNoReadySessionFailure()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "381 pass required").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS bad").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "481 authentication rejected").ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: "user", password: "bad");

            CapturingLoggerProvider loggerProvider = new();
            await using NntpArticleExecutionSessionManager manager = new(loggerProvider.CreateLogger<NntpArticleExecutionSessionManager>());

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            Assert.Contains("No acquisition sessions", ex.Message, StringComparison.Ordinal);

            CapturedLogEntry warning = Assert.Single(
                loggerProvider.Entries,
                static entry =>
                    entry.Level == LogLevel.Warning &&
                    entry.Message.Contains("Grabber session slot initialization failed", StringComparison.Ordinal));

            Assert.Contains("Account=" + account.EntryId.ToString("D"), warning.Message, StringComparison.Ordinal);
            Assert.Contains("ConnectionIndex=0", warning.Message, StringComparison.Ordinal);
            Assert.Contains($"Endpoint=127.0.0.1:{server.Port}", warning.Message, StringComparison.Ordinal);
            Assert.Contains("FailureCode=AuthenticationFailure", warning.Message, StringComparison.Ordinal);
            Assert.Contains("ResponseCode=481", warning.Message, StringComparison.Ordinal);
            Assert.Contains("authentication rejected", warning.Message, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("AUTHINFO PASS bad", warning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("AUTHINFO USER user", warning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("PASS bad", warning.Message, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms AUTHINFO USER authentication rejection emits a warning with account/endpoint context and no credential material.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenAuthInfoUserRejected_EmitsWarningWithAuthenticationFailure()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "480 authentication required").ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: "user", password: "pass");

            CapturingLoggerProvider loggerProvider = new();
            await using NntpArticleExecutionSessionManager manager = new(loggerProvider.CreateLogger<NntpArticleExecutionSessionManager>());

            _ = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            CapturedLogEntry warning = Assert.Single(
                loggerProvider.Entries,
                static entry =>
                    entry.Level == LogLevel.Warning &&
                    entry.Message.Contains("Grabber session slot initialization failed", StringComparison.Ordinal));

            Assert.Contains("Account=" + account.EntryId.ToString("D"), warning.Message, StringComparison.Ordinal);
            Assert.Contains("ConnectionIndex=0", warning.Message, StringComparison.Ordinal);
            Assert.Contains($"Endpoint=127.0.0.1:{server.Port}", warning.Message, StringComparison.Ordinal);
            Assert.Contains("FailureCode=AuthenticationFailure", warning.Message, StringComparison.Ordinal);
            Assert.Contains("ResponseCode=480", warning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("AUTHINFO USER user", warning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("AUTHINFO PASS pass", warning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("PASS pass", warning.Message, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms reconnect authentication rejection after a connection-loss retirement emits warning diagnostics without credential leakage.
        /// </summary>
        [Fact]
        public async Task ReleaseAsync_WhenReconnectAuthRejected_EmitsWarningAndLeavesSlotUnavailable()
        {
            int connectionCounter = 0;

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                int connectionId = Interlocked.Increment(ref connectionCounter);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);

                if (connectionId == 1)
                {
                    await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user").ConfigureAwait(false);
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "381 pass required").ConfigureAwait(false);
                    await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO PASS pass").ConfigureAwait(false);
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "281 authentication accepted").ConfigureAwait(false);
                    await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <reconnect-auth@test>").ConfigureAwait(false);
                    return;
                }

                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "480 authentication required").ConfigureAwait(false);
            }, acceptConnectionCount: 2).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: "user", password: "pass");
            CapturingLoggerProvider loggerProvider = new();
            await using NntpArticleExecutionSessionManager manager = new(loggerProvider.CreateLogger<NntpArticleExecutionSessionManager>());
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            await using (NntpArticleSessionLease failedLease = await manager.AcquireAsync("<reconnect-auth@test>", CancellationToken.None).ConfigureAwait(false))
            {
                using NntpArticleAcquisitionResult failed = await failedLease.Session.DownloadArticleAsync("<reconnect-auth@test>", CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ConnectionFailure, failed.FailureCode);
                failedLease.ReportAcquisitionOutcome(failed.FailureCode);
            }

            CapturedLogEntry reconnectWarning = Assert.Single(
                loggerProvider.Entries,
                static entry =>
                    entry.Level == LogLevel.Warning &&
                    entry.Message.Contains("Grabber session reconnect failed", StringComparison.Ordinal));

            Assert.Contains("Slot=0", reconnectWarning.Message, StringComparison.Ordinal);
            Assert.Contains("Account=" + account.EntryId.ToString("D"), reconnectWarning.Message, StringComparison.Ordinal);
            Assert.Contains("FailureCode=AuthenticationFailure", reconnectWarning.Message, StringComparison.Ordinal);
            Assert.Contains("ResponseCode=480", reconnectWarning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("AUTHINFO USER user", reconnectWarning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("AUTHINFO PASS pass", reconnectWarning.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("PASS pass", reconnectWarning.Message, StringComparison.Ordinal);

            Assert.Equal(2, Volatile.Read(ref connectionCounter));
            Assert.Equal(0, manager.ActiveSessionCount);

            using CancellationTokenSource acquireTimeout = new(TimeSpan.FromMilliseconds(200));
            _ = await Assert.ThrowsAsync<OperationCanceledException>(
                async () => await manager.AcquireAsync("<reconnect-auth@test>", acquireTimeout.Token).ConfigureAwait(false)).ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies shutdown cancellation during persistent slot initialization propagates cancellation instead of warning failure logs.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenShutdownCancellationOccurs_DoesNotLogSlotInitializationFailedWarningAsync()
        {
            TaskCompletionSource<bool> authInfoUserObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<bool> allowServerCleanup = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "AUTHINFO USER user").ConfigureAwait(false);
                _ = authInfoUserObserved.TrySetResult(true);
                _ = await allowServerCleanup.Task.ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: "user", password: "pass");
            CapturingLoggerProvider loggerProvider = new();
            await using NntpArticleExecutionSessionManager manager = new(loggerProvider.CreateLogger<NntpArticleExecutionSessionManager>());

            using CancellationTokenSource shutdownCancellation = new();
            Task initializeTask = manager.InitializeAsync([account], shutdownCancellation.Token);

            _ = await authInfoUserObserved.Task.ConfigureAwait(false);
            shutdownCancellation.Cancel();

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await initializeTask.ConfigureAwait(false)).ConfigureAwait(false);
            _ = allowServerCleanup.TrySetResult(true);

            Assert.DoesNotContain(
                loggerProvider.Entries,
                static entry => entry.Level == LogLevel.Warning &&
                                entry.Message.Contains("Grabber session slot initialization failed", StringComparison.Ordinal));
        }

        /// <summary>
        /// Confirms idle sessions below the configured keepalive threshold do not send DATE.
        /// </summary>
        [Fact]
        public async Task KeepAlive_WhenIdleBelowThreshold_DoesNotSendDate()
        {
            byte[] article = BuildArticleBytes("<below-threshold@test>", "body\r\n");
            ManualTimeProvider timeProvider = new(new DateTimeOffset(2026, 8, 26, 1, 1, 0, TimeSpan.Zero));
            TaskCompletionSource<bool> articleReceived = CreateSignal();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <below-threshold@test>").ConfigureAwait(false);
                _ = articleReceived.TrySetResult(true);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <below-threshold@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null, keepAliveSeconds: 30);
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance, timeProvider: timeProvider);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            timeProvider.Advance(TimeSpan.FromSeconds(5));
            await FlushBackgroundContinuationsAsync().ConfigureAwait(false);

            await using NntpArticleSessionLease lease = await manager.AcquireAsync("<below-threshold@test>", CancellationToken.None).ConfigureAwait(false);
            using NntpArticleAcquisitionResult result = await lease.Session.DownloadArticleAsync("<below-threshold@test>", CancellationToken.None).ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);

            await articleReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.True(result.IsSuccess);
        }

        /// <summary>
        /// Confirms idle sessions crossing the keepalive threshold send DATE before the next ARTICLE operation.
        /// </summary>
        [Fact]
        public async Task KeepAlive_WhenIdleThresholdReached_SendsDate()
        {
            byte[] article = BuildArticleBytes("<threshold@test>", "body\r\n");
            ManualTimeProvider timeProvider = new(new DateTimeOffset(2026, 8, 26, 1, 1, 0, TimeSpan.Zero));
            TaskCompletionSource<bool> dateReceived = CreateSignal();
            TaskCompletionSource<bool> dateResponseSent = CreateSignal();
            TaskCompletionSource<bool> articleReceived = CreateSignal();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "DATE").ConfigureAwait(false);
                _ = dateReceived.TrySetResult(true);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "111 20260826010101").ConfigureAwait(false);
                _ = dateResponseSent.TrySetResult(true);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <threshold@test>").ConfigureAwait(false);
                _ = articleReceived.TrySetResult(true);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <threshold@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null, keepAliveSeconds: 2);
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance, timeProvider: timeProvider);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            timeProvider.Advance(TimeSpan.FromSeconds(2));
            await dateReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            await dateResponseSent.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            await using NntpArticleSessionLease lease = await manager.AcquireAsync("<threshold@test>", CancellationToken.None).ConfigureAwait(false);
            using NntpArticleAcquisitionResult result = await lease.Session.DownloadArticleAsync("<threshold@test>", CancellationToken.None).ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);

            await articleReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.True(result.IsSuccess);
        }

        /// <summary>
        /// Confirms keepalive DATE is not issued while an ARTICLE command/response is in progress on a leased slot.
        /// </summary>
        [Fact]
        public async Task KeepAlive_WhenArticleActive_DoesNotSendDate()
        {
            byte[] article = BuildArticleBytes("<active@test>", "body\r\n");
            ManualTimeProvider timeProvider = new(new DateTimeOffset(2026, 8, 26, 1, 1, 0, TimeSpan.Zero));
            TaskCompletionSource<bool> articleReceived = CreateSignal();
            TaskCompletionSource<bool> releaseArticleResponse = CreateSignal();
            TaskCompletionSource<bool> unexpectedDateObserved = CreateSignal();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <active@test>").ConfigureAwait(false);
                _ = articleReceived.TrySetResult(true);

                using CancellationTokenSource unexpectedDateCancellation = new();
                Task unexpectedDateMonitor = MonitorUnexpectedDateWhileArticleActiveAsync(stream, unexpectedDateObserved, unexpectedDateCancellation.Token);

                await releaseArticleResponse.Task.ConfigureAwait(false);
                Assert.False(unexpectedDateObserved.Task.IsCompleted);
                unexpectedDateCancellation.Cancel();
                await unexpectedDateMonitor.ConfigureAwait(false);

                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <active@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null, keepAliveSeconds: 2);
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance, timeProvider: timeProvider);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            await using NntpArticleSessionLease lease = await manager.AcquireAsync("<active@test>", CancellationToken.None).ConfigureAwait(false);
            Task<NntpArticleAcquisitionResult> downloadTask = lease.Session.DownloadArticleAsync("<active@test>", CancellationToken.None).AsTask();
            await articleReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            timeProvider.Advance(TimeSpan.FromSeconds(3));
            await FlushBackgroundContinuationsAsync().ConfigureAwait(false);
            Assert.False(unexpectedDateObserved.Task.IsCompleted);
            _ = releaseArticleResponse.TrySetResult(true);

            using NntpArticleAcquisitionResult result = await downloadTask.ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);

            Assert.True(result.IsSuccess);
        }

        /// <summary>
        /// Confirms DATE keepalive failure retires the affected session and reconnects before subsequent ARTICLE work.
        /// </summary>
        [Fact]
        public async Task KeepAlive_WhenDateFails_ReconnectsSession()
        {
            byte[] article = BuildArticleBytes("<reconnect@test>", "body\r\n");
            int connectionCounter = 0;
            ManualTimeProvider timeProvider = new(new DateTimeOffset(2026, 8, 26, 1, 1, 0, TimeSpan.Zero));
            TaskCompletionSource<bool> dateReceived = CreateSignal();
            TaskCompletionSource<bool> dateFailureResponseSent = CreateSignal();
            TaskCompletionSource<bool> replacementConnectionReady = CreateSignal();
            TaskCompletionSource<bool> replacementArticleReceived = CreateSignal();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                int connectionId = Interlocked.Increment(ref connectionCounter);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);

                if (connectionId == 1)
                {
                    await FakeArticleServer.ExpectAsciiLineAsync(stream, "DATE").ConfigureAwait(false);
                    _ = dateReceived.TrySetResult(true);
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "malformed-status-line").ConfigureAwait(false);
                    _ = dateFailureResponseSent.TrySetResult(true);
                    return;
                }

                _ = replacementConnectionReady.TrySetResult(true);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <reconnect@test>").ConfigureAwait(false);
                _ = replacementArticleReceived.TrySetResult(true);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <reconnect@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }, acceptConnectionCount: 2).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null, keepAliveSeconds: 2);
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance, timeProvider: timeProvider);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            timeProvider.Advance(TimeSpan.FromSeconds(2));
            await dateReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            await dateFailureResponseSent.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            await replacementConnectionReady.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            await using NntpArticleSessionLease lease = await manager.AcquireAsync("<reconnect@test>", CancellationToken.None).ConfigureAwait(false);
            using NntpArticleAcquisitionResult result = await lease.Session.DownloadArticleAsync("<reconnect@test>", CancellationToken.None).ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);

            await replacementArticleReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.True(result.IsSuccess);
            Assert.Equal(2, Volatile.Read(ref connectionCounter));
        }

        /// <summary>
        /// Confirms a connection failure reported from one lease retires and reconnects that slot while preserving configured capacity without creating duplicate active sessions.
        /// </summary>
        [Fact]
        public async Task ReleaseAsync_WhenConnectionFailureReported_RetiresAndReconnectsMaintainingCapacity()
        {
            byte[] article = BuildArticleBytes("<recover@test>", "body\r\n");
            int connectionCounter = 0;

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                int connectionId = Interlocked.Increment(ref connectionCounter);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);

                if (connectionId == 1)
                {
                    await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <recover@test>").ConfigureAwait(false);
                    return;
                }

                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <recover@test>").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <recover@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }, acceptConnectionCount: 2).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null);
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, manager.ActiveSessionCount);

            await using (NntpArticleSessionLease failedLease = await manager.AcquireAsync("<recover@test>", CancellationToken.None).ConfigureAwait(false))
            {
                using NntpArticleAcquisitionResult failed = await failedLease.Session.DownloadArticleAsync("<recover@test>", CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(NntpArticleAcquisitionFailureCode.ConnectionFailure, failed.FailureCode);
                failedLease.ReportAcquisitionOutcome(failed.FailureCode);
            }

            Assert.Equal(1, manager.ActiveSessionCount);

            await using NntpArticleSessionLease recoveredLease = await manager.AcquireAsync("<recover@test>", CancellationToken.None).ConfigureAwait(false);
            using NntpArticleAcquisitionResult recovered = await recoveredLease.Session.DownloadArticleAsync("<recover@test>", CancellationToken.None).ConfigureAwait(false);
            recoveredLease.ReportAcquisitionOutcome(recovered.FailureCode);

            Assert.True(recovered.IsSuccess);
            Assert.Equal(2, Volatile.Read(ref connectionCounter));
            Assert.Equal(1, manager.ActiveSessionCount);
        }

        /// <summary>
        /// Confirms a zero keepalive timeout disables DATE probing and leaves ARTICLE traffic unchanged.
        /// </summary>
        [Fact]
        public async Task KeepAlive_WhenConfiguredZero_DoesNotSendDate()
        {
            byte[] article = BuildArticleBytes("<disabled@test>", "body\r\n");
            ManualTimeProvider timeProvider = new(new DateTimeOffset(2026, 8, 26, 1, 1, 0, TimeSpan.Zero));
            TaskCompletionSource<bool> articleReceived = CreateSignal();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <disabled@test>").ConfigureAwait(false);
                _ = articleReceived.TrySetResult(true);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <disabled@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null, keepAliveSeconds: 0);
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance, timeProvider: timeProvider);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            timeProvider.Advance(TimeSpan.FromSeconds(10));
            await FlushBackgroundContinuationsAsync().ConfigureAwait(false);

            await using NntpArticleSessionLease lease = await manager.AcquireAsync("<disabled@test>", CancellationToken.None).ConfigureAwait(false);
            using NntpArticleAcquisitionResult result = await lease.Session.DownloadArticleAsync("<disabled@test>", CancellationToken.None).ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);

            await articleReceived.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.True(result.IsSuccess);
        }

        /// <summary>
        /// Confirms manager disposal waits for active lease completion and then prevents new acquisitions.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenLeaseActive_WaitsThenRejectsFurtherAcquire()
        {
            byte[] article = BuildArticleBytes("<dispose@test>", "body\r\n");

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <dispose@test>").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <dispose@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null);
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            NntpArticleSessionLease lease = await manager.AcquireAsync("<dispose@test>", CancellationToken.None).ConfigureAwait(false);
            ValueTask disposeTask = manager.DisposeAsync();

            using NntpArticleAcquisitionResult result = await lease.Session.DownloadArticleAsync("<dispose@test>", CancellationToken.None).ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);
            await lease.DisposeAsync().ConfigureAwait(false);

            await disposeTask.ConfigureAwait(false);

            _ = await Assert.ThrowsAsync<ObjectDisposedException>(
                async () => await manager.AcquireAsync("<after-dispose@test>", CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);
        }

        /// <summary>
        /// Confirms disposal of an established reusable acquisition session emits QUIT and consumes the 205 closing response.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenEstablishedSessionOwned_SendsQuitAndReceives205()
        {
            CapturingLoggerProvider loggerProvider = new();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "QUIT").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "205 closing connection").ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null);
            using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            {
                _ = builder.AddProvider(loggerProvider);
                _ = builder.SetMinimumLevel(LogLevel.Debug);
            });

            await using NntpArticleExecutionSessionManager manager = new(
                loggerProvider.CreateLogger<NntpArticleExecutionSessionManager>(),
                options: null,
                timeProvider: null,
                loggerFactory: loggerFactory,
                serverCertificateValidationCallback: null);

            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);

            string logs = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.Contains("TX: QUIT", logs, StringComparison.Ordinal);
            Assert.Contains("RX: 205 closing connection", logs, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms manager disposal while a lease is active does not attempt concurrent QUIT while ARTICLE is executing.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenLeaseActive_DoesNotSendQuitConcurrentlyWithArticle()
        {
            byte[] article = BuildArticleBytes("<dispose-active@test>", "body\r\n");
            TaskCompletionSource articleCommandObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource allowArticleResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);

            CapturingLoggerProvider loggerProvider = new();

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "ARTICLE <dispose-active@test>").ConfigureAwait(false);
                _ = articleCommandObserved.TrySetResult();
                await allowArticleResponse.Task.ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "220 0 <dispose-active@test> article follows").ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, article).ConfigureAwait(false);
                await FakeArticleServer.WriteBytesAsync(stream, ".\r\n"u8.ToArray()).ConfigureAwait(false);
                await FakeArticleServer.ExpectAsciiLineAsync(stream, "QUIT").ConfigureAwait(false);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "205 closing connection").ConfigureAwait(false);
            }).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 1, username: null, password: null);
            using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            {
                _ = builder.AddProvider(loggerProvider);
                _ = builder.SetMinimumLevel(LogLevel.Debug);
            });

            await using NntpArticleExecutionSessionManager manager = new(
                loggerProvider.CreateLogger<NntpArticleExecutionSessionManager>(),
                options: null,
                timeProvider: null,
                loggerFactory: loggerFactory,
                serverCertificateValidationCallback: null);

            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            NntpArticleSessionLease lease = await manager.AcquireAsync("<dispose-active@test>", CancellationToken.None).ConfigureAwait(false);
            Task<NntpArticleAcquisitionResult> downloadTask = lease.Session.DownloadArticleAsync("<dispose-active@test>", CancellationToken.None).AsTask();
            await articleCommandObserved.Task.ConfigureAwait(false);

            ValueTask disposeTask = manager.DisposeAsync();

            string logsBeforeArticleCompletion = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.DoesNotContain("TX: QUIT", logsBeforeArticleCompletion, StringComparison.Ordinal);

            _ = allowArticleResponse.TrySetResult();
            using NntpArticleAcquisitionResult result = await downloadTask.ConfigureAwait(false);
            lease.ReportAcquisitionOutcome(result.FailureCode);
            await lease.DisposeAsync().ConfigureAwait(false);

            await disposeTask.ConfigureAwait(false);

            string logsAfterDispose = string.Join("\n", loggerProvider.Entries.Select(static entry => entry.Message));
            Assert.Contains("TX: ARTICLE <dispose-active@test> MessageId=<dispose-active@test>", logsAfterDispose, StringComparison.Ordinal);
            Assert.Contains("TX: QUIT", logsAfterDispose, StringComparison.Ordinal);
            Assert.Contains("RX: 205 closing connection", logsAfterDispose, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms keepalive-only reconciliation updates account runtime settings without reconnect churn.
        /// </summary>
        [Fact]
        public async Task ReconcileAccountAsync_WhenKeepAliveChanges_UpdatesInPlaceWithoutReconnect()
        {
            int connectionCounter = 0;
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                _ = Interlocked.Increment(ref connectionCounter);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
            }).ConfigureAwait(false);

            Guid accountId = Guid.NewGuid();
            NntpAccountSnapshot initialAccount = CreateAccount(server.Port, maxConnections: 1, username: null, password: null, entryId: accountId, keepAliveSeconds: 240);
            NntpAccountSnapshot desiredAccount = initialAccount with { KeepAliveSeconds = 180 };

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([initialAccount], CancellationToken.None).ConfigureAwait(false);

            NntpAccountSessionReconcileResult result = await manager.ReconcileAccountAsync(desiredAccount, CancellationToken.None).ConfigureAwait(false);

            Assert.True(result.KeepAliveUpdated);
            Assert.False(result.ConnectionSettingsReplaced);
            Assert.Equal(1, result.ActiveSessionCountAfter);
            Assert.Equal(1, Volatile.Read(ref connectionCounter));
        }

        /// <summary>
        /// Confirms connection-property reconciliation recreates the affected session using updated endpoint settings.
        /// </summary>
        [Fact]
        public async Task ReconcileAccountAsync_WhenPortChanges_RecreatesSession()
        {
            int firstServerConnections = 0;
            int secondServerConnections = 0;

            await using FakeArticleServer firstServer = await FakeArticleServer.StartAsync(async stream =>
            {
                _ = Interlocked.Increment(ref firstServerConnections);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
            }).ConfigureAwait(false);

            await using FakeArticleServer secondServer = await FakeArticleServer.StartAsync(async stream =>
            {
                _ = Interlocked.Increment(ref secondServerConnections);
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
            }).ConfigureAwait(false);

            Guid accountId = Guid.NewGuid();
            NntpAccountSnapshot initialAccount = CreateAccount(firstServer.Port, maxConnections: 1, username: null, password: null, entryId: accountId);
            NntpAccountSnapshot desiredAccount = initialAccount with { Port = (ushort)secondServer.Port };

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([initialAccount], CancellationToken.None).ConfigureAwait(false);

            NntpAccountSessionReconcileResult result = await manager.ReconcileAccountAsync(desiredAccount, CancellationToken.None).ConfigureAwait(false);

            Assert.True(result.ConnectionSettingsReplaced);
            Assert.Equal(1, result.RetiredSessionCount);
            Assert.Equal(1, result.ActiveSessionCountAfter);
            Assert.Equal(1, Volatile.Read(ref firstServerConnections));
            Assert.Equal(1, Volatile.Read(ref secondServerConnections));
        }

        /// <summary>
        /// Confirms reducing desired capacity while a lease is active retires the session after release without interrupting active ownership.
        /// </summary>
        [Fact]
        public async Task ReconcileAccountAsync_WhenScaleDownWithActiveLease_RetiresAfterLeaseRelease()
        {
            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
            }).ConfigureAwait(false);

            Guid accountId = Guid.NewGuid();
            NntpAccountSnapshot initialAccount = CreateAccount(server.Port, maxConnections: 1, username: null, password: null, entryId: accountId);
            NntpAccountSnapshot desiredAccount = initialAccount with { MaxConnections = 0 };

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([initialAccount], CancellationToken.None).ConfigureAwait(false);

            await using NntpArticleSessionLease lease = await manager.AcquireAsync("<scale-down@test>", CancellationToken.None).ConfigureAwait(false);

            NntpAccountSessionReconcileResult result = await manager.ReconcileAccountAsync(desiredAccount, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, result.RetiredSessionCount);
            Assert.Equal(1, manager.ActiveSessionCount);

            lease.ReportAcquisitionOutcome(NntpArticleAcquisitionFailureCode.None);
            await lease.DisposeAsync().ConfigureAwait(false);

            Assert.Equal(0, manager.ActiveSessionCount);
        }

        /// <summary>
        /// Confirms that multiple session slots are established with concurrent timing.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WithMultipleConnections_EstablishesConnectionsConcurrently()
        {
            const int ConnectionCount = 4;

            int[] connectionStartTimes = new int[ConnectionCount];
            int connectionCounter = -1;

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                int idx = Interlocked.Increment(ref connectionCounter);
                if (idx < ConnectionCount)
                {
                    connectionStartTimes[idx] = Environment.TickCount;
                }
                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
            }, acceptConnectionCount: ConnectionCount).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: ConnectionCount, username: null, password: null);

            Stopwatch stopwatch = Stopwatch.StartNew();
            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);
            stopwatch.Stop();

            Assert.Equal(ConnectionCount, manager.TotalSessionCount);

            // If running concurrently, all connections should start within ~100ms
            // If running serially, they'd be ~250ms apart (fake net delay + processing)
            int minTime = connectionStartTimes.Min();
            int maxTime = connectionStartTimes.Max();
            int spread = maxTime - minTime;

            Assert.True(spread < 300 || manager.TotalSessionCount < ConnectionCount,
                $"Connection start times spread {spread}ms suggests possible serialization");
            Assert.True(stopwatch.ElapsedMilliseconds < 500 || manager.TotalSessionCount < ConnectionCount,
                $"Total initialization {stopwatch.ElapsedMilliseconds}ms suggests serialization");
        }

        /// <summary>
        /// Confirms that partial connection failures during concurrent initialization leave successful sessions usable.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenSomeConnectionsFail_SuccessfulSessionsRemainReady()
        {
            const int RequestedConnections = 4;
            int connectionAttempt = 0;

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                int attempt = Interlocked.Increment(ref connectionAttempt);

                // Fail every other connection
                if (attempt % 2 == 1)
                {
                    // Abruptly close to simulate failure
                    stream.Close();
                    return;
                }

                await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
            }, acceptConnectionCount: RequestedConnections).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: RequestedConnections, username: null, password: null);

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);

            // Some connections should have succeeded
            Assert.True(manager.ActiveSessionCount > 0);
            Assert.True(manager.TotalSessionCount > 0);
        }

        /// <summary>
        /// Confirms that cancellation during concurrent connection establishment behaves correctly.
        /// </summary>
        /// <remarks>
        /// Note: This test verifies cancellation token propagation but is currently skipped
        /// as it requires careful synchronization between server shutdown and accept loop cleanup.
        /// The concurrent connection establishment itself is verified by the other concurrency tests.
        /// </remarks>
        /// <summary>
        /// Confirms the initialize async when cancelled during connections cancels cleanly behavior.
        /// </summary>
        /// <returns>The value returned by the initialize async when cancelled during connections cancels cleanly helper.</returns>
        [Fact(Skip = "Requires refinement of FakeArticleServer cleanup order")]
        public async Task InitializeAsync_WhenCancelledDuringConnections_CancelsCleanly()
        {
            TaskCompletionSource<bool> connectionGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

            await using FakeArticleServer server = await FakeArticleServer.StartAsync(async stream =>
            {
                try
                {
                    // Block on a gate that controls when server can proceed
                    _ = await connectionGate.Task.ConfigureAwait(false);
                    await FakeArticleServer.WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);
                }
                catch
                {
                    // Connection may be closed due to cancellation
                }
            }, acceptConnectionCount: 10).ConfigureAwait(false);

            NntpAccountSnapshot account = CreateAccount(server.Port, maxConnections: 4, username: null, password: null);

            using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(50));

            await using NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);

            try
            {
                // Initialization should be cancelled
                await manager.InitializeAsync([account], cts.Token).ConfigureAwait(false);
                Assert.Fail("Should have thrown OperationCanceledException");
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
            finally
            {
                // Unblock the server to allow cleanup
                _ = connectionGate.TrySetResult(true);
            }
        }

        /// <summary>
        /// Creates one runtime account snapshot for loopback fake-server tests.
        /// </summary>
        /// <param name="port">Loopback NNTP port.</param>
        /// <param name="maxConnections">Maximum session slots derived from this account.</param>
        /// <param name="username">Optional username for AUTHINFO.</param>
        /// <param name="password">Optional password for AUTHINFO.</param>
        /// <param name="entryId">Optional stable account identifier override.</param>
        /// <param name="keepAliveSeconds">Configured idle keepalive timeout in seconds.</param>
        /// <returns>Immutable account snapshot.</returns>
        /// <summary>
        /// Confirms the create account behavior.
        /// </summary>
        /// <returns>The value returned by the create account helper.</returns>
        private static NntpAccountSnapshot CreateAccount(int port, byte maxConnections, string? username, string? password, Guid? entryId = null, byte keepAliveSeconds = 30)
        {
            return new NntpAccountSnapshot(
                EntryId: entryId ?? Guid.NewGuid(),
                Backbone: "TestBackbone",
                Hostname: "127.0.0.1",
                KeepAliveSeconds: keepAliveSeconds,
                MaxConnections: maxConnections,
                Password: password ?? string.Empty,
                Port: (ushort)port,
                ServerId: 1,
                Username: username ?? string.Empty,
                UseSsl: false);
        }

        /// <summary>
        /// Builds parser-compatible test article bytes.
        /// </summary>
        /// <param name="messageId">Message-ID header value.</param>
        /// <param name="body">Body text.</param>
        /// <returns>Article bytes.</returns>
        /// <summary>
        /// Confirms the build article bytes behavior.
        /// </summary>
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
        /// Creates a task-completion signal that resumes waiters asynchronously.
        /// </summary>
        /// <returns>Uncompleted coordination signal for deterministic protocol sequencing.</returns>
        private static TaskCompletionSource<bool> CreateSignal()
        {
            return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>
        /// Yields control so timer-driven background continuations can observe newly advanced manual time.
        /// </summary>
        /// <returns>A task that completes after queued continuations have had an opportunity to run.</returns>
        private static async Task FlushBackgroundContinuationsAsync()
        {
            await Task.Yield();
            await Task.Yield();
        }

        /// <summary>
        /// Monitors an active ARTICLE exchange for an unexpected DATE command before the response is released.
        /// </summary>
        /// <param name="stream">Connected protocol stream.</param>
        /// <param name="unexpectedDateObserved">Signal completed when an unexpected DATE line is observed.</param>
        /// <param name="cancellationToken">Cancellation token used to stop monitoring once the guarded phase ends.</param>
        /// <returns>A task that completes when monitoring ends or cancellation is requested.</returns>
        private static async Task MonitorUnexpectedDateWhileArticleActiveAsync(Stream stream, TaskCompletionSource<bool> unexpectedDateObserved, CancellationToken cancellationToken)
        {
            try
            {
                string line = await FakeArticleServer.ReadAsciiLineAsync(stream, cancellationToken).ConfigureAwait(false);
                if (string.Equals(line, "DATE", StringComparison.Ordinal))
                {
                    _ = unexpectedDateObserved.TrySetResult(true);
                }
                else
                {
                    Assert.Fail($"Unexpected protocol command while ARTICLE was active: {line}");
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        /// <summary>
        /// Deterministic time provider that advances UTC time and timer callbacks only when tests request it.
        /// </summary>
        private sealed class ManualTimeProvider : TimeProvider
        {
            /// <summary>
            /// Synchronizes mutable timer and clock state.
            /// </summary>
            private readonly object _gate = new();

            /// <summary>
            /// Tracks timers created through this provider.
            /// </summary>
            private readonly List<ManualTimer> _timers = [];

            /// <summary>
            /// Current deterministic UTC time.
            /// </summary>
            private DateTimeOffset _utcNow;

            /// <summary>
            /// Initializes the provider at a deterministic UTC timestamp.
            /// </summary>
            /// <param name="utcNow">Initial UTC time returned by the provider.</param>
            internal ManualTimeProvider(DateTimeOffset utcNow)
            {
                _utcNow = utcNow;
            }

            /// <summary>
            /// Returns the current deterministic UTC time.
            /// </summary>
            /// <returns>Current UTC time.</returns>
            public override DateTimeOffset GetUtcNow()
            {
                lock (_gate)
                {
                    return _utcNow;
                }
            }

            /// <summary>
            /// Creates a timer bound to this provider's manually advanced clock.
            /// </summary>
            /// <param name="callback">Callback invoked when the timer becomes due.</param>
            /// <param name="state">Optional timer state.</param>
            /// <param name="dueTime">Initial due time relative to the current manual clock.</param>
            /// <param name="period">Recurring timer period or <see cref="Timeout.InfiniteTimeSpan"/> for one-shot timers.</param>
            /// <returns>Timer handle controlled by this provider.</returns>
            public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            {
                ArgumentNullException.ThrowIfNull(callback);

                ManualTimer timer = new(this, callback, state);
                timer.Change(dueTime, period);
                lock (_gate)
                {
                    _timers.Add(timer);
                }

                return timer;
            }

            /// <summary>
            /// Advances the manual clock and synchronously fires any timers that become due.
            /// </summary>
            /// <param name="elapsed">Amount of time to advance.</param>
            internal void Advance(TimeSpan elapsed)
            {
                ArgumentOutOfRangeException.ThrowIfLessThan(elapsed, TimeSpan.Zero);

                List<(TimerCallback Callback, object? State)> dueCallbacks = [];

                lock (_gate)
                {
                    _utcNow += elapsed;
                    while (TryCollectNextDueCallback(dueCallbacks))
                    {
                    }
                }

                foreach ((TimerCallback callback, object? state) in dueCallbacks)
                {
                    callback(state);
                }
            }

            /// <summary>
            /// Removes a disposed timer from provider tracking.
            /// </summary>
            /// <param name="timer">Timer to remove.</param>
            private void RemoveTimer(ManualTimer timer)
            {
                lock (_gate)
                {
                    _timers.Remove(timer);
                }
            }

            /// <summary>
            /// Collects the next due timer callback, if any, and advances that timer's schedule.
            /// </summary>
            /// <param name="dueCallbacks">Callback collection that receives the next due callback.</param>
            /// <returns><see langword="true"/> when a due callback was collected; otherwise <see langword="false"/>.</returns>
            private bool TryCollectNextDueCallback(List<(TimerCallback Callback, object? State)> dueCallbacks)
            {
                ManualTimer? nextTimer = null;
                DateTimeOffset nextDueUtc = DateTimeOffset.MaxValue;

                foreach (ManualTimer timer in _timers)
                {
                    if (!timer.TryGetNextDueUtc(out DateTimeOffset dueUtc) || dueUtc > _utcNow)
                    {
                        continue;
                    }

                    if (nextTimer is null || dueUtc < nextDueUtc)
                    {
                        nextTimer = timer;
                        nextDueUtc = dueUtc;
                    }
                }

                if (nextTimer is null)
                {
                    return false;
                }

                nextTimer.AdvanceScheduleFromFire();
                dueCallbacks.Add((nextTimer.Callback, nextTimer.State));
                return true;
            }

            /// <summary>
            /// Provider-owned timer implementation whose schedule advances only when manual time moves forward.
            /// </summary>
            private sealed class ManualTimer : ITimer
            {
                /// <summary>
                /// Owning manual time provider.
                /// </summary>
                private readonly ManualTimeProvider _owner;

                /// <summary>
                /// Timer callback invoked when due.
                /// </summary>
                private readonly TimerCallback _callback;

                /// <summary>
                /// Optional callback state.
                /// </summary>
                private readonly object? _state;

                /// <summary>
                /// Recurrence period or <see cref="Timeout.InfiniteTimeSpan"/> for one-shot timers.
                /// </summary>
                private TimeSpan _period = Timeout.InfiniteTimeSpan;

                /// <summary>
                /// Next absolute due time or <see langword="null"/> when disabled.
                /// </summary>
                private DateTimeOffset? _nextDueUtc;

                /// <summary>
                /// Tracks whether disposal has been requested.
                /// </summary>
                private bool _disposed;

                /// <summary>
                /// Initializes a provider-owned manual timer.
                /// </summary>
                /// <param name="owner">Owning manual time provider.</param>
                /// <param name="callback">Callback invoked when the timer becomes due.</param>
                /// <param name="state">Optional callback state.</param>
                internal ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state)
                {
                    _owner = owner;
                    _callback = callback;
                    _state = state;
                }

                /// <summary>
                /// Returns the callback associated with this timer.
                /// </summary>
                internal TimerCallback Callback => _callback;

                /// <summary>
                /// Returns the state associated with this timer.
                /// </summary>
                internal object? State => _state;

                /// <summary>
                /// Changes the timer due time and recurrence period.
                /// </summary>
                /// <param name="dueTime">New due time relative to the current manual clock.</param>
                /// <param name="period">New recurrence period or <see cref="Timeout.InfiniteTimeSpan"/> for one-shot timers.</param>
                /// <returns><see langword="true"/> when the timer was updated; otherwise <see langword="false"/> if already disposed.</returns>
                public bool Change(TimeSpan dueTime, TimeSpan period)
                {
                    lock (_owner._gate)
                    {
                        if (_disposed)
                        {
                            return false;
                        }

                        ValidateDelay(dueTime, nameof(dueTime));
                        ValidateDelay(period, nameof(period));
                        _period = period;
                        _nextDueUtc = dueTime == Timeout.InfiniteTimeSpan ? null : _owner._utcNow + dueTime;
                        return true;
                    }
                }

                /// <summary>
                /// Disposes the timer and removes it from provider tracking.
                /// </summary>
                public void Dispose()
                {
                    bool removed = false;
                    lock (_owner._gate)
                    {
                        if (_disposed)
                        {
                            return;
                        }

                        _disposed = true;
                        _nextDueUtc = null;
                        removed = true;
                    }

                    if (removed)
                    {
                        _owner.RemoveTimer(this);
                    }
                }

                /// <summary>
                /// Disposes the timer asynchronously.
                /// </summary>
                /// <returns>A completed disposal task.</returns>
                public ValueTask DisposeAsync()
                {
                    Dispose();
                    return ValueTask.CompletedTask;
                }

                /// <summary>
                /// Returns the next due timestamp when the timer is enabled.
                /// </summary>
                /// <param name="dueUtc">Receives the next due time when available.</param>
                /// <returns><see langword="true"/> when the timer is enabled; otherwise <see langword="false"/>.</returns>
                internal bool TryGetNextDueUtc(out DateTimeOffset dueUtc)
                {
                    if (!_disposed && _nextDueUtc is DateTimeOffset nextDueUtc)
                    {
                        dueUtc = nextDueUtc;
                        return true;
                    }

                    dueUtc = default;
                    return false;
                }

                /// <summary>
                /// Advances the timer schedule after one due callback has been collected.
                /// </summary>
                internal void AdvanceScheduleFromFire()
                {
                    if (_nextDueUtc is null)
                    {
                        return;
                    }

                    if (_period == Timeout.InfiniteTimeSpan)
                    {
                        _nextDueUtc = null;
                        return;
                    }

                    _nextDueUtc += _period;
                }

                /// <summary>
                /// Validates timer delay arguments accepted by this manual provider.
                /// </summary>
                /// <param name="delay">Delay value to validate.</param>
                /// <param name="parameterName">Associated parameter name.</param>
                /// <exception cref="ArgumentOutOfRangeException">Thrown when the delay is negative and not <see cref="Timeout.InfiniteTimeSpan"/>.</exception>
                private static void ValidateDelay(TimeSpan delay, string parameterName)
                {
                    if (delay < TimeSpan.Zero && delay != Timeout.InfiniteTimeSpan)
                    {
                        throw new ArgumentOutOfRangeException(parameterName);
                    }
                }
            }
        }

        /// <summary>
        /// Minimal in-process fake NNTP server for session-manager contract tests.
        /// </summary>
        /// <summary>
        /// Captured log entry used for structured warning assertions.
        /// </summary>
        /// <param name="Level">Log level.</param>
        /// <param name="Message">Rendered message text.</param>
        /// <summary>
        /// Confirms the captured log entry behavior.
        /// </summary>
        /// <returns>The value returned by the captured log entry helper.</returns>
        private sealed record CapturedLogEntry(LogLevel Level, string Message);

        /// <summary>
        /// In-memory logger provider for deterministic session-manager log assertions.
        /// </summary>
        private sealed class CapturingLoggerProvider : ILoggerProvider
        {
            /// <summary>
            /// Synchronization lock for concurrent test logger writes.
            /// </summary>
            private readonly object _gate = new();

            /// <summary>
            /// Captured log entries.
            /// </summary>
            internal List<CapturedLogEntry> Entries { get; } = [];

            /// <summary>
            /// Creates a typed capturing logger.
            /// </summary>
            /// <typeparam name="T">Logger category type.</typeparam>
            /// <returns>Capturing logger instance.</returns>
            internal ILogger<T> CreateLogger<T>()
            {
                return new CapturingLogger<T>(Entries, _gate);
            }

            /// <summary>
            /// Creates a logger instance for provider registration APIs.
            /// </summary>
            /// <param name="categoryName">Logger category name.</param>
            /// <returns>Capturing logger instance.</returns>
            /// <summary>
            /// Confirms the create logger behavior.
            /// </summary>
            /// <returns>The value returned by the create logger helper.</returns>
            public ILogger CreateLogger(string categoryName)
            {
                return new CapturingLogger(Entries, _gate);
            }

            /// <summary>
            /// Disposes provider resources.
            /// </summary>
            public void Dispose()
            {
            }

            /// <summary>
            /// Non-generic in-memory logger implementation.
            /// </summary>
            private sealed class CapturingLogger : ILogger
            {
                /// <summary>
                /// Null logging scope singleton for non-generic logger scope API compatibility.
                /// </summary>
                private sealed class NullScope : IDisposable
                {
                    /// <summary>
                    /// Singleton instance.
                    /// </summary>
                    internal static readonly NullScope Instance = new();

                    /// <summary>
                    /// Disposes scope instance.
                    /// </summary>
                    public void Dispose()
                    {
                    }
                }
                /// <summary>
                /// Captured-entry destination list.
                /// </summary>
                private readonly List<CapturedLogEntry> _entries;

                /// <summary>
                /// Synchronization lock.
                /// </summary>
                private readonly object _gate;

                /// <summary>
                /// Initializes a new capturing logger.
                /// </summary>
                /// <param name="entries">Captured-entry destination list.</param>
                /// <param name="gate">Synchronization lock.</param>
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
                /// Begins a logging scope.
                /// </summary>
                /// <typeparam name="TState">Scope state type.</typeparam>
                /// <param name="state">Scope state.</param>
                /// <returns>Scope disposable.</returns>
                public IDisposable BeginScope<TState>(TState state)
                    where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
                /// Returns a value indicating whether the log level is enabled.
                /// </summary>
                /// <param name="logLevel">Log level.</param>
                /// <returns>Always <see langword="true"/> for test capture.</returns>
                /// <summary>
                /// Confirms the is enabled behavior.
                /// </summary>
                /// <returns>The value returned by the is enabled helper.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return true;
                }

                /// <summary>
                /// Captures one log entry.
                /// </summary>
                /// <typeparam name="TState">State type.</typeparam>
                /// <param name="logLevel">Log level.</param>
                /// <param name="eventId">Event identifier.</param>
                /// <param name="state">State payload.</param>
                /// <param name="exception">Optional exception.</param>
                /// <param name="formatter">Formatter delegate.</param>
                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    lock (_gate)
                    {
                        _entries.Add(new CapturedLogEntry(logLevel, message));
                    }
                }
            }

            /// <summary>
            /// Null logging scope singleton shared by generic and non-generic test loggers.
            /// </summary>
            private sealed class NullScope : IDisposable
            {
                /// <summary>
                /// Singleton instance.
                /// </summary>
                internal static readonly NullScope Instance = new();

                /// <summary>
                /// Disposes scope instance.
                /// </summary>
                public void Dispose()
                {
                }
            }

            /// <summary>
            /// Typed in-memory logger implementation.
            /// </summary>
            /// <typeparam name="T">Logger category type.</typeparam>
            private sealed class CapturingLogger<T> : ILogger<T>
            {
                /// <summary>
                /// Captured-entry destination list.
                /// </summary>
                private readonly List<CapturedLogEntry> _entries;

                /// <summary>
                /// Synchronization lock.
                /// </summary>
                private readonly object _gate;

                /// <summary>
                /// Initializes a new capturing logger.
                /// </summary>
                /// <param name="entries">Captured-entry destination list.</param>
                /// <param name="gate">Synchronization lock.</param>
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
                /// Begins a logging scope.
                /// </summary>
                /// <typeparam name="TState">Scope state type.</typeparam>
                /// <param name="state">Scope state.</param>
                /// <returns>Scope disposable.</returns>
                public IDisposable BeginScope<TState>(TState state)
                    where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
                /// Returns a value indicating whether the log level is enabled.
                /// </summary>
                /// <param name="logLevel">Log level.</param>
                /// <returns>Always <see langword="true"/> for test capture.</returns>
                /// <summary>
                /// Confirms the is enabled behavior.
                /// </summary>
                /// <returns>The value returned by the is enabled helper.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return true;
                }

                /// <summary>
                /// Captures one log entry.
                /// </summary>
                /// <typeparam name="TState">State type.</typeparam>
                /// <param name="logLevel">Log level.</param>
                /// <param name="eventId">Event identifier.</param>
                /// <param name="state">State payload.</param>
                /// <param name="exception">Optional exception.</param>
                /// <param name="formatter">Formatter delegate.</param>
                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    lock (_gate)
                    {
                        _entries.Add(new CapturedLogEntry(logLevel, message));
                    }
                }

            }
        }

        /// <summary>
        /// Confirms the fake article server behavior.
        /// </summary>
        private sealed class FakeArticleServer : IAsyncDisposable
        {
            /// <summary>
            /// Listener.
            /// </summary>
            private readonly TcpListener _listener;

            /// <summary>
            /// Session callback invoked for each accepted connection.
            /// </summary>
            private readonly Func<NetworkStream, Task> _session;

            /// <summary>
            /// Cancellation source controlling accept loop shutdown.
            /// </summary>
            private readonly CancellationTokenSource _shutdown = new();

            /// <summary>
            /// Accept-loop task.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
            /// Number of accepted connections expected before normal stop.
            /// </summary>
            private readonly int _acceptConnectionCount;

            /// <summary>
            /// Initializes fake server.
            /// </summary>
            /// <param name="listener">Bound listener.</param>
            /// <param name="session">Per-connection session callback.</param>
            /// <param name="acceptConnectionCount">Expected connection accept count.</param>
            /// <summary>
            /// Confirms the r behavior.
            /// </summary>
            /// <returns>The value returned by the r helper.</returns>
            private FakeArticleServer(TcpListener listener, Func<NetworkStream, Task> session, int acceptConnectionCount)
            {
                _listener = listener;
                _session = session;
                _acceptConnectionCount = acceptConnectionCount;
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            /// <summary>
            /// Starts fake server.
            /// </summary>
            /// <param name="session">Per-connection callback script.</param>
            /// <param name="acceptConnectionCount">Expected connection count before normal loop completion.</param>
            /// <returns>Started fake server.</returns>
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <returns>The value returned by the start async helper.</returns>
            internal static async Task<FakeArticleServer> StartAsync(Func<NetworkStream, Task> session, int acceptConnectionCount = 1)
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                FakeArticleServer server = new(listener, session, acceptConnectionCount);
                await Task.Delay(20).ConfigureAwait(false);
                return server;
            }

            /// <summary>
            /// Gets bound loopback port.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Reads one ASCII line and validates expected content.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="expected">Expected line content.</param>
            /// <returns>A task that completes when the assertion passes.</returns>
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
            /// <param name="stream">Network stream.</param>
            /// <param name="cancellationToken">Cancellation token.</param>
            /// <returns>ASCII line text.</returns>
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
            /// Writes one ASCII line with CRLF.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="line">Line content.</param>
            /// <returns>A task that completes when write and flush finish.</returns>
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
            /// Writes raw bytes and flushes the stream.
            /// </summary>
            /// <param name="stream">Network stream.</param>
            /// <param name="bytes">Bytes to write.</param>
            /// <returns>A task that completes when write and flush finish.</returns>
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
            /// Disposes server resources and joins accept loop.
            /// </summary>
            /// <returns>A task that completes after loop termination.</returns>
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
            /// <returns>A task that completes after expected connections are processed or shutdown is requested.</returns>
            /// <summary>
            /// Confirms the accept loop async behavior.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            private async Task AcceptLoopAsync()
            {
                try
                {
                    for (int i = 0; i < _acceptConnectionCount; i++)
                    {
                        using TcpClient client = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                        using NetworkStream stream = client.GetStream();
                        await _session(stream).ConfigureAwait(false);
                    }
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

