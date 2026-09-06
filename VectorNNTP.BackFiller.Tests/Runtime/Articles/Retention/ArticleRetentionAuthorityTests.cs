// <copyright file="ArticleRetentionAuthorityTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Retention
// Focused tests for shared in-memory article retention ownership, admission, completion, and lease semantics.

using System.Buffers;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Retention
{
    /// <summary>
    /// Verifies shared in-memory retention admission, lookup, completion, expiration, and shutdown behavior.
    /// </summary>
    public sealed class ArticleRetentionAuthorityTests
    {
        [Fact]
        public void TryRetainSuccessArticle_WhenAdmitted_TransfersOwnershipAndLeaseReadsPayload()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            DownloadedArticleBuffer payloadOwner = CreateBuffer("retained-payload-1");
            string messageId = "<retention-admit@example.com>";

            ArticleRetentionAdmissionResult admission = authority.TryRetainSuccessArticle(messageId, payloadOwner);

            Assert.Equal(ArticleRetentionAdmissionStatus.Admitted, admission.Status);
            Assert.True(admission.IsAdmitted);

            ArticleRetentionReadLeaseResult leaseResult = authority.TryAcquireReadLeaseByMessageId(messageId);
            Assert.True(leaseResult.IsAcquired);
            using IArticleRetentionReadLease lease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(leaseResult.Lease);
            Assert.Equal(payloadOwner.Length, lease.PayloadBytes);
        }

        [Fact]
        public void TryRetainSuccessArticle_WhenDuplicateMessageId_IsRejectedAndExistingEntryPreserved()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<retention-duplicate@example.com>";

            ArticleRetentionAdmissionResult first = authority.TryRetainSuccessArticle(messageId, CreateBuffer("payload-first"));
            ArticleRetentionAdmissionResult second = authority.TryRetainSuccessArticle(messageId, CreateBuffer("payload-second"));

            Assert.Equal(ArticleRetentionAdmissionStatus.Admitted, first.Status);
            Assert.Equal(ArticleRetentionAdmissionStatus.DuplicateMessageId, second.Status);

            ArticleRetentionReadLeaseResult leaseResult = authority.TryAcquireReadLeaseByMessageId(messageId);
            Assert.True(leaseResult.IsAcquired);
            using IArticleRetentionReadLease lease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(leaseResult.Lease);
            Assert.Equal("payload-first".Length, lease.PayloadBytes);
        }

        [Fact]
        public void TryRetainSuccessArticle_WhenPayloadExceedsCapacity_IsRejected()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 8);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            DownloadedArticleBuffer payloadOwner = CreateBuffer("payload-larger-than-capacity");

            ArticleRetentionAdmissionResult admission = authority.TryRetainSuccessArticle("<oversize@example.com>", payloadOwner);

            Assert.Equal(ArticleRetentionAdmissionStatus.PayloadExceedsCapacity, admission.Status);
            Assert.False(admission.IsAdmitted);
        }

        [Fact]
        public void TryRetainSuccessArticle_WhenCapacityExceeded_EvictsOldestFirst()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 32);
            ArticleRetentionAuthority authority = new(runtimeOptions);

            _ = authority.TryRetainSuccessArticle("<oldest@example.com>", CreateBuffer("aaaaaaaaaaaaaaaa"), DateTimeOffset.UtcNow.AddSeconds(-3));
            _ = authority.TryRetainSuccessArticle("<newer@example.com>", CreateBuffer("bbbbbbbbbbbbbbbb"), DateTimeOffset.UtcNow.AddSeconds(-2));
            ArticleRetentionAdmissionResult third = authority.TryRetainSuccessArticle("<incoming@example.com>", CreateBuffer("cccccccccccccccc"), DateTimeOffset.UtcNow.AddSeconds(-1));

            Assert.Equal(ArticleRetentionAdmissionStatus.Admitted, third.Status);
            Assert.Equal(ArticleRetentionReadLeaseStatus.NotFound, authority.TryAcquireReadLeaseByMessageId("<oldest@example.com>").Status);
            Assert.True(authority.TryAcquireReadLeaseByMessageId("<newer@example.com>").IsAcquired);
            Assert.True(authority.TryAcquireReadLeaseByMessageId("<incoming@example.com>").IsAcquired);
        }

        [Fact]
        public void TryAcquireReadLeaseByMessageIdMd5_WhenPresent_AcquiresLease()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<lookup-md5@example.com>";
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("lookup-by-md5"));
            string messageIdMd5 = MessageIdHashing.ComputeCanonicalMd5Hex(messageId);

            ArticleRetentionReadLeaseResult leaseResult = authority.TryAcquireReadLeaseByMessageIdMd5(messageIdMd5);

            Assert.True(leaseResult.IsAcquired);
            using IArticleRetentionReadLease lease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(leaseResult.Lease);
            Assert.Equal(messageId, lease.MessageId);
            Assert.Equal(messageIdMd5, lease.MessageIdMd5);
        }

        [Fact]
        public void Completion_WhenBothConsumersComplete_RemovesEntry()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<completion-remove@example.com>";
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("completion-payload"));

            ArticleRetentionCompletionResult transit = authority.MarkTransitCompleted(messageId);
            ArticleRetentionCompletionResult listener = authority.MarkListenerCompleted(messageId);

            Assert.Equal(ArticleRetentionCompletionStatus.Completed, transit.Status);
            Assert.Equal(ArticleRetentionCompletionStatus.Completed, listener.Status);
            Assert.True(listener.EntryRemoved || transit.EntryRemoved);
            Assert.Equal(ArticleRetentionReadLeaseStatus.NotFound, authority.TryAcquireReadLeaseByMessageId(messageId).Status);
        }

        [Fact]
        public void Completion_WhenDuplicateCall_IsIdempotent()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<completion-idempotent@example.com>";
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("completion-idempotent"));

            ArticleRetentionCompletionResult first = authority.MarkTransitCompleted(messageId);
            ArticleRetentionCompletionResult second = authority.MarkTransitCompleted(messageId);

            Assert.False(first.AlreadyCompleted);
            Assert.True(second.AlreadyCompleted);
            Assert.Equal(ArticleRetentionCompletionStatus.Completed, second.Status);
        }

        [Fact]
        public void ExpireEligibleArticles_WhenTtlElapsed_RemovesEntry()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<ttl-expire@example.com>";
            DateTimeOffset inserted = DateTimeOffset.UtcNow.AddSeconds(-120);
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("ttl-expire"), inserted);

            long expiredBytes = authority.ExpireEligibleArticles(DateTimeOffset.UtcNow);

            Assert.True(expiredBytes > 0);
            Assert.Equal(ArticleRetentionReadLeaseStatus.NotFound, authority.TryAcquireReadLeaseByMessageId(messageId).Status);
        }

        [Fact]
        public void BeginShutdown_StopsNewAdmissionsButRetainsExistingEntries()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string existingMessageId = "<shutdown-existing@example.com>";
            _ = authority.TryRetainSuccessArticle(existingMessageId, CreateBuffer("shutdown-existing"));

            authority.BeginShutdown();
            ArticleRetentionAdmissionResult rejected = authority.TryRetainSuccessArticle("<shutdown-new@example.com>", CreateBuffer("shutdown-new"));

            Assert.Equal(ArticleRetentionAdmissionStatus.AdmissionClosed, rejected.Status);
            Assert.True(authority.TryAcquireReadLeaseByMessageId(existingMessageId).IsAcquired);
        }

        [Fact]
        public void TryRetainSuccessArticle_WhenOldestEntryHasActiveLease_EvictsNextEntriesAndPreservesLeasedPayloadUntilRelease()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 30);
            ArticleRetentionAuthority authority = new(runtimeOptions);

            _ = authority.TryRetainSuccessArticle("<entry-a@example.com>", CreateBuffer("aaaaaaaaaa"), DateTimeOffset.UtcNow.AddSeconds(-30));
            _ = authority.TryRetainSuccessArticle("<entry-b@example.com>", CreateBuffer("bbbbbbbbbb"), DateTimeOffset.UtcNow.AddSeconds(-20));
            _ = authority.TryRetainSuccessArticle("<entry-c@example.com>", CreateBuffer("cccccccccc"), DateTimeOffset.UtcNow.AddSeconds(-10));

            ArticleRetentionReadLeaseResult leaseAResult = authority.TryAcquireReadLeaseByMessageId("<entry-a@example.com>");
            Assert.True(leaseAResult.IsAcquired);
            using IArticleRetentionReadLease leaseA = Assert.IsAssignableFrom<IArticleRetentionReadLease>(leaseAResult.Lease);

            ArticleRetentionAdmissionResult admitted = authority.TryRetainSuccessArticle("<entry-d@example.com>", CreateBuffer("dddddddddd"), DateTimeOffset.UtcNow);
            Assert.Equal(ArticleRetentionAdmissionStatus.Admitted, admitted.Status);

            Assert.Equal(ArticleRetentionReadLeaseStatus.NotFound, authority.TryAcquireReadLeaseByMessageId("<entry-a@example.com>").Status);
            Assert.Equal(ArticleRetentionReadLeaseStatus.NotFound, authority.TryAcquireReadLeaseByMessageId("<entry-b@example.com>").Status);

            ArticleRetentionReadLeaseResult leaseCResult = authority.TryAcquireReadLeaseByMessageId("<entry-c@example.com>");
            Assert.True(leaseCResult.IsAcquired);
            leaseCResult.Lease?.Dispose();

            ArticleRetentionReadLeaseResult leaseDResult = authority.TryAcquireReadLeaseByMessageId("<entry-d@example.com>");
            Assert.True(leaseDResult.IsAcquired);
            leaseDResult.Lease?.Dispose();

            ArticleRetentionSnapshot beforeRelease = authority.GetSnapshot();
            Assert.Equal(3, beforeRelease.RetainedArticleCount);
            Assert.Equal(30, beforeRelease.RetainedPayloadBytes);
            Assert.Equal(1, beforeRelease.ActiveReaderCount);
            Assert.Equal(1, beforeRelease.LogicallyRemovedAwaitingReaderReleaseCount);

            leaseA.Dispose();

            ArticleRetentionSnapshot afterRelease = authority.GetSnapshot();
            Assert.Equal(2, afterRelease.RetainedArticleCount);
            Assert.Equal(20, afterRelease.RetainedPayloadBytes);
            Assert.Equal(0, afterRelease.ActiveReaderCount);
            Assert.Equal(0, afterRelease.LogicallyRemovedAwaitingReaderReleaseCount);
        }

        [Fact]
        public async Task DisposeAsync_WhenActiveLeaseExists_PreservesAccountingUntilLeaseReleaseAsync()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<dispose-active-lease@example.com>";
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("dispose-active-lease"));

            ArticleRetentionReadLeaseResult leaseResult = authority.TryAcquireReadLeaseByMessageId(messageId);
            Assert.True(leaseResult.IsAcquired);
            IArticleRetentionReadLease lease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(leaseResult.Lease);

            await authority.DisposeAsync().ConfigureAwait(false);

            ArticleRetentionSnapshot duringLease = authority.GetSnapshot();
            Assert.Equal(1, duringLease.RetainedArticleCount);
            Assert.True(duringLease.RetainedPayloadBytes > 0);
            Assert.Equal(1, duringLease.ActiveReaderCount);
            Assert.Equal(1, duringLease.LogicallyRemovedAwaitingReaderReleaseCount);

            _ = lease.Payload.Span[0];
            lease.Dispose();
            lease.Dispose();

            ArticleRetentionSnapshot afterRelease = authority.GetSnapshot();
            Assert.Equal(0, afterRelease.RetainedArticleCount);
            Assert.Equal(0, afterRelease.RetainedPayloadBytes);
            Assert.Equal(0, afterRelease.ActiveReaderCount);
            Assert.Equal(0, afterRelease.LogicallyRemovedAwaitingReaderReleaseCount);
        }

        [Fact]
        public void CompletionThenLookup_WhenCompletionWins_LookupFails()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<completion-lookup-race@example.com>";
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("completion-lookup-race"));

            _ = authority.MarkTransitCompleted(messageId);
            _ = authority.MarkListenerCompleted(messageId);

            ArticleRetentionReadLeaseResult lookup = authority.TryAcquireReadLeaseByMessageId(messageId);
            Assert.Equal(ArticleRetentionReadLeaseStatus.NotFound, lookup.Status);
        }

        [Fact]
        public void LookupThenCompletion_WhenLookupWins_LeaseRemainsValidUntilRelease()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<lookup-completion-race@example.com>";
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("lookup-completion-race"));

            ArticleRetentionReadLeaseResult leaseResult = authority.TryAcquireReadLeaseByMessageId(messageId);
            Assert.True(leaseResult.IsAcquired);
            using IArticleRetentionReadLease lease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(leaseResult.Lease);

            _ = authority.MarkTransitCompleted(messageId);
            _ = authority.MarkListenerCompleted(messageId);

            Assert.True(lease.Payload.Length > 0);
            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(1, snapshot.ActiveReaderCount);
            Assert.Equal(1, snapshot.LogicallyRemovedAwaitingReaderReleaseCount);
        }

        [Fact]
        public void MarkTransitCompleted_WhenEntryAlreadyEvicted_ReturnsNotFoundAndDoesNotRecreateEntry()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            string messageId = "<completion-after-eviction@example.com>";
            DateTimeOffset inserted = DateTimeOffset.UtcNow.AddSeconds(-120);
            _ = authority.TryRetainSuccessArticle(messageId, CreateBuffer("completion-after-eviction"), inserted);

            _ = authority.ExpireEligibleArticles(DateTimeOffset.UtcNow);

            ArticleRetentionCompletionResult completion = authority.MarkTransitCompleted(messageId);
            Assert.Equal(ArticleRetentionCompletionStatus.NotFound, completion.Status);

            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(0, snapshot.RetainedArticleCount);
            Assert.Equal(0, snapshot.RetainedPayloadBytes);
            Assert.Equal(0, snapshot.TransitCompletionCount);
        }

        [Fact]
        public void TryRetainSuccessArticle_WhenInvalidPayload_IncrementsInvalidPayloadFailures()
        {
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(capacityBytes: 4096);
            ArticleRetentionAuthority authority = new(runtimeOptions);
            DownloadedArticleBuffer empty = new(ArrayPool<byte>.Shared.Rent(1), 0);

            ArticleRetentionAdmissionResult result = authority.TryRetainSuccessArticle("<invalid-payload@example.com>", empty);

            Assert.Equal(ArticleRetentionAdmissionStatus.InvalidPayload, result.Status);
            ArticleRetentionSnapshot snapshot = authority.GetSnapshot();
            Assert.Equal(1, snapshot.InvalidPayloadFailures);

            empty.Dispose();
        }

        private static BackFillerRuntimeOptions CreateRuntimeOptions(long capacityBytes)
        {
            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "backfiller01.usenet.ninja",
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
                ValidatedLogDirectory: "C:\\logs",
                ValidatedCertificateDirectory: "C:\\certs",
                RabbitMqHosts: ["rabbit01.usenet.ninja"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: true,
                TransitServerHost: "transit01.usenet.ninja",
                TransitServerPort: 563,
                TransitServerUseSsl: true,
                BindPort: 119,
                ArticleRetention: new ArticleRetentionRuntimeOptions(
                    MaximumRetainedPayloadBytes: capacityBytes,
                    RetentionTtl: TimeSpan.FromSeconds(60),
                    SweepInterval: TimeSpan.FromSeconds(1)));
        }

        private static DownloadedArticleBuffer CreateBuffer(string payload)
        {
            byte[] bytes = System.Text.Encoding.ASCII.GetBytes(payload);
            byte[] rented = ArrayPool<byte>.Shared.Rent(bytes.Length);
            Array.Copy(bytes, rented, bytes.Length);
            return new DownloadedArticleBuffer(rented, bytes.Length);
        }
    }
}
