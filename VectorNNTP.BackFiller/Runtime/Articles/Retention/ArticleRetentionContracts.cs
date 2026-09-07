// <copyright file="ArticleRetentionContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Retention
// Contracts for shared in-memory retained article ownership, admission, completion, and read leases.

using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;

namespace VectorNNTP.Backfiller.Runtime.Articles.Retention
{
    /// <summary>
    /// Deterministic outcomes for one article-retention admission attempt.
    /// </summary>
    internal enum ArticleRetentionAdmissionStatus
    {
        /// <summary>
        /// Admission succeeded and payload ownership transferred into retention.
        /// </summary>
        Admitted = 0,

        /// <summary>
        /// Admission was rejected because retention shutdown has closed new admissions; payload ownership does not transfer.
        /// </summary>
        AdmissionClosed = 1,

        /// <summary>
        /// Admission was rejected because an entry with the same Message-ID is already retained; existing retained ownership remains authoritative.
        /// </summary>
        DuplicateMessageId = 2,

        /// <summary>
        /// Admission was rejected because the canonical Message-ID MD5 collides with a different retained Message-ID.
        /// </summary>
        Md5Collision = 3,

        /// <summary>
        /// Admission was rejected because a single payload exceeds total configured retention capacity.
        /// </summary>
        PayloadExceedsCapacity = 4,

        /// <summary>
        /// Admission was rejected because capacity could not be recovered, including after pressure-eviction attempts.
        /// </summary>
        CapacityUnavailable = 5,

        /// <summary>
        /// Admission was rejected because payload ownership was invalid for retention (for example zero-length payload).
        /// </summary>
        InvalidPayload = 6,
    }

    /// <summary>
    /// Deterministic outcomes for one article-retention completion call.
    /// </summary>
    internal enum ArticleRetentionCompletionStatus
    {
        /// <summary>
        /// Completion call was accepted for the requested Message-ID. Logical removal and physical disposal may occur when lifecycle conditions are met.
        /// </summary>
        Completed = 0,

        /// <summary>
        /// Completion call did not find a currently retained entry for the requested Message-ID.
        /// </summary>
        NotFound = 1,
    }

    /// <summary>
    /// Logical removal reason recorded for one retained article lifecycle transition.
    /// </summary>
    internal enum ArticleRetentionRemovalReason
    {
        /// <summary>
        /// Retention marked the entry logically removed after both Transit and Listener completion channels were observed.
        /// </summary>
        BothConsumersCompleted = 0,

        /// <summary>
        /// Retention marked the entry logically removed under capacity pressure to recover admission headroom.
        /// </summary>
        PressureEvicted = 1,

        /// <summary>
        /// Retention marked the entry logically removed because TTL expiration made it ineligible to remain readable.
        /// </summary>
        TtlExpired = 2,
    }

    /// <summary>
    /// Consumer completion channel tracked independently for retained article lifecycle removal.
    /// </summary>
    internal enum ArticleRetentionConsumer
    {
        Transit = 0,
        Listener = 1,
    }

    /// <summary>
    /// Lookup status for retention read-lease acquisition attempts.
    /// </summary>
    internal enum ArticleRetentionReadLeaseStatus
    {
        /// <summary>
        /// A read lease was acquired and caller may read payload bytes until the lease is released.
        /// </summary>
        Acquired = 0,

        /// <summary>
        /// No retained entry was found for the requested Message-ID identity.
        /// </summary>
        NotFound = 1,

        /// <summary>
        /// Entry identity is known but not leasable for new readers (for example logically removed or disposed); physical payload retention may still be deferred by active leases.
        /// </summary>
        Unavailable = 2,
    }

    /// <summary>
    /// Result of one article-retention admission attempt.
    /// </summary>
    /// <param name="Status">Deterministic admission status.</param>
    /// <param name="MessageId">Requested Message-ID.</param>
    /// <param name="MessageIdMd5">Canonical Message-ID MD5 when computed.</param>
    /// <param name="RetainedPayloadBytes">Retained payload bytes after the operation.</param>
    /// <param name="ReleasedPayloadBytes">Payload bytes released by pressure/expiration work during this operation.</param>
    /// <param name="ExistingMessageId">Existing retained Message-ID for duplicate/collision outcomes.</param>
    /// <param name="ExistingMessageIdMd5">Existing retained canonical MD5 for duplicate/collision outcomes.</param>
    internal sealed record ArticleRetentionAdmissionResult(
        ArticleRetentionAdmissionStatus Status,
        string MessageId,
        string? MessageIdMd5,
        long RetainedPayloadBytes,
        long ReleasedPayloadBytes,
        string? ExistingMessageId,
        string? ExistingMessageIdMd5)
    {
        /// <summary>
        /// Gets a value indicating whether admission succeeded and ownership transferred.
        /// </summary>
        internal bool IsAdmitted => Status == ArticleRetentionAdmissionStatus.Admitted;
    }

    /// <summary>
    /// Result of one completion call for Transit or Listener responsibility.
    /// </summary>
    /// <param name="Status">Deterministic completion status.</param>
    /// <param name="MessageId">Target Message-ID.</param>
    /// <param name="EntryRemoved">Whether logical removal occurred during this call.</param>
    /// <param name="AlreadyCompleted">Whether this consumer channel had already been completed before this call.</param>
    /// <param name="RemovalReason">Logical removal reason when removal occurred during this call.</param>
    internal sealed record ArticleRetentionCompletionResult(
        ArticleRetentionCompletionStatus Status,
        string MessageId,
        bool EntryRemoved,
        bool AlreadyCompleted,
        ArticleRetentionRemovalReason? RemovalReason);

    /// <summary>
    /// Snapshot view of retention authority counters and current retained set characteristics.
    /// </summary>
    /// <param name="RetainedArticleCount">Count of retained entries that still physically own payload bytes.</param>
    /// <param name="RetainedPayloadBytes">Aggregate retained payload bytes still physically owned.</param>
    /// <param name="OldestRetainedArticleAge">Age of the oldest currently retained entry.</param>
    /// <param name="AdmissionAttempts">Total admission attempts observed.</param>
    /// <param name="AdmissionSuccesses">Total successful admissions.</param>
    /// <param name="AdmissionClosedFailures">Admission failures due to shutdown/admission closure.</param>
    /// <param name="CapacityFailures">Admission failures due to unrecoverable capacity exhaustion.</param>
    /// <param name="PayloadTooLargeFailures">Admission failures where one payload exceeded total configured capacity.</param>
    /// <param name="InvalidPayloadFailures">Admission failures where payload length/ownership was invalid for retention.</param>
    /// <param name="Md5CollisionFailures">Admission failures due to canonical MD5 collisions against different Message-IDs.</param>
    /// <param name="DuplicateAdmissionAttempts">Admission attempts rejected due to duplicate Message-ID.</param>
    /// <param name="PressureEvictionCount">Count of logical removals caused by pressure eviction.</param>
    /// <param name="PressureEvictedBytes">Total bytes associated with pressure-evicted entries.</param>
    /// <param name="TtlExpirationCount">Count of logical removals caused by TTL expiration.</param>
    /// <param name="TtlExpiredBytes">Total bytes associated with TTL-expired entries.</param>
    /// <param name="ActiveReaderCount">Current count of active read leases.</param>
    /// <param name="LogicallyRemovedAwaitingReaderReleaseCount">Entries logically removed but still physically retained due to active leases.</param>
    /// <param name="TransitCompletionCount">Total first-time transit completions observed.</param>
    /// <param name="ListenerCompletionCount">Total first-time listener completions observed.</param>
    /// <param name="TotalBytesReleased">Total payload bytes physically released by the retention authority.</param>
    internal sealed record ArticleRetentionSnapshot(
        long RetainedArticleCount,
        long RetainedPayloadBytes,
        TimeSpan OldestRetainedArticleAge,
        long AdmissionAttempts,
        long AdmissionSuccesses,
        long AdmissionClosedFailures,
        long CapacityFailures,
        long PayloadTooLargeFailures,
        long InvalidPayloadFailures,
        long Md5CollisionFailures,
        long DuplicateAdmissionAttempts,
        long PressureEvictionCount,
        long PressureEvictedBytes,
        long TtlExpirationCount,
        long TtlExpiredBytes,
        long ActiveReaderCount,
        long LogicallyRemovedAwaitingReaderReleaseCount,
        long TransitCompletionCount,
        long ListenerCompletionCount,
        long TotalBytesReleased);

    /// <summary>
    /// Read lease over one retained article payload.
    /// </summary>
    internal interface IArticleRetentionReadLease : IDisposable
    {
        /// <summary>
        /// Gets retained article Message-ID.
        /// </summary>
        public string MessageId { get; }

        /// <summary>
        /// Gets canonical retained Message-ID MD5.
        /// </summary>
        public string MessageIdMd5 { get; }

        /// <summary>
        /// Gets retained payload byte count.
        /// </summary>
        public int PayloadBytes { get; }

        /// <summary>
        /// Gets read-only retained payload bytes.
        /// </summary>
        public ReadOnlyMemory<byte> Payload { get; }
    }

    /// <summary>
    /// Result of one read-lease lookup/acquisition attempt.
    /// </summary>
    /// <param name="Status">Lookup/acquisition status.</param>
    /// <param name="Lease">Acquired lease when <paramref name="Status"/> is <see cref="ArticleRetentionReadLeaseStatus.Acquired"/>.</param>
    /// <param name="MessageId">Known Message-ID identity for unavailable outcomes when present.</param>
    /// <param name="MessageIdMd5">Known canonical MD5 identity for unavailable outcomes when present.</param>
    internal sealed record ArticleRetentionReadLeaseResult(
        ArticleRetentionReadLeaseStatus Status,
        IArticleRetentionReadLease? Lease,
        string? MessageId,
        string? MessageIdMd5)
    {
        /// <summary>
        /// Gets a value indicating whether a read lease was acquired.
        /// </summary>
        internal bool IsAcquired => Status == ArticleRetentionReadLeaseStatus.Acquired && Lease is not null;

        /// <summary>
        /// Creates a not-found lookup result.
        /// </summary>
        internal static ArticleRetentionReadLeaseResult NotFound()
        {
            return new ArticleRetentionReadLeaseResult(
                Status: ArticleRetentionReadLeaseStatus.NotFound,
                Lease: null,
                MessageId: null,
                MessageIdMd5: null);
        }

        /// <summary>
        /// Creates an unavailable lookup result for a known identity that is no longer leasable.
        /// </summary>
        internal static ArticleRetentionReadLeaseResult Unavailable(string messageId, string messageIdMd5)
        {
            return new ArticleRetentionReadLeaseResult(
                Status: ArticleRetentionReadLeaseStatus.Unavailable,
                Lease: null,
                MessageId: messageId,
                MessageIdMd5: messageIdMd5);
        }

        /// <summary>
        /// Creates a successful lease acquisition result.
        /// </summary>
        internal static ArticleRetentionReadLeaseResult Acquired(IArticleRetentionReadLease lease)
        {
            ArgumentNullException.ThrowIfNull(lease);
            return new ArticleRetentionReadLeaseResult(
                Status: ArticleRetentionReadLeaseStatus.Acquired,
                Lease: lease,
                MessageId: lease.MessageId,
                MessageIdMd5: lease.MessageIdMd5);
        }
    }

    /// <summary>
    /// Shared in-memory authority that owns retained article payload lifecycle and exposes safe read leases.
    /// </summary>
    internal interface IArticleRetentionAuthority
    {
        /// <summary>
        /// Attempts to retain one successful article payload and transfer ownership into shared retention.
        /// </summary>
        public ArticleRetentionAdmissionResult TryRetainSuccessArticle(string messageId, DownloadedArticleBuffer payloadOwner, DateTimeOffset? insertedUtc = null);

        /// <summary>
        /// Attempts to acquire a read lease by exact Message-ID.
        /// </summary>
        public ArticleRetentionReadLeaseResult TryAcquireReadLeaseByMessageId(string messageId);

        /// <summary>
        /// Attempts to acquire a read lease by canonical Message-ID MD5.
        /// </summary>
        public ArticleRetentionReadLeaseResult TryAcquireReadLeaseByMessageIdMd5(string messageIdMd5);

        /// <summary>
        /// Marks transit consumer completion for one retained Message-ID.
        /// </summary>
        public ArticleRetentionCompletionResult MarkTransitCompleted(string messageId);

        /// <summary>
        /// Marks listener consumer completion for one retained Message-ID.
        /// </summary>
        public ArticleRetentionCompletionResult MarkListenerCompleted(string messageId);

        /// <summary>
        /// Runs TTL expiration over eligible entries at the supplied UTC timestamp.
        /// </summary>
        public long ExpireEligibleArticles(DateTimeOffset nowUtc);

        /// <summary>
        /// Closes admission for shutdown while allowing existing entries to drain naturally.
        /// </summary>
        public void BeginShutdown();

        /// <summary>
        /// Gets current retention counters and retained-set characteristics.
        /// </summary>
        public ArticleRetentionSnapshot GetSnapshot(DateTimeOffset? nowUtc = null);

        /// <summary>
        /// Gets the configured periodic sweep interval.
        /// </summary>
        public TimeSpan SweepInterval { get; }
    }
}
