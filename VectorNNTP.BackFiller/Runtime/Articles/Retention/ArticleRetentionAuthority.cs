// <copyright file="ArticleRetentionAuthority.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Retention
// Owns shared in-memory retained article lifecycle, including admission, completion, expiration,
// pressure eviction, safe read leases, and exact-once pooled-buffer disposal.

using System.Collections.Concurrent;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;

namespace VectorNNTP.Backfiller.Runtime.Articles.Retention
{
    /// <summary>
    /// Shared in-memory authority for retained article payload ownership and lease-safe read access.
    /// </summary>
    internal sealed class ArticleRetentionAuthority : IArticleRetentionAuthority, IAsyncDisposable
    {
        private readonly object _gate = new();
        private readonly ConcurrentDictionary<string, RetainedArticleEntry> _entriesByMessageId = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, string> _messageIdByMd5 = new(StringComparer.Ordinal);
        private readonly LinkedList<RetainedArticleEntry> _insertionOrder = [];
        private readonly long _maximumRetainedPayloadBytes;
        private readonly TimeSpan _retentionTtl;

        private bool _admissionClosed;
        private long _retainedPayloadBytes;
        private long _retainedArticleCount;
        private long _activeReaderCount;
        private long _logicallyRemovedAwaitingReaderReleaseCount;
        private long _admissionAttempts;
        private long _admissionSuccesses;
        private long _admissionClosedFailures;
        private long _capacityFailures;
        private long _payloadTooLargeFailures;
        private long _invalidPayloadFailures;
        private long _md5CollisionFailures;
        private long _duplicateAdmissionAttempts;
        private long _pressureEvictionCount;
        private long _pressureEvictedBytes;
        private long _ttlExpirationCount;
        private long _ttlExpiredBytes;
        private long _transitCompletionCount;
        private long _listenerCompletionCount;
        private long _totalBytesReleased;

        public ArticleRetentionAuthority(BackFillerRuntimeOptions runtimeOptions)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArticleRetentionRuntimeOptions retentionOptions = runtimeOptions.EffectiveArticleRetention;
            if (retentionOptions.MaximumRetainedPayloadBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(runtimeOptions), "Article retention maximum payload bytes must be greater than zero.");
            }

            if (retentionOptions.RetentionTtl <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(runtimeOptions), "Article retention TTL must be greater than zero.");
            }

            if (retentionOptions.SweepInterval <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(runtimeOptions), "Article retention sweep interval must be greater than zero.");
            }

            _maximumRetainedPayloadBytes = retentionOptions.MaximumRetainedPayloadBytes;
            _retentionTtl = retentionOptions.RetentionTtl;
            SweepInterval = retentionOptions.SweepInterval;
        }

        /// <inheritdoc/>
        public TimeSpan SweepInterval { get; }

        /// <inheritdoc/>
        public ArticleRetentionAdmissionResult TryRetainSuccessArticle(string messageId, DownloadedArticleBuffer payloadOwner, DateTimeOffset? insertedUtc = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
            ArgumentNullException.ThrowIfNull(payloadOwner);

            _ = Interlocked.Increment(ref _admissionAttempts);
            DateTimeOffset now = insertedUtc ?? DateTimeOffset.UtcNow;
            int payloadBytes = payloadOwner.Length;

            if (payloadBytes <= 0)
            {
                _ = Interlocked.Increment(ref _invalidPayloadFailures);
                return new ArticleRetentionAdmissionResult(
                    Status: ArticleRetentionAdmissionStatus.InvalidPayload,
                    MessageId: messageId,
                    MessageIdMd5: null,
                    RetainedPayloadBytes: Interlocked.Read(ref _retainedPayloadBytes),
                    ReleasedPayloadBytes: 0,
                    ExistingMessageId: null,
                    ExistingMessageIdMd5: null);
            }

            if (payloadBytes > _maximumRetainedPayloadBytes)
            {
                _ = Interlocked.Increment(ref _payloadTooLargeFailures);
                return new ArticleRetentionAdmissionResult(
                    Status: ArticleRetentionAdmissionStatus.PayloadExceedsCapacity,
                    MessageId: messageId,
                    MessageIdMd5: null,
                    RetainedPayloadBytes: Interlocked.Read(ref _retainedPayloadBytes),
                    ReleasedPayloadBytes: 0,
                    ExistingMessageId: null,
                    ExistingMessageIdMd5: null);
            }

            string messageIdMd5 = MessageIdHashing.ComputeCanonicalMd5Hex(messageId);

            lock (_gate)
            {
                if (_admissionClosed)
                {
                    _admissionClosedFailures++;
                    return new ArticleRetentionAdmissionResult(
                        Status: ArticleRetentionAdmissionStatus.AdmissionClosed,
                        MessageId: messageId,
                        MessageIdMd5: messageIdMd5,
                        RetainedPayloadBytes: _retainedPayloadBytes,
                        ReleasedPayloadBytes: 0,
                        ExistingMessageId: null,
                        ExistingMessageIdMd5: null);
                }

                if (_entriesByMessageId.ContainsKey(messageId))
                {
                    _duplicateAdmissionAttempts++;
                    return new ArticleRetentionAdmissionResult(
                        Status: ArticleRetentionAdmissionStatus.DuplicateMessageId,
                        MessageId: messageId,
                        MessageIdMd5: messageIdMd5,
                        RetainedPayloadBytes: _retainedPayloadBytes,
                        ReleasedPayloadBytes: 0,
                        ExistingMessageId: messageId,
                        ExistingMessageIdMd5: messageIdMd5);
                }

                if (_messageIdByMd5.TryGetValue(messageIdMd5, out string? existingMessageId)
                    && !string.Equals(existingMessageId, messageId, StringComparison.Ordinal))
                {
                    _md5CollisionFailures++;
                    return new ArticleRetentionAdmissionResult(
                        Status: ArticleRetentionAdmissionStatus.Md5Collision,
                        MessageId: messageId,
                        MessageIdMd5: messageIdMd5,
                        RetainedPayloadBytes: _retainedPayloadBytes,
                        ReleasedPayloadBytes: 0,
                        ExistingMessageId: existingMessageId,
                        ExistingMessageIdMd5: messageIdMd5);
                }

                long releasedByEviction = RecoverCapacityForAdmissionLocked(payloadBytes);
                if (_retainedPayloadBytes + payloadBytes > _maximumRetainedPayloadBytes)
                {
                    _capacityFailures++;
                    return new ArticleRetentionAdmissionResult(
                        Status: ArticleRetentionAdmissionStatus.CapacityUnavailable,
                        MessageId: messageId,
                        MessageIdMd5: messageIdMd5,
                        RetainedPayloadBytes: _retainedPayloadBytes,
                        ReleasedPayloadBytes: releasedByEviction,
                        ExistingMessageId: null,
                        ExistingMessageIdMd5: null);
                }

                RetainedArticleEntry entry = new(messageId, messageIdMd5, payloadOwner, payloadBytes, now);
                LinkedListNode<RetainedArticleEntry> node = _insertionOrder.AddLast(entry);
                entry.InsertionNode = node;

                if (!_entriesByMessageId.TryAdd(messageId, entry))
                {
                    _insertionOrder.Remove(node);
                    _duplicateAdmissionAttempts++;
                    return new ArticleRetentionAdmissionResult(
                        Status: ArticleRetentionAdmissionStatus.DuplicateMessageId,
                        MessageId: messageId,
                        MessageIdMd5: messageIdMd5,
                        RetainedPayloadBytes: _retainedPayloadBytes,
                        ReleasedPayloadBytes: releasedByEviction,
                        ExistingMessageId: messageId,
                        ExistingMessageIdMd5: messageIdMd5);
                }

                if (!_messageIdByMd5.TryAdd(messageIdMd5, messageId))
                {
                    _ = _entriesByMessageId.TryRemove(messageId, out _);
                    _insertionOrder.Remove(node);
                    if (_messageIdByMd5.TryGetValue(messageIdMd5, out string? collidingMessageId)
                        && !string.Equals(collidingMessageId, messageId, StringComparison.Ordinal))
                    {
                        _md5CollisionFailures++;
                        return new ArticleRetentionAdmissionResult(
                            Status: ArticleRetentionAdmissionStatus.Md5Collision,
                            MessageId: messageId,
                            MessageIdMd5: messageIdMd5,
                            RetainedPayloadBytes: _retainedPayloadBytes,
                            ReleasedPayloadBytes: releasedByEviction,
                            ExistingMessageId: collidingMessageId,
                            ExistingMessageIdMd5: messageIdMd5);
                    }

                    _duplicateAdmissionAttempts++;
                    return new ArticleRetentionAdmissionResult(
                        Status: ArticleRetentionAdmissionStatus.DuplicateMessageId,
                        MessageId: messageId,
                        MessageIdMd5: messageIdMd5,
                        RetainedPayloadBytes: _retainedPayloadBytes,
                        ReleasedPayloadBytes: releasedByEviction,
                        ExistingMessageId: messageId,
                        ExistingMessageIdMd5: messageIdMd5);
                }

                _retainedPayloadBytes += payloadBytes;
                _retainedArticleCount++;
                _admissionSuccesses++;

                return new ArticleRetentionAdmissionResult(
                    Status: ArticleRetentionAdmissionStatus.Admitted,
                    MessageId: messageId,
                    MessageIdMd5: messageIdMd5,
                    RetainedPayloadBytes: _retainedPayloadBytes,
                    ReleasedPayloadBytes: releasedByEviction,
                    ExistingMessageId: null,
                    ExistingMessageIdMd5: null);
            }
        }

        /// <inheritdoc/>
        public ArticleRetentionReadLeaseResult TryAcquireReadLeaseByMessageId(string messageId)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

            return _entriesByMessageId.TryGetValue(messageId, out RetainedArticleEntry? entry)
                ? TryAcquireLease(entry)
                : ArticleRetentionReadLeaseResult.NotFound();
        }

        /// <inheritdoc/>
        public ArticleRetentionReadLeaseResult TryAcquireReadLeaseByMessageIdMd5(string messageIdMd5)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messageIdMd5);

            return _messageIdByMd5.TryGetValue(messageIdMd5, out string? messageId)
                && !string.IsNullOrWhiteSpace(messageId)
                && _entriesByMessageId.TryGetValue(messageId, out RetainedArticleEntry? entry)
                ? TryAcquireLease(entry)
                : ArticleRetentionReadLeaseResult.NotFound();
        }

        /// <inheritdoc/>
        public ArticleRetentionCompletionResult MarkTransitCompleted(string messageId)
        {
            return MarkConsumerCompleted(messageId, ArticleRetentionConsumer.Transit);
        }

        /// <inheritdoc/>
        public ArticleRetentionCompletionResult MarkListenerCompleted(string messageId)
        {
            return MarkConsumerCompleted(messageId, ArticleRetentionConsumer.Listener);
        }

        /// <inheritdoc/>
        public long ExpireEligibleArticles(DateTimeOffset nowUtc)
        {
            long releasedBytes = 0;
            lock (_gate)
            {
                while (_insertionOrder.First is LinkedListNode<RetainedArticleEntry> node)
                {
                    RetainedArticleEntry entry = node.Value;
                    if (entry.InsertionUtc + _retentionTtl > nowUtc)
                    {
                        break;
                    }

                    if (!TryMarkLogicallyRemovedLocked(entry, ArticleRetentionRemovalReason.TtlExpired))
                    {
                        _insertionOrder.Remove(node);
                        continue;
                    }

                    _ttlExpirationCount++;
                    _ttlExpiredBytes += entry.PayloadBytes;
                    if (TryDisposePhysicallyLocked(entry, updateReleasedCounters: true))
                    {
                        releasedBytes += entry.PayloadBytes;
                    }
                }
            }

            return releasedBytes;
        }

        /// <inheritdoc/>
        public void BeginShutdown()
        {
            lock (_gate)
            {
                _admissionClosed = true;
            }
        }

        /// <inheritdoc/>
        public ArticleRetentionSnapshot GetSnapshot(DateTimeOffset? nowUtc = null)
        {
            DateTimeOffset now = nowUtc ?? DateTimeOffset.UtcNow;
            lock (_gate)
            {
                TimeSpan oldestAge = TimeSpan.Zero;
                if (_insertionOrder.First is LinkedListNode<RetainedArticleEntry> oldestNode)
                {
                    oldestAge = now - oldestNode.Value.InsertionUtc;
                    if (oldestAge < TimeSpan.Zero)
                    {
                        oldestAge = TimeSpan.Zero;
                    }
                }

                return new ArticleRetentionSnapshot(
                    RetainedArticleCount: _retainedArticleCount,
                    RetainedPayloadBytes: _retainedPayloadBytes,
                    OldestRetainedArticleAge: oldestAge,
                    AdmissionAttempts: _admissionAttempts,
                    AdmissionSuccesses: _admissionSuccesses,
                    AdmissionClosedFailures: _admissionClosedFailures,
                    CapacityFailures: _capacityFailures,
                    PayloadTooLargeFailures: _payloadTooLargeFailures,
                    InvalidPayloadFailures: _invalidPayloadFailures,
                    Md5CollisionFailures: _md5CollisionFailures,
                    DuplicateAdmissionAttempts: _duplicateAdmissionAttempts,
                    PressureEvictionCount: _pressureEvictionCount,
                    PressureEvictedBytes: _pressureEvictedBytes,
                    TtlExpirationCount: _ttlExpirationCount,
                    TtlExpiredBytes: _ttlExpiredBytes,
                    ActiveReaderCount: _activeReaderCount,
                    LogicallyRemovedAwaitingReaderReleaseCount: _logicallyRemovedAwaitingReaderReleaseCount,
                    TransitCompletionCount: _transitCompletionCount,
                    ListenerCompletionCount: _listenerCompletionCount,
                    TotalBytesReleased: _totalBytesReleased);
            }
        }

        public ValueTask DisposeAsync()
        {
            lock (_gate)
            {
                foreach (RetainedArticleEntry entry in _entriesByMessageId.Values)
                {
                    _ = TryMarkLogicallyRemovedLocked(entry, ArticleRetentionRemovalReason.PressureEvicted);
                    _ = TryDisposePhysicallyLocked(entry, updateReleasedCounters: true);
                }

                _entriesByMessageId.Clear();
                _messageIdByMd5.Clear();
                _insertionOrder.Clear();
            }

            return ValueTask.CompletedTask;
        }

        private ArticleRetentionReadLeaseResult TryAcquireLease(RetainedArticleEntry entry)
        {
            if (!entry.TryAcquireLease())
            {
                return ArticleRetentionReadLeaseResult.Unavailable(entry.MessageId, entry.MessageIdMd5);
            }

            _ = Interlocked.Increment(ref _activeReaderCount);
            return ArticleRetentionReadLeaseResult.Acquired(new ArticleRetentionReadLease(this, entry));
        }

        private ArticleRetentionCompletionResult MarkConsumerCompleted(string messageId, ArticleRetentionConsumer consumer)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

            if (!_entriesByMessageId.TryGetValue(messageId, out RetainedArticleEntry? entry))
            {
                return new ArticleRetentionCompletionResult(
                    Status: ArticleRetentionCompletionStatus.NotFound,
                    MessageId: messageId,
                    EntryRemoved: false,
                    AlreadyCompleted: false,
                    RemovalReason: null);
            }

            bool completedNow = consumer == ArticleRetentionConsumer.Transit
                ? entry.TryMarkTransitCompleted()
                : entry.TryMarkListenerCompleted();

            _ = completedNow
                ? consumer == ArticleRetentionConsumer.Transit
                    ? Interlocked.Increment(ref _transitCompletionCount)
                    : Interlocked.Increment(ref _listenerCompletionCount)
                : 0;

            bool removedNow = false;
            ArticleRetentionRemovalReason? removalReason = null;
            if (entry.AreBothConsumersCompleted)
            {
                lock (_gate)
                {
                    if (TryMarkLogicallyRemovedLocked(entry, ArticleRetentionRemovalReason.BothConsumersCompleted))
                    {
                        removedNow = true;
                        removalReason = ArticleRetentionRemovalReason.BothConsumersCompleted;
                        _ = TryDisposePhysicallyLocked(entry, updateReleasedCounters: true);
                    }
                }
            }

            return new ArticleRetentionCompletionResult(
                Status: ArticleRetentionCompletionStatus.Completed,
                MessageId: messageId,
                EntryRemoved: removedNow,
                AlreadyCompleted: !completedNow,
                RemovalReason: removalReason);
        }

        private long RecoverCapacityForAdmissionLocked(int requiredPayloadBytes)
        {
            long releasedByEviction = 0;

            while (_retainedPayloadBytes + requiredPayloadBytes > _maximumRetainedPayloadBytes)
            {
                LinkedListNode<RetainedArticleEntry>? node = _insertionOrder.First;
                if (node is null)
                {
                    break;
                }

                RetainedArticleEntry candidate = node.Value;
                if (!candidate.IsLogicallyRemoved)
                {
                    _pressureEvictionCount++;
                    _pressureEvictedBytes += candidate.PayloadBytes;
                    _ = TryMarkLogicallyRemovedLocked(candidate, ArticleRetentionRemovalReason.PressureEvicted);
                }

                if (TryDisposePhysicallyLocked(candidate, updateReleasedCounters: true))
                {
                    releasedByEviction += candidate.PayloadBytes;
                }

                continue;
            }

            return releasedByEviction;
        }

        private bool TryMarkLogicallyRemovedLocked(RetainedArticleEntry entry, ArticleRetentionRemovalReason reason)
        {
            if (!entry.TryMarkLogicallyRemoved(reason))
            {
                return false;
            }

            _ = _entriesByMessageId.TryRemove(entry.MessageId, out _);
            _ = _messageIdByMd5.TryRemove(entry.MessageIdMd5, out _);
            if (entry.InsertionNode is not null)
            {
                _insertionOrder.Remove(entry.InsertionNode);
                entry.InsertionNode = null;
            }

            _logicallyRemovedAwaitingReaderReleaseCount++;
            return true;
        }

        private bool TryDisposePhysicallyLocked(RetainedArticleEntry entry, bool updateReleasedCounters)
        {
            if (!entry.TryDisposePhysically())
            {
                return false;
            }

            _retainedArticleCount--;
            _retainedPayloadBytes -= entry.PayloadBytes;
            if (updateReleasedCounters)
            {
                _totalBytesReleased += entry.PayloadBytes;
            }

            if (entry.WasLogicallyRemoved)
            {
                _logicallyRemovedAwaitingReaderReleaseCount = Math.Max(0, _logicallyRemovedAwaitingReaderReleaseCount - 1);
            }

            return true;
        }

        private void ReleaseLease(RetainedArticleEntry entry)
        {
            if (!entry.TryReleaseLease())
            {
                return;
            }

            _ = Interlocked.Decrement(ref _activeReaderCount);
            lock (_gate)
            {
                if (entry.IsLogicallyRemoved)
                {
                    _ = TryDisposePhysicallyLocked(entry, updateReleasedCounters: true);
                }
            }
        }

        private sealed class RetainedArticleEntry(string messageId, string messageIdMd5, DownloadedArticleBuffer payloadOwner, int payloadBytes, DateTimeOffset insertionUtc)
        {
            private readonly object _entryGate = new();
            private int _activeReaders;
            private int _transitCompleted;
            private int _listenerCompleted;
            private int _logicallyRemoved;
            private int _physicallyDisposed;

            public string MessageId { get; } = messageId;

            public string MessageIdMd5 { get; } = messageIdMd5;

            public int PayloadBytes { get; } = payloadBytes;

            public DateTimeOffset InsertionUtc { get; } = insertionUtc;

            public DownloadedArticleBuffer? PayloadOwner { get; private set; } = payloadOwner;

            public LinkedListNode<RetainedArticleEntry>? InsertionNode { get; set; }

            public bool WasLogicallyRemoved => Volatile.Read(ref _logicallyRemoved) == 1;

            public bool IsLogicallyRemoved => Volatile.Read(ref _logicallyRemoved) == 1;

            public bool AreBothConsumersCompleted => Volatile.Read(ref _transitCompleted) == 1 && Volatile.Read(ref _listenerCompleted) == 1;

            public bool TryAcquireLease()
            {
                lock (_entryGate)
                {
                    if (_logicallyRemoved == 1 || _physicallyDisposed == 1 || PayloadOwner is null)
                    {
                        return false;
                    }

                    _activeReaders++;
                    return true;
                }
            }

            public bool TryReleaseLease()
            {
                lock (_entryGate)
                {
                    if (_activeReaders <= 0)
                    {
                        return false;
                    }

                    _activeReaders--;
                    return true;
                }
            }

            public bool TryMarkTransitCompleted()
            {
                return Interlocked.CompareExchange(ref _transitCompleted, 1, 0) == 0;
            }

            public bool TryMarkListenerCompleted()
            {
                return Interlocked.CompareExchange(ref _listenerCompleted, 1, 0) == 0;
            }

            public bool TryMarkLogicallyRemoved(ArticleRetentionRemovalReason reason)
            {
                _ = reason;
                return Interlocked.CompareExchange(ref _logicallyRemoved, 1, 0) == 0;
            }

            public bool TryDisposePhysically()
            {
                lock (_entryGate)
                {
                    if (_physicallyDisposed == 1 || _activeReaders > 0)
                    {
                        return false;
                    }

                    if (Interlocked.CompareExchange(ref _physicallyDisposed, 1, 0) != 0)
                    {
                        return false;
                    }

                    DownloadedArticleBuffer? payloadOwner = PayloadOwner;
                    PayloadOwner = null;
                    payloadOwner?.Dispose();
                    return true;
                }
            }

            public ReadOnlyMemory<byte> GetPayload()
            {
                DownloadedArticleBuffer payloadOwner = PayloadOwner ?? throw new ObjectDisposedException(nameof(DownloadedArticleBuffer));
                return payloadOwner.Memory;
            }
        }

        private sealed class ArticleRetentionReadLease(ArticleRetentionAuthority owner, RetainedArticleEntry entry) : IArticleRetentionReadLease
        {
            private readonly ArticleRetentionAuthority _owner = owner;
            private readonly RetainedArticleEntry _entry = entry;
            private int _released;

            public string MessageId => _entry.MessageId;

            public string MessageIdMd5 => _entry.MessageIdMd5;

            public int PayloadBytes => _entry.PayloadBytes;

            public ReadOnlyMemory<byte> Payload => _entry.GetPayload();

            public void Dispose()
            {
                if (Interlocked.CompareExchange(ref _released, 1, 0) != 0)
                {
                    return;
                }

                _owner.ReleaseLease(_entry);
            }
        }
    }
}
