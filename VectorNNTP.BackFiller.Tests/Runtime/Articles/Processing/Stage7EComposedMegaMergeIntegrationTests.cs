// <copyright file="Stage7EComposedMegaMergeIntegrationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Processing
// Minimal composed Mega Merge integration tests that connect Phase4 result sink, retention authority,
// transit admission, and listener protocol session contracts with deterministic synchronization.

using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.Listener;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Tests.Runtime.Articles.Retention;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Processing
{
    /// <summary>
    /// Provides minimal composed integration confidence for Stage 7E Mega Merge contracts.
    /// </summary>
    public sealed class Stage7EComposedMegaMergeIntegrationTests
    {
        [Fact]
        public async Task ComposedSuccessFlow_RetainsThenAdmitsTransitThenPublishesUri_ListenerAckMarksCompletionAndFinalDisposal()
        {
            const string messageId = "<12345@example.invalid>";
            const string expectedMd5 = "30edc94157aa16fe644a45a1f1ffe160";
            const string payloadText = "stage7e-success-payload";

            List<string> operationLog = [];
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions();
            await using ArticleRetentionAuthority retentionAuthority = new(runtimeOptions);
            TrackingTransitAdmissionGateway transitAdmissionGateway = new(TransitAdmissionStatus.Accepted, operationLog);
            TrackingResponsePublisher publisher = new([RabbitMqResponsePublishStatus.Confirmed], operationLog);
            TrackingDeliverySettlement settlement = new(operationLog);
            RabbitMqArticleResultSink sink = CreateSink(runtimeOptions, retentionAuthority, transitAdmissionGateway, publisher);

            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.Parse("7c1cb8a0-95f9-4c13-8e53-339773e3afaa"), messageId, "BackboneA"),
                backbone: "BackboneA",
                correlationId: "corr-stage7e-success",
                replyTo: "rpc.responses",
                deliveryTag: 7001,
                settlement: settlement);

            NntpArticleGrabberResult grabberResult = ArticleRetentionTestDataFactory.CreateSuccessfulGrabberResult(messageId, payloadText);
            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                ArticleWorkProcessingOutcome.Success,
                Guid.Parse("7c1cb8a0-95f9-4c13-8e53-339773e3afaa"),
                messageId,
                "BackboneA",
                grabberResult);

            await sink.OnProcessedAsync(result, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(["admit", "publish", "confirm", "ack"], operationLog);
            Assert.Equal(1, transitAdmissionGateway.AdmitCallCount);
            Assert.Equal(messageId, transitAdmissionGateway.LastMessageId);
            Assert.Equal(7001UL, settlement.AckDeliveryTag);
            Assert.Null(settlement.NackDeliveryTag);

            RabbitMqArticleWorkResponse response = RabbitMqArticleWorkResponseWireProtocol.ParseV1(publisher.PublishedPayloads.Single());
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.Success), response.Outcome);
            string expectedUri = $"cache://{runtimeOptions.CanonicalBackFillerFqdn}:{runtimeOptions.BindPort}/{expectedMd5}";
            Assert.Equal(expectedUri, response.Uri);

            string uri = Assert.IsType<string>(response.Uri);
            string[] uriParts = uri.Split('/');
            string md5FromUri = uriParts[^1];
            Assert.Equal(expectedMd5, md5FromUri);
            Assert.Equal(expectedMd5, MessageIdHashing.ComputeCanonicalMd5Hex(messageId));

            ArticleRetentionSnapshot afterSink = retentionAuthority.GetSnapshot();
            Assert.Equal(1, afterSink.RetainedArticleCount);
            Assert.Equal(0, afterSink.ActiveReaderCount);
            Assert.Equal(0, afterSink.ListenerCompletionCount);
            Assert.Equal(0, afterSink.TransitCompletionCount);

            ListenerProtocolRetentionRequestHandler requestHandler = new(retentionAuthority);
            byte[] request = ListenerProtocolEncoder.EncodeGetRequest(9001, md5FromUri);
            byte[] ack = ListenerProtocolEncoder.EncodeGetReceiptAck(9001);
            await using GatedAckReadTransport transport = new(request, ack, maxWriteChunkLength: int.MaxValue);
            ListenerProtocolSession session = new(
                transport,
                requestHandler,
                CreateListenerOptions(),
                onAwaitingReceiptAck: null,
                onTerminalized: requestHandler.OnRequestTerminalized,
                onFoundTransferTerminal: requestHandler.OnFoundTransferTerminal,
                onReceiptAcknowledged: requestHandler.OnReceiptAcknowledged);

            Task runTask = session.RunAsync(CancellationToken.None);

            int expectedFoundBytes = checked((int)(ListenerProtocol.HeaderLengthBytes + payloadText.Length));
            await transport.WaitForWritesAtLeastAsync(expectedFoundBytes).ConfigureAwait(false);

            ArticleRetentionSnapshot afterFoundBeforeAck = retentionAuthority.GetSnapshot();
            Assert.Equal(1, afterFoundBeforeAck.ActiveReaderCount);
            Assert.Equal(0, afterFoundBeforeAck.ListenerCompletionCount);

            transport.ReleaseAcknowledgementRead();
            await runTask.ConfigureAwait(false);

            byte[] outbound = transport.GetWrittenBytes();
            ListenerFrameParseResult parsed = ListenerProtocolParser.ParseOneFrame(outbound);
            Assert.Equal(ListenerFrameParseStatus.Success, parsed.Status);
            Assert.Equal(ListenerOpcode.GetResponseFound, parsed.Frame!.Value.Header.Opcode);
            Assert.Equal<uint>(9001, parsed.Frame.Value.Header.RequestId);
            byte[] foundPayload = parsed.Frame.Value.Payload.ToArray();
            Assert.Equal(Encoding.ASCII.GetBytes(payloadText), foundPayload);

            ArticleRetentionSnapshot afterListenerAck = retentionAuthority.GetSnapshot();
            Assert.Equal(0, afterListenerAck.ActiveReaderCount);
            Assert.Equal(1, afterListenerAck.ListenerCompletionCount);
            Assert.Equal(1, afterListenerAck.RetainedArticleCount);
            Assert.True(afterListenerAck.RetainedPayloadBytes > 0);

            ArticleRetentionCompletionResult transitCompletion = retentionAuthority.MarkTransitCompleted(messageId);
            Assert.Equal(ArticleRetentionCompletionStatus.Completed, transitCompletion.Status);

            ArticleRetentionSnapshot finalSnapshot = retentionAuthority.GetSnapshot();
            Assert.Equal(1, finalSnapshot.TransitCompletionCount);
            Assert.Equal(1, finalSnapshot.ListenerCompletionCount);
            Assert.Equal(0, finalSnapshot.ActiveReaderCount);
            Assert.Equal(0, finalSnapshot.LogicallyRemovedAwaitingReaderReleaseCount);
            Assert.Equal(0, finalSnapshot.RetainedArticleCount);
            Assert.Equal(0, finalSnapshot.RetainedPayloadBytes);
            Assert.True(finalSnapshot.TotalBytesReleased > 0);
        }

        [Fact]
        public async Task ComposedRedeliveryFlow_WhenFirstPublishConfirmFails_DuplicateRetentionUsesExistingPayloadAndSecondAttemptAcks()
        {
            const string messageId = "<stage7e-redelivery@example.com>";
            const string firstPayloadText = "first-authoritative-payload";
            const string secondPayloadText = "second-duplicate-payload";

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions();
            await using ArticleRetentionAuthority retentionAuthority = new(runtimeOptions);
            List<string> operationLog = [];
            TrackingTransitAdmissionGateway transitAdmissionGateway = new(TransitAdmissionStatus.Accepted, operationLog);
            TrackingResponsePublisher publisher = new([RabbitMqResponsePublishStatus.TimedOut, RabbitMqResponsePublishStatus.Confirmed], operationLog);
            RabbitMqArticleResultSink sink = CreateSink(runtimeOptions, retentionAuthority, transitAdmissionGateway, publisher);

            TrackingDeliverySettlement firstSettlement = new(operationLog);
            RabbitMqArticleDelivery firstDelivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), messageId, "BackboneA"),
                backbone: "BackboneA",
                correlationId: "corr-stage7e-redelivery-1",
                replyTo: "rpc.responses",
                deliveryTag: 7101,
                settlement: firstSettlement,
                redelivered: false);

            ArticleWorkProcessingResult firstResult = CreateResult(
                firstDelivery,
                ArticleWorkProcessingOutcome.Success,
                Guid.NewGuid(),
                messageId,
                "BackboneA",
                ArticleRetentionTestDataFactory.CreateSuccessfulGrabberResult(messageId, firstPayloadText));

            await sink.OnProcessedAsync(firstResult, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, transitAdmissionGateway.AdmitCallCount);
            Assert.Null(firstSettlement.AckDeliveryTag);
            Assert.Equal(7101UL, firstSettlement.NackDeliveryTag);
            Assert.True(firstSettlement.NackRequeue);

            ArticleRetentionReadLeaseResult retainedAfterFirst = retentionAuthority.TryAcquireReadLeaseByMessageId(messageId);
            Assert.True(retainedAfterFirst.IsAcquired);
            using (IArticleRetentionReadLease lease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(retainedAfterFirst.Lease))
            {
                Assert.Equal(firstPayloadText, Encoding.ASCII.GetString(lease.Payload.Span));
            }

            TrackingDeliverySettlement secondSettlement = new(operationLog);
            RabbitMqArticleDelivery secondDelivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), messageId, "BackboneA"),
                backbone: "BackboneA",
                correlationId: "corr-stage7e-redelivery-2",
                replyTo: "rpc.responses",
                deliveryTag: 7102,
                settlement: secondSettlement,
                redelivered: true);

            ArticleWorkProcessingResult secondResult = CreateResult(
                secondDelivery,
                ArticleWorkProcessingOutcome.Success,
                Guid.NewGuid(),
                messageId,
                "BackboneA",
                ArticleRetentionTestDataFactory.CreateSuccessfulGrabberResult(messageId, secondPayloadText));

            await sink.OnProcessedAsync(secondResult, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(["admit", "publish", "nack", "admit", "publish", "confirm", "ack"], operationLog);
            Assert.Equal(2, transitAdmissionGateway.AdmitCallCount);
            Assert.Equal(2, publisher.PublishCallCount);
            Assert.Equal(7102UL, secondSettlement.AckDeliveryTag);
            Assert.Null(secondSettlement.NackDeliveryTag);

            ArticleRetentionReadLeaseResult retainedAfterSecond = retentionAuthority.TryAcquireReadLeaseByMessageId(messageId);
            Assert.True(retainedAfterSecond.IsAcquired);
            using (IArticleRetentionReadLease lease = Assert.IsAssignableFrom<IArticleRetentionReadLease>(retainedAfterSecond.Lease))
            {
                Assert.Equal(firstPayloadText, Encoding.ASCII.GetString(lease.Payload.Span));
            }

            ArticleRetentionSnapshot snapshot = retentionAuthority.GetSnapshot();
            Assert.Equal(1, snapshot.RetainedArticleCount);
            Assert.Equal(1, snapshot.DuplicateAdmissionAttempts);
            Assert.Equal(0, snapshot.ActiveReaderCount);
            Assert.Equal(0, snapshot.LogicallyRemovedAwaitingReaderReleaseCount);
            Assert.Equal(0, snapshot.AdmissionClosedFailures);
            Assert.Equal(0, snapshot.TransitCompletionCount);
            Assert.Equal(0, snapshot.ListenerCompletionCount);
        }

        private static RabbitMqArticleResultSink CreateSink(
            BackFillerRuntimeOptions runtimeOptions,
            IArticleRetentionAuthority retentionAuthority,
            ITransitAdmissionGateway transitAdmissionGateway,
            IRabbitMqArticleResponsePublisher responsePublisher)
        {
            return new RabbitMqArticleResultSink(
                planner: new ArticleWorkDispositionPlanner(),
                responseFactory: new ArticleWorkResponseFactory(runtimeOptions),
                responsePublisher: responsePublisher,
                retentionAuthority: retentionAuthority,
                transitAdmissionGateway: transitAdmissionGateway,
                logger: NullLogger<RabbitMqArticleResultSink>.Instance);
        }

        private static BackFillerRuntimeOptions CreateRuntimeOptions()
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
                    MaximumRetainedPayloadBytes: 16 * 1024 * 1024,
                    RetentionTtl: TimeSpan.FromSeconds(60),
                    SweepInterval: TimeSpan.FromSeconds(1)));
        }

        private static ListenerRuntimeOptions CreateListenerOptions()
        {
            return new ListenerRuntimeOptions(
                ParserAccumulationMaxBytes: 262144,
                AwaitingReceiptAckTimeout: TimeSpan.FromSeconds(30),
                MaxQueuedFoundPayloadBytes: 67108864,
                MaxActiveConnections: 1024);
        }

        private static ArticleWorkProcessingResult CreateResult(
            RabbitMqArticleDelivery delivery,
            ArticleWorkProcessingOutcome outcome,
            Guid requestId,
            string messageId,
            string backbone,
            NntpArticleGrabberResult grabberResult)
        {
            RabbitMqArticleWorkRequest request = new(1, requestId, messageId, backbone);
            return new ArticleWorkProcessingResult(
                Request: request,
                Delivery: delivery,
                Outcome: outcome,
                Disposition: ArticleWorkDispositionRecommendation.None,
                GrabberResult: grabberResult,
                ProviderFailureCode: null,
                ResponseCode: null,
                ResponseText: null,
                UnexpectedException: null);
        }

        private static RabbitMqArticleDelivery CreateDelivery(
            string payloadText,
            string backbone,
            string correlationId,
            string replyTo,
            ulong deliveryTag,
            IRabbitMqDeliverySettlement settlement,
            bool redelivered = false)
        {
            if (settlement is TrackingDeliverySettlement tracking)
            {
                tracking.BindDeliveryTag(deliveryTag);
            }

            return new RabbitMqArticleDelivery(
                Backbone: backbone,
                Queue: "grabbers.backbonea",
                ConsumerTag: "ctag-stage7e",
                ConsumerIdentity: "consumer-stage7e",
                DeliveryTag: deliveryTag,
                Redelivered: redelivered,
                RoutingKey: "grabbers.backbonea",
                Exchange: "grabbers.backbonea",
                ConnectionGeneration: 1,
                RabbitMqMessageId: "rmq-stage7e-message-id",
                CorrelationId: correlationId,
                ReplyTo: replyTo,
                Payload: Encoding.UTF8.GetBytes(payloadText),
                Settlement: settlement,
                CancellationToken: CancellationToken.None);
        }

        private static string CreateValidJsonPayload(Guid requestId, string messageId, string backbone)
        {
            return $"{{\"version\":1,\"requestId\":\"{requestId}\",\"messageId\":\"{messageId}\",\"backbone\":\"{backbone}\"}}";
        }

        private sealed class TrackingTransitAdmissionGateway(TransitAdmissionStatus status, List<string>? operationLog = null) : ITransitAdmissionGateway
        {
            private readonly TransitAdmissionStatus _status = status;
            private readonly List<string>? _operationLog = operationLog;

            internal int AdmitCallCount { get; private set; }

            internal string? LastMessageId { get; private set; }

            public ValueTask<TransitAdmissionResult> AdmitAsync(string messageId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                AdmitCallCount++;
                LastMessageId = messageId;
                _operationLog?.Add("admit");
                return ValueTask.FromResult(new TransitAdmissionResult(messageId, _status, null));
            }
        }

        private sealed class TrackingResponsePublisher(IReadOnlyList<RabbitMqResponsePublishStatus> statuses, List<string>? operationLog = null) : IRabbitMqArticleResponsePublisher
        {
            private readonly IReadOnlyList<RabbitMqResponsePublishStatus> _statuses = statuses;
            private readonly List<string>? _operationLog = operationLog;
            private int _statusIndex;

            internal int PublishCallCount { get; private set; }

            internal List<byte[]> PublishedPayloads { get; } = [];

            public ValueTask<RabbitMqResponsePublishResult> PublishAndConfirmAsync(
                ArticleWorkProcessingResult result,
                RabbitMqArticleWorkResponse response,
                CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                PublishCallCount++;
                _operationLog?.Add("publish");
                byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
                PublishedPayloads.Add(payload);

                RabbitMqResponsePublishStatus status = _statusIndex < _statuses.Count
                    ? _statuses[_statusIndex]
                    : _statuses[^1];
                _statusIndex++;

                if (status == RabbitMqResponsePublishStatus.Confirmed)
                {
                    _operationLog?.Add("confirm");
                }

                return ValueTask.FromResult(new RabbitMqResponsePublishResult(status, result.Delivery.ConnectionGeneration, null));
            }
        }

        private sealed class TrackingDeliverySettlement(List<string>? operationLog = null) : IRabbitMqDeliverySettlement
        {
            private readonly List<string>? _operationLog = operationLog;
            private int _settled;
            private ulong _deliveryTag;

            internal ulong? AckDeliveryTag { get; private set; }

            internal ulong? NackDeliveryTag { get; private set; }

            internal bool NackRequeue { get; private set; }

            public ValueTask AckAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (Interlocked.Exchange(ref _settled, 1) != 0)
                {
                    throw new InvalidOperationException("Delivery already settled.");
                }

                AckDeliveryTag = _deliveryTag;
                _operationLog?.Add("ack");
                return ValueTask.CompletedTask;
            }

            public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (Interlocked.Exchange(ref _settled, 1) != 0)
                {
                    throw new InvalidOperationException("Delivery already settled.");
                }

                NackDeliveryTag = _deliveryTag;
                NackRequeue = requeue;
                _operationLog?.Add("nack");
                return ValueTask.CompletedTask;
            }

            internal void BindDeliveryTag(ulong deliveryTag)
            {
                _deliveryTag = deliveryTag;
            }
        }

        private sealed class GatedAckReadTransport(byte[] request, byte[] ack, int maxWriteChunkLength) : IListenerProtocolSessionTransport
        {
            private readonly byte[] _request = request;
            private readonly byte[] _ack = ack;
            private readonly TaskCompletionSource<bool> _ackRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _writeThresholds = new();
            private readonly object _writeGate = new();
            private readonly List<byte> _writes = [];
            private readonly int _maxWriteChunkLength = maxWriteChunkLength;
            private int _readStep;
            private int _writtenBytes;
            private bool _disposed;

            public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(GatedAckReadTransport));
                }

                int step = Volatile.Read(ref _readStep);
                if (step == 0)
                {
                    if (_request.Length > buffer.Length)
                    {
                        throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                    }

                    _request.CopyTo(buffer);
                    _ = Interlocked.Exchange(ref _readStep, 1);
                    return _request.Length;
                }

                if (step == 1)
                {
                    await _ackRelease.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                    if (_ack.Length > buffer.Length)
                    {
                        throw new InvalidOperationException("Read fragment exceeds provided buffer size.");
                    }

                    _ack.CopyTo(buffer);
                    _ = Interlocked.Exchange(ref _readStep, 2);
                    return _ack.Length;
                }

                return 0;
            }

            public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(GatedAckReadTransport));
                }

                int chunkLength = Math.Min(buffer.Length, _maxWriteChunkLength);
                ReadOnlySpan<byte> span = buffer.Span[..chunkLength];
                lock (_writeGate)
                {
                    for (int i = 0; i < span.Length; i++)
                    {
                        _writes.Add(span[i]);
                    }
                }

                int written = Interlocked.Add(ref _writtenBytes, chunkLength);
                SignalWriteThresholds(written);
                return ValueTask.FromResult(chunkLength);
            }

            public ValueTask DisposeAsync()
            {
                _disposed = true;
                return ValueTask.CompletedTask;
            }

            internal void ReleaseAcknowledgementRead()
            {
                _ackRelease.TrySetResult(true);
            }

            internal byte[] GetWrittenBytes()
            {
                lock (_writeGate)
                {
                    return [.. _writes];
                }
            }

            internal Task WaitForWritesAtLeastAsync(int bytes)
            {
                if (Volatile.Read(ref _writtenBytes) >= bytes)
                {
                    return Task.CompletedTask;
                }

                TaskCompletionSource<bool> threshold = _writeThresholds.GetOrAdd(
                    bytes,
                    static _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));

                if (Volatile.Read(ref _writtenBytes) >= bytes)
                {
                    threshold.TrySetResult(true);
                }

                return threshold.Task;
            }

            private void SignalWriteThresholds(int currentWritten)
            {
                foreach (KeyValuePair<int, TaskCompletionSource<bool>> threshold in _writeThresholds)
                {
                    if (threshold.Key <= currentWritten)
                    {
                        threshold.Value.TrySetResult(true);
                    }
                }
            }
        }
    }
}
