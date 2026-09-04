// <copyright file="RabbitMqBackboneConsumerSession.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq backbone consumer session behavior.

using System.Security.Cryptography;
using System.Text;
using RabbitMQ.Client.Events;
using Serilog.Context;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Owns one logical RabbitMQ consumer session for one backbone queue.
    /// </summary>
    internal sealed partial class RabbitMqBackboneConsumerSession : IRabbitMqConsumerSession
    {
        /// <summary>
        /// Gets the authoritative logical identity for this session.
        /// </summary>
        private readonly RabbitMqConsumerSessionIdentity _identity;
        /// <summary>
        /// Canonical backbone queue name consumed by this session.
        /// </summary>
        private readonly string _queueName;
        /// <summary>
        /// Connection manager that supplies generation-aware owned channels.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager;
        /// <summary>
        /// Topology initializer that ensures the target queue and exchange exist before consumption begins.
        /// </summary>
        private readonly RabbitMqTopologyInitializer _topologyInitializer;
        /// <summary>
        /// Delivery sink that receives admitted deliveries for downstream processing.
        /// </summary>
        private readonly IRabbitMqDeliverySink _deliverySink;
        /// <summary>
        /// Logger that records consumer lifecycle, drain, recreation, and diagnostic payload events.
        /// </summary>
        private readonly ILogger<RabbitMqBackboneConsumerSession> _logger;
        /// <summary>
        /// Optional broker prefetch limit applied to this consumer channel.
        /// </summary>
        private readonly ushort? _prefetchCount;
        /// <summary>
        /// Optional correlation identifier that enables payload-diagnostic logging for matching deliveries only.
        /// </summary>
        private readonly string? _diagnosticCorrelationId;
        /// <summary>
        /// Gate that serializes start, stop, replacement, and admitted-delivery drain accounting.
        /// </summary>
        private readonly SemaphoreSlim _lifecycleGate = new(1, 1);

        /// <summary>
        /// Channel lease currently owned by the session.
        /// </summary>
        private RabbitMqOwnedChannel? _ownedChannel;
        /// <summary>
        /// Active asynchronous broker consumer bound to the owned channel.
        /// </summary>
        private AsyncEventingBasicConsumer? _consumer;
        /// <summary>
        /// Broker-assigned consumer tag for the current registration.
        /// </summary>
        private string? _consumerTag;
        /// <summary>
        /// Cancellation source propagated to admitted deliveries when the session retires.
        /// </summary>
        private CancellationTokenSource? _sessionCancellation;
        /// <summary>
        /// Completion source that signals when all admitted deliveries have been settled.
        /// </summary>
        private TaskCompletionSource<bool> _drainCompletion = CreateCompletedDrainSource();
        /// <summary>
        /// Connection generation associated with the currently owned channel and consumer.
        /// </summary>
        private long _activeConnectionGeneration;
        /// <summary>
        /// Count of deliveries admitted to the sink and not yet terminally settled.
        /// </summary>
        private int _admittedDeliveryCount;
        /// <summary>
        /// Internal running, retiring, or stopped lifecycle state.
        /// </summary>
        private RabbitMqConsumerLifecycleState _lifecycleState = RabbitMqConsumerLifecycleState.Stopped;
        /// <summary>
        /// Indicates whether cancel-admitted-work shutdown has abandoned new settlement admission for the current session lifecycle.
        /// </summary>
        private bool _settlementAdmissionAbandoned;
        /// <summary>
        /// Indicates that disposal has started and the session must reject further lifecycle work.
        /// </summary>
        private bool _disposed;
        /// <summary>
        /// Serilog diagnostic scope pushed while a live consumer is active.
        /// </summary>
        private IDisposable? _connectionScope;

        /// <summary>
        /// Internal consumer-session lifecycle states.
        /// </summary>
        private enum RabbitMqConsumerLifecycleState
        {
            Running,
            Retiring,
            Stopped,
        }

        /// <summary>
        /// Initializes one logical consumer session for a single backbone queue.
        /// </summary>
        internal RabbitMqBackboneConsumerSession(
            RabbitMqConsumerSessionIdentity identity,
            RabbitMqConnectionManager connectionManager,
            RabbitMqTopologyInitializer topologyInitializer,
            IRabbitMqDeliverySink deliverySink,
            ILogger<RabbitMqBackboneConsumerSession> logger,
            ushort? prefetchCount,
            string? diagnosticCorrelationId = null)
        {
            ArgumentNullException.ThrowIfNull(identity);
            ArgumentNullException.ThrowIfNull(connectionManager);
            ArgumentNullException.ThrowIfNull(topologyInitializer);
            ArgumentNullException.ThrowIfNull(deliverySink);
            ArgumentNullException.ThrowIfNull(logger);

            _identity = identity;
            _queueName = $"grabbers.{identity.Backbone.Trim().ToLowerInvariant()}";
            _connectionManager = connectionManager;
            _topologyInitializer = topologyInitializer;
            _deliverySink = deliverySink;
            _logger = logger;
            _prefetchCount = prefetchCount;
            _diagnosticCorrelationId = string.IsNullOrWhiteSpace(diagnosticCorrelationId)
                ? null
                : diagnosticCorrelationId.Trim();
        }

        /// <summary>
        /// Gets the authoritative logical identity for this session.
        /// </summary>
        internal RabbitMqConsumerSessionIdentity Identity => _identity;

        RabbitMqConsumerSessionIdentity IRabbitMqConsumerSession.Identity => _identity;

        /// <summary>
        /// Gets a value indicating whether the session is actively consuming deliveries.
        /// </summary>
        internal bool IsRunning => _lifecycleState is RabbitMqConsumerLifecycleState.Running;

        bool IRabbitMqConsumerSession.IsRunning => _lifecycleState is RabbitMqConsumerLifecycleState.Running;

        /// <summary>
        /// Gets the connection generation associated with the currently owned channel and consumer.
        /// </summary>
        /// <value>The active generation, or zero when the session is stopped.</value>
        internal long ActiveConnectionGeneration => Interlocked.Read(ref _activeConnectionGeneration);

        long IRabbitMqConsumerSession.ActiveConnectionGeneration => ActiveConnectionGeneration;

        /// <summary>
        /// Recreates the consumer when the connection manager installs a newer connection generation.
        /// </summary>
        /// <param name="args">Connection-generation replacement details.</param>
        /// <param name="cancellationToken">Cancellation token for the replacement work.</param>
        /// <returns>A task that completes after any required recreation finishes.</returns>
        internal async Task HandleConnectionReplacedAsync(RabbitMqConnectionReplacedEventArgs args, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(args);

            if (_disposed || !args.IsReplacement)
            {
                return;
            }

            await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_disposed || _lifecycleState is not RabbitMqConsumerLifecycleState.Running)
                {
                    return;
                }

                long currentGeneration = ActiveConnectionGeneration;
                if (args.ConnectionGeneration <= currentGeneration)
                {
                    return;
                }

                if (!IsActiveConsumerStaleForCurrentConnection())
                {
                    return;
                }

                await RecreateConsumerCoreAsync(args.ConnectionGeneration, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = _lifecycleGate.Release();
            }
        }

        /// <summary>
        /// Starts the session by ensuring connectivity, declaring topology, and registering a broker consumer.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for startup.</param>
        /// <returns>A task that completes after the broker consumer is active.</returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                ThrowIfDisposed();
                if (_lifecycleState is not RabbitMqConsumerLifecycleState.Stopped)
                {
                    return;
                }

                await StartCoreAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = _lifecycleGate.Release();
            }
        }

        /// <summary>
        /// Stops the session, optionally canceling admitted work before waiting for drain completion.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the stop sequence.</param>
        /// <param name="cancelAdmittedWork"><see langword="true"/> to cancel admitted deliveries before draining.</param>
        /// <returns>A task that completes after the consumer is canceled and admitted deliveries have drained.</returns>
        public async Task StopAsync(CancellationToken cancellationToken, bool cancelAdmittedWork)
        {
            await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await StopCoreAsync(cancellationToken, expectedShutdown: true, cancelAdmittedWork).ConfigureAwait(false);
            }
            finally
            {
                _ = _lifecycleGate.Release();
            }
        }

        /// <summary>
        /// Disposes the session by performing a final stop that cancels admitted work.
        /// </summary>
        /// <returns>A value task that completes after owned session resources are released.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                await StopCoreAsync(CancellationToken.None, expectedShutdown: true, cancelAdmittedWork: true).ConfigureAwait(false);
            }
            finally
            {
                _ = _lifecycleGate.Release();
                _lifecycleGate.Dispose();
            }
        }

        /// <summary>
        /// Core start path that acquires a generation-bound channel and registers the consumer callbacks.
        /// </summary>
        private async Task StartCoreAsync(CancellationToken cancellationToken)
        {
            if (_lifecycleState is not RabbitMqConsumerLifecycleState.Stopped || _ownedChannel is not null || _consumer is not null)
            {
                throw new InvalidOperationException("RabbitMQ consumer session cannot start while a consumer instance is still active.");
            }

            await _connectionManager.EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
            await _topologyInitializer.InitializeAsync(_identity.ServerId, [_identity.Backbone], cancellationToken).ConfigureAwait(false);

            RabbitMqOwnedChannel owned = await _connectionManager.CreateOwnedChannelAsync($"rabbitmq-consumer:{_identity.SessionKey}", cancellationToken).ConfigureAwait(false);
            _ownedChannel = owned;
            _ = Interlocked.Exchange(ref _activeConnectionGeneration, owned.ConnectionGeneration);
            _sessionCancellation = new CancellationTokenSource();

            if (_prefetchCount.HasValue)
            {
                await _ownedChannel.Channel.BasicQosAsync(0u, _prefetchCount.Value, false, cancellationToken).ConfigureAwait(false);
                LogConsumerPrefetchConfigured(_logger, _identity.Backbone, _identity.SessionOrdinal, _prefetchCount.Value);
            }

            AsyncEventingBasicConsumer consumer = new(owned.Channel.UnderlyingChannel);
            consumer.ReceivedAsync += OnReceivedAsync;
            consumer.ShutdownAsync += OnConsumerShutdownAsync;
            consumer.UnregisteredAsync += OnConsumerUnregisteredAsync;
            _consumer = consumer;
            _connectionScope = BeginConnectionScope();

            try
            {
                string consumerTag = await owned.Channel.BasicConsumeAsync(
                    queue: _queueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                _consumerTag = consumerTag;
                _lifecycleState = RabbitMqConsumerLifecycleState.Running;
                _settlementAdmissionAbandoned = false;
                _drainCompletion = CreateCompletedDrainSource();
                _admittedDeliveryCount = 0;
                LogConsumerStarted(_logger, _identity.Backbone, _identity.SessionOrdinal, _queueName, ActiveConnectionGeneration, consumerTag);
            }
            catch
            {
                consumer.ReceivedAsync -= OnReceivedAsync;
                consumer.ShutdownAsync -= OnConsumerShutdownAsync;
                consumer.UnregisteredAsync -= OnConsumerUnregisteredAsync;
                _consumer = null;

                _sessionCancellation?.Cancel();
                _sessionCancellation?.Dispose();
                _sessionCancellation = null;

                if (_ownedChannel is not null)
                {
                    await _ownedChannel.DisposeAsync().ConfigureAwait(false);
                    _ownedChannel = null;
                }

                _consumerTag = null;
                _lifecycleState = RabbitMqConsumerLifecycleState.Stopped;
                _settlementAdmissionAbandoned = false;
                _admittedDeliveryCount = 0;
                _drainCompletion = CreateCompletedDrainSource();
                _connectionScope?.Dispose();
                _connectionScope = null;
                _ = Interlocked.Exchange(ref _activeConnectionGeneration, 0);
                throw;
            }
        }

        /// <summary>
        /// Core stop path that retires the consumer, optionally cancels admitted work, and waits for drain completion.
        /// </summary>
        private async Task StopCoreAsync(CancellationToken cancellationToken, bool expectedShutdown, bool cancelAdmittedWork)
        {
            bool hasSessionResources = _lifecycleState is not RabbitMqConsumerLifecycleState.Stopped || _ownedChannel is not null || _consumer is not null || _sessionCancellation is not null || !string.IsNullOrWhiteSpace(_consumerTag);
            if (!hasSessionResources)
            {
                return;
            }

            if (_lifecycleState is RabbitMqConsumerLifecycleState.Running)
            {
                _lifecycleState = RabbitMqConsumerLifecycleState.Retiring;
                if (_admittedDeliveryCount > 0)
                {
                    _drainCompletion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                LogConsumerRetiring(_logger, _identity.Backbone, _identity.SessionOrdinal, _admittedDeliveryCount);
            }

            try
            {
                if (_ownedChannel is not null && !string.IsNullOrWhiteSpace(_consumerTag))
                {
                    await _ownedChannel.Channel.BasicCancelAsync(_consumerTag, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (expectedShutdown)
            {
                LogConsumerCancelDuringShutdownFailed(_logger, _identity.Backbone, _identity.SessionOrdinal, ex.Message);
            }
            catch (Exception ex)
            {
                LogConsumerCancellationFailed(_logger, _identity.Backbone, _identity.SessionOrdinal, ex);
                throw;
            }

            if (cancelAdmittedWork)
            {
                _sessionCancellation?.Cancel();
                _settlementAdmissionAbandoned = true;
                AbandonAdmittedDeliveriesForDrainAccountingNoLock();
            }

            Task drainTask = _drainCompletion.Task;
            if (!drainTask.IsCompleted)
            {
                LogConsumerDrainStarted(_logger, _identity.Backbone, _identity.SessionOrdinal, _admittedDeliveryCount);
            }

            _ = _lifecycleGate.Release();
            try
            {
                await drainTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            }

            LogConsumerDrainCompleted(_logger, _identity.Backbone, _identity.SessionOrdinal);

            if (_consumer is not null)
            {
                _consumer.ReceivedAsync -= OnReceivedAsync;
                _consumer.ShutdownAsync -= OnConsumerShutdownAsync;
                _consumer.UnregisteredAsync -= OnConsumerUnregisteredAsync;
                _consumer = null;
            }

            _sessionCancellation?.Dispose();
            _sessionCancellation = null;

            if (_ownedChannel is not null)
            {
                try
                {
                    await _ownedChannel.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex) when (expectedShutdown)
                {
                    LogConsumerChannelDisposeDuringShutdownFailed(_logger, _identity.Backbone, _identity.SessionOrdinal, ex.Message);
                }
                catch (Exception ex)
                {
                    LogConsumerChannelDisposeFailed(_logger, _identity.Backbone, _identity.SessionOrdinal, ex);
                    throw;
                }
            }

            _ownedChannel = null;
            _consumerTag = null;
            _ = Interlocked.Exchange(ref _activeConnectionGeneration, 0);
            _lifecycleState = RabbitMqConsumerLifecycleState.Stopped;
            _settlementAdmissionAbandoned = false;
            _admittedDeliveryCount = 0;
            _drainCompletion = CreateCompletedDrainSource();

            _connectionScope?.Dispose();
            _connectionScope = null;

            if (expectedShutdown)
            {
                LogConsumerStopped(_logger, _identity.Backbone, _identity.SessionOrdinal);
            }
        }

        /// <summary>
        /// Admits a broker delivery, copies its payload, and forwards a settlement-capable envelope to the sink.
        /// </summary>
        private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs args)
        {
            if (!IsEventFromActiveConsumer(sender))
            {
                return;
            }

            CancellationToken cancellationToken = _sessionCancellation?.Token ?? CancellationToken.None;
            long deliveryGeneration = ActiveConnectionGeneration;
            if (deliveryGeneration <= 0 || _connectionManager.ConnectionGeneration > deliveryGeneration)
            {
                LogDeliveryIgnoredFromStaleGeneration(_logger, _identity.Backbone, _identity.SessionOrdinal, deliveryGeneration, _connectionManager.ConnectionGeneration);
                return;
            }

            await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            RabbitMqAdmittedDeliveryTracker? tracker = null;
            try
            {
                if (_lifecycleState is not RabbitMqConsumerLifecycleState.Running || !IsEventFromActiveConsumer(sender))
                {
                    return;
                }

                _admittedDeliveryCount++;
                if (_admittedDeliveryCount == 1)
                {
                    _drainCompletion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                tracker = new RabbitMqAdmittedDeliveryTracker(this);
            }
            finally
            {
                _ = _lifecycleGate.Release();
            }

            string? correlationId = args.BasicProperties?.CorrelationId;
            string? rabbitMqMessageId = args.BasicProperties?.MessageId;
            string? replyTo = args.BasicProperties?.ReplyTo;

            if (ShouldLogDiagnosticPayload(correlationId))
            {
                LogPayloadDiagnosticAtCallbackEntry(
                    _logger,
                    DateTimeOffset.UtcNow,
                    _identity.Backbone,
                    _identity.SessionOrdinal,
                    _identity.SessionKey,
                    args.DeliveryTag,
                    correlationId,
                    rabbitMqMessageId,
                    replyTo,
                    args.Body);
            }

            byte[] payloadCopy = args.Body.ToArray();

            RabbitMqArticleDelivery delivery = new(
                Backbone: _identity.Backbone,
                Queue: _queueName,
                ConsumerTag: args.ConsumerTag,
                ConsumerIdentity: _identity.SessionKey,
                DeliveryTag: args.DeliveryTag,
                Redelivered: args.Redelivered,
                RoutingKey: args.RoutingKey,
                Exchange: args.Exchange,
                ConnectionGeneration: deliveryGeneration,
                RabbitMqMessageId: rabbitMqMessageId,
                CorrelationId: correlationId,
                ReplyTo: replyTo,
                Payload: payloadCopy,
                CancellationToken: cancellationToken,
                Settlement: new RabbitMqDeliverySettlement(this, args.DeliveryTag, deliveryGeneration, tracker),
                AdmissionTracker: tracker);

            try
            {
                await _deliverySink.OnDeliveryAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                tracker?.MarkSettled();
            }
            catch
            {
                tracker?.MarkSettled();
                throw;
            }
        }

        /// <summary>
        /// Recreates the consumer when the broker shuts down the active registration unexpectedly.
        /// </summary>
        private async Task OnConsumerShutdownAsync(object sender, ShutdownEventArgs args)
        {
            if (_lifecycleState is not RabbitMqConsumerLifecycleState.Running || _disposed || !IsEventFromActiveConsumer(sender))
            {
                return;
            }

            LogConsumerShutdownObserved(_logger, _identity.Backbone, _identity.SessionOrdinal, args.ReplyCode, args.ReplyText, args.Initiator.ToString());

            await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                if (_disposed || _lifecycleState is not RabbitMqConsumerLifecycleState.Running || !IsEventFromActiveConsumer(sender))
                {
                    return;
                }

                await RecreateConsumerCoreAsync(_connectionManager.ConnectionGeneration, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogConsumerRecreationFailed(_logger, _identity.Backbone, _identity.SessionOrdinal, ex);
            }
            finally
            {
                _ = _lifecycleGate.Release();
            }
        }

        /// <summary>
        /// Recreates the consumer when the broker unregisters the active consumer unexpectedly.
        /// </summary>
        private async Task OnConsumerUnregisteredAsync(object sender, ConsumerEventArgs args)
        {
            if (_disposed || _lifecycleState is not RabbitMqConsumerLifecycleState.Running || !IsEventFromActiveConsumer(sender))
            {
                return;
            }

            int consumerTagCount = args.ConsumerTags.Length;
            LogConsumerCancellationObserved(_logger, _identity.Backbone, _identity.SessionOrdinal, consumerTagCount);

            await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                if (_disposed || _lifecycleState is not RabbitMqConsumerLifecycleState.Running || !IsEventFromActiveConsumer(sender))
                {
                    return;
                }

                await RecreateConsumerCoreAsync(_connectionManager.ConnectionGeneration, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogConsumerRecreationFailed(_logger, _identity.Backbone, _identity.SessionOrdinal, ex);
            }
            finally
            {
                _ = _lifecycleGate.Release();
            }
        }

        /// <summary>
        /// Recreates the consumer against a newer connection generation.
        /// </summary>
        private async Task RecreateConsumerCoreAsync(long requestedGeneration, CancellationToken cancellationToken)
        {
            long previousGeneration = ActiveConnectionGeneration;
            LogConsumerRecreationStarting(_logger, _identity.Backbone, _identity.SessionOrdinal, previousGeneration, requestedGeneration);

            await StopCoreAsync(CancellationToken.None, expectedShutdown: false, cancelAdmittedWork: true).ConfigureAwait(false);
            await StartCoreAsync(cancellationToken).ConfigureAwait(false);

            LogConsumerRecreationCompleted(_logger, _identity.Backbone, _identity.SessionOrdinal, ActiveConnectionGeneration);
        }

        /// <summary>
        /// Determines whether the consumer is stale relative to the current connection generation.
        /// </summary>
        private bool IsActiveConsumerStaleForCurrentConnection()
        {
            if (_lifecycleState is not RabbitMqConsumerLifecycleState.Running || _ownedChannel is null)
            {
                return false;
            }

            long activeGeneration = ActiveConnectionGeneration;
            return activeGeneration <= 0 || _ownedChannel.ConnectionGeneration != activeGeneration || _connectionManager.ConnectionGeneration > activeGeneration;
        }

        /// <summary>
        /// Determines whether an event callback came from the currently active consumer instance.
        /// </summary>
        private bool IsEventFromActiveConsumer(object sender)
        {
            return _consumer is not null && ReferenceEquals(sender, _consumer);
        }

        /// <summary>
        /// Updates admitted-delivery drain accounting after a delivery reaches terminal settlement.
        /// </summary>
        private async Task OnAdmittedDeliverySettledAsync()
        {
            await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                if (_admittedDeliveryCount <= 0)
                {
                    return;
                }

                _admittedDeliveryCount--;
                if (_admittedDeliveryCount == 0)
                {
                    _ = _drainCompletion.TrySetResult(true);
                }
            }
            finally
            {
                _ = _lifecycleGate.Release();
            }
        }

        /// <summary>
        /// Abandons admitted-delivery drain accounting after cooperative cancellation has been signaled.
        /// </summary>
        /// <remarks>
        /// This helper releases StopAsync waiters without forcing broker settlement. Late ACK/NACK attempts remain invalid because
        /// settlement still requires an active owning channel generation, preserving broker-side ownership semantics after disposal.
        /// </remarks>
        private void AbandonAdmittedDeliveriesForDrainAccountingNoLock()
        {
            if (_admittedDeliveryCount <= 0)
            {
                return;
            }

            _admittedDeliveryCount = 0;
            _ = _drainCompletion.TrySetResult(true);
        }

        /// <summary>
        /// Handles create completed drain source for rabbit mq backbone consumer session.
        /// </summary>
        private static TaskCompletionSource<bool> CreateCompletedDrainSource()
        {
            TaskCompletionSource<bool> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = source.TrySetResult(true);
            return source;
        }

        /// <summary>
        /// Tracks whether one admitted delivery has contributed back to session drain accounting.
        /// </summary>
        private sealed class RabbitMqAdmittedDeliveryTracker : IRabbitMqAdmittedDeliveryTracker
        {
            /// <summary>
            /// Session whose admitted-delivery counters are updated when settlement completes.
            /// </summary>
            private readonly RabbitMqBackboneConsumerSession _owner;
            /// <summary>
            /// Single-bit guard that prevents duplicate settlement accounting.
            /// </summary>
            private int _completed;

            /// <summary>
            /// Initializes the tracker for one admitted delivery.
            /// </summary>
            /// <param name="owner">Owning consumer session.</param>
            internal RabbitMqAdmittedDeliveryTracker(RabbitMqBackboneConsumerSession owner)
            {
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            }

            /// <summary>
            /// Records terminal settlement once and releases one admitted-delivery slot.
            /// </summary>
            public void MarkSettled()
            {
                if (Interlocked.Exchange(ref _completed, 1) != 0)
                {
                    return;
                }

                _ = _owner.OnAdmittedDeliverySettledAsync();
            }
        }

        /// <summary>
        /// Settlement handle that ACKs or NACKs a delivery on the original consumer channel generation.
        /// </summary>
        private sealed class RabbitMqDeliverySettlement : IRabbitMqDeliverySettlement
        {
            /// <summary>
            /// Session whose admitted-delivery counters are updated when settlement completes.
            /// </summary>
            private readonly RabbitMqBackboneConsumerSession _owner;
            /// <summary>
            /// Broker delivery tag to settle.
            /// </summary>
            private readonly ulong _deliveryTag;
            /// <summary>
            /// Connection generation on which the delivery was admitted.
            /// </summary>
            private readonly long _deliveryGeneration;
            /// <summary>
            /// Optional tracker that returns admitted-delivery capacity after settlement.
            /// </summary>
            private readonly RabbitMqAdmittedDeliveryTracker? _admissionTracker;
            /// <summary>
            /// Single-bit guard that enforces exactly-once settlement.
            /// </summary>
            private int _settled;

            /// <summary>
            /// Initializes a settlement handle for one admitted delivery.
            /// </summary>
            /// <param name="owner">Owning consumer session.</param>
            /// <param name="deliveryTag">Broker delivery tag to settle.</param>
            /// <param name="deliveryGeneration">Connection generation on which the delivery was admitted.</param>
            /// <param name="admissionTracker">Optional admitted-delivery tracker.</param>
            internal RabbitMqDeliverySettlement(RabbitMqBackboneConsumerSession owner, ulong deliveryTag, long deliveryGeneration, RabbitMqAdmittedDeliveryTracker? admissionTracker)
            {
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
                _deliveryTag = deliveryTag;
                _deliveryGeneration = deliveryGeneration;
                _admissionTracker = admissionTracker;
            }

            /// <summary>
            /// Positively acknowledges the delivery on the original consumer channel generation.
            /// </summary>
            /// <param name="cancellationToken">Cancellation token for broker acknowledgement.</param>
            /// <returns>A value task that completes after broker acknowledgement succeeds.</returns>
            public async ValueTask AckAsync(CancellationToken cancellationToken)
            {
                await SettleAsync(requeue: null, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Negatively acknowledges the delivery on the original consumer channel generation.
            /// </summary>
            /// <param name="requeue"><see langword="true"/> to request broker requeue.</param>
            /// <param name="cancellationToken">Cancellation token for broker negative acknowledgement.</param>
            /// <returns>A value task that completes after broker settlement succeeds.</returns>
            public async ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                await SettleAsync(requeue, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Performs generation-checked broker settlement and returns admitted-delivery capacity exactly once.
            /// </summary>
            private async ValueTask SettleAsync(bool? requeue, CancellationToken cancellationToken)
            {
                if (Interlocked.Exchange(ref _settled, 1) != 0)
                {
                    throw new InvalidOperationException("RabbitMQ delivery has already been settled.");
                }

                await _owner._lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    if (_owner._ownedChannel is null || _owner._lifecycleState is RabbitMqConsumerLifecycleState.Stopped)
                    {
                        throw new InvalidOperationException("RabbitMQ consumer session channel is not available for settlement.");
                    }

                    if (_owner._settlementAdmissionAbandoned)
                    {
                        throw new InvalidOperationException("RabbitMQ delivery settlement was abandoned during consumer shutdown.");
                    }

                    long activeGeneration = _owner.ActiveConnectionGeneration;
                    if (activeGeneration <= 0 || activeGeneration != _deliveryGeneration || _owner._ownedChannel.ConnectionGeneration != _deliveryGeneration)
                    {
                        throw new InvalidOperationException("RabbitMQ delivery settlement channel generation is stale.");
                    }

                    if (requeue.HasValue)
                    {
                        await _owner._ownedChannel.Channel.BasicNackAsync(_deliveryTag, multiple: false, requeue: requeue.Value, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await _owner._ownedChannel.Channel.BasicAckAsync(_deliveryTag, multiple: false, cancellationToken).ConfigureAwait(false);
                    }

                    _admissionTracker?.MarkSettled();
                }
                catch
                {
                    _ = Interlocked.Exchange(ref _settled, 0);
                    throw;
                }
                finally
                {
                    _ = _owner._lifecycleGate.Release();
                }
            }
        }

        /// <summary>
        /// Pushes the connection-scoped Serilog properties used while the consumer is active.
        /// </summary>
        private IDisposable BeginConnectionScope()
        {
            List<IDisposable> scopes =
            [
                LogContext.PushProperty("Backbone", _identity.Backbone),
                LogContext.PushProperty("AccountUsername", _identity.AccountUsername),
                LogContext.PushProperty("AccountId", _identity.AccountId),
                LogContext.PushProperty("ServerId", _identity.ServerId),
                LogContext.PushProperty("ConnectionNumber", _identity.ConnectionNumber),
                LogContext.PushProperty("ConnectionLimit", _identity.ConnectionLimit),
                LogContext.PushProperty("ConnectionHost", _identity.Host),
                LogContext.PushProperty("ConnectionPort", _identity.Port),
                LogContext.PushProperty("ConnectionUseSsl", _identity.UseSsl),
                LogContext.PushProperty("ConnectionPrefix", BuildConnectionPrefix(_identity.Backbone, _identity.AccountUsername, _identity.ConnectionNumber, _identity.ConnectionLimit)),
            ];

            return new CompositeDisposable(scopes);
        }

        /// <summary>
        /// Disposable that unwinds pushed logging scopes in reverse order.
        /// </summary>
        private sealed class CompositeDisposable(IReadOnlyList<IDisposable> scopes) : IDisposable
        {
            /// <summary>
            /// Logging scopes to dispose in reverse push order.
            /// </summary>
            private readonly IReadOnlyList<IDisposable> _scopes = scopes ?? throw new ArgumentNullException(nameof(scopes));

            /// <summary>
            /// Disposes the captured scopes in reverse order.
            /// </summary>
            public void Dispose()
            {
                for (int i = _scopes.Count - 1; i >= 0; i--)
                {
                    _scopes[i].Dispose();
                }
            }
        }

        /// <summary>
        /// Builds the fixed-width connection prefix used in connection-scoped diagnostics.
        /// </summary>
        private static string BuildConnectionPrefix(string backbone, string accountUsername, int connectionNumber, int connectionLimit)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
            ArgumentException.ThrowIfNullOrWhiteSpace(accountUsername);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectionNumber, 0);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectionLimit, 0);

            int width = Math.Max(3, connectionLimit.ToString(System.Globalization.CultureInfo.InvariantCulture).Length);
            return $"{backbone}/{accountUsername}[{connectionNumber.ToString($"D{width}", System.Globalization.CultureInfo.InvariantCulture)}/{connectionLimit.ToString($"D{width}", System.Globalization.CultureInfo.InvariantCulture)}]: ";
        }

        /// <summary>
        /// Throws when the session has already been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RabbitMqBackboneConsumerSession));
            }
        }


        /// <summary>
        /// Emits the informational log describing a newly started consumer session and its broker registration details.
        /// </summary>
        /// <param name="logger">Logger receiving the start event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the started consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="queue">RabbitMQ queue that the consumer registered against.</param>
        /// <param name="connectionGeneration">Connection generation assigned to the owned channel when the consumer started.</param>
        /// <param name="consumerTag">Broker-issued consumer tag returned from the BasicConsume registration.</param>
        [LoggerMessage(EventId = 4300, Level = LogLevel.Information, Message = "RabbitMQ consumer session started. Backbone={Backbone} Session={SessionOrdinal} Queue={Queue} ConnectionGeneration={ConnectionGeneration} ConsumerTag={ConsumerTag}")]
        private static partial void LogConsumerStarted(ILogger logger, string backbone, int sessionOrdinal, string queue, long connectionGeneration, string consumerTag);

        /// <summary>
        /// Emits the informational log that the consumer session has fully stopped.
        /// </summary>
        /// <param name="logger">Logger receiving the stop event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the stopped consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        [LoggerMessage(EventId = 4301, Level = LogLevel.Information, Message = "RabbitMQ consumer session stopped. Backbone={Backbone} Session={SessionOrdinal}")]
        private static partial void LogConsumerStopped(ILogger logger, string backbone, int sessionOrdinal);

        /// <summary>
        /// Emits the informational log that the consumer session is entering its retiring state.
        /// </summary>
        /// <param name="logger">Logger receiving the retirement event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the retiring consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="admittedDeliveries">Number of admitted deliveries still tracked when retirement begins.</param>
        [LoggerMessage(EventId = 4302, Level = LogLevel.Information, Message = "RabbitMQ consumer session entering retiring state. Backbone={Backbone} Session={SessionOrdinal} AdmittedDeliveries={AdmittedDeliveries}")]
        private static partial void LogConsumerRetiring(ILogger logger, string backbone, int sessionOrdinal, int admittedDeliveries);

        /// <summary>
        /// Emits the informational log that drain processing has started for a retiring consumer session.
        /// </summary>
        /// <param name="logger">Logger receiving the drain-start event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the draining consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="pendingAdmittedDeliveries">Number of admitted deliveries still pending when drain begins.</param>
        [LoggerMessage(EventId = 4303, Level = LogLevel.Information, Message = "RabbitMQ consumer drain started. Backbone={Backbone} Session={SessionOrdinal} PendingAdmittedDeliveries={PendingAdmittedDeliveries}")]
        private static partial void LogConsumerDrainStarted(ILogger logger, string backbone, int sessionOrdinal, int pendingAdmittedDeliveries);

        /// <summary>
        /// Emits the informational log that drain processing has completed for a consumer session.
        /// </summary>
        /// <param name="logger">Logger receiving the drain-complete event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the drained consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        [LoggerMessage(EventId = 4304, Level = LogLevel.Information, Message = "RabbitMQ consumer drain completed. Backbone={Backbone} Session={SessionOrdinal}")]
        private static partial void LogConsumerDrainCompleted(ILogger logger, string backbone, int sessionOrdinal);

        /// <summary>
        /// Emits the warning log that a broker shutdown notification was observed for the active consumer registration.
        /// </summary>
        /// <param name="logger">Logger receiving the shutdown-observed event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the affected consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="replyCode">Broker shutdown reply code reported by RabbitMQ.</param>
        /// <param name="replyText">Broker shutdown reply text reported by RabbitMQ.</param>
        /// <param name="initiator">String form of the shutdown initiator reported by RabbitMQ.</param>
        [LoggerMessage(EventId = 4305, Level = LogLevel.Warning, Message = "RabbitMQ consumer shutdown observed. Backbone={Backbone} Session={SessionOrdinal} ReplyCode={ReplyCode} ReplyText={ReplyText} Initiator={Initiator}")]
        private static partial void LogConsumerShutdownObserved(ILogger logger, string backbone, int sessionOrdinal, ushort replyCode, string replyText, string initiator);

        /// <summary>
        /// Emits the informational log that a consumer recreation has begun because the connection generation changed.
        /// </summary>
        /// <param name="logger">Logger receiving the recreation-start event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the consumer being recreated.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="previousGeneration">Connection generation that was active before the replacement began.</param>
        /// <param name="newGeneration">Requested connection generation that triggered the replacement.</param>
        [LoggerMessage(EventId = 4306, Level = LogLevel.Information, Message = "RabbitMQ consumer recreation starting due to connection replacement. Backbone={Backbone} Session={SessionOrdinal} PreviousGeneration={PreviousGeneration} NewGeneration={NewGeneration}")]
        private static partial void LogConsumerRecreationStarting(ILogger logger, string backbone, int sessionOrdinal, long previousGeneration, long newGeneration);

        /// <summary>
        /// Emits the informational log that a consumer recreation completed and is now bound to the replacement generation.
        /// </summary>
        /// <param name="logger">Logger receiving the recreation-complete event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the recreated consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="activeGeneration">Connection generation now bound to the active consumer registration.</param>
        [LoggerMessage(EventId = 4307, Level = LogLevel.Information, Message = "RabbitMQ consumer recreation completed. Backbone={Backbone} Session={SessionOrdinal} ActiveGeneration={ActiveGeneration}")]
        private static partial void LogConsumerRecreationCompleted(ILogger logger, string backbone, int sessionOrdinal, long activeGeneration);

        /// <summary>
        /// Emits the debug log that a cancellation request during expected shutdown encountered an error message.
        /// </summary>
        /// <param name="logger">Logger receiving the shutdown-cancellation diagnostic.</param>
        /// <param name="backbone">Backbone queue identity associated with the shutdown attempt.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="reason">Exception message captured from the failed cancellation attempt.</param>
        [LoggerMessage(EventId = 4308, Level = LogLevel.Debug, Message = "RabbitMQ consumer cancellation during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}")]
        private static partial void LogConsumerCancelDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason);

        /// <summary>
        /// Emits the debug log that a channel disposal request during expected shutdown encountered an error message.
        /// </summary>
        /// <param name="logger">Logger receiving the shutdown-disposal diagnostic.</param>
        /// <param name="backbone">Backbone queue identity associated with the shutdown attempt.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="reason">Exception message captured from the failed channel disposal attempt.</param>
        [LoggerMessage(EventId = 4309, Level = LogLevel.Debug, Message = "RabbitMQ consumer channel dispose during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}")]
        private static partial void LogConsumerChannelDisposeDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason);

        /// <summary>
        /// Emits the informational log that the consumer prefetch setting has been configured.
        /// </summary>
        /// <param name="logger">Logger receiving the prefetch-configured event for the consumer session.</param>
        /// <param name="backbone">Backbone queue identity associated with the configured consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="prefetchCount">Prefetch count applied to the RabbitMQ channel.</param>
        [LoggerMessage(EventId = 4310, Level = LogLevel.Information, Message = "RabbitMQ consumer prefetch configured. Backbone={Backbone} Session={SessionOrdinal} PrefetchCount={PrefetchCount}")]
        private static partial void LogConsumerPrefetchConfigured(ILogger logger, string backbone, int sessionOrdinal, ushort prefetchCount);

        /// <summary>
        /// Emits the error log that cancellation of the consumer session failed unexpectedly.
        /// </summary>
        /// <param name="logger">Logger receiving the cancellation-failure event.</param>
        /// <param name="backbone">Backbone queue identity associated with the affected consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="exception">Exception raised while cancelling the active RabbitMQ consumer registration.</param>
        [LoggerMessage(EventId = 4311, Level = LogLevel.Error, Message = "RabbitMQ consumer cancellation failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}")]
        private static partial void LogConsumerCancellationFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception);

        /// <summary>
        /// Emits the error log that channel disposal for the consumer session failed unexpectedly.
        /// </summary>
        /// <param name="logger">Logger receiving the channel-disposal failure event.</param>
        /// <param name="backbone">Backbone queue identity associated with the affected consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="exception">Exception raised while disposing the owned RabbitMQ channel.</param>
        [LoggerMessage(EventId = 4312, Level = LogLevel.Error, Message = "RabbitMQ consumer channel disposal failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}")]
        private static partial void LogConsumerChannelDisposeFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception);

        /// <summary>
        /// Emits the warning log that the broker unregistered the consumer registration unexpectedly.
        /// </summary>
        /// <param name="logger">Logger receiving the broker-unregistered event.</param>
        /// <param name="backbone">Backbone queue identity associated with the affected consumer.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="consumerTagCount">Number of consumer tags reported by the broker in the unregistration callback.</param>
        [LoggerMessage(EventId = 4313, Level = LogLevel.Warning, Message = "RabbitMQ consumer unregistered by broker. Backbone={Backbone} Session={SessionOrdinal} ConsumerTagCount={ConsumerTagCount}")]
        private static partial void LogConsumerCancellationObserved(ILogger logger, string backbone, int sessionOrdinal, int consumerTagCount);

        /// <summary>
        /// Determines whether a delivery matches the configured diagnostic correlation identifier.
        /// </summary>
        private bool ShouldLogDiagnosticPayload(string? correlationId)
        {
            return !string.IsNullOrWhiteSpace(_diagnosticCorrelationId)
                && !string.IsNullOrWhiteSpace(correlationId)
                && string.Equals(_diagnosticCorrelationId, correlationId, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Emits the debug log that a delivery was ignored because the consumer generation is stale.
        /// </summary>
        /// <param name="logger">Logger receiving the stale-generation diagnostic.</param>
        /// <param name="backbone">Backbone queue identity associated with the ignored delivery.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="deliveryGeneration">Generation captured when the delivery was admitted.</param>
        /// <param name="currentGeneration">Current connection generation used to determine staleness.</param>
        [LoggerMessage(EventId = 4314, Level = LogLevel.Debug, Message = "RabbitMQ delivery ignored because session generation is stale. Backbone={Backbone} Session={SessionOrdinal} DeliveryGeneration={DeliveryGeneration} CurrentGeneration={CurrentGeneration}")]
        private static partial void LogDeliveryIgnoredFromStaleGeneration(ILogger logger, string backbone, int sessionOrdinal, long deliveryGeneration, long currentGeneration);

        /// <summary>
        /// Emits the error log that consumer recreation failed unexpectedly after a shutdown or unregistration callback.
        /// </summary>
        /// <param name="logger">Logger receiving the recreation-failure event.</param>
        /// <param name="backbone">Backbone queue identity associated with the consumer being recreated.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="exception">Exception raised while stopping or restarting the consumer registration.</param>
        [LoggerMessage(EventId = 4315, Level = LogLevel.Error, Message = "RabbitMQ consumer recreation failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}")]
        private static partial void LogConsumerRecreationFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception);

        /// <summary>
        /// Emits the informational log that records a diagnostic snapshot of the callback payload before settlement and forwarding.
        /// </summary>
        /// <param name="logger">Logger receiving the payload-diagnostic event.</param>
        /// <param name="timestampUtc">UTC time at which the callback snapshot was captured.</param>
        /// <param name="backbone">Backbone queue identity associated with the delivery callback.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="sessionKey">Canonical session key that identifies the owning session.</param>
        /// <param name="deliveryTag">Broker delivery tag associated with the snapshot.</param>
        /// <param name="correlationId">Message correlation identifier observed on the delivery.</param>
        /// <param name="rabbitMqMessageId">RabbitMQ message identifier observed on the delivery.</param>
        /// <param name="replyTo">Reply-to address observed on the delivery.</param>
        /// <param name="payload">Payload captured from the callback body.</param>
        private static void LogPayloadDiagnosticAtCallbackEntry(
            ILogger logger,
            DateTimeOffset timestampUtc,
            string backbone,
            int sessionOrdinal,
            string sessionKey,
            ulong deliveryTag,
            string? correlationId,
            string? rabbitMqMessageId,
            string? replyTo,
            ReadOnlyMemory<byte> payload)
        {
            if (!logger.IsEnabled(LogLevel.Information))
            {
                return;
            }

            string payloadUtf8 = Encoding.UTF8.GetString(payload.Span);
            string payloadHex = Convert.ToHexString(payload.Span);
            string payloadSha256 = Convert.ToHexString(SHA256.HashData(payload.Span));

            LogPayloadDiagnosticAtCallbackEntryMessage(
                logger,
                timestampUtc,
                backbone,
                sessionOrdinal,
                sessionKey,
                deliveryTag,
                correlationId,
                rabbitMqMessageId,
                replyTo,
                payload.Length,
                payloadUtf8,
                payloadHex,
                payloadSha256);
        }

        /// <summary>
        /// Emits the informational log that records a diagnostic snapshot of the callback payload before settlement and forwarding.
        /// </summary>
        /// <param name="logger">Logger receiving the payload-diagnostic event.</param>
        /// <param name="timestampUtc">UTC time at which the callback snapshot was captured.</param>
        /// <param name="backbone">Backbone queue identity associated with the delivery callback.</param>
        /// <param name="sessionOrdinal">Stable session ordinal for the consumer instance.</param>
        /// <param name="sessionKey">Canonical session key that identifies the owning session.</param>
        /// <param name="deliveryTag">Broker delivery tag associated with the snapshot.</param>
        /// <param name="correlationId">Message correlation identifier observed on the delivery.</param>
        /// <param name="rabbitMqMessageId">RabbitMQ message identifier observed on the delivery.</param>
        /// <param name="replyTo">Reply-to address observed on the delivery.</param>
        /// <param name="payloadLength">Payload length captured from the callback body.</param>
        /// <param name="payloadUtf8">UTF-8 decoded payload representation used for diagnostics.</param>
        /// <param name="payloadHex">Hexadecimal payload representation used for diagnostics.</param>
        /// <param name="payloadSha256">SHA-256 digest of the payload used for correlation and triage.</param>
        [LoggerMessage(EventId = 4316, Level = LogLevel.Information, Message = "RabbitMQ payload diagnostic callback-entry. TimestampUtc={TimestampUtc:o} Backbone={Backbone} Session={SessionOrdinal} SessionKey={SessionKey} DeliveryTag={DeliveryTag} CorrelationId={CorrelationId} RabbitMqMessageId={RabbitMqMessageId} ReplyTo={ReplyTo} PayloadLength={PayloadLength} PayloadUtf8={PayloadUtf8} PayloadHex={PayloadHex} PayloadSha256={PayloadSha256}")]
        private static partial void LogPayloadDiagnosticAtCallbackEntryMessage(
            ILogger logger,
            DateTimeOffset timestampUtc,
            string backbone,
            int sessionOrdinal,
            string sessionKey,
            ulong deliveryTag,
            string? correlationId,
            string? rabbitMqMessageId,
            string? replyTo,
            int payloadLength,
            string payloadUtf8,
            string payloadHex,
            string payloadSha256);
    }
}
