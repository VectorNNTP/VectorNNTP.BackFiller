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
        /// Tracks identity for rabbit mq backbone consumer session.
        /// </summary>
        private readonly RabbitMqConsumerSessionIdentity _identity;
        /// <summary>
        /// Tracks queue name for rabbit mq backbone consumer session.
        /// </summary>
        private readonly string _queueName;
        /// <summary>
        /// Tracks connection manager for rabbit mq backbone consumer session.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager;
        /// <summary>
        /// Tracks topology initializer for rabbit mq backbone consumer session.
        /// </summary>
        private readonly RabbitMqTopologyInitializer _topologyInitializer;
        /// <summary>
        /// Tracks delivery sink for rabbit mq backbone consumer session.
        /// </summary>
        private readonly IRabbitMqDeliverySink _deliverySink;
        /// <summary>
        /// Provides logging for rabbit mq backbone consumer session.
        /// </summary>
        private readonly ILogger<RabbitMqBackboneConsumerSession> _logger;
        /// <summary>
        /// Limits prefetch count for rabbit mq backbone consumer session.
        /// </summary>
        private readonly ushort? _prefetchCount;
        /// <summary>
        /// Tracks diagnostic correlation id for rabbit mq backbone consumer session.
        /// </summary>
        private readonly string? _diagnosticCorrelationId;
        /// <summary>
        /// Tracks lifecycle gate for rabbit mq backbone consumer session.
        /// </summary>
        private readonly SemaphoreSlim _lifecycleGate = new(1, 1);

        /// <summary>
        /// Tracks owned channel for rabbit mq backbone consumer session.
        /// </summary>
        private RabbitMqOwnedChannel? _ownedChannel;
        /// <summary>
        /// Tracks consumer for rabbit mq backbone consumer session.
        /// </summary>
        private AsyncEventingBasicConsumer? _consumer;
        /// <summary>
        /// Tracks consumer tag for rabbit mq backbone consumer session.
        /// </summary>
        private string? _consumerTag;
        /// <summary>
        /// Tracks session cancellation for rabbit mq backbone consumer session.
        /// </summary>
        private CancellationTokenSource? _sessionCancellation;
        /// <summary>
        /// Tracks drain completion for rabbit mq backbone consumer session.
        /// </summary>
        private TaskCompletionSource<bool> _drainCompletion = CreateCompletedDrainSource();
        /// <summary>
        /// Tracks active connection generation for rabbit mq backbone consumer session.
        /// </summary>
        private long _activeConnectionGeneration;
        /// <summary>
        /// Limits admitted delivery count for rabbit mq backbone consumer session.
        /// </summary>
        private int _admittedDeliveryCount;
        /// <summary>
        /// Tracks lifecycle state for rabbit mq backbone consumer session.
        /// </summary>
        private RabbitMqConsumerLifecycleState _lifecycleState = RabbitMqConsumerLifecycleState.Stopped;
        /// <summary>
        /// Tracks disposed for rabbit mq backbone consumer session.
        /// </summary>
        private bool _disposed;
        /// <summary>
        /// Tracks connection scope for rabbit mq backbone consumer session.
        /// </summary>
        private IDisposable? _connectionScope;

        /// <summary>
        /// Defines rabbit mq consumer lifecycle state and its rabbit mq backbone consumer session contract.
        /// </summary>
        private enum RabbitMqConsumerLifecycleState
        {
            Running,
            Retiring,
            Stopped,
        }

        /// <summary>
        /// Coordinates rabbit mq backbone consumer session for rabbit mq backbone consumer session.
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
        /// Tracks identity for rabbit mq backbone consumer session.
        /// </summary>
        internal RabbitMqConsumerSessionIdentity Identity => _identity;

        RabbitMqConsumerSessionIdentity IRabbitMqConsumerSession.Identity => _identity;

        /// <summary>
        /// Tracks is running for rabbit mq backbone consumer session.
        /// </summary>
        internal bool IsRunning => _lifecycleState is RabbitMqConsumerLifecycleState.Running;

        bool IRabbitMqConsumerSession.IsRunning => _lifecycleState is RabbitMqConsumerLifecycleState.Running;

        /// <summary>
        /// Tracks active connection generation for rabbit mq backbone consumer session.
        /// </summary>
        internal long ActiveConnectionGeneration => Interlocked.Read(ref _activeConnectionGeneration);

        long IRabbitMqConsumerSession.ActiveConnectionGeneration => ActiveConnectionGeneration;

        /// <summary>
        /// Coordinates handle connection replaced async for rabbit mq backbone consumer session.
        /// </summary>
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
        /// Coordinates start async for rabbit mq backbone consumer session.
        /// </summary>
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
        /// Coordinates stop async for rabbit mq backbone consumer session.
        /// </summary>
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
        /// Coordinates dispose async for rabbit mq backbone consumer session.
        /// </summary>
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
        /// Coordinates start core async for rabbit mq backbone consumer session.
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
                _admittedDeliveryCount = 0;
                _drainCompletion = CreateCompletedDrainSource();
                _connectionScope?.Dispose();
                _connectionScope = null;
                _ = Interlocked.Exchange(ref _activeConnectionGeneration, 0);
                throw;
            }
        }

        /// <summary>
        /// Coordinates stop core async for rabbit mq backbone consumer session.
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
        /// Coordinates on received async for rabbit mq backbone consumer session.
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
        /// Coordinates on consumer shutdown async for rabbit mq backbone consumer session.
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
        /// Coordinates on consumer unregistered async for rabbit mq backbone consumer session.
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
        /// Coordinates recreate consumer core async for rabbit mq backbone consumer session.
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
        /// Coordinates is active consumer stale for current connection for rabbit mq backbone consumer session.
        /// </summary>
        private bool IsActiveConsumerStaleForCurrentConnection()
        {
            if (_lifecycleState is not RabbitMqConsumerLifecycleState.Running || _ownedChannel is null)
            {
                return false;
            }

            long activeGeneration = ActiveConnectionGeneration;
            if (activeGeneration <= 0)
            {
                return true;
            }

            if (_ownedChannel.ConnectionGeneration != activeGeneration)
            {
                return true;
            }

            return _connectionManager.ConnectionGeneration > activeGeneration;
        }

        /// <summary>
        /// Coordinates is event from active consumer for rabbit mq backbone consumer session.
        /// </summary>
        private bool IsEventFromActiveConsumer(object sender)
        {
            return _consumer is not null && ReferenceEquals(sender, _consumer);
        }

        /// <summary>
        /// Coordinates on admitted delivery settled async for rabbit mq backbone consumer session.
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
        /// Coordinates create completed drain source for rabbit mq backbone consumer session.
        /// </summary>
        private static TaskCompletionSource<bool> CreateCompletedDrainSource()
        {
            TaskCompletionSource<bool> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = source.TrySetResult(true);
            return source;
        }

        /// <summary>
        /// Defines rabbit mq admitted delivery tracker and its rabbit mq backbone consumer session contract.
        /// </summary>
        private sealed class RabbitMqAdmittedDeliveryTracker : IRabbitMqAdmittedDeliveryTracker
        {
            /// <summary>
            /// Tracks owner for rabbit mq backbone consumer session.
            /// </summary>
            private readonly RabbitMqBackboneConsumerSession _owner;
            /// <summary>
            /// Tracks completed for rabbit mq backbone consumer session.
            /// </summary>
            private int _completed;

            /// <summary>
            /// Coordinates rabbit mq admitted delivery tracker for rabbit mq backbone consumer session.
            /// </summary>
            internal RabbitMqAdmittedDeliveryTracker(RabbitMqBackboneConsumerSession owner)
            {
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            }

            /// <summary>
            /// Coordinates mark settled for rabbit mq backbone consumer session.
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
        /// Defines rabbit mq delivery settlement and its rabbit mq backbone consumer session contract.
        /// </summary>
        private sealed class RabbitMqDeliverySettlement : IRabbitMqDeliverySettlement
        {
            /// <summary>
            /// Tracks owner for rabbit mq backbone consumer session.
            /// </summary>
            private readonly RabbitMqBackboneConsumerSession _owner;
            /// <summary>
            /// Tracks delivery tag for rabbit mq backbone consumer session.
            /// </summary>
            private readonly ulong _deliveryTag;
            /// <summary>
            /// Tracks delivery generation for rabbit mq backbone consumer session.
            /// </summary>
            private readonly long _deliveryGeneration;
            /// <summary>
            /// Tracks admission tracker for rabbit mq backbone consumer session.
            /// </summary>
            private readonly RabbitMqAdmittedDeliveryTracker? _admissionTracker;
            /// <summary>
            /// Tracks settled for rabbit mq backbone consumer session.
            /// </summary>
            private int _settled;

            /// <summary>
            /// Coordinates rabbit mq delivery settlement for rabbit mq backbone consumer session.
            /// </summary>
            internal RabbitMqDeliverySettlement(RabbitMqBackboneConsumerSession owner, ulong deliveryTag, long deliveryGeneration, RabbitMqAdmittedDeliveryTracker? admissionTracker)
            {
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
                _deliveryTag = deliveryTag;
                _deliveryGeneration = deliveryGeneration;
                _admissionTracker = admissionTracker;
            }

            /// <summary>
            /// Coordinates ack async for rabbit mq backbone consumer session.
            /// </summary>
            public async ValueTask AckAsync(CancellationToken cancellationToken)
            {
                await SettleAsync(requeue: null, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Coordinates nack async for rabbit mq backbone consumer session.
            /// </summary>
            public async ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                await SettleAsync(requeue, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Coordinates settle async for rabbit mq backbone consumer session.
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
        /// Coordinates begin connection scope for rabbit mq backbone consumer session.
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
        /// Defines composite disposable and its rabbit mq backbone consumer session contract.
        /// </summary>
        private sealed class CompositeDisposable(IReadOnlyList<IDisposable> scopes) : IDisposable
        {
            /// <summary>
            /// Tracks scopes for rabbit mq backbone consumer session.
            /// </summary>
            private readonly IReadOnlyList<IDisposable> _scopes = scopes ?? throw new ArgumentNullException(nameof(scopes));

            /// <summary>
            /// Coordinates dispose for rabbit mq backbone consumer session.
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
        /// Coordinates build connection prefix for rabbit mq backbone consumer session.
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
        /// Coordinates throw if disposed for rabbit mq backbone consumer session.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RabbitMqBackboneConsumerSession));
            }
        }

        /// <summary>
        /// Coordinates log consumer started for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerStarted(ILogger logger, string backbone, int sessionOrdinal, string queue, long connectionGeneration, string consumerTag)
        {
            logger.LogInformation("RabbitMQ consumer session started. Backbone={Backbone} Session={SessionOrdinal} Queue={Queue} ConnectionGeneration={ConnectionGeneration} ConsumerTag={ConsumerTag}", backbone, sessionOrdinal, queue, connectionGeneration, consumerTag);
        }

        /// <summary>
        /// Coordinates log consumer stopped for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerStopped(ILogger logger, string backbone, int sessionOrdinal)
        {
            logger.LogInformation("RabbitMQ consumer session stopped. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Coordinates log consumer retiring for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerRetiring(ILogger logger, string backbone, int sessionOrdinal, int admittedCount)
        {
            logger.LogInformation("RabbitMQ consumer session entering retiring state. Backbone={Backbone} Session={SessionOrdinal} AdmittedDeliveries={AdmittedDeliveries}", backbone, sessionOrdinal, admittedCount);
        }

        /// <summary>
        /// Coordinates log consumer drain started for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerDrainStarted(ILogger logger, string backbone, int sessionOrdinal, int admittedCount)
        {
            logger.LogInformation("RabbitMQ consumer drain started. Backbone={Backbone} Session={SessionOrdinal} PendingAdmittedDeliveries={PendingAdmittedDeliveries}", backbone, sessionOrdinal, admittedCount);
        }

        /// <summary>
        /// Coordinates log consumer drain completed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerDrainCompleted(ILogger logger, string backbone, int sessionOrdinal)
        {
            logger.LogInformation("RabbitMQ consumer drain completed. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Coordinates log consumer shutdown observed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerShutdownObserved(ILogger logger, string backbone, int sessionOrdinal, ushort replyCode, string replyText, string initiator)
        {
            logger.LogWarning("RabbitMQ consumer shutdown observed. Backbone={Backbone} Session={SessionOrdinal} ReplyCode={ReplyCode} ReplyText={ReplyText} Initiator={Initiator}", backbone, sessionOrdinal, replyCode, replyText, initiator);
        }

        /// <summary>
        /// Coordinates log consumer recreation starting for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerRecreationStarting(ILogger logger, string backbone, int sessionOrdinal, long previousGeneration, long newGeneration)
        {
            logger.LogInformation("RabbitMQ consumer recreation starting due to connection replacement. Backbone={Backbone} Session={SessionOrdinal} PreviousGeneration={PreviousGeneration} NewGeneration={NewGeneration}", backbone, sessionOrdinal, previousGeneration, newGeneration);
        }

        /// <summary>
        /// Coordinates log consumer recreation completed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerRecreationCompleted(ILogger logger, string backbone, int sessionOrdinal, long activeGeneration)
        {
            logger.LogInformation("RabbitMQ consumer recreation completed. Backbone={Backbone} Session={SessionOrdinal} ActiveGeneration={ActiveGeneration}", backbone, sessionOrdinal, activeGeneration);
        }

        /// <summary>
        /// Coordinates log consumer cancel during shutdown failed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerCancelDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason)
        {
            logger.LogDebug("RabbitMQ consumer cancellation during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}", backbone, sessionOrdinal, reason);
        }

        /// <summary>
        /// Coordinates log consumer channel dispose during shutdown failed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerChannelDisposeDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason)
        {
            logger.LogDebug("RabbitMQ consumer channel dispose during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}", backbone, sessionOrdinal, reason);
        }

        /// <summary>
        /// Coordinates log consumer prefetch configured for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerPrefetchConfigured(ILogger logger, string backbone, int sessionOrdinal, ushort prefetchCount)
        {
            logger.LogInformation("RabbitMQ consumer prefetch configured. Backbone={Backbone} Session={SessionOrdinal} PrefetchCount={PrefetchCount}", backbone, sessionOrdinal, prefetchCount);
        }

        /// <summary>
        /// Coordinates log consumer cancellation failed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerCancellationFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
        {
            logger.LogError(exception, "RabbitMQ consumer cancellation failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Coordinates log consumer channel dispose failed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerChannelDisposeFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
        {
            logger.LogError(exception, "RabbitMQ consumer channel disposal failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Coordinates log consumer cancellation observed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerCancellationObserved(ILogger logger, string backbone, int sessionOrdinal, int consumerTagCount)
        {
            logger.LogWarning("RabbitMQ consumer unregistered by broker. Backbone={Backbone} Session={SessionOrdinal} ConsumerTagCount={ConsumerTagCount}", backbone, sessionOrdinal, consumerTagCount);
        }

        /// <summary>
        /// Coordinates should log diagnostic payload for rabbit mq backbone consumer session.
        /// </summary>
        private bool ShouldLogDiagnosticPayload(string? correlationId)
        {
            return !string.IsNullOrWhiteSpace(_diagnosticCorrelationId)
                && !string.IsNullOrWhiteSpace(correlationId)
                && string.Equals(_diagnosticCorrelationId, correlationId, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Coordinates log payload diagnostic at callback entry for rabbit mq backbone consumer session.
        /// </summary>
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
            string payloadUtf8 = Encoding.UTF8.GetString(payload.Span);
            string payloadHex = Convert.ToHexString(payload.Span);
            string payloadSha256 = Convert.ToHexString(SHA256.HashData(payload.Span));

            logger.LogInformation(
                "RabbitMQ payload diagnostic callback-entry. TimestampUtc={TimestampUtc:o} Backbone={Backbone} Session={SessionOrdinal} SessionKey={SessionKey} DeliveryTag={DeliveryTag} CorrelationId={CorrelationId} RabbitMqMessageId={RabbitMqMessageId} ReplyTo={ReplyTo} PayloadLength={PayloadLength} PayloadUtf8={PayloadUtf8} PayloadHex={PayloadHex} PayloadSha256={PayloadSha256}",
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
        /// Coordinates log delivery ignored from stale generation for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogDeliveryIgnoredFromStaleGeneration(ILogger logger, string backbone, int sessionOrdinal, long deliveryGeneration, long currentGeneration)
        {
            logger.LogDebug("RabbitMQ delivery ignored because session generation is stale. Backbone={Backbone} Session={SessionOrdinal} DeliveryGeneration={DeliveryGeneration} CurrentGeneration={CurrentGeneration}", backbone, sessionOrdinal, deliveryGeneration, currentGeneration);
        }

        /// <summary>
        /// Coordinates log consumer recreation failed for rabbit mq backbone consumer session.
        /// </summary>
        private static void LogConsumerRecreationFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
        {
            logger.LogError(exception, "RabbitMQ consumer recreation failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }
    }
}
