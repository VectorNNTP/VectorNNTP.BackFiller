// <copyright file="RabbitMqBackboneConsumerSession.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq backbone consumer session responsibilities for this subsystem boundary.

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
        /// Stores the identity state used to enforce this component's runtime contract.
        /// </summary>
        private readonly RabbitMqConsumerSessionIdentity _identity;
        /// <summary>
        /// Stores the queue name state used to enforce this component's runtime contract.
        /// </summary>
        private readonly string _queueName;
        /// <summary>
        /// Stores the connection manager state used to enforce this component's runtime contract.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager;
        /// <summary>
        /// Stores the topology initializer state used to enforce this component's runtime contract.
        /// </summary>
        private readonly RabbitMqTopologyInitializer _topologyInitializer;
        /// <summary>
        /// Stores the delivery sink state used to enforce this component's runtime contract.
        /// </summary>
        private readonly IRabbitMqDeliverySink _deliverySink;
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<RabbitMqBackboneConsumerSession> _logger;
        /// <summary>
        /// Stores the prefetch count state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ushort? _prefetchCount;
        /// <summary>
        /// Stores the diagnostic correlation id state used to enforce this component's runtime contract.
        /// </summary>
        private readonly string? _diagnosticCorrelationId;
        /// <summary>
        /// Stores the lifecycle gate state used to enforce this component's runtime contract.
        /// </summary>
        private readonly SemaphoreSlim _lifecycleGate = new(1, 1);

        /// <summary>
        /// Stores the owned channel state used to enforce this component's runtime contract.
        /// </summary>
        private RabbitMqOwnedChannel? _ownedChannel;
        /// <summary>
        /// Stores the consumer state used to enforce this component's runtime contract.
        /// </summary>
        private AsyncEventingBasicConsumer? _consumer;
        /// <summary>
        /// Stores the consumer tag state used to enforce this component's runtime contract.
        /// </summary>
        private string? _consumerTag;
        /// <summary>
        /// Stores the session cancellation state used to enforce this component's runtime contract.
        /// </summary>
        private CancellationTokenSource? _sessionCancellation;
        /// <summary>
        /// Stores the drain completion state used to enforce this component's runtime contract.
        /// </summary>
        private TaskCompletionSource<bool> _drainCompletion = CreateCompletedDrainSource();
        /// <summary>
        /// Stores the active connection generation state used to enforce this component's runtime contract.
        /// </summary>
        private long _activeConnectionGeneration;
        /// <summary>
        /// Stores the admitted delivery count state used to enforce this component's runtime contract.
        /// </summary>
        private int _admittedDeliveryCount;
        /// <summary>
        /// Stores the lifecycle state state used to enforce this component's runtime contract.
        /// </summary>
        private RabbitMqConsumerLifecycleState _lifecycleState = RabbitMqConsumerLifecycleState.Stopped;
        /// <summary>
        /// Stores the disposed state used to enforce this component's runtime contract.
        /// </summary>
        private bool _disposed;
        /// <summary>
        /// Stores the connection scope state used to enforce this component's runtime contract.
        /// </summary>
        private IDisposable? _connectionScope;

        /// <summary>
        /// Defines the rabbit mq consumer lifecycle state component and its contracts for this subsystem.
        /// </summary>
        private enum RabbitMqConsumerLifecycleState
        {
            Running,
            Retiring,
            Stopped,
        }

        /// <summary>
        /// Performs the rabbit mq backbone consumer session operation while preserving this component's lifecycle and state contracts.
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
        /// Stores the identity state used to enforce this component's runtime contract.
        /// </summary>
        internal RabbitMqConsumerSessionIdentity Identity => _identity;

        RabbitMqConsumerSessionIdentity IRabbitMqConsumerSession.Identity => _identity;

        /// <summary>
        /// Stores the is running state used to enforce this component's runtime contract.
        /// </summary>
        internal bool IsRunning => _lifecycleState is RabbitMqConsumerLifecycleState.Running;

        bool IRabbitMqConsumerSession.IsRunning => _lifecycleState is RabbitMqConsumerLifecycleState.Running;

        /// <summary>
        /// Stores the active connection generation state used to enforce this component's runtime contract.
        /// </summary>
        internal long ActiveConnectionGeneration => Interlocked.Read(ref _activeConnectionGeneration);

        long IRabbitMqConsumerSession.ActiveConnectionGeneration => ActiveConnectionGeneration;

        /// <summary>
        /// Performs the handle connection replaced operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the start operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the stop operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the dispose operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the start core operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the stop core operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the on received operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the on consumer shutdown operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the on consumer unregistered operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the recreate consumer core operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the is active consumer stale for current connection operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the is event from active consumer operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private bool IsEventFromActiveConsumer(object sender)
        {
            return _consumer is not null && ReferenceEquals(sender, _consumer);
        }

        /// <summary>
        /// Performs the on admitted delivery settled operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the create completed drain source operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static TaskCompletionSource<bool> CreateCompletedDrainSource()
        {
            TaskCompletionSource<bool> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = source.TrySetResult(true);
            return source;
        }

        /// <summary>
        /// Defines the rabbit mq admitted delivery tracker component and its contracts for this subsystem.
        /// </summary>
        private sealed class RabbitMqAdmittedDeliveryTracker : IRabbitMqAdmittedDeliveryTracker
        {
            /// <summary>
            /// Stores the owner state used to enforce this component's runtime contract.
            /// </summary>
            private readonly RabbitMqBackboneConsumerSession _owner;
            /// <summary>
            /// Stores the completed state used to enforce this component's runtime contract.
            /// </summary>
            private int _completed;

            /// <summary>
            /// Performs the rabbit mq admitted delivery tracker operation while preserving this component's lifecycle and state contracts.
            /// </summary>
            internal RabbitMqAdmittedDeliveryTracker(RabbitMqBackboneConsumerSession owner)
            {
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            }

            /// <summary>
            /// Performs the mark settled operation while preserving this component's lifecycle and state contracts.
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
        /// Defines the rabbit mq delivery settlement component and its contracts for this subsystem.
        /// </summary>
        private sealed class RabbitMqDeliverySettlement : IRabbitMqDeliverySettlement
        {
            /// <summary>
            /// Stores the owner state used to enforce this component's runtime contract.
            /// </summary>
            private readonly RabbitMqBackboneConsumerSession _owner;
            /// <summary>
            /// Stores the delivery tag state used to enforce this component's runtime contract.
            /// </summary>
            private readonly ulong _deliveryTag;
            /// <summary>
            /// Stores the delivery generation state used to enforce this component's runtime contract.
            /// </summary>
            private readonly long _deliveryGeneration;
            /// <summary>
            /// Stores the admission tracker state used to enforce this component's runtime contract.
            /// </summary>
            private readonly RabbitMqAdmittedDeliveryTracker? _admissionTracker;
            /// <summary>
            /// Stores the settled state used to enforce this component's runtime contract.
            /// </summary>
            private int _settled;

            /// <summary>
            /// Performs the rabbit mq delivery settlement operation while preserving this component's lifecycle and state contracts.
            /// </summary>
            internal RabbitMqDeliverySettlement(RabbitMqBackboneConsumerSession owner, ulong deliveryTag, long deliveryGeneration, RabbitMqAdmittedDeliveryTracker? admissionTracker)
            {
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
                _deliveryTag = deliveryTag;
                _deliveryGeneration = deliveryGeneration;
                _admissionTracker = admissionTracker;
            }

            /// <summary>
            /// Performs the ack operation while preserving this component's lifecycle and state contracts.
            /// </summary>
            public async ValueTask AckAsync(CancellationToken cancellationToken)
            {
                await SettleAsync(requeue: null, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Performs the nack operation while preserving this component's lifecycle and state contracts.
            /// </summary>
            public async ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                await SettleAsync(requeue, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Performs the settle operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the begin connection scope operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the composite disposable operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private sealed class CompositeDisposable(IReadOnlyList<IDisposable> scopes) : IDisposable
        {
            /// <summary>
            /// Stores the scopes state used to enforce this component's runtime contract.
            /// </summary>
            private readonly IReadOnlyList<IDisposable> _scopes = scopes ?? throw new ArgumentNullException(nameof(scopes));

            /// <summary>
            /// Performs the dispose operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the build connection prefix operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the throw if disposed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RabbitMqBackboneConsumerSession));
            }
        }

        /// <summary>
        /// Performs the log consumer started operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerStarted(ILogger logger, string backbone, int sessionOrdinal, string queue, long connectionGeneration, string consumerTag)
        {
            logger.LogInformation("RabbitMQ consumer session started. Backbone={Backbone} Session={SessionOrdinal} Queue={Queue} ConnectionGeneration={ConnectionGeneration} ConsumerTag={ConsumerTag}", backbone, sessionOrdinal, queue, connectionGeneration, consumerTag);
        }

        /// <summary>
        /// Performs the log consumer stopped operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerStopped(ILogger logger, string backbone, int sessionOrdinal)
        {
            logger.LogInformation("RabbitMQ consumer session stopped. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Performs the log consumer retiring operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerRetiring(ILogger logger, string backbone, int sessionOrdinal, int admittedCount)
        {
            logger.LogInformation("RabbitMQ consumer session entering retiring state. Backbone={Backbone} Session={SessionOrdinal} AdmittedDeliveries={AdmittedDeliveries}", backbone, sessionOrdinal, admittedCount);
        }

        /// <summary>
        /// Performs the log consumer drain started operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerDrainStarted(ILogger logger, string backbone, int sessionOrdinal, int admittedCount)
        {
            logger.LogInformation("RabbitMQ consumer drain started. Backbone={Backbone} Session={SessionOrdinal} PendingAdmittedDeliveries={PendingAdmittedDeliveries}", backbone, sessionOrdinal, admittedCount);
        }

        /// <summary>
        /// Performs the log consumer drain completed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerDrainCompleted(ILogger logger, string backbone, int sessionOrdinal)
        {
            logger.LogInformation("RabbitMQ consumer drain completed. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Performs the log consumer shutdown observed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerShutdownObserved(ILogger logger, string backbone, int sessionOrdinal, ushort replyCode, string replyText, string initiator)
        {
            logger.LogWarning("RabbitMQ consumer shutdown observed. Backbone={Backbone} Session={SessionOrdinal} ReplyCode={ReplyCode} ReplyText={ReplyText} Initiator={Initiator}", backbone, sessionOrdinal, replyCode, replyText, initiator);
        }

        /// <summary>
        /// Performs the log consumer recreation starting operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerRecreationStarting(ILogger logger, string backbone, int sessionOrdinal, long previousGeneration, long newGeneration)
        {
            logger.LogInformation("RabbitMQ consumer recreation starting due to connection replacement. Backbone={Backbone} Session={SessionOrdinal} PreviousGeneration={PreviousGeneration} NewGeneration={NewGeneration}", backbone, sessionOrdinal, previousGeneration, newGeneration);
        }

        /// <summary>
        /// Performs the log consumer recreation completed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerRecreationCompleted(ILogger logger, string backbone, int sessionOrdinal, long activeGeneration)
        {
            logger.LogInformation("RabbitMQ consumer recreation completed. Backbone={Backbone} Session={SessionOrdinal} ActiveGeneration={ActiveGeneration}", backbone, sessionOrdinal, activeGeneration);
        }

        /// <summary>
        /// Performs the log consumer cancel during shutdown failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerCancelDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason)
        {
            logger.LogDebug("RabbitMQ consumer cancellation during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}", backbone, sessionOrdinal, reason);
        }

        /// <summary>
        /// Performs the log consumer channel dispose during shutdown failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerChannelDisposeDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason)
        {
            logger.LogDebug("RabbitMQ consumer channel dispose during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}", backbone, sessionOrdinal, reason);
        }

        /// <summary>
        /// Performs the log consumer prefetch configured operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerPrefetchConfigured(ILogger logger, string backbone, int sessionOrdinal, ushort prefetchCount)
        {
            logger.LogInformation("RabbitMQ consumer prefetch configured. Backbone={Backbone} Session={SessionOrdinal} PrefetchCount={PrefetchCount}", backbone, sessionOrdinal, prefetchCount);
        }

        /// <summary>
        /// Performs the log consumer cancellation failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerCancellationFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
        {
            logger.LogError(exception, "RabbitMQ consumer cancellation failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Performs the log consumer channel dispose failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerChannelDisposeFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
        {
            logger.LogError(exception, "RabbitMQ consumer channel disposal failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }

        /// <summary>
        /// Performs the log consumer cancellation observed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerCancellationObserved(ILogger logger, string backbone, int sessionOrdinal, int consumerTagCount)
        {
            logger.LogWarning("RabbitMQ consumer unregistered by broker. Backbone={Backbone} Session={SessionOrdinal} ConsumerTagCount={ConsumerTagCount}", backbone, sessionOrdinal, consumerTagCount);
        }

        /// <summary>
        /// Performs the should log diagnostic payload operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private bool ShouldLogDiagnosticPayload(string? correlationId)
        {
            return !string.IsNullOrWhiteSpace(_diagnosticCorrelationId)
                && !string.IsNullOrWhiteSpace(correlationId)
                && string.Equals(_diagnosticCorrelationId, correlationId, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Performs the log payload diagnostic at callback entry operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the log delivery ignored from stale generation operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogDeliveryIgnoredFromStaleGeneration(ILogger logger, string backbone, int sessionOrdinal, long deliveryGeneration, long currentGeneration)
        {
            logger.LogDebug("RabbitMQ delivery ignored because session generation is stale. Backbone={Backbone} Session={SessionOrdinal} DeliveryGeneration={DeliveryGeneration} CurrentGeneration={CurrentGeneration}", backbone, sessionOrdinal, deliveryGeneration, currentGeneration);
        }

        /// <summary>
        /// Performs the log consumer recreation failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogConsumerRecreationFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
        {
            logger.LogError(exception, "RabbitMQ consumer recreation failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
        }
    }
}
