using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog.Context;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq;

/// <summary>
/// Owns one logical RabbitMQ consumer session for one backbone queue.
/// </summary>
internal sealed partial class RabbitMqBackboneConsumerSession : IRabbitMqConsumerSession
{
    private readonly RabbitMqConsumerSessionIdentity _identity;
    private readonly string _queueName;
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly RabbitMqTopologyInitializer _topologyInitializer;
    private readonly IRabbitMqDeliverySink _deliverySink;
    private readonly ILogger<RabbitMqBackboneConsumerSession> _logger;
    private readonly ushort? _prefetchCount;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);

    private RabbitMqOwnedChannel? _ownedChannel;
    private AsyncEventingBasicConsumer? _consumer;
    private string? _consumerTag;
    private CancellationTokenSource? _sessionCancellation;
    private long _activeConnectionGeneration;
    private bool _running;
    private bool _disposed;
    private IDisposable? _connectionScope;

    internal RabbitMqBackboneConsumerSession(
        RabbitMqConsumerSessionIdentity identity,
        RabbitMqConnectionManager connectionManager,
        RabbitMqTopologyInitializer topologyInitializer,
        IRabbitMqDeliverySink deliverySink,
        ILogger<RabbitMqBackboneConsumerSession> logger,
        ushort? prefetchCount)
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
    }

    internal RabbitMqConsumerSessionIdentity Identity => _identity;

    RabbitMqConsumerSessionIdentity IRabbitMqConsumerSession.Identity => _identity;

    internal bool IsRunning => _running;

    bool IRabbitMqConsumerSession.IsRunning => _running;

    internal long ActiveConnectionGeneration => Interlocked.Read(ref _activeConnectionGeneration);

    long IRabbitMqConsumerSession.ActiveConnectionGeneration => ActiveConnectionGeneration;

    internal async Task HandleConnectionReplacedAsync(RabbitMqConnectionReplacedEventArgs args, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);

        if (_disposed)
        {
            return;
        }

        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                return;
            }

            if (!_running)
            {
                return;
            }

            long currentGeneration = ActiveConnectionGeneration;
            if (args.ConnectionGeneration <= currentGeneration)
            {
                return;
            }

            LogConsumerRecreationStarting(_logger, _identity.Backbone, _identity.SessionOrdinal, currentGeneration, args.ConnectionGeneration);
            await StopCoreAsync(CancellationToken.None, expectedShutdown: false).ConfigureAwait(false);
            await StartCoreAsync(cancellationToken).ConfigureAwait(false);
            LogConsumerRecreationCompleted(_logger, _identity.Backbone, _identity.SessionOrdinal, ActiveConnectionGeneration);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (_running)
            {
                return;
            }

            await StartCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await StopCoreAsync(cancellationToken, expectedShutdown: true).ConfigureAwait(false);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

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
            await StopCoreAsync(CancellationToken.None, expectedShutdown: true).ConfigureAwait(false);
        }
        finally
        {
            _lifecycleGate.Release();
            _lifecycleGate.Dispose();
        }
    }

    private async Task StartCoreAsync(CancellationToken cancellationToken)
    {
        await _connectionManager.EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
        await _topologyInitializer.InitializeAsync(_identity.ServerId, [_identity.Backbone], cancellationToken).ConfigureAwait(false);

        RabbitMqOwnedChannel owned = await _connectionManager.CreateOwnedChannelAsync($"rabbitmq-consumer:{_identity.SessionKey}", cancellationToken).ConfigureAwait(false);
        _ownedChannel = owned;
        _activeConnectionGeneration = owned.ConnectionGeneration;

        if (_prefetchCount.HasValue)
        {
            await _ownedChannel.Channel.BasicQosAsync(0u, _prefetchCount.Value, false, cancellationToken).ConfigureAwait(false);
            LogConsumerPrefetchConfigured(_logger, _identity.Backbone, _identity.SessionOrdinal, _prefetchCount.Value);
        }

        _sessionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        AsyncEventingBasicConsumer consumer = new(_ownedChannel.Channel.UnderlyingChannel);
        consumer.ReceivedAsync += OnReceivedAsync;
        consumer.ShutdownAsync += OnConsumerShutdownAsync;
        consumer.UnregisteredAsync += OnConsumerUnregisteredAsync;
        _consumer = consumer;

        _connectionScope = BeginConnectionScope();

        try
        {
            string consumerTag = await _ownedChannel.Channel.BasicConsumeAsync(
                queue: _queueName,
                autoAck: false,
                consumer: consumer,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _consumerTag = consumerTag;
            _running = true;

            LogConsumerStarted(_logger, _identity.Backbone, _identity.SessionOrdinal, _queueName, _activeConnectionGeneration, consumerTag);
        }
        catch
        {
            consumer.ReceivedAsync -= OnReceivedAsync;
            consumer.ShutdownAsync -= OnConsumerShutdownAsync;
            consumer.UnregisteredAsync -= OnConsumerUnregisteredAsync;
            _consumer = null;
            _connectionScope?.Dispose();
            _connectionScope = null;

            _sessionCancellation?.Cancel();
            _sessionCancellation?.Dispose();
            _sessionCancellation = null;

            if (_ownedChannel is not null)
            {
                await _ownedChannel.DisposeAsync().ConfigureAwait(false);
                _ownedChannel = null;
            }

            Interlocked.Exchange(ref _activeConnectionGeneration, 0);
            throw;
        }
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken, bool expectedShutdown)
    {
        if (!_running)
        {
            return;
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

        if (_consumer is not null)
        {
            _consumer.ReceivedAsync -= OnReceivedAsync;
            _consumer.ShutdownAsync -= OnConsumerShutdownAsync;
            _consumer.UnregisteredAsync -= OnConsumerUnregisteredAsync;
            _consumer = null;
        }

        _sessionCancellation?.Cancel();
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
        _running = false;
        Interlocked.Exchange(ref _activeConnectionGeneration, 0);

        _connectionScope?.Dispose();
        _connectionScope = null;

        if (expectedShutdown)
        {
            LogConsumerStopped(_logger, _identity.Backbone, _identity.SessionOrdinal);
        }
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        if (!_running)
        {
            return;
        }

        CancellationToken cancellationToken = _sessionCancellation?.Token ?? CancellationToken.None;

        RabbitMqArticleDelivery delivery = new(
            Backbone: _identity.Backbone,
            Queue: _queueName,
            ConsumerTag: args.ConsumerTag,
            DeliveryTag: args.DeliveryTag,
            Redelivered: args.Redelivered,
            RoutingKey: args.RoutingKey,
            Exchange: args.Exchange,
            ConnectionGeneration: ActiveConnectionGeneration,
            Payload: args.Body,
            CancellationToken: cancellationToken);

        try
        {
            await _deliverySink.OnDeliveryAsync(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private Task OnConsumerShutdownAsync(object sender, ShutdownEventArgs args)
    {
        LogConsumerShutdownObserved(_logger, _identity.Backbone, _identity.SessionOrdinal, args.ReplyCode, args.ReplyText, args.Initiator.ToString());
        return Task.CompletedTask;
    }

    private async Task OnConsumerUnregisteredAsync(object sender, ConsumerEventArgs args)
    {
        if (_disposed)
        {
            return;
        }

        int consumerTagCount = args.ConsumerTags.Count;
        LogConsumerCancellationObserved(_logger, _identity.Backbone, _identity.SessionOrdinal, consumerTagCount);

        await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (_disposed || !_running)
            {
                return;
            }

            await StopCoreAsync(CancellationToken.None, expectedShutdown: false).ConfigureAwait(false);
            await StartCoreAsync(CancellationToken.None).ConfigureAwait(false);
            LogConsumerRecreationCompleted(_logger, _identity.Backbone, _identity.SessionOrdinal, ActiveConnectionGeneration);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

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

    private sealed class CompositeDisposable(IReadOnlyList<IDisposable> scopes) : IDisposable
    {
        private readonly IReadOnlyList<IDisposable> _scopes = scopes ?? throw new ArgumentNullException(nameof(scopes));

        public void Dispose()
        {
            for (int i = _scopes.Count - 1; i >= 0; i--)
            {
                _scopes[i].Dispose();
            }
        }
    }

    private static string BuildConnectionPrefix(string backbone, string accountUsername, int connectionNumber, int connectionLimit)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
        ArgumentException.ThrowIfNullOrWhiteSpace(accountUsername);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectionNumber, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectionLimit, 0);

        int width = Math.Max(3, connectionLimit.ToString(System.Globalization.CultureInfo.InvariantCulture).Length);
        return $"{backbone}/{accountUsername}[{connectionNumber.ToString($"D{width}", System.Globalization.CultureInfo.InvariantCulture)}/{connectionLimit.ToString($"D{width}", System.Globalization.CultureInfo.InvariantCulture)}]: ";
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMqBackboneConsumerSession));
        }
    }

    private static void LogConsumerStarted(ILogger logger, string backbone, int sessionOrdinal, string queue, long connectionGeneration, string consumerTag)
    {
        logger.LogInformation("RabbitMQ consumer session started. Backbone={Backbone} Session={SessionOrdinal} Queue={Queue} ConnectionGeneration={ConnectionGeneration} ConsumerTag={ConsumerTag}", backbone, sessionOrdinal, queue, connectionGeneration, consumerTag);
    }

    private static void LogConsumerStopped(ILogger logger, string backbone, int sessionOrdinal)
    {
        logger.LogInformation("RabbitMQ consumer session stopped. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
    }

    private static void LogConsumerShutdownObserved(ILogger logger, string backbone, int sessionOrdinal, ushort replyCode, string replyText, string initiator)
    {
        logger.LogWarning("RabbitMQ consumer shutdown observed. Backbone={Backbone} Session={SessionOrdinal} ReplyCode={ReplyCode} ReplyText={ReplyText} Initiator={Initiator}", backbone, sessionOrdinal, replyCode, replyText, initiator);
    }

    private static void LogConsumerRecreationStarting(ILogger logger, string backbone, int sessionOrdinal, long previousGeneration, long newGeneration)
    {
        logger.LogInformation("RabbitMQ consumer recreation starting due to connection replacement. Backbone={Backbone} Session={SessionOrdinal} PreviousGeneration={PreviousGeneration} NewGeneration={NewGeneration}", backbone, sessionOrdinal, previousGeneration, newGeneration);
    }

    private static void LogConsumerRecreationCompleted(ILogger logger, string backbone, int sessionOrdinal, long activeGeneration)
    {
        logger.LogInformation("RabbitMQ consumer recreation completed. Backbone={Backbone} Session={SessionOrdinal} ActiveGeneration={ActiveGeneration}", backbone, sessionOrdinal, activeGeneration);
    }

    private static void LogConsumerCancelDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason)
    {
        logger.LogDebug("RabbitMQ consumer cancellation during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}", backbone, sessionOrdinal, reason);
    }

    private static void LogConsumerChannelDisposeDuringShutdownFailed(ILogger logger, string backbone, int sessionOrdinal, string reason)
    {
        logger.LogDebug("RabbitMQ consumer channel dispose during shutdown encountered error. Backbone={Backbone} Session={SessionOrdinal} Reason={Reason}", backbone, sessionOrdinal, reason);
    }

    private static void LogConsumerPrefetchConfigured(ILogger logger, string backbone, int sessionOrdinal, ushort prefetchCount)
    {
        logger.LogInformation("RabbitMQ consumer prefetch configured. Backbone={Backbone} Session={SessionOrdinal} PrefetchCount={PrefetchCount}", backbone, sessionOrdinal, prefetchCount);
    }

    private static void LogConsumerCancellationFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
    {
        logger.LogError(exception, "RabbitMQ consumer cancellation failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
    }

    private static void LogConsumerChannelDisposeFailed(ILogger logger, string backbone, int sessionOrdinal, Exception exception)
    {
        logger.LogError(exception, "RabbitMQ consumer channel disposal failed unexpectedly. Backbone={Backbone} Session={SessionOrdinal}", backbone, sessionOrdinal);
    }

    private static void LogConsumerCancellationObserved(ILogger logger, string backbone, int sessionOrdinal, int consumerTagCount)
    {
        logger.LogWarning("RabbitMQ consumer unregistered by broker. Backbone={Backbone} Session={SessionOrdinal} ConsumerTagCount={ConsumerTagCount}", backbone, sessionOrdinal, consumerTagCount);
    }
}
