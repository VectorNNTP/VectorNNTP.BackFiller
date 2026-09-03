// <copyright file="RabbitMqTopology.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq topology behavior.

using RabbitMQ.Client;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Immutable topology definition for one backbone-scoped RabbitMQ exchange, queue, and binding.
    /// </summary>
    /// <param name="Backbone">Canonical backbone name represented by this topology definition.</param>
    /// <param name="ExchangeName">Exchange name that receives article work for the backbone.</param>
    /// <param name="ExchangeType">RabbitMQ exchange type used for the backbone exchange.</param>
    /// <param name="ExchangeDurable">Indicates whether the exchange survives broker restarts.</param>
    /// <param name="ExchangeAutoDelete">Indicates whether the exchange is auto-deleted when unused.</param>
    /// <param name="QueueName">Queue name bound to the backbone exchange.</param>
    /// <param name="QueueDurable">Indicates whether the queue survives broker restarts.</param>
    /// <param name="QueueExclusive">Indicates whether the queue is exclusive to a single connection.</param>
    /// <param name="QueueAutoDelete">Indicates whether the queue is auto-deleted when unused.</param>
    /// <param name="RoutingKey">Routing key used when binding the queue to the exchange.</param>
    /// <param name="QueueArguments">Optional queue arguments applied during declaration.</param>
    /// <param name="ExchangeArguments">Optional exchange arguments applied during declaration.</param>
    /// <param name="BindingArguments">Optional binding arguments applied during declaration.</param>
    internal sealed record RabbitMqBackboneTopologyDefinition(
        string Backbone,
        string ExchangeName,
        string ExchangeType,
        bool ExchangeDurable,
        bool ExchangeAutoDelete,
        string QueueName,
        bool QueueDurable,
        bool QueueExclusive,
        bool QueueAutoDelete,
        string RoutingKey,
        IReadOnlyDictionary<string, object?>? QueueArguments = null,
        IReadOnlyDictionary<string, object?>? ExchangeArguments = null,
        IReadOnlyDictionary<string, object?>? BindingArguments = null);

    /// <summary>
    /// Builds the canonical RabbitMQ topology definitions used by BackFiller consumer infrastructure.
    /// </summary>
    internal static class RabbitMqTopologyBuilder
    {
        /// <summary>
        /// Builds the distinct backbone topology definitions required for the supplied account backbones.
        /// </summary>
        /// <param name="serverId">BackFiller server identifier retained for caller compatibility.</param>
        /// <param name="backbones">Backbone names discovered from the authoritative account snapshot.</param>
        /// <returns>Topology definitions ordered by backbone name with duplicates and blank entries removed.</returns>
        /// <remarks>
        /// Topology identity is backbone-only. Existing compatibility rules require both the exchange and queue to use the
        /// legacy name <c>grabbers.{backbone.ToLowerInvariant()}</c>, independent of server identifier.
        /// </remarks>
        internal static IReadOnlyList<RabbitMqBackboneTopologyDefinition> BuildDefinitions(
            int serverId,
            IEnumerable<string> backbones)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(serverId);
            ArgumentNullException.ThrowIfNull(backbones);

            HashSet<string> uniqueBackbones = new(StringComparer.OrdinalIgnoreCase);
            List<RabbitMqBackboneTopologyDefinition> definitions = [];

            foreach (string backbone in backbones)
            {
                if (string.IsNullOrWhiteSpace(backbone))
                {
                    continue;
                }

                string canonicalBackbone = backbone.Trim();
                if (!uniqueBackbones.Add(canonicalBackbone))
                {
                    continue;
                }

                string topologyEntityName = BuildLegacyBackboneEntityName(canonicalBackbone);

                definitions.Add(new RabbitMqBackboneTopologyDefinition(
                    Backbone: canonicalBackbone,
                    ExchangeName: topologyEntityName,
                    ExchangeType: ExchangeType.Fanout,
                    ExchangeDurable: true,
                    ExchangeAutoDelete: false,
                    QueueName: topologyEntityName,
                    QueueDurable: true,
                    QueueExclusive: false,
                    QueueAutoDelete: false,
                    RoutingKey: topologyEntityName,
                    QueueArguments: new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["x-queue-type"] = "quorum",
                    }));
            }

            return [.. definitions.OrderBy(static x => x.Backbone, StringComparer.OrdinalIgnoreCase)];
        }

        /// <summary>
        /// Builds the legacy queue and exchange name used for a backbone-scoped article-work topology.
        /// </summary>
        /// <param name="backbone">Canonical backbone name.</param>
        /// <returns>The lower-cased legacy entity name in the form <c>grabbers.{backbone}</c>.</returns>
        private static string BuildLegacyBackboneEntityName(string backbone)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
            return $"grabbers.{backbone.Trim().ToLowerInvariant()}";
        }
    }

    /// <summary>
    /// Declares RabbitMQ exchanges, queues, and bindings for the currently configured backbones.
    /// </summary>
    /// <remarks>
    /// Declaration is serialized so repeated initialization calls remain idempotent for the active connection
    /// generation. When the connection generation changes, previously remembered declaration keys are discarded and the
    /// topology is declared again on the replacement connection.
    /// </remarks>
    internal sealed partial class RabbitMqTopologyInitializer(
        RabbitMqConnectionManager connectionManager,
        ILogger<RabbitMqTopologyInitializer> logger) : IDisposable
    {
        /// <summary>
        /// Connection manager that supplies ready connections and channel leases for topology work.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));

        /// <summary>
        /// Logger for topology initialization events.
        /// </summary>
        private readonly ILogger<RabbitMqTopologyInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Serializes topology declaration across concurrent callers.
        /// </summary>
        private readonly SemaphoreSlim _initializationGate = new(1, 1);

        /// <summary>
        /// Tracks queue-based declaration keys already applied for the active connection generation.
        /// </summary>
        private readonly HashSet<string> _declaredTopologyKeys = new(StringComparer.Ordinal);

        /// <summary>
        /// Connection generation against which <see cref="_declaredTopologyKeys"/> was recorded.
        /// </summary>
        private long _declaredTopologyGeneration;

        /// <summary>
        /// Ensures disposal of the initialization gate is idempotent.
        /// </summary>
        private int _disposeSignaled;

        /// <summary>
        /// Declares RabbitMQ topology for the supplied backbone set on the current active connection generation.
        /// </summary>
        /// <param name="serverId">BackFiller server identifier forwarded to topology-definition construction.</param>
        /// <param name="backbones">Backbone names whose topology must exist.</param>
        /// <param name="cancellationToken">Cancellation token for the declaration workflow.</param>
        /// <returns>A task that completes after the required declarations succeed and the connection manager is marked topology-ready.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no active connection generation is available for topology declaration.</exception>
        internal async Task InitializeAsync(
            int serverId,
            IReadOnlyList<string> backbones,
            CancellationToken cancellationToken)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(serverId);
            ArgumentNullException.ThrowIfNull(backbones);

            IReadOnlyList<RabbitMqBackboneTopologyDefinition> definitions = RabbitMqTopologyBuilder.BuildDefinitions(serverId, backbones);

            LogTopologyInitializationStarted(_logger, definitions.Count);

            await _initializationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                long generation = _connectionManager.ConnectionGeneration;
                if (generation <= 0)
                {
                    throw new InvalidOperationException("RabbitMQ connection generation is not available for topology initialization.");
                }

                if (_declaredTopologyGeneration != generation)
                {
                    _declaredTopologyKeys.Clear();
                    _declaredTopologyGeneration = generation;
                }

                int declaredCount = 0;
                for (int i = 0; i < definitions.Count; i++)
                {
                    RabbitMqBackboneTopologyDefinition definition = definitions[i];
                    string declarationKey = BuildTopologyDeclarationKey(definition.QueueName);
                    if (_declaredTopologyKeys.Contains(declarationKey))
                    {
                        continue;
                    }

                    await DeclareBackboneTopologyAsync(definition, cancellationToken).ConfigureAwait(false);
                    _ = _declaredTopologyKeys.Add(declarationKey);
                    declaredCount++;
                }

                _connectionManager.MarkTopologyReady();
                LogTopologyInitializationCompleted(_logger, declaredCount);
            }
            finally
            {
                _ = _initializationGate.Release();
            }
        }

        /// <summary>
        /// Builds the idempotency key used to remember a declared topology entry within one connection generation.
        /// </summary>
        /// <param name="queueName">Queue name that identifies the declared backbone topology.</param>
        /// <returns>The declaration key stored in the per-generation declaration cache.</returns>
        private static string BuildTopologyDeclarationKey(string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
            return queueName;
        }

        /// <summary>
        /// Declares one backbone exchange, queue, and binding on a dedicated channel lease.
        /// </summary>
        /// <param name="definition">Topology definition to declare.</param>
        /// <param name="cancellationToken">Cancellation token for broker operations.</param>
        /// <returns>A task that completes after the exchange, queue, and binding have been declared.</returns>
        private async Task DeclareBackboneTopologyAsync(
            RabbitMqBackboneTopologyDefinition definition,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(definition);

            LogBackboneTopologyInitializationStarted(
                _logger,
                definition.Backbone,
                definition.ExchangeName,
                definition.QueueName,
                definition.RoutingKey);

            RabbitMqOwnedChannel ownedChannel = await _connectionManager
                .CreateOwnedChannelAsync($"topology:{definition.Backbone}", cancellationToken)
                .ConfigureAwait(false);

            await using (ownedChannel.ConfigureAwait(false))
            {
                await ownedChannel.Channel.ExchangeDeclareAsync(
                    exchange: definition.ExchangeName,
                    type: definition.ExchangeType,
                    durable: definition.ExchangeDurable,
                    autoDelete: definition.ExchangeAutoDelete,
                    arguments: definition.ExchangeArguments,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                await ownedChannel.Channel.QueueDeclareAsync(
                    queue: definition.QueueName,
                    durable: definition.QueueDurable,
                    exclusive: definition.QueueExclusive,
                    autoDelete: definition.QueueAutoDelete,
                    arguments: definition.QueueArguments,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                await ownedChannel.Channel.QueueBindAsync(
                    queue: definition.QueueName,
                    exchange: definition.ExchangeName,
                    routingKey: definition.RoutingKey,
                    arguments: definition.BindingArguments,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            LogBackboneTopologyInitializationCompleted(
                _logger,
                definition.Backbone,
                definition.ExchangeName,
                definition.QueueName,
                definition.RoutingKey);
        }

        /// <summary>
        /// Disposes synchronization resources owned by the topology initializer.
        /// </summary>
        void IDisposable.Dispose()
        {
            if (Interlocked.Exchange(ref _disposeSignaled, 1) != 0)
            {
                return;
            }

            _initializationGate.Dispose();
        }

        /// <summary>
        /// Declares the informational log emitted when topology initialization begins.
        /// </summary>
        [LoggerMessage(EventId = 4100, Level = LogLevel.Information, Message = "RabbitMQ topology initialization started. BackboneCount={BackboneCount}")]
        private static partial void LogTopologyInitializationStarted(ILogger logger, int backboneCount);

        /// <summary>
        /// Declares the informational log emitted after topology initialization finishes.
        /// </summary>
        [LoggerMessage(EventId = 4101, Level = LogLevel.Information, Message = "RabbitMQ topology initialization completed. BackboneCount={BackboneCount}")]
        private static partial void LogTopologyInitializationCompleted(ILogger logger, int backboneCount);

        /// <summary>
        /// Declares the informational log emitted before one backbone topology is declared.
        /// </summary>
        [LoggerMessage(EventId = 4102, Level = LogLevel.Information, Message = "RabbitMQ backbone topology initialization started. Backbone={Backbone} Exchange={Exchange} Queue={Queue} RoutingKey={RoutingKey}")]
        private static partial void LogBackboneTopologyInitializationStarted(ILogger logger, string backbone, string exchange, string queue, string routingKey);

        /// <summary>
        /// Declares the informational log emitted after one backbone topology is declared.
        /// </summary>
        [LoggerMessage(EventId = 4103, Level = LogLevel.Information, Message = "RabbitMQ backbone topology initialization completed. Backbone={Backbone} Exchange={Exchange} Queue={Queue} RoutingKey={RoutingKey}")]
        private static partial void LogBackboneTopologyInitializationCompleted(ILogger logger, string backbone, string exchange, string queue, string routingKey);
    }
}
