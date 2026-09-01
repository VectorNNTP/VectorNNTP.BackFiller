// <copyright file="RabbitMqTopology.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using System.Collections.Frozen;
using RabbitMQ.Client;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// RabbitMQ topology declaration contract for one backbone namespace.
    /// </summary>
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
    /// Builds backbone-isolated RabbitMQ topology definitions.
    /// </summary>
    internal static class RabbitMqTopologyBuilder
    {
        /// <summary>
        /// Builds unique Backbone-scoped topology definitions.
        /// </summary>
        /// <remarks>
        /// Legacy Grabber compatibility model: exchange and queue are both named grabbers.{backbone.ToLowerInvariant()}.
        /// ServerId and BackFiller instance identity do not participate in article-work topology identity.
        /// </remarks>
        /// <param name="serverId">BackFiller server identifier (retained for call-site compatibility; topology identity is Backbone-only).</param>
        /// <param name="backbones">Configured account backbones.</param>
        /// <returns>Immutable topology definitions by backbone.</returns>
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

            return [.. definitions.ToFrozenSet().OrderBy(static x => x.Backbone, StringComparer.OrdinalIgnoreCase)];
        }

        private static string BuildLegacyBackboneEntityName(string backbone)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
            return $"grabbers.{backbone.Trim().ToLowerInvariant()}";
        }
    }

    /// <summary>
    /// Declares RabbitMQ topology for configured backbones.
    /// </summary>
    internal sealed partial class RabbitMqTopologyInitializer(
        RabbitMqConnectionManager connectionManager,
        ILogger<RabbitMqTopologyInitializer> logger)
    {
        private readonly RabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        private readonly ILogger<RabbitMqTopologyInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        private readonly SemaphoreSlim _initializationGate = new(1, 1);
        private readonly HashSet<string> _declaredTopologyKeys = new(StringComparer.Ordinal);
        private long _declaredTopologyGeneration;

        /// <summary>
        /// Declares all required RabbitMQ topology for configured backbones.
        /// </summary>
        /// <param name="serverId">BackFiller server identifier.</param>
        /// <param name="backbones">Backbone names to isolate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when all declarations succeed.</returns>
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

        private static string BuildTopologyDeclarationKey(string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
            return queueName;
        }

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

            await using RabbitMqOwnedChannel ownedChannel = await _connectionManager
                .CreateOwnedChannelAsync($"topology:{definition.Backbone}", cancellationToken)
                .ConfigureAwait(false);

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

            LogBackboneTopologyInitializationCompleted(
                _logger,
                definition.Backbone,
                definition.ExchangeName,
                definition.QueueName,
                definition.RoutingKey);
        }

        [LoggerMessage(EventId = 4100, Level = LogLevel.Information, Message = "RabbitMQ topology initialization started. BackboneCount={BackboneCount}")]
        private static partial void LogTopologyInitializationStarted(ILogger logger, int backboneCount);

        [LoggerMessage(EventId = 4101, Level = LogLevel.Information, Message = "RabbitMQ topology initialization completed. BackboneCount={BackboneCount}")]
        private static partial void LogTopologyInitializationCompleted(ILogger logger, int backboneCount);

        [LoggerMessage(EventId = 4102, Level = LogLevel.Information, Message = "RabbitMQ backbone topology initialization started. Backbone={Backbone} Exchange={Exchange} Queue={Queue} RoutingKey={RoutingKey}")]
        private static partial void LogBackboneTopologyInitializationStarted(ILogger logger, string backbone, string exchange, string queue, string routingKey);

        [LoggerMessage(EventId = 4103, Level = LogLevel.Information, Message = "RabbitMQ backbone topology initialization completed. Backbone={Backbone} Exchange={Exchange} Queue={Queue} RoutingKey={RoutingKey}")]
        private static partial void LogBackboneTopologyInitializationCompleted(ILogger logger, string backbone, string exchange, string queue, string routingKey);
    }
}
