// <copyright file="RabbitMqStartupInitializer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq startup initializer behavior.

using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Accounts;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Startup initializer that establishes RabbitMQ connectivity and topology before runtime execution continues.
    /// </summary>
    /// <remarks>
    /// Phase 1 scope: connection and topology only. No message consumption or message processing starts here.
    /// </remarks>
    internal sealed partial class RabbitMqStartupInitializer(
        RabbitMqConnectionManager connectionManager,
        RabbitMqTopologyInitializer topologyInitializer,
        MySqlNntpAccountSnapshotProvider accountSnapshotProvider,
        BackFillerRuntimeOptions runtimeOptions,
        ILogger<RabbitMqStartupInitializer> logger) : IHostedService
    {
        private readonly RabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        private readonly RabbitMqTopologyInitializer _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));
        /// <summary>
        /// Limits account snapshot provider for rabbit mq startup initializer.
        /// </summary>
        private readonly MySqlNntpAccountSnapshotProvider _accountSnapshotProvider = accountSnapshotProvider ?? throw new ArgumentNullException(nameof(accountSnapshotProvider));
        private readonly BackFillerRuntimeOptions _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));
        /// <summary>
        /// Supplies the logger used by rabbit mq startup initializer.
        /// </summary>
        private readonly ILogger<RabbitMqStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Establishes RabbitMQ connection and declares backbone-specific topology.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task that completes when RabbitMQ infrastructure is ready.</returns>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            LogStartupInitializationBeginning(_logger);

            await _connectionManager.EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);

            IReadOnlyList<string> backbones = [.. _accountSnapshotProvider.CurrentSnapshot.Accounts
                .Select(static account => account.Backbone)
                .Where(static backbone => !string.IsNullOrWhiteSpace(backbone))
                .Distinct(StringComparer.OrdinalIgnoreCase)];

            await _topologyInitializer
                .InitializeAsync(_runtimeOptions.BackFillerId, backbones, cancellationToken)
                .ConfigureAwait(false);

            LogStartupInitializationCompleted(_logger, _connectionManager.State, backbones.Count);
        }

        /// <summary>
        /// Stops RabbitMQ recovery activity and disposes the RabbitMQ connection manager.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token.</param>
        /// <returns>A task that completes after shutdown/disposal is complete.</returns>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _connectionManager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Emits the startup initialization beginning log event for rabbit mq startup initializer.
        /// </summary>
        [LoggerMessage(EventId = 4200, Level = LogLevel.Information, Message = "RabbitMQ startup initializer beginning infrastructure initialization")]
        private static partial void LogStartupInitializationBeginning(ILogger logger);

        /// <summary>
        /// Emits the startup initialization completed log event for rabbit mq startup initializer.
        /// </summary>
        [LoggerMessage(EventId = 4201, Level = LogLevel.Information, Message = "RabbitMQ startup initializer completed. State={State} BackboneCount={BackboneCount}")]
        private static partial void LogStartupInitializationCompleted(ILogger logger, RabbitMqInfrastructureState state, int backboneCount);
    }
}

