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
    /// Hosted-service initializer that establishes RabbitMQ connectivity and declares required topology before runtime work begins.
    /// </summary>
    /// <remarks>
    /// This startup phase stops after connection and topology readiness. It does not start consumer reconciliation or
    /// article processing; later hosted services depend on the readiness established here.
    /// </remarks>
    internal sealed partial class RabbitMqStartupInitializer(
        RabbitMqConnectionManager connectionManager,
        RabbitMqTopologyInitializer topologyInitializer,
        MySqlNntpAccountSnapshotProvider accountSnapshotProvider,
        BackFillerRuntimeOptions runtimeOptions,
        ILogger<RabbitMqStartupInitializer> logger) : IHostedService
    {
        /// <summary>
        /// Connection manager responsible for initial broker connectivity and recovery ownership.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));

        /// <summary>
        /// Topology initializer that declares backbone exchanges, queues, and bindings.
        /// </summary>
        private readonly RabbitMqTopologyInitializer _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));

        /// <summary>
        /// Authoritative account snapshot provider used to discover backbone names at startup.
        /// </summary>
        private readonly MySqlNntpAccountSnapshotProvider _accountSnapshotProvider = accountSnapshotProvider ?? throw new ArgumentNullException(nameof(accountSnapshotProvider));

        /// <summary>
        /// Validated runtime options used to supply the BackFiller server identifier.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions = runtimeOptions ?? throw new ArgumentNullException(nameof(runtimeOptions));

        /// <summary>
        /// Logger for startup readiness events.
        /// </summary>
        private readonly ILogger<RabbitMqStartupInitializer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// Connects to RabbitMQ and declares topology for every distinct non-empty backbone in the current account snapshot.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task that completes when RabbitMQ infrastructure is ready for later hosted services.</returns>
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
        /// Disposes RabbitMQ connection-management resources during host shutdown.
        /// </summary>
        /// <param name="cancellationToken">Shutdown cancellation token.</param>
        /// <returns>A task that completes after the connection manager has been disposed.</returns>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _connectionManager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Declares the informational log emitted when RabbitMQ startup initialization begins.
        /// </summary>
        [LoggerMessage(EventId = 4200, Level = LogLevel.Information, Message = "RabbitMQ startup initializer beginning infrastructure initialization")]
        private static partial void LogStartupInitializationBeginning(ILogger logger);

        /// <summary>
        /// Declares the informational log emitted after startup connection and topology readiness are established.
        /// </summary>
        /// <remarks>
        /// The structured payload reports the final <see cref="RabbitMqInfrastructureState"/> and the number of unique
        /// backbones whose topology was declared.
        /// </remarks>
        [LoggerMessage(EventId = 4201, Level = LogLevel.Information, Message = "RabbitMQ startup initializer completed. State={State} BackboneCount={BackboneCount}")]
        private static partial void LogStartupInitializationCompleted(ILogger logger, RabbitMqInfrastructureState state, int backboneCount);
    }
}
