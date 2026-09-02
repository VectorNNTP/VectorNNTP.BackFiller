// <copyright file="NntpConnectionLogContext.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Connection-scoped logging metadata for reusable NNTP article acquisition sessions.

using Serilog.Context;

namespace VectorNNTP.Backfiller.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Immutable connection-scoped logging metadata for one NNTP acquisition session.
    /// </summary>
    /// <remarks>
    /// The context is created once per connection and reused for the full lifetime of the session so that all logs
    /// emitted while the connection is active can carry the same human-readable prefix and structured properties.
    /// </remarks>
    internal sealed class NntpConnectionLogContext
    {
        /// <summary>
        /// Stores scope properties used by nntp connection log context.
        /// </summary>
        private readonly KeyValuePair<string, object?>[] _scopeProperties;
        /// <summary>
        /// Stores connection prefix used by nntp connection log context.
        /// </summary>
        private readonly string _connectionPrefix;

        /// <summary>
        /// Initializes a new instance of the <see cref="NntpConnectionLogContext"/> class.
        /// </summary>
        /// <param name="backbone">Provider or backbone name used to group the connection.</param>
        /// <param name="accountUsername">Configured account username associated with the connection.</param>
        /// <param name="accountId">Stable identifier of the account being served.</param>
        /// <param name="serverId">Identifier of the owning NNTP server configuration.</param>
        /// <param name="host">Remote NNTP host name or address.</param>
        /// <param name="port">Remote NNTP port.</param>
        /// <param name="useSsl">Whether the connection uses SSL/TLS.</param>
        /// <param name="connectionNumber">One-based connection number within the account.</param>
        /// <param name="connectionLimit">Configured maximum number of account connections.</param>
        /// <exception cref="ArgumentException">Thrown when a required text value is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when a connection number or limit is not positive.</exception>
        public NntpConnectionLogContext(
            string backbone,
            string accountUsername,
            Guid accountId,
            byte serverId,
            string host,
            int port,
            bool useSsl,
            int connectionNumber,
            int connectionLimit)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
            ArgumentException.ThrowIfNullOrWhiteSpace(accountUsername);
            ArgumentException.ThrowIfNullOrWhiteSpace(host);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectionNumber, 0);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectionLimit, 0);

            Backbone = backbone;
            AccountUsername = accountUsername;
            AccountId = accountId;
            ServerId = serverId;
            Host = host;
            Port = port;
            UseSsl = useSsl;
            ConnectionNumber = connectionNumber;
            ConnectionLimit = connectionLimit;

            int width = Math.Max(3, connectionLimit.ToString(System.Globalization.CultureInfo.InvariantCulture).Length);
            _connectionPrefix = $"{Backbone}/{AccountUsername}[{ConnectionNumber.ToString($"D{width}", System.Globalization.CultureInfo.InvariantCulture)}/{ConnectionLimit.ToString($"D{width}", System.Globalization.CultureInfo.InvariantCulture)}]: ";
            _scopeProperties =
            [
                new KeyValuePair<string, object?>("Backbone", Backbone),
                new KeyValuePair<string, object?>("AccountUsername", AccountUsername),
                new KeyValuePair<string, object?>("AccountId", AccountId),
                new KeyValuePair<string, object?>("ServerId", ServerId),
                new KeyValuePair<string, object?>("ConnectionNumber", ConnectionNumber),
                new KeyValuePair<string, object?>("ConnectionLimit", ConnectionLimit),
                new KeyValuePair<string, object?>("ConnectionPrefix", _connectionPrefix),
                new KeyValuePair<string, object?>("ConnectionHost", Host),
                new KeyValuePair<string, object?>("ConnectionPort", Port),
                new KeyValuePair<string, object?>("ConnectionUseSsl", UseSsl),
            ];
        }

        /// <summary>
        /// Returns the provider/backbone name.
        /// </summary>
        internal string Backbone { get; }

        /// <summary>
        /// Returns the configured account username.
        /// </summary>
        internal string AccountUsername { get; }

        /// <summary>
        /// Returns the stable account identifier.
        /// </summary>
        internal Guid AccountId { get; }

        /// <summary>
        /// Returns the owning server identifier.
        /// </summary>
        internal byte ServerId { get; }

        /// <summary>
        /// Returns the remote NNTP host.
        /// </summary>
        internal string Host { get; }

        /// <summary>
        /// Returns the remote NNTP port.
        /// </summary>
        internal int Port { get; }

        /// <summary>
        /// Gets a value indicating whether SSL/TLS is enabled.
        /// </summary>
        internal bool UseSsl { get; }

        /// <summary>
        /// Returns the one-based connection number within the account.
        /// </summary>
        internal int ConnectionNumber { get; }

        /// <summary>
        /// Returns the configured maximum connection count for the account.
        /// </summary>
        internal int ConnectionLimit { get; }

        /// <summary>
        /// Returns the human-readable connection prefix rendered in logs.
        /// </summary>
        internal string ConnectionPrefix => _connectionPrefix;

        /// <summary>
        /// Returns the logging scope properties for the connection.
        /// </summary>
        internal IReadOnlyList<KeyValuePair<string, object?>> ScopeProperties => _scopeProperties;

        /// <summary>
        /// Pushes the connection properties into the current logging context.
        /// </summary>
        /// <returns>A disposable scope that removes the properties when disposed.</returns>
        internal IDisposable Push()
        {
            List<IDisposable> disposables = [];
            for (int index = 0; index < _scopeProperties.Length; index++)
            {
                KeyValuePair<string, object?> property = _scopeProperties[index];
                disposables.Add(LogContext.PushProperty(property.Key, property.Value));
            }

            return new CompositeDisposable(disposables);
        }

        /// <summary>
        /// Empty composite disposable used to unwind multiple LogContext pushes together.
        /// </summary>
        private sealed class CompositeDisposable : IDisposable
        {
            /// <summary>
            /// Stores disposables used by nntp connection log context.
            /// </summary>
            private readonly IReadOnlyList<IDisposable> _disposables;

            /// <summary>
            /// Handles composite disposable for nntp connection log context.
            /// </summary>
            internal CompositeDisposable(IReadOnlyList<IDisposable> disposables)
            {
                _disposables = disposables ?? throw new ArgumentNullException(nameof(disposables));
            }

            /// <summary>
            /// Handles dispose for nntp connection log context.
            /// </summary>
            public void Dispose()
            {
                for (int index = _disposables.Count - 1; index >= 0; index--)
                {
                    _disposables[index].Dispose();
                }
            }
        }
    }
}
