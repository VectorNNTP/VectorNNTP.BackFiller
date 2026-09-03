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
    /// Precomputes the human-readable prefix and structured properties attached to one reusable acquisition connection.
    /// </summary>
    /// <remarks>
    /// The session manager creates this context once per connection slot so repeated ARTICLE, DATE, reconnect, and shutdown logs can reuse the same identifiers without reformatting them on every log call.
    /// </remarks>
    internal sealed class NntpConnectionLogContext
    {
        /// <summary>
        /// Structured properties pushed into the ambient Serilog context.
        /// </summary>
        private readonly KeyValuePair<string, object?>[] _scopeProperties;

        /// <summary>
        /// Human-readable prefix rendered into connection-scoped diagnostics.
        /// </summary>
        private readonly string _connectionPrefix;

        /// <summary>
        /// Initializes a new connection logging context.
        /// </summary>
        /// <param name="backbone">Provider/backbone namespace that owns the connection.</param>
        /// <param name="accountUsername">Configured account username associated with the connection.</param>
        /// <param name="accountId">Stable identifier of the account being served.</param>
        /// <param name="serverId">Identifier of the owning NNTP server configuration.</param>
        /// <param name="host">Remote NNTP host name or address.</param>
        /// <param name="port">Remote NNTP port.</param>
        /// <param name="useSsl">Whether the connection uses SSL/TLS.</param>
        /// <param name="connectionNumber">One-based connection number within the account.</param>
        /// <param name="connectionLimit">Configured maximum connection count for the account.</param>
        /// <exception cref="ArgumentException">Thrown when a required text argument is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="connectionNumber"/> or <paramref name="connectionLimit"/> is not positive.</exception>
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
        /// Gets the provider/backbone namespace that owns the connection.
        /// </summary>
        /// <value>The configured backbone name used for grouping related sessions.</value>
        internal string Backbone { get; }

        /// <summary>
        /// Gets the configured provider-account username.
        /// </summary>
        /// <value>The username associated with this connection slot.</value>
        internal string AccountUsername { get; }

        /// <summary>
        /// Gets the stable provider-account identifier.
        /// </summary>
        /// <value>The account identifier attached to emitted log properties.</value>
        internal Guid AccountId { get; }

        /// <summary>
        /// Gets the owning BackFiller server identifier.
        /// </summary>
        /// <value>The server identifier propagated into structured logs.</value>
        internal byte ServerId { get; }

        /// <summary>
        /// Gets the remote NNTP host.
        /// </summary>
        /// <value>The host name or address currently associated with the connection.</value>
        internal string Host { get; }

        /// <summary>
        /// Gets the remote NNTP port.
        /// </summary>
        /// <value>The port currently associated with the connection.</value>
        internal int Port { get; }

        /// <summary>
        /// Gets a value indicating whether the connection uses SSL/TLS.
        /// </summary>
        /// <value><see langword="true"/> when the connection is configured for implicit TLS.</value>
        internal bool UseSsl { get; }

        /// <summary>
        /// Gets the one-based connection number within the account.
        /// </summary>
        /// <value>The slot number rendered in <see cref="ConnectionPrefix"/>.</value>
        internal int ConnectionNumber { get; }

        /// <summary>
        /// Gets the configured maximum connection count for the account.
        /// </summary>
        /// <value>The per-account capacity used to zero-pad connection numbering.</value>
        internal int ConnectionLimit { get; }

        /// <summary>
        /// Gets the preformatted connection prefix used by human-readable diagnostics.
        /// </summary>
        /// <value>A prefix such as <c>Backbone/user[001/010]: </c>.</value>
        internal string ConnectionPrefix => _connectionPrefix;

        /// <summary>
        /// Gets the structured properties associated with the connection.
        /// </summary>
        /// <value>The immutable property set pushed by <see cref="Push"/>.</value>
        internal IReadOnlyList<KeyValuePair<string, object?>> ScopeProperties => _scopeProperties;

        /// <summary>
        /// Pushes the connection properties into the current Serilog ambient context.
        /// </summary>
        /// <returns>A disposable scope that removes the pushed properties in reverse order.</returns>
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
        /// Unwinds a batch of Serilog context pushes as a single disposable scope.
        /// </summary>
        private sealed class CompositeDisposable : IDisposable
        {
            /// <summary>
            /// Disposables returned by individual <see cref="LogContext.PushProperty(string, object?, bool)"/> calls.
            /// </summary>
            private readonly IReadOnlyList<IDisposable> _disposables;

            /// <summary>
            /// Initializes a new composite scope wrapper.
            /// </summary>
            /// <param name="disposables">Property scopes that should be unwound together.</param>
            internal CompositeDisposable(IReadOnlyList<IDisposable> disposables)
            {
                _disposables = disposables ?? throw new ArgumentNullException(nameof(disposables));
            }

            /// <summary>
            /// Disposes the pushed property scopes in reverse order of acquisition.
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
