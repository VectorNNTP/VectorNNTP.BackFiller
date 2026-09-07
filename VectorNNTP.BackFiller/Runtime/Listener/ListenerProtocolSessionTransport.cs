// <copyright file="ListenerProtocolSessionTransport.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Minimal connected-transport abstraction and Stream adapter for listener protocol sessions.

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Represents one established connected transport owned by a Listener protocol session.
    /// </summary>
    /// <remarks>
    /// This abstraction keeps session tests transport-agnostic while preserving explicit async read/write/cancellation semantics.
    /// Write operations return the number of bytes accepted, allowing session logic to complete partial writes deterministically.
    /// </remarks>
    internal interface IListenerProtocolSessionTransport : IAsyncDisposable
    {
        /// <summary>
        /// Reads bytes from the connected transport.
        /// </summary>
        /// <param name="buffer">Destination buffer for received bytes.</param>
        /// <param name="cancellationToken">Cancellation token for cooperative shutdown.</param>
        /// <returns>Number of bytes read, or zero when the remote side closed the transport.</returns>
        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken);

        /// <summary>
        /// Writes bytes to the connected transport.
        /// </summary>
        /// <param name="buffer">Source bytes to write.</param>
        /// <param name="cancellationToken">Cancellation token for cooperative shutdown.</param>
        /// <returns>Number of bytes accepted by the transport for this write call.</returns>
        public ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Stream-backed transport adapter for Listener protocol sessions.
    /// </summary>
    internal sealed class StreamListenerProtocolSessionTransport : IListenerProtocolSessionTransport
    {
        private readonly Stream _stream;
        private bool _disposed;

        /// <summary>
        /// Initializes a stream-backed listener session transport.
        /// </summary>
        /// <param name="stream">Connected stream instance owned by the transport adapter.</param>
        /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <see langword="null"/>.</exception>
        internal StreamListenerProtocolSessionTransport(Stream stream)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        /// <inheritdoc/>
        public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _stream.ReadAsync(buffer, cancellationToken);
        }

        /// <inheritdoc/>
        public async ValueTask<int> WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (buffer.IsEmpty)
            {
                return 0;
            }

            await _stream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            return buffer.Length;
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
