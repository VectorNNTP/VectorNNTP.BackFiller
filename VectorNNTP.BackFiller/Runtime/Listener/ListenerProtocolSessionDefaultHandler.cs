// <copyright file="ListenerProtocolSessionDefaultHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Default Stage 6B request handler that preserves session behavior without retention integration.

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Default Listener protocol request handler that intentionally returns NotFound for every validated request.
    /// </summary>
    /// <remarks>
    /// This inert fallback is useful for tests and non-retention session wiring that only needs protocol/lifecycle behavior.
    /// Production listener socket wiring uses <see cref="ListenerProtocolRetentionRequestHandler"/> to integrate shared retention lookups and completion callbacks.
    /// </remarks>
    internal sealed class ListenerProtocolSessionDefaultHandler : IListenerProtocolRequestHandler
    {
        /// <inheritdoc/>
        public ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
            uint requestId,
            ReadOnlyMemory<byte> messageIdMd5Payload,
            CancellationToken cancellationToken)
        {
            _ = requestId;
            _ = messageIdMd5Payload;
            _ = cancellationToken;
            return ValueTask.FromResult(ListenerSessionRequestDispatchResult.NotFound());
        }
    }
}
