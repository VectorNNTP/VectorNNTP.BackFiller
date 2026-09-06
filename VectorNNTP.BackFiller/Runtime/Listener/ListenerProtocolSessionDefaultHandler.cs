// <copyright file="ListenerProtocolSessionDefaultHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Default Stage 6B request handler that preserves session behavior without retention integration.

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Default Listener protocol request handler used before retention integration is implemented.
    /// </summary>
    /// <remarks>
    /// This handler intentionally returns NotFound for every validated request so the per-connection session behavior
    /// can be exercised without fabricating article payloads or integrating retention ownership flows.
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
