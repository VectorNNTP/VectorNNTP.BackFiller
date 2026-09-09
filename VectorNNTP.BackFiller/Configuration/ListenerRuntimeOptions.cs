// <copyright file="ListenerRuntimeOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller.Configuration
// Immutable Listener runtime options projected from validated BackFiller Listener configuration.

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Immutable runtime options that bound Listener parser buffering, handshake/session I/O progress tenure, receipt-ack lifetime, Found payload queue pressure, and active connections.
    /// </summary>
    /// <param name="ParserAccumulationMaxBytes">Maximum accumulated incomplete inbound protocol bytes per connection before forced termination.</param>
    /// <param name="TlsHandshakeTimeout">Maximum time allowed for one accepted connection to complete TLS server authentication before the connection is terminated.</param>
    /// <param name="IoProgressTimeout">Maximum no-progress interval for one listener session read or write operation before the connection is terminated.</param>
    /// <param name="AwaitingReceiptAckTimeout">Maximum duration to wait for ReceiptAck after a Found transfer has completed.</param>
    /// <param name="MaxQueuedFoundPayloadBytes">Maximum queued/in-flight Found payload bytes tracked per connection.</param>
    /// <param name="MaxActiveConnections">Maximum concurrently active accepted Listener connections.</param>
    internal sealed record ListenerRuntimeOptions(
        int ParserAccumulationMaxBytes,
        TimeSpan TlsHandshakeTimeout,
        TimeSpan IoProgressTimeout,
        TimeSpan AwaitingReceiptAckTimeout,
        int MaxQueuedFoundPayloadBytes,
        int MaxActiveConnections);
}
