// <copyright file="RabbitMqArticleWorkRequestWireProtocol.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: rabbit mq article work request wire protocol in the articles processing subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="RabbitMqArticleWorkRequestWireProtocol.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Canonical JSON wire protocol helpers for compact deterministic article-work request
// serialization and deserialization contracts.

using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Defines the canonical JSON wire protocol contract for RabbitMQ article-work requests.
    /// </summary>
    internal static class RabbitMqArticleWorkRequestWireProtocol
    {
        /// <summary>
        /// Canonical protocol name used in documentation and integration boundaries.
        /// </summary>
        internal const string ProtocolName = "BackFiller RabbitMQ Article-Work Request";

        /// <summary>
        /// Canonical protocol version supported by this runtime.
        /// </summary>
        internal const int CurrentVersion = 1;

        /// <summary>
        /// Creates a deterministic compact JSON payload for the canonical version-1 request object.
        /// </summary>
        /// <param name="request">Structured request to serialize.</param>
        /// <returns>UTF-8 encoded compact JSON payload.</returns>
        internal static byte[] SerializeV1(RabbitMqArticleWorkRequest request)
        {
            ArgumentNullException.ThrowIfNull(request);

            ArrayBufferWriter<byte> writer = new();
            using Utf8JsonWriter jsonWriter = new(writer, new JsonWriterOptions
            {
                Indented = false,
                SkipValidation = false,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

            jsonWriter.WriteStartObject();
            jsonWriter.WriteNumber("version", request.Version);
            jsonWriter.WriteString("requestId", request.RequestId);
            jsonWriter.WriteString("messageId", request.MessageId);
            jsonWriter.WriteString("backbone", request.Backbone);
            jsonWriter.WriteEndObject();
            jsonWriter.Flush();

            return writer.WrittenSpan.ToArray();
        }
    }
}
