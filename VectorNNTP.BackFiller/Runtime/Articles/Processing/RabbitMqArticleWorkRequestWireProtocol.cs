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
    /// Defines the canonical JSON body contract for RabbitMQ article-work requests.
    /// </summary>
    /// <remarks>
    /// The application payload carries only version, request identity, Message-ID, and backbone. AMQP RPC fields such as <c>CorrelationId</c> and <c>ReplyTo</c> stay on broker metadata instead of being duplicated in the JSON body.
    /// </remarks>
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
        /// Serializes a version-1 article-work request into its compact canonical JSON form.
        /// </summary>
        /// <param name="request">Structured request whose application fields should be written into the payload body.</param>
        /// <returns>UTF-8 encoded compact JSON containing <c>version</c>, <c>requestId</c>, <c>messageId</c>, and <c>backbone</c> in that order.</returns>
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
