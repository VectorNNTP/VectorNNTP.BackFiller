// <copyright file="RabbitMqArticleWorkResponseWireProtocol.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Canonical JSON wire protocol helpers for deterministic article-work response serialization.

using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Defines the canonical JSON body contract for RabbitMQ article-work responses.
    /// </summary>
    /// <remarks>
    /// Success responses always emit an explicit <c>uri</c> property, which is currently <see langword="null"/> in this runtime, while failure responses omit <c>uri</c> and include <c>error</c> only when text is available.
    /// </remarks>
    internal static class RabbitMqArticleWorkResponseWireProtocol
    {
        /// <summary>
        /// Canonical protocol name used in documentation and integration boundaries.
        /// </summary>
        internal const string ProtocolName = "BackFiller RabbitMQ Article-Work Response";

        /// <summary>
        /// Canonical protocol version supported by this runtime.
        /// </summary>
        internal const int CurrentVersion = 1;

        /// <summary>
        /// Serializes a version-1 response payload into its compact canonical JSON form.
        /// </summary>
        /// <param name="response">Structured response to serialize.</param>
        /// <returns>UTF-8 encoded compact JSON body for RabbitMQ RPC publication.</returns>
        internal static byte[] SerializeV1(RabbitMqArticleWorkResponse response)
        {
            ArgumentNullException.ThrowIfNull(response);

            ArrayBufferWriter<byte> writer = new();
            using Utf8JsonWriter jsonWriter = new(writer, new JsonWriterOptions
            {
                Indented = false,
                SkipValidation = false,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

            jsonWriter.WriteStartObject();
            jsonWriter.WriteNumber("version", response.Version);
            jsonWriter.WriteString("requestId", response.RequestId);
            jsonWriter.WriteString("messageId", response.MessageId);
            jsonWriter.WriteString("backbone", response.Backbone);
            jsonWriter.WriteString("outcome", response.Outcome);

            if (string.Equals(response.Outcome, nameof(ArticleWorkProcessingOutcome.Success), StringComparison.Ordinal))
            {
                if (response.Uri is null)
                {
                    jsonWriter.WriteNull("uri");
                }
                else
                {
                    jsonWriter.WriteString("uri", response.Uri);
                }
            }

            if (!string.IsNullOrWhiteSpace(response.Error))
            {
                jsonWriter.WriteString("error", response.Error);
            }

            jsonWriter.WriteEndObject();
            jsonWriter.Flush();

            return writer.WrittenSpan.ToArray();
        }

        /// <summary>
        /// Parses a version-1 response payload for tests and integration contract verification.
        /// </summary>
        /// <param name="payload">UTF-8 JSON response payload.</param>
        /// <returns>Parsed response instance.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="payload"/> is empty.</exception>
        /// <exception cref="JsonException">Thrown when <paramref name="payload"/> is not valid JSON.</exception>
        /// <exception cref="KeyNotFoundException">Thrown when a required property is missing.</exception>
        /// <exception cref="FormatException">Thrown when a required property has an invalid GUID or numeric representation.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the payload is not a JSON object, uses an unsupported version, or a required string property is JSON <see langword="null"/>.
        /// </exception>
        internal static RabbitMqArticleWorkResponse ParseV1(ReadOnlySpan<byte> payload)
        {
            if (payload.IsEmpty)
            {
                throw new ArgumentException("Response payload is empty.", nameof(payload));
            }

            using JsonDocument doc = JsonDocument.Parse(payload.ToArray());
            JsonElement root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException("Response payload must be a JSON object.");
            }

            int version = root.GetProperty("version").GetInt32();
            if (version != CurrentVersion)
            {
                throw new InvalidOperationException($"Unsupported response version '{version}'.");
            }

            Guid requestId = root.GetProperty("requestId").GetGuid();
            string messageId = root.GetProperty("messageId").GetString()
                ?? throw new InvalidOperationException("Response payload is missing required 'messageId'.");
            string backbone = root.GetProperty("backbone").GetString()
                ?? throw new InvalidOperationException("Response payload is missing required 'backbone'.");
            string outcome = root.GetProperty("outcome").GetString()
                ?? throw new InvalidOperationException("Response payload is missing required 'outcome'.");

            string? uri = null;
            if (root.TryGetProperty("uri", out JsonElement uriElement) && uriElement.ValueKind == JsonValueKind.String)
            {
                uri = uriElement.GetString();
            }

            string? error = null;
            if (root.TryGetProperty("error", out JsonElement errorElement) && errorElement.ValueKind == JsonValueKind.String)
            {
                error = errorElement.GetString();
            }

            return new RabbitMqArticleWorkResponse(version, requestId, messageId, backbone, outcome, uri, error);
        }
    }
}
