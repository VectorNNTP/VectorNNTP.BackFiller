// <copyright file="RabbitMqArticleWorkResponseWireProtocol.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Canonical JSON wire protocol helpers for deterministic article-work response serialization.

using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Defines the canonical JSON body contract for RabbitMQ article-work responses.
    /// </summary>
    /// <remarks>
    /// Success responses always emit an explicit <c>uri</c> property using the canonical cache URI contract,
    /// while failure responses omit <c>uri</c> and include <c>error</c> only when text is available.
    /// </remarks>
    internal static partial class RabbitMqArticleWorkResponseWireProtocol
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

            if (response.RequestId.HasValue)
            {
                jsonWriter.WriteString("requestId", response.RequestId.Value);
            }
            else
            {
                jsonWriter.WriteNull("requestId");
            }

            if (response.MessageId is null)
            {
                jsonWriter.WriteNull("messageId");
            }
            else
            {
                jsonWriter.WriteString("messageId", response.MessageId);
            }

            if (response.Backbone is null)
            {
                jsonWriter.WriteNull("backbone");
            }
            else
            {
                jsonWriter.WriteString("backbone", response.Backbone);
            }

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

            string outcome = root.GetProperty("outcome").GetString()
                ?? throw new InvalidOperationException("Response payload is missing required 'outcome'.");

            Guid? requestId = ReadOptionalGuidProperty(root, "requestId");
            string? messageId = ReadOptionalStringProperty(root, "messageId");
            string? backbone = ReadOptionalStringProperty(root, "backbone");

            bool successOutcome = string.Equals(outcome, nameof(ArticleWorkProcessingOutcome.Success), StringComparison.Ordinal);
            bool articleNotFoundOutcome = string.Equals(outcome, nameof(ArticleWorkProcessingOutcome.ArticleNotFound), StringComparison.Ordinal);
            bool invalidArticleOutcome = string.Equals(outcome, nameof(ArticleWorkProcessingOutcome.InvalidArticle), StringComparison.Ordinal);
            bool invalidRequestOutcome = string.Equals(outcome, nameof(ArticleWorkProcessingOutcome.InvalidRequest), StringComparison.Ordinal);

            if (!successOutcome && !articleNotFoundOutcome && !invalidArticleOutcome && !invalidRequestOutcome)
            {
                throw new InvalidOperationException($"Unsupported response outcome '{outcome}'.");
            }

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

            if (successOutcome)
            {
                if (!requestId.HasValue || requestId.Value == Guid.Empty)
                {
                    throw new InvalidOperationException("Success response payload requires a concrete non-empty 'requestId'.");
                }

                if (string.IsNullOrWhiteSpace(messageId) || !NntpMessageIdValidation.IsValidMessageId(messageId.AsSpan()))
                {
                    throw new InvalidOperationException("Success response payload requires a canonical non-empty 'messageId'.");
                }

                if (string.IsNullOrWhiteSpace(backbone))
                {
                    throw new InvalidOperationException("Success response payload requires a non-empty 'backbone'.");
                }

                if (string.IsNullOrWhiteSpace(uri) || !IsCanonicalCacheUri(uri))
                {
                    throw new InvalidOperationException("Success response payload requires a canonical non-empty 'uri'.");
                }

                if (error is not null)
                {
                    throw new InvalidOperationException("Success response payload must not include 'error'.");
                }
            }
            else if (articleNotFoundOutcome || invalidArticleOutcome)
            {
                if (!requestId.HasValue || requestId.Value == Guid.Empty)
                {
                    throw new InvalidOperationException("Terminal non-invalid-request response payload requires a concrete non-empty 'requestId'.");
                }

                if (string.IsNullOrWhiteSpace(messageId) || !NntpMessageIdValidation.IsValidMessageId(messageId.AsSpan()))
                {
                    throw new InvalidOperationException("Terminal non-invalid-request response payload requires a canonical non-empty 'messageId'.");
                }

                if (string.IsNullOrWhiteSpace(backbone))
                {
                    throw new InvalidOperationException("Terminal non-invalid-request response payload requires a non-empty 'backbone'.");
                }

                if (uri is not null)
                {
                    throw new InvalidOperationException("Terminal failure response payload must not include 'uri'.");
                }

                if (string.IsNullOrWhiteSpace(error))
                {
                    throw new InvalidOperationException("Terminal failure response payload requires a non-empty 'error'.");
                }
            }
            else
            {
                if (requestId.HasValue && requestId.Value == Guid.Empty)
                {
                    throw new InvalidOperationException("InvalidRequest payload must not use Guid.Empty sentinel request identity.");
                }

                if (messageId is not null && (string.IsNullOrWhiteSpace(messageId) || !NntpMessageIdValidation.IsValidMessageId(messageId.AsSpan())))
                {
                    throw new InvalidOperationException("InvalidRequest payload messageId, when provided, must be canonical and non-empty.");
                }

                if (backbone is not null && string.IsNullOrWhiteSpace(backbone))
                {
                    throw new InvalidOperationException("InvalidRequest payload backbone, when provided, must be non-empty.");
                }

                if (uri is not null)
                {
                    throw new InvalidOperationException("InvalidRequest payload must not include 'uri'.");
                }

                if (string.IsNullOrWhiteSpace(error))
                {
                    throw new InvalidOperationException("InvalidRequest payload requires a non-empty 'error'.");
                }
            }

            return new RabbitMqArticleWorkResponse(version, requestId, messageId, backbone, outcome, uri, error);
        }

        [GeneratedRegex("^cache://(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?:(?:6553[0-5]|655[0-2][0-9]|65[0-4][0-9]{2}|6[0-4][0-9]{3}|[1-5][0-9]{4}|[1-9][0-9]{0,3})/[0-9a-f]{32}$", RegexOptions.CultureInvariant)]
        private static partial Regex CanonicalCacheUriRegex();

        private static bool IsCanonicalCacheUri(string uri)
        {
            return CanonicalCacheUriRegex().IsMatch(uri);
        }

        private static Guid? ReadOptionalGuidProperty(JsonElement root, string propertyName)
        {
            JsonElement property = root.GetProperty(propertyName);
            return property.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.String => property.GetGuid(),
                JsonValueKind.Undefined => throw new InvalidOperationException($"Response payload property '{propertyName}' is undefined."),
                JsonValueKind.Object => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be an object."),
                JsonValueKind.Array => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be an array."),
                JsonValueKind.Number => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be numeric."),
                JsonValueKind.True => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be boolean true."),
                JsonValueKind.False => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be boolean false."),
                _ => throw new InvalidOperationException($"Response payload property '{propertyName}' must be a JSON string or null."),
            };
        }

        private static string? ReadOptionalStringProperty(JsonElement root, string propertyName)
        {
            JsonElement property = root.GetProperty(propertyName);
            return property.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.String => property.GetString() ?? throw new InvalidOperationException($"Response payload property '{propertyName}' string value was null."),
                JsonValueKind.Undefined => throw new InvalidOperationException($"Response payload property '{propertyName}' is undefined."),
                JsonValueKind.Object => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be an object."),
                JsonValueKind.Array => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be an array."),
                JsonValueKind.Number => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be numeric."),
                JsonValueKind.True => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be boolean true."),
                JsonValueKind.False => throw new InvalidOperationException($"Response payload property '{propertyName}' must not be boolean false."),
                _ => throw new InvalidOperationException($"Response payload property '{propertyName}' must be a JSON string or null."),
            };
        }
    }
}
