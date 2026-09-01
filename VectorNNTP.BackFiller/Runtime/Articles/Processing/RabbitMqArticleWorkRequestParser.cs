// <copyright file="RabbitMqArticleWorkRequestParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Phase 3 RabbitMQ delivery payload parser that extracts Message-ID work requests without
// inventing unsupported legacy wire-protocol assumptions.

using System.Text.Json;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Parses RabbitMQ delivery payload bytes into versioned JSON article-work requests.
    /// </summary>
    /// <remarks>
    /// This parser enforces the canonical BackFiller JSON wire protocol and rejects malformed,
    /// ambiguous, or unsupported payloads as invalid requests before provider processing.
    /// </remarks>
    internal sealed class RabbitMqArticleWorkRequestParser : IRabbitMqArticleWorkRequestParser
    {
        private const int SupportedVersion = 1;

        /// <inheritdoc/>
        public ValueTask<RabbitMqArticleWorkParseResult> ParseAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(delivery);
            cancellationToken.ThrowIfCancellationRequested();

            if (delivery.Payload.IsEmpty)
            {
                return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload was empty."));
            }

            JsonDocument jsonDocument;
            try
            {
                jsonDocument = JsonDocument.Parse(delivery.Payload);
            }
            catch (JsonException)
            {
                return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload was not valid JSON."));
            }

            using (jsonDocument)
            {
                JsonElement root = jsonDocument.RootElement;
                if (root.ValueKind != JsonValueKind.Object)
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload must be a JSON object."));
                }

                if (!TryReadRequiredInt32(root, "version", out int version))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload is missing required integer property 'version'."));
                }

                if (version != SupportedVersion)
                {
                    return ValueTask.FromResult(Failed(delivery, $"RabbitMQ article-work payload uses unsupported version '{version}'."));
                }

                if (!TryReadRequiredGuid(root, "requestId", out Guid requestId))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload contains missing or invalid 'requestId'."));
                }

                if (!TryReadRequiredString(root, "messageId", out string? messageId) || string.IsNullOrWhiteSpace(messageId))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload contains missing or invalid 'messageId'."));
                }

                if (!NntpMessageIdValidation.IsValidMessageId(messageId.AsSpan()))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload 'messageId' is not a canonical NNTP Message-ID."));
                }

                if (!TryReadRequiredString(root, "backbone", out string? messageBackbone) || string.IsNullOrWhiteSpace(messageBackbone))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload contains missing or invalid 'backbone'."));
                }

                if (!string.Equals(messageBackbone, delivery.Backbone, StringComparison.OrdinalIgnoreCase))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload backbone does not match the consuming queue backbone context."));
                }

                if (string.IsNullOrWhiteSpace(delivery.CorrelationId))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ delivery is missing required AMQP CorrelationId property."));
                }

                if (string.IsNullOrWhiteSpace(delivery.ReplyTo))
                {
                    return ValueTask.FromResult(Failed(delivery, "RabbitMQ delivery is missing required AMQP ReplyTo property."));
                }

                RabbitMqArticleWorkRequest request = new(
                    Version: version,
                    RequestId: requestId,
                    MessageId: messageId,
                    Backbone: messageBackbone);

                return ValueTask.FromResult(new RabbitMqArticleWorkParseResult(request, Failure: null));
            }
        }

        private static RabbitMqArticleWorkParseResult Failed(RabbitMqArticleDelivery delivery, string reason)
        {
            ArticleWorkProcessingResult result = new(
                Request: new RabbitMqArticleWorkRequest(
                    Version: SupportedVersion,
                    RequestId: Guid.Empty,
                    MessageId: string.Empty,
                    Backbone: delivery.Backbone),
                Delivery: delivery,
                Outcome: ArticleWorkProcessingOutcome.InvalidRequest,
                Disposition: ArticleWorkDispositionRecommendation.NackDrop,
                GrabberResult: null,
                ProviderFailureCode: NntpArticleAcquisitionFailureCode.InvalidMessageId,
                ResponseCode: null,
                ResponseText: reason,
                UnexpectedException: null);

            return new RabbitMqArticleWorkParseResult(Request: null, Failure: result);
        }

        private static bool TryReadRequiredInt32(JsonElement root, string propertyName, out int value)
        {
            value = default;
            if (!root.TryGetProperty(propertyName, out JsonElement property) || property.ValueKind != JsonValueKind.Number)
            {
                return false;
            }

            return property.TryGetInt32(out value);
        }

        private static bool TryReadRequiredGuid(JsonElement root, string propertyName, out Guid value)
        {
            value = Guid.Empty;
            if (!TryReadRequiredString(root, propertyName, out string? textValue) || string.IsNullOrWhiteSpace(textValue))
            {
                return false;
            }

            return Guid.TryParse(textValue, out value);
        }

        private static bool TryReadRequiredString(JsonElement root, string propertyName, out string? value)
        {
            value = null;
            if (!root.TryGetProperty(propertyName, out JsonElement property) || property.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            value = property.GetString();
            return value is not null;
        }
    }
}
