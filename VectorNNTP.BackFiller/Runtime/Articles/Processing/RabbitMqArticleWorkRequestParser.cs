// <copyright file="RabbitMqArticleWorkRequestParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: rabbit mq article work request parser in the articles processing subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="RabbitMqArticleWorkRequestParser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Phase 3 RabbitMQ delivery payload parser that extracts Message-ID work requests without
// inventing unsupported legacy wire-protocol assumptions.

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
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
        /// <summary>
        /// Stores the supported version state used to enforce this component's runtime contract.
        /// </summary>
        private const int SupportedVersion = 1;

        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<RabbitMqArticleWorkRequestParser> _logger;
        /// <summary>
        /// Stores the diagnostic correlation id state used to enforce this component's runtime contract.
        /// </summary>
        private readonly string? _diagnosticCorrelationId;

        /// <summary>
        /// Initializes a new parser instance.
        /// </summary>
        /// <param name="runtimeOptions">Immutable runtime options used for optional targeted diagnostics.</param>
        /// <param name="logger">Logger.</param>
        public RabbitMqArticleWorkRequestParser(
            BackFillerRuntimeOptions? runtimeOptions = null,
            ILogger<RabbitMqArticleWorkRequestParser>? logger = null)
        {
            _logger = logger ?? NullLogger<RabbitMqArticleWorkRequestParser>.Instance;

            RabbitMqRuntimeOptions? rabbitMq = runtimeOptions?.RabbitMq;
            _diagnosticCorrelationId = string.IsNullOrWhiteSpace(rabbitMq?.DiagnosticPayloadCorrelationId)
                ? null
                : rabbitMq.DiagnosticPayloadCorrelationId.Trim();
        }

        /// <inheritdoc/>
        public ValueTask<RabbitMqArticleWorkParseResult> ParseAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(delivery);
            cancellationToken.ThrowIfCancellationRequested();

            bool shouldLogDiagnosticPayload = ShouldLogDiagnosticPayload(delivery.CorrelationId);
            if (shouldLogDiagnosticPayload)
            {
                LogPayloadDiagnosticAtParserEntry(
                    _logger,
                    DateTimeOffset.UtcNow,
                    delivery.CorrelationId,
                    delivery.RabbitMqMessageId,
                    delivery.ReplyTo,
                    delivery.ConsumerIdentity,
                    delivery.DeliveryTag,
                    delivery.Payload);
            }

            if (delivery.Payload.IsEmpty)
            {
                return ValueTask.FromResult(Failed(delivery, "RabbitMQ article-work payload was empty."));
            }

            JsonDocument jsonDocument;
            try
            {
                jsonDocument = JsonDocument.Parse(delivery.Payload);
            }
            catch (JsonException ex)
            {
                if (shouldLogDiagnosticPayload)
                {
                    LogPayloadDiagnosticJsonException(
                        _logger,
                        delivery.CorrelationId,
                        delivery.DeliveryTag,
                        ex.Message,
                        ex.Path,
                        ex.LineNumber,
                        ex.BytePositionInLine);
                }

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

        /// <summary>
        /// Performs the should log diagnostic payload operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private bool ShouldLogDiagnosticPayload(string? correlationId)
        {
            return !string.IsNullOrWhiteSpace(_diagnosticCorrelationId)
                && !string.IsNullOrWhiteSpace(correlationId)
                && string.Equals(_diagnosticCorrelationId, correlationId, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Performs the log payload diagnostic at parser entry operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogPayloadDiagnosticAtParserEntry(
            ILogger logger,
            DateTimeOffset timestampUtc,
            string? correlationId,
            string? rabbitMqMessageId,
            string? replyTo,
            string consumerIdentity,
            ulong deliveryTag,
            ReadOnlyMemory<byte> payload)
        {
            string payloadUtf8 = Encoding.UTF8.GetString(payload.Span);
            string payloadHex = Convert.ToHexString(payload.Span);
            string payloadSha256 = Convert.ToHexString(SHA256.HashData(payload.Span));

            logger.LogInformation(
                "RabbitMQ payload diagnostic parser-entry. TimestampUtc={TimestampUtc:o} ConsumerIdentity={ConsumerIdentity} DeliveryTag={DeliveryTag} CorrelationId={CorrelationId} RabbitMqMessageId={RabbitMqMessageId} ReplyTo={ReplyTo} PayloadLength={PayloadLength} PayloadUtf8={PayloadUtf8} PayloadHex={PayloadHex} PayloadSha256={PayloadSha256}",
                timestampUtc,
                consumerIdentity,
                deliveryTag,
                correlationId,
                rabbitMqMessageId,
                replyTo,
                payload.Length,
                payloadUtf8,
                payloadHex,
                payloadSha256);
        }

        /// <summary>
        /// Performs the log payload diagnostic json exception operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static void LogPayloadDiagnosticJsonException(
            ILogger logger,
            string? correlationId,
            ulong deliveryTag,
            string message,
            string? path,
            long? lineNumber,
            long? bytePositionInLine)
        {
            logger.LogWarning(
                "RabbitMQ payload diagnostic parser JsonException. CorrelationId={CorrelationId} DeliveryTag={DeliveryTag} JsonExceptionMessage={JsonExceptionMessage} JsonPath={JsonPath} JsonLineNumber={JsonLineNumber} JsonBytePositionInLine={JsonBytePositionInLine}",
                correlationId,
                deliveryTag,
                message,
                path,
                lineNumber,
                bytePositionInLine);
        }

        /// <summary>
        /// Performs the failed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private RabbitMqArticleWorkParseResult Failed(RabbitMqArticleDelivery delivery, string reason)
        {
            string payloadSha256 = Convert.ToHexString(SHA256.HashData(delivery.Payload.Span));

            _logger.LogWarning(
                "RabbitMQ article-work request rejected. Reason={Reason} CorrelationId={CorrelationId} ReplyTo={ReplyTo} RabbitMqMessageId={RabbitMqMessageId} DeliveryTag={DeliveryTag} Backbone={Backbone} PayloadLength={PayloadLength} PayloadSha256={PayloadSha256}",
                reason,
                delivery.CorrelationId,
                delivery.ReplyTo,
                delivery.RabbitMqMessageId,
                delivery.DeliveryTag,
                delivery.Backbone,
                delivery.Payload.Length,
                payloadSha256);

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

        /// <summary>
        /// Performs the try read required int32 operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static bool TryReadRequiredInt32(JsonElement root, string propertyName, out int value)
        {
            value = default;
            if (!root.TryGetProperty(propertyName, out JsonElement property) || property.ValueKind != JsonValueKind.Number)
            {
                return false;
            }

            return property.TryGetInt32(out value);
        }

        /// <summary>
        /// Performs the try read required guid operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static bool TryReadRequiredGuid(JsonElement root, string propertyName, out Guid value)
        {
            value = Guid.Empty;
            if (!TryReadRequiredString(root, propertyName, out string? textValue) || string.IsNullOrWhiteSpace(textValue))
            {
                return false;
            }

            return Guid.TryParse(textValue, out value);
        }

        /// <summary>
        /// Performs the try read required string operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
