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
    internal sealed partial class RabbitMqArticleWorkRequestParser : IRabbitMqArticleWorkRequestParser
    {
        /// <summary>
        /// Only application payload version accepted by this parser.
        /// </summary>
        private const int SupportedVersion = 1;

        /// <summary>
        /// Supplies the logger used by rabbit mq article work request parser.
        /// </summary>
        private readonly ILogger<RabbitMqArticleWorkRequestParser> _logger;
        /// <summary>
        /// Optional correlation identifier that enables targeted payload preview logging for one RPC conversation.
        /// </summary>
        private readonly string? _diagnosticCorrelationId;

        /// <summary>
        /// Initializes a parser instance with optional targeted payload-diagnostic support.
        /// </summary>
        /// <param name="runtimeOptions">
        /// Immutable runtime options that may supply <c>DiagnosticPayloadCorrelationId</c> for one-correlation payload preview logging.
        /// </param>
        /// <param name="logger">Logger used for request rejection and optional diagnostic payload events.</param>
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

        /// <summary>
        /// Parses one admitted delivery into the canonical application request model.
        /// </summary>
        /// <param name="delivery">Admitted RabbitMQ delivery whose JSON body and AMQP RPC metadata must satisfy the request contract.</param>
        /// <param name="cancellationToken">Cancellation token for parsing work.</param>
        /// <returns>
        /// A successful parse result when the payload matches the version-1 wire contract, or an invalid-request failure result when validation fails.
        /// </returns>
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
        /// Determines whether this delivery matches the optional one-correlation diagnostic payload logging gate.
        /// </summary>
        /// <param name="correlationId">Correlation identifier carried by the admitted RabbitMQ delivery.</param>
        /// <returns><see langword="true"/> when runtime configuration selected this exact correlation identifier for payload diagnostics.</returns>
        private bool ShouldLogDiagnosticPayload(string? correlationId)
        {
            return !string.IsNullOrWhiteSpace(_diagnosticCorrelationId)
                && !string.IsNullOrWhiteSpace(correlationId)
                && string.Equals(_diagnosticCorrelationId, correlationId, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Emits the targeted parser-entry diagnostic event with bounded payload previews and a full-payload digest.
        /// </summary>
        /// <param name="logger">Logger receiving the diagnostic event.</param>
        /// <param name="timestampUtc">UTC timestamp captured at parser entry.</param>
        /// <param name="correlationId">AMQP correlation identifier used to target diagnostics.</param>
        /// <param name="rabbitMqMessageId">Optional AMQP message identifier supplied by the publisher.</param>
        /// <param name="replyTo">AMQP reply destination carried by the delivery.</param>
        /// <param name="consumerIdentity">Logical consumer identity that admitted the delivery.</param>
        /// <param name="deliveryTag">Broker delivery tag for the admitted message.</param>
        /// <param name="payload">
        /// Raw payload bytes. The log includes a UTF-8 preview capped at 256 characters, a hexadecimal preview of the first 128 bytes, and a SHA-256 of the full payload.
        /// </param>
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
            ArgumentNullException.ThrowIfNull(logger);

            if (!logger.IsEnabled(LogLevel.Information))
            {
                return;
            }

            const int DiagnosticPreviewLength = 256;

            string payloadUtf8Full = Encoding.UTF8.GetString(payload.Span);
            string payloadUtf8Preview = payloadUtf8Full.Length <= DiagnosticPreviewLength
                ? payloadUtf8Full
                : payloadUtf8Full[..DiagnosticPreviewLength];
            if (payloadUtf8Preview.Length > 0
                && payloadUtf8Preview.Length < payloadUtf8Full.Length
                && char.IsHighSurrogate(payloadUtf8Preview[^1]))
            {
                payloadUtf8Preview = payloadUtf8Preview[..^1];
            }

            int hexPreviewByteLength = Math.Min(payload.Length, DiagnosticPreviewLength / 2);
            string payloadHexPreview = Convert.ToHexString(payload.Span[..hexPreviewByteLength]);

            string payloadSha256 = Convert.ToHexString(SHA256.HashData(payload.Span));

            LogPayloadDiagnosticParserEntry(
                logger,
                timestampUtc,
                consumerIdentity,
                deliveryTag,
                correlationId,
                rabbitMqMessageId,
                replyTo,
                payload.Length,
                payloadUtf8Preview,
                payloadHexPreview,
                payloadSha256);
        }

        /// <summary>
        /// Emits the targeted JSON parse-failure diagnostic for a payload selected by correlation identifier.
        /// </summary>
        /// <param name="logger">Logger receiving the diagnostic event.</param>
        /// <param name="correlationId">AMQP correlation identifier for the rejected payload.</param>
        /// <param name="deliveryTag">Broker delivery tag for the rejected payload.</param>
        /// <param name="message">Parser-supplied JSON exception message.</param>
        /// <param name="path">JSON path reported by <see cref="JsonException"/>, when available.</param>
        /// <param name="lineNumber">Line number reported by <see cref="JsonException"/>, when available.</param>
        /// <param name="bytePositionInLine">Byte position within the line reported by <see cref="JsonException"/>, when available.</param>
        private static void LogPayloadDiagnosticJsonException(
            ILogger logger,
            string? correlationId,
            ulong deliveryTag,
            string message,
            string? path,
            long? lineNumber,
            long? bytePositionInLine)
        {
            LogPayloadDiagnosticParserJsonException(
                logger,
                correlationId,
                deliveryTag,
                message,
                path,
                lineNumber,
                bytePositionInLine);
        }

        /// <summary>
        /// Constructs the invalid-request result returned for malformed payloads or missing transport metadata.
        /// </summary>
        /// <param name="delivery">Delivery that could not be converted into a valid application request.</param>
        /// <param name="reason">Human-readable rejection reason surfaced to logs and any terminal invalid-request response.</param>
        /// <returns>A parse result whose <see cref="RabbitMqArticleWorkParseResult.Failure"/> is preclassified as <see cref="ArticleWorkProcessingOutcome.InvalidRequest"/>.</returns>
        private RabbitMqArticleWorkParseResult Failed(RabbitMqArticleDelivery delivery, string reason)
        {
            if (_logger.IsEnabled(LogLevel.Warning))
            {
                string payloadSha256 = Convert.ToHexString(SHA256.HashData(delivery.Payload.Span));

                LogArticleWorkRequestRejected(
                    _logger,
                    reason,
                    delivery.CorrelationId,
                    delivery.ReplyTo,
                    delivery.RabbitMqMessageId,
                    delivery.DeliveryTag,
                    delivery.Backbone,
                    delivery.Payload.Length,
                    payloadSha256);
            }

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
        /// Reads a required integer property from the request object without allocating an intermediate model.
        /// </summary>
        /// <param name="root">Root JSON object representing the payload.</param>
        /// <param name="propertyName">Required property name.</param>
        /// <param name="value">Parsed integer value when the property exists and is a valid <see cref="int"/>.</param>
        /// <returns><see langword="true"/> when the property exists, is numeric, and fits in <see cref="int"/>.</returns>
        private static bool TryReadRequiredInt32(JsonElement root, string propertyName, out int value)
        {
            value = default;
            return root.TryGetProperty(propertyName, out JsonElement property) && property.ValueKind == JsonValueKind.Number && property.TryGetInt32(out value);
        }

        /// <summary>
        /// Reads a required GUID property from the request object.
        /// </summary>
        /// <param name="root">Root JSON object representing the payload.</param>
        /// <param name="propertyName">Required property name.</param>
        /// <param name="value">Parsed GUID value when the property exists and contains a non-blank GUID string.</param>
        /// <returns><see langword="true"/> when the property exists, is a string, and parses as a GUID.</returns>
        private static bool TryReadRequiredGuid(JsonElement root, string propertyName, out Guid value)
        {
            value = Guid.Empty;
            return TryReadRequiredString(root, propertyName, out string? textValue) && !string.IsNullOrWhiteSpace(textValue) && Guid.TryParse(textValue, out value);
        }

        /// <summary>
        /// Reads a required string property from the request object.
        /// </summary>
        /// <param name="root">Root JSON object representing the payload.</param>
        /// <param name="propertyName">Required property name.</param>
        /// <param name="value">String value when the property exists and contains a JSON string.</param>
        /// <returns><see langword="true"/> when the property exists, is a JSON string, and deserializes to a non-null managed string.</returns>
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

        /// <summary>
        /// Writes the parser-entry payload diagnostic event.
        /// </summary>
        /// <remarks>
        /// The event is informational and can be suppressed by log filtering. Structured fields include payload previews plus a SHA-256 of the full body for later correlation without replaying the entire payload.
        /// </remarks>
        /// <param name="logger">The logger instance.</param>
        /// <param name="timestampUtc">The timestamp in UTC.</param>
        /// <param name="consumerIdentity">The consumer identity.</param>
        /// <param name="deliveryTag">The delivery tag.</param>
        /// <param name="correlationId">The correlation ID.</param>
        /// <param name="rabbitMqMessageId">The RabbitMQ message ID.</param>
        /// <param name="replyTo">The reply-to address.</param>
        /// <param name="payloadLength">The payload length.</param>
        /// <param name="payloadUtf8">The UTF-8 encoded payload.</param>
        /// <param name="payloadHex">The hexadecimal representation of the payload.</param>
        /// <param name="payloadSha256">The SHA-256 hash of the payload.</param>
        [LoggerMessage(EventId = 3500, Level = LogLevel.Information, Message = "RabbitMQ payload diagnostic parser-entry. TimestampUtc={TimestampUtc} ConsumerIdentity={ConsumerIdentity} DeliveryTag={DeliveryTag} CorrelationId={CorrelationId} RabbitMqMessageId={RabbitMqMessageId} ReplyTo={ReplyTo} PayloadLength={PayloadLength} PayloadUtf8={PayloadUtf8} PayloadHex={PayloadHex} PayloadSha256={PayloadSha256}")]
        private static partial void LogPayloadDiagnosticParserEntry(
            ILogger logger,
            DateTimeOffset timestampUtc,
            string consumerIdentity,
            ulong deliveryTag,
            string? correlationId,
            string? rabbitMqMessageId,
            string? replyTo,
            int payloadLength,
            string payloadUtf8,
            string payloadHex,
            string payloadSha256);

        /// <summary>
        /// Writes the targeted JSON parse-failure diagnostic event for a selected payload.
        /// </summary>
        /// <param name="logger">The logger instance.</param>
        /// <param name="correlationId">The correlation ID.</param>
        /// <param name="deliveryTag">The delivery tag.</param>
        /// <param name="jsonExceptionMessage">The JSON exception message.</param>
        /// <param name="jsonPath">The JSON path.</param>
        /// <param name="jsonLineNumber">The JSON line number.</param>
        /// <param name="jsonBytePositionInLine">The JSON byte position in line.</param>
        [LoggerMessage(EventId = 3501, Level = LogLevel.Warning, Message = "RabbitMQ payload diagnostic parser JsonException. CorrelationId={CorrelationId} DeliveryTag={DeliveryTag} JsonExceptionMessage={JsonExceptionMessage} JsonPath={JsonPath} JsonLineNumber={JsonLineNumber} JsonBytePositionInLine={JsonBytePositionInLine}")]
        private static partial void LogPayloadDiagnosticParserJsonException(
            ILogger logger,
            string? correlationId,
            ulong deliveryTag,
            string jsonExceptionMessage,
            string? jsonPath,
            long? jsonLineNumber,
            long? jsonBytePositionInLine);

        /// <summary>
        /// Writes the invalid-request rejection event emitted when a delivery cannot be converted into a canonical article-work request.
        /// </summary>
        /// <remarks>
        /// The structured payload digest allows correlating repeated malformed deliveries without logging the full message body.
        /// </remarks>
        /// <param name="logger">The logger instance.</param>
        /// <param name="reason">The reason for rejection.</param>
        /// <param name="correlationId">The correlation ID.</param>
        /// <param name="replyTo">The reply-to address.</param>
        /// <param name="rabbitMqMessageId">The RabbitMQ message ID.</param>
        /// <param name="deliveryTag">The delivery tag.</param>
        /// <param name="backbone">The backbone.</param>
        /// <param name="payloadLength">The payload length.</param>
        /// <param name="payloadSha256">The SHA-256 hash of the payload.</param>
        [LoggerMessage(EventId = 3502, Level = LogLevel.Warning, Message = "RabbitMQ article-work request rejected. Reason={Reason} CorrelationId={CorrelationId} ReplyTo={ReplyTo} RabbitMqMessageId={RabbitMqMessageId} DeliveryTag={DeliveryTag} Backbone={Backbone} PayloadLength={PayloadLength} PayloadSha256={PayloadSha256}")]
        private static partial void LogArticleWorkRequestRejected(
            ILogger logger,
            string reason,
            string? correlationId,
            string? replyTo,
            string? rabbitMqMessageId,
            ulong deliveryTag,
            string backbone,
            int payloadLength,
            string payloadSha256);
    }
}
