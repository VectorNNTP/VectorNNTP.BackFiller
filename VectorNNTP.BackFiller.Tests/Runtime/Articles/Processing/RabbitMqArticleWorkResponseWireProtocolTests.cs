// <copyright file="RabbitMqArticleWorkResponseWireProtocolTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for rabbit mq article work response wire protocol, covering NNTP article and transport behavior; dependency integration and failure handling.
// Primary responsibility: documents the executable contracts covered by the rabbit mq article work response wire protocol test suite.

using System.Text;
using System.Text.Json;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Processing
{
    /// <summary>
    /// Verifies deterministic response wire protocol shape and parser behavior.
    /// </summary>
    public sealed class RabbitMqArticleWorkResponseWireProtocolTests
    {
        /// <summary>
        /// Confirms the serialize v1 when success contains canonical fields and concrete uri behavior.
        /// </summary>
        [Fact]
        public void SerializeV1_WhenSuccess_ContainsCanonicalFieldsAndConcreteUri()
        {
            RabbitMqArticleWorkResponse response = new(
                Version: 1,
                RequestId: Guid.Parse("7c1cb8a0-95f9-4c13-8e53-339773e3afaa"),
                MessageId: "<12345@example.invalid>",
                Backbone: "Giganews",
                Outcome: nameof(ArticleWorkProcessingOutcome.Success),
                Uri: "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160",
                Error: null);

            byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
            string json = Encoding.UTF8.GetString(payload);

            Assert.Equal("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<12345@example.invalid>\",\"backbone\":\"Giganews\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160\"}", json);
            Assert.DoesNotContain("correlationId", json, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("replyTo", json, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Confirms the serialize v1 when terminal failure contains error and no uri behavior.
        /// </summary>
        [Fact]
        public void SerializeV1_WhenTerminalFailure_ContainsErrorAndNoUri()
        {
            RabbitMqArticleWorkResponse response = new(
                Version: 1,
                RequestId: Guid.Parse("eb540d49-c8f1-49ce-92ee-8ebd33662ab7"),
                MessageId: "<missing@example.invalid>",
                Backbone: "Eweka",
                Outcome: nameof(ArticleWorkProcessingOutcome.ArticleNotFound),
                Uri: null,
                Error: "No article with that message-id");

            byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
            string json = Encoding.UTF8.GetString(payload);

            Assert.Contains("\"error\":\"No article with that message-id\"", json, StringComparison.Ordinal);
            Assert.DoesNotContain("\"uri\"", json, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms contract-invalid responses are rejected before any serialization output is emitted, including success responses missing canonical URI.
        /// </summary>
        [Theory]
        [InlineData(1, "7c1cb8a0-95f9-4c13-8e53-339773e3afaa", "<12345@example.invalid>", "Giganews", "Success", null, null)]
        [InlineData(1, "7c1cb8a0-95f9-4c13-8e53-339773e3afaa", "not-message-id", "Giganews", "Success", "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160", null)]
        [InlineData(1, "7c1cb8a0-95f9-4c13-8e53-339773e3afaa", "<12345@example.invalid>", "Giganews", "Success", "cache://backfiller01.usenet.ninja:119/de438dc83d64b1fa9206cf4da9eed5cc", null)]
        [InlineData(1, "7c1cb8a0-95f9-4c13-8e53-339773e3afaa", "<12345@example.invalid>", "Giganews", "Success", "cache://bad host:119/30edc94157aa16fe644a45a1f1ffe160", null)]
        [InlineData(1, "7c1cb8a0-95f9-4c13-8e53-339773e3afaa", "<12345@example.invalid>", "Giganews", "Success", "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160", "forbidden")]
        [InlineData(1, null, "<missing@example.invalid>", "Eweka", "ArticleNotFound", null, "No article")]
        [InlineData(1, "eb540d49-c8f1-49ce-92ee-8ebd33662ab7", "not-message-id", "Eweka", "ArticleNotFound", null, "No article")]
        [InlineData(1, "eb540d49-c8f1-49ce-92ee-8ebd33662ab7", "<missing@example.invalid>", "Eweka", null, null, "No article")]
        [InlineData(1, "eb540d49-c8f1-49ce-92ee-8ebd33662ab7", "<missing@example.invalid>", "Eweka", "ArticleNotFound", "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160", "No article")]
        [InlineData(1, "eb540d49-c8f1-49ce-92ee-8ebd33662ab7", "<missing@example.invalid>", "Eweka", "ArticleNotFound", null, "   ")]
        [InlineData(1, "eb540d49-c8f1-49ce-92ee-8ebd33662ab7", "<missing@example.invalid>", "Eweka", "InvalidArticle", null, "   ")]
        [InlineData(1, "00000000-0000-0000-0000-000000000000", null, null, "InvalidRequest", null, "Request payload was invalid.")]
        [InlineData(1, null, "not-message-id", null, "InvalidRequest", null, "Request payload was invalid.")]
        [InlineData(1, null, null, "   ", "InvalidRequest", null, "Request payload was invalid.")]
        [InlineData(1, null, null, null, "InvalidRequest", "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160", "Request payload was invalid.")]
        [InlineData(1, null, null, null, "InvalidRequest", null, "\t")]
        [InlineData(1, "7c1cb8a0-95f9-4c13-8e53-339773e3afaa", "<12345@example.invalid>", "Giganews", "ProviderFailure", null, "provider")]
        public void SerializeV1_WhenContractInvalid_ThrowsInvalidOperationException(
            int version,
            string? requestId,
            string? messageId,
            string? backbone,
            string? outcome,
            string? uri,
            string? error)
        {
            Guid? parsedRequestId = requestId is null ? null : Guid.Parse(requestId);
            RabbitMqArticleWorkResponse response = new(
                Version: version,
                RequestId: parsedRequestId,
                MessageId: messageId,
                Backbone: backbone,
                Outcome: outcome!,
                Uri: uri,
                Error: error);

            Assert.Throws<InvalidOperationException>(() => RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response));
        }

        [Fact]
        public void SerializeV1_WhenValidSuccessWithBoundUri_WritesDeterministicPayload()
        {
            RabbitMqArticleWorkResponse response = new(
                Version: 1,
                RequestId: Guid.Parse("7c1cb8a0-95f9-4c13-8e53-339773e3afaa"),
                MessageId: "<12345@example.invalid>",
                Backbone: "Giganews",
                Outcome: nameof(ArticleWorkProcessingOutcome.Success),
                Uri: "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160",
                Error: null);

            byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
            string json = Encoding.UTF8.GetString(payload);

            Assert.Equal("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<12345@example.invalid>\",\"backbone\":\"Giganews\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160\"}", json);
        }
        /// <summary>
        /// Confirms the parse v1 when payload is valid round trips canonical fields behavior.
        /// </summary>
        [Fact]
        public void ParseV1_WhenPayloadIsValid_RoundTripsCanonicalFields()
        {
            RabbitMqArticleWorkResponse source = new(
                Version: 1,
                RequestId: Guid.NewGuid(),
                MessageId: "<roundtrip@example.invalid>",
                Backbone: "BackboneA",
                Outcome: nameof(ArticleWorkProcessingOutcome.InvalidRequest),
                Uri: null,
                Error: "Request payload was invalid.");

            byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(source);
            RabbitMqArticleWorkResponse parsed = RabbitMqArticleWorkResponseWireProtocol.ParseV1(payload);

            Assert.Equal(source.Version, parsed.Version);
            Assert.Equal(source.RequestId, parsed.RequestId);
            Assert.Equal(source.MessageId, parsed.MessageId);
            Assert.Equal(source.Backbone, parsed.Backbone);
            Assert.Equal(source.Outcome, parsed.Outcome);
            Assert.Equal(source.Error, parsed.Error);
        }

        [Fact]
        public void SerializeAndParseV1_WhenInvalidRequestIdentityUnavailable_RoundTripsNullIdentity()
        {
            RabbitMqArticleWorkResponse source = new(
                Version: 1,
                RequestId: null,
                MessageId: null,
                Backbone: null,
                Outcome: nameof(ArticleWorkProcessingOutcome.InvalidRequest),
                Uri: null,
                Error: "Request payload was invalid.");

            byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(source);
            RabbitMqArticleWorkResponse parsed = RabbitMqArticleWorkResponseWireProtocol.ParseV1(payload);

            Assert.Null(parsed.RequestId);
            Assert.Null(parsed.MessageId);
            Assert.Null(parsed.Backbone);
            Assert.Equal(source.Error, parsed.Error);
        }

        [Fact]
        public void ParseV1_WhenInvalidRequestIdentityNull_IsAccepted()
        {
            byte[] payload = Encoding.UTF8.GetBytes("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}");
            RabbitMqArticleWorkResponse parsed = RabbitMqArticleWorkResponseWireProtocol.ParseV1(payload);

            Assert.Null(parsed.RequestId);
            Assert.Null(parsed.MessageId);
            Assert.Null(parsed.Backbone);
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.InvalidRequest), parsed.Outcome);
        }

        [Theory]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":null,\"backbone\":\"B\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":null,\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":123,\"backbone\":\"B\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":true,\"backbone\":\"B\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":123,\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":[],\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"ArticleNotFound\",\"error\":\"No article\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"00000000-0000-0000-0000-000000000000\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":\"\",\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":\"   \",\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":\"not-message-id\",\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":123,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":\"\",\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":\"   \",\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":123,\"outcome\":\"InvalidRequest\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"ProviderFailure\",\"error\":\"Provider failure\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Cancelled\",\"error\":\"Cancelled\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"UnexpectedFailure\",\"error\":\"Unexpected failure\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"SomeUnknownOutcome\",\"error\":\"Unknown\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"error\":null,\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"error\":123,\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"error\":true,\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"error\":[],\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"error\":{},\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"error\":\"must-not-exist\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"uri\":\"cache://bad host:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"ArticleNotFound\",\"error\":\"No article\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"InvalidArticle\",\"error\":\"Invalid article\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"ArticleNotFound\",\"error\":\"\"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"InvalidArticle\",\"error\":\"   \"}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"ArticleNotFound\",\"error\":\"No article\",\"uri\":false}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"InvalidArticle\",\"error\":\"Invalid\",\"uri\":123}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"ArticleNotFound\",\"error\":\"No article\",\"uri\":[]}")]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"InvalidArticle\",\"error\":\"Invalid\",\"uri\":{}}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"uri\":\"cache://backfiller01.usenet.ninja:119/86590d16c882d3a0b763fdb57cb0ce2f\",\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"uri\":null,\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"uri\":false,\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"uri\":123,\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"uri\":[],\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"uri\":{},\"error\":\"Request payload was invalid.\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"   \"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"\\n\"}")]
        [InlineData("{\"version\":1,\"requestId\":null,\"messageId\":null,\"backbone\":null,\"outcome\":\"InvalidRequest\",\"error\":\"\\t\"}")]
        public void ParseV1_WhenConcreteIdentityRequiredButMissingOrInvalid_ThrowsInvalidOperationException(string json)
        {
            byte[] payload = Encoding.UTF8.GetBytes(json);
            Assert.Throws<InvalidOperationException>(() => RabbitMqArticleWorkResponseWireProtocol.ParseV1(payload));
        }

        [Fact]
        public void ParseV1_WhenValidSuccessMessageIdAndUriHashBinding_IsAccepted()
        {
            byte[] payload = Encoding.UTF8.GetBytes("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\",\"uri\":\"cache://backfiller01.usenet.ninja:119/ae0e21405948aff3dcc95daa2560485a\"}");

            RabbitMqArticleWorkResponse parsed = RabbitMqArticleWorkResponseWireProtocol.ParseV1(payload);
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.Success), parsed.Outcome);
            Assert.Equal("<m@example.invalid>", parsed.MessageId);
            Assert.Equal("cache://backfiller01.usenet.ninja:119/ae0e21405948aff3dcc95daa2560485a", parsed.Uri);
        }

        /// <summary>
        /// Confirms the parse v1 when version unsupported throws invalid operation exception behavior.
        /// </summary>
        [Fact]
        public void ParseV1_WhenVersionUnsupported_ThrowsInvalidOperationException()
        {
            byte[] payload = Encoding.UTF8.GetBytes("{\"version\":2,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\"}");

            Assert.Throws<InvalidOperationException>(() => RabbitMqArticleWorkResponseWireProtocol.ParseV1(payload));
        }
    }
}
