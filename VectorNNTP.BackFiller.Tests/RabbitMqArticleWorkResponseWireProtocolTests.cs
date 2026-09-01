// <copyright file="RabbitMqArticleWorkResponseWireProtocolTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / Article Processing
// Focused contract tests for canonical response wire protocol serialization and parsing.

using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies deterministic response wire protocol shape and parser behavior.
    /// </summary>
    public sealed class RabbitMqArticleWorkResponseWireProtocolTests
    {
        [Fact]
        public void SerializeV1_WhenSuccess_ContainsCanonicalFieldsAndUriNull()
        {
            RabbitMqArticleWorkResponse response = new(
                Version: 1,
                RequestId: Guid.Parse("7c1cb8a0-95f9-4c13-8e53-339773e3afaa"),
                MessageId: "<12345@example.invalid>",
                Backbone: "Giganews",
                Outcome: nameof(ArticleWorkProcessingOutcome.Success),
                Uri: null,
                Error: null);

            byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
            string json = Encoding.UTF8.GetString(payload);

            Assert.Equal("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<12345@example.invalid>\",\"backbone\":\"Giganews\",\"outcome\":\"Success\",\"uri\":null}", json);
            Assert.DoesNotContain("correlationId", json, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("replyTo", json, StringComparison.OrdinalIgnoreCase);
        }

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
        public void ParseV1_WhenVersionUnsupported_ThrowsInvalidOperationException()
        {
            byte[] payload = Encoding.UTF8.GetBytes("{\"version\":2,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<m@example.invalid>\",\"backbone\":\"B\",\"outcome\":\"Success\"}");

            Assert.Throws<InvalidOperationException>(() => RabbitMqArticleWorkResponseWireProtocol.ParseV1(payload));
        }
    }
}
