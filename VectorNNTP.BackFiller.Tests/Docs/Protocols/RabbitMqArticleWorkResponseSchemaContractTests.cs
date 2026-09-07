// <copyright file="RabbitMqArticleWorkResponseSchemaContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Docs / Protocols
// Focused schema contract tests for RabbitMQ article-work response JSON examples.

using System.Text.Json;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Docs.Protocols
{
    /// <summary>
    /// Verifies canonical response schema outcome contracts for success and terminal-failure payloads.
    /// </summary>
    public sealed class RabbitMqArticleWorkResponseSchemaContractTests
    {
        [Fact]
        public void Schema_WhenOutcomeSuccessWithConcreteUri_IsValid()
        {
            using JsonDocument payload = Parse("""
                {
                  "version": 1,
                  "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "Success",
                  "uri": "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160"
                }
                """);

            AssertValidV1Contract(payload.RootElement);
        }

        [Fact]
        public void Schema_WhenOutcomeSuccessWithNullUri_IsInvalid()
        {
            using JsonDocument payload = Parse("""
                {
                  "version": 1,
                  "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "Success",
                  "uri": null
                }
                """);

            Assert.Throws<InvalidOperationException>(() => AssertValidV1Contract(payload.RootElement));
        }

        [Fact]
        public void Schema_WhenOutcomeFailureWithErrorAndNoUri_IsValid()
        {
            using JsonDocument payload = Parse("""
                {
                  "version": 1,
                  "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "ArticleNotFound",
                  "error": "No article with that message-id"
                }
                """);

            AssertValidV1Contract(payload.RootElement);
        }

        private static JsonDocument Parse(string json)
        {
            return JsonDocument.Parse(json);
        }

        private static void AssertValidV1Contract(JsonElement root)
        {
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException("Response must be a JSON object.");
            }

            bool hasVersion = root.TryGetProperty("version", out JsonElement version) && version.ValueKind == JsonValueKind.Number && version.GetInt32() == 1;
            bool hasRequestId = root.TryGetProperty("requestId", out JsonElement requestId) && requestId.ValueKind == JsonValueKind.String && Guid.TryParse(requestId.GetString(), out _);
            bool hasMessageId = root.TryGetProperty("messageId", out JsonElement messageId) && messageId.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(messageId.GetString());
            bool hasBackbone = root.TryGetProperty("backbone", out JsonElement backbone) && backbone.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(backbone.GetString());
            bool hasOutcome = root.TryGetProperty("outcome", out JsonElement outcome) && outcome.ValueKind == JsonValueKind.String;

            if (!hasVersion || !hasRequestId || !hasMessageId || !hasBackbone || !hasOutcome)
            {
                throw new InvalidOperationException("Response is missing required base contract properties.");
            }

            string outcomeText = outcome.GetString()!;
            bool hasUri = root.TryGetProperty("uri", out JsonElement uri);
            bool hasError = root.TryGetProperty("error", out JsonElement error);

            switch (outcomeText)
            {
                case "Success":
                    if (!hasUri || uri.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(uri.GetString()))
                    {
                        throw new InvalidOperationException("Success outcome requires non-null string uri.");
                    }

                    if (hasError)
                    {
                        throw new InvalidOperationException("Success outcome must not contain error.");
                    }

                    break;

                case "ArticleNotFound":
                case "InvalidArticle":
                case "InvalidRequest":
                    if (!hasError || error.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(error.GetString()))
                    {
                        throw new InvalidOperationException("Terminal failure outcome requires non-empty error.");
                    }

                    if (hasUri)
                    {
                        throw new InvalidOperationException("Terminal failure outcome must not contain uri.");
                    }

                    break;

                default:
                    throw new InvalidOperationException($"Unsupported outcome '{outcomeText}'.");
            }
        }
    }
}
