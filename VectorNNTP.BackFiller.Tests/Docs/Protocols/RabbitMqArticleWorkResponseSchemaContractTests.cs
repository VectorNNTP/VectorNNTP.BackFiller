// <copyright file="RabbitMqArticleWorkResponseSchemaContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Docs / Protocols
// Focused schema contract tests for RabbitMQ article-work response JSON examples.

using NJsonSchema;
using NJsonSchema.Validation;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Docs.Protocols
{
    /// <summary>
    /// Verifies canonical response schema outcome contracts for success and terminal-failure payloads.
    /// </summary>
    public sealed class RabbitMqArticleWorkResponseSchemaContractTests
    {
        private static readonly Lazy<JsonSchema> ResponseSchema = new(LoadSchema);

        [Fact]
        public void Schema_WhenOutcomeSuccessWithConcreteUri_IsValid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "Success",
                  "uri": "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160"
                }
                """);

            Assert.Empty(errors);
        }

        [Fact]
        public void Schema_WhenOutcomeSuccessWithNullUri_IsInvalid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "Success",
                  "uri": null
                }
                """);

            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                string.Equals(error.Path, "#.uri", StringComparison.Ordinal)
                || string.Equals(error.Path, "uri", StringComparison.Ordinal)
                || error.ToString().Contains("uri", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void Schema_WhenOutcomeFailureWithErrorAndNoUri_IsValid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "ArticleNotFound",
                  "error": "No article with that message-id"
                }
                """);

            Assert.Empty(errors);
        }

        private static ICollection<ValidationError> Validate(string json)
        {
            return ResponseSchema.Value.Validate(json);
        }

        private static JsonSchema LoadSchema()
        {
            string schemaPath = Path.Combine(
                AppContext.BaseDirectory,
                "Docs",
                "Protocols",
                "RabbitMqArticleWorkResponse.v1.schema.json");

            if (!File.Exists(schemaPath))
            {
                throw new FileNotFoundException($"Schema file was not found at '{schemaPath}'.", schemaPath);
            }

            string schemaJson = File.ReadAllText(schemaPath);
            return JsonSchema.FromJsonAsync(schemaJson).GetAwaiter().GetResult();
        }
    }
}
