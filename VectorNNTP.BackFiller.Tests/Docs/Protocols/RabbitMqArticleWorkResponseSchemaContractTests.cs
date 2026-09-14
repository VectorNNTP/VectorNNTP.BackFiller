// <copyright file="RabbitMqArticleWorkResponseSchemaContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Docs / Protocols
// Focused schema contract tests for RabbitMQ article-work response JSON examples.

using NJsonSchema;
using NJsonSchema.Validation;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
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

        [Fact]
        public void Schema_WhenOutcomeInvalidRequestWithUnavailableIdentityAndError_IsValid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": null,
                  "messageId": null,
                  "backbone": null,
                  "outcome": "InvalidRequest",
                  "error": "Request payload was invalid."
                }
                """);

            Assert.Empty(errors);
        }

        [Fact]
        public void Schema_WhenOutcomeInvalidRequestUsesNilRequestId_IsInvalid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": "00000000-0000-0000-0000-000000000000",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "InvalidRequest",
                  "error": "Request payload was invalid."
                }
                """);

            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("requestId", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void Schema_WhenOutcomeSuccessUsesNilRequestId_IsInvalid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": "00000000-0000-0000-0000-000000000000",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "Success",
                  "uri": "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160"
                }
                """);

            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("requestId", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void Schema_WhenOutcomeArticleNotFoundUsesNilRequestId_IsInvalid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": "00000000-0000-0000-0000-000000000000",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "ArticleNotFound",
                  "error": "No article with that message-id"
                }
                """);

            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("requestId", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void Schema_WhenOutcomeInvalidArticleUsesNilRequestId_IsInvalid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": "00000000-0000-0000-0000-000000000000",
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "InvalidArticle",
                  "error": "Article content was invalid."
                }
                """);

            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("requestId", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void Schema_WhenOutcomeArticleNotFoundWithUnavailableIdentity_IsInvalid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": null,
                  "messageId": "<12345@example.invalid>",
                  "backbone": "Giganews",
                  "outcome": "ArticleNotFound",
                  "error": "No article with that message-id"
                }
                """);

            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("requestId", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void Schema_WhenOutcomeInvalidRequestMissingError_IsInvalid()
        {
            ICollection<ValidationError> errors = Validate("""
                {
                  "version": 1,
                  "requestId": null,
                  "messageId": null,
                  "backbone": null,
                  "outcome": "InvalidRequest"
                }
                """);

            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("error", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData("ArticleNotFound")]
        [InlineData("InvalidArticle")]
        [InlineData("InvalidRequest")]
        public void Schema_WhenOutcomeErrorWhitespaceOnly_IsInvalid(string outcome)
        {
            string json = $$"""
                {
                  "version": 1,
                  "requestId": {{(string.Equals(outcome, "InvalidRequest", StringComparison.Ordinal) ? "null" : "\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\"")}},
                  "messageId": {{(string.Equals(outcome, "InvalidRequest", StringComparison.Ordinal) ? "null" : "\"<12345@example.invalid>\"")}},
                  "backbone": {{(string.Equals(outcome, "InvalidRequest", StringComparison.Ordinal) ? "null" : "\"Giganews\"")}},
                  "outcome": "{{outcome}}",
                  "error": "   "
                }
                """;

            ICollection<ValidationError> errors = Validate(json);
            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("error", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData("ArticleNotFound")]
        [InlineData("InvalidArticle")]
        [InlineData("InvalidRequest")]
        public void Schema_WhenOutcomeErrorTabsOrNewlinesOnly_IsInvalid(string outcome)
        {
            string json = $$"""
                {
                  "version": 1,
                  "requestId": {{(string.Equals(outcome, "InvalidRequest", StringComparison.Ordinal) ? "null" : "\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\"")}},
                  "messageId": {{(string.Equals(outcome, "InvalidRequest", StringComparison.Ordinal) ? "null" : "\"<12345@example.invalid>\"")}},
                  "backbone": {{(string.Equals(outcome, "InvalidRequest", StringComparison.Ordinal) ? "null" : "\"Giganews\"")}},
                  "outcome": "{{outcome}}",
                  "error": "\n\t"
                }
                """;

            ICollection<ValidationError> errors = Validate(json);
            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("error", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData("Success")]
        [InlineData("ArticleNotFound")]
        [InlineData("InvalidArticle")]
        [InlineData("InvalidRequest")]
        public void Schema_WhenOutcomeBackboneWhitespaceOnly_IsInvalid(string outcome)
        {
            string json = outcome switch
            {
                "Success" => """
                    {
                      "version": 1,
                      "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                      "messageId": "<12345@example.invalid>",
                      "backbone": "   ",
                      "outcome": "Success",
                      "uri": "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160"
                    }
                    """,
                "ArticleNotFound" => """
                    {
                      "version": 1,
                      "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                      "messageId": "<12345@example.invalid>",
                      "backbone": "\t\n",
                      "outcome": "ArticleNotFound",
                      "error": "No article with that message-id"
                    }
                    """,
                "InvalidArticle" => """
                    {
                      "version": 1,
                      "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                      "messageId": "<12345@example.invalid>",
                      "backbone": "\t",
                      "outcome": "InvalidArticle",
                      "error": "Article content was invalid."
                    }
                    """,
                "InvalidRequest" => """
                    {
                      "version": 1,
                      "requestId": null,
                      "messageId": null,
                      "backbone": "\n",
                      "outcome": "InvalidRequest",
                      "error": "Request payload was invalid."
                    }
                    """,
                _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null),
            };

            ICollection<ValidationError> errors = Validate(json);
            Assert.NotEmpty(errors);
            Assert.Contains(errors, static error =>
                error.ToString().Contains("backbone", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData("messageId", "123")]
        [InlineData("messageId", "true")]
        [InlineData("messageId", "[]")]
        [InlineData("messageId", "{}")]
        [InlineData("backbone", "123")]
        [InlineData("backbone", "true")]
        [InlineData("backbone", "[]")]
        [InlineData("backbone", "{}")]
        [InlineData("error", "123")]
        [InlineData("error", "true")]
        [InlineData("error", "[]")]
        [InlineData("error", "{}")]
        [InlineData("uri", "123")]
        [InlineData("uri", "true")]
        [InlineData("uri", "[]")]
        [InlineData("uri", "{}")]
        public void Schema_WhenPropertyHasWrongJsonType_IsInvalid(string propertyName, string rawJsonValue)
        {
            string payload = propertyName switch
            {
                "messageId" => $$"""
                    {
                      "version": 1,
                      "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                      "messageId": {{rawJsonValue}},
                      "backbone": "Giganews",
                      "outcome": "Success",
                      "uri": "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160"
                    }
                    """,
                "backbone" => $$"""
                    {
                      "version": 1,
                      "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                      "messageId": "<12345@example.invalid>",
                      "backbone": {{rawJsonValue}},
                      "outcome": "Success",
                      "uri": "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160"
                    }
                    """,
                "error" => $$"""
                    {
                      "version": 1,
                      "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                      "messageId": "<12345@example.invalid>",
                      "backbone": "Giganews",
                      "outcome": "ArticleNotFound",
                      "error": {{rawJsonValue}}
                    }
                    """,
                "uri" => $$"""
                    {
                      "version": 1,
                      "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                      "messageId": "<12345@example.invalid>",
                      "backbone": "Giganews",
                      "outcome": "Success",
                      "uri": {{rawJsonValue}}
                    }
                    """,
                _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, null),
            };

            ICollection<ValidationError> errors = Validate(payload);
            Assert.NotEmpty(errors);
            Assert.Contains(errors, error =>
                error.ToString().Contains(propertyName, StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void Schema_WhenOutcomeSuccessMessageIdCanonicalityMatchesRuntimeVectors()
        {
            string[] validMessageIds =
            [
                "<12345@example.invalid>",
                "<abc.def+tag@example.invalid>",
                "<a@b>",
                "<abc@[127.0.0.1]>",
                "<abc@[IPv6:2001:db8::1]>",
                "<abc@[x@y]>",
                BuildMessageIdWithTotalLength(250)
            ];

            string[] invalidMessageIds =
            [
                "not-message-id",
                string.Empty,
                "   ",
                "<double..dot@example.com>",
                "<\"quoted\"@example.com>",
                "<nodomain@>",
                "<@example.com>",
                "<abc@example..com>",
                "<abc@exa mple.com>",
                "<abc@@example.com>",
                "<abc@[127.0.0.1>",
                "<abc@[]>",
                "abc@example.com",
                "<abc@example.com",
                "abc@example.com>",
                BuildMessageIdWithTotalLength(251)
            ];

            foreach (string messageId in validMessageIds)
            {
                Assert.True(NntpMessageIdValidation.IsValidMessageId(messageId), $"Runtime validator unexpectedly rejected: {messageId}");
                ICollection<ValidationError> validErrors = Validate(BuildSuccessPayloadWithMessageId(messageId));
                Assert.Empty(validErrors);
            }

            foreach (string messageId in invalidMessageIds)
            {
                Assert.False(NntpMessageIdValidation.IsValidMessageId(messageId), $"Runtime validator unexpectedly accepted: {messageId}");
                ICollection<ValidationError> invalidErrors = Validate(BuildSuccessPayloadWithMessageId(messageId));
                Assert.NotEmpty(invalidErrors);
                Assert.Contains(invalidErrors, static error =>
                    error.ToString().Contains("messageId", StringComparison.OrdinalIgnoreCase));
            }
        }

        [Fact]
        public void Schema_WhenOutcomeInvalidRequestHasStringMessageId_CanonicalityIsEnforced()
        {
            const string validMessageId = "<abc@[127.0.0.1]>";
            const string invalidMessageId = "<double..dot@example.com>";

            Assert.True(NntpMessageIdValidation.IsValidMessageId(validMessageId));
            Assert.False(NntpMessageIdValidation.IsValidMessageId(invalidMessageId));

            ICollection<ValidationError> validErrors = Validate("""
                {
                  "version": 1,
                  "requestId": null,
                  "messageId": "<abc@[127.0.0.1]>",
                  "backbone": null,
                  "outcome": "InvalidRequest",
                  "error": "Request payload was invalid."
                }
                """);
            Assert.Empty(validErrors);

            ICollection<ValidationError> invalidErrors = Validate("""
                {
                  "version": 1,
                  "requestId": null,
                  "messageId": "<double..dot@example.com>",
                  "backbone": null,
                  "outcome": "InvalidRequest",
                  "error": "Request payload was invalid."
                }
                """);
            Assert.NotEmpty(invalidErrors);
            Assert.Contains(invalidErrors, static error =>
                error.ToString().Contains("messageId", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData("cache://backfiller01.usenet.ninja:1/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01.usenet.ninja:65535/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://bf01.example:443/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
        public void Schema_WhenOutcomeSuccessWithCanonicalUriVariants_IsValid(string uri)
        {
            ICollection<ValidationError> errors = Validate(BuildSuccessPayload(uri));
            Assert.Empty(errors);
        }

        [Theory]
        [InlineData("cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe16")]
        [InlineData("cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe1600")]
        [InlineData("cache://backfiller01.usenet.ninja:119/30EDC94157AA16FE644A45A1F1FFE160")]
        [InlineData("cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe16g")]
        [InlineData("cache://backfiller01.usenet.ninja:0/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01.usenet.ninja:65536/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01.usenet.ninja:01/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01.usenet.ninja:/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01.usenet.ninja/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01..usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://-backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01-.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://backfiller01.usenet.ninja:119:120/30edc94157aa16fe644a45a1f1ffe160")]
        [InlineData("cache://user@backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160")]
        public void Schema_WhenOutcomeSuccessWithMalformedUri_IsInvalid(string uri)
        {
            ICollection<ValidationError> errors = Validate(BuildSuccessPayload(uri));
            Assert.NotEmpty(errors);
        }

        private static ICollection<ValidationError> Validate(string json)
        {
            return ResponseSchema.Value.Validate(json);
        }

        private static string BuildSuccessPayload(string uri)
        {
            return BuildSuccessPayloadWithMessageId("<12345@example.invalid>", uri);
        }

        private static string BuildSuccessPayloadWithMessageId(string messageId, string uri = "cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160")
        {
            string escapedMessageId = messageId.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal);
            return $$"""
                {
                  "version": 1,
                  "requestId": "7c1cb8a0-95f9-4c13-8e53-339773e3afaa",
                  "messageId": "{{escapedMessageId}}",
                  "backbone": "Giganews",
                  "outcome": "Success",
                  "uri": "{{uri}}"
                }
                """;
        }

        private static string BuildMessageIdWithTotalLength(int totalLength)
        {
            const string domainPart = "@example.invalid>";
            if (totalLength < 3 + domainPart.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(totalLength));
            }

            int localLength = totalLength - 1 - domainPart.Length;
            return $"<{new string('a', localLength)}{domainPart}";
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
