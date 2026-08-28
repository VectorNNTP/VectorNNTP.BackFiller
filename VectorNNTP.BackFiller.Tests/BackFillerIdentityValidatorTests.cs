// <copyright file="BackFillerIdentityValidatorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests for BackFiller Name/Id/DnsSuffix validation and FQDN suitability.
    /// </summary>
    public class BackFillerIdentityValidatorTests
    {
        [Fact]
        public void Validate_WhenInputsAreValid_ReturnsNoErrors()
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: "NNTPBACKFILLER",
                id: 1,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }

        [Fact]
        public void Validate_WhenDnsSuffixIsValidMultiLabel_ReturnsNoDnsSuffixError()
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: "backfiller",
                id: 1,
                dnsSuffix: "prod.usenet.ninja",
                settingPrefix: "BackFiller");

            Assert.DoesNotContain(
                diagnostics,
                static d => d.Setting == "BackFiller:DnsSuffix"
                           && d.Severity == ValidationSeverity.Error);
        }

        [Theory]
        [InlineData("https://usenet.ninja")]
        [InlineData("usenet.ninja:443")]
        [InlineData("usenet.ninja/path")]
        [InlineData("usenet ninja")]
        public void Validate_WhenDnsSuffixContainsInvalidComponents_ReturnsDnsSuffixError(string dnsSuffix)
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: "backfiller",
                id: 1,
                dnsSuffix: dnsSuffix,
                settingPrefix: "BackFiller");

            BackFillerIdentityValidationResult error = Assert.Single(
                diagnostics,
                static d => d.Severity == ValidationSeverity.Error);

            Assert.Equal("BackFiller:DnsSuffix", error.Setting);
        }

        [Fact]
        public void Validate_WhenDnsSuffixMissing_ReturnsDnsSuffixError()
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: "backfiller",
                id: 1,
                dnsSuffix: null,
                settingPrefix: "BackFiller");

            BackFillerIdentityValidationResult error = Assert.Single(
                diagnostics,
                static d => d.Severity == ValidationSeverity.Error);

            Assert.Equal("BackFiller:DnsSuffix", error.Setting);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void Validate_WhenNameIsMissing_ReturnsNameError(string? name)
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: name,
                id: 1,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            BackFillerIdentityValidationResult error = Assert.Single(
                diagnostics,
                static d => d.Severity == ValidationSeverity.Error);

            Assert.Equal("BackFiller:Name", error.Setting);
        }

        [Theory]
        [InlineData("a")]
        [InlineData("backfiller")]
        [InlineData("back-filler")]
        [InlineData("BACKFILLER")]
        [InlineData("a1")]
        public void Validate_WhenNameIsValidDnsLabel_ReturnsNoNameError(string name)
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: name,
                id: 1,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            Assert.DoesNotContain(
                diagnostics,
                static d => d.Setting == "BackFiller:Name"
                           && d.Severity == ValidationSeverity.Error);
        }

        [Theory]
        [InlineData("back filler")]
        [InlineData("back/filler")]
        [InlineData("back.filler")]
        [InlineData("back_filler")]
        [InlineData("back@filler")]
        public void Validate_WhenNameContainsInvalidDnsLabelCharacters_ReturnsNameError(string name)
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: name,
                id: 1,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            BackFillerIdentityValidationResult error = Assert.Single(
                diagnostics,
                static d => d.Severity == ValidationSeverity.Error);

            Assert.Equal("BackFiller:Name", error.Setting);
            Assert.Contains("valid DNS label", error.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData("-backfiller")]
        [InlineData("backfiller-")]
        public void Validate_WhenNameHasInvalidHyphenPlacement_ReturnsNameError(string name)
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: name,
                id: 1,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            BackFillerIdentityValidationResult error = Assert.Single(
                diagnostics,
                static d => d.Severity == ValidationSeverity.Error);

            Assert.Equal("BackFiller:Name", error.Setting);
            Assert.Contains("valid DNS label", error.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Validate_WhenNamePlusIdProducesHostLabelOverMaximumLength_ReturnsNameError()
        {
            string name = new('a', 63);

            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: name,
                id: 1,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            BackFillerIdentityValidationResult error = Assert.Single(
                diagnostics,
                static d => d.Severity == ValidationSeverity.Error);

            Assert.Equal("BackFiller:Name", error.Setting);
            Assert.Contains("invalid host label", error.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(99)]
        public void Validate_WhenIdIsInAllowedRange_ReturnsNoIdError(int id)
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: "backfiller",
                id: id,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            Assert.DoesNotContain(
                diagnostics,
                static d => d.Setting == "BackFiller:Id"
                           && d.Severity == ValidationSeverity.Error);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(100)]
        public void Validate_WhenIdIsOutsideAllowedRange_ReturnsIdError(int id)
        {
            List<BackFillerIdentityValidationResult> diagnostics = BackFillerIdentityValidator.Validate(
                name: "backfiller",
                id: id,
                dnsSuffix: "usenet.ninja",
                settingPrefix: "BackFiller");

            BackFillerIdentityValidationResult error = Assert.Single(
                diagnostics,
                static d => d.Severity == ValidationSeverity.Error);

            Assert.Equal("BackFiller:Id", error.Setting);
            Assert.Contains("between 0 and 99", error.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(0, "00")]
        [InlineData(1, "01")]
        [InlineData(99, "99")]
        public void FormatBackFillerId_WhenIdIsInRange_ReturnsTwoDigitZeroPaddedValue(int id, string expected)
        {
            string formatted = BackFillerIdentityValidator.FormatBackFillerId(id);

            Assert.Equal(expected, formatted);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(100)]
        public void FormatBackFillerId_WhenIdIsOutOfRange_ThrowsArgumentOutOfRangeException(int id)
        {
            _ = Assert.Throws<ArgumentOutOfRangeException>(() => BackFillerIdentityValidator.FormatBackFillerId(id));
        }

        [Fact]
        public void BackFillerOptions_DefaultsDnsSuffixToUsenetNinja()
        {
            BackFillerOptions options = new();

            Assert.Equal("usenet.ninja", options.DnsSuffix);
        }
    }
}
