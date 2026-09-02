// <copyright file="BackFillerFqdnGenerationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for back filler fqdn generation, covering configuration and validation contracts.

using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests for canonical BackFiller FQDN generation rules.
    /// </summary>
    public class BackFillerFqdnGenerationTests
    {
        /// <summary>
        /// Exercises build back filler fqdn  when inputs are valid  returns canonical fqdn behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void BuildBackFillerFqdn_WhenInputsAreValid_ReturnsCanonicalFqdn()
        {
            string fqdn = BackFillerIdentityValidator.BuildBackFillerFqdn(
                name: "nntpBackFiller",
                id: 1,
                dnsSuffix: "usenet.ninja");

            Assert.Equal("nntpbackfiller01.usenet.ninja", fqdn);
        }
        /// <summary>
        /// Exercises build back filler fqdn  when inputs contain whitespace  trims and normalizes behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void BuildBackFillerFqdn_WhenInputsContainWhitespace_TrimsAndNormalizes()
        {
            string fqdn = BackFillerIdentityValidator.BuildBackFillerFqdn(
                name: "  BackFiller  ",
                id: 9,
                dnsSuffix: "  Usenet.Ninja  ");

            Assert.Equal("backfiller09.usenet.ninja", fqdn);
        }
        /// <summary>
        /// Exercises build back filler fqdn  when id is valid  returns two digit id behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(0, "backfiller00.usenet.ninja")]
        [InlineData(1, "backfiller01.usenet.ninja")]
        [InlineData(9, "backfiller09.usenet.ninja")]
        [InlineData(99, "backfiller99.usenet.ninja")]
        public void BuildBackFillerFqdn_WhenIdIsValid_ReturnsTwoDigitId(int id, string expected)
        {
            string fqdn = BackFillerIdentityValidator.BuildBackFillerFqdn(
                name: "backfiller",
                id: id,
                dnsSuffix: "usenet.ninja");

            Assert.Equal(expected, fqdn);
        }
        /// <summary>
        /// Exercises build back filler fqdn  when name missing  throws argument exception behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void BuildBackFillerFqdn_WhenNameMissing_ThrowsArgumentException(string? name)
        {
            _ = Assert.ThrowsAny<ArgumentException>(() => BackFillerIdentityValidator.BuildBackFillerFqdn(
                name: name!,
                id: 1,
                dnsSuffix: "usenet.ninja"));
        }
        /// <summary>
        /// Exercises build back filler fqdn  when dns suffix missing  throws argument exception behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void BuildBackFillerFqdn_WhenDnsSuffixMissing_ThrowsArgumentException(string? dnsSuffix)
        {
            _ = Assert.ThrowsAny<ArgumentException>(() => BackFillerIdentityValidator.BuildBackFillerFqdn(
                name: "backfiller",
                id: 1,
                dnsSuffix: dnsSuffix!));
        }
        /// <summary>
        /// Exercises build back filler fqdn  when id out of range  throws argument out of range exception behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(-1)]
        [InlineData(100)]
        public void BuildBackFillerFqdn_WhenIdOutOfRange_ThrowsArgumentOutOfRangeException(int id)
        {
            _ = Assert.Throws<ArgumentOutOfRangeException>(() => BackFillerIdentityValidator.BuildBackFillerFqdn(
                name: "backfiller",
                id: id,
                dnsSuffix: "usenet.ninja"));
        }
        /// <summary>
        /// Exercises build back filler fqdn  when name uses allowed casing or hyphen  preserves canonicalization contract behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData("Back-Filler", "back-filler01.usenet.ninja")]
        [InlineData("BACKFILLER", "backfiller01.usenet.ninja")]
        [InlineData("backfiller", "backfiller01.usenet.ninja")]
        public void BuildBackFillerFqdn_WhenNameUsesAllowedCasingOrHyphen_PreservesCanonicalizationContract(string name, string expected)
        {
            string fqdn = BackFillerIdentityValidator.BuildBackFillerFqdn(
                name: name,
                id: 1,
                dnsSuffix: "usenet.ninja");

            Assert.Equal(expected, fqdn);
        }
    }
}
