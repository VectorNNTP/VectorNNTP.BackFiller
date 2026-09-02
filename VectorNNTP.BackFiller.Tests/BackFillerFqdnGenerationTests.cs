// <copyright file="BackFillerFqdnGenerationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for back filler fqdn generation.

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
        /// Verifies the BuildBackFillerFqdn_WhenInputsAreValid_ReturnsCanonicalFqdn scenario and expected contract.
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
        /// Verifies the BuildBackFillerFqdn_WhenInputsContainWhitespace_TrimsAndNormalizes scenario and expected contract.
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
        /// Verifies the BuildBackFillerFqdn_WhenIdIsValid_ReturnsTwoDigitId scenario and expected contract.
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
        /// Verifies the BuildBackFillerFqdn_WhenNameMissing_ThrowsArgumentException scenario and expected contract.
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
        /// Verifies the BuildBackFillerFqdn_WhenDnsSuffixMissing_ThrowsArgumentException scenario and expected contract.
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
        /// Verifies the BuildBackFillerFqdn_WhenIdOutOfRange_ThrowsArgumentOutOfRangeException scenario and expected contract.
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
        /// Verifies the BuildBackFillerFqdn_WhenNameUsesAllowedCasingOrHyphen_PreservesCanonicalizationContract scenario and expected contract.
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
