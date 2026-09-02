// <copyright file="TransitBenchmarkCliOptionsTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for transit benchmark cli options, covering NNTP article and transport behavior; benchmark measurement and runtime identity contracts.

using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates benchmark CLI option parsing contracts for fixed-count mode.
    /// </summary>
    public sealed class TransitBenchmarkCliOptionsTests
    {
        /// <summary>
        /// Exercises parse  when article count provided  parses expected value behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void Parse_WhenArticleCountProvided_ParsesExpectedValue()
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse(["--article-count", "200"]);

            Assert.Equal(200, options.ArticleCount);
        }

        /// <summary>
        /// Verifies invalid non-positive article-count values are rejected.
        /// </summary>
        [Theory]
        [InlineData("0")]
        [InlineData("-1")]
        public void Parse_WhenArticleCountIsInvalid_ThrowsArgumentException(string raw)
        {
            _ = Assert.Throws<ArgumentException>(() => TransitBenchmarkCliOptions.Parse(["--article-count", raw]));
        }

        /// <summary>
        /// Verifies production dependency identity options are parsed for runtime provenance checks.
        /// </summary>
        [Fact]
        public void Parse_WhenProductionDependencyIdentityOptionsProvided_ParsesExpectedValues()
        {
            TransitBenchmarkCliOptions options = TransitBenchmarkCliOptions.Parse([
                "--expected-production-assembly-path", @"C:\bench\VectorNNTP.BackFiller.dll",
                "--expected-production-assembly-version", "1.1.230.6262",
                "--expected-production-file-version", "1.1.230.6262"]);

            Assert.Equal(@"C:\bench\VectorNNTP.BackFiller.dll", options.ExpectedProductionAssemblyPath);
            Assert.Equal("1.1.230.6262", options.ExpectedProductionAssemblyVersion);
            Assert.Equal("1.1.230.6262", options.ExpectedProductionFileVersion);
        }
    }
}
