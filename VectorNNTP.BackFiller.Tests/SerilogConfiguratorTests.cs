// <copyright file="SerilogConfiguratorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for serilog configurator, covering the covered test contracts.
// Primary responsibility: documents the executable contracts covered by the serilog configurator test suite.

using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Startup.Logging;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Covers serilog configurator behavior and invariants exercised by this test suite.
    /// </summary>
    public sealed class SerilogConfiguratorTests
    {
        /// <summary>
        /// Exercises parse microsoft log level for testing  when configured value provided  returns expected level behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData("Verbose", LogLevel.Trace)]
        [InlineData("verbose", LogLevel.Trace)]
        [InlineData("Fatal", LogLevel.Critical)]
        [InlineData("fatal", LogLevel.Critical)]
        [InlineData("Warning", LogLevel.Warning)]
        [InlineData("Information", LogLevel.Information)]
        [InlineData("invalid", LogLevel.Information)]
        public void ParseMicrosoftLogLevelForTesting_WhenConfiguredValueProvided_ReturnsExpectedLevel(string configured, LogLevel expected)
        {
            LogLevel actual = SerilogConfigurator.ParseMicrosoftLogLevelForTesting(configured);

            Assert.Equal(expected, actual);
        }
    }
}
