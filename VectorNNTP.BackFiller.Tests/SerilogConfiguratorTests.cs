// <copyright file="SerilogConfiguratorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for serilog configurator.

using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Startup.Logging;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Documents the SerilogConfiguratorTests test type and its protected contract.
    /// </summary>
    public sealed class SerilogConfiguratorTests
    {
        /// <summary>
        /// Verifies the ParseMicrosoftLogLevelForTesting_WhenConfiguredValueProvided_ReturnsExpectedLevel scenario and expected contract.
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
