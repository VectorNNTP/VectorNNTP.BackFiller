// <copyright file="SerilogConfiguratorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Startup.Logging;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    public sealed class SerilogConfiguratorTests
    {
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
