using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Startup.Logging;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

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
