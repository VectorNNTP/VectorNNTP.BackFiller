using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Validates the isolated channel wake-up experiment command-line configuration.
/// </summary>
public sealed class SyntheticChannelWakeupOptionsTests
{
    /// <summary>
    /// Ensures the default configuration retains enough independent warm-up and measured waves.
    /// </summary>
    [Fact]
    public void Parse_WhenOptionsAreOmitted_UsesForensicDefaults()
    {
        SyntheticChannelWakeupOptions options = SyntheticChannelWakeupOptions.Parse([]);

        Assert.Equal(10, options.WarmupWaves);
        Assert.Equal(100, options.MeasuredWaves);
        Assert.Equal(3, options.Trials);
    }

    /// <summary>
    /// Ensures every supported experiment dimension is independently configurable.
    /// </summary>
    [Fact]
    public void Parse_WhenValuesAreSupplied_UsesConfiguredPositiveValues()
    {
        SyntheticChannelWakeupOptions options = SyntheticChannelWakeupOptions.Parse(
            ["--warmup-waves", "2", "--measured-waves", "7", "--trials", "4"]);

        Assert.Equal(2, options.WarmupWaves);
        Assert.Equal(7, options.MeasuredWaves);
        Assert.Equal(4, options.Trials);
    }

    /// <summary>
    /// Ensures invalid experiment dimensions are rejected before a benchmark begins.
    /// </summary>
    /// <param name="args">The invalid command-line arguments.</param>
    [Theory]
    [InlineData("--trials", "0")]
    [InlineData("--unknown", "1")]
    [InlineData("--warmup-waves")]
    public void Parse_WhenArgumentsAreInvalid_ThrowsArgumentException(params string[] args)
    {
        Assert.Throws<ArgumentException>(() => SyntheticChannelWakeupOptions.Parse(args));
    }
}
