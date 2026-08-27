using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Startup;
using VectorNNTP.Backfiller.Startup.Commands;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests strict command-line parsing and exit-code behavior for operational commands.
/// </summary>
public sealed class ProgramCommandLineTests
{
    public ProgramCommandLineTests()
    {
        BuildInfoService.InitializeBuildInfo(DateTimeOffset.UtcNow);
    }

    [Fact]
    public void TryHandleCommand_WhenNoArguments_ReturnsNull()
    {
        int? exitCode = ParseAndMaybeExecute([]);

        Assert.Null(exitCode);
    }

    [Fact]
    public void TryParseCommandLine_WhenArgsIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            OperationalCommandParser.TryParseCommandLine(null!, out _, out _));
    }

    [Theory]
    [InlineData("--help")]
    [InlineData("--version")]
    [InlineData("--diagnostics")]
    [InlineData("--VERSION")]
    public void TryHandleCommand_WhenSimpleInformationalCommand_ReturnsSuccess(string command)
    {
        int? exitCode = ParseAndMaybeExecute([command]);

        Assert.Equal(ExitCodePolicy.ExitCodeNormalShutdown, exitCode);
    }

    [Theory]
    [InlineData("--version=x")]
    [InlineData("--versions")]
    [InlineData("--validate-config=x")]
    public void TryHandleCommand_WhenOptionIsNotExactCommand_ReturnsConfigurationFailure(string argument)
    {
        int? exitCode = ParseAndMaybeExecute([argument]);

        Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
    }

    [Theory]
    [InlineData("--bogus")]
    [InlineData("foo")]
    public void TryHandleCommand_WhenArgumentUnknown_ReturnsConfigurationFailure(string argument)
    {
        int? exitCode = ParseAndMaybeExecute([argument]);

        Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
    }

    [Theory]
    [InlineData("--bogus", "--version")]
    [InlineData("--version", "--bogus")]
    public void TryHandleCommand_WhenUnknownOptionPresentAlongsideValidCommand_ReturnsConfigurationFailure(string first, string second)
    {
        int? exitCode = ParseAndMaybeExecute([first, second]);

        Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
    }

    [Theory]
    [InlineData("--version", "--help")]
    [InlineData("--help", "--diagnostics")]
    [InlineData("--validate-config", "--dump-config")]
    public void TryHandleCommand_WhenMultipleCommandsSpecified_ReturnsConfigurationFailure(string first, string second)
    {
        int? exitCode = ParseAndMaybeExecute([first, second]);

        Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
    }

    [Theory]
    [InlineData("--dump-config")]
    [InlineData("--validate-config")]
    [InlineData("--validate-startup")]
    [InlineData("--Validate-Config")]
    public void TryHandleCommand_WhenConfigurationCommandAndConfigurationUnavailable_ReturnsUnexpectedFailure(string command)
    {
        int? exitCode = ParseAndMaybeExecute([command], configuration: null);

        Assert.Equal(ExitCodePolicy.ExitCodeUnexpectedFailure, exitCode);
    }

    [Fact]
    public void TryHandleCommand_WhenDumpConfigHasConfiguration_ReturnsSuccess()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:Name"] = "backfiller",
                ["BackFiller:Id"] = "1",
                ["BackFiller:DnsSuffix"] = "usenet.ninja",
                ["BackFiller:BindPort"] = "119",
            })
            .Build();

        int? exitCode = ParseAndMaybeExecute(["--dump-config"], configuration);

        Assert.Equal(ExitCodePolicy.ExitCodeNormalShutdown, exitCode);
    }

    [Fact]
    public void TryHandleCommand_WhenDumpConfigIncludesUseStagingDirectory_PrintsCleartextValue()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:LetsEncrypt:UseStagingDirectory"] = "true",
            })
            .Build();

        StringWriter captured = new();
        TextWriter synchronizedOut = TextWriter.Synchronized(captured);
        TextWriter originalOut = Console.Out;

        try
        {
            Console.SetOut(synchronizedOut);

            int? exitCode = ParseAndMaybeExecute(["--dump-config"], configuration);

            Assert.Equal(ExitCodePolicy.ExitCodeNormalShutdown, exitCode);
            Assert.Contains("BackFiller:LetsEncrypt:UseStagingDirectory: true", captured.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }

    [Fact]
    public void TryHandleCommand_WhenMultipleCommandsAndUnknownOptionArePresent_ReturnsConfigurationFailure()
    {
        int? exitCode = ParseAndMaybeExecute(["--version", "--bogus", "--help"]);

        Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void TryHandleCommand_WhenArgumentIsEmptyOrWhitespace_ReturnsConfigurationFailure(string argument)
    {
        int? exitCode = ParseAndMaybeExecute(["--version", argument]);

        Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
    }

    private static int? ParseAndMaybeExecute(string[] args, IConfiguration? configuration = null)
    {
        bool parsed = OperationalCommandParser.TryParseCommandLine(args, out OperationalCommand? command, out int? parseErrorExitCode);

        if (!parsed)
        {
            return parseErrorExitCode ?? ExitCodePolicy.ExitCodeConfigurationFailure;
        }

        return command.HasValue
            ? OperationalCommandExecutor.ExecuteCommand(command.Value, configuration)
            : null;
    }
}
