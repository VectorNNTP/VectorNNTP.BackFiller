// <copyright file="ProgramCommandLineTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for program command line, covering configuration, runtime, and failure-handling contracts exercised by the tests.
// Primary responsibility: documents the executable contracts covered by the program command line test suite.

using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Startup;
using VectorNNTP.Backfiller.Startup.Commands;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests strict command-line parsing and exit-code behavior for operational commands.
    /// </summary>
    public sealed class ProgramCommandLineTests
    {
        /// <summary>
        /// Verifies the program command line tests scenario and its documented contract.
        /// </summary>
        public ProgramCommandLineTests()
        {
            BuildInfoService.InitializeBuildInfo(DateTimeOffset.UtcNow);
        }
        /// <summary>
        /// Verifies the try handle command when no arguments returns null scenario and its documented contract.
        /// </summary>
        [Fact]
        public void TryHandleCommand_WhenNoArguments_ReturnsNull()
        {
            int? exitCode = ParseAndMaybeExecute([]);

            Assert.Null(exitCode);
        }
        /// <summary>
        /// Verifies the try parse command line when args is null throws argument null exception scenario and its documented contract.
        /// </summary>
        [Fact]
        public void TryParseCommandLine_WhenArgsIsNull_ThrowsArgumentNullException()
        {
            _ = Assert.Throws<ArgumentNullException>(() =>
                OperationalCommandParser.TryParseCommandLine(null!, out _, out _));
        }
        /// <summary>
        /// Verifies the try handle command when simple informational command returns success scenario and its documented contract.
        /// </summary>
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
        /// <summary>
        /// Verifies the try handle command when option is not exact command returns configuration failure scenario and its documented contract.
        /// </summary>
        [Theory]
        [InlineData("--version=x")]
        [InlineData("--versions")]
        [InlineData("--validate-config=x")]
        public void TryHandleCommand_WhenOptionIsNotExactCommand_ReturnsConfigurationFailure(string argument)
        {
            int? exitCode = ParseAndMaybeExecute([argument]);

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
        }
        /// <summary>
        /// Verifies the try handle command when argument unknown returns configuration failure scenario and its documented contract.
        /// </summary>
        [Theory]
        [InlineData("--bogus")]
        [InlineData("foo")]
        public void TryHandleCommand_WhenArgumentUnknown_ReturnsConfigurationFailure(string argument)
        {
            int? exitCode = ParseAndMaybeExecute([argument]);

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
        }
        /// <summary>
        /// Verifies the try handle command when unknown option present alongside valid command returns configuration failure scenario and its documented contract.
        /// </summary>
        [Theory]
        [InlineData("--bogus", "--version")]
        [InlineData("--version", "--bogus")]
        public void TryHandleCommand_WhenUnknownOptionPresentAlongsideValidCommand_ReturnsConfigurationFailure(string first, string second)
        {
            int? exitCode = ParseAndMaybeExecute([first, second]);

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
        }
        /// <summary>
        /// Verifies the try handle command when multiple commands specified returns configuration failure scenario and its documented contract.
        /// </summary>
        [Theory]
        [InlineData("--version", "--help")]
        [InlineData("--help", "--diagnostics")]
        [InlineData("--validate-config", "--dump-config")]
        public void TryHandleCommand_WhenMultipleCommandsSpecified_ReturnsConfigurationFailure(string first, string second)
        {
            int? exitCode = ParseAndMaybeExecute([first, second]);

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
        }
        /// <summary>
        /// Verifies the try handle command when configuration command and configuration unavailable returns unexpected failure scenario and its documented contract.
        /// </summary>
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
        /// <summary>
        /// Verifies the try handle command when dump config has configuration returns success scenario and its documented contract.
        /// </summary>
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
        /// <summary>
        /// Verifies the try handle command when dump config includes use staging directory prints cleartext value scenario and its documented contract.
        /// </summary>
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
        /// <summary>
        /// Verifies the try handle command when multiple commands and unknown option are present returns configuration failure scenario and its documented contract.
        /// </summary>
        [Fact]
        public void TryHandleCommand_WhenMultipleCommandsAndUnknownOptionArePresent_ReturnsConfigurationFailure()
        {
            int? exitCode = ParseAndMaybeExecute(["--version", "--bogus", "--help"]);

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
        }
        /// <summary>
        /// Verifies the try handle command when argument is empty or whitespace returns configuration failure scenario and its documented contract.
        /// </summary>
        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("\t")]
        public void TryHandleCommand_WhenArgumentIsEmptyOrWhitespace_ReturnsConfigurationFailure(string argument)
        {
            int? exitCode = ParseAndMaybeExecute(["--version", argument]);

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
        }

        /// <summary>
        /// Verifies the parse and maybe execute scenario and its documented contract.
        /// </summary>
        /// <returns>The parse and maybe execute value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the parse and maybe execute scenario and its documented contract.
        /// </summary>
        /// <param name="args">The args supplied to the helper.</param>
        /// <param name="configuration">The configuration supplied to the helper.</param>
        /// <returns>The parse and maybe execute value produced for the requested scenario.</returns>
        private static int? ParseAndMaybeExecute(string[] args, IConfiguration? configuration = null)
        {
            bool parsed = OperationalCommandParser.TryParseCommandLine(args, out OperationalCommand? command, out int? parseErrorExitCode);

            return !parsed
                ? parseErrorExitCode ?? ExitCodePolicy.ExitCodeConfigurationFailure
                : command.HasValue
                ? OperationalCommandExecutor.ExecuteCommand(command.Value, configuration)
                : null;
        }
    }
}
