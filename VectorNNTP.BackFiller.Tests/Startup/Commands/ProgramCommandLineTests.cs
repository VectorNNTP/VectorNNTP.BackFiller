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
using VectorNNTP.BackFiller.Tests.Startup.Validation;
using VectorNNTP.BackFiller.Tests.TestInfrastructure;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Startup.Commands
{
    /// <summary>
    /// Tests strict command-line parsing and exit-code behavior for operational commands.
    /// </summary>
    public sealed class ProgramCommandLineTests
    {
        /// <summary>
        /// Confirms the program command line tests behavior.
        /// </summary>
        public ProgramCommandLineTests()
        {
            BuildInfoService.InitializeBuildInfo(DateTimeOffset.UtcNow);
        }
        /// <summary>
        /// Confirms the try handle command when no arguments returns null behavior.
        /// </summary>
        [Fact]
        public void TryHandleCommand_WhenNoArguments_ReturnsNull()
        {
            int? exitCode = ParseAndMaybeExecute([]);

            Assert.Null(exitCode);
        }
        /// <summary>
        /// Confirms the try parse command line when args is null throws argument null exception behavior.
        /// </summary>
        [Fact]
        public void TryParseCommandLine_WhenArgsIsNull_ThrowsArgumentNullException()
        {
            _ = Assert.Throws<ArgumentNullException>(() =>
                OperationalCommandParser.TryParseCommandLine(null!, out _, out _));
        }
        /// <summary>
        /// Confirms the try handle command when simple informational command returns success behavior.
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
        /// Confirms the try handle command when option is not exact command returns configuration failure behavior.
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
        /// Confirms the try handle command when argument unknown returns configuration failure behavior.
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
        /// Confirms the try handle command when unknown option present alongside valid command returns configuration failure behavior.
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
        /// Confirms the try handle command when multiple commands specified returns configuration failure behavior.
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
        /// Confirms the try handle command when configuration command and configuration unavailable returns unexpected failure behavior.
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
        /// Confirms the try handle command when dump config has configuration returns success behavior.
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
        /// Confirms validate-startup enforces full startup listener ACME requirements and fails when mandatory listener certificate settings are missing.
        /// </summary>
        [Fact]
        public void TryHandleCommand_WhenValidateStartupMissingListenerAcmeSettings_ReturnsConfigurationFailure()
        {
            IConfiguration configuration = BuildStartupValidationConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = string.Empty,
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = string.Empty,
            });

            using ConsoleOutputScope outputScope = ConsoleOutputScope.Capture();
            int? exitCode = ParseAndMaybeExecute(["--validate-startup"], configuration);
            string output = outputScope.GetCapturedOutput();

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
            Assert.DoesNotContain("Startup validation PASSED", output, StringComparison.Ordinal);
        }

        /// <summary>
        /// Confirms validate-config preserves scoped non-listener behavior and does not fail solely on missing listener ACME certificate settings.
        /// </summary>
        [Fact]
        public void TryHandleCommand_WhenValidateConfigMissingListenerAcmeSettings_ReturnsSuccess()
        {
            IConfiguration configuration = BuildStartupValidationConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = string.Empty,
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = string.Empty,
            });

            using ConsoleOutputScope outputScope = ConsoleOutputScope.Capture();
            int? exitCode = ParseAndMaybeExecute(["--validate-config"], configuration);
            string output = outputScope.GetCapturedOutput();

            Assert.Equal(ExitCodePolicy.ExitCodeNormalShutdown, exitCode);
            Assert.Contains("Configuration validation PASSED", output, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the try handle command when dump config includes use staging directory prints cleartext value behavior.
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

            using ConsoleOutputScope outputScope = ConsoleOutputScope.Capture();

            int? exitCode = ParseAndMaybeExecute(["--dump-config"], configuration);

            Assert.Equal(ExitCodePolicy.ExitCodeNormalShutdown, exitCode);
            Assert.Contains("BackFiller:LetsEncrypt:UseStagingDirectory: true", outputScope.GetCapturedOutput(), StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Confirms the try handle command when multiple commands and unknown option are present returns configuration failure behavior.
        /// </summary>
        [Fact]
        public void TryHandleCommand_WhenMultipleCommandsAndUnknownOptionArePresent_ReturnsConfigurationFailure()
        {
            int? exitCode = ParseAndMaybeExecute(["--version", "--bogus", "--help"]);

            Assert.Equal(ExitCodePolicy.ExitCodeConfigurationFailure, exitCode);
        }
        /// <summary>
        /// Confirms the try handle command when argument is empty or whitespace returns configuration failure behavior.
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
        /// Confirms the parse and maybe execute behavior.
        /// </summary>
        /// <returns>The value returned by the parse and maybe execute helper.</returns>
        /// <summary>
        /// Confirms the parse and maybe execute behavior.
        /// </summary>
        /// <param name="args">The args used by this test scenario.</param>
        /// <param name="configuration">The configuration used by this test scenario.</param>
        /// <returns>The value returned by the parse and maybe execute helper.</returns>
        private static int? ParseAndMaybeExecute(string[] args, IConfiguration? configuration = null)
        {
            bool parsed = OperationalCommandParser.TryParseCommandLine(args, out OperationalCommand? command, out int? parseErrorExitCode);

            return !parsed
                ? parseErrorExitCode ?? ExitCodePolicy.ExitCodeConfigurationFailure
                : command.HasValue
                ? OperationalCommandExecutor.ExecuteCommand(command.Value, configuration)
                : null;
        }

        private static IConfiguration BuildStartupValidationConfiguration(Dictionary<string, string?> overrides)
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirLogs"] = "logs",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "120",
                ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.1",
                ["BackFiller:RabbitMQ:Port"] = "5672",
                ["BackFiller:RabbitMQ:Username"] = "nntparticles",
                ["BackFiller:RabbitMQ:Password"] = "password-1",
                ["BackFiller:RabbitMQ:VirtualHost"] = "/",
                ["BackFiller:RabbitMQ:EnableSsl"] = "false",
                ["BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"] = "300",
                ["BackFiller:RabbitMQ:ScaleDownCooldownSeconds"] = "60",
                ["BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"] = "30",
                ["BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"] = "60",
                ["BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"] = "100",
                ["BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"] = "1000",
                ["BackFiller:RabbitMQ:MaxPendingLeaseWaiters"] = "10",
                ["BackFiller:RabbitMQ:UnhealthyLeasesThreshold"] = "30",
                ["BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"] = "3",
                ["BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"] = "30",
                ["BackFiller:TransitServer:Host"] = "transit01.example.net",
                ["BackFiller:TransitServer:Port"] = "119",
                ["BackFiller:TransitServer:UseSsl"] = "false",
            };

            foreach ((string key, string? value) in overrides)
            {
                values[key] = value;
            }

            return ProgramValidationSemanticsTests.BuildConfigurationForCommandTests(values);
        }
    }
}
