using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Commands;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests validation-pipeline semantics that are critical for startup safety.
/// </summary>
/// <remarks>
/// Cancellation coverage currently verifies the already-canceled token path.
/// Mid-flight dependency-operation cancellation propagation remains a future integration target.
/// Real external-dependency network paths are classified with the Integration test category.
/// </remarks>
public class ProgramValidationSemanticsTests
{
    [Fact]
    public void ConfigurationValidationResult_WhenOnlyWarnings_IsValidTrue()
    {
        ConfigurationValidationResult result = new(
            errors: [],
            warnings: [("BackFiller:LetsEncrypt:Enabled", "TLS disabled")]);

        Assert.True(result.IsValid);
        Assert.Single(result.Warnings);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void ConfigurationValidationResult_WhenErrorsPresent_IsValidFalse()
    {
        ConfigurationValidationResult result = new(
            errors: [("BackFiller:BindPort", "Out of range")],
            warnings: [("BackFiller:LetsEncrypt:Enabled", "TLS disabled")]);

        Assert.False(result.IsValid);
        Assert.Single(result.Warnings);
        Assert.Single(result.Errors);
    }

    [Fact]
    public void BuildValidateConfigCommandResult_WhenDirLogsMissingFromRuntimeSnapshotValidation_ReturnsConfigurationError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:DirLogs"] = string.Empty,
        });

        ConfigurationValidationResult result = ValidateConfigCommandHandler.BuildValidateConfigCommandResult(configuration);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, static e =>
            e.Setting == "BackFiller"
            && e.Error.Contains("DirLogs", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenCanonicalIdentityAvailable_DoesNotUseConfiguredDomainNames()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:DirLogs"] = "logs",
            ["BackFiller:LetsEncrypt:Enabled"] = "true",
            ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "",
            ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "",
            ["BackFiller:LetsEncrypt:PfxExportPassword"] = "",
            ["BackFiller:LetsEncrypt:RenewalCheckIntervalHours"] = "",
            ["BackFiller:LetsEncrypt:RenewalJitterRatio"] = "",
            ["BackFiller:LetsEncrypt:RenewBeforeExpiryDays"] = "",
            ["BackFiller:LetsEncrypt:DomainNames:0"] = "malicious-or-wrong.example.net",
            // Do not inherit the repository-wide RabbitMQ baseline for this test; supply a minimal explicit RabbitMQ block
            // so the full pipeline binding sees exactly the values we intend to exercise.
        }, includeRabbitMqBaseline: false);

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:LetsEncrypt:DomainNames", StringComparison.Ordinal));
        Assert.Equal("grabber12.example.com", BackFillerIdentityValidator.BuildBackFillerFqdn("Grabber", 12, "example.com"));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenIdentityInvalid_DoesNotFallbackToConfiguredDomainNames()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "true",
            ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "",
            ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "",
            ["BackFiller:LetsEncrypt:PfxExportPassword"] = "",
            ["BackFiller:LetsEncrypt:RenewalCheckIntervalHours"] = "",
            ["BackFiller:LetsEncrypt:RenewalJitterRatio"] = "",
            ["BackFiller:LetsEncrypt:RenewBeforeExpiryDays"] = "",
            ["BackFiller:LetsEncrypt:DomainNames:0"] = "malicious-or-wrong.example.net",
        }, includeRabbitMqBaseline: false);

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting.StartsWith("BackFiller", StringComparison.Ordinal)
            && e.Setting.Contains("Name", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:LetsEncrypt:DomainNames", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenConfigurationFails_SkipsDependencyValidation()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "0",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
        }, includeRabbitMqBaseline: false);

        (ConfigurationValidationResult configResult, DependencyValidationResult dependencyResult) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.False(configResult.IsValid);
        Assert.True(dependencyResult.IsValid);
        Assert.Empty(dependencyResult.FailedDependencies);
        Assert.Empty(dependencyResult.Warnings);
        Assert.Empty(dependencyResult.Errors);
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenLetsEncryptDisabledAndCloudflareTokenMissing_ReturnsCloudflareConfigurationError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
        }, includeRabbitMqBaseline: false);

        (ConfigurationValidationResult configResult, DependencyValidationResult dependencyResult) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.False(configResult.IsValid);
        Assert.Contains(configResult.Errors, static e => e.Setting == "BackFiller:LetsEncrypt:CloudFlareApiToken");
        Assert.True(dependencyResult.IsValid);
        Assert.Empty(dependencyResult.FailedDependencies);
    }

    [Trait("Category", "Integration")]
    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenLetsEncryptDisabledAndCloudflareConfigured_StillRunsCloudflareDependencyValidation()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
        });

        (ConfigurationValidationResult configResult, DependencyValidationResult dependencyResult) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.Contains(dependencyResult.FailedDependencies, static d => d.Dependency == "CloudflareZone");
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenTlsDisabledAndCloudflareConfigured_PreservesWarningsWithoutInvalidatingConfiguration()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
        });

        (ConfigurationValidationResult configResult, _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.Empty(configResult.Errors);
        Assert.Contains(configResult.Warnings, static w => w.Setting == "BackFiller:LetsEncrypt:Enabled");
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenRabbitMqEndpointUnreachable_DoesNotReturnRabbitMqDependencyFailure()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.1",
            ["BackFiller:RabbitMQ:Port"] = "5672",
            ["BackFiller:TransitServer:Host"] = "localhost",
            ["BackFiller:TransitServer:Port"] = "119",
            // Intentionally omit UseSsl to validate default behavior (should be treated as false)
            ["BackFiller:TransitServer:UseSsl"] = "false",
        });

        List<(string Setting, string Error)> configErrors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);
        Assert.Empty(configErrors);

        (_, DependencyValidationResult dependencyResult) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromMilliseconds(500),
                CancellationToken.None);

        Assert.DoesNotContain(dependencyResult.FailedDependencies, static d => d.Dependency == "RabbitMQ");
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenTransitServerEndpointUnreachable_ReturnsTransitServerDependencyFailure()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.2",
            ["BackFiller:RabbitMQ:Port"] = "5672",
            ["BackFiller:TransitServer:Host"] = "203.0.113.1",
            ["BackFiller:TransitServer:Port"] = "119",
            ["BackFiller:TransitServer:UseSsl"] = "false",
        });

        List<(string Setting, string Error)> configErrors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);
        Assert.Empty(configErrors);

        (_, DependencyValidationResult dependencyResult) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromMilliseconds(500),
                CancellationToken.None);

        Assert.Contains(dependencyResult.FailedDependencies, static d => d.Dependency == "TransitServer");
    }

    [Theory]
    [InlineData("example.com")]
    [InlineData("EXAMPLE.COM")]
    [InlineData("example.com.")]
    [InlineData(" EXAMPLE.COM. ")]
    public void BackFillerIdentityValidator_CanonicalizeDnsSuffix_NormalizesEquivalentInputs(string input)
    {
        string canonical = BackFillerIdentityValidator.CanonicalizeDnsSuffix(input);
        Assert.Equal("example.com", canonical);
    }

    [Theory]
    [InlineData("example.com")]
    [InlineData("EXAMPLE.COM")]
    [InlineData("example.com.")]
    [InlineData(" EXAMPLE.COM. ")]
    public void BackFillerIdentityValidator_BuildBackFillerFqdn_UsesCanonicalDnsSuffix(string dnsSuffix)
    {
        string fqdn = BackFillerIdentityValidator.BuildBackFillerFqdn("Grabber", 12, dnsSuffix);
        Assert.Equal("grabber12.example.com", fqdn);
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenAlreadyCanceled_PropagatesOperationCanceledException()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;Connection Timeout=1",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
        });

        using CancellationTokenSource cts = new();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(5),
                cts.Token).ConfigureAwait(false));
    }

    public static TheoryData<TimeSpan> InvalidDependencyTimeouts =>
        [
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(-1),
        ];

    public static TheoryData<int, string> MySqlSanitizedErrorMappings => new()
    {
        { 1045, "MySQL connection failed: Access denied" },
        { 1049, "MySQL connection failed: Unknown database" },
        { 1130, "MySQL connection failed: Host is not allowed to connect" },
        { 2002, "MySQL connection failed: Unable to reach MySQL server" },
        { 2003, "MySQL connection failed: Unable to reach MySQL server" },
        { 2013, "MySQL connection failed: Lost connection during query" },
        { 2026, "MySQL connection failed: TLS/SSL handshake failed" },
        { 2061, "MySQL connection failed: Authentication plugin error" },
        { 9999, "MySQL connection failed" },
    };

    [Theory]
    [MemberData(nameof(InvalidDependencyTimeouts))]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenTimeoutIsInvalid_ThrowsArgumentOutOfRangeException(TimeSpan invalidTimeout)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
        });

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                invalidTimeout,
                CancellationToken.None).ConfigureAwait(false));
    }

    [Theory]
    [MemberData(nameof(MySqlSanitizedErrorMappings))]
    public void GetSanitizedMySqlConnectionFailureReason_WhenErrorCodeKnown_ReturnsSanitizedMessage(int mySqlErrorNumber, string expectedMessage)
    {
        string sanitizedMessage = global::VectorNNTP.Backfiller.Startup.Validation.DatabaseDependencyProbe.GetSanitizedMySqlConnectionFailureReason(mySqlErrorNumber);

        Assert.Equal(expectedMessage, sanitizedMessage);
    }

    [Fact]
    public async Task ValidateDatabaseConnectivityAsync_WhenUnexpectedExceptionOccurs_ReturnsSanitizedFailureReason()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server==invalid",
            })
            .Build();

        DependencyValidationResult result = await global::VectorNNTP.Backfiller.Startup.Validation.DatabaseDependencyProbe
            .ValidateDatabaseConnectivityAsync(configuration, TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.Contains(result.FailedDependencies, static d =>
            d.Dependency == "GrabberDB" &&
            d.Reason == "Failed to connect");
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqChannelLeaseTimeoutMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqRpcTimeoutSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:RpcTimeoutSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("3601")]
    public void ValidateBackFillerOptions_WhenRabbitMqRpcTimeoutSecondsOutOfRange_ReturnsError(string rpcTimeoutSeconds)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = rpcTimeoutSeconds,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:RpcTimeoutSeconds"
            && (e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase)
                || e.Error.Contains("between 1 and 3600", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqChannelLeaseTimeoutLessThanRpcTimeout_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "20",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"
            && e.Error.Contains("greater than or equal to RpcTimeoutSeconds", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqChannelLeaseTimeoutValidAndCoherent_DoesNotReturnRabbitMqErrors()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:RabbitMQ", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqConnectionBlockedTimeoutMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqConnectionBlockedTimeoutLessThanMinimum_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "4",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"
            && e.Error.Contains("between 5 and 3600", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqConnectionBlockedTimeoutLessThanRpcTimeout_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "20",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"
            && e.Error.Contains("greater than or equal to RpcTimeoutSeconds", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqEnableSslMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:EnableSsl"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPortMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Port"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("65536")]
    public void ValidateBackFillerOptions_WhenRabbitMqPortOutOfRange_ReturnsError(string port)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Port"] = port,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Port"
            && (e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase)
                || e.Error.Contains("between 1 and 65535", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqUsernameConfiguredAndPasswordMissing_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            // Ensure baseline is applied but then explicitly clear Password to test Username-only case
            ["BackFiller:RabbitMQ:Username"] = "nntparticles",
            ["BackFiller:RabbitMQ:Password"] = "",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Password"
            && e.Error.Contains("required when Username is configured", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPasswordConfiguredAndUsernameMissing_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            // Ensure baseline is applied but then explicitly clear Username to test Password-only case
            ["BackFiller:RabbitMQ:Password"] = "password-1",
            ["BackFiller:RabbitMQ:Username"] = "",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Username"
            && e.Error.Contains("required when Password is configured", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqUsernameWhitespace_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Username"] = "   ",
            ["BackFiller:RabbitMQ:Password"] = "password-1",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Username"
            && e.Error.Contains("must not be empty or whitespace", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqVirtualHostMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:VirtualHost"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqVirtualHostWhitespace_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:VirtualHost"] = "   ",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:VirtualHost"
            && e.Error.Contains("must not be empty or whitespace", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPasswordEmpty_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Username"] = "nntparticles",
            ["BackFiller:RabbitMQ:Password"] = string.Empty,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Password"
            && e.Error.Contains("must not be empty", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPasswordWhitespace_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Username"] = "nntparticles",
            ["BackFiller:RabbitMQ:Password"] = "   ",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Password"
            && e.Error.Contains("must not be empty or whitespace", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqUsernameAndPasswordAreValid_DoesNotReturnCredentialErrors()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Username"] = "nntparticles",
            ["BackFiller:RabbitMQ:Password"] = "password-1",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:Password"
            || e.Setting == "BackFiller:RabbitMQ:Username");
    }

    [Theory]
    [InlineData("true")]
    [InlineData("false")]
    public void ValidateBackFillerOptions_WhenRabbitMqEnableSslBooleanValue_DoesNotReturnError(string enableSsl)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:EnableSsl"] = enableSsl,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:RabbitMQ:EnableSsl");
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqHostsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:RabbitMQ:Hosts", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqHostEntryContainsScheme_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Hosts:0"] = "amqps://rabbit01.example.net",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting.StartsWith("BackFiller:RabbitMQ:Hosts:", StringComparison.Ordinal)
            && e.Error.Contains("must not include a URI scheme", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqHostsContainDuplicates_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Hosts:0"] = "rabbit01.example.net",
            ["BackFiller:RabbitMQ:Hosts:1"] = "RABBIT01.EXAMPLE.NET",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting.StartsWith("BackFiller:RabbitMQ:Hosts:", StringComparison.Ordinal)
            && e.Error.Contains("Duplicate host entries", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqHostsAreValid_DoesNotReturnHostErrors()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:Hosts:0"] = "rabbit01.example.net",
            ["BackFiller:RabbitMQ:Hosts:1"] = "10.20.30.11",
            ["BackFiller:RabbitMQ:Hosts:2"] = "2001:db8::10",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:RabbitMQ:Hosts", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqConnectionScaleDownIdleSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"] = "10",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"
            && e.Error.Contains("between 30 and 86400", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqScaleDownCooldownSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ScaleDownCooldownSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("3601")]
    public void ValidateBackFillerOptions_WhenRabbitMqScaleDownCooldownSecondsOutOfRange_ReturnsError(string scaleDownCooldownSeconds)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ScaleDownCooldownSeconds"] = scaleDownCooldownSeconds,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ScaleDownCooldownSeconds"
            && (e.Error.Contains("greater than or equal to zero", StringComparison.OrdinalIgnoreCase)
                || e.Error.Contains("between 0 and 3600", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMinConnectionsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"] = "300",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MinConnections"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMinConnectionsLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MinConnections"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MinConnections"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaxConnectionsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MinConnections"] = "4",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaxConnections"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMinConnectionsGreaterThanMaxConnections_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"] = "300",
            ["BackFiller:RabbitMQ:MinConnections"] = "5",
            ["BackFiller:RabbitMQ:MaxConnections"] = "4",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MinConnections"
            && e.Error.Contains("less than or equal to MaxConnections", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaxConsecutiveRecoveryFailuresMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaxConsecutiveRecoveryFailuresLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaxConsecutiveRecoveryFailuresTooLarge_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"] = "101",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"
            && e.Error.Contains("between 1 and 100", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPublishConfirmTimeoutSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPublishConfirmTimeoutSecondsLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPublishConfirmTimeoutSecondsTooLarge_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"] = "3601",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"
            && e.Error.Contains("between 1 and 3600", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaximumShutdownDrainTimeoutSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaximumShutdownDrainTimeoutSecondsLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaximumShutdownDrainTimeoutSecondsTooLarge_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"] = "3601",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"
            && e.Error.Contains("between 1 and 3600", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ConfigureHostShutdownTimeout_SetsConfiguredTimeout()
    {
        ServiceCollection services = [];
        ShutdownOptions shutdownOptions = new()
        {
            GracePeriodSeconds = 60,
        };

        global::VectorNNTP.Backfiller.Startup.Hosting.HostComposer.ConfigureHostShutdownTimeout(services, shutdownOptions);

        using ServiceProvider serviceProvider = services.BuildServiceProvider();
        IOptions<HostOptions> hostOptions = serviceProvider.GetRequiredService<IOptions<HostOptions>>();

        Assert.Equal(TimeSpan.FromSeconds(60), hostOptions.Value.ShutdownTimeout);
    }

    [Fact]
    public void ConfigureHostShutdownTimeout_WhenGracePeriodInvalid_Throws()
    {
        ServiceCollection services = [];
        ShutdownOptions shutdownOptions = new()
        {
            GracePeriodSeconds = 0,
        };

        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            global::VectorNNTP.Backfiller.Startup.Hosting.HostComposer.ConfigureHostShutdownTimeout(services, shutdownOptions));

        Assert.Equal("shutdownOptions", exception.ParamName);
    }

    [Theory]
    [InlineData(20, 60)]
    [InlineData(30, 31)]
    public void ShutdownConfiguration_RejectsRabbitMqDrainLongerThanGracePeriod(
        int gracePeriodSeconds,
        int rabbitMqMaximumShutdownDrainTimeoutSeconds)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"] = rabbitMqMaximumShutdownDrainTimeoutSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["BackFiller:Shutdown:GracePeriodSeconds"] = gracePeriodSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"
            && e.Error.Contains("less than or equal to BackFiller:Shutdown:GracePeriodSeconds", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenShutdownSectionIsNull_ReturnsValidationErrorWithoutThrowing()
    {
        BackFillerOptions options = new()
        {
            BindPort = 119,
            Name = "Grabber",
            Id = 12,
            DnsSuffix = "example.com",
            DirCerts = "certs",
            LetsEncrypt = new LetsEncryptOptions
            {
                Enabled = false,
                CloudFlareApiToken = "v1.abcdef1234567890abcdef1234567890abcdef12",
                CloudFlareZoneId = "5811a29d39a0732afb5f160c9b137c3d",
            },
            RabbitMQ = new RabbitMqOptions(),
            TransitServer = new TransitServerOptions(),
        };

        System.Reflection.PropertyInfo? shutdownProperty = typeof(BackFillerOptions).GetProperty(nameof(BackFillerOptions.Shutdown));
        Assert.NotNull(shutdownProperty);
        shutdownProperty.SetValue(options, null);

        List<(string Setting, string Message)> warnings = [];
        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller.Shutdown"
            && e.Error.Contains("BackFiller:Shutdown is required", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMinimumConnectionLifetimeSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMinimumConnectionLifetimeSecondsTooSmall_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"] = "10",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"
            && e.Error.Contains("between 30 and 86400", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqNetworkRecoveryIntervalSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqNetworkRecoveryIntervalSecondsLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqNetworkRecoveryIntervalSecondsTooLarge_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"] = "3601",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"
            && e.Error.Contains("between 1 and 3600", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPoolReconnectBaseDelayMsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPoolReconnectMaxDelayMsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPoolReconnectMaxDelayMsLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPoolReconnectMaxDelayMsOutOfRange_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"] = "300001",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"
            && e.Error.Contains("between 50 and 300000", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPoolReconnectMaxDelayMsLessThanBaseDelay_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"] = "1000",
            ["BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"] = "999",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"
            && e.Error.Contains("greater than or equal to PoolReconnectBaseDelayMs", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPoolReconnectBaseDelayMsLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqPoolReconnectBaseDelayMsOutOfRange_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"] = "49",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"
            && e.Error.Contains("between 50 and 60000", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaxPendingLeaseWaitersMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaxPendingLeaseWaiters"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaxPendingLeaseWaitersLessThanZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaxPendingLeaseWaiters"] = "-1",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaxPendingLeaseWaiters"
            && e.Error.Contains("greater than or equal to zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqMaxPendingLeaseWaitersTooLarge_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaxPendingLeaseWaiters"] = "65537",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:MaxPendingLeaseWaiters"
            && e.Error.Contains("between 0 and 65536", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqDegradedThresholdMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:DegradedThreshold"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-0.01")]
    [InlineData("1.01")]
    public void ValidateBackFillerOptions_WhenRabbitMqDegradedThresholdOutOfRange_ReturnsError(string degradedThreshold)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:DegradedThreshold"] = degradedThreshold,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:DegradedThreshold"
            && e.Error.Contains("greater than 0 and less than or equal to 1", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqDegradedThresholdValid_DoesNotReturnError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:DegradedThreshold"] = "0.75",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:RabbitMQ:DegradedThreshold");
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqUnhealthyThresholdMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:UnhealthyThreshold"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("121")]
    public void ValidateBackFillerOptions_WhenRabbitMqUnhealthyThresholdOutOfRange_ReturnsError(string unhealthyThreshold)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:UnhealthyThreshold"] = unhealthyThreshold,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:UnhealthyThreshold"
            && (e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase)
                || e.Error.Contains("between 1 and 120", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqChannelPoolSizeMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ChannelPoolSize"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqChannelPoolSizeLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ChannelPoolSize"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ChannelPoolSize"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqRequestedHeartbeatSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:RequestedHeartbeatSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("3601")]
    public void ValidateBackFillerOptions_WhenRabbitMqRequestedHeartbeatSecondsOutOfRange_ReturnsError(string requestedHeartbeatSeconds)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:RequestedHeartbeatSeconds"] = requestedHeartbeatSeconds,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:RequestedHeartbeatSeconds"
            && (e.Error.Contains("greater than or equal to zero", StringComparison.OrdinalIgnoreCase)
                || e.Error.Contains("between 0 and 3600", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqSocketTimeoutSecondsMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:SocketTimeoutSeconds"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("601")]
    public void ValidateBackFillerOptions_WhenRabbitMqSocketTimeoutSecondsOutOfRange_ReturnsError(string socketTimeoutSeconds)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:SocketTimeoutSeconds"] = socketTimeoutSeconds,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:SocketTimeoutSeconds"
            && (e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase)
                || e.Error.Contains("between 5 and 600", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqRequestedChannelMaxMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:RequestedChannelMax"
            && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqRequestedChannelMaxLessThanOrEqualToZero_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:RequestedChannelMax"] = "0",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:RequestedChannelMax"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqRequestedChannelMaxTooLarge_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:RequestedChannelMax"] = "65536",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:RequestedChannelMax"
            && e.Error.Contains("between 1 and 65535", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqChannelPoolSizeExceedsEffectiveChannelLimit_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ChannelPoolSize"] = "513",
            ["BackFiller:RabbitMQ:MaxConnections"] = "1",
            ["BackFiller:RabbitMQ:RequestedChannelMax"] = "512",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ChannelPoolSize"
            && e.Error.Contains("effective channel limit", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenRabbitMqChannelPoolSizeWithinEffectiveChannelLimit_DoesNotReturnChannelPoolErrors()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ChannelPoolSize"] = "512",
            ["BackFiller:RabbitMQ:MaxConnections"] = "1",
            ["BackFiller:RabbitMQ:RequestedChannelMax"] = "512",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e =>
            e.Setting == "BackFiller:RabbitMQ:ChannelPoolSize"
            && e.Error.Contains("effective channel limit", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerHostMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:TransitServer:Host");
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerHostWhitespace_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "   ",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:TransitServer:Host"
            && e.Error.Contains("must not be empty", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerHostContainsScheme_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "nntp://transit01.example.net",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:TransitServer:Host"
            && e.Error.Contains("must not include a URI scheme", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerHostContainsCredentials_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "user:password@transit.example.net",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:TransitServer:Host"
            && e.Error.Contains("must not include credentials", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerHostContainsPort_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net:119",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:TransitServer:Host"
            && e.Error.Contains("must not include a port value", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerHostInvalid_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "bad host",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:TransitServer:Host"
            && e.Error.Contains("valid hostname or IP address", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerHostValid_DoesNotReturnError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:TransitServer:Host");
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerPortMissing_UsesDefaultWithoutError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:TransitServer:Port");
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    public void ValidateBackFillerOptions_WhenTransitServerPortLessThanOrEqualToZero_ReturnsError(string port)
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = port,
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:TransitServer:Port"
            && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateBackFillerOptions_WhenTransitServerPortTooLarge_ReturnsError()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "65536",
        });

        List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

        Assert.Contains(errors, static e =>
            e.Setting == "BackFiller:TransitServer:Port"
            && e.Error.Contains("between 1 and 65535", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenTransitServerUseSslMissing_UsesDefaultFalseWithoutUseSslErrors()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "119",
        });

        (ConfigurationValidationResult configResult, DependencyValidationResult _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        // Sanity-check configuration-level validation first and report any config errors for diagnosis.
        List<(string Setting, string Error)> configErrors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);
        Assert.True(configErrors.Count == 0, $"Unexpected configuration errors: {string.Join("; ", configErrors.Select(e => e.Setting + ": " + e.Error))}");

        Assert.True(configResult.IsValid);
        Assert.DoesNotContain(configResult.Errors, static e => e.Setting == "BackFiller:TransitServer:UseSsl");
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenTransitServerUseSslTrueWithPort119_ReturnsWarning()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "119",
            ["BackFiller:TransitServer:UseSsl"] = "true",
        });

        (ConfigurationValidationResult configResult, DependencyValidationResult _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.Contains(configResult.Warnings, static w =>
            w.Setting == "BackFiller:TransitServer:Port"
            && w.Message.Contains("conventionally non-TLS", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenTransitServerUseSslFalseWithPort563_ReturnsWarning()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "563",
            ["BackFiller:TransitServer:UseSsl"] = "false",
        });

        (ConfigurationValidationResult configResult, DependencyValidationResult _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.Contains(configResult.Warnings, static w =>
            w.Setting == "BackFiller:TransitServer:Port"
            && w.Message.Contains("conventionally TLS", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenTransitServerUseSslTrueWithPort563_DoesNotReturnPortWarnings()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "563",
            ["BackFiller:TransitServer:UseSsl"] = "true",
        });

        (ConfigurationValidationResult configResult, DependencyValidationResult _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.DoesNotContain(configResult.Warnings, static w => w.Setting == "BackFiller:TransitServer:Port");
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenRabbitMqNetworkRecoveryIntervalExceedsConnectionBlockedTimeout_ReturnsWarning()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            // Explicit RabbitMQ configuration
            ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.7",
            ["BackFiller:RabbitMQ:Port"] = "5672",
            ["BackFiller:RabbitMQ:VirtualHost"] = "/",
            ["BackFiller:RabbitMQ:EnableSsl"] = "false",
            ["BackFiller:RabbitMQ:MinConnections"] = "1",
            ["BackFiller:RabbitMQ:MaxConnections"] = "10",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "5",
            ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "10",
            ["BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"] = "20",
            ["BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"] = "100",
            ["BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"] = "1000",
            ["BackFiller:RabbitMQ:MaxPendingLeaseWaiters"] = "10",
            ["BackFiller:RabbitMQ:UnhealthyLeasesThreshold"] = "30",
            ["BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"] = "3",
            ["BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"] = "30",
        }, includeRabbitMqBaseline: false);

        (ConfigurationValidationResult configResult, _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.Contains(configResult.Warnings, static w =>
            w.Setting == "BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"
            && w.Message.Contains("exceeds ConnectionBlockedTimeoutSeconds", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenRabbitMqPublishConfirmTimeoutExceedsRpcTimeout_ReturnsWarning()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            // Make both values explicit so baseline does not mask the comparison
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "10",
            ["BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"] = "20",
        });

        (ConfigurationValidationResult configResult, _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.Contains(configResult.Warnings, static w =>
            w.Setting == "BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"
            && w.Message.Contains("exceeds RpcTimeoutSeconds", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ValidateConfigurationAndDependenciesAsync_WhenRabbitMqMinimumConnectionLifetimeExceedsScaleDownIdle_ReturnsWarning()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            // Explicitly set both sides of the inequality to ensure the warning condition is exercised
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"] = "60",
            ["BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"] = "120",
        });

        (ConfigurationValidationResult configResult, _) =
            await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                configuration,
                TimeSpan.FromSeconds(1),
                CancellationToken.None);

        Assert.True(configResult.IsValid);
        Assert.Contains(configResult.Warnings, static w =>
            w.Setting == "BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"
            && w.Message.Contains("exceeds ConnectionScaleDownIdleSeconds", StringComparison.OrdinalIgnoreCase));
    }

    private static IConfiguration BuildConfiguration(Dictionary<string, string?> values, bool includeRabbitMqBaseline = true)
    {
        if (includeRabbitMqBaseline)
        {
            var baseline = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:Enabled"] = "false",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                // RabbitMQ baseline prerequisites to allow deeper validator checks
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
            };

            // merge overrides
            foreach (var kv in values)
            {
                baseline[kv.Key] = kv.Value;
            }

            values = baseline;
        }

        if (!values.ContainsKey("BackFiller:DirLogs"))
        {
            values["BackFiller:DirLogs"] = "logs";
        }

        return new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();
    }

}



