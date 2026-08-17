using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;
using Xunit.Abstractions;

namespace VectorNNTP.Backfiller.Tests;

public class TransitServerValidatorIsolationTests
{
    private readonly ITestOutputHelper _out;

    public TransitServerValidatorIsolationTests(ITestOutputHelper output) => _out = output;

    private static IConfiguration Build(Dictionary<string, string?> values)
    {
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    [Fact]
    public async Task TransitServer_UseSslMissing_DefaultFalse_DirectAndFullPipeline()
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            ["BackFiller:BindPort"] = "119",
            ["BackFiller:Name"] = "Grabber",
            ["BackFiller:Id"] = "12",
            ["BackFiller:DnsSuffix"] = "example.com",
            ["BackFiller:DirCerts"] = "certs",
            ["BackFiller:DirLogs"] = "logs",
            ["BackFiller:LetsEncrypt:Enabled"] = "false",
            ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "v1.abcdef1234567890abcdef1234567890abcdef12",
            ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            // Minimal RabbitMQ baseline so configuration validation does not fail unrelatedly
            ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.1",
            ["BackFiller:RabbitMQ:Port"] = "5672",
            ["BackFiller:RabbitMQ:VirtualHost"] = "/",
            ["BackFiller:RabbitMQ:EnableSsl"] = "false",
            ["BackFiller:RabbitMQ:Username"] = "nntparticles",
            ["BackFiller:RabbitMQ:Password"] = "password-1",
            ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "120",
            ["BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"] = "100",
            ["BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"] = "1000",
            ["BackFiller:RabbitMQ:MaxPendingLeaseWaiters"] = "10",
            ["BackFiller:RabbitMQ:UnhealthyLeasesThreshold"] = "30",
            ["BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"] = "3",
            ["BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"] = "30",
            ["BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"] = "300",
            ["BackFiller:RabbitMQ:ScaleDownCooldownSeconds"] = "60",
            ["BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"] = "30",
            ["BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"] = "60",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "119",
        };

        IConfiguration config = Build(values);

        // Convert the diagnostic harness into real assertions: verify that the full pipeline
        // produces the expected outcome (no TransitServer UseSsl error when UseSsl is missing)
        var (configResult, dependencyResult, runtimeOptions) = await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

        string errorSummary = string.Join("; ", configResult.Errors.Select(e => $"{e.Setting}: {e.Error}"));
        string warnSummary = string.Join("; ", configResult.Warnings.Select(w => $"{w.Setting}: {w.Message}"));

        Assert.True(configResult.IsValid, $"Configuration invalid. Errors=[{errorSummary}] Warnings=[{warnSummary}]");
        // No error should be produced for the missing UseSsl setting; it should default to false and not be an error
        Assert.DoesNotContain(configResult.Errors, static e => e.Setting == "BackFiller:TransitServer:UseSsl");
    }

    [Fact]
    public async Task TransitServer_UseSslTrue_Port119()
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
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
            // Minimal RabbitMQ fixture
            ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.11",
            ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "120",
            ["BackFiller:RabbitMQ:Port"] = "5672",
            ["BackFiller:RabbitMQ:EnableSsl"] = "false",
            ["BackFiller:DirLogs"] = "logs",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "119",
            ["BackFiller:TransitServer:UseSsl"] = "true",
        };

        IConfiguration config = Build(values);

        var (configResult, dependencyResult, runtimeOptions) = await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

        string errorSummary = string.Join("; ", configResult.Errors.Select(e => $"{e.Setting}: {e.Error}"));
        string warnSummary = string.Join("; ", configResult.Warnings.Select(w => $"{w.Setting}: {w.Message}"));

        Assert.True(configResult.IsValid, $"Configuration invalid. Errors=[{errorSummary}] Warnings=[{warnSummary}]");
        // When UseSsl=true and Port=119 the validator should emit a port warning about non-TLS port
        Assert.Contains(configResult.Warnings, static w =>
            w.Setting == "BackFiller:TransitServer:Port" && w.Message.Contains("conventionally non-TLS", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task TransitServer_UseSslFalse_Port563()
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
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
            // Minimal RabbitMQ fixture
            ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.12",
            ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "120",
            ["BackFiller:RabbitMQ:Port"] = "5672",
            ["BackFiller:RabbitMQ:EnableSsl"] = "false",
            ["BackFiller:DirLogs"] = "logs",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "563",
            ["BackFiller:TransitServer:UseSsl"] = "false",
        };

        IConfiguration config = Build(values);

        var (configResult, dependencyResult, runtimeOptions) = await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.True(configResult.IsValid);
        // When UseSsl=false and Port=563 the validator should emit a port warning about TLS convention
        Assert.Contains(configResult.Warnings, static w =>
            w.Setting == "BackFiller:TransitServer:Port" && w.Message.Contains("conventionally TLS", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task TransitServer_UseSslTrue_Port563()
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
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
            // Minimal RabbitMQ fixture
            ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.13",
            ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "120",
            ["BackFiller:RabbitMQ:Port"] = "5672",
            ["BackFiller:RabbitMQ:EnableSsl"] = "false",
            ["BackFiller:DirLogs"] = "logs",
            ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            ["BackFiller:TransitServer:Port"] = "563",
            ["BackFiller:TransitServer:UseSsl"] = "true",
        };

        IConfiguration config = Build(values);

        var (configResult, dependencyResult, runtimeOptions) = await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

        Assert.True(configResult.IsValid);
        // When UseSsl=true and Port=563 there should be no port warning
        Assert.DoesNotContain(configResult.Warnings, static w => w.Setting == "BackFiller:TransitServer:Port");
    }
}

