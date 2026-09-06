// <copyright file="ArticleRetentionConfigurationContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Startup validation
// Focused tests for article-retention configuration contract binding, validation, and runtime projection.

using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Configuration;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Startup.Validation
{
    /// <summary>
    /// Verifies the external article-retention configuration contract and projection into runtime retention options.
    /// </summary>
    public sealed class ArticleRetentionConfigurationContractTests
    {
        private const long BytesPerGibibyte = 1024L * 1024L * 1024L;

        [Fact]
        public void ValidateBackFillerOptions_WhenRetentionSettingsUseDefaults_AcceptsConfiguration()
        {
            IConfiguration configuration = BuildConfiguration([]);
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

            Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:ArticleRetention", StringComparison.Ordinal));
            Assert.Equal(4, options.ArticleRetention.MaximumRetainedPayloadGigabytes);
            Assert.Equal(60, options.ArticleRetention.RetentionTtlSeconds);
            Assert.Equal(1, options.ArticleRetention.SweepIntervalSeconds);
        }

        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenRetentionSettingsUseDefaults_ProjectsExpectedRuntimeDefaults()
        {
            IConfiguration configuration = BuildConfiguration([]);
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Error)> configErrors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(configuration, options, configErrors);

            Assert.Empty(configErrors);
            Assert.NotNull(runtimeOptions);
            Assert.Equal(4L * BytesPerGibibyte, runtimeOptions.EffectiveArticleRetention.MaximumRetainedPayloadBytes);
            Assert.Equal(TimeSpan.FromSeconds(60), runtimeOptions.EffectiveArticleRetention.RetentionTtl);
            Assert.Equal(TimeSpan.FromSeconds(1), runtimeOptions.EffectiveArticleRetention.SweepInterval);
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenMaximumRetainedPayloadGigabytesValid_AcceptsConfiguration()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = "8",
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes");
        }

        [Theory]
        [InlineData(1)]
        [InlineData(60)]
        public void ValidateBackFillerOptions_WhenRetentionTtlSecondsOnBoundary_AcceptsConfiguration(int ttlSeconds)
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:RetentionTtlSeconds"] = ttlSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:ArticleRetention:RetentionTtlSeconds");
        }

        [Theory]
        [InlineData(1)]
        [InlineData(60)]
        public void ValidateBackFillerOptions_WhenSweepIntervalSecondsOnBoundary_AcceptsConfiguration(int sweepSeconds)
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:SweepIntervalSeconds"] = sweepSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:ArticleRetention:SweepIntervalSeconds");
        }

        [Theory]
        [InlineData(0)]
        [InlineData(61)]
        public void ValidateBackFillerOptions_WhenRetentionTtlSecondsOutsideRange_RejectsConfiguration(int ttlSeconds)
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:RetentionTtlSeconds"] = ttlSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:ArticleRetention:RetentionTtlSeconds"
                && e.Error.Contains("between 1 and 60", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(61)]
        public void ValidateBackFillerOptions_WhenSweepIntervalSecondsOutsideRange_RejectsConfiguration(int sweepSeconds)
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:SweepIntervalSeconds"] = sweepSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:ArticleRetention:SweepIntervalSeconds"
                && e.Error.Contains("between 1 and 60", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void ValidateBackFillerOptions_WhenMaximumRetainedPayloadGigabytesNotPositive_RejectsConfiguration(int configuredGigabytes)
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"
                && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenMaximumRetainedPayloadGigabytesConfigured_ProjectsExactBytes()
        {
            const int configuredGigabytes = 7;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Error)> configErrors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(configuration, options, configErrors);

            Assert.Empty(configErrors);
            Assert.NotNull(runtimeOptions);
            Assert.Equal(checked(configuredGigabytes * BytesPerGibibyte), runtimeOptions.EffectiveArticleRetention.MaximumRetainedPayloadBytes);
        }

        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenMaximumRetainedPayloadGigabytesAtIntMax_ProjectsWithoutOverflow()
        {
            const int configuredGigabytes = int.MaxValue;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Error)> configErrors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(configuration, options, configErrors);

            Assert.Empty(configErrors);
            Assert.NotNull(runtimeOptions);
            Assert.Equal(checked(configuredGigabytes * BytesPerGibibyte), runtimeOptions.EffectiveArticleRetention.MaximumRetainedPayloadBytes);
        }

        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenRetentionTtlSecondsConfigured_ProjectsEquivalentTimeSpan()
        {
            const int configuredTtlSeconds = 37;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:RetentionTtlSeconds"] = configuredTtlSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Error)> configErrors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(configuration, options, configErrors);

            Assert.Empty(configErrors);
            Assert.NotNull(runtimeOptions);
            Assert.Equal(TimeSpan.FromSeconds(configuredTtlSeconds), runtimeOptions.EffectiveArticleRetention.RetentionTtl);
        }

        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenSweepIntervalSecondsConfigured_ProjectsEquivalentTimeSpan()
        {
            const int configuredSweepIntervalSeconds = 9;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:SweepIntervalSeconds"] = configuredSweepIntervalSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Error)> configErrors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(configuration, options, configErrors);

            Assert.Empty(configErrors);
            Assert.NotNull(runtimeOptions);
            Assert.Equal(TimeSpan.FromSeconds(configuredSweepIntervalSeconds), runtimeOptions.EffectiveArticleRetention.SweepInterval);
        }

        [Theory]
        [InlineData("BackFiller:ArticleRetention:RetentionTtlSeconds", "00:00:05")]
        [InlineData("BackFiller:ArticleRetention:SweepIntervalSeconds", "00:00:01")]
        public void ValidateBackFillerOptions_WhenRetentionSecondsConfiguredAsTimeSpanString_RejectsConfiguration(string settingKey, string configuredValue)
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                [settingKey] = configuredValue,
            });

            Assert.Throws<InvalidOperationException>(() => configuration.GetSection("BackFiller").Get<BackFillerOptions>());
        }

        private static BackFillerOptions BindBackFillerOptions(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            return configuration.GetSection("BackFiller").Get<BackFillerOptions>()
                ?? throw new InvalidOperationException("BackFiller section is required for this test scenario.");
        }

        private static IConfiguration BuildConfiguration(Dictionary<string, string?> overrides)
        {
            ArgumentNullException.ThrowIfNull(overrides);

            string root = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            string logs = Path.Combine(root, "logs");
            string certs = Path.Combine(root, "certs");

            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = certs,
                ["BackFiller:DirLogs"] = logs,
                ["BackFiller:LetsEncrypt:Enabled"] = "false",
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
                ["BackFiller:TransitServer:Port"] = "563",
                ["BackFiller:TransitServer:UseSsl"] = "true",
            };

            foreach (KeyValuePair<string, string?> entry in overrides)
            {
                values[entry.Key] = entry.Value;
            }

            return new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build();
        }
    }
}
