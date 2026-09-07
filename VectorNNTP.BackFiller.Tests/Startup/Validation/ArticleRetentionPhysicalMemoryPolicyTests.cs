// <copyright file="ArticleRetentionPhysicalMemoryPolicyTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Startup validation
// Focused tests for article-retention 80%-of-physical-memory configuration policy.

using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Startup.Configuration;
using Xunit;
using static VectorNNTP.BackFiller.Tests.Startup.Validation.ArticleRetentionPhysicalMemoryPolicyTests;

namespace VectorNNTP.BackFiller.Tests.Startup.Validation
{
    /// <summary>
    /// Verifies the 80%-of-physical-memory ceiling policy for article-retention configured gigabytes.
    /// </summary>
    public sealed class ArticleRetentionPhysicalMemoryPolicyTests
    {
        private const ulong BytesPerGibibyte = 1024UL * 1024UL * 1024UL;
        private const string MaximumRetainedPayloadGigabytesSetting = "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes";

        [Fact]
        public void ValidateBackFillerOptions_WhenConfiguredCapacityBelowPolicyCeiling_IsValid()
        {
            const ulong physicalMemoryBytes = 64UL * BytesPerGibibyte;
            const int configuredGigabytes = 50;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes");
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenConfiguredCapacityEqualsLargestWholeGibPolicyCeiling_IsValid()
        {
            const ulong physicalMemoryBytes = 64UL * BytesPerGibibyte;
            const int configuredGigabytes = 51;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes");
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenConfiguredCapacityExceedsLargestWholeGibPolicyCeilingByOne_IsInvalid()
        {
            const ulong physicalMemoryBytes = 64UL * BytesPerGibibyte;
            const int configuredGigabytes = 52;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"
                && e.Error.Contains("80% physical-memory ceiling", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenDefaultRetentionConfigured_IsValidOnRepresentativePhysicalMemory()
        {
            const ulong physicalMemoryBytes = 32UL * BytesPerGibibyte;
            IConfiguration configuration = BuildConfiguration([]);
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes");
            Assert.Equal(4, options.ArticleRetention.MaximumRetainedPayloadGigabytes);
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenPolicyBoundaryIsFractional_DoesNotRoundUp()
        {
            const ulong physicalMemoryBytes = 32UL * BytesPerGibibyte;
            IConfiguration validConfiguration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = "25",
            });
            IConfiguration invalidConfiguration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = "26",
            });

            BackFillerOptions validOptions = BindBackFillerOptions(validConfiguration);
            BackFillerOptions invalidOptions = BindBackFillerOptions(invalidConfiguration);
            List<(string Setting, string Message)> validWarnings = [];
            List<(string Setting, string Message)> invalidWarnings = [];
            FixedPhysicalSystemMemoryProvider provider = new(physicalMemoryBytes);

            List<(string Setting, string Error)> validErrors = ConfigurationValidator.ValidateBackFillerOptions(validOptions, validWarnings, provider);
            List<(string Setting, string Error)> invalidErrors = ConfigurationValidator.ValidateBackFillerOptions(invalidOptions, invalidWarnings, provider);

            Assert.DoesNotContain(validErrors, static e => e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes");
            Assert.Contains(invalidErrors, static e =>
                e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"
                && e.Error.Contains("80% physical-memory ceiling", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenPhysicalMemoryIsVeryLarge_DoesNotOverflowValidationMath()
        {
            const ulong physicalMemoryBytes = ulong.MaxValue;
            const int configuredGigabytes = int.MaxValue;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes");
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenPhysicalMemoryDiscoveryFails_ReturnsDeterministicRetentionValidationError()
        {
            const int configuredGigabytes = 4;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                [MaximumRetainedPayloadGigabytesSetting] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(
                options,
                warnings,
                new FailingPhysicalSystemMemoryProvider("/proc/meminfo MemTotal could not be read."));

            (string Setting, string Error) error = Assert.Single(errors, static e => e.Setting == MaximumRetainedPayloadGigabytesSetting);
            Assert.Contains("could not be determined", error.Error, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("could not be read", error.Error, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenPhysicalMemoryDiscoveryFails_DoesNotBypassEightyPercentSafetyPolicy()
        {
            const int configuredGigabytes = 1;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                [MaximumRetainedPayloadGigabytesSetting] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(
                options,
                warnings,
                new FailingPhysicalSystemMemoryProvider("GlobalMemoryStatusEx failed."));

            Assert.Contains(errors, static e => e.Setting == MaximumRetainedPayloadGigabytesSetting);
            Assert.DoesNotContain(errors, static e => e.Error.Contains("exceeds the allowed 80% physical-memory ceiling", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void ValidateBackFillerOptions_WhenConfiguredGigabytesNotPositive_RemainsInvalid(int configuredGigabytes)
        {
            const ulong physicalMemoryBytes = 64UL * BytesPerGibibyte;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"
                && e.Error.Contains("greater than zero", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(61)]
        public void ValidateBackFillerOptions_WhenRetentionTtlOutsideRange_RemainsInvalid(int ttlSeconds)
        {
            const ulong physicalMemoryBytes = 64UL * BytesPerGibibyte;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:RetentionTtlSeconds"] = ttlSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:ArticleRetention:RetentionTtlSeconds"
                && e.Error.Contains("between 1 and 60", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(61)]
        public void ValidateBackFillerOptions_WhenSweepIntervalOutsideRange_RemainsInvalid(int sweepSeconds)
        {
            const ulong physicalMemoryBytes = 64UL * BytesPerGibibyte;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:SweepIntervalSeconds"] = sweepSeconds.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];

            List<(string Setting, string Error)> errors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:ArticleRetention:SweepIntervalSeconds"
                && e.Error.Contains("between 1 and 60", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenConfiguredGigabytesIsValid_UsesExistingExactCheckedProjection()
        {
            const ulong physicalMemoryBytes = 64UL * BytesPerGibibyte;
            const int configuredGigabytes = 8;
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes"] = configuredGigabytes.ToString(),
            });
            BackFillerOptions options = BindBackFillerOptions(configuration);
            List<(string Setting, string Message)> warnings = [];
            List<(string Setting, string Error)> validationErrors = ConfigurationValidator.ValidateBackFillerOptions(options, warnings, new FixedPhysicalSystemMemoryProvider(physicalMemoryBytes));
            List<(string Setting, string Error)> projectionErrors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(configuration, options, projectionErrors);

            Assert.Empty(validationErrors);
            Assert.Empty(projectionErrors);
            Assert.NotNull(runtimeOptions);
            Assert.Equal(checked(configuredGigabytes * (long)BytesPerGibibyte), runtimeOptions.EffectiveArticleRetention.MaximumRetainedPayloadBytes);
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

        private sealed class FixedPhysicalSystemMemoryProvider(ulong totalPhysicalMemoryBytes) : IPhysicalSystemMemoryProvider
        {
            private readonly ulong _totalPhysicalMemoryBytes = totalPhysicalMemoryBytes;

            public ulong GetTotalPhysicalMemoryBytes() => _totalPhysicalMemoryBytes;
        }

        private sealed class FailingPhysicalSystemMemoryProvider(string reason) : IPhysicalSystemMemoryProvider
        {
            private readonly string _reason = reason;

            public ulong GetTotalPhysicalMemoryBytes()
            {
                throw new PhysicalSystemMemoryDiscoveryException(_reason);
            }
        }
    }
}
