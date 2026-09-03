// <copyright file="LetsEncryptEnabledValidationFlowTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for lets encrypt enabled validation flow, covering configuration and validation contracts; certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the lets encrypt enabled validation flow test suite.

using Microsoft.Extensions.Configuration;
using Startup = global::VectorNNTP.Backfiller.Startup;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Startup.Validation
{
    /// <summary>
    /// Tests conditional Let's Encrypt validation behavior when TLS is enabled or disabled.
    /// </summary>
    public class LetsEncryptEnabledValidationFlowTests
    {
        /// <summary>
        /// Confirms the validate back filler options when bind address is omitted does not return bind address errors behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenBindAddressIsOmitted_DoesNotReturnBindAddressErrors()
        {
            IConfiguration configuration = BuildBackFillerConfiguration(enabled: false, bindAddresses: null);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:BindAddress", StringComparison.Ordinal));
        }
        /// <summary>
        /// Confirms the validate back filler options when bind address is wildcard does not return local assignment error behavior.
        /// </summary>
        [Theory]
        [InlineData("0.0.0.0")]
        [InlineData("::")]
        public void ValidateBackFillerOptions_WhenBindAddressIsWildcard_DoesNotReturnLocalAssignmentError(string bindAddress)
        {
            IConfiguration configuration = BuildBackFillerConfiguration(enabled: false, [bindAddress]);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting.StartsWith("BackFiller:BindAddress", StringComparison.Ordinal)
                && e.Error.Contains("not assigned to any local network interface", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when bind address contains duplicate returns duplicate bind address error behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenBindAddressContainsDuplicate_ReturnsDuplicateBindAddressError()
        {
            IConfiguration configuration = BuildBackFillerConfiguration(enabled: false, "127.0.0.1", "127.0.0.1");

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:BindAddress[1]"
                && e.Error.Contains("Duplicate bind address", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when bind port is out of range returns bind port range error behavior.
        /// </summary>
        [Theory]
        [InlineData("0")]
        [InlineData("65536")]
        [InlineData("-1")]
        public void ValidateBackFillerOptions_WhenBindPortIsOutOfRange_ReturnsBindPortRangeError(string bindPort)
        {
            IConfiguration configuration = BuildBackFillerConfigurationWithRawBindPort(
                enabled: false,
                bindPort: bindPort,
                bindAddresses: ["127.0.0.1"],
                domainNames: null);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:BindPort"
                && e.Error.Contains("between 1 and 65535", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when lets encrypt disabled does not require acme settings behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenLetsEncryptDisabled_DoesNotRequireAcmeSettings()
        {
            IConfiguration configuration = BuildBackFillerConfiguration(enabled: false);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting is "BackFiller:LetsEncrypt:AcmeAccountEmail"
                or "BackFiller:LetsEncrypt:AcmeAccountKeyPem"
                or "BackFiller:LetsEncrypt:PfxExportPassword"
                or "BackFiller:LetsEncrypt:RenewalCheckIntervalHours"
                or "BackFiller:LetsEncrypt:RenewalJitterRatio"
                or "BackFiller:LetsEncrypt:RenewBeforeExpiryDays");
        }
        /// <summary>
        /// Confirms the validate back filler options when lets encrypt disabled ignores invalid acme and renewal settings behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenLetsEncryptDisabled_IgnoresInvalidAcmeAndRenewalSettings()
        {
            // Use the shared BuildBackFillerConfiguration helper to supply the standard baseline
            // (includes a minimal RabbitMQ baseline) while customizing Let's Encrypt values.
            IConfiguration configuration = BuildBackFillerConfiguration(enabled: false);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting is "BackFiller:LetsEncrypt:AcmeAccountEmail"
                or "BackFiller:LetsEncrypt:AcmeAccountKeyPem"
                or "BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts"
                or "BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes"
                or "BackFiller:LetsEncrypt:ClockSkewMaxMinutes"
                or "BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes"
                or "BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio"
                or "BackFiller:LetsEncrypt:DnsPropagationDelaySeconds"
                or "BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds"
                or "BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds"
                or "BackFiller:LetsEncrypt:PfxExportPassword"
                or "BackFiller:LetsEncrypt:RenewalCheckIntervalHours"
                or "BackFiller:LetsEncrypt:RenewalJitterRatio"
                or "BackFiller:LetsEncrypt:RenewBeforeExpiryDays");
        }
        /// <summary>
        /// Confirms the validate back filler options when lets encrypt disabled requires cloudflare settings behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenLetsEncryptDisabled_RequiresCloudflareSettings()
        {
            IConfiguration configuration = BuildBackFillerConfiguration(enabled: false);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:CloudFlareApiToken");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:CloudFlareZoneId");
        }
        /// <summary>
        /// Confirms the validate back filler options when lets encrypt disabled and cloudflare configured is valid with invalid acme settings behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenLetsEncryptDisabledAndCloudflareConfigured_IsValidWithInvalidAcmeSettings()
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["BackFiller:BindPort"] = "119",
                    ["BackFiller:Name"] = "backfiller",
                    ["BackFiller:Id"] = "1",
                    ["BackFiller:DnsSuffix"] = "usenet.ninja",
                    ["BackFiller:DirCerts"] = "certs",
                    ["BackFiller:LetsEncrypt:Enabled"] = "false",
                    ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "not-an-email",
                    ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "C:\\absolute\\invalid-account-key.pem",
                    ["BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts"] = "0",
                    ["BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes"] = "0",
                    ["BackFiller:LetsEncrypt:ClockSkewMaxMinutes"] = "0",
                    ["BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes"] = "0",
                    ["BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio"] = "0",
                    ["BackFiller:LetsEncrypt:DnsPropagationDelaySeconds"] = "-1",
                    ["BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds"] = "0",
                    ["BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds"] = "0",
                    ["BackFiller:LetsEncrypt:PfxExportPassword"] = "shortpass",
                    ["BackFiller:LetsEncrypt:RenewalCheckIntervalHours"] = "0",
                    ["BackFiller:LetsEncrypt:RenewalJitterRatio"] = "1",
                    ["BackFiller:LetsEncrypt:RenewBeforeExpiryDays"] = "0",
                    ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                    ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                    // Minimal RabbitMQ baseline so validator does not fail unrelatedly
                    ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.1",
                    ["BackFiller:RabbitMQ:Port"] = "5672",
                    ["BackFiller:RabbitMQ:VirtualHost"] = "/",
                    ["BackFiller:RabbitMQ:EnableSsl"] = "false",
                    ["BackFiller:RabbitMQ:Username"] = "nntparticles",
                    ["BackFiller:RabbitMQ:Password"] = "password-1",
                    ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                    ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                })
                .Build();

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:LetsEncrypt", StringComparison.Ordinal));
            Assert.Empty(errors);
        }
        /// <summary>
        /// Confirms the validate back filler options when lets encrypt enabled requires acme and cloudflare settings behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenLetsEncryptEnabled_RequiresAcmeAndCloudflareSettings()
        {
            IConfiguration configuration = BuildBackFillerConfiguration(enabled: true);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountEmail");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountKeyPem");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:CloudFlareApiToken");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:CloudFlareZoneId");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:PfxExportPassword");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:RenewalCheckIntervalHours");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:RenewalJitterRatio");
            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:RenewBeforeExpiryDays");
        }
        /// <summary>
        /// Confirms the validate back filler options when lets encrypt enabled and configured domain names invalid does not use configured domain names behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenLetsEncryptEnabledAndConfiguredDomainNamesInvalid_DoesNotUseConfiguredDomainNames()
        {
            IConfiguration configuration = BuildBackFillerConfigurationWithRawBindPort(
                enabled: true,
                bindPort: "119",
                bindAddresses: ["127.0.0.1"],
                domainNames: ["invalid domain"]);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting.StartsWith("BackFiller:LetsEncrypt:DomainNames", StringComparison.Ordinal));
        }

        /// <summary>
        /// Confirms invoke validate back filler options behavior.
        /// </summary>
        private static List<(string Setting, string Error)> InvokeValidateBackFillerOptions(IConfiguration configuration)
        {
            return global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);
        }

        /// <summary>
        /// Confirms the build back filler configuration behavior.
        /// </summary>
        /// <returns>The value returned by the build back filler configuration helper.</returns>
        /// <summary>
        /// Confirms the build back filler configuration behavior.
        /// </summary>
        /// <param name="enabled">The enabled used by this test scenario.</param>
        /// <param name="bindAddresses">The bind addresses used by this test scenario.</param>
        /// <returns>The value returned by the build back filler configuration helper.</returns>
        private static IConfiguration BuildBackFillerConfiguration(bool enabled, params string[]? bindAddresses)
        {
            return BuildBackFillerConfigurationWithRawBindPort(enabled, "119", bindAddresses, domainNames: null);
        }

        /// <summary>
        /// Confirms the build back filler configuration with raw bind port behavior.
        /// </summary>
        private static IConfiguration BuildBackFillerConfigurationWithRawBindPort(
            bool enabled,
            string bindPort,
            string[]? bindAddresses,
            string[]? domainNames)
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:BindPort"] = bindPort,
                ["BackFiller:Name"] = "backfiller",
                ["BackFiller:Id"] = "1",
                ["BackFiller:DnsSuffix"] = "usenet.ninja",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:Enabled"] = enabled ? "true" : "false",
                ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "",
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = "",
                ["BackFiller:LetsEncrypt:RenewalCheckIntervalHours"] = "",
                ["BackFiller:LetsEncrypt:RenewalJitterRatio"] = "",
                ["BackFiller:LetsEncrypt:RenewBeforeExpiryDays"] = ""
            };

            // Ensure a minimal valid RabbitMQ block is present so RabbitMQ validation does not interfere
            // with tests that focus on Let's Encrypt / Cloudflare behavior. Use the same minimal
            // baseline keys consumed by other tests to avoid duplicating the full baseline helper.
            values["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.5";
            values["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60";
            values["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30";
            values["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "120";
            values["BackFiller:RabbitMQ:Port"] = "5672";
            values["BackFiller:RabbitMQ:EnableSsl"] = "false";

            if (bindAddresses is { Length: > 0 })
            {
                for (int i = 0; i < bindAddresses.Length; i++)
                {
                    values[$"BackFiller:BindAddress:{i}"] = bindAddresses[i];
                }
            }

            if (domainNames is { Length: > 0 })
            {
                for (int i = 0; i < domainNames.Length; i++)
                {
                    values[$"BackFiller:LetsEncrypt:DomainNames:{i}"] = domainNames[i];
                }
            }

            return new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build();
        }

    }

}
