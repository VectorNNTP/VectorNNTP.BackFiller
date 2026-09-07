// <copyright file="LetsEncryptEnabledValidationFlowTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for lets encrypt validation flow, covering configuration and validation contracts; certificate and DNS dependency behavior.

using Microsoft.Extensions.Configuration;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Startup.Validation
{
    /// <summary>
    /// Tests mandatory TLS/ACME configuration validation behavior.
    /// </summary>
    public class LetsEncryptEnabledValidationFlowTests
    {
        [Fact]
        public void ValidateBackFillerOptions_WhenBindAddressIsOmitted_DoesNotReturnBindAddressErrors()
        {
            IConfiguration configuration = BuildBackFillerConfiguration(bindAddresses: null);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:BindAddress", StringComparison.Ordinal));
        }

        [Theory]
        [InlineData("0.0.0.0")]
        [InlineData("::")]
        public void ValidateBackFillerOptions_WhenBindAddressIsWildcard_DoesNotReturnLocalAssignmentError(string bindAddress)
        {
            IConfiguration configuration = BuildBackFillerConfiguration([bindAddress]);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting.StartsWith("BackFiller:BindAddress", StringComparison.Ordinal)
                && e.Error.Contains("not assigned to any local network interface", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenBindAddressContainsDuplicate_ReturnsDuplicateBindAddressError()
        {
            IConfiguration configuration = BuildBackFillerConfiguration("127.0.0.1", "127.0.0.1");

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:BindAddress[1]"
                && e.Error.Contains("Duplicate bind address", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData("0")]
        [InlineData("65536")]
        [InlineData("-1")]
        public void ValidateBackFillerOptions_WhenBindPortIsOutOfRange_ReturnsBindPortRangeError(string bindPort)
        {
            IConfiguration configuration = BuildBackFillerConfigurationWithRawBindPort(
                bindPort: bindPort,
                bindAddresses: ["127.0.0.1"],
                domainNames: null,
                includeLegacyEnabledKey: false);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:BindPort"
                && e.Error.Contains("between 1 and 65535", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void ValidateBackFillerOptions_RequiresAcmeAndCloudflareSettings()
        {
            IConfiguration configuration = BuildBackFillerConfiguration();

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

        [Fact]
        public void ValidateBackFillerOptions_WhenLegacyLetsEncryptEnabledKeyProvided_ReturnsUnsupportedSettingError()
        {
            IConfiguration configuration = BuildBackFillerConfigurationWithRawBindPort(
                bindPort: "119",
                bindAddresses: ["127.0.0.1"],
                domainNames: null,
                includeLegacyEnabledKey: true);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:LetsEncrypt:Enabled"
                && e.Error.Contains("no longer supported", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void ValidateBackFillerOptions_WhenConfiguredDomainNamesInvalid_DoesNotUseConfiguredDomainNames()
        {
            IConfiguration configuration = BuildBackFillerConfigurationWithRawBindPort(
                bindPort: "119",
                bindAddresses: ["127.0.0.1"],
                domainNames: ["invalid domain"],
                includeLegacyEnabledKey: false);

            List<(string Setting, string Error)> errors = InvokeValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting.StartsWith("BackFiller:LetsEncrypt:DomainNames", StringComparison.Ordinal));
        }

        private static List<(string Setting, string Error)> InvokeValidateBackFillerOptions(IConfiguration configuration)
        {
            return global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);
        }

        private static IConfiguration BuildBackFillerConfiguration(params string[]? bindAddresses)
        {
            return BuildBackFillerConfigurationWithRawBindPort("119", bindAddresses, domainNames: null, includeLegacyEnabledKey: false);
        }

        private static IConfiguration BuildBackFillerConfigurationWithRawBindPort(
            string bindPort,
            string[]? bindAddresses,
            string[]? domainNames,
            bool includeLegacyEnabledKey)
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:BindPort"] = bindPort,
                ["BackFiller:Name"] = "backfiller",
                ["BackFiller:Id"] = "1",
                ["BackFiller:DnsSuffix"] = "usenet.ninja",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "",
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = "",
                ["BackFiller:LetsEncrypt:RenewalCheckIntervalHours"] = "",
                ["BackFiller:LetsEncrypt:RenewalJitterRatio"] = "",
                ["BackFiller:LetsEncrypt:RenewBeforeExpiryDays"] = ""
            };

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

            if (includeLegacyEnabledKey)
            {
                values["BackFiller:LetsEncrypt:Enabled"] = "false";
            }

            return new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build();
        }
    }
}
