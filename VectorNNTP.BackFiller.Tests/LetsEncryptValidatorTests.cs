// <copyright file="LetsEncryptValidatorTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for lets encrypt validator, covering configuration and validation contracts; certificate and DNS dependency behavior.
// Primary responsibility: documents the executable contracts covered by the lets encrypt validator test suite.

using System.Security.Cryptography;
using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests for Let's Encrypt configuration validation.
    /// </summary>
    /// <remarks>
    /// DomainNames tests in this class cover syntax/shape validation only.
    /// Primary certificate identity authority is the generated canonical BackFiller FQDN.
    /// </remarks>
    public class LetsEncryptValidatorTests
    {
        /// <summary>
        /// Exercises validate acme account email  when valid email  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateAcmeAccountEmail_WhenValidEmail_ReturnsNoErrors()
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountEmail(
                acmeAccountEmail: "security@usenet.ninja",
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate acme account email  when missing  returns required error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void ValidateAcmeAccountEmail_WhenMissing_ReturnsRequiredError(string? acmeAccountEmail)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountEmail(
                acmeAccountEmail,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);

            Assert.Equal("BackFiller:LetsEncrypt:AcmeAccountEmail", error.Setting);
            Assert.Contains("required", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Exercises validate acme account email  when contains whitespace or control characters  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData("security @usenet.ninja")]
        [InlineData("security@usenet.ninja ")]
        [InlineData("security@usenet.ninja\t")]
        [InlineData("security@usenet.ninja\r\n")]
        public void ValidateAcmeAccountEmail_WhenContainsWhitespaceOrControlCharacters_ReturnsError(string acmeAccountEmail)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountEmail(
                acmeAccountEmail,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);

            Assert.Equal("BackFiller:LetsEncrypt:AcmeAccountEmail", error.Setting);
            Assert.Contains("whitespace", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Exercises validate acme account email  when email syntax invalid  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData("not-an-email")]
        [InlineData("security@")]
        [InlineData("@usenet.ninja")]
        public void ValidateAcmeAccountEmail_WhenEmailSyntaxInvalid_ReturnsError(string acmeAccountEmail)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountEmail(
                acmeAccountEmail,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);

            Assert.Equal("BackFiller:LetsEncrypt:AcmeAccountEmail", error.Setting);
            Assert.Contains("valid email", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Exercises validate acme account key pem  when valid relative pem file  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateAcmeAccountKeyPem_WhenValidRelativePemFile_ReturnsNoErrors()
        {
            string certDirectory = CreateUniqueTempDirectory();
            string keyFileName = "account-key.pem";
            string keyFilePath = Path.Combine(certDirectory, keyFileName);

            using RSA rsa = RSA.Create(2048);
            File.WriteAllText(keyFilePath, rsa.ExportPkcs8PrivateKeyPem());

            try
            {
                List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountKeyPem(
                    acmeAccountKeyPem: keyFileName,
                    dirCerts: certDirectory,
                    settingPrefix: "BackFiller:LetsEncrypt");

                Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
            }
            finally
            {
                DeleteDirectoryIfExists(certDirectory);
            }
        }
        /// <summary>
        /// Exercises validate acme account key pem  when absolute path provided  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateAcmeAccountKeyPem_WhenAbsolutePathProvided_ReturnsError()
        {
            string certDirectory = CreateUniqueTempDirectory();
            string keyPath = Path.Combine(certDirectory, "account-key.pem");

            try
            {
                List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountKeyPem(
                    acmeAccountKeyPem: keyPath,
                    dirCerts: certDirectory,
                    settingPrefix: "BackFiller:LetsEncrypt");

                LetsEncryptValidationResult error = Assert.Single(diagnostics);
                Assert.Equal("BackFiller:LetsEncrypt:AcmeAccountKeyPem", error.Setting);
                Assert.Contains("relative", error.Message, StringComparison.OrdinalIgnoreCase);
            }
            finally
            {
                DeleteDirectoryIfExists(certDirectory);
            }
        }
        /// <summary>
        /// Exercises validate acme account key pem  when file missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateAcmeAccountKeyPem_WhenFileMissing_ReturnsError()
        {
            string certDirectory = CreateUniqueTempDirectory();

            try
            {
                List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountKeyPem(
                    acmeAccountKeyPem: "missing-account-key.pem",
                    dirCerts: certDirectory,
                    settingPrefix: "BackFiller:LetsEncrypt");

                LetsEncryptValidationResult error = Assert.Single(diagnostics);
                Assert.Equal("BackFiller:LetsEncrypt:AcmeAccountKeyPem", error.Setting);
                Assert.Contains("does not exist", error.Message, StringComparison.OrdinalIgnoreCase);
            }
            finally
            {
                DeleteDirectoryIfExists(certDirectory);
            }
        }
        /// <summary>
        /// Exercises validate acme account key pem  when pem content invalid  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateAcmeAccountKeyPem_WhenPemContentInvalid_ReturnsError()
        {
            string certDirectory = CreateUniqueTempDirectory();
            string keyFileName = "account-key.pem";
            string keyFilePath = Path.Combine(certDirectory, keyFileName);
            File.WriteAllText(keyFilePath, "not-a-pem-private-key");

            try
            {
                List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountKeyPem(
                    acmeAccountKeyPem: keyFileName,
                    dirCerts: certDirectory,
                    settingPrefix: "BackFiller:LetsEncrypt");

                LetsEncryptValidationResult error = Assert.Single(diagnostics);
                Assert.Equal("BackFiller:LetsEncrypt:AcmeAccountKeyPem", error.Setting);
                Assert.Contains("private key", error.Message, StringComparison.OrdinalIgnoreCase);
            }
            finally
            {
                DeleteDirectoryIfExists(certDirectory);
            }
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults acme account email to security at usenet ninja behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsAcmeAccountEmailToSecurityAtUsenetNinja()
        {
            LetsEncryptOptions options = new();

            Assert.Equal("security@usenet.ninja", options.AcmeAccountEmail);
        }
        /// <summary>
        /// Exercises validate acme transient retry max attempts  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public void ValidateAcmeTransientRetryMaxAttempts_WhenWithinRange_ReturnsNoErrors(int maxAttempts)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeTransientRetryMaxAttempts(
                acmeTransientRetryMaxAttempts: maxAttempts,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate acme transient retry max attempts  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(11)]
        public void ValidateAcmeTransientRetryMaxAttempts_WhenOutOfRangeOrMissing_ReturnsError(int? maxAttempts)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeTransientRetryMaxAttempts(
                acmeTransientRetryMaxAttempts: maxAttempts,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults acme account key pem to account key behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsAcmeAccountKeyPemToAccountKey()
        {
            LetsEncryptOptions options = new();

            Assert.Equal("account.key", options.AcmeAccountKeyPem);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults acme transient retry max attempts to five behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsAcmeTransientRetryMaxAttemptsToFive()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(5, options.AcmeTransientRetryMaxAttempts);
        }
        /// <summary>
        /// Exercises validate clock skew check ttl minutes  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(60)]
        public void ValidateClockSkewCheckTtlMinutes_WhenWithinRange_ReturnsNoErrors(int ttlMinutes)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateClockSkewCheckTtlMinutes(
                clockSkewCheckTtlMinutes: ttlMinutes,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate clock skew check ttl minutes  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(61)]
        public void ValidateClockSkewCheckTtlMinutes_WhenOutOfRangeOrMissing_ReturnsError(int? ttlMinutes)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateClockSkewCheckTtlMinutes(
                clockSkewCheckTtlMinutes: ttlMinutes,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults clock skew check ttl minutes to five behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsClockSkewCheckTtlMinutesToFive()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(5, options.ClockSkewCheckTtlMinutes);
        }
        /// <summary>
        /// Exercises validate clock skew max minutes  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(60)]
        public void ValidateClockSkewMaxMinutes_WhenWithinRange_ReturnsNoErrors(int maxMinutes)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateClockSkewMaxMinutes(
                clockSkewMaxMinutes: maxMinutes,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate clock skew max minutes  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(61)]
        public void ValidateClockSkewMaxMinutes_WhenOutOfRangeOrMissing_ReturnsError(int? maxMinutes)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateClockSkewMaxMinutes(
                clockSkewMaxMinutes: maxMinutes,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:ClockSkewMaxMinutes", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults clock skew max minutes to ten behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsClockSkewMaxMinutesToTen()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(10, options.ClockSkewMaxMinutes);
        }
        /// <summary>
        /// Exercises validate cloud flare api token  when valid  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateCloudFlareApiToken_WhenValid_ReturnsNoErrors()
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateCloudFlareApiToken(
                cloudFlareApiToken: "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate cloud flare api token  when missing or invalid  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("token with space")]
        [InlineData("token\twith-tab")]
        public void ValidateCloudFlareApiToken_WhenMissingOrInvalid_ReturnsError(string? token)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateCloudFlareApiToken(
                cloudFlareApiToken: token,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:CloudFlareApiToken", error.Setting);
        }
        /// <summary>
        /// Exercises validate cloud flare api token  when template placeholder used  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateCloudFlareApiToken_WhenTemplatePlaceholderUsed_ReturnsError()
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateCloudFlareApiToken(
                cloudFlareApiToken: "YOUR_CLOUDFLARE_API_TOKEN",
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:CloudFlareApiToken", error.Setting);
            Assert.Contains("placeholder", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Exercises validate cloud flare zone id  when valid  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData("5811a29d39a0732afb5f160c9b137c3d")]
        [InlineData("00000000000000000000000000000000")]
        public void ValidateCloudFlareZoneId_WhenValid_ReturnsNoErrors(string zoneId)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateCloudFlareZoneId(
                cloudFlareZoneId: zoneId,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate cloud flare zone id  when missing or invalid  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("5811A29D39A0732AFB5F160C9B137C3D")]
        [InlineData("5811a29d-39a0-732a-fb5f-160c9b137c3d")]
        [InlineData("5811a29d39a0732afb5f160c9b137c3")]
        [InlineData("5811a29d39a0732afb5f160c9b137c3dg")]
        public void ValidateCloudFlareZoneId_WhenMissingOrInvalid_ReturnsError(string? zoneId)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateCloudFlareZoneId(
                cloudFlareZoneId: zoneId,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:CloudFlareZoneId", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults cloud flare api token to placeholder behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsCloudFlareApiTokenToPlaceholder()
        {
            LetsEncryptOptions options = new();

            Assert.Equal("YOUR_CLOUDFLARE_API_TOKEN", options.CloudFlareApiToken);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults cloud flare zone id to usenet ninja zone behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsCloudFlareZoneIdToUsenetNinjaZone()
        {
            LetsEncryptOptions options = new();

            Assert.Equal("5811a29d39a0732afb5f160c9b137c3d", options.CloudFlareZoneId);
        }
        /// <summary>
        /// Exercises validate dns authoritative ns cache minutes  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(60)]
        public void ValidateDnsAuthoritativeNsCacheMinutes_WhenWithinRange_ReturnsNoErrors(int cacheMinutes)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsAuthoritativeNsCacheMinutes(
                dnsAuthoritativeNsCacheMinutes: cacheMinutes,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate dns authoritative ns cache minutes  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(61)]
        public void ValidateDnsAuthoritativeNsCacheMinutes_WhenOutOfRangeOrMissing_ReturnsError(int? cacheMinutes)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsAuthoritativeNsCacheMinutes(
                dnsAuthoritativeNsCacheMinutes: cacheMinutes,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults dns authoritative ns cache minutes to five behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsDnsAuthoritativeNsCacheMinutesToFive()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(5, options.DnsAuthoritativeNsCacheMinutes);
        }
        /// <summary>
        /// Exercises validate dns authoritative quorum ratio  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(0.1)]
        [InlineData(0.7)]
        [InlineData(1.0)]
        public void ValidateDnsAuthoritativeQuorumRatio_WhenWithinRange_ReturnsNoErrors(double quorumRatio)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsAuthoritativeQuorumRatio(
                dnsAuthoritativeQuorumRatio: quorumRatio,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate dns authoritative quorum ratio  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0.0)]
        [InlineData(-0.1)]
        [InlineData(1.1)]
        [InlineData(double.NaN)]
        [InlineData(double.PositiveInfinity)]
        [InlineData(double.NegativeInfinity)]
        public void ValidateDnsAuthoritativeQuorumRatio_WhenOutOfRangeOrMissing_ReturnsError(double? quorumRatio)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsAuthoritativeQuorumRatio(
                dnsAuthoritativeQuorumRatio: quorumRatio,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults dns authoritative quorum ratio to point seven behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsDnsAuthoritativeQuorumRatioToPointSeven()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(0.7, options.DnsAuthoritativeQuorumRatio);
        }
        /// <summary>
        /// Exercises validate dns propagation delay seconds  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(15)]
        [InlineData(600)]
        public void ValidateDnsPropagationDelaySeconds_WhenWithinRange_ReturnsNoErrors(int delaySeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsPropagationDelaySeconds(
                dnsPropagationDelaySeconds: delaySeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate dns propagation delay seconds  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(-1)]
        [InlineData(601)]
        public void ValidateDnsPropagationDelaySeconds_WhenOutOfRangeOrMissing_ReturnsError(int? delaySeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsPropagationDelaySeconds(
                dnsPropagationDelaySeconds: delaySeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:DnsPropagationDelaySeconds", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults dns propagation delay seconds to fifteen behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsDnsPropagationDelaySecondsToFifteen()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(15, options.DnsPropagationDelaySeconds);
        }
        /// <summary>
        /// Exercises validate dns txt poll interval seconds  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(60)]
        public void ValidateDnsTxtPollIntervalSeconds_WhenWithinRange_ReturnsNoErrors(int intervalSeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsTxtPollIntervalSeconds(
                dnsTxtPollIntervalSeconds: intervalSeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate dns txt poll interval seconds  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(61)]
        public void ValidateDnsTxtPollIntervalSeconds_WhenOutOfRangeOrMissing_ReturnsError(int? intervalSeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsTxtPollIntervalSeconds(
                dnsTxtPollIntervalSeconds: intervalSeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults dns txt poll interval seconds to three behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsDnsTxtPollIntervalSecondsToThree()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(3, options.DnsTxtPollIntervalSeconds);
        }
        /// <summary>
        /// Exercises validate dns txt poll timeout seconds  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(600)]
        [InlineData(3600)]
        public void ValidateDnsTxtPollTimeoutSeconds_WhenWithinRange_ReturnsNoErrors(int timeoutSeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsTxtPollTimeoutSeconds(
                dnsTxtPollTimeoutSeconds: timeoutSeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate dns txt poll timeout seconds  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(3601)]
        public void ValidateDnsTxtPollTimeoutSeconds_WhenOutOfRangeOrMissing_ReturnsError(int? timeoutSeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsTxtPollTimeoutSeconds(
                dnsTxtPollTimeoutSeconds: timeoutSeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults dns txt poll timeout seconds to six hundred behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsDnsTxtPollTimeoutSecondsToSixHundred()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(600, options.DnsTxtPollTimeoutSeconds);
        }
        /// <summary>
        /// Exercises validate dns txt polling coherence  when timeout greater than interval  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1, 2)]
        [InlineData(3, 600)]
        [InlineData(59, 60)]
        public void ValidateDnsTxtPollingCoherence_WhenTimeoutGreaterThanInterval_ReturnsNoErrors(int intervalSeconds, int timeoutSeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsTxtPollingCoherence(
                dnsTxtPollIntervalSeconds: intervalSeconds,
                dnsTxtPollTimeoutSeconds: timeoutSeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate dns txt polling coherence  when timeout not greater than interval  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(3, 3)]
        [InlineData(60, 1)]
        public void ValidateDnsTxtPollingCoherence_WhenTimeoutNotGreaterThanInterval_ReturnsError(int intervalSeconds, int timeoutSeconds)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDnsTxtPollingCoherence(
                dnsTxtPollIntervalSeconds: intervalSeconds,
                dnsTxtPollTimeoutSeconds: timeoutSeconds,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds", error.Setting);
        }
        /// <summary>
        /// Exercises validate domain names syntax  when null  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateDomainNamesSyntax_WhenNull_ReturnsNoErrors()
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDomainNames(
                domainNames: null,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.Empty(diagnostics);
        }
        /// <summary>
        /// Exercises validate domain names syntax  when empty array  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidateDomainNamesSyntax_WhenEmptyArray_ReturnsError()
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDomainNames(
                domainNames: [],
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:DomainNames", error.Setting);
        }
        /// <summary>
        /// Exercises validate domain names syntax  when entries are valid  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData("nntp.example.com")]
        [InlineData("nntp.example.net")]
        [InlineData("*.example.com")]
        public void ValidateDomainNamesSyntax_WhenEntriesAreValid_ReturnsNoErrors(string domainName)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDomainNames(
                domainNames: [domainName],
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate domain names syntax  when entry is invalid  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("bad host")]
        [InlineData("https://example.com")]
        [InlineData("*example.com")]
        [InlineData("*.com")]
        public void ValidateDomainNamesSyntax_WhenEntryIsInvalid_ReturnsError(string domainName)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDomainNames(
                domainNames: [domainName],
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.Contains(diagnostics, d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults domain names to null behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsDomainNamesToNull()
        {
            LetsEncryptOptions options = new();

            Assert.Null(options.DomainNames);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults enabled to true behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsEnabledToTrue()
        {
            LetsEncryptOptions options = new();

            Assert.True(options.Enabled);
        }
        /// <summary>
        /// Exercises validate pfx export password  when valid  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidatePfxExportPassword_WhenValid_ReturnsNoErrors()
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidatePfxExportPassword(
                pfxExportPassword: "s3curePfxPassword",
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate pfx export password  when missing weak or whitespace  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("shortpass")]
        [InlineData("space pass12")]
        public void ValidatePfxExportPassword_WhenMissingWeakOrWhitespace_ReturnsError(string? password)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidatePfxExportPassword(
                pfxExportPassword: password,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:PfxExportPassword", error.Setting);
        }
        /// <summary>
        /// Exercises validate pfx export password  when template placeholder used  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ValidatePfxExportPassword_WhenTemplatePlaceholderUsed_ReturnsError()
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidatePfxExportPassword(
                pfxExportPassword: "YOUR_PFX_PASSWORD",
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:PfxExportPassword", error.Setting);
            Assert.Contains("placeholder", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults pfx export password to placeholder behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsPfxExportPasswordToPlaceholder()
        {
            LetsEncryptOptions options = new();

            Assert.Equal("YOUR_PFX_PASSWORD", options.PfxExportPassword);
        }
        /// <summary>
        /// Exercises validate renewal check interval hours  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(6)]
        [InlineData(168)]
        public void ValidateRenewalCheckIntervalHours_WhenWithinRange_ReturnsNoErrors(int intervalHours)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateRenewalCheckIntervalHours(
                renewalCheckIntervalHours: intervalHours,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate renewal check interval hours  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(169)]
        public void ValidateRenewalCheckIntervalHours_WhenOutOfRangeOrMissing_ReturnsError(int? intervalHours)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateRenewalCheckIntervalHours(
                renewalCheckIntervalHours: intervalHours,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:RenewalCheckIntervalHours", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults renewal check interval hours to six behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsRenewalCheckIntervalHoursToSix()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(6, options.RenewalCheckIntervalHours);
        }
        /// <summary>
        /// Exercises validate renewal jitter ratio  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(0.0)]
        [InlineData(0.1)]
        [InlineData(0.9)]
        public void ValidateRenewalJitterRatio_WhenWithinRange_ReturnsNoErrors(double ratio)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateRenewalJitterRatio(
                renewalJitterRatio: ratio,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate renewal jitter ratio  when out of range or invalid  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(-0.1)]
        [InlineData(1.0)]
        [InlineData(1.1)]
        [InlineData(double.NaN)]
        [InlineData(double.PositiveInfinity)]
        [InlineData(double.NegativeInfinity)]
        public void ValidateRenewalJitterRatio_WhenOutOfRangeOrInvalid_ReturnsError(double? ratio)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateRenewalJitterRatio(
                renewalJitterRatio: ratio,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:RenewalJitterRatio", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults renewal jitter ratio to point one behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsRenewalJitterRatioToPointOne()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(0.1, options.RenewalJitterRatio);
        }
        /// <summary>
        /// Exercises validate renew before expiry days  when within range  returns no errors behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(60)]
        public void ValidateRenewBeforeExpiryDays_WhenWithinRange_ReturnsNoErrors(int days)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateRenewBeforeExpiryDays(
                renewBeforeExpiryDays: days,
                settingPrefix: "BackFiller:LetsEncrypt");

            Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Exercises validate renew before expiry days  when out of range or missing  returns error behavior, including the expected result and failure semantics.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(61)]
        public void ValidateRenewBeforeExpiryDays_WhenOutOfRangeOrMissing_ReturnsError(int? days)
        {
            List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateRenewBeforeExpiryDays(
                renewBeforeExpiryDays: days,
                settingPrefix: "BackFiller:LetsEncrypt");

            LetsEncryptValidationResult error = Assert.Single(diagnostics);
            Assert.Equal("BackFiller:LetsEncrypt:RenewBeforeExpiryDays", error.Setting);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults renew before expiry days to seven behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsRenewBeforeExpiryDaysToSeven()
        {
            LetsEncryptOptions options = new();

            Assert.Equal(7, options.RenewBeforeExpiryDays);
        }
        /// <summary>
        /// Exercises lets encrypt options  defaults use staging directory to false behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void LetsEncryptOptions_DefaultsUseStagingDirectoryToFalse()
        {
            LetsEncryptOptions options = new();

            Assert.False(options.UseStagingDirectory);
        }

        /// <summary>
        /// Verifies the create unique temp directory behavior and expected contract.
        /// </summary>
        private static string CreateUniqueTempDirectory()
        {
            string path = Path.Combine(Path.GetTempPath(), $"vectornntp-letsencrypt-test-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(path);
            return path;
        }

        /// <summary>
        /// Verifies the delete directory if exists behavior and expected contract.
        /// </summary>
        private static void DeleteDirectoryIfExists(string path)
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }
}
