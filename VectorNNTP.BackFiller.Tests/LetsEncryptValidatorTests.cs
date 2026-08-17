using System.Security.Cryptography;
using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests for Let's Encrypt configuration validation.
/// </summary>
/// <remarks>
/// DomainNames tests in this class cover syntax/shape validation only.
/// Primary certificate identity authority is the generated canonical BackFiller FQDN.
/// </remarks>
public class LetsEncryptValidatorTests
{
    [Fact]
    public void ValidateAcmeAccountEmail_WhenValidEmail_ReturnsNoErrors()
    {
        List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateAcmeAccountEmail(
            acmeAccountEmail: "security@usenet.ninja",
            settingPrefix: "BackFiller:LetsEncrypt");

        Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsAcmeAccountEmailToSecurityAtUsenetNinja()
    {
        LetsEncryptOptions options = new();

        Assert.Equal("security@usenet.ninja", options.AcmeAccountEmail);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsAcmeAccountKeyPemToAccountKey()
    {
        LetsEncryptOptions options = new();

        Assert.Equal("account.key", options.AcmeAccountKeyPem);
    }

    [Fact]
    public void LetsEncryptOptions_DefaultsAcmeTransientRetryMaxAttemptsToFive()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(5, options.AcmeTransientRetryMaxAttempts);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsClockSkewCheckTtlMinutesToFive()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(5, options.ClockSkewCheckTtlMinutes);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsClockSkewMaxMinutesToTen()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(10, options.ClockSkewMaxMinutes);
    }

    [Fact]
    public void ValidateCloudFlareApiToken_WhenValid_ReturnsNoErrors()
    {
        List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateCloudFlareApiToken(
            cloudFlareApiToken: "v1.abcdef1234567890abcdef1234567890abcdef12",
            settingPrefix: "BackFiller:LetsEncrypt");

        Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsCloudFlareApiTokenToPlaceholder()
    {
        LetsEncryptOptions options = new();

        Assert.Equal("YOUR_CLOUDFLARE_API_TOKEN", options.CloudFlareApiToken);
    }

    [Fact]
    public void LetsEncryptOptions_DefaultsCloudFlareZoneIdToUsenetNinjaZone()
    {
        LetsEncryptOptions options = new();

        Assert.Equal("5811a29d39a0732afb5f160c9b137c3d", options.CloudFlareZoneId);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsDnsAuthoritativeNsCacheMinutesToFive()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(5, options.DnsAuthoritativeNsCacheMinutes);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsDnsAuthoritativeQuorumRatioToPointSeven()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(0.7, options.DnsAuthoritativeQuorumRatio);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsDnsPropagationDelaySecondsToFifteen()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(15, options.DnsPropagationDelaySeconds);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsDnsTxtPollIntervalSecondsToThree()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(3, options.DnsTxtPollIntervalSeconds);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsDnsTxtPollTimeoutSecondsToSixHundred()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(600, options.DnsTxtPollTimeoutSeconds);
    }

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

    [Fact]
    public void ValidateDomainNamesSyntax_WhenNull_ReturnsNoErrors()
    {
        List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDomainNames(
            domainNames: null,
            settingPrefix: "BackFiller:LetsEncrypt");

        Assert.Empty(diagnostics);
    }

    [Fact]
    public void ValidateDomainNamesSyntax_WhenEmptyArray_ReturnsError()
    {
        List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidateDomainNames(
            domainNames: [],
            settingPrefix: "BackFiller:LetsEncrypt");

        LetsEncryptValidationResult error = Assert.Single(diagnostics);
        Assert.Equal("BackFiller:LetsEncrypt:DomainNames", error.Setting);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsDomainNamesToNull()
    {
        LetsEncryptOptions options = new();

        Assert.Null(options.DomainNames);
    }

    [Fact]
    public void LetsEncryptOptions_DefaultsEnabledToTrue()
    {
        LetsEncryptOptions options = new();

        Assert.True(options.Enabled);
    }

    [Fact]
    public void ValidatePfxExportPassword_WhenValid_ReturnsNoErrors()
    {
        List<LetsEncryptValidationResult> diagnostics = LetsEncryptValidator.ValidatePfxExportPassword(
            pfxExportPassword: "s3curePfxPassword",
            settingPrefix: "BackFiller:LetsEncrypt");

        Assert.DoesNotContain(diagnostics, static d => d.Severity == ValidationSeverity.Error);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsPfxExportPasswordToPlaceholder()
    {
        LetsEncryptOptions options = new();

        Assert.Equal("YOUR_PFX_PASSWORD", options.PfxExportPassword);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsRenewalCheckIntervalHoursToSix()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(6, options.RenewalCheckIntervalHours);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsRenewalJitterRatioToPointOne()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(0.1, options.RenewalJitterRatio);
    }

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

    [Fact]
    public void LetsEncryptOptions_DefaultsRenewBeforeExpiryDaysToSeven()
    {
        LetsEncryptOptions options = new();

        Assert.Equal(7, options.RenewBeforeExpiryDays);
    }

    [Fact]
    public void LetsEncryptOptions_DefaultsUseStagingDirectoryToFalse()
    {
        LetsEncryptOptions options = new();

        Assert.False(options.UseStagingDirectory);
    }

    private static string CreateUniqueTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"vectornntp-letsencrypt-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive: true);
        }
    }
}
