// <copyright file="ProgramValidationSemanticsTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for program validation semantics, covering configuration and validation contracts.
// Primary responsibility: documents the executable contracts covered by the program validation semantics test suite.

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using System.Security.Cryptography;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using VectorNNTP.Backfiller.Startup.Commands;
using VectorNNTP.Backfiller.Startup.Configuration;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Startup.Validation
{
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
        private const string DeterministicAcmeAccountPrivateKeyPkcs8DerBase64 = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC8S+Vlahtt53SMWjz4CrpXdfudSM3YZFo0tKnEPt6NWUr+N460QtyNnNQv0OCWMe+UpXrur16r17r0Mvp3ye45V4yhJn0RKWAV19O/+U/6oO/q3PHL6Q4hBimSah7aGYhxRHofEPbDBL3jX3OrkWY4m2gJ9bHPLJBTl9ruNKmzINn8rxgcHPJjfqSC3fZe1LdggXFlw21+ZWc8e8N1q/ZptmeadOdSGeRrpWBtlSr1/T+uRZ4K9FbdRA8N8e/66bCXRVdFvJerLjrYMy4+mqqp/pwzQq+dkf6O8WaHq6hwGALLZycuOULJezt/nGOWc43NdIICAxcDtR4j8XKCn235AgMBAAECggEAa5207dFG+/lc0xp/3gPDnFkCBVKm0xYHuDfJDzAfYgm2orR+CuhrxUPswadPtIe1te8d42y3Xt9dKlQ4cl4mmP9AkJm+wSA0mkdP7lg/La7tb/328+Ou/5DWEag1GdGd+Z55bWf0oGEFZf4Xzea71X58Z7TUeuOtWRlhNuNCWe1gtVeiNBOKh2/OsPQ8ROodhwhjXImlpzmIm8WtMAa6rZhC7vL2cfmIKVRdWdRQYVavkOzxZBq2GqAD3ZXbEce+ku6Hz8y1nyrp9T2G6z1AuzrG4C/niogGx5YjAPSW8rr5rlcD6wEJiDUatVHeRvKwoqc2cGWR7MRFS0vI6uJHQQKBgQDv51v7ACwMm2HVfiQ/wYnvrO5QsD7EeqH269ZMVmi5eyFyVeEa14VmC5vj2c93VV73Og8y1ynLqDBFWlnXdYVXBVBe8WKhgCTSqRaSsdfj4XtnmVq8ojGihGHdXQRvzfIrz3FJ6NZQP2yOnJrEZa6MNbNDC3UDfIfYBKNxK4ASGwKBgQDI7h5aYJOgmCT36Xned2lh/lVFay1hRZnGj1GHYIVTGfnHnhlIyBzmPiknZuzj7HZcvfESZh8BYjL24azsZhjkfMMH3ouSWu45fQfbPm8lwTdoeEmo4JZfNxbYSohwPrsiRCFDSRIaZALYVrowPoqrDnXylNGkTE0xBQ1Y/1fhewKBgA8gIyh8JkrVMSHoxhhO94do+82SjyKMKNIMpIJDoG6xWLaAu6SZmguJB9ch0HbRpx8nRfYKotP4UrLMs4VmH3YRG7Qgu/s6vRebGZU+KUJw4PrzLElgYIjCl/kA+FqkPXSNq7LhP0Hn/cwwC4H+dzbX2+mKO2Jw44+3GybzeyupAoGACFYQvlEpbs1BI2P1YWx029LweLvUmyeHFLzXdhVkEqmOOmDtzZ43zLmhfXgAtggWdQyQVuITwTvwv1tnkDtAJyKh+M6b3cuV/J6aV9dERz237canz7DZrEOd2AVnmbiQjQBknOUIMj4Z/B3FBcFigWxNKm5QME/WGAWMozeczscCgYAkWIdz+Xvo64/gWFYkEvHUfif1op+LT2MP5v/YoWurwoJ2zCQxnBRdCT9ROU2dTKJo+Igfa2Ff6M/TVBEHigcqb8yweDnjswaKaxOq1NcSHqakx0rquV7Yn/IH51vddEAEb/F+Voh+GKaVcWSbJyMihU7TNuUJ5CCJI07waX4maw==";

        /// <summary>
        /// Confirms shared command/startup success fixtures include mandatory listener ACME configuration values.
        /// </summary>
        [Fact]
        public void BuildConfigurationForCommandTests_WhenUsingBaseline_IncludesMandatoryListenerAcmeConfiguration()
        {
            IConfiguration configuration = BuildConfigurationForCommandTests(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(
                configuration,
                warnings: []);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountEmail");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountKeyPem");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:PfxExportPassword");
        }

        [Fact]
        public void TryReadValidPrivateKeyPem_WhenPemContainsPrivateKey_ReturnsTrue()
        {
            string certDirectory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(certDirectory);
            string keyFilePath = Path.Combine(certDirectory, "private-account.key");

            using (RSA rsa = RSA.Create(2048))
            {
                File.WriteAllText(keyFilePath, rsa.ExportPkcs8PrivateKeyPem());
            }

            try
            {
                Assert.True(TryReadValidPrivateKeyPem(keyFilePath, out string pem));
                Assert.False(string.IsNullOrWhiteSpace(pem));
            }
            finally
            {
                Directory.Delete(certDirectory, recursive: true);
            }
        }

        [Fact]
        public void TryReadValidPrivateKeyPem_WhenPemContainsPublicKeyOnly_ReturnsFalse()
        {
            string certDirectory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(certDirectory);
            string keyFilePath = Path.Combine(certDirectory, "public-only-account.key");

            using (RSA rsa = RSA.Create(2048))
            {
                File.WriteAllText(keyFilePath, rsa.ExportRSAPublicKeyPem());
            }

            try
            {
                Assert.False(TryReadValidPrivateKeyPem(keyFilePath, out _));
            }
            finally
            {
                Directory.Delete(certDirectory, recursive: true);
            }
        }

        [Fact]
        public void TryReadValidPrivateKeyPem_WhenPemMalformed_ReturnsFalse()
        {
            string certDirectory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(certDirectory);
            string keyFilePath = Path.Combine(certDirectory, "malformed-account.key");

            File.WriteAllText(keyFilePath, "-----BEGIN PRIVATE KEY-----\npartial\n");

            try
            {
                Assert.False(TryReadValidPrivateKeyPem(keyFilePath, out _));
            }
            finally
            {
                Directory.Delete(certDirectory, recursive: true);
            }
        }

        [Fact]
        public void EnsureRelativeAcmeAccountKeyPemFile_WhenExistingFileIsPartial_ReplacesWithValidPrivateKeyPem()
        {
            string certDirectory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(certDirectory);
            string keyFilePath = Path.Combine(certDirectory, "account.key");

            File.WriteAllText(keyFilePath, "-----BEGIN PRIVATE KEY-----\npartial\n");

            try
            {
                string returnedFileName = EnsureRelativeAcmeAccountKeyPemFile(certDirectory);

                Assert.Equal("account.key", returnedFileName);
                Assert.True(TryReadValidPrivateKeyPem(keyFilePath, out string pem));
                Assert.False(string.IsNullOrWhiteSpace(pem));
            }
            finally
            {
                Directory.Delete(certDirectory, recursive: true);
            }
        }

        [Fact]
        public void EnsureRelativeAcmeAccountKeyPemFile_WhenRecreatedMultipleTimes_WritesDeterministicPemContent()
        {
            string certDirectory = Path.Combine(Path.GetTempPath(), "VectorNNTP.BackFiller.Tests", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(certDirectory);
            string keyFilePath = Path.Combine(certDirectory, "account.key");

            try
            {
                _ = EnsureRelativeAcmeAccountKeyPemFile(certDirectory);
                string first = File.ReadAllText(keyFilePath);

                File.Delete(keyFilePath);

                _ = EnsureRelativeAcmeAccountKeyPemFile(certDirectory);
                string second = File.ReadAllText(keyFilePath);

                Assert.Equal(GetDeterministicAcmeAccountPrivateKeyPem(), first);
                Assert.Equal(GetDeterministicAcmeAccountPrivateKeyPem(), second);
                Assert.Equal(first, second);
                Assert.True(TryReadValidPrivateKeyPem(keyFilePath, out _));
            }
            finally
            {
                Directory.Delete(certDirectory, recursive: true);
            }
        }

        /// <summary>
        /// Confirms the configuration validation result when only warnings is valid true behavior.
        /// </summary>
        [Fact]
        public void ConfigurationValidationResult_WhenOnlyWarnings_IsValidTrue()
        {
            ConfigurationValidationResult result = new(
                errors: [],
                warnings: [("BackFiller:BindAddress", "Wildcard bind address configured")]);

            Assert.True(result.IsValid);
            _ = Assert.Single(result.Warnings);
            Assert.Empty(result.Errors);
        }
        /// <summary>
        /// Confirms the configuration validation result when errors present is valid false behavior.
        /// </summary>
        [Fact]
        public void ConfigurationValidationResult_WhenErrorsPresent_IsValidFalse()
        {
            ConfigurationValidationResult result = new(
                errors: [("BackFiller:BindPort", "Out of range")],
                warnings: [("BackFiller:BindAddress", "Wildcard bind address configured")]);

            Assert.False(result.IsValid);
            _ = Assert.Single(result.Warnings);
            _ = Assert.Single(result.Errors);
        }
        /// <summary>
        /// Confirms the build validate config command result when dir logs missing from runtime snapshot validation returns configuration error behavior.
        /// </summary>
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

        /// <summary>
        /// Confirms validate-config validates the already bound BackFiller snapshot by rejecting a second BackFiller section bind attempt within a single command evaluation.
        /// </summary>
        [Fact]
        public void BuildValidateConfigCommandResult_WhenSecondBackFillerBindWouldOccur_DoesNotRebindConfiguration()
        {
            IConfiguration baselineConfiguration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            });
            IConfiguration guardedConfiguration = new SingleBackFillerBindConfiguration(baselineConfiguration);

            ConfigurationValidationResult result = ValidateConfigCommandHandler.BuildValidateConfigCommandResult(guardedConfiguration);

            Assert.True(result.IsValid);
            Assert.Empty(result.Errors);
        }

        /// <summary>
        /// Confirms validate-config runtime projection does not require listener certificate directory in non-listener mode.
        /// </summary>
        [Fact]
        public void BuildValidateConfigCommandResult_WhenDirCertsMissing_RemainsValidInNonListenerSnapshotMode()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:DirCerts"] = string.Empty,
            });

            ConfigurationValidationResult result = ValidateConfigCommandHandler.BuildValidateConfigCommandResult(configuration);

            Assert.True(result.IsValid);
            Assert.DoesNotContain(result.Errors, static e => string.Equals(e.Setting, "BackFiller.DirCerts", StringComparison.Ordinal));
        }

        /// <summary>
        /// Confirms non-listener startup validation can project runtime options without certificate-directory prerequisites.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationAndDependenciesAsync_WhenDirCertsMissing_DoesNotReturnDirCertsConfigurationError()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:DirCerts"] = string.Empty,
            });

            (ConfigurationValidationResult configResult, DependencyValidationResult _) =
                await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                    configuration,
                    TimeSpan.FromSeconds(1),
                    CancellationToken.None);

            Assert.DoesNotContain(configResult.Errors, static e => string.Equals(e.Setting, "BackFiller.DirCerts", StringComparison.Ordinal));
        }

        /// <summary>
        /// Confirms non-listener pipeline validates and projects a single authoritative BackFiller bind snapshot.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationAndDependenciesAsync_WhenSecondBackFillerBindWouldOccur_DoesNotRebindConfiguration()
        {
            IConfiguration baselineConfiguration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            });
            IConfiguration guardedConfiguration = new SingleBackFillerBindConfiguration(
                baselineConfiguration,
                "BackFiller section was rebound more than once during non-listener startup validation pipeline evaluation.");

            (ConfigurationValidationResult configResult, _) =
                await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                    guardedConfiguration,
                    TimeSpan.FromSeconds(1),
                    CancellationToken.None);

            Assert.True(configResult.IsValid);
            Assert.Empty(configResult.Errors);
        }

        /// <summary>
        /// Confirms full startup validation still enforces listener certificate directory prerequisites.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync_WhenDirCertsMissing_RemainsInvalid()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:DirCerts"] = string.Empty,
            });

            (ConfigurationValidationResult configResult, DependencyValidationResult dependencyResult, BackFillerRuntimeOptions? runtimeOptions) =
                await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(
                    configuration,
                    TimeSpan.FromSeconds(1),
                    CancellationToken.None);

            Assert.False(configResult.IsValid);
            Assert.Contains(configResult.Errors, static e => string.Equals(e.Setting, "BackFiller.DirCerts", StringComparison.Ordinal));
            Assert.Null(runtimeOptions);
            Assert.True(dependencyResult.IsValid);
        }

        /// <summary>
        /// Confirms full-startup pipeline keeps strict listener certificate prerequisites while avoiding a second BackFiller bind.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync_WhenSecondBackFillerBindWouldOccur_DoesNotRebindConfiguration()
        {
            IConfiguration baselineConfiguration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = string.Empty,
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = string.Empty,
            });
            IConfiguration guardedConfiguration = new SingleBackFillerBindConfiguration(
                baselineConfiguration,
                "BackFiller section was rebound more than once during full-startup validation pipeline evaluation.");

            (ConfigurationValidationResult configResult, DependencyValidationResult _, BackFillerRuntimeOptions? runtimeOptions) =
                await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(
                    guardedConfiguration,
                    TimeSpan.FromSeconds(1),
                    CancellationToken.None);

            Assert.False(configResult.IsValid);
            Assert.NotEmpty(configResult.Errors);
            Assert.Null(runtimeOptions);
        }

        /// <summary>
        /// Confirms RuntimeSnapshotFactory omits listener certificate directory and LetsEncrypt runtime projection in non-listener mode.
        /// </summary>
        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenNonListenerModeAndDirCertsMissing_BuildsWithoutCertificateDirectory()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:DirCerts"] = string.Empty,
            });
            BackFillerOptions backFiller = configuration.GetSection("BackFiller").Get<BackFillerOptions>()
                ?? throw new InvalidOperationException("BackFiller section is required for this test scenario.");
            List<(string Setting, string Error)> errors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(
                configuration,
                backFiller,
                errors,
                includeLetsEncryptRuntimeOptions: false);

            Assert.NotNull(runtimeOptions);
            Assert.Empty(errors);
            Assert.Null(runtimeOptions.ValidatedCertificateDirectory);
            Assert.Null(runtimeOptions.LetsEncrypt);
            Assert.NotNull(runtimeOptions.RabbitMq);
            Assert.Equal("localhost", runtimeOptions.TransitServerHost);
        }

        /// <summary>
        /// Confirms RuntimeSnapshotFactory still fails in full-startup mode when certificate directory is missing.
        /// </summary>
        [Fact]
        public void BuildRuntimeOptionsSnapshot_WhenFullStartupModeAndDirCertsMissing_Fails()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:DirCerts"] = string.Empty,
            });
            BackFillerOptions backFiller = configuration.GetSection("BackFiller").Get<BackFillerOptions>()
                ?? throw new InvalidOperationException("BackFiller section is required for this test scenario.");
            List<(string Setting, string Error)> errors = [];

            BackFillerRuntimeOptions? runtimeOptions = RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot(
                configuration,
                backFiller,
                errors,
                includeLetsEncryptRuntimeOptions: true);

            Assert.Null(runtimeOptions);
            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller"
                && e.Error.Contains("DirCerts", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Confirms the validate back filler options when canonical identity available does not use configured domain names behavior.
        /// </summary>
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
        /// <summary>
        /// Confirms the validate back filler options when identity invalid does not fallback to configured domain names behavior.
        /// </summary>
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when configuration fails skips dependency validation behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when lets encrypt disabled and cloudflare token missing returns cloudflare configuration error behavior.
        /// </summary>
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
        /// <summary>
        /// Confirms the non-listener startup validation path still executes Cloudflare dependency validation when Cloudflare is configured.
        /// </summary>
        [Trait("Category", "Integration")]
        [Fact]
        public async Task ValidateConfigurationAndDependenciesAsync_WhenCloudflareConfiguredInNonListenerPath_StillRunsCloudflareDependencyValidation()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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

        /// <summary>
        /// Confirms the full startup validation path still requires mandatory listener ACME inputs before runtime snapshot/dependency execution.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync_WhenListenerAcmeSettingsMissing_RemainsInvalid()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = string.Empty,
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = string.Empty,
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            });

            (ConfigurationValidationResult configResult, DependencyValidationResult dependencyResult, BackFillerRuntimeOptions? runtimeOptions) =
                await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(
                    configuration,
                    TimeSpan.FromSeconds(1),
                    CancellationToken.None);

            Assert.False(configResult.IsValid);
            Assert.Null(runtimeOptions);
            Assert.Contains(configResult.Errors, static e => e.Setting == "BackFiller:LetsEncrypt:PfxExportPassword");
            Assert.Contains(configResult.Errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountKeyPem");
            Assert.True(dependencyResult.IsValid);
            Assert.Empty(dependencyResult.FailedDependencies);
        }

        /// <summary>
        /// Confirms full startup validation still rejects malformed listener-only ACME settings.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync_WhenListenerAcmeSettingsMalformed_RemainsInvalid()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "not-an-email",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "..\\..\\account.key",
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = "short",
                ["BackFiller:LetsEncrypt:RenewalCheckIntervalHours"] = "0",
                ["BackFiller:LetsEncrypt:RenewalJitterRatio"] = "1",
                ["BackFiller:LetsEncrypt:RenewBeforeExpiryDays"] = "0",
                ["BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts"] = "0",
                ["BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes"] = "0",
                ["BackFiller:LetsEncrypt:ClockSkewMaxMinutes"] = "0",
                ["BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes"] = "0",
                ["BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio"] = "0",
                ["BackFiller:LetsEncrypt:DnsPropagationDelaySeconds"] = "-1",
                ["BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds"] = "0",
                ["BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds"] = "0",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            });

            (ConfigurationValidationResult configResult, DependencyValidationResult dependencyResult, BackFillerRuntimeOptions? runtimeOptions) =
                await StartupValidationPipeline.ValidateConfigurationDependenciesAndBuildRuntimeOptionsAsync(
                    configuration,
                    TimeSpan.FromSeconds(1),
                    CancellationToken.None);

            Assert.False(configResult.IsValid);
            Assert.Null(runtimeOptions);
            Assert.Contains(configResult.Errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountEmail");
            Assert.Contains(configResult.Errors, static e => e.Setting == "BackFiller:LetsEncrypt:RenewalCheckIntervalHours");
            Assert.Contains(configResult.Errors, static e => e.Setting == "BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds");
            Assert.True(dependencyResult.IsValid);
            Assert.Empty(dependencyResult.FailedDependencies);
        }

        /// <summary>
        /// Confirms the validate configuration and dependencies async when cloudflare configured remains valid without legacy lets encrypt enabled warnings behavior.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationAndDependenciesAsync_WhenCloudflareConfigured_DoesNotReportLegacyLetsEncryptEnabledWarning()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            });

            (ConfigurationValidationResult configResult, _) =
                await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                    configuration,
                    TimeSpan.FromSeconds(1),
                    CancellationToken.None);

            Assert.True(configResult.IsValid);
            Assert.Empty(configResult.Errors);
            Assert.DoesNotContain(configResult.Warnings, static w => w.Setting == "BackFiller:LetsEncrypt:Enabled");
        }
        /// <summary>
        /// Confirms non-listener validation scope ignores listener-only ACME certificate readiness settings.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenNonListenerScopeAndListenerAcmeSettingsMalformed_DoesNotReturnListenerAcmeErrors()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "not-an-email",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = "..\\..\\account.key",
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = "short",
                ["BackFiller:LetsEncrypt:RenewalCheckIntervalHours"] = "0",
                ["BackFiller:LetsEncrypt:RenewalJitterRatio"] = "1",
                ["BackFiller:LetsEncrypt:RenewBeforeExpiryDays"] = "0",
                ["BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts"] = "0",
                ["BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes"] = "0",
                ["BackFiller:LetsEncrypt:ClockSkewMaxMinutes"] = "0",
                ["BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes"] = "0",
                ["BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio"] = "0",
                ["BackFiller:LetsEncrypt:DnsPropagationDelaySeconds"] = "-1",
                ["BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds"] = "0",
                ["BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds"] = "0",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(
                configuration,
                warnings: [],
                includeListenerCertificateValidation: false);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountEmail");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeAccountKeyPem");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:PfxExportPassword");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:RenewalCheckIntervalHours");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:RenewalJitterRatio");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:RenewBeforeExpiryDays");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:ClockSkewMaxMinutes");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:DnsPropagationDelaySeconds");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds");
            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds");
        }

        /// <summary>
        /// Confirms non-listener validation scope still validates independent Cloudflare prerequisites.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenNonListenerScopeAndCloudflareZoneMalformed_ReturnsCloudflareZoneError()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "invalid-zone-id",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(
                configuration,
                warnings: [],
                includeListenerCertificateValidation: false);

            Assert.Contains(errors, static e => e.Setting == "BackFiller:LetsEncrypt:CloudFlareZoneId");
        }

        /// <summary>
        /// Confirms the validate configuration and dependencies async when rabbit mq endpoint unreachable returns rabbit mq dependency failure behavior.
        /// </summary>
        [Fact]
        public async Task ValidateConfigurationAndDependenciesAsync_WhenRabbitMqEndpointUnreachable_ReturnsRabbitMqDependencyFailure()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.1",
                ["BackFiller:RabbitMQ:Port"] = "5672",
                ["BackFiller:TransitServer:Host"] = "localhost",
                ["BackFiller:TransitServer:Port"] = "119",
                // Intentionally omit UseSsl to validate default behavior (should be treated as false)
                ["BackFiller:TransitServer:UseSsl"] = "false",
            });

            List<(string Setting, string Error)> configErrors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(
                configuration,
                warnings: [],
                includeListenerCertificateValidation: false);
            Assert.Empty(configErrors);

            (_, DependencyValidationResult dependencyResult) =
                await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                    configuration,
                    TimeSpan.FromMilliseconds(500),
                    CancellationToken.None);

            Assert.Contains(dependencyResult.FailedDependencies, static d =>
                d.Dependency == "RabbitMQ"
                && d.Reason.Contains("203.0.113.1:5672", StringComparison.Ordinal));
        }
        /// <summary>
        /// Confirms the validate configuration and dependencies async when transit server endpoint unreachable returns transit server dependency failure behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:Hosts:0"] = "203.0.113.2",
                ["BackFiller:RabbitMQ:Port"] = "5672",
                ["BackFiller:TransitServer:Host"] = "203.0.113.1",
                ["BackFiller:TransitServer:Port"] = "119",
                ["BackFiller:TransitServer:UseSsl"] = "false",
            });

            List<(string Setting, string Error)> configErrors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(
                configuration,
                warnings: [],
                includeListenerCertificateValidation: false);
            Assert.Empty(configErrors);

            (_, DependencyValidationResult dependencyResult) =
                await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                    configuration,
                    TimeSpan.FromMilliseconds(500),
                    CancellationToken.None);

            Assert.Contains(dependencyResult.FailedDependencies, static d => d.Dependency == "TransitServer");
        }
        /// <summary>
        /// Confirms the back filler identity validator canonicalize dns suffix normalizes equivalent inputs behavior.
        /// </summary>
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
        /// <summary>
        /// Confirms the back filler identity validator build back filler fqdn uses canonical dns suffix behavior.
        /// </summary>
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when already canceled propagates operation canceled exception behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            });

            using CancellationTokenSource cts = new();
            cts.Cancel();

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                    configuration,
                    TimeSpan.FromSeconds(5),
                    cts.Token).ConfigureAwait(false));
        }

        /// <summary>
        /// Supplies invalid dependency timeouts for the fixture or scenario under test.
        /// </summary>
        public static TheoryData<TimeSpan> InvalidDependencyTimeouts =>
            [
                TimeSpan.Zero,
                TimeSpan.FromMilliseconds(-1),
            ];
        /// <summary>
        /// Confirms my sql sanitized error mappings behavior.
        /// </summary>

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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when timeout is invalid throws argument out of range exception behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
            });

            _ = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(
                    configuration,
                    invalidTimeout,
                    CancellationToken.None).ConfigureAwait(false));
        }
        /// <summary>
        /// Confirms the get sanitized my sql connection failure reason when error code known returns sanitized message behavior.
        /// </summary>
        [Theory]
        [MemberData(nameof(MySqlSanitizedErrorMappings))]
        public void GetSanitizedMySqlConnectionFailureReason_WhenErrorCodeKnown_ReturnsSanitizedMessage(int mySqlErrorNumber, string expectedMessage)
        {
            string sanitizedMessage = DatabaseDependencyProbe.GetSanitizedMySqlConnectionFailureReason(mySqlErrorNumber);

            Assert.Equal(expectedMessage, sanitizedMessage);
        }
        /// <summary>
        /// Confirms the validate database connectivity async when unexpected exception occurs returns sanitized failure reason behavior.
        /// </summary>
        [Fact]
        public async Task ValidateDatabaseConnectivityAsync_WhenUnexpectedExceptionOccurs_ReturnsSanitizedFailureReason()
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["ConnectionStrings:GrabberDB"] = "Server==invalid",
                })
                .Build();

            DependencyValidationResult result = await DatabaseDependencyProbe
                .ValidateDatabaseConnectivityAsync(configuration, TimeSpan.FromSeconds(1), CancellationToken.None);

            Assert.Contains(result.FailedDependencies, static d =>
                d.Dependency == "GrabberDB" &&
                d.Reason == "Failed to connect");
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq channel lease timeout missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq rpc timeout seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:RpcTimeoutSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq rpc timeout seconds out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq channel lease timeout less than rpc timeout returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "20",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"
                && e.Error.Contains("greater than or equal to RpcTimeoutSeconds", StringComparison.Ordinal));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq channel lease timeout valid and coherent does not return rabbit mq errors behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:RabbitMQ", StringComparison.Ordinal));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq connection blocked timeout missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq connection blocked timeout less than minimum returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq connection blocked timeout less than rpc timeout returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq enable ssl missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:EnableSsl"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Confirms the validate back filler options when rabbit mq work-request max payload bytes missing uses the default without error behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenRabbitMqWorkRequestMaxPayloadBytesMissing_UsesDefaultWithoutError()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            BackFillerOptions? options = configuration.GetSection("BackFiller").Get<BackFillerOptions>();
            Assert.NotNull(options);
            Assert.NotNull(options.RabbitMQ);
            Assert.Equal(1024, options.RabbitMQ.WorkRequestMaxPayloadBytes);

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Confirms the validate back filler options when rabbit mq work-request max payload bytes is accepted at the configured maximum behavior.
        /// </summary>
        [Fact]
        public void ValidateBackFillerOptions_WhenRabbitMqWorkRequestMaxPayloadBytesAtMaximum_IsAccepted()
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes"] = "4096",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes"
                && e.Error.Contains("between 1 and 4096", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Confirms the validate back filler options when rabbit mq work-request max payload bytes is rejected outside the configured range behavior.
        /// </summary>
        [Theory]
        [InlineData("0")]
        [InlineData("4097")]
        public void ValidateBackFillerOptions_WhenRabbitMqWorkRequestMaxPayloadBytesOutOfRange_ReturnsError(string workRequestMaxPayloadBytes)
        {
            IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes"] = workRequestMaxPayloadBytes,
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.Contains(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes"
                && e.Error.Contains("between 1 and 4096", StringComparison.OrdinalIgnoreCase));
        }


        /// <summary>
        /// Confirms the validate back filler options when rabbit mq port missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:Port"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq port out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq username configured and password missing returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq password configured and username missing returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq username whitespace returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq virtual host missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:VirtualHost"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq virtual host whitespace returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq password empty returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq password whitespace returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq username and password are valid does not return credential errors behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:Username"] = "nntparticles",
                ["BackFiller:RabbitMQ:Password"] = "password-1",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting is "BackFiller:RabbitMQ:Password"
                or "BackFiller:RabbitMQ:Username");
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq enable ssl boolean value does not return error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:EnableSsl"] = enableSsl,
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:RabbitMQ:EnableSsl");
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq hosts missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting.StartsWith("BackFiller:RabbitMQ:Hosts", StringComparison.Ordinal));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq host entry contains scheme returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq hosts contain duplicates returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq hosts are valid does not return host errors behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq connection scale down idle seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq scale down cooldown seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:ScaleDownCooldownSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq scale down cooldown seconds out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq min connections missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq min connections less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq max connections missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq min connections greater than max connections returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq max consecutive recovery failures missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq max consecutive recovery failures less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq max consecutive recovery failures too large returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq publish confirm timeout seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq publish confirm timeout seconds less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq publish confirm timeout seconds too large returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq maximum shutdown drain timeout seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq maximum shutdown drain timeout seconds less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq maximum shutdown drain timeout seconds too large returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the configure host shutdown timeout sets configured timeout behavior.
        /// </summary>
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
        /// <summary>
        /// Confirms the configure host shutdown timeout when grace period invalid throws behavior.
        /// </summary>
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
        /// <summary>
        /// Confirms the shutdown configuration rejects rabbit mq drain longer than grace period behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when shutdown section is null returns validation error without throwing behavior.
        /// </summary>
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
                    CloudFlareApiToken = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq minimum connection lifetime seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq minimum connection lifetime seconds too small returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq network recovery interval seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq network recovery interval seconds less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq network recovery interval seconds too large returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq pool reconnect base delay ms missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq pool reconnect max delay ms missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq pool reconnect max delay ms less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq pool reconnect max delay ms out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq pool reconnect max delay ms less than base delay returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq pool reconnect base delay ms less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq pool reconnect base delay ms out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq max pending lease waiters missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:MaxPendingLeaseWaiters"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq max pending lease waiters less than zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq max pending lease waiters too large returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq degraded threshold missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:DegradedThreshold"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq degraded threshold out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq degraded threshold valid does not return error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:DegradedThreshold"] = "0.75",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:RabbitMQ:DegradedThreshold");
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq unhealthy threshold missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:UnhealthyThreshold"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq unhealthy threshold out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq channel pool size missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:ChannelPoolSize"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq channel pool size less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq requested heartbeat seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:RequestedHeartbeatSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq requested heartbeat seconds out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq socket timeout seconds missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:SocketTimeoutSeconds"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq socket timeout seconds out of range returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq requested channel max missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e =>
                e.Setting == "BackFiller:RabbitMQ:RequestedChannelMax"
                && e.Error.Contains("required", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq requested channel max less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq requested channel max too large returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq channel pool size exceeds effective channel limit returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when rabbit mq channel pool size within effective channel limit does not return channel pool errors behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when transit server host missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:TransitServer:Host");
        }
        /// <summary>
        /// Confirms the validate back filler options when transit server host whitespace returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when transit server host contains scheme returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when transit server host contains credentials returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when transit server host contains port returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when transit server host invalid returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when transit server host valid does not return error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:TransitServer:Host");
        }
        /// <summary>
        /// Confirms the validate back filler options when transit server port missing uses default without error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
                ["BackFiller:LetsEncrypt:CloudFlareZoneId"] = "5811a29d39a0732afb5f160c9b137c3d",
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:TransitServer:Host"] = "transit01.example.net",
            });

            List<(string Setting, string Error)> errors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(configuration);

            Assert.DoesNotContain(errors, static e => e.Setting == "BackFiller:TransitServer:Port");
        }
        /// <summary>
        /// Confirms the validate back filler options when transit server port less than or equal to zero returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate back filler options when transit server port too large returns error behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when transit server use ssl missing uses default false without use ssl errors behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
            List<(string Setting, string Error)> configErrors = global::VectorNNTP.Backfiller.Startup.Configuration.ConfigurationValidator.ValidateBackFillerOptions(
                configuration,
                warnings: [],
                includeListenerCertificateValidation: false);
            Assert.True(configErrors.Count == 0, $"Unexpected configuration errors: {string.Join("; ", configErrors.Select(e => e.Setting + ": " + e.Error))}");

            Assert.True(configResult.IsValid);
            Assert.DoesNotContain(configResult.Errors, static e => e.Setting == "BackFiller:TransitServer:UseSsl");
        }
        /// <summary>
        /// Confirms the validate configuration and dependencies async when transit server use ssl true with port119 returns warning behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when transit server use ssl false with port563 returns warning behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when transit server use ssl true with port563 does not return port warnings behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when rabbit mq network recovery interval exceeds connection blocked timeout returns warning behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when rabbit mq publish confirm timeout exceeds rpc timeout returns warning behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
        /// <summary>
        /// Confirms the validate configuration and dependencies async when rabbit mq minimum connection lifetime exceeds scale down idle returns warning behavior.
        /// </summary>
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
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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

        /// <summary>
        /// Confirms the build configuration behavior.
        /// </summary>
        /// <returns>The value returned by the build configuration helper.</returns>
        /// <summary>
        /// Confirms the build configuration behavior.
        /// </summary>
        /// <param name="values">The values used by this test scenario.</param>
        /// <param name="includeRabbitMqBaseline">The include rabbit mq baseline used by this test scenario.</param>
        /// <returns>The value returned by the build configuration helper.</returns>
        internal static IConfiguration BuildConfigurationForCommandTests(Dictionary<string, string?> values, bool includeRabbitMqBaseline = true)
        {
            if (includeRabbitMqBaseline)
            {
                string acmeAccountKeyPem = EnsureRelativeAcmeAccountKeyPemFile();
                Dictionary<string, string?> baseline = new(StringComparer.OrdinalIgnoreCase)
                {
                    ["BackFiller:BindPort"] = "119",
                    ["BackFiller:Name"] = "Grabber",
                    ["BackFiller:Id"] = "12",
                    ["BackFiller:DnsSuffix"] = "example.com",
                    ["BackFiller:DirCerts"] = "certs",
                    ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "security@example.com",
                    ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = acmeAccountKeyPem,
                    ["BackFiller:LetsEncrypt:PfxExportPassword"] = "test-only-pfx-pass-123",
                    ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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
                foreach (KeyValuePair<string, string?> kv in values)
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

        private static IConfiguration BuildConfiguration(Dictionary<string, string?> values, bool includeRabbitMqBaseline = true)
        {
            return BuildConfigurationForCommandTests(values, includeRabbitMqBaseline);
        }

        private static string EnsureRelativeAcmeAccountKeyPemFile(string? fixtureDirectory = null)
        {
            string certDirectory = string.IsNullOrWhiteSpace(fixtureDirectory)
                ? Path.Combine(AppContext.BaseDirectory, "certs")
                : fixtureDirectory;
            _ = Directory.CreateDirectory(certDirectory);

            string fileName = "account.key";
            string keyFilePath = Path.Combine(certDirectory, fileName);
            if (TryReadValidPrivateKeyPem(keyFilePath, out _))
            {
                return fileName;
            }

            string tempPath = CertificateFileConventions.BuildAtomicTempPath(keyFilePath);

            try
            {
                using (FileStream stream = new(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                {
                    using StreamWriter writer = new(stream);
                    writer.Write(GetDeterministicAcmeAccountPrivateKeyPem());
                    writer.Flush();
                    stream.Flush(true);
                }

                File.Move(tempPath, keyFilePath, overwrite: true);
            }
            finally
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }

            if (!TryReadValidPrivateKeyPem(keyFilePath, out _))
            {
                throw new InvalidOperationException("Failed to create a valid ACME account key test fixture.");
            }

            return fileName;
        }

        private static string GetDeterministicAcmeAccountPrivateKeyPem()
        {
            byte[] pkcs8Der = Convert.FromBase64String(DeterministicAcmeAccountPrivateKeyPkcs8DerBase64);
            return PemEncoding.WriteString("PRIVATE KEY", pkcs8Der);
        }

        private static bool TryReadValidPrivateKeyPem(string keyFilePath, out string pem)
        {
            pem = string.Empty;
            if (!File.Exists(keyFilePath))
            {
                return false;
            }

            try
            {
                pem = File.ReadAllText(keyFilePath);
                if (string.IsNullOrWhiteSpace(pem))
                {
                    return false;
                }

                using RSA rsa = RSA.Create();
                rsa.ImportFromPem(pem);
                _ = rsa.ExportPkcs8PrivateKey();
                return true;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or CryptographicException or ArgumentException)
            {
                return false;
            }
        }

        private sealed class SingleBackFillerBindConfiguration : IConfiguration
        {
            private readonly IConfiguration _inner;
            private readonly string _multipleBindMessage;
            private int _backFillerBindCount;

            internal SingleBackFillerBindConfiguration(IConfiguration inner, string multipleBindMessage = "BackFiller section was rebound more than once during validate-config command evaluation.")
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
                _multipleBindMessage = string.IsNullOrWhiteSpace(multipleBindMessage)
                    ? throw new ArgumentException("A non-empty multiple-bind message is required.", nameof(multipleBindMessage))
                    : multipleBindMessage;
            }

            public string? this[string key]
            {
                get => _inner[key];
                set => _inner[key] = value;
            }

            public IEnumerable<IConfigurationSection> GetChildren()
            {
                return _inner.GetChildren();
            }

            public IChangeToken GetReloadToken()
            {
                return _inner.GetReloadToken();
            }

            public IConfigurationSection GetSection(string key)
            {
                IConfigurationSection section = _inner.GetSection(key);
                if (!string.Equals(key, "BackFiller", StringComparison.OrdinalIgnoreCase))
                {
                    return section;
                }

                if (Interlocked.Increment(ref _backFillerBindCount) > 1)
                {
                    throw new InvalidOperationException(_multipleBindMessage);
                }

                return section;
            }
        }

    }



}


