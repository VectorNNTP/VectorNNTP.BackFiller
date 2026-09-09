// <copyright file="TransitServerValidatorIsolationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit server validator isolation, covering configuration and validation contracts; NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit server validator isolation test suite.

using Microsoft.Extensions.Configuration;
using System.Security.Cryptography;
using VectorNNTP.Backfiller.Runtime.Certificates;
using VectorNNTP.Backfiller.Startup.Validation;
using Xunit;
using Xunit.Abstractions;

namespace VectorNNTP.BackFiller.Tests.Startup.Validation
{
    /// <summary>
    /// Confirms the transit server validator isolation tests behavior.
    /// </summary>
    /// <returns>The value returned by the transit server validator isolation tests helper.</returns>
    /// <summary>
    /// Confirms the transit server validator isolation tests behavior.
    /// </summary>
    /// <param name="output">The output used by this test scenario.</param>
    /// <returns>The value returned by the transit server validator isolation tests helper.</returns>
    public class TransitServerValidatorIsolationTests(ITestOutputHelper output)
    {
        private const string DeterministicAcmeAccountPrivateKeyPkcs8DerBase64 = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC8S+Vlahtt53SMWjz4CrpXdfudSM3YZFo0tKnEPt6NWUr+N460QtyNnNQv0OCWMe+UpXrur16r17r0Mvp3ye45V4yhJn0RKWAV19O/+U/6oO/q3PHL6Q4hBimSah7aGYhxRHofEPbDBL3jX3OrkWY4m2gJ9bHPLJBTl9ruNKmzINn8rxgcHPJjfqSC3fZe1LdggXFlw21+ZWc8e8N1q/ZptmeadOdSGeRrpWBtlSr1/T+uRZ4K9FbdRA8N8e/66bCXRVdFvJerLjrYMy4+mqqp/pwzQq+dkf6O8WaHq6hwGALLZycuOULJezt/nGOWc43NdIICAxcDtR4j8XKCn235AgMBAAECggEAa5207dFG+/lc0xp/3gPDnFkCBVKm0xYHuDfJDzAfYgm2orR+CuhrxUPswadPtIe1te8d42y3Xt9dKlQ4cl4mmP9AkJm+wSA0mkdP7lg/La7tb/328+Ou/5DWEag1GdGd+Z55bWf0oGEFZf4Xzea71X58Z7TUeuOtWRlhNuNCWe1gtVeiNBOKh2/OsPQ8ROodhwhjXImlpzmIm8WtMAa6rZhC7vL2cfmIKVRdWdRQYVavkOzxZBq2GqAD3ZXbEce+ku6Hz8y1nyrp9T2G6z1AuzrG4C/niogGx5YjAPSW8rr5rlcD6wEJiDUatVHeRvKwoqc2cGWR7MRFS0vI6uJHQQKBgQDv51v7ACwMm2HVfiQ/wYnvrO5QsD7EeqH269ZMVmi5eyFyVeEa14VmC5vj2c93VV73Og8y1ynLqDBFWlnXdYVXBVBe8WKhgCTSqRaSsdfj4XtnmVq8ojGihGHdXQRvzfIrz3FJ6NZQP2yOnJrEZa6MNbNDC3UDfIfYBKNxK4ASGwKBgQDI7h5aYJOgmCT36Xned2lh/lVFay1hRZnGj1GHYIVTGfnHnhlIyBzmPiknZuzj7HZcvfESZh8BYjL24azsZhjkfMMH3ouSWu45fQfbPm8lwTdoeEmo4JZfNxbYSohwPrsiRCFDSRIaZALYVrowPoqrDnXylNGkTE0xBQ1Y/1fhewKBgA8gIyh8JkrVMSHoxhhO94do+82SjyKMKNIMpIJDoG6xWLaAu6SZmguJB9ch0HbRpx8nRfYKotP4UrLMs4VmH3YRG7Qgu/s6vRebGZU+KUJw4PrzLElgYIjCl/kA+FqkPXSNq7LhP0Hn/cwwC4H+dzbX2+mKO2Jw44+3GybzeyupAoGACFYQvlEpbs1BI2P1YWx029LweLvUmyeHFLzXdhVkEqmOOmDtzZ43zLmhfXgAtggWdQyQVuITwTvwv1tnkDtAJyKh+M6b3cuV/J6aV9dERz237canz7DZrEOd2AVnmbiQjQBknOUIMj4Z/B3FBcFigWxNKm5QME/WGAWMozeczscCgYAkWIdz+Xvo64/gWFYkEvHUfif1op+LT2MP5v/YoWurwoJ2zCQxnBRdCT9ROU2dTKJo+Igfa2Ff6M/TVBEHigcqb8yweDnjswaKaxOq1NcSHqakx0rquV7Yn/IH51vddEAEb/F+Voh+GKaVcWSbJyMihU7TNuUJ5CCJI07waX4maw==";

        /// <summary>
        /// Supplies  out for the fixture or scenario under test.
        /// </summary>
        private readonly ITestOutputHelper _out = output;

        /// <summary>
        /// Confirms the build behavior.
        /// </summary>
        /// <returns>The value returned by the build helper.</returns>
        /// <summary>
        /// Confirms the build behavior.
        /// </summary>
        /// <param name="values">The values used by this test scenario.</param>
        /// <returns>The value returned by the build helper.</returns>
        private static IConfiguration Build(Dictionary<string, string?> values)
        {
            string acmeAccountKeyPem = EnsureRelativeAcmeAccountKeyPemFile();
            Dictionary<string, string?> baseline = new(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:LetsEncrypt:AcmeAccountEmail"] = "security@example.com",
                ["BackFiller:LetsEncrypt:AcmeAccountKeyPem"] = acmeAccountKeyPem,
                ["BackFiller:LetsEncrypt:PfxExportPassword"] = "test-only-pfx-pass-123",
            };

            foreach (KeyValuePair<string, string?> kv in values)
            {
                baseline[kv.Key] = kv.Value;
            }

            return new ConfigurationBuilder().AddInMemoryCollection(baseline).Build();
        }

        private static string EnsureRelativeAcmeAccountKeyPemFile()
        {
            string certDirectory = Path.Combine(AppContext.BaseDirectory, "certs");
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
        /// <summary>
        /// Confirms the transit server use ssl missing default false direct and full pipeline behavior.
        /// </summary>
        [Fact]
        public async Task TransitServer_UseSslMissing_DefaultFalse_DirectAndFullPipeline()
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
            {
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
                ["BackFiller:BindPort"] = "119",
                ["BackFiller:Name"] = "Grabber",
                ["BackFiller:Id"] = "12",
                ["BackFiller:DnsSuffix"] = "example.com",
                ["BackFiller:DirCerts"] = "certs",
                ["BackFiller:DirLogs"] = "logs",
                ["BackFiller:LetsEncrypt:CloudFlareApiToken"] = "test-only-cloudflare-token-1deeff5c65baf93f1db745d8",
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

            // Convert the diagnostic harness into real assertions: verify that the non-listener validation pipeline
            // produces the expected outcome (no TransitServer UseSsl error when UseSsl is missing)
            (ConfigurationValidationResult configResult, DependencyValidationResult _) = await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

            string errorSummary = string.Join("; ", configResult.Errors.Select(e => $"{e.Setting}: {e.Error}"));
            string warnSummary = string.Join("; ", configResult.Warnings.Select(w => $"{w.Setting}: {w.Message}"));

            Assert.True(configResult.IsValid, $"Configuration invalid. Errors=[{errorSummary}] Warnings=[{warnSummary}]");
            // No error should be produced for the missing UseSsl setting; it should default to false and not be an error
            Assert.DoesNotContain(configResult.Errors, static e => e.Setting == "BackFiller:TransitServer:UseSsl");
        }
        /// <summary>
        /// Confirms the transit server use ssl true port119 behavior.
        /// </summary>
        [Fact]
        public async Task TransitServer_UseSslTrue_Port119()
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
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

            (ConfigurationValidationResult configResult, DependencyValidationResult _) = await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

            string errorSummary = string.Join("; ", configResult.Errors.Select(e => $"{e.Setting}: {e.Error}"));
            string warnSummary = string.Join("; ", configResult.Warnings.Select(w => $"{w.Setting}: {w.Message}"));

            Assert.True(configResult.IsValid, $"Configuration invalid. Errors=[{errorSummary}] Warnings=[{warnSummary}]");
            // When UseSsl=true and Port=119 the validator should emit a port warning about non-TLS port
            Assert.Contains(configResult.Warnings, static w =>
                w.Setting == "BackFiller:TransitServer:Port" && w.Message.Contains("conventionally non-TLS", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the transit server use ssl false port563 behavior.
        /// </summary>
        [Fact]
        public async Task TransitServer_UseSslFalse_Port563()
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
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

            (ConfigurationValidationResult configResult, DependencyValidationResult _) = await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

            Assert.True(configResult.IsValid);
            // When UseSsl=false and Port=563 the validator should emit a port warning about TLS convention
            Assert.Contains(configResult.Warnings, static w =>
                w.Setting == "BackFiller:TransitServer:Port" && w.Message.Contains("conventionally TLS", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Confirms the transit server use ssl true port563 behavior.
        /// </summary>
        [Fact]
        public async Task TransitServer_UseSslTrue_Port563()
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
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

            (ConfigurationValidationResult configResult, DependencyValidationResult _) = await StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync(config, TimeSpan.FromSeconds(1), CancellationToken.None);

            Assert.True(configResult.IsValid);
            // When UseSsl=true and Port=563 there should be no port warning
            Assert.DoesNotContain(configResult.Warnings, static w => w.Setting == "BackFiller:TransitServer:Port");
        }
    }

}
