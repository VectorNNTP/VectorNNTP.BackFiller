// <copyright file="ConfigurationValidator.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using System.ComponentModel.DataAnnotations;
using Serilog;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Owns structural configuration validation only and performs no network access.
    /// </summary>
    internal class ConfigurationValidator
    {
        /// <summary>
        /// Validates an options object using the DataAnnotations validation pipeline.
        /// </summary>
        internal static List<(string, string)> ValidateAnnotatedObject<TOptions>(TOptions options, string prefix)
            where TOptions : class
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(prefix);

            List<(string, string)> errors = [];

            ValidationContext context = new(options);
            List<ValidationResult> validationResults = [];
            _ = Validator.TryValidateObject(options, context, validationResults, validateAllProperties: true);

            foreach (ValidationResult result in validationResults)
            {
                string message = result.ErrorMessage ?? "Unknown error";
                if (result.MemberNames != null && result.MemberNames.Any())
                {
                    foreach (string memberName in result.MemberNames)
                    {
                        errors.Add(($"{prefix}.{memberName}", message));
                    }
                }
                else
                {
                    errors.Add((prefix, message));
                }
            }

            return errors;
        }

        /// <summary>
        /// Validates ConnectionStrings configuration section.
        /// </summary>
        internal static List<(string Setting, string Error)> ValidateConnectionStrings(IConfiguration configuration)
        {
            List<(string Setting, string Message)> warnings = [];
            return ValidateConnectionStrings(configuration, warnings);
        }

        /// <summary>
        /// Validates ConnectionStrings configuration section.
        /// </summary>
        internal static List<(string Setting, string Error)> ValidateConnectionStrings(
            IConfiguration configuration,
            List<(string Setting, string Message)> warnings)
        {
            List<(string Setting, string Error)> errors = [];

            // Bind and validate ConnectionStrings:GrabberDB
            ConnectionStringsOptions? connectionStrings = configuration
                .GetSection("ConnectionStrings")
                .Get<ConnectionStringsOptions>();

            // Use DataAnnotations validation for required field
            if (connectionStrings == null)
            {
                errors.Add(("ConnectionStrings", "ConnectionStrings section is missing from configuration"));
                return errors;
            }

            errors.AddRange(ValidateAnnotatedObject(connectionStrings, "ConnectionStrings"));

            // Detailed connection string validation using custom validator
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionStrings.GrabberDB,
                "ConnectionStrings:GrabberDB");

            AddDiagnostics(errors, warnings, diagnostics);

            return errors;
        }

        /// <summary>
        /// Validates BackFiller configuration section.
        /// </summary>
        internal static List<(string Setting, string Error)> ValidateBackFillerOptions(IConfiguration configuration)
        {
            List<(string Setting, string Message)> warnings = [];
            return ValidateBackFillerOptions(configuration, warnings);
        }

        /// <summary>
        /// Validates BackFiller configuration section.
        /// </summary>
        internal static List<(string Setting, string Error)> ValidateBackFillerOptions(
            IConfiguration configuration,
            List<(string Setting, string Message)> warnings)
        {
            BackFillerOptions? backFiller = configuration
                .GetSection("BackFiller")
                .Get<BackFillerOptions>();

            return ValidateBackFillerOptions(backFiller, warnings);
        }

        /// <summary>
        /// Validates the bound BackFiller options instance and emits configuration diagnostics.
        /// </summary>
        /// <param name="backFiller">Bound BackFiller options instance.</param>
        /// <param name="warnings">Warning collection to append non-blocking diagnostics to.</param>
        /// <returns>Collection of blocking configuration errors.</returns>
        internal static List<(string Setting, string Error)> ValidateBackFillerOptions(
            BackFillerOptions? backFiller,
            List<(string Setting, string Message)> warnings)
        {
            List<(string Setting, string Error)> errors = [];

            // Use DataAnnotations validation for required fields and ranges
            if (backFiller == null)
            {
                errors.Add(("BackFiller", "BackFiller section is missing from configuration"));
                return errors;
            }

            errors.AddRange(ValidateAnnotatedObject(backFiller, "BackFiller"));

            // Detailed bind address validation using custom validator
            List<BindAddressValidationResult> diagnostics = BindAddressValidator.Validate(
                backFiller.BindAddress,
                backFiller.BindPort,
                "BackFiller");

            AddDiagnostics(errors, warnings, diagnostics);

            // Validate Name/Id/DnsSuffix and generated FQDN suitability for ACME/TLS.
            List<BackFillerIdentityValidationResult> identityDiagnostics = BackFillerIdentityValidator.Validate(
                backFiller.Name,
                backFiller.Id,
                backFiller.DnsSuffix,
                "BackFiller");

            AddDiagnostics(errors, warnings, identityDiagnostics);

            // Validate RabbitMQ lease-timeout configuration constraints.
            List<RabbitMqValidationResult> rabbitMqDiagnostics = RabbitMqValidator.Validate(
                backFiller.RabbitMQ,
                "BackFiller");

            AddDiagnostics(errors, warnings, rabbitMqDiagnostics);

            // Validate TransitServer host configuration constraints.
            List<TransitServerValidationResult> transitServerDiagnostics = TransitServerValidator.Validate(
                backFiller.TransitServer,
                "BackFiller");

            AddDiagnostics(errors, warnings, transitServerDiagnostics);

            // Validate graceful shutdown policy constraints.
            if (backFiller.Shutdown != null)
            {
                errors.AddRange(ValidateAnnotatedObject(backFiller.Shutdown, "BackFiller:Shutdown"));
            }

            int configuredGracePeriodSeconds = backFiller.Shutdown?.GracePeriodSeconds ?? 0;
            int configuredRabbitMqDrainTimeoutSeconds = backFiller.RabbitMQ?.MaximumShutdownDrainTimeoutSeconds ?? 0;
            if (configuredGracePeriodSeconds > 0 &&
                configuredRabbitMqDrainTimeoutSeconds > 0 &&
                configuredRabbitMqDrainTimeoutSeconds > configuredGracePeriodSeconds)
            {
                errors.Add((
                    "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds",
                    "MaximumShutdownDrainTimeoutSeconds must be less than or equal to BackFiller:Shutdown:GracePeriodSeconds to preserve bounded shutdown semantics."));
            }

            bool letsEncryptEnabled = backFiller.LetsEncrypt?.Enabled ?? true;

            if (!letsEncryptEnabled)
            {
                warnings.Add((
                    "BackFiller:LetsEncrypt:Enabled",
                    "BackFiller TLS is disabled (BackFiller:LetsEncrypt:Enabled=false). Listener will operate without transport encryption."));

                // Architectural invariant:
                // Cloudflare remains mandatory even with TLS disabled because BackFiller still
                // requires DNS/FQDN operational workflows independent of certificate issuance.
                List<LetsEncryptValidationResult> cloudflareApiTokenDiagnosticsWhenTlsDisabled = LetsEncryptValidator.ValidateCloudFlareApiToken(
                    backFiller.LetsEncrypt?.CloudFlareApiToken,
                    "BackFiller:LetsEncrypt");
                AddDiagnostics(errors, warnings, cloudflareApiTokenDiagnosticsWhenTlsDisabled);

                List<LetsEncryptValidationResult> cloudflareZoneDiagnosticsWhenTlsDisabled = LetsEncryptValidator.ValidateCloudFlareZoneId(
                    backFiller.LetsEncrypt?.CloudFlareZoneId,
                    "BackFiller:LetsEncrypt");
                AddDiagnostics(errors, warnings, cloudflareZoneDiagnosticsWhenTlsDisabled);

                return errors;
            }

            bool useStagingDirectory = backFiller.LetsEncrypt?.UseStagingDirectory ?? false;
            if (useStagingDirectory)
            {
                warnings.Add((
                    "BackFiller:LetsEncrypt:UseStagingDirectory",
                    "Let's Encrypt staging directory is enabled (BackFiller:LetsEncrypt:UseStagingDirectory=true). Issued certificates are for testing and are not suitable for production trust."));
            }

            string? generatedBackFillerFqdn = null;
            if (!string.IsNullOrWhiteSpace(backFiller.Name) &&
                backFiller.Id is >= 0 and <= 99 &&
                !string.IsNullOrWhiteSpace(backFiller.DnsSuffix))
            {
                try
                {
                    generatedBackFillerFqdn = BackFillerIdentityValidator.BuildBackFillerFqdn(
                        backFiller.Name,
                        backFiller.Id.Value,
                        backFiller.DnsSuffix);
                    Log.Information("Generated canonical BackFiller certificate identity FQDN: {BackFillerFqdn}", generatedBackFillerFqdn);
                }
                catch (ArgumentOutOfRangeException ex)
                {
                    errors.Add(("BackFiller:Id", $"Failed to generate canonical BackFiller FQDN: {ex.Message}"));
                }
                catch (ArgumentException ex)
                {
                    errors.Add(("BackFiller", $"Failed to generate canonical BackFiller FQDN: {ex.Message}"));
                }
            }

            // BackFiller source-of-truth certificate identity is the generated canonical FQDN.
            // Do not mutate configuration-derived options here; use a derived runtime value for validation.
            string[]? effectiveDomainNames = !string.IsNullOrWhiteSpace(generatedBackFillerFqdn)
                ? [generatedBackFillerFqdn]
                : null;

            // Validate the effective DomainNames shape used by runtime certificate operations.
            if (effectiveDomainNames != null)
            {
                List<LetsEncryptValidationResult> domainNamesDiagnostics = LetsEncryptValidator.ValidateDomainNames(
                    effectiveDomainNames,
                    "BackFiller:LetsEncrypt");

                AddDiagnostics(errors, warnings, domainNamesDiagnostics);
            }

            // Validate PFX export password requirements for certificate bundle protection.
            List<LetsEncryptValidationResult> pfxPasswordDiagnostics = LetsEncryptValidator.ValidatePfxExportPassword(
                backFiller.LetsEncrypt?.PfxExportPassword,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, pfxPasswordDiagnostics);

            // Validate renewal-check scheduler interval bounds.
            List<LetsEncryptValidationResult> renewalCheckIntervalDiagnostics = LetsEncryptValidator.ValidateRenewalCheckIntervalHours(
                backFiller.LetsEncrypt?.RenewalCheckIntervalHours,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, renewalCheckIntervalDiagnostics);

            // Validate renewal-check scheduling jitter ratio bounds.
            List<LetsEncryptValidationResult> renewalJitterDiagnostics = LetsEncryptValidator.ValidateRenewalJitterRatio(
                backFiller.LetsEncrypt?.RenewalJitterRatio,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, renewalJitterDiagnostics);

            // Validate renewal eligibility threshold bounds.
            List<LetsEncryptValidationResult> renewBeforeExpiryDaysDiagnostics = LetsEncryptValidator.ValidateRenewBeforeExpiryDays(
                backFiller.LetsEncrypt?.RenewBeforeExpiryDays,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, renewBeforeExpiryDaysDiagnostics);

            // Validate ACME account email when Let's Encrypt integration is configured.
            List<LetsEncryptValidationResult> letsEncryptDiagnostics = LetsEncryptValidator.ValidateAcmeAccountEmail(
                backFiller.LetsEncrypt?.AcmeAccountEmail,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, letsEncryptDiagnostics);

            // Validate account-key filename semantics and PEM private key loadability.
            List<LetsEncryptValidationResult> accountKeyDiagnostics = LetsEncryptValidator.ValidateAcmeAccountKeyPem(
                backFiller.LetsEncrypt?.AcmeAccountKeyPem,
                backFiller.DirCerts,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, accountKeyDiagnostics);

            // Validate transient ACME retry attempt bounds.
            List<LetsEncryptValidationResult> transientRetryDiagnostics = LetsEncryptValidator.ValidateAcmeTransientRetryMaxAttempts(
                backFiller.LetsEncrypt?.AcmeTransientRetryMaxAttempts,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, transientRetryDiagnostics);

            // Validate clock-skew check TTL bounds.
            List<LetsEncryptValidationResult> clockSkewCheckTtlDiagnostics = LetsEncryptValidator.ValidateClockSkewCheckTtlMinutes(
                backFiller.LetsEncrypt?.ClockSkewCheckTtlMinutes,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, clockSkewCheckTtlDiagnostics);

            // Validate maximum permitted clock-skew bounds.
            List<LetsEncryptValidationResult> clockSkewMaxDiagnostics = LetsEncryptValidator.ValidateClockSkewMaxMinutes(
                backFiller.LetsEncrypt?.ClockSkewMaxMinutes,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, clockSkewMaxDiagnostics);

            // Validate authoritative DNS nameserver cache TTL bounds.
            List<LetsEncryptValidationResult> dnsAuthoritativeNsCacheDiagnostics = LetsEncryptValidator.ValidateDnsAuthoritativeNsCacheMinutes(
                backFiller.LetsEncrypt?.DnsAuthoritativeNsCacheMinutes,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, dnsAuthoritativeNsCacheDiagnostics);

            // Validate authoritative DNS quorum ratio bounds.
            List<LetsEncryptValidationResult> dnsAuthoritativeQuorumRatioDiagnostics = LetsEncryptValidator.ValidateDnsAuthoritativeQuorumRatio(
                backFiller.LetsEncrypt?.DnsAuthoritativeQuorumRatio,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, dnsAuthoritativeQuorumRatioDiagnostics);

            // Validate DNS propagation-delay bounds.
            List<LetsEncryptValidationResult> dnsPropagationDelayDiagnostics = LetsEncryptValidator.ValidateDnsPropagationDelaySeconds(
                backFiller.LetsEncrypt?.DnsPropagationDelaySeconds,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, dnsPropagationDelayDiagnostics);

            // Validate DNS TXT polling interval bounds.
            List<LetsEncryptValidationResult> dnsTxtPollIntervalDiagnostics = LetsEncryptValidator.ValidateDnsTxtPollIntervalSeconds(
                backFiller.LetsEncrypt?.DnsTxtPollIntervalSeconds,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, dnsTxtPollIntervalDiagnostics);

            // Validate DNS TXT polling timeout bounds.
            List<LetsEncryptValidationResult> dnsTxtPollTimeoutDiagnostics = LetsEncryptValidator.ValidateDnsTxtPollTimeoutSeconds(
                backFiller.LetsEncrypt?.DnsTxtPollTimeoutSeconds,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, dnsTxtPollTimeoutDiagnostics);

            // Validate DNS TXT polling interval/timeout coherence.
            List<LetsEncryptValidationResult> dnsTxtPollingCoherenceDiagnostics = LetsEncryptValidator.ValidateDnsTxtPollingCoherence(
                backFiller.LetsEncrypt?.DnsTxtPollIntervalSeconds,
                backFiller.LetsEncrypt?.DnsTxtPollTimeoutSeconds,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, dnsTxtPollingCoherenceDiagnostics);

            // Validate Cloudflare API token formatting and requiredness.
            List<LetsEncryptValidationResult> cloudflareApiTokenDiagnostics = LetsEncryptValidator.ValidateCloudFlareApiToken(
                backFiller.LetsEncrypt?.CloudFlareApiToken,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, cloudflareApiTokenDiagnostics);

            // Validate Cloudflare zone identifier formatting and requiredness.
            List<LetsEncryptValidationResult> cloudflareZoneDiagnostics = LetsEncryptValidator.ValidateCloudFlareZoneId(
                backFiller.LetsEncrypt?.CloudFlareZoneId,
                "BackFiller:LetsEncrypt");

            AddDiagnostics(errors, warnings, cloudflareZoneDiagnostics);

            return errors;
        }

        /// <summary>
        /// Adds bind-address validation diagnostics to error and warning collections.
        /// </summary>
        /// <param name="errors">Error collection to receive blocking diagnostics.</param>
        /// <param name="warnings">Warning collection to receive non-blocking diagnostics.</param>
        /// <param name="diagnostics">Bind-address diagnostics to project into output collections.</param>
        private static void AddDiagnostics(
            List<(string Setting, string Error)> errors,
            List<(string Setting, string Message)> warnings,
            IEnumerable<BindAddressValidationResult> diagnostics)
        {
            foreach (BindAddressValidationResult diagnostic in diagnostics)
            {
                if (diagnostic.Severity == ValidationSeverity.Error)
                {
                    errors.Add((diagnostic.Setting, diagnostic.Message));
                }
                else if (diagnostic.Severity == ValidationSeverity.Warning)
                {
                    warnings.Add((diagnostic.Setting, diagnostic.Message));
                }
            }
        }

        /// <summary>
        /// Adds BackFiller identity validation diagnostics to error and warning collections.
        /// </summary>
        /// <param name="errors">Error collection to receive blocking diagnostics.</param>
        /// <param name="warnings">Warning collection to receive non-blocking diagnostics.</param>
        /// <param name="diagnostics">Identity diagnostics to project into output collections.</param>
        private static void AddDiagnostics(
            List<(string Setting, string Error)> errors,
            List<(string Setting, string Message)> warnings,
            IEnumerable<BackFillerIdentityValidationResult> diagnostics)
        {
            foreach (BackFillerIdentityValidationResult diagnostic in diagnostics)
            {
                if (diagnostic.Severity == ValidationSeverity.Error)
                {
                    errors.Add((diagnostic.Setting, diagnostic.Message));
                }
                else if (diagnostic.Severity == ValidationSeverity.Warning)
                {
                    warnings.Add((diagnostic.Setting, diagnostic.Message));
                }
            }
        }

        /// <summary>
        /// Adds RabbitMQ validation diagnostics to error and warning collections.
        /// </summary>
        /// <param name="errors">Error collection to receive blocking diagnostics.</param>
        /// <param name="warnings">Warning collection to receive non-blocking diagnostics.</param>
        /// <param name="diagnostics">RabbitMQ diagnostics to project into output collections.</param>
        private static void AddDiagnostics(
            List<(string Setting, string Error)> errors,
            List<(string Setting, string Message)> warnings,
            IEnumerable<RabbitMqValidationResult> diagnostics)
        {
            foreach (RabbitMqValidationResult diagnostic in diagnostics)
            {
                if (diagnostic.Severity == ValidationSeverity.Error)
                {
                    errors.Add((diagnostic.Setting, diagnostic.Message));
                }
                else if (diagnostic.Severity == ValidationSeverity.Warning)
                {
                    warnings.Add((diagnostic.Setting, diagnostic.Message));
                }
            }
        }

        /// <summary>
        /// Adds TransitServer validation diagnostics to error and warning collections.
        /// </summary>
        /// <param name="errors">Error collection to receive blocking diagnostics.</param>
        /// <param name="warnings">Warning collection to receive non-blocking diagnostics.</param>
        /// <param name="diagnostics">TransitServer diagnostics to project into output collections.</param>
        private static void AddDiagnostics(
            List<(string Setting, string Error)> errors,
            List<(string Setting, string Message)> warnings,
            IEnumerable<TransitServerValidationResult> diagnostics)
        {
            foreach (TransitServerValidationResult diagnostic in diagnostics)
            {
                if (diagnostic.Severity == ValidationSeverity.Error)
                {
                    errors.Add((diagnostic.Setting, diagnostic.Message));
                }
                else if (diagnostic.Severity == ValidationSeverity.Warning)
                {
                    warnings.Add((diagnostic.Setting, diagnostic.Message));
                }
            }
        }

        /// <summary>
        /// Adds Let's Encrypt validation diagnostics to error and warning collections.
        /// </summary>
        /// <param name="errors">Error collection to receive blocking diagnostics.</param>
        /// <param name="warnings">Warning collection to receive non-blocking diagnostics.</param>
        /// <param name="diagnostics">Let's Encrypt diagnostics to project into output collections.</param>
        private static void AddDiagnostics(
            List<(string Setting, string Error)> errors,
            List<(string Setting, string Message)> warnings,
            IEnumerable<LetsEncryptValidationResult> diagnostics)
        {
            foreach (LetsEncryptValidationResult diagnostic in diagnostics)
            {
                if (diagnostic.Severity == ValidationSeverity.Error)
                {
                    errors.Add((diagnostic.Setting, diagnostic.Message));
                }
                else if (diagnostic.Severity == ValidationSeverity.Warning)
                {
                    warnings.Add((diagnostic.Setting, diagnostic.Message));
                }
            }
        }

        /// <summary>
        /// Adds connection string validation diagnostics to error and warning collections.
        /// </summary>
        /// <param name="errors">Error collection to receive blocking diagnostics.</param>
        /// <param name="warnings">Warning collection to receive non-blocking diagnostics.</param>
        /// <param name="diagnostics">Connection string diagnostics to project into output collections.</param>
        private static void AddDiagnostics(
            List<(string Setting, string Error)> errors,
            List<(string Setting, string Message)> warnings,
            IEnumerable<ConnectionStringValidationResult> diagnostics)
        {
            foreach (ConnectionStringValidationResult diagnostic in diagnostics)
            {
                if (diagnostic.Severity == ValidationSeverity.Error)
                {
                    errors.Add((diagnostic.Setting, diagnostic.Message));
                }
                else if (diagnostic.Severity == ValidationSeverity.Warning)
                {
                    warnings.Add((diagnostic.Setting, diagnostic.Message));
                }
            }
        }
    }
}
