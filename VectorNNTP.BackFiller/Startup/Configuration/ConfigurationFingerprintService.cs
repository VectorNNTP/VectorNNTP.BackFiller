// <copyright file="ConfigurationFingerprintService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Configuration
// Implements the configuration fingerprint service behavior.

using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Builds a deterministic, non-secret configuration fingerprint by filtering sensitive keys, sanitizing
    /// connection-string values, and hashing canonicalized key/value material.
    /// </summary>
    /// <remarks>
    /// This type supports startup diagnostics rather than configuration validity decisions. It emits warning/error
    /// log entries only when fingerprint extraction fails for specific values or for the full calculation path.
    /// Validation errors and warnings remain owned by the startup validation pipeline and
    /// <see cref="ConfigurationValidationResult"/>.
    /// </remarks>
    internal static class ConfigurationFingerprintService
    {
        /// <summary>
        /// Prefix embedded in emitted fingerprint strings to identify the canonicalization/hash format version.
        /// </summary>
        private const string FingerprintAlgorithmVersion = "v1";

        /// <summary>
        /// Case-insensitive key-segment patterns treated as secret-bearing and excluded from direct fingerprint input.
        /// </summary>
        private static readonly string[] SensitiveSegmentPatterns =
        [
            "password",
            "passwd",
            "pwd",
            "secret",
            "token",
            "apikey",
            "api_key",
            "accesskey",
            "access_key",
            "secretkey",
            "secret_key",
            "privatekey",
            "private_key",
            "signingkey",
            "signing_key",
            "encryptionkey",
            "encryption_key",
            "credential",
            "credentials"
        ];

        /// <summary>
        /// Determines whether any colon-delimited configuration-key segment matches a sensitive exact or suffix pattern.
        /// </summary>
        /// <param name="configurationKey">Configuration key to inspect.</param>
        /// <returns><see langword="true"/> when a segment is considered sensitive; otherwise <see langword="false"/>.</returns>
        internal static bool IsSensitiveConfigurationKey(string configurationKey)
        {
            string[] segments = configurationKey.Split(':');

            foreach (string segment in segments)
            {
                foreach (string pattern in SensitiveSegmentPatterns)
                {
                    if (segment.Equals(pattern, StringComparison.OrdinalIgnoreCase) ||
                        segment.EndsWith(pattern, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Identifies configuration keys that should be treated as connection strings for sanitization.
        /// </summary>
        /// <param name="configurationKey">Configuration key path to classify.</param>
        /// <returns>
        /// <see langword="true"/> when any key segment matches the connection-string naming patterns;
        /// otherwise <see langword="false"/>.
        /// </returns>
        internal static bool IsConnectionString(string configurationKey)
        {
            string[] segments = configurationKey.Split(':');

            foreach (string segment in segments)
            {
                if (segment.Equals("ConnectionStrings", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                if (segment.Equals("connectionstring", StringComparison.OrdinalIgnoreCase) ||
                    segment.EndsWith("connectionstring", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                if (segment.Equals("connection_string", StringComparison.OrdinalIgnoreCase) ||
                    segment.EndsWith("connection_string", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Produces a canonical, non-secret representation of a connection string for fingerprint input.
        /// </summary>
        /// <param name="connectionString">Raw connection-string value from configuration.</param>
        /// <returns>
        /// A normalized connection string containing only selected non-secret properties when parsing succeeds;
        /// otherwise <see langword="null"/>.
        /// </returns>
        /// <remarks>
        /// Parsing/sanitization failures are intentionally downgraded to a <see langword="null"/> result so callers
        /// can exclude the value and emit a warning instead of failing startup.
        /// </remarks>
        internal static string? SanitizeConnectionString(string connectionString)
        {
            try
            {
                DbConnectionStringBuilder builder = new() { ConnectionString = connectionString };

                Dictionary<string, object?> canonicalProperties = new(StringComparer.Ordinal);

                if (MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server)
                    && !string.IsNullOrWhiteSpace(server))
                {
                    canonicalProperties["Server"] = server;
                }

                if (MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database)
                    && !string.IsNullOrWhiteSpace(database))
                {
                    canonicalProperties["Database"] = database;
                }

                if (MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username)
                    && !string.IsNullOrWhiteSpace(username))
                {
                    canonicalProperties["Username"] = username;
                }

                if (MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize))
                {
                    canonicalProperties["Min Pool Size"] = minPoolSize;
                }

                if (MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize))
                {
                    canonicalProperties["Max Pool Size"] = maxPoolSize;
                }

                Dictionary<string, string> additionalMappings = new(StringComparer.OrdinalIgnoreCase)
                {
                    ["Port"] = "Port",
                    ["Server Port"] = "Port",
                    ["Application Name"] = "Application Name",
                    ["ApplicationName"] = "Application Name",
                    ["Connection Timeout"] = "Connection Timeout",
                    ["ConnectTimeout"] = "Connection Timeout",
                    ["Connect Timeout"] = "Connection Timeout",
                    ["Timeout"] = "Connection Timeout",
                    ["Command Timeout"] = "Command Timeout",
                    ["CommandTimeout"] = "Command Timeout",
                    ["Connection Idle Timeout"] = "Connection Idle Timeout",
                    ["ConnectionIdleTimeout"] = "Connection Idle Timeout",
                    ["Pooling"] = "Pooling",
                    ["Encrypt"] = "Encrypt",
                    ["TrustServerCertificate"] = "TrustServerCertificate",
                    ["Trust Server Certificate"] = "TrustServerCertificate",
                    ["Integrated Security"] = "Integrated Security",
                    ["IntegratedSecurity"] = "Integrated Security",
                    ["Enlist"] = "Enlist",
                    ["MultipleActiveResultSets"] = "MultipleActiveResultSets",
                    ["Multiple Active Result Sets"] = "MultipleActiveResultSets"
                };

                foreach (string originalKey in builder.Keys.Cast<string>())
                {
                    if (additionalMappings.TryGetValue(originalKey, out string? canonicalKey))
                    {
                        canonicalProperties[canonicalKey] = builder[originalKey];
                    }
                }

                DbConnectionStringBuilder canonical = [];

                foreach (string canonicalKey in canonicalProperties.Keys.OrderBy(k => k, StringComparer.Ordinal))
                {
                    canonical[canonicalKey] = canonicalProperties[canonicalKey];
                }

                return canonical.ConnectionString;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Applies key-specific canonicalization rules to non-secret configuration values before hashing.
        /// </summary>
        /// <param name="configurationKey">Configuration key that controls whether canonicalization is applied.</param>
        /// <param name="value">Raw non-secret configuration value.</param>
        /// <returns>The canonicalized value when a key-specific rule applies; otherwise the original value.</returns>
        /// <remarks>
        /// Canonicalization failures for supported keys are intentionally non-fatal and fall back to the original value
        /// to keep fingerprint generation best-effort.
        /// </remarks>
        private static string CanonicalizeNonSecretValue(string configurationKey, string value)
        {
            if (configurationKey.Equals("BackFiller:DnsSuffix", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    return BackFillerIdentityValidator.CanonicalizeDnsSuffix(value);
                }
                catch (ArgumentException)
                {
                    return value;
                }
            }

            return value;
        }

        /// <summary>
        /// Calculates the configuration fingerprint used by startup diagnostics to detect non-secret configuration drift.
        /// </summary>
        /// <param name="configuration">Merged application configuration to fingerprint.</param>
        /// <returns>
        /// A fingerprint formatted as <c>&lt;version&gt;:&lt;hash-prefix&gt;</c> (for example <c>v1:...</c>) when successful;
        /// otherwise the sentinel value <c>UNAVAILABLE</c>.
        /// </returns>
        /// <remarks>
        /// Sensitive keys are excluded, connection strings are sanitized before inclusion, and only non-null values are hashed.
        /// When connection-string sanitization fails, a warning is logged with structured field <c>ConfigKey</c> and that
        /// key is excluded from the fingerprint. Unexpected top-level failures are logged as errors with the exception attached
        /// and return <c>UNAVAILABLE</c>.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is <see langword="null"/>.</exception>
        internal static string CalculateConfigurationFingerprint(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);

            try
            {
                List<(string Key, string? Value)> configPairs = [];

                foreach (KeyValuePair<string, string?> item in configuration.AsEnumerable())
                {
                    if (item.Value == null)
                    {
                        continue;
                    }

                    if (IsConnectionString(item.Key))
                    {
                        string? sanitized = SanitizeConnectionString(item.Value);

                        if (sanitized == null)
                        {
                            Serilog.Log.Warning(
                                "Connection string parsing failed for key {ConfigKey} - excluding from fingerprint to prevent false equivalence. " +
                                "Connection string may be malformed or use unsupported format.",
                                item.Key);
                            continue;
                        }

                        configPairs.Add((item.Key, sanitized));
                    }
                    else if (!IsSensitiveConfigurationKey(item.Key))
                    {
                        string canonicalValue = CanonicalizeNonSecretValue(item.Key, item.Value);
                        configPairs.Add((item.Key, canonicalValue));
                    }
                }

                configPairs.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));

                StringBuilder sb = new();
                foreach ((string key, string? value) in configPairs)
                {
                    _ = sb.Append(key.Length);
                    _ = sb.Append(':');
                    _ = sb.Append(key);
                    _ = sb.Append(value!.Length);
                    _ = sb.Append(':');
                    _ = sb.Append(value);
                }

                string canonicalConfig = sb.ToString();
                byte[] data = Encoding.UTF8.GetBytes(canonicalConfig);
                byte[] hash = SHA256.HashData(data);

                string hashHex = $"{hash[0]:x2}{hash[1]:x2}{hash[2]:x2}{hash[3]:x2}{hash[4]:x2}{hash[5]:x2}{hash[6]:x2}{hash[7]:x2}";
                return $"{FingerprintAlgorithmVersion}:{hashHex}";
            }
            catch (Exception ex)
            {
                Serilog.Log.Error(ex, "Configuration fingerprint calculation failed - this is unusual and indicates a problem. Fingerprint will be UNAVAILABLE.");
                return "UNAVAILABLE";
            }
        }
    }
}
