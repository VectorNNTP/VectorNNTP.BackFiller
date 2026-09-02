// <copyright file="ConfigurationFingerprintService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Configuration
// Implements the configuration fingerprint service responsibilities for this subsystem boundary.

using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Owns configuration fingerprint calculation, sensitive-value detection and sanitization, and fingerprint logging.
    /// </summary>
    internal static class ConfigurationFingerprintService
    {
        /// <summary>
        /// Stores the fingerprint algorithm version state used to enforce this component's runtime contract.
        /// </summary>
        private const string FingerprintAlgorithmVersion = "v1";

        /// <summary>
        /// Stores the sensitive segment patterns state used to enforce this component's runtime contract.
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
        /// Performs the is sensitive configuration key operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the is connection string operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the sanitize connection string operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the canonicalize non secret value operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the calculate configuration fingerprint operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
