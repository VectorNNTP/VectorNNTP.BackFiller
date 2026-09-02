// <copyright file="MySqlConnectionStringUtilities.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: my sql connection string utilities in the vector nntp.back filler configuration subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// MySqlConnectionStringUtilities.cs -- Canonical interpretation of MySQL connection strings.
//
// Provides application-level parsing of MySQL connection strings with dual-path architecture
// to enforce ambiguity detection while maintaining provider-correct typed validation.
//
// Architecture (dual-path):
//
//   Raw connection string
//          │
//          ├─────────────────────────────────────┐
//          │                                     │
//          │                                     │
//          ▼                                     ▼
//   ParseRawKeyValuePairs                MySqlConnectionStringBuilder
//     (parse once)                         (provider-owned canonicalization)
//          │                                     │
//          ├──────┬──────┬──────┬─────┬─────┐    │
//          ▼      ▼      ▼      ▼     ▼     ▼    │
//       Server Database User  Pwd  Min  Max      │
//       check   check   check check Pool Pool    │
//          │      │      │      │     │    │     │
//          └──────┴──────┴──────┴─────┴────┘     │
//                     │                          │
//                     ▼                          ▼
//          Ambiguity detection          Provider canonical value + typed validation
//       (HasAmbiguousAliases)              (TryParse, pool size parsing)
//       (TryGetServer, etc.)
//                     │                          │
//                     └──────────┬───────────────┘
//                                ▼
//                     Application semantics
//                  (validation, fingerprinting)
//
// Path 1: Raw key/value parsing via ParseRawKeyValuePairs
//   - Used by: TryGetServer, TryGetDatabase, TryGetUsername, TryGetPassword (independent calls)
//   - Used by: HasAmbiguousAliases → HasConflictingAliases (parse once, reuse for all groups)
//   - Purpose: Detect conflicts by preserving duplicates and validating syntax
//   - Rationale: MySqlConnectionStringBuilder and DbConnectionStringBuilder both silently apply 
//                last-write-wins semantics, which creates two security/correctness problems:
//                  1. Different aliases: "Server=db01;Host=db02" → builder.Server == "db02"
//                  2. Duplicate same key: "Server=db01;Server=db02" → builder.Server == "db02"
//                Both scenarios MUST be detected as ambiguous configurations before trusting
//                any canonical value. The raw parser preserves ALL key/value pairs including
//                duplicates, quoted values (both single and double quotes per ADO.NET spec),
//                and escaped quotes (e.g., 'it''s' → it's, "say ""hi""" → say "hi"),
//                then validates syntax to match DbConnectionStringBuilder behavior
//                (e.g., rejects empty keys, consecutive semicolons, unterminated quoted values,
//                and unexpected text after closing quotes).
//                HasAmbiguousAliases() parses once and reuses the result for all property checks.
//                TryGet* methods parse independently; for multiple property extraction from the
//                same connection string, consider using TryParseEffective() to get a builder,
//                then extract properties from the builder to avoid redundant parsing.
//
// Path 2: Provider-owned canonicalization via MySqlConnectionStringBuilder
//   - Used by: TryParse, TryGetMinPoolSize, TryGetMaxPoolSize
//   - Purpose: Leverage provider's typed validation (e.g., pool sizes must be valid uint)
//   - Rationale: The provider enforces constraints we cannot replicate safely
//                (e.g., negative pool sizes are rejected with ArgumentException)
//
// Both paths use the same alias lists (ServerAliases, DatabaseAliases, etc.) derived from
// MySqlConnector documentation to ensure consistent interpretation.

using MySqlConnector;
using System.Data.Common;

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Result status for connection string property parsing.
    /// </summary>
    internal enum ConnectionStringParseResult
    {
        /// <summary>Property was successfully parsed with a valid, non-ambiguous value.</summary>
        Success,

        /// <summary>Property is not present in the connection string.</summary>
        Missing,

        /// <summary>Property value is malformed or invalid.</summary>
        Invalid,

        /// <summary>Property has conflicting values from multiple aliases (e.g., Server=db01;Host=db02).</summary>
        Ambiguous
    }

    /// <summary>
    /// Utilities for canonical interpretation of MySQL connection strings.
    /// </summary>
    /// <remarks>
    /// <para>Provides application-level parsing of MySQL connection strings using a dual-path architecture:</para>
    /// <list type="bullet">
    /// <item><description>Path 1: Raw key/value parsing via <see cref="ParseRawKeyValuePairs"/> for ambiguity detection</description></item>
    /// <item><description>Path 2: Provider canonicalization via <see cref="MySqlConnectionStringBuilder"/> for typed validation</description></item>
    /// </list>
    /// <para>This dual-path design allows the application to detect conflicting aliases (e.g., Server=db01;Host=db02) 
    /// and duplicate keys (e.g., Server=db01;Server=db02) before the provider silently canonicalizes them (last-write-wins), 
    /// while still leveraging provider-enforced constraints for typed properties (e.g., pool sizes must be valid uint values).</para>
    /// <para><strong>RECOMMENDED API:</strong> Use <see cref="TryParseEffective"/> for normal application code. 
    /// It validates ambiguity before returning a canonical builder.</para>
    /// <para>Used by both validation and fingerprinting to ensure consistent interpretation.</para>
    /// </remarks>
    internal static class MySqlConnectionStringUtilities
    {
        // MySqlConnector aliases for server/host (official documentation)
        /// <summary>
        /// Stores server aliases used by my sql connection string utilities.
        /// </summary>
        private static readonly HashSet<string> ServerAliases = new(StringComparer.OrdinalIgnoreCase)
        {
            "Server",
            "Host",
            "Data Source",
            "DataSource",
            "Address",
            "Addr",
            "Network Address"
        };

        // MySqlConnector aliases for database name
        /// <summary>
        /// Stores database aliases used by my sql connection string utilities.
        /// </summary>
        private static readonly HashSet<string> DatabaseAliases = new(StringComparer.OrdinalIgnoreCase)
        {
            "Database",
            "Initial Catalog",
            "InitialCatalog"
        };

        // MySqlConnector aliases for username
        /// <summary>
        /// Stores username aliases used by my sql connection string utilities.
        /// </summary>
        private static readonly HashSet<string> UsernameAliases = new(StringComparer.OrdinalIgnoreCase)
        {
            "User ID",
            "UserID",
            "Username",
            "Uid",
            "User name",
            "User"
        };

        // MySqlConnector aliases for password
        /// <summary>
        /// Stores password aliases used by my sql connection string utilities.
        /// </summary>
        private static readonly HashSet<string> PasswordAliases = new(StringComparer.OrdinalIgnoreCase)
        {
            "Password",
            "Pwd"
        };

        // MySqlConnector aliases for minimum pool size
        /// <summary>
        /// Limits min pool size aliases for my sql connection string utilities.
        /// </summary>
        private static readonly HashSet<string> MinPoolSizeAliases = new(StringComparer.OrdinalIgnoreCase)
        {
            "Min Pool Size",
            "MinPoolSize",
            "Minimum Pool Size",
            "MinimumPoolSize"
        };

        // MySqlConnector aliases for maximum pool size
        /// <summary>
        /// Limits max pool size aliases for my sql connection string utilities.
        /// </summary>
        private static readonly HashSet<string> MaxPoolSizeAliases = new(StringComparer.OrdinalIgnoreCase)
        {
            "Max Pool Size",
            "MaxPoolSize",
            "Maximum Pool Size",
            "MaximumPoolSize"
        };

        /// <summary>
        /// Attempts to parse and validate a MySQL connection string into a MySqlConnectionStringBuilder.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="builder">The parsed builder if successful and unambiguous.</param>
        /// <returns><c>true</c> if the connection string was successfully parsed and contains no ambiguous aliases; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>This is the SAFE-BY-DEFAULT API for application code. It combines:</para>
        /// <list type="number">
        /// <item><description>Raw syntax validation (via <see cref="ParseRawKeyValuePairs"/>)</description></item>
        /// <item><description>Ambiguity detection (rejects Server=db01;Host=db02 or Server=db01;Server=db02)</description></item>
        /// <item><description>Provider canonicalization (via <see cref="MySqlConnectionStringBuilder"/>)</description></item>
        /// </list>
        /// <para>Use this method for normal application code where you need a validated, unambiguous builder.</para>
        /// <para>Only use <see cref="TryParse"/> if you need raw provider behavior without ambiguity checks.</para>
        /// </remarks>
        public static bool TryParseEffective(string? connectionString, out MySqlConnectionStringBuilder? builder)
        {
            builder = null;

            // Step 1: Check for ambiguous aliases FIRST (rejects conflicting configurations)
            if (HasAmbiguousAliases(connectionString))
            {
                return false;
            }

            // Step 2: Parse via provider (gets canonical builder)
            return TryParse(connectionString, out builder);
        }

        /// <summary>
        /// Attempts to parse a MySQL connection string into a MySqlConnectionStringBuilder WITHOUT ambiguity detection.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="builder">The parsed builder if successful.</param>
        /// <returns><c>true</c> if the connection string was successfully parsed; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>⚠️ WARNING: This method does NOT perform ambiguity detection.</para>
        /// <para>It will silently accept configurations like:</para>
        /// <list type="bullet">
        /// <item><description>Server=db01;Host=db02 (different aliases, last-write-wins)</description></item>
        /// <item><description>Server=db01;Server=db02 (duplicate keys, last-write-wins)</description></item>
        /// </list>
        /// <para>PREFER <see cref="TryParseEffective"/> for normal application code.</para>
        /// <para>Only use this method if you need raw provider behavior for specific use cases
        /// (e.g., testing provider semantics, diagnostic tooling).</para>
        /// <para>Uses Path 2 (provider-owned canonicalization) via <see cref="MySqlConnectionStringBuilder"/>.</para>
        /// </remarks>
        public static bool TryParse(string? connectionString, out MySqlConnectionStringBuilder? builder)
        {
            try
            {
                builder = new MySqlConnectionStringBuilder(connectionString ?? string.Empty);
                return true;
            }
            catch (ArgumentException)
            {
                // Invalid connection string syntax
                builder = null;
                return false;
            }
        }

        /// <summary>
        /// Attempts to parse the server/host from a MySQL connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="server">The server/host value if found.</param>
        /// <returns><c>true</c> if server/host was found and non-ambiguous; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>Uses Path 1 (raw alias inspection) via <see cref="DbConnectionStringBuilder"/> for ambiguity detection.</para>
        /// <para>Returns <c>false</c> if conflicting server aliases are detected (e.g., Server=db01;Host=db02).</para>
        /// </remarks>
        public static bool TryGetServer(string? connectionString, out string? server)
        {
            return TryGetConnectionStringValue(connectionString, ServerAliases, out server) && !string.IsNullOrWhiteSpace(server);
        }

        /// <summary>
        /// Attempts to parse the database name from a MySQL connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="database">The database name if found.</param>
        /// <returns><c>true</c> if database name was found and non-ambiguous; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>Uses Path 1 (raw alias inspection) via <see cref="DbConnectionStringBuilder"/> for ambiguity detection.</para>
        /// <para>Returns <c>false</c> if conflicting database aliases are detected (e.g., Database=foo;Initial Catalog=bar).</para>
        /// </remarks>
        public static bool TryGetDatabase(string? connectionString, out string? database)
        {
            return TryGetConnectionStringValue(connectionString, DatabaseAliases, out database) && !string.IsNullOrWhiteSpace(database);
        }

        /// <summary>
        /// Attempts to parse the username from a MySQL connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="username">The username/user ID if found.</param>
        /// <returns><c>true</c> if username was found and non-ambiguous; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>Uses Path 1 (raw alias inspection) via <see cref="DbConnectionStringBuilder"/> for ambiguity detection.</para>
        /// <para>Returns <c>false</c> if conflicting username aliases are detected (e.g., User ID=alice;Username=bob).</para>
        /// </remarks>
        public static bool TryGetUsername(string? connectionString, out string? username)
        {
            return TryGetConnectionStringValue(connectionString, UsernameAliases, out username) && !string.IsNullOrWhiteSpace(username);
        }

        /// <summary>
        /// Attempts to parse the password from a MySQL connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="password">The password if found.</param>
        /// <returns><c>true</c> if password was found and non-ambiguous; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>Uses Path 1 (raw alias inspection) via <see cref="DbConnectionStringBuilder"/> for ambiguity detection.</para>
        /// <para><b>SECURITY WARNING:</b> This method returns sensitive credential information.</para>
        /// <para><b>NEVER</b> use this for configuration fingerprinting or logging.</para>
        /// <para>Valid use cases: runtime validation (checking for ambiguous aliases),
        /// ambiguity detection (via <see cref="HasAmbiguousAliases"/>).</para>
        /// <para>Password is optional and may be provided programmatically via ProvidePasswordCallback.</para>
        /// <para>Returns <c>false</c> if conflicting password aliases are detected (e.g., Password=secret1;Pwd=secret2).</para>
        /// <para>Returns <c>false</c> if password is empty or whitespace-only.</para>
        /// </remarks>
        public static bool TryGetPassword(string? connectionString, out string? password)
        {
            return TryGetConnectionStringValue(connectionString, PasswordAliases, out password) && !string.IsNullOrWhiteSpace(password);
        }

        /// <summary>
        /// Attempts to parse the minimum pool size from a MySQL connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="minPoolSize">The parsed minimum pool size if found and valid.</param>
        /// <returns><c>true</c> if a valid minimum pool size was found and non-ambiguous; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>Uses dual-path architecture:</para>
        /// <list type="bullet">
        /// <item><description>Path 1: Raw alias inspection for ambiguity detection (via <see cref="DbConnectionStringBuilder"/>)</description></item>
        /// <item><description>Path 2: Provider typed validation (via <see cref="MySqlConnectionStringBuilder"/>) to enforce uint semantics</description></item>
        /// </list>
        /// <para>Returns <c>false</c> if:</para>
        /// <list type="bullet">
        /// <item><description>No min pool size property found</description></item>
        /// <item><description>Value is not a valid non-negative integer</description></item>
        /// <item><description>Value exceeds <see cref="int.MaxValue"/> (would cause integer overflow)</description></item>
        /// <item><description>Multiple conflicting min pool size aliases present</description></item>
        /// </list>
        /// </remarks>
        public static bool TryGetMinPoolSize(string? connectionString, out int minPoolSize)
        {
            // Step 1: Check for ambiguous aliases and get the raw value
            if (!TryGetConnectionStringValue(connectionString, MinPoolSizeAliases, out string? value))
            {
                minPoolSize = 0;
                return false;
            }

            // Defend against null/empty value (shouldn't happen after TryGetConnectionStringValue returns true, but satisfy analyzer)
            if (string.IsNullOrWhiteSpace(value))
            {
                minPoolSize = 0;
                return false;
            }

            // Step 2: Validate using MySqlConnectionStringBuilder for provider-correct uint semantics
            // Create a minimal connection string with just the pool size property to avoid parsing failures
            // from other potentially invalid properties in the original string
            try
            {
                // Parse and validate as uint first
                uint parsed = uint.Parse(value, System.Globalization.CultureInfo.InvariantCulture);

                // Reject values that would overflow when cast to int
                // (uint.MaxValue = 4294967295 would become -1 after cast)
                if (parsed > int.MaxValue)
                {
                    minPoolSize = 0;
                    return false;
                }

                // Use MySqlConnectionStringBuilder to verify provider accepts the value
                MySqlConnectionStringBuilder builder = new() { MinimumPoolSize = parsed };
                minPoolSize = (int)builder.MinimumPoolSize;
                return true;
            }
            catch (Exception ex) when (ex is FormatException or OverflowException or ArgumentException)
            {
                // Invalid format, negative value, or out of uint range
                minPoolSize = 0;
                return false;
            }
        }

        /// <summary>
        /// Attempts to parse the maximum pool size from a MySQL connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="maxPoolSize">The parsed maximum pool size if found and valid.</param>
        /// <returns><c>true</c> if a valid maximum pool size was found and non-ambiguous; otherwise <c>false</c>.</returns>
        /// <remarks>
        /// <para>Uses dual-path architecture:</para>
        /// <list type="bullet">
        /// <item><description>Path 1: Raw alias inspection for ambiguity detection (via <see cref="DbConnectionStringBuilder"/>)</description></item>
        /// <item><description>Path 2: Provider typed validation (via <see cref="MySqlConnectionStringBuilder"/>) to enforce uint semantics</description></item>
        /// </list>
        /// <para>Returns <c>false</c> if:</para>
        /// <list type="bullet">
        /// <item><description>No max pool size property found</description></item>
        /// <item><description>Value is not a valid non-negative integer</description></item>
        /// <item><description>Value exceeds <see cref="int.MaxValue"/> (would cause integer overflow)</description></item>
        /// <item><description>Multiple conflicting max pool size aliases present</description></item>
        /// </list>
        /// </remarks>
        public static bool TryGetMaxPoolSize(string? connectionString, out int maxPoolSize)
        {
            // Step 1: Check for ambiguous aliases and get the raw value
            if (!TryGetConnectionStringValue(connectionString, MaxPoolSizeAliases, out string? value))
            {
                maxPoolSize = 0;
                return false;
            }

            // Defend against null/empty value (shouldn't happen after TryGetConnectionStringValue returns true, but satisfy analyzer)
            if (string.IsNullOrWhiteSpace(value))
            {
                maxPoolSize = 0;
                return false;
            }

            // Step 2: Validate using MySqlConnectionStringBuilder for provider-correct uint semantics
            // Create a minimal connection string with just the pool size property to avoid parsing failures
            // from other potentially invalid properties in the original string
            try
            {
                // Parse and validate as uint first
                uint parsed = uint.Parse(value, System.Globalization.CultureInfo.InvariantCulture);

                // Reject values that would overflow when cast to int
                // (uint.MaxValue = 4294967295 would become -1 after cast)
                if (parsed > int.MaxValue)
                {
                    maxPoolSize = 0;
                    return false;
                }

                // Use MySqlConnectionStringBuilder to verify provider accepts the value
                MySqlConnectionStringBuilder builder = new() { MaximumPoolSize = parsed };
                maxPoolSize = (int)builder.MaximumPoolSize;
                return true;
            }
            catch (Exception ex) when (ex is FormatException or OverflowException or ArgumentException)
            {
                // Invalid format, negative value, or out of uint range
                maxPoolSize = 0;
                return false;
            }
        }

        /// <summary>
        /// Checks if a PARSEABLE connection string contains ambiguous/conflicting aliases for any property.
        /// </summary>
        /// <param name="connectionString">The connection string to check.</param>
        /// <returns><c>true</c> if the connection string is parseable AND contains ambiguous aliases; 
        /// <c>false</c> if the string is unparseable, null, whitespace, or contains no conflicts.</returns>
        /// <remarks>
        /// <para><strong>⚠️ CRITICAL:</strong> This method ONLY answers the question:</para>
        /// <para><strong>"Does this syntactically parseable connection string contain conflicting aliases?"</strong></para>
        /// <para>It does <strong>NOT</strong> answer:</para>
        /// <list type="bullet">
        /// <item><description>"Is this connection string valid?" (use <see cref="TryParse"/> or <see cref="TryParseEffective"/>)</description></item>
        /// <item><description>"Is this connection string safe to use?" (use <see cref="TryParseEffective"/>)</description></item>
        /// <item><description>"Does this connection string have all required properties?" (check individual properties)</description></item>
        /// </list>
        /// <para><strong>Behavior:</strong></para>
        /// <list type="bullet">
        /// <item><description>Returns <c>false</c> for null/whitespace (no conflict, but also not valid)</description></item>
        /// <item><description>Returns <c>false</c> for malformed syntax (parsing fails, so no conflict detected)</description></item>
        /// <item><description>Returns <c>false</c> for syntactically valid strings with no conflicts</description></item>
        /// <item><description>Returns <c>true</c> ONLY if parseable AND contains conflicts like <c>Server=db01;Host=db02</c></description></item>
        /// </list>
        /// <para><strong>WRONG usage:</strong></para>
        /// <code>
        /// if (!HasAmbiguousAliases(cs)) { /* UNSAFE! String might be malformed! */ }
        /// </code>
        /// <para><strong>CORRECT usage:</strong></para>
        /// <code>
        /// if (TryParseEffective(cs, out var builder)) { /* Safe */ }
        /// // or:
        /// if (HasAmbiguousAliases(cs)) { throw new Exception("Ambiguous config"); }
        /// if (!TryParse(cs, out var builder)) { throw new Exception("Invalid syntax"); }
        /// </code>
        /// <para>Detects cases where multiple aliases for the same property have different values,
        /// such as <c>Server=db01;Host=db02</c> or <c>User ID=alice;Username=bob</c>.</para>
        /// <para>Missing properties (e.g., no password, no pool size) are NOT considered ambiguous.</para>
        /// <para>This is critical for security and configuration fingerprinting integrity.</para>
        /// </remarks>
        public static bool HasAmbiguousAliases(string? connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return false;
            }

            // Parse connection string ONCE
#pragma warning disable CA1859 // Use concrete types when possible for improved performance - readonly semantics preferred for clarity
            IReadOnlyList<(string Key, string Value)> rawPairs;
#pragma warning restore CA1859
            try
            {
                rawPairs = ParseRawKeyValuePairs(connectionString);
            }
            catch (ArgumentException)
            {
                // Malformed connection string - cannot detect conflicts in unparseable input
                // Returning false does NOT mean "safe" - it means "no conflicts detected"
                return false;
            }

            // Check each property group for conflicts using the same parsed pairs
            return HasConflictingAliases(rawPairs, ServerAliases) ||
                   HasConflictingAliases(rawPairs, DatabaseAliases) ||
                   HasConflictingAliases(rawPairs, UsernameAliases) ||
                   HasConflictingAliases(rawPairs, PasswordAliases) ||
                   HasConflictingAliases(rawPairs, MinPoolSizeAliases) ||
                   HasConflictingAliases(rawPairs, MaxPoolSizeAliases);
        }

        // <summary>
        // Parses a connection string into raw key-value pairs BEFORE DbConnectionStringBuilder canonicalization.
        // </summary>
        // <param name="connectionString">The connection string to parse.</param>
        // <returns>List of (key, value) pairs preserving all instances including duplicates.</returns>
        // <exception cref="ArgumentException">Thrown if the connection string is malformed.</exception>
        // <remarks>
        // <para>This parser respects connection string syntax:</para>
        // <list type="bullet">
        // <item><description>Semicolon (;) as delimiter</description></item>
        // <item><description>Equals (=) as key-value separator</description></item>
        // <item><description>Single or double quotes for values containing special characters</description></item>
        // <item><description>Escaped quotes ("" or '') within quoted values</description></item>
        // <item><description>Whitespace trimming around keys and values</description></item>
        // </list>
        // <para>Unlike DbConnectionStringBuilder, this preserves duplicate keys so we can detect:</para>
        // <para><c>Server=db01;Server=db02</c> (same key repeated with different values)</para>
        // <para>Validates syntax to match DbConnectionStringBuilder behavior (e.g., rejects empty keys, consecutive semicolons).</para>
        // </remarks>
#pragma warning disable CA1859 // Use concrete types when possible for improved performance - readonly semantics preferred for clarity
        /// <summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <returns>Key-value pairs preserving duplicate entries for ambiguity detection.</returns>
        /// <exception cref="ArgumentException">Thrown when the connection string syntax is malformed.</exception>
        /// <remarks>Parsing occurs before provider canonicalization so conflicting aliases remain observable.</remarks>
        /// </summary>
        private static IReadOnlyList<(string Key, string Value)> ParseRawKeyValuePairs(string connectionString)
#pragma warning restore CA1859
        {
            List<(string, string)> pairs = [];
            int i = 0;
            int length = connectionString.Length;

            while (i < length)
            {
                // Skip whitespace
                while (i < length && char.IsWhiteSpace(connectionString[i]))
                {
                    i++;
                }

                if (i >= length)
                {
                    break;
                }

                // Check for consecutive semicolons or leading semicolon (invalid syntax)
                if (connectionString[i] == ';')
                {
                    throw new ArgumentException("Malformed connection string: empty key-value pair", nameof(connectionString));
                }

                // Read key until '='
                int keyStart = i;
                while (i < length && connectionString[i] != '=' && connectionString[i] != ';')
                {
                    i++;
                }

                if (i >= length || connectionString[i] == ';')
                {
                    // No '=' found before end or semicolon, invalid pair
                    throw new ArgumentException("Malformed connection string: missing '=' in key-value pair", nameof(connectionString));
                }

                string key = connectionString[keyStart..i].Trim();

                if (string.IsNullOrEmpty(key))
                {
                    throw new ArgumentException("Malformed connection string: empty key", nameof(connectionString));
                }

                i++; // Skip '='

                // Skip leading whitespace after '=' per ADO.NET spec
                while (i < length && char.IsWhiteSpace(connectionString[i]))
                {
                    i++;
                }

                // Read value until ';' or end
                string value;
                if (i < length && connectionString[i] is '"' or '\'')
                {
                    // Quoted value (supports both double and single quotes per ADO.NET spec)
                    char quote = connectionString[i];
                    i++; // Skip opening quote
                    System.Text.StringBuilder valueBuilder = new();
                    bool closedQuote = false;

                    while (i < length)
                    {
                        if (connectionString[i] == quote)
                        {
                            // Check if it's an escaped quote (doubled quote)
                            if (i + 1 < length && connectionString[i + 1] == quote)
                            {
                                _ = valueBuilder.Append(quote);
                                i += 2; // Skip escaped quote
                            }
                            else
                            {
                                // End of quoted value
                                i++; // Skip closing quote
                                closedQuote = true;
                                break;
                            }
                        }
                        else
                        {
                            _ = valueBuilder.Append(connectionString[i]);
                            i++;
                        }
                    }

                    if (!closedQuote)
                    {
                        throw new ArgumentException("Malformed connection string: unterminated quoted value", nameof(connectionString));
                    }

                    value = valueBuilder.ToString();

                    // After closing quote, only whitespace, semicolon, or end-of-string is valid
                    while (i < length && connectionString[i] != ';')
                    {
                        if (!char.IsWhiteSpace(connectionString[i]))
                        {
                            throw new ArgumentException("Malformed connection string: unexpected text after closing quote", nameof(connectionString));
                        }
                        i++;
                    }
                }
                else
                {
                    // Unquoted value
                    int valueStart = i;
                    while (i < length && connectionString[i] != ';')
                    {
                        i++;
                    }

                    value = connectionString[valueStart..i].Trim();
                }

                pairs.Add((key, value));

                // Skip semicolon
                if (i < length && connectionString[i] == ';')
                {
                    i++;
                }
            }

            return pairs;
        }

        /// <summary>
        /// Parses a connection string property value from a set of possible alias keys.
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="possibleKeys">The aliases to check.</param>
        /// <param name="value">The parsed value if successful.</param>
        /// <returns>The parse result status.</returns>
        /// <remarks>
        /// <para>Returns <see cref="ConnectionStringParseResult.Missing"/> if the property is not present.</para>
        /// <para>Returns <see cref="ConnectionStringParseResult.Ambiguous"/> if conflicting values exist (including duplicate keys).</para>
        /// <para>Returns <see cref="ConnectionStringParseResult.Invalid"/> if the connection string is malformed.</para>
        /// <para>Returns <see cref="ConnectionStringParseResult.Success"/> if a valid, unambiguous value is found.</para>
        /// </remarks>
        private static ConnectionStringParseResult ParseConnectionStringValue(
            string? connectionString,
            HashSet<string> possibleKeys,
            out string? value)
        {
            value = null;

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return ConnectionStringParseResult.Missing;
            }

            // Parse raw key-value pairs to catch duplicate keys BEFORE canonicalization
#pragma warning disable CA1859 // Use concrete types when possible for improved performance - readonly semantics preferred for clarity
            IReadOnlyList<(string Key, string Value)> rawPairs;
#pragma warning restore CA1859
            try
            {
                rawPairs = ParseRawKeyValuePairs(connectionString);
            }
            catch (ArgumentException)
            {
                // Malformed connection string
                return ConnectionStringParseResult.Invalid;
            }

            // Collect all values for the given alias group (case-insensitive key matching)
            string? foundValue = null;

            foreach ((string key, string pairValue) in rawPairs)
            {
                // Check if this key is in the alias set (case-insensitive via HashSet comparer)
                if (possibleKeys.Contains(key))
                {
                    if (foundValue == null)
                    {
                        // First occurrence found
                        foundValue = pairValue;
                    }
                    else if (!string.Equals(foundValue, pairValue, StringComparison.Ordinal))
                    {
                        // Conflicting value detected (either different aliases or duplicate keys)
                        return ConnectionStringParseResult.Ambiguous;
                    }
                    // else: same value, redundant but acceptable
                }
            }

            if (foundValue == null)
            {
                return ConnectionStringParseResult.Missing;
            }

            value = foundValue;
            return ConnectionStringParseResult.Success;
        }

        /// <summary>
        /// Checks if a set of parsed key-value pairs contains conflicting values for a set of aliases.
        /// </summary>
        /// <param name="rawPairs">The parsed key-value pairs.</param>
        /// <param name="possibleKeys">The aliases to check for conflicts.</param>
        /// <returns><c>true</c> if conflicting values are found; <c>false</c> if absent or consistent.</returns>
        /// <remarks>
        /// <para>This method inspects pre-parsed key-value pairs (no re-parsing needed).</para>
        /// <para>It detects BOTH:</para>
        /// <list type="bullet">
        /// <item><description>Different aliases with different values: <c>Server=db01;Host=db02</c></description></item>
        /// <item><description>Same key repeated with different values: <c>Server=db01;Server=db02</c></description></item>
        /// </list>
        /// <para>Returns <c>false</c> if the property is absent (not an error).</para>
        /// <para>Returns <c>true</c> only if multiple keys/values exist with different values.</para>
        /// <para>This is used for ambiguity detection independently of property extraction.</para>
        /// </remarks>
        private static bool HasConflictingAliases(IReadOnlyList<(string Key, string Value)> rawPairs, HashSet<string> possibleKeys)
        {
            // Collect all values for the given alias group (case-insensitive key matching)
            string? foundValue = null;

            foreach ((string key, string value) in rawPairs)
            {
                // Check if this key is in the alias set (case-insensitive via HashSet comparer)
                if (possibleKeys.Contains(key))
                {
                    if (foundValue == null)
                    {
                        // First occurrence found
                        foundValue = value;
                    }
                    else if (!string.Equals(foundValue, value, StringComparison.Ordinal))
                    {
                        // Conflicting value detected
                        return true;
                    }
                    // else: same value, redundant but acceptable
                }
            }

            // No conflicts found (property may be absent or have consistent values)
            return false;
        }

        /// <summary>
        /// Attempts to retrieve a connection string value by trying multiple possible key names (case-insensitive).
        /// </summary>
        /// <param name="connectionString">The connection string to parse.</param>
        /// <param name="possibleKeys">Set of possible key names to try.</param>
        /// <param name="value">The value if found.</param>
        /// <returns><c>true</c> if exactly one value was found for the keys; <c>false</c> if none or multiple conflicting values found.</returns>
        /// <remarks>
        /// <para>This method detects ambiguous configurations where multiple aliases for the same property
        /// are present with different values (e.g., <c>Server=db01;Host=db02</c>).</para>
        /// <para>If multiple aliases are present with the <b>same value</b>, the configuration is accepted
        /// (e.g., <c>Server=db01;Host=db01</c> is valid but redundant).</para>
        /// <para>If multiple aliases have <b>different values</b>, this returns <c>false</c> to signal
        /// an ambiguous/invalid configuration.</para>
        /// </remarks>
        private static bool TryGetConnectionStringValue(string? connectionString, HashSet<string> possibleKeys, out string? value)
        {
            return ParseConnectionStringValue(connectionString, possibleKeys, out value) == ConnectionStringParseResult.Success;
        }
    }
}
