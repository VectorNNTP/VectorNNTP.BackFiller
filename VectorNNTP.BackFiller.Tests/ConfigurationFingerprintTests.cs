// <copyright file="ConfigurationFingerprintTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for configuration fingerprint, covering configuration and validation contracts.

using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Startup.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests for configuration fingerprinting functionality in ConfigurationFingerprintService.
    /// </summary>
    /// <remarks>
    /// <para>Validates the core assumptions around IConfiguration.AsEnumerable() behavior,
    /// connection string detection, sanitization, and overall fingerprint determinism.</para>
    /// </remarks>
    public class ConfigurationFingerprintTests
    {
        /// <summary>
        /// Verifies that IConfiguration.AsEnumerable() returns value-bearing entries for
        /// ConnectionStrings:Main and handles the parent ConnectionStrings entry correctly.
        /// </summary>
        /// <remarks>
        /// <para>This is the critical test for the issue raised: when you have JSON like:</para>
        /// <code>
        /// {
        ///   "ConnectionStrings": {
        ///     "Main": "Server=db01;Database=NNTP;User ID=foo;Password=secret"
        ///   }
        /// }
        /// </code>
        /// <para>IConfiguration.AsEnumerable() produces entries like:</para>
        /// <list type="bullet">
        /// <item><description>ConnectionStrings (value = null)</description></item>
        /// <item><description>ConnectionStrings:Main (value = "Server=db01;...")</description></item>
        /// </list>
        /// <para>We need to ensure:</para>
        /// <list type="number">
        /// <item><description>The parent entry with null value is skipped (already handled by null check)</description></item>
        /// <item><description>The child entry ConnectionStrings:Main is recognized as a connection string</description></item>
        /// <item><description>SanitizeConnectionString() is called on the actual connection string value</description></item>
        /// </list>
        /// </remarks>
        [Fact]
        public void WhenConnectionStringsSection_ThenValueBearingEntryIsDetectedAndSanitized()
        {
            // Arrange: Build IConfiguration from JSON simulating ConnectionStrings section
            Dictionary<string, string?> configData = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Database=NNTP;User ID=foo;Password=secret123;Port=5432"
            };

            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(configData)
                .Build();

            // Act: Calculate fingerprint (will internally call IsConnectionString and SanitizeConnectionString)
            string fingerprint = ConfigurationFingerprintService.CalculateConfigurationFingerprint(configuration);

            // Assert: Fingerprint should be calculated successfully (not UNAVAILABLE)
            Assert.NotEqual("UNAVAILABLE", fingerprint);
            Assert.Equal(19, fingerprint.Length); // "v1:" + 16-char hex = 19 chars
            Assert.StartsWith("v1:", fingerprint); // Version prefix

            // Verify behavior by checking enumeration output
            List<KeyValuePair<string, string?>> entries = [.. configuration.AsEnumerable()];

            // Should contain the value-bearing entry
            KeyValuePair<string, string?> mainEntry = entries.FirstOrDefault(kv => kv.Key == "ConnectionStrings:Main");
            Assert.NotNull(mainEntry.Key);
            Assert.NotNull(mainEntry.Value); // Has actual connection string value

            // IsConnectionString should recognize this key
            Assert.True(ConfigurationFingerprintService.IsConnectionString("ConnectionStrings:Main"));
        }

        /// <summary>
        /// Verifies that connection string detection handles all supported patterns.
        /// </summary>
        [Theory]
        [InlineData("ConnectionStrings:Main", true)]              // Conventional .NET section (plural)
        [InlineData("Database:ConnectionString", true)]           // Singular suffix
        [InlineData("MyDbConnectionString", true)]                // Singular suffix (no colon)
        [InlineData("Database:Connection_String", true)]          // Underscore variant
        [InlineData("MyDbConnection_String", true)]               // Underscore variant (no colon)
        [InlineData("ConnectionString", true)]                    // Exact match (singular)
        [InlineData("Connection_String", true)]                   // Exact match (underscore)
        [InlineData("DatabaseHost", false)]                       // Not a connection string
        [InlineData("Database:Password", false)]                  // Sensitive key, not connection string
        public void IsConnectionString_DetectsVariousPatterns(string key, bool expectedResult)
        {
            // Act
            bool result = ConfigurationFingerprintService.IsConnectionString(key);

            // Assert
            Assert.Equal(expectedResult, result);
        }

        /// <summary>
        /// Verifies that connection string sanitization uses allowlist filtering and removes credentials.
        /// </summary>
        [Fact]
        public void SanitizeConnectionString_RemovesCredentialsUsingAllowlist()
        {
            // Arrange
            /// <summary>
            /// Supplies original for the fixture or scenario under test.
            /// </summary>
            const string Original = "Server=db01;Database=NNTP;User ID=foo;Password=secret123;Port=5432";

            // Act
            string? sanitized = ConfigurationFingerprintService.SanitizeConnectionString(Original);

            // Assert
            Assert.NotNull(sanitized);
            Assert.DoesNotContain("Password", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("secret123", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Server=db01", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Database=NNTP", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Username=foo", sanitized, StringComparison.OrdinalIgnoreCase); // Normalized from "User ID"
        }

        /// <summary>
        /// Verifies that connection string sanitization handles quoted values with embedded semicolons.
        /// </summary>
        [Fact]
        public void SanitizeConnectionString_HandlesQuotedValuesWithSemicolons()
        {
            // Arrange: Password contains semicolon inside quotes
            /// <summary>
            /// Supplies original for the fixture or scenario under test.
            /// </summary>
            const string Original = "Server=db01;Password=\"abc;123\";Database=NNTP";

            // Act
            string? sanitized = ConfigurationFingerprintService.SanitizeConnectionString(Original);

            // Assert
            Assert.NotNull(sanitized);
            Assert.DoesNotContain("Password", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("abc;123", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Server=db01", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Database=NNTP", sanitized, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Verifies that malformed connection strings return null (not empty string).
        /// </summary>
        [Fact]
        public void SanitizeConnectionString_WhenMalformed_ReturnsNull()
        {
            // Arrange: Malformed connection string (unbalanced quotes)
            /// <summary>
            /// Supplies malformed for the fixture or scenario under test.
            /// </summary>
            const string Malformed = "Server=db01;Password=\"unclosed;Database=NNTP";

            // Act
            string? result = ConfigurationFingerprintService.SanitizeConnectionString(Malformed);

            // Assert: Should return null, not empty string (to avoid false equivalence)
            Assert.Null(result);
        }

        /// <summary>
        /// Verifies that all common credential properties are removed from connection strings.
        /// </summary>
        /// <remarks>
        /// <para>Tests the allowlist approach: only known-safe operational properties are retained,
        /// all others (including all credential types) are excluded.</para>
        /// </remarks>
        [Theory]
        [InlineData("Server=db01;Password=secret", "Password")]
        [InlineData("Server=db01;Pwd=secret", "Pwd")]
        [InlineData("Server=db01;User Password=secret", "User Password")]
        [InlineData("Server=db01;AccessToken=abc123", "AccessToken")]
        [InlineData("Server=db01;ClientSecret=xyz789", "ClientSecret")]
        [InlineData("Server=db01;AccountKey=key123", "AccountKey")]
        [InlineData("Server=db01;SharedAccessSignature=sas123", "SharedAccessSignature")]
        [InlineData("Server=db01;ApiKey=apikey123", "ApiKey")]
        [InlineData("Server=db01;Secret=mysecret", "Secret")]
        [InlineData("Server=db01;Token=mytoken", "Token")]
        [InlineData("Server=db01;EncryptionSecret=encsecret", "EncryptionSecret")]
        [InlineData("Server=db01;AccessTokenValue=tokenval", "AccessTokenValue")]
        [InlineData("Server=db01;CustomApiKey=customkey", "CustomApiKey")]
        public void SanitizeConnectionString_RemovesAllCredentialProperties(string connectionString, string credentialProperty)
        {
            // Act
            string? sanitized = ConfigurationFingerprintService.SanitizeConnectionString(connectionString);

            // Assert: Credential property should be removed
            Assert.NotNull(sanitized);
            Assert.DoesNotContain(credentialProperty, sanitized, StringComparison.OrdinalIgnoreCase);
            // Server should still be present (operational property)
            Assert.Contains("Server=db01", sanitized, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Verifies that operational connection string properties are preserved.
        /// </summary>
        /// <remarks>
        /// <para>These are the allowlisted properties that contribute to the fingerprint:</para>
        /// <list type="bullet">
        /// <item><description>Infrastructure: Server, Host, Address, Port</description></item>
        /// <item><description>Database: Database, Initial Catalog</description></item>
        /// <item><description>Identity: User ID, Username (usernames are operational)</description></item>
        /// <item><description>Application: Application Name</description></item>
        /// <item><description>Timeouts: Connect Timeout, Command Timeout, Connection Timeout</description></item>
        /// <item><description>Pooling: Pooling, Min Pool Size, Max Pool Size</description></item>
        /// <item><description>Security modes: Encrypt, TrustServerCertificate, Integrated Security</description></item>
        /// </list>
        /// </remarks>
        [Fact]
        public void SanitizeConnectionString_PreservesOperationalProperties()
        {
            // Arrange: Connection string with many operational properties and credentials
            /// <summary>
            /// Supplies original for the fixture or scenario under test.
            /// </summary>
            const string Original =
                "Server=db01.example.com;" +
                "Port=5432;" +
                "Database=MyDatabase;" +
                "User ID=appuser;" +
                "Password=secret123;" +
                "Application Name=MyApp;" +
                "Connect Timeout=30;" +
                "Pooling=true;" +
                "Min Pool Size=5;" +
                "Max Pool Size=50;" +
                "Encrypt=true;" +
                "TrustServerCertificate=false;" +
                "AccessToken=shouldBeRemoved";

            // Act
            string? sanitized = ConfigurationFingerprintService.SanitizeConnectionString(Original);

            // Assert: All operational properties retained
            Assert.NotNull(sanitized);
            Assert.Contains("Server=db01.example.com", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Port=5432", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Database=MyDatabase", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Username=appuser", sanitized, StringComparison.OrdinalIgnoreCase); // Normalized from "User ID"
            Assert.Contains("Application Name=MyApp", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Connection Timeout=30", sanitized, StringComparison.OrdinalIgnoreCase); // Normalized from "Connect Timeout"
            Assert.Contains("Pooling=true", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Min Pool Size=5", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Max Pool Size=50", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Encrypt=true", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("TrustServerCertificate=false", sanitized, StringComparison.OrdinalIgnoreCase);

            // Credentials removed
            Assert.DoesNotContain("Password", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("secret123", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("AccessToken", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("shouldBeRemoved", sanitized, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Exercises sanitize connection string  handles quoted credentials with special characters behavior, including the expected result and failure semantics.
        /// </summary>
        /// <remarks>
        /// <para>This is the exact edge case that naive string splitting would handle incorrectly.
        /// DbConnectionStringBuilder properly parses: Password="abc;123" as a single value.</para>
        /// </remarks>
        [Theory]
        [InlineData("Server=db01;Password=\"abc;123\";Database=NNTP", "abc;123")]
        [InlineData("Server=db01;Password=\"value=with=equals\";Database=NNTP", "value=with=equals")]
        [InlineData("Server=db01;Password=\"value;with;multiple;semicolons\";Database=NNTP", "value;with;multiple;semicolons")]
        [InlineData("Server=db01;Pwd=\"quoted=value;123\";Port=5432", "quoted=value;123")]
        public void SanitizeConnectionString_HandlesQuotedCredentialsWithSpecialCharacters(string connectionString, string secretValue)
        {
            // Act
            string? sanitized = ConfigurationFingerprintService.SanitizeConnectionString(connectionString);

            // Assert: Quoted credential value should be completely removed
            Assert.NotNull(sanitized);
            Assert.DoesNotContain(secretValue, sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Password", sanitized, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Pwd", sanitized, StringComparison.OrdinalIgnoreCase);
            // Operational properties preserved
            Assert.Contains("Server=db01", sanitized, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Verifies that connection strings with different property ordering produce identical sanitized output.
        /// </summary>
        /// <remarks>
        /// <para>Canonicalization ensures fingerprint stability: operationally equivalent connection strings
        /// should produce identical fingerprints regardless of property ordering.</para>
        /// </remarks>
        [Fact]
        public void SanitizeConnectionString_NormalizesPropertyOrdering()
        {
            // Arrange: Same properties in different orders
            /// <summary>
            /// Supplies connection string1 for the fixture or scenario under test.
            /// </summary>
            const string ConnectionString1 = "Server=db01;Database=NNTP;Port=5432";
            /// <summary>
            /// Supplies connection string2 for the fixture or scenario under test.
            /// </summary>
            const string ConnectionString2 = "Port=5432;Database=NNTP;Server=db01";
            /// <summary>
            /// Supplies connection string3 for the fixture or scenario under test.
            /// </summary>
            const string ConnectionString3 = "Database=NNTP;Port=5432;Server=db01";

            // Act
            string? sanitized1 = ConfigurationFingerprintService.SanitizeConnectionString(ConnectionString1);
            string? sanitized2 = ConfigurationFingerprintService.SanitizeConnectionString(ConnectionString2);
            string? sanitized3 = ConfigurationFingerprintService.SanitizeConnectionString(ConnectionString3);

            // Assert: All three should produce identical sanitized strings
            Assert.NotNull(sanitized1);
            Assert.NotNull(sanitized2);
            Assert.NotNull(sanitized3);
            Assert.Equal(sanitized1, sanitized2);
            Assert.Equal(sanitized1, sanitized3);
        }

        /// <summary>
        /// Verifies that connection string property aliases are normalized to canonical forms.
        /// </summary>
        /// <remarks>
        /// <para>Alias normalization ensures: "Server=db01" ≡ "Data Source=db01" ≡ "Host=db01"</para>
        /// <para>This prevents false fingerprint differences when administrators use different syntax.</para>
        /// </remarks>
        [Theory]
        [InlineData("Server=db01", "Data Source=db01")]
        [InlineData("Server=db01", "DataSource=db01")]
        [InlineData("Server=db01", "Host=db01")]
        [InlineData("Server=db01", "Address=db01")]
        [InlineData("Database=MyDB", "Initial Catalog=MyDB")]
        [InlineData("Database=MyDB", "InitialCatalog=MyDB")]
        [InlineData("Username=user1", "User ID=user1")]
        [InlineData("Username=user1", "UserID=user1")]
        [InlineData("Username=user1", "UID=user1")]
        [InlineData("Port=5432", "Server Port=5432")]
        [InlineData("Application Name=MyApp", "ApplicationName=MyApp")]
        [InlineData("Connection Timeout=30", "ConnectTimeout=30")]
        [InlineData("Connection Timeout=30", "Timeout=30")]
        [InlineData("Max Pool Size=50", "MaxPoolSize=50")]
        [InlineData("Max Pool Size=50", "Maximum Pool Size=50")]
        [InlineData("Min Pool Size=5", "MinPoolSize=5")]
        [InlineData("Min Pool Size=5", "Minimum Pool Size=5")]
        [InlineData("TrustServerCertificate=false", "Trust Server Certificate=false")]
        [InlineData("Integrated Security=true", "IntegratedSecurity=true")]
        [InlineData("MultipleActiveResultSets=true", "Multiple Active Result Sets=true")]
        public void SanitizeConnectionString_NormalizesAliasesToCanonicalForm(string canonical, string aliasForm)
        {
            // Act
            string? sanitizedCanonical = ConfigurationFingerprintService.SanitizeConnectionString(canonical);
            string? sanitizedAlias = ConfigurationFingerprintService.SanitizeConnectionString(aliasForm);

            // Assert: Both forms should produce identical sanitized output
            Assert.NotNull(sanitizedCanonical);
            Assert.NotNull(sanitizedAlias);
            Assert.Equal(sanitizedCanonical, sanitizedAlias);
        }

        /// <summary>
        /// Verifies comprehensive canonicalization: aliases + ordering + credentials combined.
        /// </summary>
        /// <remarks>
        /// <para>This is the critical deployment comparison use case: administrators may write
        /// connection strings with different syntax, ordering, and passwords. The fingerprint
        /// should be identical if the effective endpoint configuration matches.</para>
        /// </remarks>
        [Fact]
        public void SanitizeConnectionString_CombinesAliasNormalizationAndOrdering()
        {
            // Arrange: Operationally identical connection strings with different syntax/ordering/passwords
            /// <summary>
            /// Supplies admin1 for the fixture or scenario under test.
            /// </summary>
            const string Admin1 = "Server=db01;Database=NNTP;User ID=app;Password=secret1;Port=5432";
            /// <summary>
            /// Supplies admin2 for the fixture or scenario under test.
            /// </summary>
            const string Admin2 = "Data Source=db01;Initial Catalog=NNTP;Username=app;Password=secret2;Server Port=5432";
            /// <summary>
            /// Supplies admin3 for the fixture or scenario under test.
            /// </summary>
            const string Admin3 = "Port=5432;Host=db01;UID=app;Password=secret3;InitialCatalog=NNTP";

            // Act
            string? sanitized1 = ConfigurationFingerprintService.SanitizeConnectionString(Admin1);
            string? sanitized2 = ConfigurationFingerprintService.SanitizeConnectionString(Admin2);
            string? sanitized3 = ConfigurationFingerprintService.SanitizeConnectionString(Admin3);

            // Assert: All three should produce identical fingerprints (different passwords ignored)
            Assert.NotNull(sanitized1);
            Assert.NotNull(sanitized2);
            Assert.NotNull(sanitized3);
            Assert.Equal(sanitized1, sanitized2);
            Assert.Equal(sanitized1, sanitized3);

            // Verify credentials removed
            Assert.DoesNotContain("secret1", sanitized1);
            Assert.DoesNotContain("secret2", sanitized2);
            Assert.DoesNotContain("secret3", sanitized3);
        }

        /// <summary>
        /// Verifies that sensitive configuration keys are detected using segment-based matching.
        /// </summary>
        /// <remarks>
        /// <para>Tests cover the exact cases from the security specification:</para>
        /// <list type="bullet">
        /// <item><description>Exact matches: Password, ApiKey, SigningKey, Credentials</description></item>
        /// <item><description>Suffix matches: MyPassword, DatabasePassword, CustomApiKey</description></item>
        /// <item><description>False positives avoided: MonkeyCount, TurkeyMode, KeyPerformanceMetrics</description></item>
        /// <item><description>Prefix not sensitive: PasswordPolicy (pattern must be suffix)</description></item>
        /// </list>
        /// </remarks>
        [Theory]
        // Exact matches (base patterns)
        [InlineData("Database:Password", true)]
        [InlineData("Auth:ApiKey", true)]
        [InlineData("Jwt:SigningKey", true)]
        [InlineData("Credentials", true)]
        [InlineData("MySecret", true)]
        [InlineData("Auth:Token", true)]
        [InlineData("Database:Pwd", true)]
        [InlineData("SMTP:Passwd", true)]
        // Suffix matches (compound names ending with sensitive pattern)
        [InlineData("MyPassword", true)]
        [InlineData("DatabasePassword", true)]
        [InlineData("CustomApiKey", true)]
        [InlineData("JwtSigningKey", true)]
        [InlineData("Database:CustomSecret", true)]
        [InlineData("Auth:BearerToken", true)]
        [InlineData("Database:AdminPwd", true)]
        [InlineData("LDAP:BindPasswd", true)]
        [InlineData("OAuth:ClientSecret", true)]
        [InlineData("Azure:AccessKey", true)]
        [InlineData("AWS:SecretKey", true)]
        [InlineData("Auth:PrivateKey", true)]
        [InlineData("Encryption:EncryptionKey", true)]
        // False positives avoided (no segment match, pattern appears in middle/prefix)
        [InlineData("MonkeyCount", false)]          // "key" in middle, not suffix
        [InlineData("TurkeyMode", false)]           // "key" in middle, not suffix
        [InlineData("KeyPerformanceMetrics", false)] // "key" as prefix, not suffix
        [InlineData("PasswordPolicy", false)]        // "password" as prefix, not suffix
        [InlineData("SecretConfiguration", false)]   // "secret" as prefix, not suffix
        [InlineData("TokenBucket", false)]           // "token" as prefix, not suffix
        // Operational/Non-sensitive keys
        /// <summary>
        /// Exercises is sensitive configuration key  uses segment based matching behavior, including the expected result and failure semantics.
        /// </summary>
        [InlineData("Database:Host", false)]
        [InlineData("Database:Port", false)]
        [InlineData("Database:Name", false)]
        [InlineData("Database:Username", false)]     // Username is operational, not secret
        [InlineData("Application:Name", false)]
        [InlineData("Logging:Level", false)]
        public void IsSensitiveConfigurationKey_UsesSegmentBasedMatching(string key, bool expectedSensitive)
        {
            // Act
            bool result = ConfigurationFingerprintService.IsSensitiveConfigurationKey(key);

            // Assert
            Assert.Equal(expectedSensitive, result);
        }

        /// <summary>
        /// Verifies that fingerprints are deterministic (same input = same output).
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_IsDeterministic()
        {
            // Arrange: Same configuration
            Dictionary<string, string?> configData = new()
            {
                ["NNTP:Host"] = "news.example.com",
                ["NNTP:Port"] = "119",
                ["NNTP:Username"] = "testuser"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(configData).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(configData).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Same configuration should produce identical fingerprints
            Assert.Equal(fingerprint1, fingerprint2);
            Assert.Equal(19, fingerprint1.Length); // "v1:" + 16-char hex = 19 chars
            Assert.StartsWith("v1:", fingerprint1); // Version prefix
        }

        /// <summary>
        /// Verifies that fingerprints include algorithm version prefix.
        /// </summary>
        /// <remarks>
        /// <para>Version prefix enables detection of algorithm changes. Format is "v1:8F7A9C2E41B7D903"
        /// where "v1:" is the algorithm version identifier.</para>
        /// <para>This test ensures the version is present and properly formatted.</para>
        /// </remarks>
        [Fact]
        public void CalculateConfigurationFingerprint_IncludesVersionPrefix()
        {
            // Arrange
            Dictionary<string, string?> configData = new()
            {
                ["TestKey"] = "TestValue"
            };

            IConfiguration config = new ConfigurationBuilder().AddInMemoryCollection(configData).Build();

            // Act
            string fingerprint = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config);

            // Assert
            Assert.StartsWith("v1:", fingerprint);
            Assert.Equal(19, fingerprint.Length); // "v1:" (3) + 16 hex chars (16) = 19
            Assert.Matches(@"^v1:[0-9a-f]{16}$", fingerprint); // Regex: v1: followed by 16 lowercase hex chars
        }

        /// <summary>
        /// Verifies that different non-secret configuration produces different fingerprints.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_DifferentConfigProducesDifferentFingerprint()
        {
            // Arrange
            Dictionary<string, string?> config1Data = new()
            {
                ["NNTP:Host"] = "news1.example.com"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["NNTP:Host"] = "news2.example.com"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Different hosts should produce different fingerprints
            Assert.NotEqual(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that secret changes do NOT affect fingerprint (intentional behavior).
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_SecretChangesDoNotAffectFingerprint()
        {
            // Arrange: Same non-secret config, different passwords
            Dictionary<string, string?> config1Data = new()
            {
                ["NNTP:Host"] = "news.example.com",
                ["NNTP:Username"] = "user",
                ["NNTP:Password"] = "password123"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["NNTP:Host"] = "news.example.com",
                ["NNTP:Username"] = "user",
                ["NNTP:Password"] = "differentPassword456"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Password change should NOT affect fingerprint (secrets excluded)
            Assert.Equal(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that connection string credential changes do NOT affect fingerprint,
        /// but operational property changes DO affect it.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_ConnectionStringCredentialChangesIgnored_OperationalChangesDetected()
        {
            // Arrange: Same server/database, different password
            Dictionary<string, string?> config1Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Database=NNTP;User ID=foo;Password=secret123"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Database=NNTP;User ID=foo;Password=differentSecret"
            };

            Dictionary<string, string?> config3Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db02;Database=NNTP;User ID=foo;Password=secret123"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();
            IConfiguration config3 = new ConfigurationBuilder().AddInMemoryCollection(config3Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);
            string fingerprint3 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config3);

            // Assert
            // Password change should NOT affect fingerprint
            Assert.Equal(fingerprint1, fingerprint2);

            // Server change SHOULD affect fingerprint
            Assert.NotEqual(fingerprint1, fingerprint3);
        }

        /// <summary>
        /// Verifies that API key changes do NOT affect fingerprint.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_ApiKeyChangesDoNotAffectFingerprint()
        {
            // Arrange
            Dictionary<string, string?> config1Data = new()
            {
                ["Service:Endpoint"] = "https://api.example.com",
                ["Service:ApiKey"] = "key123"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["Service:Endpoint"] = "https://api.example.com",
                ["Service:ApiKey"] = "key456"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: API key change should NOT affect fingerprint
            Assert.Equal(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that database server changes produce different fingerprints.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_DatabaseServerChangesProduceDifferentFingerprint()
        {
            // Arrange
            Dictionary<string, string?> config1Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Database=NNTP"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db02;Database=NNTP"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Different server should produce different fingerprint
            Assert.NotEqual(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that database name changes produce different fingerprints.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_DatabaseNameChangesProduceDifferentFingerprint()
        {
            // Arrange
            Dictionary<string, string?> config1Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Database=Production"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Database=Staging"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Different database should produce different fingerprint
            Assert.NotEqual(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that username changes produce different fingerprints.
        /// </summary>
        /// <remarks>
        /// <para>Usernames are operational configuration (not secrets), so changes should affect the fingerprint.
        /// This distinguishes them from passwords/tokens which are true secrets.</para>
        /// </remarks>
        [Fact]
        public void CalculateConfigurationFingerprint_UsernameChangesProduceDifferentFingerprint()
        {
            // Arrange: Different usernames (operational config)
            Dictionary<string, string?> config1Data = new()
            {
                ["NNTP:Host"] = "news.example.com",
                ["NNTP:Username"] = "user1"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["NNTP:Host"] = "news.example.com",
                ["NNTP:Username"] = "user2"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Different username should produce different fingerprint
            Assert.NotEqual(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that configuration key ordering does not affect fingerprint.
        /// </summary>
        /// <remarks>
        /// <para>Dictionary insertion order shouldn't matter - the canonical format sorts keys
        /// to ensure deterministic output regardless of how configuration sources are ordered.</para>
        /// </remarks>
        [Fact]
        public void CalculateConfigurationFingerprint_OrderingDoesNotAffectFingerprint()
        {
            // Arrange: Same configuration, different insertion order
            Dictionary<string, string?> config1Data = new()
            {
                ["NNTP:Host"] = "news.example.com",
                ["NNTP:Port"] = "119",
                ["NNTP:Username"] = "testuser"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["NNTP:Username"] = "testuser",
                ["NNTP:Host"] = "news.example.com",
                ["NNTP:Port"] = "119"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Order should NOT affect fingerprint
            Assert.Equal(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that malformed connection strings are excluded from the fingerprint rather than
        /// converted to an empty value that could create false equivalence.
        /// </summary>
        /// <remarks>
        /// <para>Critical behavior: When connection string parsing fails, <c>SanitizeConnectionString</c>
        /// returns <c>null</c> and the key-value pair is excluded from the fingerprint entirely.</para>
        /// 
        /// <para>This prevents false equivalence where different malformed strings would all become
        /// empty and incorrectly match. Instead, malformed strings are simply excluded, and the
        /// fingerprint is based on the remaining valid configuration.</para>
        /// 
        /// <para>Test cases:</para>
        /// <list type="bullet">
        /// <item><description>config1 and config2: Different malformed connection strings, identical other config
        /// → Both exclude the malformed connection string → Fingerprints match (based on identical other config)</description></item>
        /// <item><description>config1 and config3: Same malformed connection string, different other config
        /// → Fingerprints differ (based on different other config)</description></item>
        /// </list>
        /// </remarks>
        [Fact]
        public void CalculateConfigurationFingerprint_MalformedConnectionStrings_AreExcluded()
        {
            // Arrange: Two different malformed connection strings
            Dictionary<string, string?> config1Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Password=\"unclosed1",
                ["OtherConfig"] = "value1"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db02;Password=\"unclosed2",
                ["OtherConfig"] = "value1"
            };

            Dictionary<string, string?> config3Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Password=\"unclosed1",
                ["OtherConfig"] = "value2"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();
            IConfiguration config3 = new ConfigurationBuilder().AddInMemoryCollection(config3Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);
            string fingerprint3 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config3);

            // Assert: All should produce valid fingerprints (not UNAVAILABLE)
            Assert.NotEqual("UNAVAILABLE", fingerprint1);
            Assert.NotEqual("UNAVAILABLE", fingerprint2);
            Assert.NotEqual("UNAVAILABLE", fingerprint3);

            // Malformed connection strings are excluded (both return null), so if other config is identical,
            // fingerprints should match
            Assert.Equal(fingerprint1, fingerprint2);

            // But if other config differs, fingerprints should differ
            Assert.NotEqual(fingerprint1, fingerprint3);
        }

        /// <summary>
        /// Verifies that malformed connection strings (excluded) produce different fingerprints than
        /// valid connection strings (sanitized and included).
        /// </summary>
        /// <remarks>
        /// <para>This is the critical regression test: a malformed connection string is excluded entirely,
        /// while a valid connection string contributes its sanitized allowlisted properties to the fingerprint.</para>
        /// 
        /// <para>Example:</para>
        /// <list type="bullet">
        /// <item><description>Malformed: "Server=db01;Password=\"unclosed" → excluded (null) → contributes nothing</description></item>
        /// <item><description>Valid: "Server=db01;Password=secret" → sanitized to "Server=db01" → contributes to fingerprint</description></item>
        /// </list>
        /// 
        /// <para>Since the valid connection string contributes operational properties (Server=db01) and the
        /// malformed one contributes nothing, the fingerprints MUST differ even if all other config is identical.</para>
        /// 
        /// <para>This prevents a subtle bug where malformed strings would be treated as "empty" and accidentally
        /// match anything.</para>
        /// </remarks>
        [Fact]
        public void CalculateConfigurationFingerprint_MalformedVsValid_ProduceDifferentFingerprints()
        {
            // Arrange: Malformed connection string vs. valid connection string with identical other config
            Dictionary<string, string?> configMalformedData = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Password=\"unclosed", // Malformed (unclosed quote)
                ["OtherConfig"] = "same"
            };

            Dictionary<string, string?> configValidData = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Password=secret", // Valid (will sanitize to Server=db01)
                ["OtherConfig"] = "same"
            };

            IConfiguration configMalformed = new ConfigurationBuilder().AddInMemoryCollection(configMalformedData).Build();
            IConfiguration configValid = new ConfigurationBuilder().AddInMemoryCollection(configValidData).Build();

            // Act
            string fingerprintMalformed = ConfigurationFingerprintService.CalculateConfigurationFingerprint(configMalformed);
            string fingerprintValid = ConfigurationFingerprintService.CalculateConfigurationFingerprint(configValid);

            // Assert: Both should produce valid fingerprints (not UNAVAILABLE)
            Assert.NotEqual("UNAVAILABLE", fingerprintMalformed);
            Assert.NotEqual("UNAVAILABLE", fingerprintValid);

            // Critical: Malformed is excluded (contributes nothing), valid contributes "Server=db01"
            // Therefore fingerprints MUST differ
            Assert.NotEqual(fingerprintMalformed, fingerprintValid);
        }

        /// <summary>
        /// Verifies that connection string property ordering does not affect the fingerprint (end-to-end test).
        /// </summary>
        /// <remarks>
        /// <para>This is the critical end-to-end canonicalization test. Connection strings with identical
        /// properties but different ordering must produce identical fingerprints because:</para>
        /// <list type="number">
        /// <item><description><c>SanitizeConnectionString</c> normalizes aliases to canonical forms</description></item>
        /// <item><description><c>SanitizeConnectionString</c> sorts properties by canonical key name</description></item>
        /// <item><description>The fingerprint then hashes this canonical representation</description></item>
        /// </list>
        /// 
        /// <para>This test validates the complete pipeline: configuration → detection → sanitization → 
        /// canonicalization → fingerprinting.</para>
        /// 
        /// <para>Without proper canonicalization, <c>DbConnectionStringBuilder.ConnectionString</c> could
        /// return properties in original input order, causing semantically identical connection strings
        /// to produce different fingerprints.</para>
        /// </remarks>
        [Fact]
        public void CalculateConfigurationFingerprint_ConnectionStringPropertyOrderingDoesNotMatter()
        {
            // Arrange: Same properties, different ordering
            /// <summary>
            /// Supplies connection1 for the fixture or scenario under test.
            /// </summary>
            const string Connection1 = "Server=db01;Database=NNTP;Port=5432;User ID=app";
            /// <summary>
            /// Supplies connection2 for the fixture or scenario under test.
            /// </summary>
            const string Connection2 = "User ID=app;Port=5432;Database=NNTP;Server=db01";

            IConfiguration config1 = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Main"] = Connection1
                })
                .Build();

            IConfiguration config2 = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Main"] = Connection2
                })
                .Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Property ordering should not affect fingerprint
            Assert.Equal(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that connection string property aliases are normalized to canonical forms (end-to-end test).
        /// </summary>
        /// <remarks>
        /// <para>This is the critical end-to-end alias normalization test. Connection strings using different
        /// property name aliases but identical values must produce identical fingerprints because:</para>
        /// <list type="number">
        /// <item><description><c>SanitizeConnectionString</c> maps aliases to canonical forms (e.g., "Data Source" → "Server")</description></item>
        /// <item><description><c>SanitizeConnectionString</c> sorts properties by canonical key name</description></item>
        /// <item><description>The fingerprint then hashes this canonical representation</description></item>
        /// </list>
        /// 
        /// <para>Test examples:</para>
        /// <list type="bullet">
        /// <item><description>"Server=db01" ≡ "Data Source=db01" ≡ "Host=db01" (all normalize to Server=db01)</description></item>
        /// <item><description>"Database=NNTP" ≡ "Initial Catalog=NNTP" (both normalize to Database=NNTP)</description></item>
        /// <item><description>"User ID=app" ≡ "Username=app" (both normalize to Username=app)</description></item>
        /// </list>
        /// 
        /// <para><b>Design decision:</b> Aliases are treated as semantically equivalent for deployment comparison.
        /// This ensures that administrators can use their preferred syntax (SQL Server vs. PostgreSQL conventions,
        /// for example) without causing false fingerprint mismatches. The fingerprint reflects operational
        /// configuration identity, not syntactic representation.</para>
        /// </remarks>
        [Fact]
        public void CalculateConfigurationFingerprint_ConnectionStringAliasesAreEquivalent()
        {
            // Arrange: Multiple alias combinations representing the same operational configuration
            /// <summary>
            /// Supplies canonical for the fixture or scenario under test.
            /// </summary>
            const string Canonical = "Server=db01;Database=NNTP;Username=app;Port=5432";
            /// <summary>
            /// Supplies sql server style for the fixture or scenario under test.
            /// </summary>
            const string SqlServerStyle = "Data Source=db01;Initial Catalog=NNTP;User ID=app;Server Port=5432";
            /// <summary>
            /// Supplies postgres style for the fixture or scenario under test.
            /// </summary>
            const string PostgresStyle = "Host=db01;Database=NNTP;Username=app;Port=5432";
            /// <summary>
            /// Supplies compact style for the fixture or scenario under test.
            /// </summary>
            const string CompactStyle = "DataSource=db01;InitialCatalog=NNTP;UID=app;Port=5432";

            IConfiguration configCanonical = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Main"] = Canonical
                })
                .Build();

            IConfiguration configSqlServer = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Main"] = SqlServerStyle
                })
                .Build();

            IConfiguration configPostgres = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Main"] = PostgresStyle
                })
                .Build();

            IConfiguration configCompact = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Main"] = CompactStyle
                })
                .Build();

            // Act
            string fingerprintCanonical = ConfigurationFingerprintService.CalculateConfigurationFingerprint(configCanonical);
            string fingerprintSqlServer = ConfigurationFingerprintService.CalculateConfigurationFingerprint(configSqlServer);
            string fingerprintPostgres = ConfigurationFingerprintService.CalculateConfigurationFingerprint(configPostgres);
            string fingerprintCompact = ConfigurationFingerprintService.CalculateConfigurationFingerprint(configCompact);

            // Assert: All aliases should produce identical fingerprints
            Assert.Equal(fingerprintCanonical, fingerprintSqlServer);
            Assert.Equal(fingerprintCanonical, fingerprintPostgres);
            Assert.Equal(fingerprintCanonical, fingerprintCompact);
        }

        /// <summary>
        /// Verifies that connection string port changes produce different fingerprints.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_ConnectionStringPortChangesProduceDifferentFingerprint()
        {
            // Arrange
            Dictionary<string, string?> config1Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Port=5432;Database=NNTP"
            };

            Dictionary<string, string?> config2Data = new()
            {
                ["ConnectionStrings:Main"] = "Server=db01;Port=5433;Database=NNTP"
            };

            IConfiguration config1 = new ConfigurationBuilder().AddInMemoryCollection(config1Data).Build();
            IConfiguration config2 = new ConfigurationBuilder().AddInMemoryCollection(config2Data).Build();

            // Act
            string fingerprint1 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config1);
            string fingerprint2 = ConfigurationFingerprintService.CalculateConfigurationFingerprint(config2);

            // Assert: Different port should produce different fingerprint
            Assert.NotEqual(fingerprint1, fingerprint2);
        }

        /// <summary>
        /// Verifies that DNS suffix canonicalization parity is preserved for fingerprinting.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_DnsSuffixCanonicalizationParity_ProducesSameFingerprint()
        {
            // Arrange: Runtime treats these as equivalent via BackFillerIdentityValidator.CanonicalizeDnsSuffix.
            Dictionary<string, string?> configCanonical = new()
            {
                ["BackFiller:DnsSuffix"] = "example.com"
            };

            Dictionary<string, string?> configRawEquivalent = new()
            {
                ["BackFiller:DnsSuffix"] = " Example.COM. "
            };

            IConfiguration canonicalConfiguration = new ConfigurationBuilder().AddInMemoryCollection(configCanonical).Build();
            IConfiguration rawEquivalentConfiguration = new ConfigurationBuilder().AddInMemoryCollection(configRawEquivalent).Build();

            // Act
            string canonicalFingerprint = ConfigurationFingerprintService.CalculateConfigurationFingerprint(canonicalConfiguration);
            string rawEquivalentFingerprint = ConfigurationFingerprintService.CalculateConfigurationFingerprint(rawEquivalentConfiguration);

            // Assert: Operationally equivalent runtime values should have matching fingerprints.
            Assert.Equal(canonicalFingerprint, rawEquivalentFingerprint);
        }

        /// <summary>
        /// Verifies complex real-world scenario: multiple configuration changes.
        /// </summary>
        [Fact]
        public void CalculateConfigurationFingerprint_ComplexRealWorldScenario()
        {
            // Arrange: Production vs Staging environment configs
            Dictionary<string, string?> productionConfig = new()
            {
                ["ConnectionStrings:Main"] = "Server=prod-db01;Database=NNTP;User ID=app;Password=prodSecret123;Port=5432",
                ["NNTP:Host"] = "news.prod.example.com",
                ["NNTP:Port"] = "119",
                ["NNTP:Username"] = "produser",
                ["NNTP:Password"] = "prodNNTPpass",
                ["Logging:Level"] = "Warning",
                ["Auth:ApiKey"] = "prodApiKey123"
            };

            Dictionary<string, string?> stagingConfig = new()
            {
                ["ConnectionStrings:Main"] = "Server=staging-db01;Database=NNTP;User ID=app;Password=stagingSecret456;Port=5432",
                ["NNTP:Host"] = "news.staging.example.com",
                ["NNTP:Port"] = "119",
                ["NNTP:Username"] = "staginguser",
                ["NNTP:Password"] = "stagingNNTPpass",
                ["Logging:Level"] = "Information",
                ["Auth:ApiKey"] = "stagingApiKey456"
            };

            Dictionary<string, string?> productionSameSecretsConfig = new()
            {
                ["ConnectionStrings:Main"] = "Server=prod-db01;Database=NNTP;User ID=app;Password=DIFFERENT_SECRET;Port=5432",
                ["NNTP:Host"] = "news.prod.example.com",
                ["NNTP:Port"] = "119",
                ["NNTP:Username"] = "produser",
                ["NNTP:Password"] = "DIFFERENT_NNTP_PASS",
                ["Logging:Level"] = "Warning",
                ["Auth:ApiKey"] = "DIFFERENT_API_KEY"
            };

            IConfiguration prodConfig = new ConfigurationBuilder().AddInMemoryCollection(productionConfig).Build();
            IConfiguration stagConfig = new ConfigurationBuilder().AddInMemoryCollection(stagingConfig).Build();
            IConfiguration prodSameConfig = new ConfigurationBuilder().AddInMemoryCollection(productionSameSecretsConfig).Build();

            // Act
            string prodFingerprint = ConfigurationFingerprintService.CalculateConfigurationFingerprint(prodConfig);
            string stagFingerprint = ConfigurationFingerprintService.CalculateConfigurationFingerprint(stagConfig);
            string prodSameFingerprint = ConfigurationFingerprintService.CalculateConfigurationFingerprint(prodSameConfig);

            // Assert
            // Production and staging have different operational config → different fingerprints
            Assert.NotEqual(prodFingerprint, stagFingerprint);

            // Production with different secrets but same operational config → SAME fingerprint
            Assert.Equal(prodFingerprint, prodSameFingerprint);
        }

        /// <summary>
        /// SECURITY: Compile-time verification that ConfigurationFingerprintService never calls TryGetPassword().
        /// </summary>
        /// <remarks>
        /// <para>This test reads the source file and ensures the critical security boundary is maintained:</para>
        /// <para><b>TryGetPassword() MUST NEVER be called in fingerprinting code.</b></para>
        /// <para>Fingerprints intentionally exclude passwords. If this test fails, a security violation was introduced.</para>
        /// </remarks>
        [Fact]
        public void FingerprintCode_NeverCallsTryGetPassword()
        {
            // Arrange: Read the fingerprint source file
            string fingerprintSourcePath = ResolveConfigurationFingerprintSourcePath();

            // Act: Read source code and strip comments/strings to avoid false positives
            string sourceCode = File.ReadAllText(fingerprintSourcePath);

            // Remove single-line comments
            sourceCode = System.Text.RegularExpressions.Regex.Replace(
                sourceCode,
                @"//.*$",
                "",
                System.Text.RegularExpressions.RegexOptions.Multiline
            );

            // Remove multi-line comments
            sourceCode = System.Text.RegularExpressions.Regex.Replace(
                sourceCode,
                @"/\*.*?\*/",
                "",
                System.Text.RegularExpressions.RegexOptions.Singleline
            );

            // Assert: SECURITY CRITICAL - TryGetPassword must NEVER appear in actual code
            // (Comments have been stripped, so this only catches real method calls)
            Assert.DoesNotContain("TryGetPassword", sourceCode, StringComparison.Ordinal);
        }

        /// <summary>
        /// Exercises resolve configuration fingerprint source path behavior, including the expected result and failure semantics.
        /// </summary>
        private static string ResolveConfigurationFingerprintSourcePath()
        {
            /// <summary>
            /// Supplies solution marker for the fixture or scenario under test.
            /// </summary>
            const string SolutionMarker = "VectorNNTP.BackFiller.slnx";
            string? current = AppContext.BaseDirectory;

            while (!string.IsNullOrWhiteSpace(current))
            {
                string markerPath = Path.Combine(current, SolutionMarker);
                if (File.Exists(markerPath))
                {
                    string sourcePath = Path.Combine(
                        current,
                        "VectorNNTP.BackFiller",
                        "Startup",
                        "Configuration",
                        "ConfigurationFingerprintService.cs");

                    if (File.Exists(sourcePath))
                    {
                        return sourcePath;
                    }

                    break;
                }

                DirectoryInfo? parent = Directory.GetParent(current);
                current = parent?.FullName;
            }

            throw new DirectoryNotFoundException("Could not locate repository root for ConfigurationFingerprintService source contract test.");
        }
    }

}
