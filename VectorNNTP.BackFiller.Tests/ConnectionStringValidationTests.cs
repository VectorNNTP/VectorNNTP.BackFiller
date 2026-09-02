// <copyright file="ConnectionStringValidationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for connection string validation.

using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Documents the ConnectionStringValidationTests test type and its protected contract.
    /// </summary>
    public sealed class ConnectionStringValidationTests
    {
        #region Parameter Validation Tests
        /// <summary>
        /// Verifies the Validate_NullSettingName_Throws scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_NullSettingName_Throws()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret";
            string? settingName = null;

            // Act & Assert
            // ArgumentException.ThrowIfNullOrWhiteSpace throws ArgumentNullException for null
            ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() =>
                ConnectionStringValidator.Validate(connectionString, settingName!));

            Assert.Equal("settingName", ex.ParamName);
        }
        /// <summary>
        /// Verifies the Validate_EmptySettingName_Throws scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_EmptySettingName_Throws()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret";
            string settingName = string.Empty;

            // Act & Assert
            // ArgumentException.ThrowIfNullOrWhiteSpace throws ArgumentException for empty
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
                ConnectionStringValidator.Validate(connectionString, settingName));

            Assert.Equal("settingName", ex.ParamName);
        }
        /// <summary>
        /// Verifies the Validate_WhitespaceSettingName_Throws scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_WhitespaceSettingName_Throws()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret";
            string settingName = "   \t\n   ";

            // Act & Assert
            // ArgumentException.ThrowIfNullOrWhiteSpace throws ArgumentException for whitespace
            ArgumentException ex = Assert.Throws<ArgumentException>(() =>
                ConnectionStringValidator.Validate(connectionString, settingName));

            Assert.Equal("settingName", ex.ParamName);
        }

        #endregion

        #region Basic Validation Tests
        /// <summary>
        /// Verifies the Validate_NullConnectionString_ReturnsRequiredError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_NullConnectionString_ReturnsRequiredError()
        {
            // Arrange
            string? connectionString = null;

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            _ = Assert.Single(diagnostics);
            Assert.Contains("required", diagnostics[0].Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Verifies the Validate_EmptyConnectionString_ReturnsRequiredError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_EmptyConnectionString_ReturnsRequiredError()
        {
            // Arrange
            string connectionString = string.Empty;

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            _ = Assert.Single(diagnostics);
            Assert.Contains("required", diagnostics[0].Message, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Verifies the Validate_WhitespaceConnectionString_ReturnsRequiredError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_WhitespaceConnectionString_ReturnsRequiredError()
        {
            // Arrange
            string connectionString = "   \t\n   ";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            _ = Assert.Single(diagnostics);
            Assert.Contains("required", diagnostics[0].Message, StringComparison.OrdinalIgnoreCase);
        }

        #endregion

        #region Syntax Validation Tests
        /// <summary>
        /// Verifies the Validate_MalformedConnectionString_ReturnsSyntaxError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_MalformedConnectionString_ReturnsSyntaxError()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=test;this is not valid syntax;;;";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, e => e.Message.Contains("syntax", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_InvalidKeyValuePairs_ReturnsSyntaxError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_InvalidKeyValuePairs_ReturnsSyntaxError()
        {
            // Arrange
            string connectionString = "Server===localhost;;Database";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, e => e.Message.Contains("syntax", StringComparison.OrdinalIgnoreCase));
        }

        #endregion

        #region Required Components Tests - Server/Host
        /// <summary>
        /// Verifies the Validate_MissingServer_ReturnsServerRequiredError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_MissingServer_ReturnsServerRequiredError()
        {
            // Arrange
            string connectionString = "Database=GrabberDB;User ID=admin;Password=pass";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, e => e.Message.Contains("server", StringComparison.OrdinalIgnoreCase)
                                         && e.Message.Contains("host", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_EmptyServer_ReturnsServerError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_EmptyServer_ReturnsServerError()
        {
            // Arrange
            // Note: DbConnectionStringBuilder removes keys with empty values
            string connectionString = "Server=;Database=GrabberDB;User ID=admin;Password=pass";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            // DbConnectionStringBuilder treats "Server=" as missing, not empty
            Assert.Contains(diagnostics, e => e.Message.Contains("server", StringComparison.OrdinalIgnoreCase)
                                         || e.Message.Contains("host", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_ValidServerKeyVariations_AcceptsServerValue scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        [InlineData("Host=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        [InlineData("Data Source=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        [InlineData("DataSource=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        [InlineData("Address=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        [InlineData("Addr=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        [InlineData("Network Address=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        public void Validate_ValidServerKeyVariations_AcceptsServerValue(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.DoesNotContain(diagnostics, e => e.Message.Contains("server", StringComparison.OrdinalIgnoreCase)
                                               || e.Message.Contains("host", StringComparison.OrdinalIgnoreCase));
        }

        #endregion

        #region Required Components Tests - Database
        /// <summary>
        /// Verifies the Validate_MissingDatabase_ReturnsDatabaseRequiredError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_MissingDatabase_ReturnsDatabaseRequiredError()
        {
            // Arrange
            string connectionString = "Server=localhost;User ID=admin;Password=pass";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, e => e.Message.Contains("database", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_EmptyDatabase_ReturnsDatabaseError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_EmptyDatabase_ReturnsDatabaseError()
        {
            // Arrange
            // Note: DbConnectionStringBuilder removes keys with empty values
            string connectionString = "Server=localhost;Database=;User ID=admin;Password=pass";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            // DbConnectionStringBuilder treats "Database=" as missing, not empty
            Assert.Contains(diagnostics, e => e.Message.Contains("database", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_ValidDatabaseKeyVariations_AcceptsDatabaseValue scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass")]
        [InlineData("Server=localhost;Initial Catalog=GrabberDB;User ID=admin;Password=pass")]
        public void Validate_ValidDatabaseKeyVariations_AcceptsDatabaseValue(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.DoesNotContain(diagnostics, e => e.Message.Contains("database", StringComparison.OrdinalIgnoreCase)
                                               && e.Message.Contains("must specify", StringComparison.OrdinalIgnoreCase));
        }

        #endregion

        #region Authentication Validation Tests
        /// <summary>
        /// Verifies the Validate_MissingAuthentication_ReturnsAuthenticationRequiredError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_MissingAuthentication_ReturnsAuthenticationRequiredError()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=GrabberDB";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, e => e.Message.Contains("user ID", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_UsernamePasswordVariations_AcceptsAuthentication scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=sa;Password=P@ssw0rd")]
        [InlineData("Server=localhost;Database=GrabberDB;UserID=sa;Password=P@ssw0rd")]
        [InlineData("Server=localhost;Database=GrabberDB;Username=admin;Password=secret")]
        [InlineData("Server=localhost;Database=GrabberDB;Uid=admin;Password=secret")]
        [InlineData("Server=localhost;Database=GrabberDB;User name=admin;Password=secret")]
        [InlineData("Server=localhost;Database=GrabberDB;User=admin;Password=secret")]
        public void Validate_UsernamePasswordVariations_AcceptsAuthentication(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.DoesNotContain(diagnostics, e => e.Message.Contains("user ID", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_UsernameWithoutPassword_AcceptsAuthentication scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_UsernameWithoutPassword_AcceptsAuthentication()
        {
            // Arrange
            // MySqlConnector supports authentication where the password/token
            // is supplied programmatically rather than embedded in the connection string.
            // Password in connection string is optional; User ID is required.
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=sa";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            // Should not ERROR on missing password - MySqlConnector supports ProvidePasswordCallback
            Assert.DoesNotContain(diagnostics, e => e.Message.Contains("password", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(diagnostics, e => e.Message.Contains("user ID", StringComparison.OrdinalIgnoreCase));
        }

        #endregion

        #region Connection Pooling Validation Tests
        /// <summary>
        /// Verifies the Validate_ExcessiveMinPoolSize_ReturnsPoolingWarning scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_ExcessiveMinPoolSize_ReturnsPoolingWarning()
        {
            // Arrange
            // Control-plane database should use Min Pool Size 0 or 1
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Min Pool Size=5";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - should be a WARNING, not an error
            ConnectionStringValidationResult? poolWarning = diagnostics
                .FirstOrDefault(e => e.Message.Contains("Min Pool Size", StringComparison.Ordinal)
                                     && e.Message.Contains("control-plane", StringComparison.OrdinalIgnoreCase));

            Assert.NotNull(poolWarning);
            Assert.Equal(ValidationSeverity.Warning, poolWarning.Severity);
        }
        /// <summary>
        /// Verifies the Validate_ExcessiveMaxPoolSize_ReturnsPoolingWarning scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_ExcessiveMaxPoolSize_ReturnsPoolingWarning()
        {
            // Arrange
            // Control-plane database should use Max Pool Size <= 10
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Max Pool Size=100";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - should be a WARNING, not an error
            ConnectionStringValidationResult? poolWarning = diagnostics
                .FirstOrDefault(e => e.Message.Contains("Max Pool Size", StringComparison.Ordinal)
                                     && e.Message.Contains("control-plane", StringComparison.OrdinalIgnoreCase));

            Assert.NotNull(poolWarning);
            Assert.Equal(ValidationSeverity.Warning, poolWarning.Severity);
        }
        /// <summary>
        /// Verifies the Validate_AppropriatePoolSize_AcceptsConfiguration scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Min Pool Size=0")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Min Pool Size=1")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Max Pool Size=5")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Max Pool Size=10")]
        public void Validate_AppropriatePoolSize_AcceptsConfiguration(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.DoesNotContain(diagnostics, e => e.Message.Contains("Pool Size", StringComparison.Ordinal));
        }
        /// <summary>
        /// Verifies the Validate_NoPoolingConfiguration_AcceptsConnectionString scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_NoPoolingConfiguration_AcceptsConnectionString()
        {
            // Arrange
            // If no pooling is configured, use provider defaults (which are usually reasonable)
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=pass";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.DoesNotContain(diagnostics, e => e.Message.Contains("Pool Size", StringComparison.Ordinal));
        }
        /// <summary>
        /// Verifies the Validate_InvalidMinPoolSizeSingleAlias_ReturnsInvalidValueError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_InvalidMinPoolSizeSingleAlias_ReturnsInvalidValueError()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Min Pool Size=-1";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, d =>
                d.Severity == ValidationSeverity.Error &&
                d.Message.Contains("invalid minimum pool size", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(diagnostics, d =>
                d.Message.Contains("conflicting minimum pool size aliases", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_InvalidMaxPoolSizeSingleAlias_ReturnsInvalidValueError scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_InvalidMaxPoolSizeSingleAlias_ReturnsInvalidValueError()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Max Pool Size=-1";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, d =>
                d.Severity == ValidationSeverity.Error &&
                d.Message.Contains("invalid maximum pool size", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(diagnostics, d =>
                d.Message.Contains("conflicting maximum pool size aliases", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_ExcessiveMinPoolSize_AlternativeAliases_ReturnsPoolingWarning scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;MinimumPoolSize=5")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;MINIMUMPOOLSIZE=5")] // Case-insensitive
        public void Validate_ExcessiveMinPoolSize_AlternativeAliases_ReturnsPoolingWarning(string connectionString)
        {
            // Arrange: Test MinimumPoolSize alias (in addition to "Min Pool Size", "MinPoolSize", "Minimum Pool Size")
            // DbConnectionStringBuilder performs case-insensitive key lookups, so MINIMUMPOOLSIZE also resolves correctly.

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - should be a WARNING, not an error
            ConnectionStringValidationResult? poolWarning = diagnostics
                .FirstOrDefault(e => e.Message.Contains("Min Pool Size", StringComparison.Ordinal)
                                     && e.Message.Contains("control-plane", StringComparison.OrdinalIgnoreCase));

            Assert.NotNull(poolWarning);
            Assert.Equal(ValidationSeverity.Warning, poolWarning.Severity);
        }
        /// <summary>
        /// Verifies the Validate_ExcessiveMaxPoolSize_AlternativeAliases_ReturnsPoolingWarning scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;MaximumPoolSize=100")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;MAXIMUMPOOLSIZE=100")] // Case-insensitive
        public void Validate_ExcessiveMaxPoolSize_AlternativeAliases_ReturnsPoolingWarning(string connectionString)
        {
            // Arrange: Test MaximumPoolSize alias (in addition to "Max Pool Size", "MaxPoolSize", "Maximum Pool Size")
            // DbConnectionStringBuilder performs case-insensitive key lookups, so MAXIMUMPOOLSIZE also resolves correctly.

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - should be a WARNING, not an error
            ConnectionStringValidationResult? poolWarning = diagnostics
                .FirstOrDefault(e => e.Message.Contains("Max Pool Size", StringComparison.Ordinal)
                                     && e.Message.Contains("control-plane", StringComparison.OrdinalIgnoreCase));

            Assert.NotNull(poolWarning);
            Assert.Equal(ValidationSeverity.Warning, poolWarning.Severity);
        }

        #endregion

        #region MySQL-Specific Connection String Options Tests
        /// <summary>
        /// Verifies the Validate_MySqlAcceptsSyntacticallyValidCustomPort scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;Port=3306")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;Port=33060")] // X Protocol port - syntactically valid, but operationally wrong for MySqlConnector
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Port=3307")] // Custom port, no password
        public void Validate_MySqlAcceptsSyntacticallyValidCustomPort(string connectionString)
        {
            // Arrange: The static validator accepts any syntactically valid TCP port number.
            // Runtime connectivity testing (Program.Validation.cs) will catch if the server
            // is not actually listening on the specified port or is using an incompatible protocol
            // (e.g., X Protocol on 33060 instead of classic MySQL protocol).

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - Port is optional and doesn't trigger validation errors or warnings.
            // Operational validity (whether the server actually listens on that port with the
            // correct protocol) is determined by runtime connectivity checks.
            Assert.DoesNotContain(diagnostics, e => e.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Verifies the Validate_MySqlWithSslMode_AcceptsConfiguration scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;SslMode=Required")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;SslMode=Preferred")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;SSL Mode=None")]
        public void Validate_MySqlWithSslMode_AcceptsConfiguration(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - SslMode is a valid MySQL option
            Assert.DoesNotContain(diagnostics, e => e.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Verifies the Validate_MySqlWithCharSet_AcceptsConfiguration scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;CharSet=utf8mb4")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;Character Set=utf8")]
        public void Validate_MySqlWithCharSet_AcceptsConfiguration(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - CharSet is a valid MySQL option
            Assert.DoesNotContain(diagnostics, e => e.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Verifies the Validate_MySqlWithTimeouts_AcceptsConfiguration scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;Connection Timeout=30")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;ConnectionTimeout=60")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;Default Command Timeout=120")]
        public void Validate_MySqlWithTimeouts_AcceptsConfiguration(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - Timeout settings are valid MySQL options
            Assert.DoesNotContain(diagnostics, e => e.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Verifies the Validate_MySqlWithAllowUserVariables_AcceptsConfiguration scenario and expected contract.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;Allow User Variables=true")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret;AllowUserVariables=True")]
        public void Validate_MySqlWithAllowUserVariables_AcceptsConfiguration(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - AllowUserVariables is a valid MySQL option
            Assert.DoesNotContain(diagnostics, e => e.Severity == ValidationSeverity.Error);
        }
        /// <summary>
        /// Verifies the Validate_MySqlWithMultipleOptions_AcceptsComprehensiveConfiguration scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_MySqlWithMultipleOptions_AcceptsComprehensiveConfiguration()
        {
            // Arrange
            string connectionString = "Server=db.example.com;Port=3306;Database=GrabberDB;" +
                                    "User ID=grabber;Password=secret;" +
                                    "SslMode=Required;CharSet=utf8mb4;" +
                                    "Connection Timeout=30;Min Pool Size=1;Max Pool Size=5;" +
                                    "Allow User Variables=true";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - All these MySQL options should be accepted without errors
            Assert.DoesNotContain(diagnostics, e => e.Severity == ValidationSeverity.Error);
        }

        #endregion

        #region Comprehensive Valid MySQL Connection Strings Tests

        /// <summary>
        /// Tests well-formed MySQL connection strings using various MySqlConnector alias combinations.
        /// These demonstrate different but equivalent ways to specify the same required properties.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=grabber;Password=secret")]
        [InlineData("Host=localhost;Port=3306;Database=GrabberDB;Username=grabber;Password=secret")]
        [InlineData("Data Source=localhost;Initial Catalog=GrabberDB;UID=grabber;PWD=secret")]
        [InlineData("Server=localhost;Port=3306;Database=GrabberDB;User=grabber;Password=secret")]
        [InlineData("Address=localhost;Database=GrabberDB;User ID=grabber;Password=secret;CharSet=utf8mb4")]
        [InlineData("Network Address=localhost;Initial Catalog=GrabberDB;User name=grabber;Password=secret;SslMode=Required")]
        public void Validate_WellFormedMySqlConnectionStrings_ReturnsNoErrors(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - All these are valid MySQL connection strings with different alias combinations
            Assert.DoesNotContain(diagnostics, d => d.Severity == ValidationSeverity.Error);
        }

        /// <summary>
        /// Tests that connection strings without passwords are accepted.
        /// Password can be supplied programmatically via ProvidePasswordCallback.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=grabber")]
        [InlineData("Host=localhost;Port=3306;Database=GrabberDB;Username=grabber")]
        [InlineData("Server=localhost;Database=GrabberDB;User ID=grabber;Min Pool Size=1;Max Pool Size=5")]
        public void Validate_MySqlWithoutPassword_AcceptsConfiguration(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - Password is optional (can be provided via ProvidePasswordCallback)
            Assert.DoesNotContain(diagnostics, d => d.Severity == ValidationSeverity.Error);
        }

        /// <summary>
        /// Tests that Windows authentication connection strings are rejected.
        /// MySqlConnector does NOT support Windows/Integrated authentication.
        /// </summary>
        [Theory]
        [InlineData("Server=localhost;Database=GrabberDB;Integrated Security=true")]
        [InlineData("Server=localhost;Database=GrabberDB;IntegratedSecurity=true")]
        [InlineData("Server=localhost;Database=GrabberDB;Trusted_Connection=true")]
        [InlineData("Server=localhost;Database=GrabberDB;Integrated Security=SSPI")]
        public void Validate_MySqlUnsupportedWindowsAuthentication_ReturnsError(string connectionString)
        {
            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - Windows auth is not supported by MySqlConnector, should require User ID
            Assert.Contains(diagnostics, d =>
                d.Severity == ValidationSeverity.Error &&
                d.Message.Contains("user ID", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Tests that connection strings with conflicting/ambiguous aliases are rejected.
        /// This is CRITICAL for security and configuration fingerprinting.
        /// </summary>
        [Theory]
        [InlineData("Server=db01;Host=db02;Database=GrabberDB;User ID=admin")] // Conflicting server
        [InlineData("Server=localhost;Database=dbA;Initial Catalog=dbB;User ID=admin")] // Conflicting database
        [InlineData("Server=localhost;Database=GrabberDB;User ID=alice;Username=bob")] // Conflicting username
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Password=secret1;Pwd=secret2")] // Conflicting password
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Min Pool Size=5;MinimumPoolSize=10")] // Conflicting min pool
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Max Pool Size=50;MaximumPoolSize=100")] // Conflicting max pool
        public void Validate_AmbiguousConflictingAliases_ReturnsError(string connectionString)
        {
            // Arrange: Connection strings with DIFFERENT values for the same property via aliases
            // Example: Server=db01;Host=db02 is AMBIGUOUS — which server should be used?
            // This is especially dangerous for configuration fingerprinting and security.

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - Must report at least one ERROR for ambiguous configuration
            Assert.Contains(diagnostics, d => d.Severity == ValidationSeverity.Error);
        }

        /// <summary>
        /// Tests that connection strings with redundant but IDENTICAL aliases are accepted.
        /// While redundant, they don't create ambiguity.
        /// </summary>
        [Theory]
        [InlineData("Server=db01;Host=db01;Database=GrabberDB;User ID=admin")] // Redundant but consistent server
        [InlineData("Server=localhost;Database=GrabberDB;Initial Catalog=GrabberDB;User ID=admin")] // Redundant but consistent database
        [InlineData("Server=localhost;Database=GrabberDB;User ID=admin;Username=admin")] // Redundant but consistent username
        public void Validate_RedundantButIdenticalAliases_AcceptsConfiguration(string connectionString)
        {
            // Arrange: Connection strings with SAME values for the same property via aliases
            // Example: Server=db01;Host=db01 is redundant but consistent — not ambiguous

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert - Redundant but consistent configuration should be accepted (no errors)
            Assert.DoesNotContain(diagnostics, d => d.Severity == ValidationSeverity.Error);
        }

        #endregion

        #region Multiple diagnostics Tests
        /// <summary>
        /// Verifies the Validate_MultipleIssues_ReturnsAllErrors scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_MultipleIssues_ReturnsAllErrors()
        {
            // Arrange
            // Missing: database, User ID
            string connectionString = "Server=localhost";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.True(diagnostics.Count >= 2, "Should report multiple validation diagnostics");
            Assert.Contains(diagnostics, e => e.Message.Contains("database", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(diagnostics, e => e.Message.Contains("user ID", StringComparison.OrdinalIgnoreCase));
        }
        /// <summary>
        /// Verifies the Validate_AllPoolingIssues_ReportsAllWarnings scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_AllPoolingIssues_ReportsAllWarnings()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=GrabberDB;User ID=admin;Password=pass;Min Pool Size=5;Max Pool Size=100";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                "ConnectionStrings:GrabberDB");

            // Assert
            Assert.Contains(diagnostics, e => e.Message.Contains("Min Pool Size", StringComparison.Ordinal));
            Assert.Contains(diagnostics, e => e.Message.Contains("Max Pool Size", StringComparison.Ordinal));
        }

        #endregion

        #region Setting Name Tests
        /// <summary>
        /// Verifies the Validate_CustomSettingName_UsesProvidedName scenario and expected contract.
        /// </summary>
        [Fact]
        public void Validate_CustomSettingName_UsesProvidedName()
        {
            // Arrange
            string customSettingName = "CustomDatabase:ConnectionString";
            string connectionString = "invalid";

            // Act
            List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
                connectionString,
                customSettingName);

            // Assert
            Assert.All(diagnostics, e => Assert.Equal(customSettingName, e.Setting));
        }

        #endregion
    }
}


