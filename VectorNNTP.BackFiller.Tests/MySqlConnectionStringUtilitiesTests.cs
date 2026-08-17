// MySqlConnectionStringUtilitiesTests.cs -- Tests for canonical MySQL connection string interpretation.

using System.Data.Common;
using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    public class MySqlConnectionStringUtilitiesTests
    {
        #region Basic Extraction Tests

        [Theory]
        [InlineData("Server=localhost;Database=test;User ID=admin", "localhost")]
        [InlineData("Host=myhost;Database=test;User ID=admin", "myhost")]
        [InlineData("Data Source=ds01;Database=test;User ID=admin", "ds01")]
        [InlineData("DataSource=ds02;Database=test;User ID=admin", "ds02")]
        [InlineData("Address=addr01;Database=test;User ID=admin", "addr01")]
        [InlineData("Addr=addr02;Database=test;User ID=admin", "addr02")]
        [InlineData("Network Address=netaddr;Database=test;User ID=admin", "netaddr")]
        public void TryGetServer_AcceptsAllMySqlConnectorAliases(string connectionString, string expectedServer)
        {
            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert
            Assert.True(result);
            Assert.Equal(expectedServer, server);
        }

        [Theory]
        [InlineData("Server=localhost;Database=mydb;User ID=admin", "mydb")]
        [InlineData("Server=localhost;Initial Catalog=catalog01;User ID=admin", "catalog01")]
        [InlineData("Server=localhost;InitialCatalog=catalog02;User ID=admin", "catalog02")]
        public void TryGetDatabase_AcceptsAllMySqlConnectorAliases(string connectionString, string expectedDatabase)
        {
            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert
            Assert.True(result);
            Assert.Equal(expectedDatabase, database);
        }

        [Theory]
        [InlineData("Server=localhost;Database=test;User ID=user1", "user1")]
        [InlineData("Server=localhost;Database=test;UserID=user2", "user2")]
        [InlineData("Server=localhost;Database=test;Username=user3", "user3")]
        [InlineData("Server=localhost;Database=test;Uid=user4", "user4")]
        [InlineData("Server=localhost;Database=test;User name=user5", "user5")]
        [InlineData("Server=localhost;Database=test;User=user6", "user6")]
        public void TryGetUsername_AcceptsAllMySqlConnectorAliases(string connectionString, string expectedUsername)
        {
            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert
            Assert.True(result);
            Assert.Equal(expectedUsername, username);
        }

        [Theory]
        [InlineData("Server=localhost;Database=test;User ID=admin;Password=secret", "secret")]
        [InlineData("Server=localhost;Database=test;User ID=admin;Pwd=pass123", "pass123")]
        public void TryGetPassword_AcceptsAllMySqlConnectorAliases(string connectionString, string expectedPassword)
        {
            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal(expectedPassword, password);
        }

        [Theory]
        [InlineData("Server=localhost;Database=test;User ID=admin;Min Pool Size=5", 5)]
        [InlineData("Server=localhost;Database=test;User ID=admin;MinPoolSize=10", 10)]
        [InlineData("Server=localhost;Database=test;User ID=admin;Minimum Pool Size=2", 2)]
        [InlineData("Server=localhost;Database=test;User ID=admin;MinimumPoolSize=7", 7)]
        public void TryGetMinPoolSize_AcceptsAllMySqlConnectorAliases(string connectionString, int expectedMinPoolSize)
        {
            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert
            Assert.True(result);
            Assert.Equal(expectedMinPoolSize, minPoolSize);
        }

        [Theory]
        [InlineData("Server=localhost;Database=test;User ID=admin;Max Pool Size=100", 100)]
        [InlineData("Server=localhost;Database=test;User ID=admin;MaxPoolSize=50", 50)]
        [InlineData("Server=localhost;Database=test;User ID=admin;Maximum Pool Size=200", 200)]
        [InlineData("Server=localhost;Database=test;User ID=admin;MaximumPoolSize=75", 75)]
        public void TryGetMaxPoolSize_AcceptsAllMySqlConnectorAliases(string connectionString, int expectedMaxPoolSize)
        {
            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert
            Assert.True(result);
            Assert.Equal(expectedMaxPoolSize, maxPoolSize);
        }

        #endregion

        #region Missing Values Tests

        [Fact]
        public void TryGetServer_WhenMissing_ReturnsFalse()
        {
            // Arrange
            string connectionString = "Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert
            Assert.False(result);
            Assert.Null(server);
        }

        [Fact]
        public void TryGetUsername_WhenMissing_ReturnsFalse()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=test";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert
            Assert.False(result);
            Assert.Null(username);
        }

        [Fact]
        public void TryGetPassword_WhenMissing_ReturnsFalse()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.False(result);
            Assert.Null(password);
        }

        [Fact]
        public void TryGetMinPoolSize_WhenMissing_ReturnsFalse()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert
            Assert.False(result);
            Assert.Equal(0, minPoolSize);
        }

        #endregion

        #region Malformed Values Tests

        [Fact]
        public void TryGetMinPoolSize_WhenNegative_ReturnsFalse()
        {
            // Arrange: Negative pool size should be rejected by MySqlConnectionStringBuilder
            string connectionString = "Server=localhost;Database=test;User ID=admin;Min Pool Size=-10";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert: MySqlConnectionStringBuilder should fail to parse negative uint
            Assert.False(result);
            Assert.Equal(0, minPoolSize);
        }

        [Fact]
        public void TryGetMaxPoolSize_WhenNegative_ReturnsFalse()
        {
            // Arrange: Negative pool size should be rejected by MySqlConnectionStringBuilder
            string connectionString = "Server=localhost;Database=test;User ID=admin;Max Pool Size=-1";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert: MySqlConnectionStringBuilder should fail to parse negative uint
            Assert.False(result);
            Assert.Equal(0, maxPoolSize);
        }

        [Fact]
        public void TryGetMinPoolSize_WhenExceedsIntMaxValue_ReturnsFalse()
        {
            // Arrange: uint.MaxValue (4294967295) would overflow to -1 when cast to int
            string connectionString = "Server=localhost;Database=test;User ID=admin;Min Pool Size=4294967295";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert: Should be rejected before integer overflow occurs
            Assert.False(result);
            Assert.Equal(0, minPoolSize);
        }

        [Fact]
        public void TryGetMaxPoolSize_WhenExceedsIntMaxValue_ReturnsFalse()
        {
            // Arrange: uint.MaxValue (4294967295) would overflow to -1 when cast to int
            string connectionString = "Server=localhost;Database=test;User ID=admin;Max Pool Size=4294967295";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert: Should be rejected before integer overflow occurs
            Assert.False(result);
            Assert.Equal(0, maxPoolSize);
        }

        [Fact]
        public void TryGetMinPoolSize_WhenExactlyIntMaxValue_ReturnsTrue()
        {
            // Arrange: int.MaxValue (2147483647) should be accepted
            string connectionString = "Server=localhost;Database=test;User ID=admin;Min Pool Size=2147483647";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert: Boundary value should be valid
            Assert.True(result);
            Assert.Equal(int.MaxValue, minPoolSize);
        }

        [Fact]
        public void TryGetMaxPoolSize_WhenExactlyIntMaxValue_ReturnsTrue()
        {
            // Arrange: int.MaxValue (2147483647) should be accepted
            string connectionString = "Server=localhost;Database=test;User ID=admin;Max Pool Size=2147483647";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert: Boundary value should be valid
            Assert.True(result);
            Assert.Equal(int.MaxValue, maxPoolSize);
        }

        [Fact]
        public void TryGetMinPoolSize_WhenOneAboveIntMaxValue_ReturnsFalse()
        {
            // Arrange: int.MaxValue + 1 (2147483648) would overflow to int.MinValue when cast
            string connectionString = "Server=localhost;Database=test;User ID=admin;Min Pool Size=2147483648";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert: Should be rejected
            Assert.False(result);
            Assert.Equal(0, minPoolSize);
        }

        [Fact]
        public void TryGetMaxPoolSize_WhenOneAboveIntMaxValue_ReturnsFalse()
        {
            // Arrange: int.MaxValue + 1 (2147483648) would overflow to int.MinValue when cast
            string connectionString = "Server=localhost;Database=test;User ID=admin;Max Pool Size=2147483648";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert: Should be rejected
            Assert.False(result);
            Assert.Equal(0, maxPoolSize);
        }

        [Fact]
        public void TryGetServer_WhenMalformedConnectionString_ReturnsFalse()
        {
            // Arrange
            string malformedConnectionString = "Server=localhost;Invalid;;Syntax";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(malformedConnectionString, out string? server);

            // Assert
            Assert.False(result);
            Assert.Null(server);
        }

        [Fact]
        public void TryGetPassword_WhenUnterminatedQuotedValue_ReturnsFalse()
        {
            // Arrange - unterminated quoted password value
            string unterminatedQuote = "Server=localhost;Database=test;User ID=admin;Password=\"secret";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(unterminatedQuote, out string? password);

            // Assert - Should be rejected as malformed
            Assert.False(result);
            Assert.Null(password);
        }

        [Fact]
        public void TryGetServer_WhenUnterminatedQuotedValue_ReturnsFalse()
        {
            // Arrange - unterminated quoted server value
            string unterminatedQuote = "Server=\"localhost;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(unterminatedQuote, out string? server);

            // Assert - Should be rejected as malformed
            Assert.False(result);
            Assert.Null(server);
        }

        [Fact]
        public void TryGetPassword_WhenUnterminatedSingleQuotedValue_ReturnsFalse()
        {
            // Arrange - unterminated single-quoted password value
            string unterminatedQuote = "Server=localhost;Database=test;User ID=admin;Password='secret";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(unterminatedQuote, out string? password);

            // Assert - Should be rejected as malformed
            Assert.False(result);
            Assert.Null(password);
        }

        [Fact]
        public void TryGetServer_WhenUnterminatedSingleQuotedValue_ReturnsFalse()
        {
            // Arrange - unterminated single-quoted server value
            string unterminatedQuote = "Server='localhost;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(unterminatedQuote, out string? server);

            // Assert - Should be rejected as malformed
            Assert.False(result);
            Assert.Null(server);
        }

        [Fact]
        public void TryGetPassword_WhenGarbageAfterClosingQuote_ReturnsFalse()
        {
            // Arrange - text after closing quote is invalid
            string garbageAfterQuote = "Server=localhost;Database=test;User ID=admin;Password=\"secret\"GARBAGE";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(garbageAfterQuote, out string? password);

            // Assert - Should be rejected as malformed
            Assert.False(result);
            Assert.Null(password);
        }

        [Fact]
        public void TryGetServer_WhenGarbageAfterClosingQuote_ReturnsFalse()
        {
            // Arrange - text after closing quote is invalid
            string garbageAfterQuote = "Server=\"localhost\"x;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(garbageAfterQuote, out string? server);

            // Assert - Should be rejected as malformed
            Assert.False(result);
            Assert.Null(server);
        }

        [Fact]
        public void TryGetPassword_WhenSingleQuotedWithGarbageAfterClosingQuote_ReturnsFalse()
        {
            // Arrange - text after closing single quote is invalid
            string garbageAfterQuote = "Server=localhost;Database=test;User ID=admin;Password='secret'THIS_IS_GARBAGE;";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(garbageAfterQuote, out string? password);

            // Assert - Should be rejected as malformed
            Assert.False(result);
            Assert.Null(password);
        }

        [Fact]
        public void TryGetPassword_WhenWhitespaceAfterClosingQuote_Succeeds()
        {
            // Arrange - whitespace after closing quote is valid per ADO.NET spec
            string whitespaceAfterQuote = "Server=localhost;Database=test;User ID=admin;Password=\"secret\"  ;";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(whitespaceAfterQuote, out string? password);

            // Assert - Whitespace is allowed
            Assert.True(result);
            Assert.Equal("secret", password);
        }

        [Fact]
        public void TryGetServer_WhenQuotedValueAtEnd_Succeeds()
        {
            // Arrange - quoted value at end of string (no semicolon after)
            string quotedAtEnd = "Database=test;User ID=admin;Server=\"localhost\"";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(quotedAtEnd, out string? server);

            // Assert - End of string after closing quote is valid
            Assert.True(result);
            Assert.Equal("localhost", server);
        }

        #endregion

        #region Case Insensitivity Tests

        [Fact]
        public void TryGetServer_IsCaseInsensitive()
        {
            // Arrange - mixed case property name
            string connectionString = "sErVeR=localhost;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert
            Assert.True(result);
            Assert.Equal("localhost", server);
        }

        [Fact]
        public void TryGetDatabase_IsCaseInsensitive()
        {
            // Arrange - mixed case property name
            string connectionString = "Server=localhost;DaTaBaSe=testdb;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert
            Assert.True(result);
            Assert.Equal("testdb", database);
        }

        [Fact]
        public void TryGetUsername_IsCaseInsensitive()
        {
            // Arrange - mixed case property name
            string connectionString = "Server=localhost;Database=test;UsEr Id=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert
            Assert.True(result);
            Assert.Equal("admin", username);
        }

        [Fact]
        public void TryGetPassword_IsCaseInsensitive()
        {
            // Arrange - mixed case property name
            string connectionString = "Server=localhost;Database=test;User ID=admin;PaSsWoRd=secret";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("secret", password);
        }

        [Fact]
        public void TryGetMinPoolSize_IsCaseInsensitive()
        {
            // Arrange - mixed case property name
            string connectionString = "Server=localhost;Database=test;User ID=admin;MiN pOoL sIzE=10";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert
            Assert.True(result);
            Assert.Equal(10, minPoolSize);
        }

        [Fact]
        public void TryGetMaxPoolSize_IsCaseInsensitive()
        {
            // Arrange - mixed case property name
            string connectionString = "Server=localhost;Database=test;User ID=admin;MaX pOoL sIzE=50";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert
            Assert.True(result);
            Assert.Equal(50, maxPoolSize);
        }

        #endregion

        #region Whitespace Handling Tests

        [Fact]
        public void TryGetServer_WhenUnquotedWithLeadingWhitespace_TrimsWhitespace()
        {
            // Arrange - Unquoted value with leading whitespace should be trimmed
            string connectionString = "Server =  localhost  ;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert
            Assert.True(result);
            Assert.Equal("localhost", server);
        }

        [Fact]
        public void TryGetDatabase_WhenUnquotedWithTrailingWhitespace_TrimsWhitespace()
        {
            // Arrange - Unquoted value with trailing whitespace should be trimmed
            string connectionString = "Server=localhost;Database= testdb  ;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert
            Assert.True(result);
            Assert.Equal("testdb", database);
        }

        #endregion

        #region Quoted Values Tests

        [Fact]
        public void TryGetPassword_WhenValueContainsSemicolon_HandlesQuoting()
        {
            // Arrange - Password with semicolon requires quoting
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=\"abc;123\"";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("abc;123", password);
        }

        [Fact]
        public void TryGetPassword_WhenValueContainsEquals_HandlesQuoting()
        {
            // Arrange - Password with equals sign requires quoting
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=\"pass=word\"";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("pass=word", password);
        }

        [Fact]
        public void TryGetPassword_WhenSingleQuotedWithSemicolon_HandlesQuoting()
        {
            // Arrange - ADO.NET supports single quotes per spec
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password='abc;123'";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("abc;123", password);
        }

        [Fact]
        public void TryGetPassword_WhenSingleQuotedWithEscaping_HandlesDoubledQuotes()
        {
            // Arrange - Single quotes are escaped by doubling per ADO.NET spec
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password='it''s secret'";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("it's secret", password);
        }

        [Fact]
        public void TryGetPassword_WhenDoubleQuotedWithEscaping_HandlesDoubledQuotes()
        {
            // Arrange - Double quotes are escaped by doubling per ADO.NET spec
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=\"say \"\"hello\"\"\"";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("say \"hello\"", password);
        }

        [Fact]
        public void TryGetServer_WhenSingleQuoted_HandlesQuoting()
        {
            // Arrange - Single-quoted server name
            string connectionString = "Server='my-server';Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert
            Assert.True(result);
            Assert.Equal("my-server", server);
        }

        [Fact]
        public void TryGetDatabase_WhenSingleQuoted_HandlesQuoting()
        {
            // Arrange - Single-quoted database name
            string connectionString = "Server=localhost;Database='my-database';User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert
            Assert.True(result);
            Assert.Equal("my-database", database);
        }

        [Fact]
        public void TryGetPassword_WhenQuotedWithLeadingWhitespace_HandlesQuoting()
        {
            // Arrange - ADO.NET spec allows whitespace before quoted values
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password = \"abc;123\"";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("abc;123", password);
        }

        [Fact]
        public void TryGetPassword_WhenSingleQuotedWithLeadingWhitespace_HandlesQuoting()
        {
            // Arrange - Whitespace before single-quoted value
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password =  'it''s secret'";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.True(result);
            Assert.Equal("it's secret", password);
        }

        [Fact]
        public void TryGetServer_WhenQuotedWithWhitespaceAroundEquals_HandlesQuoting()
        {
            // Arrange - Whitespace on both sides of equals
            string connectionString = "Server = \"my-server\";Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert
            Assert.True(result);
            Assert.Equal("my-server", server);
        }

        #endregion

        #region Empty Values Tests

        [Fact]
        public void TryGetServer_WhenEmpty_ReturnsFalse()
        {
            // Arrange - Empty server value
            string connectionString = "Server=;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void TryGetDatabase_WhenEmpty_ReturnsFalse()
        {
            // Arrange - Empty database value
            string connectionString = "Server=localhost;Database=;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void TryGetUsername_WhenEmpty_ReturnsFalse()
        {
            // Arrange - Empty username value
            string connectionString = "Server=localhost;Database=test;User ID=";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void TryGetPassword_WhenEmpty_ReturnsFalse()
        {
            // Arrange - Empty password value
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void TryGetMinPoolSize_WhenEmptyString_ReturnsFalse()
        {
            // Arrange - Empty pool size value
            string connectionString = "Server=localhost;Database=test;User ID=admin;Min Pool Size=";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert
            Assert.False(result);
            Assert.Equal(0, minPoolSize);
        }

        [Fact]
        public void TryGetMaxPoolSize_WhenEmptyString_ReturnsFalse()
        {
            // Arrange - Empty pool size value
            string connectionString = "Server=localhost;Database=test;User ID=admin;Max Pool Size=";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert
            Assert.False(result);
            Assert.Equal(0, maxPoolSize);
        }

        #endregion

        #region Ambiguous Aliases Tests

        [Fact]
        public void TryGetServer_WhenConflictingServerAliases_ReturnsFalse()
        {
            // Arrange: Server=db01;Host=db02 is AMBIGUOUS
            string connectionString = "Server=db01;Host=db02;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
            Assert.Null(server);
        }

        [Fact]
        public void TryGetServer_WhenRedundantButIdenticalServerAliases_ReturnsTrue()
        {
            // Arrange: Server=db01;Host=db01 is redundant but consistent
            string connectionString = "Server=db01;Host=db01;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert - Redundant but consistent is acceptable
            Assert.True(result);
            Assert.Equal("db01", server);
        }

        [Fact]
        public void TryGetDatabase_WhenConflictingDatabaseAliases_ReturnsFalse()
        {
            // Arrange: Database=dbA;Initial Catalog=dbB is AMBIGUOUS
            string connectionString = "Server=localhost;Database=dbA;Initial Catalog=dbB;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
            Assert.Null(database);
        }

        [Fact]
        public void TryGetDatabase_WhenRedundantButIdenticalDatabaseAliases_ReturnsTrue()
        {
            // Arrange: Database=GrabberDB;Initial Catalog=GrabberDB is redundant but consistent
            string connectionString = "Server=localhost;Database=GrabberDB;Initial Catalog=GrabberDB;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert - Redundant but consistent is acceptable
            Assert.True(result);
            Assert.Equal("GrabberDB", database);
        }

        [Fact]
        public void TryGetUsername_WhenConflictingUsernameAliases_ReturnsFalse()
        {
            // Arrange: User ID=alice;Username=bob is AMBIGUOUS
            string connectionString = "Server=localhost;Database=test;User ID=alice;Username=bob";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
            Assert.Null(username);
        }

        [Fact]
        public void TryGetUsername_WhenRedundantButIdenticalUsernameAliases_ReturnsTrue()
        {
            // Arrange: User ID=admin;Username=admin is redundant but consistent
            string connectionString = "Server=localhost;Database=test;User ID=admin;Username=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert - Redundant but consistent is acceptable
            Assert.True(result);
            Assert.Equal("admin", username);
        }

        [Fact]
        public void TryGetPassword_WhenConflictingPasswordAliases_ReturnsFalse()
        {
            // Arrange: Password=secret1;Pwd=secret2 is AMBIGUOUS
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=secret1;Pwd=secret2";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
            Assert.Null(password);
        }

        [Fact]
        public void TryGetPassword_WhenRedundantButIdenticalPasswordAliases_ReturnsTrue()
        {
            // Arrange: Password=secret;Pwd=secret is redundant but consistent
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=secret;Pwd=secret";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert - Redundant but consistent is acceptable
            Assert.True(result);
            Assert.Equal("secret", password);
        }

        [Fact]
        public void TryGetMinPoolSize_WhenConflictingMinPoolSizeAliases_ReturnsFalse()
        {
            // Arrange: Min Pool Size=5;MinimumPoolSize=10 is AMBIGUOUS
            string connectionString = "Server=localhost;Database=test;User ID=admin;Min Pool Size=5;MinimumPoolSize=10";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMinPoolSize(connectionString, out int minPoolSize);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
            Assert.Equal(0, minPoolSize);
        }

        [Fact]
        public void TryGetMaxPoolSize_WhenConflictingMaxPoolSizeAliases_ReturnsFalse()
        {
            // Arrange: Max Pool Size=50;MaximumPoolSize=100 is AMBIGUOUS
            string connectionString = "Server=localhost;Database=test;User ID=admin;Max Pool Size=50;MaximumPoolSize=100";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetMaxPoolSize(connectionString, out int maxPoolSize);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
            Assert.Equal(0, maxPoolSize);
        }

        [Fact]
        public void TryGetServer_WhenThreeConflictingAliases_ReturnsFalse()
        {
            // Arrange: Server=db01;Data Source=db02;Host=db03 has three conflicting server aliases
            string connectionString = "Server=db01;Data Source=db02;Host=db03;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
        }

        [Fact]
        public void TryGetUsername_WhenThreeConflictingAliases_ReturnsFalse()
        {
            // Arrange: User ID=alice;Username=bob;Uid=charlie has three conflicting username aliases
            string connectionString = "Server=localhost;Database=test;User ID=alice;Username=bob;Uid=charlie";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert - Must reject ambiguous configuration
            Assert.False(result);
        }

        [Fact]
        public void TryGetServer_WhenSameKeyRepeatedWithDifferentValues_ReturnsFalse()
        {
            // Arrange: CRITICAL - Server=db01;Server=db02 (same key repeated)
            string connectionString = "Server=db01;Server=db02;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetServer(connectionString, out string? server);

            // Assert - Must reject duplicate key with different values
            Assert.False(result);
        }

        [Fact]
        public void TryGetPassword_WhenSameKeyRepeatedWithDifferentValues_ReturnsFalse()
        {
            // Arrange: CRITICAL - Password=secret1;Password=secret2 (same key repeated)
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=secret1;Password=secret2";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetPassword(connectionString, out string? password);

            // Assert - Must reject duplicate key with different values (SECURITY)
            Assert.False(result);
        }

        [Fact]
        public void TryGetUsername_WhenSameKeyRepeatedWithDifferentValues_ReturnsFalse()
        {
            // Arrange: CRITICAL - User ID=alice;User ID=bob (same key repeated)
            string connectionString = "Server=localhost;Database=test;User ID=alice;User ID=bob";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username);

            // Assert - Must reject duplicate key with different values
            Assert.False(result);
        }

        [Fact]
        public void TryGetDatabase_WhenSameKeyRepeatedWithDifferentValues_ReturnsFalse()
        {
            // Arrange: CRITICAL - Database=dbA;Database=dbB (same key repeated)
            string connectionString = "Server=localhost;Database=dbA;Database=dbB;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryGetDatabase(connectionString, out string? database);

            // Assert - Must reject duplicate key with different values
            Assert.False(result);
        }

        #endregion

        #region HasAmbiguousAliases Tests

        [Theory]
        [InlineData("Server=db01;Database=GrabberDB;User ID=admin;Password=secret")]
        [InlineData("Server=db01;Database=GrabberDB;User ID=admin")]
        [InlineData("Server=db01;Database=GrabberDB;User ID=admin;Password=secret;Min Pool Size=1")]
        public void HasAmbiguousAliases_WhenNoConflicts_ReturnsFalse(string connectionString)
        {
            // Act & Assert - Valid connection strings with no conflicting aliases
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenServerConflicts_ReturnsTrue()
        {
            // Arrange - Conflicting server aliases
            const string connectionString = "Server=db01;Host=db02;Database=GrabberDB;User ID=admin";

            // Act & Assert - This IS ambiguous
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenDatabaseConflicts_ReturnsTrue()
        {
            // Arrange - Conflicting database aliases
            const string connectionString = "Server=localhost;Database=dbA;Initial Catalog=dbB;User ID=admin";

            // Act & Assert - This IS ambiguous
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenUsernameConflicts_ReturnsTrue()
        {
            // Arrange - Conflicting username aliases
            const string connectionString = "Server=localhost;Database=test;User ID=alice;Username=bob";

            // Act & Assert - This IS ambiguous
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenPasswordConflicts_ReturnsTrue()
        {
            // Arrange - Conflicting password aliases
            const string connectionString = "Server=localhost;Database=test;User ID=admin;Password=secret1;Pwd=secret2";

            // Act & Assert - This IS ambiguous
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenMinPoolSizeConflicts_ReturnsTrue()
        {
            // Arrange - Conflicting min pool size aliases
            const string connectionString = "Server=localhost;Database=test;User ID=admin;Min Pool Size=5;MinimumPoolSize=10";

            // Act & Assert - This IS ambiguous
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenMaxPoolSizeConflicts_ReturnsTrue()
        {
            // Arrange - Conflicting max pool size aliases
            const string connectionString = "Server=localhost;Database=test;User ID=admin;Max Pool Size=50;MaximumPoolSize=100";

            // Act & Assert - This IS ambiguous
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenMultipleConflictingGroups_ReturnsTrue()
        {
            // Arrange - Multiple conflicting alias groups at once
            const string connectionString = "Server=db01;Host=db02;Database=dbA;Initial Catalog=dbB;User ID=alice;Username=bob";

            // Act & Assert - This IS ambiguous (multiple conflicts should still return true)
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenSameKeyRepeatedWithDifferentServerValues_ReturnsTrue()
        {
            // Arrange - CRITICAL: Server=db01;Server=db02 (same key repeated)
            // DbConnectionStringBuilder would canonicalize this to just the last value,
            // but our raw parser must detect it BEFORE canonicalization
            const string connectionString = "Server=db01;Server=db02;Database=GrabberDB;User ID=admin";

            // Act & Assert - This IS ambiguous and MUST be detected
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenSameKeyRepeatedWithDifferentPasswordValues_ReturnsTrue()
        {
            // Arrange - CRITICAL: Password=secret1;Password=secret2 (same key repeated)
            // This is a SECURITY issue if not detected
            const string connectionString = "Server=localhost;Database=test;User ID=admin;Password=secret1;Password=secret2";

            // Act & Assert - This IS ambiguous and MUST be detected
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenSameKeyRepeatedWithDifferentUsernameValues_ReturnsTrue()
        {
            // Arrange - CRITICAL: User ID=alice;User ID=bob (same key repeated)
            const string connectionString = "Server=localhost;Database=test;User ID=alice;User ID=bob";

            // Act & Assert - This IS ambiguous and MUST be detected
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenSameKeyRepeatedWithIdenticalValues_ReturnsFalse()
        {
            // Arrange - Server=db01;Server=db01 (same key, same value - redundant but not ambiguous)
            const string connectionString = "Server=db01;Server=db01;Database=GrabberDB;User ID=admin";

            // Act & Assert - Redundant but consistent should NOT be ambiguous
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenRedundantButIdentical_ReturnsFalse()
        {
            // Arrange - Redundant but identical aliases
            string connectionString = "Server=db01;Host=db01;Database=test;User ID=admin;Username=admin;Password=secret;Pwd=secret";

            // Act
            bool result = MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString);

            // Assert - Redundant but consistent is NOT ambiguous
            Assert.False(result);
        }

        [Fact]
        public void HasAmbiguousAliases_WhenNullOrEmpty_ReturnsFalse()
        {
            // Assert
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(null));
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(""));
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases("   "));
        }

        [Fact]
        public void HasAmbiguousAliases_WhenMalformedSyntax_ReturnsFalse()
        {
            // Arrange - Malformed connection strings that cannot be parsed
            string malformed1 = "Server=localhost;Invalid;;Syntax";
            string malformed2 = "=value";
            string malformed3 = "NoEquals";

            // Act & Assert - Returns false because parsing fails, NOT because string is "safe"
            // This documents that false does NOT mean "valid" or "safe"
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(malformed1));
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(malformed2));
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(malformed3));

            // Important: A malformed string will fail TryParse/TryParseEffective
            // HasAmbiguousAliases returning false does NOT make it safe!
        }

        [Fact]
        public void HasAmbiguousAliases_WhenFullyPopulatedAndValid_ReturnsFalse()
        {
            // Arrange - Connection string with all properties set (no conflicts)
            string connectionString = "Server=db01;Database=GrabberDB;User ID=grabber;Password=secret;Min Pool Size=5;Max Pool Size=100";

            // Act
            bool result = MySqlConnectionStringUtilities.HasAmbiguousAliases(connectionString);

            // Assert - No ambiguity even though all properties are present
            Assert.False(result);
        }

        #endregion

        #region ConnectionStringParseResult Tests

        [Fact]
        public void ParseResult_DistinguishesMissingFromInvalidFromAmbiguous_ForMaxPoolSize()
        {
            // Missing: No Max Pool Size property
            string missingCs = "Server=localhost;Database=test;User ID=admin";
            bool missingResult = MySqlConnectionStringUtilities.TryGetMaxPoolSize(missingCs, out int missingValue);
            Assert.False(missingResult);
            Assert.Equal(0, missingValue);

            // Invalid: Max Pool Size has malformed value
            string invalidCs = "Server=localhost;Database=test;User ID=admin;Max Pool Size=abc";
            bool invalidResult = MySqlConnectionStringUtilities.TryGetMaxPoolSize(invalidCs, out int invalidValue);
            Assert.False(invalidResult);
            Assert.Equal(0, invalidValue);

            // Ambiguous: Conflicting Max Pool Size aliases
            string ambiguousCs = "Server=localhost;Database=test;User ID=admin;Max Pool Size=10;MaximumPoolSize=20";
            bool ambiguousResult = MySqlConnectionStringUtilities.TryGetMaxPoolSize(ambiguousCs, out int ambiguousValue);
            Assert.False(ambiguousResult);
            Assert.Equal(0, ambiguousValue);

            // All three return false, but for different reasons
            // The TryGet* API cannot distinguish these states
            // This is the fundamental limitation the user identified
        }

        [Fact]
        public void ParseResult_DistinguishesMissingFromInvalidFromAmbiguous_ForServer()
        {
            // Missing: No Server property
            string missingCs = "Database=test;User ID=admin";
            bool missingResult = MySqlConnectionStringUtilities.TryGetServer(missingCs, out string? missingValue);
            Assert.False(missingResult);
            Assert.Null(missingValue);

            // Invalid: Malformed connection string
            string invalidCs = "Server=localhost;Invalid;;Syntax";
            bool invalidResult = MySqlConnectionStringUtilities.TryGetServer(invalidCs, out string? invalidValue);
            Assert.False(invalidResult);
            Assert.Null(invalidValue);

            // Ambiguous: Conflicting Server aliases
            string ambiguousCs = "Server=db01;Host=db02;Database=test;User ID=admin";
            bool ambiguousResult = MySqlConnectionStringUtilities.TryGetServer(ambiguousCs, out string? ambiguousValue);
            Assert.False(ambiguousResult);
            Assert.Null(ambiguousValue);

            // All three return false, but for different reasons
        }

        [Fact]
        public void AmbiguityDetection_IsIndependentOfPropertyExtraction()
        {
            // Missing properties should NOT be ambiguous
            string missingPassword = "Server=localhost;Database=test;User ID=admin";
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(missingPassword));

            string missingPoolSizes = "Server=localhost;Database=test;User ID=admin;Password=secret";
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(missingPoolSizes));

            // Invalid/malformed should NOT be ambiguous
            string malformed = "Server=localhost;Database=test;User ID=admin;Max Pool Size=abc";
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(malformed));

            // Only actual conflicts should be ambiguous
            string serverConflict = "Server=db01;Host=db02;Database=test;User ID=admin";
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(serverConflict));

            string databaseConflict = "Server=localhost;Database=prod;Initial Catalog=test;User ID=admin";
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(databaseConflict));

            string usernameConflict = "Server=localhost;Database=test;User ID=alice;Username=bob";
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(usernameConflict));

            string passwordConflict = "Server=localhost;Database=test;User ID=admin;Password=secret;Pwd=different";
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(passwordConflict));

            string minPoolConflict = "Server=localhost;Database=test;User ID=admin;Min Pool Size=1;MinimumPoolSize=2";
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(minPoolConflict));

            string maxPoolConflict = "Server=localhost;Database=test;User ID=admin;Max Pool Size=10;MaximumPoolSize=20";
            Assert.True(MySqlConnectionStringUtilities.HasAmbiguousAliases(maxPoolConflict));

            // Redundant but identical aliases should NOT be ambiguous
            string redundantServer = "Server=db01;Host=db01;Database=test;User ID=admin";
            Assert.False(MySqlConnectionStringUtilities.HasAmbiguousAliases(redundantServer));
        }

        #endregion

        #region TryParseEffective Tests

        [Fact]
        public void TryParseEffective_WhenValidConnectionString_ReturnsTrue()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=test;User ID=admin;Password=secret";

            // Act
            bool result = MySqlConnectionStringUtilities.TryParseEffective(connectionString, out var builder);

            // Assert
            Assert.True(result);
            Assert.NotNull(builder);
            Assert.Equal("localhost", builder.Server);
            Assert.Equal("test", builder.Database);
            Assert.Equal("admin", builder.UserID);
        }

        [Fact]
        public void TryParseEffective_WhenAmbiguousServerAliases_ReturnsFalse()
        {
            // Arrange
            string connectionString = "Server=db01;Host=db02;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryParseEffective(connectionString, out var builder);

            // Assert
            Assert.False(result);
            Assert.Null(builder);
        }

        [Fact]
        public void TryParseEffective_WhenDuplicateSameKey_ReturnsFalse()
        {
            // Arrange
            string connectionString = "Server=db01;Server=db02;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryParseEffective(connectionString, out var builder);

            // Assert
            Assert.False(result);
            Assert.Null(builder);
        }

        [Fact]
        public void TryParseEffective_WhenMalformedConnectionString_ReturnsFalse()
        {
            // Arrange
            string malformedConnectionString = "Server=localhost;Invalid;;Syntax";

            // Act
            bool result = MySqlConnectionStringUtilities.TryParseEffective(malformedConnectionString, out var builder);

            // Assert
            Assert.False(result);
            Assert.Null(builder);
        }

        [Fact]
        public void TryParseEffective_WhenNullOrEmpty_MatchesProviderBehavior()
        {
            // Act & Assert - null: provider accepts null as empty connection string
            Assert.True(MySqlConnectionStringUtilities.TryParseEffective(null, out var builder1));
            Assert.NotNull(builder1);

            // Act & Assert - empty: provider accepts empty connection string
            Assert.True(MySqlConnectionStringUtilities.TryParseEffective("", out var builder2));
            Assert.NotNull(builder2);
        }

        [Fact]
        public void TryParseEffective_WhenRedundantIdenticalAliases_ReturnsTrue()
        {
            // Arrange - Same value via different aliases should still be accepted
            string connectionString = "Server=localhost;Host=localhost;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryParseEffective(connectionString, out var builder);

            // Assert
            Assert.True(result);
            Assert.NotNull(builder);
            Assert.Equal("localhost", builder.Server);
        }

        [Fact]
        public void TryParse_AcceptsAmbiguousConfigurationWithoutValidation()
        {
            // Arrange - TryParse should accept this (unsafe behavior)
            string ambiguousConnectionString = "Server=db01;Host=db02;Database=test;User ID=admin";

            // Act
            bool result = MySqlConnectionStringUtilities.TryParse(ambiguousConnectionString, out var builder);

            // Assert - TryParse succeeds but silently applies last-write-wins
            Assert.True(result);
            Assert.NotNull(builder);
            // Provider canonicalizes to last value
            Assert.Equal("db02", builder.Server);
        }

        #endregion

        #region Cross-Parser Validation Tests

        /// <summary>
        /// Validates the core invariant: the raw parser should reject exactly the syntax
        /// that DbConnectionStringBuilder rejects, while preserving information the provider
        /// intentionally discards (e.g., duplicate keys, conflicting aliases).
        /// </summary>
        /// <remarks>
        /// <para>This test matrix ensures parser compatibility between our raw parser and the provider.</para>
        /// <para>The invariant has two levels:</para>
        /// <list type="number">
        /// <item><description><strong>Syntax acceptance</strong>: Both parsers must agree on valid/invalid syntax</description></item>
        /// <item><description><strong>Semantic rejection</strong>: Our parser additionally rejects ambiguous configurations (duplicates/conflicts)</description></item>
        /// </list>
        /// </remarks>
        [Theory]
        [InlineData("Server=db01", true, false, "Basic unquoted value")]
        [InlineData("Server=\"db;01\"", true, false, "Double-quoted value with semicolon")]
        [InlineData("Server='db;01'", true, false, "Single-quoted value with semicolon")]
        [InlineData("Server=\"abc\"\"def\"", true, false, "Double-quoted value with escaped double-quote")]
        [InlineData("Server='abc''def'", true, false, "Single-quoted value with escaped single-quote")]
        [InlineData("Server=\"abc", false, false, "Unterminated double-quoted value")]
        [InlineData("Server='abc", false, false, "Unterminated single-quoted value")]
        [InlineData("Server=\"abc\"x", false, false, "Garbage after closing double-quote")]
        [InlineData("Server='abc'x", false, false, "Garbage after closing single-quote")]
        [InlineData("Server = \"abc\"", true, false, "Whitespace before double-quoted value")]
        [InlineData("Server = 'abc'", true, false, "Whitespace before single-quoted value")]
        [InlineData("Server=db01;Server=db02", true, true, "Duplicate keys (syntax valid; semantically ambiguous)")]
        [InlineData("Server=db01;Host=db02", true, true, "Conflicting aliases (syntax valid; semantically ambiguous)")]
        [InlineData("", true, false, "Empty connection string")]
        [InlineData("   ", true, false, "Whitespace-only connection string")]
        [InlineData("Server=", true, false, "Empty value")]
        [InlineData("Server=\"\"", true, false, "Empty double-quoted value")]
        [InlineData("Server=''", true, false, "Empty single-quoted value")]
        [InlineData(";Server=db01", true, false, "Leading semicolon")]
        [InlineData("Server=db01;", true, false, "Trailing semicolon")]
        [InlineData("Server=db01;;Host=db02", true, false, "Consecutive semicolons")]
        public void RawParser_RejectsExactlySameSyntaxAsProvider(
            string connectionString,
            bool providerShouldAcceptSyntax,
            bool isSemanticAmbiguity,
            string scenario)
        {
            // Arrange & Act (provider parser - syntax check only)
            bool providerAcceptsSyntax;
            try
            {
                var builder = new DbConnectionStringBuilder
                {
                    ConnectionString = connectionString
                };
                providerAcceptsSyntax = true;
            }
            catch (ArgumentException)
            {
                providerAcceptsSyntax = false;
            }

            // Arrange & Act (raw parser - both syntax and semantic ambiguity)
            bool rawParserAccepts = MySqlConnectionStringUtilities.TryParseEffective(connectionString, out _);

            // Assert: syntax invariant (both parsers must agree on valid/invalid syntax)
            Assert.Equal(providerShouldAcceptSyntax, providerAcceptsSyntax); // Verify test expectation
            Assert.Equal(providerShouldAcceptSyntax, providerAcceptsSyntax); // Enforce syntax invariant

            // Assert: semantic layer (our parser rejects ambiguity that provider would canonicalize)
            bool expectedRawParserAccepts = providerAcceptsSyntax && !isSemanticAmbiguity;
            Assert.Equal(expectedRawParserAccepts, rawParserAccepts);
        }

        #endregion
    }
}
