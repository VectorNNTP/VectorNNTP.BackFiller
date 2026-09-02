// <copyright file="MySqlNntpAccountSnapshotProviderMappingTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for my sql nntp account snapshot provider mapping.

using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests strict row mapping parsers used by the MySQL startup account snapshot provider.
    /// </summary>
    public sealed class MySqlNntpAccountSnapshotProviderMappingTests
    {
        /// <summary>
        /// Verifies the ParseUseSsl_WhenY_ReturnsTrue scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseUseSsl_WhenY_ReturnsTrue()
        {
            bool result = MySqlNntpAccountSnapshotProvider.ParseUseSsl("y");

            Assert.True(result);
        }
        /// <summary>
        /// Verifies the ParseUseSsl_WhenN_ReturnsFalse scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseUseSsl_WhenN_ReturnsFalse()
        {
            bool result = MySqlNntpAccountSnapshotProvider.ParseUseSsl("n");

            Assert.False(result);
        }
        /// <summary>
        /// Verifies the ParseKeepAliveValue_WhenNullDatabaseValue_ThrowsInvalidOperationException scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseKeepAliveValue_WhenNullDatabaseValue_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseKeepAliveValue(DBNull.Value));

            Assert.Contains("nntpbackfilleraccounts.keepalive", ex.Message, StringComparison.Ordinal);
            Assert.Contains("NULL", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Verifies the ParseKeepAliveValue_WhenOutOfRange_ThrowsOverflowException scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseKeepAliveValue_WhenOutOfRange_ThrowsOverflowException()
        {
            _ = Assert.Throws<OverflowException>(() => MySqlNntpAccountSnapshotProvider.ParseKeepAliveValue(1000));
        }
        /// <summary>
        /// Verifies the ParseUseSsl_WhenUnexpected_ThrowsInvalidOperationException scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseUseSsl_WhenUnexpected_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseUseSsl("maybe"));

            Assert.Contains("nntpbackfilleraccounts.usessl", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Verifies the ParseEntryId_WhenGuid_ReturnsGuid scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseEntryId_WhenGuid_ReturnsGuid()
        {
            Guid expected = Guid.NewGuid();

            Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryId(expected.ToString("D"));

            Assert.Equal(expected, actual);
        }
        /// <summary>
        /// Verifies the ParseEntryId_WhenInvalid_ThrowsInvalidOperationException scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseEntryId_WhenInvalid_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseEntryId("not-a-guid"));

            Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Verifies the ParseEntryIdValue_WhenGuidObject_ReturnsGuid scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseEntryIdValue_WhenGuidObject_ReturnsGuid()
        {
            Guid expected = Guid.NewGuid();

            Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(expected);

            Assert.Equal(expected, actual);
        }
        /// <summary>
        /// Verifies the ParseEntryIdValue_WhenGuidStringObject_ReturnsGuid scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseEntryIdValue_WhenGuidStringObject_ReturnsGuid()
        {
            Guid expected = Guid.NewGuid();

            Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(expected.ToString("D"));

            Assert.Equal(expected, actual);
        }
        /// <summary>
        /// Verifies the ParseEntryIdValue_WhenInvalidGuidString_ThrowsInvalidOperationException scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseEntryIdValue_WhenInvalidGuidString_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseEntryIdValue("not-a-guid"));

            Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Verifies the ParseEntryIdValue_WhenUnsupportedType_ThrowsInvalidOperationException scenario and expected contract.
        /// </summary>
        [Fact]
        public void ParseEntryIdValue_WhenUnsupportedType_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(123));

            Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
            Assert.Contains("Expected GUID or string GUID", ex.Message, StringComparison.Ordinal);
        }
    }
}
