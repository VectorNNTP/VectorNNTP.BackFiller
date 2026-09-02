// <copyright file="MySqlNntpAccountSnapshotProviderMappingTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for my sql nntp account snapshot provider mapping, covering NNTP article and transport behavior; dependency integration and failure handling.
// Primary responsibility: documents the executable contracts covered by the my sql nntp account snapshot provider mapping test suite.

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
        /// Confirms the parse use ssl when y returns true behavior.
        /// </summary>
        [Fact]
        public void ParseUseSsl_WhenY_ReturnsTrue()
        {
            bool result = MySqlNntpAccountSnapshotProvider.ParseUseSsl("y");

            Assert.True(result);
        }
        /// <summary>
        /// Confirms the parse use ssl when n returns false behavior.
        /// </summary>
        [Fact]
        public void ParseUseSsl_WhenN_ReturnsFalse()
        {
            bool result = MySqlNntpAccountSnapshotProvider.ParseUseSsl("n");

            Assert.False(result);
        }
        /// <summary>
        /// Confirms the parse keep alive value when null database value throws invalid operation exception behavior.
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
        /// Confirms the parse keep alive value when out of range throws overflow exception behavior.
        /// </summary>
        [Fact]
        public void ParseKeepAliveValue_WhenOutOfRange_ThrowsOverflowException()
        {
            _ = Assert.Throws<OverflowException>(() => MySqlNntpAccountSnapshotProvider.ParseKeepAliveValue(1000));
        }
        /// <summary>
        /// Confirms the parse use ssl when unexpected throws invalid operation exception behavior.
        /// </summary>
        [Fact]
        public void ParseUseSsl_WhenUnexpected_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseUseSsl("maybe"));

            Assert.Contains("nntpbackfilleraccounts.usessl", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the parse entry id when guid returns guid behavior.
        /// </summary>
        [Fact]
        public void ParseEntryId_WhenGuid_ReturnsGuid()
        {
            Guid expected = Guid.NewGuid();

            Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryId(expected.ToString("D"));

            Assert.Equal(expected, actual);
        }
        /// <summary>
        /// Confirms the parse entry id when invalid throws invalid operation exception behavior.
        /// </summary>
        [Fact]
        public void ParseEntryId_WhenInvalid_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseEntryId("not-a-guid"));

            Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the parse entry id value when guid object returns guid behavior.
        /// </summary>
        [Fact]
        public void ParseEntryIdValue_WhenGuidObject_ReturnsGuid()
        {
            Guid expected = Guid.NewGuid();

            Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(expected);

            Assert.Equal(expected, actual);
        }
        /// <summary>
        /// Confirms the parse entry id value when guid string object returns guid behavior.
        /// </summary>
        [Fact]
        public void ParseEntryIdValue_WhenGuidStringObject_ReturnsGuid()
        {
            Guid expected = Guid.NewGuid();

            Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(expected.ToString("D"));

            Assert.Equal(expected, actual);
        }
        /// <summary>
        /// Confirms the parse entry id value when invalid guid string throws invalid operation exception behavior.
        /// </summary>
        [Fact]
        public void ParseEntryIdValue_WhenInvalidGuidString_ThrowsInvalidOperationException()
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => MySqlNntpAccountSnapshotProvider.ParseEntryIdValue("not-a-guid"));

            Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
        }
        /// <summary>
        /// Confirms the parse entry id value when unsupported type throws invalid operation exception behavior.
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
