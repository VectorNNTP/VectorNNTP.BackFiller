using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests strict row mapping parsers used by the MySQL startup account snapshot provider.
/// </summary>
public sealed class MySqlNntpAccountSnapshotProviderMappingTests
{
    [Fact]
    public void ParseUseSsl_WhenY_ReturnsTrue()
    {
        bool result = MySqlNntpAccountSnapshotProvider.ParseUseSsl("y");

        Assert.True(result);
    }

    [Fact]
    public void ParseUseSsl_WhenN_ReturnsFalse()
    {
        bool result = MySqlNntpAccountSnapshotProvider.ParseUseSsl("n");

        Assert.False(result);
    }

    [Fact]
    public void ParseKeepAliveValue_WhenNullDatabaseValue_ThrowsInvalidOperationException()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => MySqlNntpAccountSnapshotProvider.ParseKeepAliveValue(DBNull.Value));

        Assert.Contains("nntpbackfilleraccounts.keepalive", ex.Message, StringComparison.Ordinal);
        Assert.Contains("NULL", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseKeepAliveValue_WhenOutOfRange_ThrowsOverflowException()
    {
        Assert.Throws<OverflowException>(() => MySqlNntpAccountSnapshotProvider.ParseKeepAliveValue(1000));
    }

    [Fact]
    public void ParseUseSsl_WhenUnexpected_ThrowsInvalidOperationException()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => MySqlNntpAccountSnapshotProvider.ParseUseSsl("maybe"));

        Assert.Contains("nntpbackfilleraccounts.usessl", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseEntryId_WhenGuid_ReturnsGuid()
    {
        Guid expected = Guid.NewGuid();

        Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryId(expected.ToString("D"));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ParseEntryId_WhenInvalid_ThrowsInvalidOperationException()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => MySqlNntpAccountSnapshotProvider.ParseEntryId("not-a-guid"));

        Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseEntryIdValue_WhenGuidObject_ReturnsGuid()
    {
        Guid expected = Guid.NewGuid();

        Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(expected);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ParseEntryIdValue_WhenGuidStringObject_ReturnsGuid()
    {
        Guid expected = Guid.NewGuid();

        Guid actual = MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(expected.ToString("D"));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ParseEntryIdValue_WhenInvalidGuidString_ThrowsInvalidOperationException()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => MySqlNntpAccountSnapshotProvider.ParseEntryIdValue("not-a-guid"));

        Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseEntryIdValue_WhenUnsupportedType_ThrowsInvalidOperationException()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => MySqlNntpAccountSnapshotProvider.ParseEntryIdValue(123));

        Assert.Contains("nntpbackfilleraccounts.entryid", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Expected GUID or string GUID", ex.Message, StringComparison.Ordinal);
    }
}
