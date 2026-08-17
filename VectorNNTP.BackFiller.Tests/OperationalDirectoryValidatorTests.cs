using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests for startup validation of operational directories.
/// </summary>
public class OperationalDirectoryValidatorTests
{
    [Fact]
    public void ResolveAndValidateLogDirectory_WhenConfiguredPathIsExistingFile_ThrowsInvalidOperationException()
    {
        string tempRoot = Path.Combine(Path.GetTempPath(), $"vectornntp-validator-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        string occupiedPath = Path.Combine(tempRoot, "occupied-path");
        File.WriteAllText(occupiedPath, "this is a file, not a directory");

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BackFiller:DirLogs"] = occupiedPath,
            })
            .Build();

        try
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
                OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration));

            Assert.Contains("BackFiller:DirLogs", ex.Message, StringComparison.Ordinal);
            Assert.Contains("could not be created", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
            {
                Directory.Delete(tempRoot, recursive: true);
            }
        }
    }

    [Fact]
    public void ResolveAndValidateLogDirectory_WhenDirectorySupportsRequiredFileOperations_Succeeds()
    {
        string uniqueRootName = $"validator-nested-{Guid.NewGuid():N}";
        string relativePath = Path.Combine(uniqueRootName, "logs", "backfiller", "runtime");
        string expectedPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, relativePath));
        string cleanupRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, uniqueRootName));

        if (Directory.Exists(cleanupRoot))
        {
            Directory.Delete(cleanupRoot, recursive: true);
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BackFiller:DirLogs"] = relativePath,
            })
            .Build();

        try
        {
            string resolvedPath = OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration);

            Assert.Equal(expectedPath, resolvedPath);
            Assert.True(Directory.Exists(expectedPath));
        }
        finally
        {
            if (Directory.Exists(cleanupRoot))
            {
                Directory.Delete(cleanupRoot, recursive: true);
            }
        }
    }

    [Fact]
    public void ResolveAndValidateLogDirectory_WhenConfiguredPathHasSurroundingWhitespace_TrimsBeforeResolving()
    {
        string uniqueRootName = $"validator-trim-{Guid.NewGuid():N}";
        string relativePath = Path.Combine(uniqueRootName, "logs");
        string configuredValueWithWhitespace = $"  {relativePath}  ";
        string expectedPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, relativePath));
        string cleanupRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, uniqueRootName));

        if (Directory.Exists(cleanupRoot))
        {
            Directory.Delete(cleanupRoot, recursive: true);
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BackFiller:DirLogs"] = configuredValueWithWhitespace,
            })
            .Build();

        try
        {
            string resolvedPath = OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration);

            Assert.Equal(expectedPath, resolvedPath);
            Assert.True(Directory.Exists(expectedPath));
        }
        finally
        {
            if (Directory.Exists(cleanupRoot))
            {
                Directory.Delete(cleanupRoot, recursive: true);
            }
        }
    }

    [Fact]
    public void ResolveAndValidateLogDirectory_WhenConfigurationIsMissing_ThrowsInvalidOperationException()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration));

        Assert.Contains("BackFiller:DirLogs", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolveAndValidateLogDirectory_WhenConfigurationIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            OperationalDirectoryValidator.ResolveAndValidateLogDirectory(null!));
    }

    [Fact]
    public void ResolveAndValidateCertificateDirectory_WhenConfigurationIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            OperationalDirectoryValidator.ResolveAndValidateCertificateDirectory(null!));
    }

    [Fact]
    public void ResolveAndValidateCertificateDirectory_WhenConfigurationIsMissing_ReportsCorrectSetting()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            OperationalDirectoryValidator.ResolveAndValidateCertificateDirectory(configuration));

        Assert.Contains("BackFiller:DirCerts", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   ")]
    public void ResolveAndValidateLogDirectory_WhenConfiguredPathIsWhitespace_ThrowsInvalidOperationException(
        string configuredPath)
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BackFiller:DirLogs"] = configuredPath,
            })
            .Build();

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration));

        Assert.Contains("BackFiller:DirLogs", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolveAndValidateLogDirectory_WhenConfiguredPathIsAbsolute_ReturnsCanonicalAbsolutePath()
    {
        string tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"vectornntp-absolute-{Guid.NewGuid():N}");

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BackFiller:DirLogs"] = tempRoot,
            })
            .Build();

        try
        {
            string resolved = OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration);

            Assert.Equal(Path.GetFullPath(tempRoot), resolved);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
            {
                Directory.Delete(tempRoot, recursive: true);
            }
        }
    }

    [Fact]
    public void ResolveAndValidateLogDirectory_WhenDirectoryAlreadyExists_Succeeds()
    {
        string tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"vectornntp-existing-{Guid.NewGuid():N}");

        Directory.CreateDirectory(tempRoot);

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BackFiller:DirLogs"] = tempRoot,
            })
            .Build();

        try
        {
            string resolved = OperationalDirectoryValidator.ResolveAndValidateLogDirectory(configuration);

            Assert.Equal(Path.GetFullPath(tempRoot), resolved);
        }
        finally
        {
            if (Directory.Exists(tempRoot))
            {
                Directory.Delete(tempRoot, recursive: true);
            }
        }
    }

    [Fact]
    public void ResolveAndValidateCertificateDirectory_WhenConfiguredPathIsValid_ReturnsCanonicalAbsolutePath()
    {
        string tempRoot = Path.Combine(
            Path.GetTempPath(),
            $"vectornntp-cert-validator-{Guid.NewGuid():N}");

        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["BackFiller:DirCerts"] = tempRoot,
            })
            .Build();

        try
        {
            string resolved = OperationalDirectoryValidator.ResolveAndValidateCertificateDirectory(configuration);

            Assert.Equal(Path.GetFullPath(tempRoot), resolved);
            Assert.True(Directory.Exists(resolved));
        }
        finally
        {
            if (Directory.Exists(tempRoot))
            {
                Directory.Delete(tempRoot, recursive: true);
            }
        }
    }
}
