using System.Reflection;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Validates runtime identity guard semantics for benchmark assembly and production dependency provenance.
/// </summary>
public sealed class RuntimeIdentityGuardTests
{
    [Fact]
    public void EnsureMatches_WhenBenchmarkAndProductionDependencyMatch_DoesNotThrow()
    {
        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: typeof(TransitPublisher).Assembly.Location,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity);
    }

    [Fact]
    public void EnsureMatches_WhenBenchmarkAssemblyPathDiffers_ThrowsWithBenchmarkAssemblyLabel()
    {
        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedAssemblyPath: @"C:\other\VectorNNTP.BackFiller.Benchmarks.dll",
            expectedProductionAssemblyPath: typeof(TransitPublisher).Assembly.Location,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: GetLoadedProductionFileVersion());

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity));

        Assert.Contains("EXECUTING BENCHMARK ASSEMBLY", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureMatches_WhenProductionDependencyIsCopiedButBinaryIdentical_DoesNotThrow()
    {
        string sourcePath = typeof(TransitPublisher).Assembly.Location;
        string copiedPath = Path.Combine(Path.GetTempPath(), $"VectorNNTP.BackFiller-copy-{Guid.NewGuid():N}.dll");
        File.Copy(sourcePath, copiedPath, overwrite: true);

        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: copiedPath,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: GetLoadedProductionFileVersion());

        try
        {
            RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity);
        }
        finally
        {
            File.Delete(copiedPath);
        }
    }

    [Fact]
    public void EnsureMatches_WhenProductionBinaryDiffersButVersionsMatch_ThrowsWithBinaryIdentityDetails()
    {
        string sourcePath = typeof(TransitPublisher).Assembly.Location;
        string mutatedPath = Path.Combine(Path.GetTempPath(), $"VectorNNTP.BackFiller-mutated-{Guid.NewGuid():N}.dll");
        File.Copy(sourcePath, mutatedPath, overwrite: true);

        byte[] bytes = File.ReadAllBytes(mutatedPath);
        bytes[bytes.Length - 1] ^= 0xFF;
        File.WriteAllBytes(mutatedPath, bytes);

        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: mutatedPath,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: GetLoadedProductionFileVersion());

        try
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity));

            Assert.Contains("EXPECTED PRODUCTION ARTIFACT", ex.Message, StringComparison.Ordinal);
            Assert.Contains("ACTUAL LOADED PRODUCTION ASSEMBLY", ex.Message, StringComparison.Ordinal);
            Assert.Contains("BINARY_IDENTITY: DIFFERENT", ex.Message, StringComparison.Ordinal);
            Assert.Contains("ProductionBinarySha256", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(mutatedPath);
        }
    }

    [Fact]
    public void EnsureMatches_WhenProductionAssemblyVersionDiffers_Throws()
    {
        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: typeof(TransitPublisher).Assembly.Location,
            expectedProductionAssemblyVersion: "9.9.9.9",
            expectedProductionFileVersion: GetLoadedProductionFileVersion());

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity));
        Assert.Contains("ProductionAssemblyVersion", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureMatches_WhenProductionFileVersionDiffers_Throws()
    {
        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: typeof(TransitPublisher).Assembly.Location,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: "9.9.9.9");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity));
        Assert.Contains("ProductionFileVersion", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureMatches_WhenTargetFrameworkDiffers_Throws()
    {
        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: typeof(TransitPublisher).Assembly.Location,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: GetLoadedProductionFileVersion(),
            expectedTargetFramework: "net9.0");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity));
        Assert.Contains("TargetFramework", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureMatches_WhenArchitectureDiffers_Throws()
    {
        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: typeof(TransitPublisher).Assembly.Location,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: GetLoadedProductionFileVersion(),
            expectedArchitecture: "Arm64");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity));
        Assert.Contains("Architecture", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureMatches_WhenExpectedProductionArtifactMissing_ThrowsClearly()
    {
        RuntimeExecutionIdentity runtimeIdentity = CreateRuntimeIdentity(
            productionDependencyAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            productionDependencyFileVersion: GetLoadedProductionFileVersion());

        string missingExpectedPath = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.dll");

        RuntimeIdentityExpectation expected = CreateExpectation(
            expectedProductionAssemblyPath: missingExpectedPath,
            expectedProductionAssemblyVersion: GetLoadedProductionAssemblyVersion(),
            expectedProductionFileVersion: GetLoadedProductionFileVersion());

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => RuntimeIdentityGuard.EnsureMatches(expected, runtimeIdentity));

        Assert.Contains("Expected production artifact file not found", ex.Message, StringComparison.Ordinal);
    }

    private static RuntimeExecutionIdentity CreateRuntimeIdentity(
        string runtimeAssemblyPath = @"C:\bench\VectorNNTP.BackFiller.Benchmarks.dll",
        string runtimeAssemblyVersion = "1.1.230.6262",
        string runtimeFileVersion = "1.1.230.6262",
        string productionDependencyPath = @"C:\bench\VectorNNTP.BackFiller.dll",
        string? productionDependencyAssemblyVersion = null,
        string? productionDependencyFileVersion = null)
    {
        return new RuntimeExecutionIdentity(
            RuntimeAssemblyPath: runtimeAssemblyPath,
            RuntimeAssemblyVersion: runtimeAssemblyVersion,
            AssemblyFileVersion: runtimeFileVersion,
            ProcessPath: @"C:\bench\VectorNNTP.BackFiller.Benchmarks.exe",
            WorkingDirectory: @"C:\bench",
            Configuration: "Release",
            Platform: "x64",
            TargetFramework: "net8.0",
            RuntimeIdentifier: "win-x64",
            Architecture: "X64",
            SourceRevision: "phase3",
            BuildTimestampUtc: DateTimeOffset.UtcNow,
            ProductionDependencyPath: productionDependencyPath,
            ProductionDependencyAssemblyVersion: productionDependencyAssemblyVersion,
            ProductionDependencyFileVersion: productionDependencyFileVersion);
    }

    private static RuntimeIdentityExpectation CreateExpectation(
        string expectedAssemblyPath = @"C:\bench\VectorNNTP.BackFiller.Benchmarks.dll",
        string expectedAssemblyVersion = "1.1.230.6262",
        string expectedFileVersion = "1.1.230.6262",
        string expectedProductionAssemblyPath = @"C:\bench\VectorNNTP.BackFiller.dll",
        string expectedProductionAssemblyVersion = "1.1.230.6262",
        string expectedProductionFileVersion = "1.1.230.6262",
        string expectedTargetFramework = "net8.0",
        string expectedArchitecture = "X64")
    {
        return new RuntimeIdentityExpectation(
            ExpectedAssemblyPath: expectedAssemblyPath,
            ExpectedAssemblyVersion: expectedAssemblyVersion,
            ExpectedFileVersion: expectedFileVersion,
            ExpectedConfiguration: "Release",
            ExpectedPlatform: "x64",
            ExpectedTargetFramework: expectedTargetFramework,
            ExpectedRuntimeIdentifier: "win-x64",
            ExpectedArchitecture: expectedArchitecture,
            ExpectedProductionAssemblyPath: expectedProductionAssemblyPath,
            ExpectedProductionAssemblyVersion: expectedProductionAssemblyVersion,
            ExpectedProductionFileVersion: expectedProductionFileVersion);
    }

    private static string GetLoadedProductionAssemblyVersion()
    {
        return typeof(TransitPublisher).Assembly.GetName().Version?.ToString() ?? "(unknown)";
    }

    private static string GetLoadedProductionFileVersion()
    {
        return typeof(TransitPublisher).Assembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version ?? "(unknown)";
    }
}
