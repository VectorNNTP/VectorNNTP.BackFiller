using System.Reflection;

namespace VectorNNTP.BackFiller.Benchmarks;

internal readonly record struct RuntimeIdentityExpectation(
    string? ExpectedAssemblyPath,
    string? ExpectedAssemblyVersion,
    string? ExpectedFileVersion,
    string? ExpectedConfiguration,
    string? ExpectedPlatform,
    string? ExpectedTargetFramework,
    string? ExpectedRuntimeIdentifier,
    string? ExpectedArchitecture);

internal readonly record struct RuntimeExecutionIdentity(
    string RuntimeAssemblyPath,
    string RuntimeAssemblyVersion,
    string? AssemblyFileVersion,
    string ProcessPath,
    string WorkingDirectory,
    string? Configuration,
    string? Platform,
    string? TargetFramework,
    string? RuntimeIdentifier,
    string Architecture,
    string? SourceRevision,
    DateTimeOffset? BuildTimestampUtc);

internal static class AppBuildConfiguration
{
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("Configuration")
        ?? typeof(AppBuildConfiguration).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration;
}

internal static class AppBuildPlatform
{
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("Platform");
}

internal static class AppTargetFramework
{
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("TargetFramework")
        ?? typeof(AppTargetFramework).Assembly.GetCustomAttribute<System.Runtime.Versioning.TargetFrameworkAttribute>()?.FrameworkName;
}

internal static class AppRuntimeIdentifier
{
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("RuntimeIdentifier");
}

internal static class AppAssemblyMetadata
{
    internal static string? GetValue(string key)
    {
        return typeof(AppAssemblyMetadata).Assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .FirstOrDefault(metadata => string.Equals(metadata.Key, key, StringComparison.OrdinalIgnoreCase))
            ?.Value;
    }
}
