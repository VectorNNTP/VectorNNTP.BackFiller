// <copyright file="RuntimeExecutionIdentity.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Identity/RuntimeExecutionIdentity: verifies that benchmark measurements execute against the intended build identity.

using System.Reflection;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the runtime IdentityExpectation record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct RuntimeIdentityExpectation(
    string? ExpectedAssemblyPath,
    string? ExpectedAssemblyVersion,
    string? ExpectedFileVersion,
    string? ExpectedConfiguration,
    string? ExpectedPlatform,
    string? ExpectedTargetFramework,
    string? ExpectedRuntimeIdentifier,
    string? ExpectedArchitecture,
    string? ExpectedProductionAssemblyPath,
    string? ExpectedProductionAssemblyVersion,
    string? ExpectedProductionFileVersion);

/// <summary>
/// Represents the runtime ExecutionIdentity record struct used by the benchmark or regression gate.
/// </summary>
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
    DateTimeOffset? BuildTimestampUtc,
    string? ProductionDependencyPath,
    string? ProductionDependencyAssemblyVersion,
    string? ProductionDependencyFileVersion);

/// <summary>
/// Represents the app BuildConfiguration class used by the benchmark or regression gate.
/// </summary>
internal static class AppBuildConfiguration
{
    /// <summary>
    /// Runs the value benchmark scenario.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("Configuration")
        ?? typeof(AppBuildConfiguration).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration;
}

/// <summary>
/// Represents the app BuildPlatform class used by the benchmark or regression gate.
/// </summary>
internal static class AppBuildPlatform
{
    /// <summary>
    /// Runs the value benchmark scenario.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("Platform");
}

/// <summary>
/// Represents the app TargetFramework class used by the benchmark or regression gate.
/// </summary>
internal static class AppTargetFramework
{
    /// <summary>
    /// Runs the value benchmark scenario.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("TargetFramework")
        ?? typeof(AppTargetFramework).Assembly.GetCustomAttribute<System.Runtime.Versioning.TargetFrameworkAttribute>()?.FrameworkName;
}

/// <summary>
/// Represents the app RuntimeIdentifier class used by the benchmark or regression gate.
/// </summary>
internal static class AppRuntimeIdentifier
{
    /// <summary>
    /// Runs the value benchmark scenario.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("RuntimeIdentifier");
}

/// <summary>
/// Represents the app AssemblyMetadata class used by the benchmark or regression gate.
/// </summary>
internal static class AppAssemblyMetadata
{
    /// <summary>
    /// Gets Value.

    /// </summary>
    internal static string? GetValue(string key)
    {
        return typeof(AppAssemblyMetadata).Assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .FirstOrDefault(metadata => string.Equals(metadata.Key, key, StringComparison.OrdinalIgnoreCase))
            ?.Value;
    }
}
