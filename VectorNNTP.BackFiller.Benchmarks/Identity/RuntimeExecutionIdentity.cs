// <copyright file="RuntimeExecutionIdentity.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Identity/RuntimeExecutionIdentity: verifies that benchmark measurements execute against the intended build identity.

using System.Reflection;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the runtime IdentityExpectation record struct for benchmark or isolated-regression execution.
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
/// Defines the runtime ExecutionIdentity record struct for benchmark or isolated-regression execution.
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
/// Defines the app BuildConfiguration class for benchmark or isolated-regression execution.
/// </summary>
internal static class AppBuildConfiguration
{
    /// <summary>
    /// Performs the value operation.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("Configuration")
        ?? typeof(AppBuildConfiguration).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration;
}

/// <summary>
/// Defines the app BuildPlatform class for benchmark or isolated-regression execution.
/// </summary>
internal static class AppBuildPlatform
{
    /// <summary>
    /// Performs the value operation.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("Platform");
}

/// <summary>
/// Defines the app TargetFramework class for benchmark or isolated-regression execution.
/// </summary>
internal static class AppTargetFramework
{
    /// <summary>
    /// Performs the value operation.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("TargetFramework")
        ?? typeof(AppTargetFramework).Assembly.GetCustomAttribute<System.Runtime.Versioning.TargetFrameworkAttribute>()?.FrameworkName;
}

/// <summary>
/// Defines the app RuntimeIdentifier class for benchmark or isolated-regression execution.
/// </summary>
internal static class AppRuntimeIdentifier
{
    /// <summary>
    /// Performs the value operation.
    /// </summary>
    internal static readonly string? Value = AppAssemblyMetadata.GetValue("RuntimeIdentifier");
}

/// <summary>
/// Defines the app AssemblyMetadata class for benchmark or isolated-regression execution.
/// </summary>
internal static class AppAssemblyMetadata
{
    /// <summary>
    /// Performs the get Value operation.
    /// </summary>
    internal static string? GetValue(string key)
    {
        return typeof(AppAssemblyMetadata).Assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .FirstOrDefault(metadata => string.Equals(metadata.Key, key, StringComparison.OrdinalIgnoreCase))
            ?.Value;
    }
}
