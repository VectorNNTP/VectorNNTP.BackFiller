using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class RuntimeExecutionIdentityCapture
{
    /// <summary>
    /// Captures runtime identity using the process entry assembly when available.
    /// </summary>
    internal static RuntimeExecutionIdentity Capture(Assembly fallbackAssembly)
    {
        return Capture(fallbackAssembly, Assembly.GetEntryAssembly());
    }

    /// <summary>
    /// Captures runtime identity using an explicit entry assembly override when provided.
    /// </summary>
    internal static RuntimeExecutionIdentity Capture(Assembly fallbackAssembly, Assembly? entryAssembly)
    {
        ArgumentNullException.ThrowIfNull(fallbackAssembly);

        Assembly assembly = entryAssembly ?? fallbackAssembly;
        string runtimeAssemblyPath = Path.GetFullPath(assembly.Location);
        string runtimeAssemblyVersion = assembly.GetName().Version?.ToString() ?? "(unknown)";
        string? assemblyFileVersion = assembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version;

        string processPath = Environment.ProcessPath ?? Process.GetCurrentProcess().MainModule?.FileName ?? "(unknown)";
        string workingDirectory = Environment.CurrentDirectory;

        string? sourceRevision = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        DateTimeOffset? buildTimestampUtc = null;
        if (File.Exists(runtimeAssemblyPath))
        {
            DateTime lastWriteUtc = File.GetLastWriteTimeUtc(runtimeAssemblyPath);
            if (lastWriteUtc != DateTime.MinValue)
            {
                buildTimestampUtc = new DateTimeOffset(lastWriteUtc, TimeSpan.Zero);
            }
        }

        Assembly? productionAssembly = AppDomain.CurrentDomain
            .GetAssemblies()
            .FirstOrDefault(static x => string.Equals(x.GetName().Name, "VectorNNTP.BackFiller", StringComparison.Ordinal))
            ?? typeof(TransitPublisher).Assembly;

        string? productionDependencyPath = null;
        string? productionDependencyAssemblyVersion = null;
        string? productionDependencyFileVersion = null;

        if (productionAssembly is not null)
        {
            productionDependencyPath = Path.GetFullPath(productionAssembly.Location);
            productionDependencyAssemblyVersion = productionAssembly.GetName().Version?.ToString();
            productionDependencyFileVersion = productionAssembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version;
        }

        return new RuntimeExecutionIdentity(
            RuntimeAssemblyPath: runtimeAssemblyPath,
            RuntimeAssemblyVersion: runtimeAssemblyVersion,
            AssemblyFileVersion: assemblyFileVersion,
            ProcessPath: processPath,
            WorkingDirectory: workingDirectory,
            Configuration: AppBuildConfiguration.Value,
            Platform: AppBuildPlatform.Value,
            TargetFramework: AppTargetFramework.Value,
            RuntimeIdentifier: AppRuntimeIdentifier.Value,
            Architecture: RuntimeInformation.ProcessArchitecture.ToString(),
            SourceRevision: sourceRevision,
            BuildTimestampUtc: buildTimestampUtc,
            ProductionDependencyPath: productionDependencyPath,
            ProductionDependencyAssemblyVersion: productionDependencyAssemblyVersion,
            ProductionDependencyFileVersion: productionDependencyFileVersion);
    }
}
