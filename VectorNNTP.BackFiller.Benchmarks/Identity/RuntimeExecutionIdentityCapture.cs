using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class RuntimeExecutionIdentityCapture
{
    internal static RuntimeExecutionIdentity Capture(Assembly fallbackAssembly)
    {
        ArgumentNullException.ThrowIfNull(fallbackAssembly);

        Assembly assembly = Assembly.GetEntryAssembly() ?? fallbackAssembly;
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
            BuildTimestampUtc: buildTimestampUtc);
    }
}
