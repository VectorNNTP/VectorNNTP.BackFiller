using System.Text;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class RuntimeIdentityGuard
{
    internal static void EnsureMatches(RuntimeIdentityExpectation expected, RuntimeExecutionIdentity runtimeIdentity)
    {
        if (string.IsNullOrWhiteSpace(expected.ExpectedAssemblyPath) ||
            string.IsNullOrWhiteSpace(expected.ExpectedAssemblyVersion) ||
            string.IsNullOrWhiteSpace(expected.ExpectedFileVersion) ||
            string.IsNullOrWhiteSpace(expected.ExpectedTargetFramework) ||
            string.IsNullOrWhiteSpace(expected.ExpectedArchitecture))
        {
            throw new InvalidOperationException(
                "Runtime identity guard requires expected hard-identity options: --expected-assembly-path, --expected-assembly-version, --expected-file-version, --expected-target-framework, and --expected-architecture.");
        }

        List<string> mismatches = [];
        List<string> provenanceNotes = [];

        string expectedPath = Path.GetFullPath(expected.ExpectedAssemblyPath);
        string actualPath = Path.GetFullPath(runtimeIdentity.RuntimeAssemblyPath);
        if (!string.Equals(expectedPath, actualPath, StringComparison.OrdinalIgnoreCase))
        {
            mismatches.Add($"AssemblyPath expected='{expectedPath}' actual='{actualPath}'");
        }

        if (!string.Equals(expected.ExpectedAssemblyVersion, runtimeIdentity.RuntimeAssemblyVersion, StringComparison.Ordinal))
        {
            mismatches.Add($"AssemblyVersion expected='{expected.ExpectedAssemblyVersion}' actual='{runtimeIdentity.RuntimeAssemblyVersion}'");
        }

        string actualFileVersion = runtimeIdentity.AssemblyFileVersion ?? "(unknown)";
        if (!string.Equals(expected.ExpectedFileVersion, actualFileVersion, StringComparison.Ordinal))
        {
            mismatches.Add($"FileVersion expected='{expected.ExpectedFileVersion}' actual='{actualFileVersion}'");
        }

        string actualConfiguration = runtimeIdentity.Configuration ?? "(unknown)";
        if (!string.IsNullOrWhiteSpace(expected.ExpectedConfiguration))
        {
            if (IsUnknownIdentityValue(actualConfiguration))
            {
                provenanceNotes.Add($"Configuration expected='{expected.ExpectedConfiguration}' actual='(unknown)' (treated as build provenance)");
            }
            else if (!string.Equals(expected.ExpectedConfiguration, actualConfiguration, StringComparison.OrdinalIgnoreCase))
            {
                provenanceNotes.Add($"Configuration expected='{expected.ExpectedConfiguration}' actual='{actualConfiguration}' (treated as build provenance)");
            }
        }

        string actualPlatform = runtimeIdentity.Platform ?? "(unknown)";
        if (!string.IsNullOrWhiteSpace(expected.ExpectedPlatform))
        {
            if (IsUnknownIdentityValue(actualPlatform))
            {
                provenanceNotes.Add($"Platform expected='{expected.ExpectedPlatform}' actual='(unknown)' (treated as build provenance)");
            }
            else if (!string.Equals(expected.ExpectedPlatform, actualPlatform, StringComparison.OrdinalIgnoreCase))
            {
                provenanceNotes.Add($"Platform expected='{expected.ExpectedPlatform}' actual='{actualPlatform}' (treated as build provenance)");
            }
        }

        string actualTargetFramework = runtimeIdentity.TargetFramework ?? "(unknown)";
        string normalizedExpectedTargetFramework = NormalizeTargetFramework(expected.ExpectedTargetFramework);
        string normalizedActualTargetFramework = NormalizeTargetFramework(actualTargetFramework);
        if (!string.Equals(normalizedExpectedTargetFramework, normalizedActualTargetFramework, StringComparison.OrdinalIgnoreCase))
        {
            mismatches.Add($"TargetFramework expected='{expected.ExpectedTargetFramework}' (normalized='{normalizedExpectedTargetFramework}') actual='{actualTargetFramework}' (normalized='{normalizedActualTargetFramework}')");
        }

        string actualRuntimeIdentifier = runtimeIdentity.RuntimeIdentifier ?? "(unknown)";
        if (!string.IsNullOrWhiteSpace(expected.ExpectedRuntimeIdentifier))
        {
            if (IsUnknownIdentityValue(actualRuntimeIdentifier))
            {
                provenanceNotes.Add($"RuntimeIdentifier expected='{expected.ExpectedRuntimeIdentifier}' actual='(unknown)' (treated as build provenance)");
            }
            else if (!string.Equals(expected.ExpectedRuntimeIdentifier, actualRuntimeIdentifier, StringComparison.OrdinalIgnoreCase))
            {
                provenanceNotes.Add($"RuntimeIdentifier expected='{expected.ExpectedRuntimeIdentifier}' actual='{actualRuntimeIdentifier}' (treated as build provenance)");
            }
        }

        if (!string.Equals(expected.ExpectedArchitecture, runtimeIdentity.Architecture, StringComparison.OrdinalIgnoreCase))
        {
            mismatches.Add($"Architecture expected='{expected.ExpectedArchitecture}' actual='{runtimeIdentity.Architecture}'");
        }

        if (mismatches.Count == 0)
        {
            if (provenanceNotes.Count > 0)
            {
                Console.WriteLine("Runtime identity provenance notes:");
                foreach (string note in provenanceNotes)
                {
                    Console.WriteLine($"  {note}");
                }
            }

            return;
        }

        StringBuilder message = new();
        message.AppendLine("Runtime identity mismatch detected. ABORTING before warmup/measurement.");
        message.AppendLine("EXPECTED:");
        message.AppendLine($"  path={expected.ExpectedAssemblyPath}");
        message.AppendLine($"  assemblyVersion={expected.ExpectedAssemblyVersion}");
        message.AppendLine($"  fileVersion={expected.ExpectedFileVersion}");
        message.AppendLine($"  configuration={expected.ExpectedConfiguration ?? "(unspecified)"}");
        message.AppendLine($"  platform={expected.ExpectedPlatform ?? "(unspecified)"}");
        message.AppendLine($"  targetFramework={expected.ExpectedTargetFramework}");
        message.AppendLine($"  runtimeIdentifier={expected.ExpectedRuntimeIdentifier ?? "(unspecified)"}");
        message.AppendLine($"  architecture={expected.ExpectedArchitecture}");
        message.AppendLine("ACTUAL:");
        message.AppendLine($"  path={runtimeIdentity.RuntimeAssemblyPath}");
        message.AppendLine($"  assemblyVersion={runtimeIdentity.RuntimeAssemblyVersion}");
        message.AppendLine($"  fileVersion={actualFileVersion}");
        message.AppendLine($"  configuration={actualConfiguration}");
        message.AppendLine($"  platform={actualPlatform}");
        message.AppendLine($"  targetFramework={actualTargetFramework} (normalized={normalizedActualTargetFramework})");
        message.AppendLine($"  runtimeIdentifier={actualRuntimeIdentifier}");
        message.AppendLine($"  architecture={runtimeIdentity.Architecture}");
        message.AppendLine($"  processPath={runtimeIdentity.ProcessPath}");
        message.AppendLine($"  workingDirectory={runtimeIdentity.WorkingDirectory}");
        message.AppendLine("MISMATCH DETAILS:");
        foreach (string mismatch in mismatches)
        {
            message.AppendLine($"  - {mismatch}");
        }

        if (provenanceNotes.Count > 0)
        {
            message.AppendLine("PROVENANCE NOTES:");
            foreach (string note in provenanceNotes)
            {
                message.AppendLine($"  - {note}");
            }
        }

        throw new InvalidOperationException(message.ToString());
    }

    private static bool IsUnknownIdentityValue(string? value)
    {
        return string.IsNullOrWhiteSpace(value) || string.Equals(value, "(unknown)", StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeTargetFramework(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "(unknown)";
        }

        string candidate = value.Trim();
        if (candidate.StartsWith("net", StringComparison.OrdinalIgnoreCase))
        {
            return candidate.ToLowerInvariant();
        }

        const string prefix = ".NETCoreApp,Version=v";
        if (candidate.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            string version = candidate[prefix.Length..];
            if (version.Length > 0)
            {
                return "net" + version;
            }
        }

        return candidate.ToLowerInvariant();
    }
}
