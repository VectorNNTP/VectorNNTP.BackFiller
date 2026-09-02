// <copyright file="RuntimeIdentityGuard.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Identity/RuntimeIdentityGuard: verifies that benchmark measurements execute against the intended build identity.

using System.Reflection;
using System.Runtime.Loader;
using System.Security.Cryptography;
using System.Text;

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Verifies that benchmark execution uses the expected benchmark and production assembly identities.
    /// </summary>
    internal static class RuntimeIdentityGuard
    {
        /// <summary>
        /// Compares expected build provenance with loaded assemblies and throws on hard-identity mismatches.
        /// </summary>
        /// <param name="expected">Expected paths, versions, framework, and architecture.</param>
        /// <param name="runtimeIdentity">Identity captured from the current process.</param>
        /// <exception cref="InvalidOperationException">Thrown when required expectations are missing or identities differ.</exception>
        internal static void EnsureMatches(RuntimeIdentityExpectation expected, RuntimeExecutionIdentity runtimeIdentity)
        {
            if (string.IsNullOrWhiteSpace(expected.ExpectedAssemblyPath) ||
                string.IsNullOrWhiteSpace(expected.ExpectedAssemblyVersion) ||
                string.IsNullOrWhiteSpace(expected.ExpectedFileVersion) ||
                string.IsNullOrWhiteSpace(expected.ExpectedTargetFramework) ||
                string.IsNullOrWhiteSpace(expected.ExpectedArchitecture) ||
                string.IsNullOrWhiteSpace(expected.ExpectedProductionAssemblyPath) ||
                string.IsNullOrWhiteSpace(expected.ExpectedProductionAssemblyVersion) ||
                string.IsNullOrWhiteSpace(expected.ExpectedProductionFileVersion))
            {
                throw new InvalidOperationException(
                    "Runtime identity guard requires expected hard-identity options for executing benchmark assembly and production dependency: --expected-assembly-path, --expected-assembly-version, --expected-file-version, --expected-target-framework, --expected-architecture, --expected-production-assembly-path, --expected-production-assembly-version, and --expected-production-file-version.");
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

            string expectedProductionPath = Path.GetFullPath(expected.ExpectedProductionAssemblyPath);
            Assembly? loadedProductionAssembly = ResolveLoadedProductionAssembly();
            string? loadedProductionAssemblyName = loadedProductionAssembly?.GetName().Name;

            string actualProductionPath = ResolveActualProductionAssemblyPath(runtimeIdentity, loadedProductionAssembly);
            string actualProductionAssemblyVersion = runtimeIdentity.ProductionDependencyAssemblyVersion ?? "(unknown)";
            string actualProductionFileVersion = runtimeIdentity.ProductionDependencyFileVersion ?? "(unknown)";

            if (loadedProductionAssembly is null)
            {
                mismatches.Add("ProductionAssemblyName expected='VectorNNTP.BackFiller' actual='(not loaded)'");
            }
            else if (!string.Equals(loadedProductionAssemblyName, "VectorNNTP.BackFiller", StringComparison.Ordinal))
            {
                mismatches.Add($"ProductionAssemblyName expected='VectorNNTP.BackFiller' actual='{loadedProductionAssemblyName}'");
            }

            string expectedProductionSha256 = "(unavailable)";
            string actualProductionSha256 = "(unavailable)";
            string binaryIdentity = "UNAVAILABLE";

            if (!File.Exists(expectedProductionPath))
            {
                mismatches.Add($"Expected production artifact file not found at path '{expectedProductionPath}'.");
            }
            else
            {
                expectedProductionSha256 = ComputeSha256(expectedProductionPath);
            }

            if (IsUnknownIdentityValue(actualProductionPath))
            {
                mismatches.Add("ProductionAssemblyPath actual='(unknown)' (VectorNNTP.BackFiller dependency not loaded)");
            }
            else if (!File.Exists(actualProductionPath))
            {
                mismatches.Add($"Loaded production assembly file not found at path '{actualProductionPath}'.");
            }
            else
            {
                actualProductionSha256 = ComputeSha256(actualProductionPath);
            }

            if (!string.Equals(expectedProductionSha256, "(unavailable)", StringComparison.Ordinal)
                && !string.Equals(actualProductionSha256, "(unavailable)", StringComparison.Ordinal))
            {
                bool hashesMatch = string.Equals(expectedProductionSha256, actualProductionSha256, StringComparison.OrdinalIgnoreCase);
                binaryIdentity = hashesMatch ? "IDENTICAL" : "DIFFERENT";
                if (!hashesMatch)
                {
                    mismatches.Add($"ProductionBinarySha256 expected='{expectedProductionSha256}' actual='{actualProductionSha256}'");
                }
            }

            if (!string.Equals(expected.ExpectedProductionAssemblyVersion, actualProductionAssemblyVersion, StringComparison.Ordinal))
            {
                mismatches.Add($"ProductionAssemblyVersion expected='{expected.ExpectedProductionAssemblyVersion}' actual='{actualProductionAssemblyVersion}'");
            }

            if (!string.Equals(expected.ExpectedProductionFileVersion, actualProductionFileVersion, StringComparison.Ordinal))
            {
                mismatches.Add($"ProductionFileVersion expected='{expected.ExpectedProductionFileVersion}' actual='{actualProductionFileVersion}'");
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
            message.AppendLine("EXECUTING BENCHMARK ASSEMBLY (EXPECTED):");
            message.AppendLine($"  path={expected.ExpectedAssemblyPath}");
            message.AppendLine($"  assemblyVersion={expected.ExpectedAssemblyVersion}");
            message.AppendLine($"  fileVersion={expected.ExpectedFileVersion}");
            message.AppendLine($"  targetFramework={expected.ExpectedTargetFramework}");
            message.AppendLine($"  architecture={expected.ExpectedArchitecture}");
            message.AppendLine($"  configuration={expected.ExpectedConfiguration ?? "(unspecified)"}");
            message.AppendLine($"  platform={expected.ExpectedPlatform ?? "(unspecified)"}");
            message.AppendLine($"  runtimeIdentifier={expected.ExpectedRuntimeIdentifier ?? "(unspecified)"}");
            message.AppendLine("EXECUTING BENCHMARK ASSEMBLY (ACTUAL):");
            message.AppendLine($"  path={runtimeIdentity.RuntimeAssemblyPath}");
            message.AppendLine($"  assemblyVersion={runtimeIdentity.RuntimeAssemblyVersion}");
            message.AppendLine($"  fileVersion={actualFileVersion}");
            message.AppendLine($"  targetFramework={actualTargetFramework} (normalized={normalizedActualTargetFramework})");
            message.AppendLine($"  architecture={runtimeIdentity.Architecture}");
            message.AppendLine($"  configuration={actualConfiguration}");
            message.AppendLine($"  platform={actualPlatform}");
            message.AppendLine($"  runtimeIdentifier={actualRuntimeIdentifier}");
            message.AppendLine($"  processPath={runtimeIdentity.ProcessPath}");
            message.AppendLine($"  workingDirectory={runtimeIdentity.WorkingDirectory}");
            message.AppendLine("EXPECTED PRODUCTION ARTIFACT:");
            message.AppendLine($"  path={expectedProductionPath}");
            message.AppendLine($"  assemblyVersion={expected.ExpectedProductionAssemblyVersion}");
            message.AppendLine($"  fileVersion={expected.ExpectedProductionFileVersion}");
            message.AppendLine($"  SHA256={expectedProductionSha256}");
            message.AppendLine("ACTUAL LOADED PRODUCTION ASSEMBLY:");
            message.AppendLine($"  name={loadedProductionAssemblyName ?? "(not loaded)"}");
            message.AppendLine($"  path={actualProductionPath}");
            message.AppendLine($"  assemblyVersion={actualProductionAssemblyVersion}");
            message.AppendLine($"  fileVersion={actualProductionFileVersion}");
            message.AppendLine($"  SHA256={actualProductionSha256}");
            message.AppendLine($"BINARY_IDENTITY: {binaryIdentity}");
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

        /// <summary>
        /// Resolves LoadedProductionAssembly.
        /// </summary>
        private static Assembly? ResolveLoadedProductionAssembly()
        {
            return AssemblyLoadContext.Default.Assemblies
                .FirstOrDefault(static assembly => string.Equals(assembly.GetName().Name, "VectorNNTP.BackFiller", StringComparison.Ordinal));
        }

        /// <summary>
        /// Resolves ActualProductionAssemblyPath.
        /// </summary>
        private static string ResolveActualProductionAssemblyPath(RuntimeExecutionIdentity runtimeIdentity, Assembly? loadedProductionAssembly)
        {
            if (loadedProductionAssembly is not null && !string.IsNullOrWhiteSpace(loadedProductionAssembly.Location))
            {
                return Path.GetFullPath(loadedProductionAssembly.Location);
            }

            return IsUnknownIdentityValue(runtimeIdentity.ProductionDependencyPath)
                ? "(unknown)"
                : Path.GetFullPath(runtimeIdentity.ProductionDependencyPath!);
        }

        /// <summary>
        /// Computes Sha256.
        /// </summary>
        private static string ComputeSha256(string filePath)
        {
            using FileStream stream = File.OpenRead(filePath);
            byte[] hash = SHA256.HashData(stream);
            return Convert.ToHexString(hash);
        }
        private static bool IsUnknownIdentityValue(string? value)
        {
            return string.IsNullOrWhiteSpace(value) || string.Equals(value, "(unknown)", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Normalizes TargetFramework.
        /// </summary>
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
}


