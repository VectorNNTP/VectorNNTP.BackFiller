// <copyright file="BuildInfo.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: build info in the vector nntp.back filler subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// BuildInfo.cs — Application version and runtime metadata.
//
// Contains immutable identity details for the current executable plus runtime
// environment details resolved at process startup.
//
// Thread safety: All properties are immutable; safe for concurrent access.
//
// Usage:
//   - Create at startup: BuildInfo.Create(processStartedAt)
//   - Query version: buildInfo.Version
//   - Display diagnostic snapshot: buildInfo.GetVersionString()
//   - Log build metadata: logger.LogInformation("Build: {BuildInfo}", buildInfo)
//   - Inject into services: services.AddSingleton(buildInfo)

using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace VectorNNTP.Backfiller
{
    /// <summary>
    /// Immutable application identity and runtime metadata.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Build metadata is resolved from assembly attributes so the executable is its own source of truth.
    /// Runtime metadata is collected at startup and describes the current host environment.
    /// </para>
    /// </remarks>
    public sealed class BuildInfo
    {
        /// <summary>
        /// Represents unknown or unavailable metadata values.
        /// </summary>
        private const string UnknownValue = "unknown";

        /// <summary>
        /// Tracks metadata token separators for build info.
        /// </summary>
        private static readonly char[] MetadataTokenSeparators = ['.', '-', '_'];

        /// <summary>
        /// Gets the canonical service name.
        /// </summary>
        public const string ServiceName = "VectorNNTP.BackFiller";

        /// <summary>
        /// Creates a build information snapshot using an explicit process start timestamp.
        /// </summary>
        /// <param name="processStartedAt">The process start timestamp.</param>
        /// <returns>A populated <see cref="BuildInfo"/> instance.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="processStartedAt"/> is the default value.</exception>
        public static BuildInfo Create(DateTimeOffset processStartedAt)
        {
            return processStartedAt == default
                ? throw new ArgumentException("Process start timestamp must be specified.", nameof(processStartedAt))
                : LoadCurrent(processStartedAt.ToUniversalTime());
        }

        /// <summary>
        /// Gets the service name associated with this metadata snapshot.
        /// </summary>
        public string Service { get; init; } = ServiceName;

        /// <summary>
        /// Gets the semantic version component derived from assembly metadata.
        /// </summary>
        /// <remarks>
        /// This value is derived from the portion of <see cref="InformationalVersion"/> before build metadata
        /// (<c>+</c>) when available; otherwise it falls back to assembly version information.
        /// This class does not perform runtime semantic-version validation.
        /// </remarks>
        public string Version { get; init; } = UnknownValue;

        /// <summary>
        /// Gets the assembly informational version.
        /// </summary>
        /// <remarks>
        /// Usually contains semantic version plus optional build metadata.
        /// Example: <c>1.4.2+8e17a2f91d8e0c7f3f9ce6f9b4dceef4f5af355b</c>.
        /// </remarks>
        public string InformationalVersion { get; init; } = UnknownValue;

        /// <summary>
        /// Gets the assembly version.
        /// </summary>
        /// <remarks>
        /// Often kept stable for binding compatibility (for example, major-only versioning).
        /// </remarks>
        public string AssemblyVersion { get; init; } = UnknownValue;

        /// <summary>
        /// Gets the file version.
        /// </summary>
        /// <remarks>
        /// Commonly aligned to release versioning for deployment diagnostics.
        /// </remarks>
        public string FileVersion { get; init; } = UnknownValue;

        /// <summary>
        /// Gets the repository commit SHA for the build.
        /// </summary>
        /// <remarks>
        /// Prefer full 40-character SHA values for uniqueness.
        /// </remarks>
        public string Commit { get; init; } = UnknownValue;

        /// <summary>
        /// Gets the repository dirty state for the build.
        /// </summary>
        /// <remarks>
        /// <see langword="true"/> indicates a dirty build, <see langword="false"/> indicates a clean build,
        /// and <see langword="null"/> indicates the dirty state is unknown.
        /// </remarks>
        public bool? IsDirty { get; init; }

        /// <summary>
        /// Gets the build timestamp supplied by the build pipeline.
        /// </summary>
        /// <remarks>
        /// This value is read from assembly metadata and is never synthesized from process startup time.
        /// The expected format is ISO 8601 UTC, but the value is trusted as provided by CI/CD metadata.
        /// When unavailable, the value is <c>unknown</c>.
        /// </remarks>
        public string BuildTimestamp { get; init; } = UnknownValue;

        /// <summary>
        /// Gets the process start timestamp (UTC).
        /// </summary>
        public DateTimeOffset StartedAt { get; init; }

        /// <summary>
        /// Gets the build configuration (Debug, Release, or custom).
        /// </summary>
        public string BuildConfiguration { get; init; } = UnknownValue;

        /// <summary>
        /// Gets the target framework moniker this assembly was built against.
        /// </summary>
        public string TargetFramework { get; init; } = UnknownValue;

        /// <summary>
        /// Gets runtime details for the current process.
        /// </summary>
        public RuntimeDetails Runtime { get; init; } = RuntimeDetails.CreateCurrent();

        /// <summary>
        /// Gets the .NET runtime description.
        /// </summary>
        /// <remarks>
        /// Kept for compatibility with existing call sites that expect <c>DotNetVersion</c>.
        /// </remarks>
        public string DotNetVersion => Runtime.Framework;

        /// <summary>
        /// Loads current build information from assembly and runtime.
        /// </summary>
        /// <param name="processStartedAt">The process start timestamp (UTC).</param>
        /// <returns>A populated <see cref="BuildInfo"/> instance.</returns>
        private static BuildInfo LoadCurrent(DateTimeOffset processStartedAt)
        {
            Assembly assembly = typeof(BuildInfo).Assembly;
            IReadOnlyDictionary<string, string> metadata = GetAssemblyMetadataMap(assembly);
            string informationalVersion = ResolveInformationalVersion(assembly);
            string assemblyVersion = ResolveAssemblyVersion(assembly);
            string fileVersion = ResolveFileVersion(assembly);
            string version = ResolveVersion(informationalVersion, assemblyVersion);
            string commit = ResolveCommit(metadata, informationalVersion);
            bool? isDirty = ResolveDirtyState(metadata, informationalVersion);
            string buildTimestamp = ResolveBuildTimestamp(metadata);
            string buildConfiguration = ResolveBuildConfiguration(assembly);
            string targetFramework = ResolveTargetFramework(assembly);
            RuntimeDetails runtime = RuntimeDetails.CreateCurrent();

            return new BuildInfo
            {
                Service = ServiceName,
                Version = version,
                InformationalVersion = informationalVersion,
                AssemblyVersion = assemblyVersion,
                FileVersion = fileVersion,
                Commit = commit,
                IsDirty = isDirty,
                BuildTimestamp = buildTimestamp,
                StartedAt = processStartedAt,
                BuildConfiguration = buildConfiguration,
                TargetFramework = targetFramework,
                Runtime = runtime,
            };
        }

        /// <summary>
        /// Resolves the assembly informational version value.
        /// </summary>
        /// <param name="assembly">The assembly to inspect.</param>
        /// <returns>The informational version or <c>unknown</c>.</returns>
        private static string ResolveInformationalVersion(Assembly assembly)
        {
            AssemblyInformationalVersionAttribute? attribute = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>();
            string? informationalVersion = attribute?.InformationalVersion;

            return string.IsNullOrWhiteSpace(informationalVersion) ? UnknownValue : informationalVersion;
        }

        /// <summary>
        /// Resolves the assembly version value.
        /// </summary>
        /// <param name="assembly">The assembly to inspect.</param>
        /// <returns>The assembly version or <c>unknown</c>.</returns>
        private static string ResolveAssemblyVersion(Assembly assembly)
        {
            Version? assemblyVersion = assembly.GetName().Version;
            return assemblyVersion?.ToString() ?? UnknownValue;
        }

        /// <summary>
        /// Resolves the file version value.
        /// </summary>
        /// <param name="assembly">The assembly to inspect.</param>
        /// <returns>The file version or <c>unknown</c>.</returns>
        private static string ResolveFileVersion(Assembly assembly)
        {
            AssemblyFileVersionAttribute? fileVersionAttribute = assembly.GetCustomAttribute<AssemblyFileVersionAttribute>();
            string? fileVersion = fileVersionAttribute?.Version;

            return string.IsNullOrWhiteSpace(fileVersion) ? UnknownValue : fileVersion;
        }

        /// <summary>
        /// Resolves the semantic version from informational or assembly version values.
        /// </summary>
        /// <param name="informationalVersion">The resolved informational version.</param>
        /// <param name="assemblyVersion">The resolved assembly version.</param>
        /// <returns>The semantic version string.</returns>
        private static string ResolveVersion(string informationalVersion, string assemblyVersion)
        {
            if (!string.Equals(informationalVersion, UnknownValue, StringComparison.Ordinal))
            {
                string[] segments = informationalVersion.Split('+', 2, StringSplitOptions.None);
                if (!string.IsNullOrWhiteSpace(segments[0]))
                {
                    return segments[0];
                }
            }

            return assemblyVersion;
        }

        /// <summary>
        /// Resolves the commit SHA, preferring authoritative assembly metadata.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <c>SourceRevisionId</c> is the authoritative source and should be stamped by the build pipeline.
        /// Values are accepted only when they are valid 40-character hexadecimal Git SHAs.
        /// </para>
        /// <para>
        /// Informational version token parsing exists only as a compatibility fallback for artifacts
        /// that do not provide commit metadata attributes.
        /// </para>
        /// </remarks>
        /// <param name="metadata">The assembly metadata map.</param>
        /// <param name="informationalVersion">The resolved informational version.</param>
        /// <returns>The commit SHA value when available.</returns>
        private static string ResolveCommit(IReadOnlyDictionary<string, string> metadata, string informationalVersion)
        {
            string? sourceRevisionId = TryGetAssemblyMetadataValue(metadata, "SourceRevisionId");
            if (!string.IsNullOrWhiteSpace(sourceRevisionId) && IsHexString(sourceRevisionId, 40))
            {
                return sourceRevisionId;
            }

            string? commitSha = TryGetAssemblyMetadataValue(metadata, "CommitSha");
            if (!string.IsNullOrWhiteSpace(commitSha) && IsHexString(commitSha, 40))
            {
                return commitSha;
            }

            string? repositoryCommit = TryGetAssemblyMetadataValue(metadata, "RepositoryCommit");
            if (!string.IsNullOrWhiteSpace(repositoryCommit) && IsHexString(repositoryCommit, 40))
            {
                return repositoryCommit;
            }

            string? gitCommitId = TryGetAssemblyMetadataValue(metadata, "GitCommitId");
            if (!string.IsNullOrWhiteSpace(gitCommitId) && IsHexString(gitCommitId, 40))
            {
                return gitCommitId;
            }

            if (string.Equals(informationalVersion, UnknownValue, StringComparison.Ordinal))
            {
                return UnknownValue;
            }

            // Compatibility fallback only: parse informational version metadata when commit
            // attributes are not available on the assembly.
            string[] segments = informationalVersion.Split('+', 2, StringSplitOptions.None);
            if (segments.Length < 2)
            {
                return UnknownValue;
            }

            string[] metadataTokens = segments[1].Split(MetadataTokenSeparators, StringSplitOptions.RemoveEmptyEntries);
            for (int index = 0; index < metadataTokens.Length; index++)
            {
                string token = metadataTokens[index];
                if (IsHexString(token, 40))
                {
                    return token;
                }

                if (string.Equals(token, "sha", StringComparison.OrdinalIgnoreCase) && index + 1 < metadataTokens.Length)
                {
                    string candidate = metadataTokens[index + 1];
                    if (IsHexString(candidate, 40))
                    {
                        return candidate;
                    }
                }

                if (token.StartsWith("sha", StringComparison.OrdinalIgnoreCase))
                {
                    string suffix = token[3..];
                    if (IsHexString(suffix, 40))
                    {
                        return suffix;
                    }
                }
            }

            return UnknownValue;
        }

        /// <summary>
        /// Resolves the dirty repository state, preferring explicit metadata.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <c>RepositoryDirty</c> (or equivalent metadata keys) is the authoritative signal and should be
        /// stamped by the build pipeline.
        /// </para>
        /// <para>
        /// Informational version parsing is a compatibility fallback and only recognizes explicit
        /// metadata tokens such as <c>+...dirty</c> or <c>+...dirty=true</c>.
        /// </para>
        /// </remarks>
        /// <param name="metadata">The assembly metadata map.</param>
        /// <param name="informationalVersion">The resolved informational version.</param>
        /// <returns>
        /// <see langword="true"/> when the build is marked dirty, <see langword="false"/> when explicitly clean,
        /// and <see langword="null"/> when dirty state information is unavailable.
        /// </returns>
        internal static bool? ResolveDirtyState(IReadOnlyDictionary<string, string> metadata, string informationalVersion)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(informationalVersion);

            string? dirtyValue = TryGetAssemblyMetadataValue(metadata, "RepositoryDirty")
                ?? TryGetAssemblyMetadataValue(metadata, "GitDirty")
                ?? TryGetAssemblyMetadataValue(metadata, "IsDirty");

            return TryParseBoolean(dirtyValue, out bool parsedDirty)
                ? parsedDirty
                : string.Equals(informationalVersion, UnknownValue, StringComparison.Ordinal)
                ? null
                : TryParseDirtyFromInformationalVersion(informationalVersion, out bool parsedInformationalDirty)
                    ? parsedInformationalDirty
                    : null;
        }

        /// <summary>
        /// Attempts to parse an explicit dirty token from informational version metadata.
        /// </summary>
        /// <param name="informationalVersion">The informational version value.</param>
        /// <param name="isDirty">The parsed dirty state when successful.</param>
        /// <returns><see langword="true"/> when an explicit dirty token is recognized.</returns>
        private static bool TryParseDirtyFromInformationalVersion(string informationalVersion, out bool isDirty)
        {
            string[] segments = informationalVersion.Split('+', 2, StringSplitOptions.None);
            if (segments.Length < 2)
            {
                isDirty = false;
                return false;
            }

            string[] metadataTokens = segments[1].Split(MetadataTokenSeparators, StringSplitOptions.RemoveEmptyEntries);
            foreach (string token in metadataTokens)
            {
                if (string.Equals(token, "dirty", StringComparison.OrdinalIgnoreCase))
                {
                    isDirty = true;
                    return true;
                }

                if (string.Equals(token, "clean", StringComparison.OrdinalIgnoreCase))
                {
                    isDirty = false;
                    return true;
                }

                if (token.StartsWith("dirty=", StringComparison.OrdinalIgnoreCase))
                {
                    string value = token["dirty=".Length..];
                    if (TryParseBoolean(value, out bool parsedBoolean))
                    {
                        isDirty = parsedBoolean;
                        return true;
                    }
                }
            }

            isDirty = false;
            return false;
        }

        /// <summary>
        /// Resolves the build timestamp from assembly metadata.
        /// </summary>
        /// <remarks>
        /// Build timestamp metadata is treated as trusted pipeline input and returned as-is when present.
        /// </remarks>
        /// <param name="metadata">The assembly metadata map.</param>
        /// <returns>The build timestamp value or <c>unknown</c>.</returns>
        private static string ResolveBuildTimestamp(IReadOnlyDictionary<string, string> metadata)
        {
            string? buildTimestamp = TryGetAssemblyMetadataValue(metadata, "BuildTimestamp")
                ?? TryGetAssemblyMetadataValue(metadata, "BuildDateUtc")
                ?? TryGetAssemblyMetadataValue(metadata, "BuildDate");

            return string.IsNullOrWhiteSpace(buildTimestamp) ? UnknownValue : buildTimestamp;
        }

        /// <summary>
        /// Resolves the build configuration from assembly metadata.
        /// </summary>
        /// <param name="assembly">The assembly to inspect.</param>
        /// <returns>The build configuration name or <c>unknown</c>.</returns>
        private static string ResolveBuildConfiguration(Assembly assembly)
        {
            AssemblyConfigurationAttribute? attribute = assembly.GetCustomAttribute<AssemblyConfigurationAttribute>();
            string? configuration = attribute?.Configuration;

            return string.IsNullOrWhiteSpace(configuration) ? UnknownValue : configuration;
        }

        /// <summary>
        /// Resolves the target framework moniker from assembly attributes.
        /// </summary>
        /// <param name="assembly">The assembly to inspect.</param>
        /// <returns>The target framework moniker.</returns>
        private static string ResolveTargetFramework(Assembly assembly)
        {
            TargetFrameworkAttribute? targetFrameworkAttribute = assembly.GetCustomAttribute<TargetFrameworkAttribute>();
            string? frameworkName = targetFrameworkAttribute?.FrameworkName;

            if (string.IsNullOrWhiteSpace(frameworkName))
            {
                return UnknownValue;
            }

            const string Prefix = ".NETCoreApp,Version=v";
            if (frameworkName.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
            {
                string version = frameworkName[Prefix.Length..];
                return $"net{version}";
            }

            return frameworkName;
        }

        /// <summary>
        /// Builds an assembly metadata map keyed by metadata key.
        /// </summary>
        /// <param name="assembly">The assembly to inspect.</param>
        /// <returns>A case-insensitive metadata map.</returns>
        private static Dictionary<string, string> GetAssemblyMetadataMap(Assembly assembly)
        {
            ArgumentNullException.ThrowIfNull(assembly);

            Dictionary<string, string> metadata = new(StringComparer.OrdinalIgnoreCase);
            AssemblyMetadataAttribute[] metadataAttributes = [.. assembly.GetCustomAttributes<AssemblyMetadataAttribute>()];
            foreach (AssemblyMetadataAttribute metadataAttribute in metadataAttributes)
            {
                if (string.IsNullOrWhiteSpace(metadataAttribute.Key))
                {
                    continue;
                }

                metadata[metadataAttribute.Key] = metadataAttribute.Value ?? string.Empty;
            }

            return metadata;
        }

        /// <summary>
        /// Attempts to read an assembly metadata value by key.
        /// </summary>
        /// <param name="metadata">The cached assembly metadata map.</param>
        /// <param name="key">The metadata key to locate.</param>
        /// <returns>The metadata value if found; otherwise <see langword="null"/>.</returns>
        private static string? TryGetAssemblyMetadataValue(IReadOnlyDictionary<string, string> metadata, string key)
        {
            ArgumentNullException.ThrowIfNull(metadata);

            return string.IsNullOrWhiteSpace(key)
                ? throw new ArgumentException("Metadata key must be non-empty.", nameof(key))
                : !metadata.TryGetValue(key, out string? value) || string.IsNullOrWhiteSpace(value)
                ? null
                : value;
        }

        /// <summary>
        /// Parses a Boolean value from metadata text.
        /// </summary>
        /// <param name="value">The raw metadata value.</param>
        /// <param name="result">The parsed Boolean result.</param>
        /// <returns><see langword="true"/> when parsing succeeded; otherwise <see langword="false"/>.</returns>
        private static bool TryParseBoolean(string? value, out bool result)
        {
            if (bool.TryParse(value, out bool parsedBoolean))
            {
                result = parsedBoolean;
                return true;
            }

            if (string.Equals(value, "1", StringComparison.Ordinal))
            {
                result = true;
                return true;
            }

            if (string.Equals(value, "0", StringComparison.Ordinal))
            {
                result = false;
                return true;
            }

            result = false;
            return false;
        }

        /// <summary>
        /// Determines whether a token is a hexadecimal string with the expected length.
        /// </summary>
        /// <param name="token">The candidate token.</param>
        /// <param name="length">The required string length.</param>
        /// <returns><see langword="true"/> when the token is hex with the requested length; otherwise <see langword="false"/>.</returns>
        private static bool IsHexString(string token, int length)
        {
            if (token.Length != length)
            {
                return false;
            }

            foreach (char character in token)
            {
                if (character is not ((>= '0' and <= '9')
                    or (>= 'a' and <= 'f')
                    or (>= 'A' and <= 'F')))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Gets a formatted version string suitable for startup diagnostics.
        /// </summary>
        /// <returns>A multi-line diagnostic version summary.</returns>
        public string GetVersionString()
        {
            string compactCommit = GetCompactCommit();
            string dirtyText = IsDirty?.ToString() ?? UnknownValue;
            string assemblyVersionLine = string.Empty;
            string fileVersionLine = string.Empty;

            if (!string.Equals(AssemblyVersion, UnknownValue, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(AssemblyVersion, Version, StringComparison.Ordinal))
            {
                assemblyVersionLine = $"Assembly:      {AssemblyVersion}\n";
            }

            if (!string.Equals(FileVersion, UnknownValue, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(FileVersion, Version, StringComparison.Ordinal)
                && !string.Equals(FileVersion, AssemblyVersion, StringComparison.Ordinal))
            {
                fileVersionLine = $"File:          {FileVersion}\n";
            }

            return $"{Service}\n" +
                   $"Version:       {Version}\n" +
                   assemblyVersionLine +
                   fileVersionLine +
                   $"Info Version:  {InformationalVersion}\n" +
                   $"Commit:        {Commit}\n" +
                   $"Commit Short:  {compactCommit}\n" +
                   $"Dirty:         {dirtyText}\n" +
                   $"Build:         {BuildTimestamp}\n" +
                   $"Started:       {StartedAt:O}\n" +
                   $"Configuration: {BuildConfiguration}\n" +
                   $"Target:        {TargetFramework}\n" +
                   $"Runtime:       {Runtime.Framework}\n" +
                   $"OS:            {Runtime.OS}\n" +
                   $"OS Arch:       {Runtime.OSArchitecture}\n" +
                   $"Process Arch:  {Runtime.ProcessArchitecture}";
        }

        /// <summary>
        /// Gets a compact one-line version identifier.
        /// </summary>
        /// <remarks>
        /// Format: <c>v1.4.2+8e17a2f9</c>.
        /// </remarks>
        /// <returns>The compact version value.</returns>
        public string GetCompactVersion()
        {
            return $"v{Version}+{GetCompactCommit()}";
        }

        /// <summary>
        /// Gets a compact commit identifier for display scenarios.
        /// </summary>
        /// <returns>The short commit token or <c>unknown</c>.</returns>
        public string GetCompactCommit()
        {
            string commit = Commit;
            return string.IsNullOrWhiteSpace(commit) || string.Equals(commit, UnknownValue, StringComparison.OrdinalIgnoreCase)
                ? UnknownValue
                : commit.Length <= 8 ? commit : commit[..8];
        }

        /// <summary>
        /// Returns a compact string representation suitable for logs.
        /// </summary>
        /// <returns>A compact version string.</returns>
        public override string ToString()
        {
            return GetCompactVersion();
        }
    }

    /// <summary>
    /// Runtime metadata captured at process startup.
    /// </summary>
    public sealed class RuntimeDetails
    {
        /// <summary>
        /// Gets the .NET runtime description.
        /// </summary>
        public string Framework { get; init; } = "unknown";

        /// <summary>
        /// Gets the operating system description.
        /// </summary>
        public string OS { get; init; } = "unknown";

        /// <summary>
        /// Gets the operating system architecture.
        /// </summary>
        public string OSArchitecture { get; init; } = "unknown";

        /// <summary>
        /// Gets the current process architecture.
        /// </summary>
        public string ProcessArchitecture { get; init; } = "unknown";

        /// <summary>
        /// Captures runtime metadata for the current process.
        /// </summary>
        /// <returns>A populated <see cref="RuntimeDetails"/> instance.</returns>
        public static RuntimeDetails CreateCurrent()
        {
            return new()
            {
                Framework = RuntimeInformation.FrameworkDescription,
                OS = RuntimeInformation.OSDescription,
                OSArchitecture = RuntimeInformation.OSArchitecture.ToString(),
                ProcessArchitecture = RuntimeInformation.ProcessArchitecture.ToString(),
            };
        }
    }
}
