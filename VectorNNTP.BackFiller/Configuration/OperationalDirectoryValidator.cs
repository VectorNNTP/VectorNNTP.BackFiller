// <copyright file="OperationalDirectoryValidator.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: operational directory validator in the vector nntp.back filler configuration subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// OperationalDirectoryValidator.cs -- Startup filesystem validation for operational directories.
//
// Validates BackFiller:DirLogs and BackFiller:DirCerts before host startup continues.
// Validation includes:
//   1. Configuration presence and whitespace normalization
//   2. Cross-platform absolute path resolution (absolute or AppContext.BaseDirectory-relative)
//   3. Directory creation (including parent hierarchy)
//   4. Directory type verification
//   5. Required file capability probe (create, write, read, replace, delete)
//
// The validator is fail-fast by design: any validation failure throws InvalidOperationException
// with the responsible setting name for clear startup diagnostics.

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Resolves and validates startup-critical operational directories before runtime snapshot composition proceeds.
    /// </summary>
    /// <remarks>
    /// <para>This validator is invoked during startup configuration projection to hard-fail when log or certificate
    /// directory requirements are not satisfiable for the current process identity.</para>
    /// <para>Validation uses concrete filesystem operations (create/read/write/replace/delete) so permission and
    /// platform-behavior failures are detected deterministically before host startup continues.</para>
    /// </remarks>
    internal static class OperationalDirectoryValidator
    {
        /// <summary>
        /// Resolves and validates the configured logging directory path.
        /// </summary>
        /// <param name="configuration">Application configuration source.</param>
        /// <returns>The canonical absolute path for the validated logging directory.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the configured logging directory is missing or fails validation.</exception>
        internal static string ResolveAndValidateLogDirectory(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);

            return ResolveAndValidateOperationalDirectory(
                configuration,
                primarySetting: "BackFiller:DirLogs",
                logicalName: "logging",
                probePrefix: ".log-dir-validation");
        }

        /// <summary>
        /// Resolves and validates the configured certificate directory path.
        /// </summary>
        /// <param name="configuration">Application configuration source.</param>
        /// <returns>The canonical absolute path for the validated certificate directory.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the configured certificate directory is missing or fails validation.</exception>
        internal static string ResolveAndValidateCertificateDirectory(IConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);

            return ResolveAndValidateOperationalDirectory(
                configuration,
                primarySetting: "BackFiller:DirCerts",
                logicalName: "certificate",
                probePrefix: ".cert-dir-validation");
        }

        /// <summary>
        /// Resolves one configured operational directory to an absolute path and enforces startup capability checks.
        /// </summary>
        /// <param name="configuration">Application configuration source.</param>
        /// <param name="primarySetting">Configuration key containing the directory path.</param>
        /// <param name="logicalName">Logical directory name used in diagnostic messages.</param>
        /// <param name="probePrefix">Prefix for temporary validation probe files.</param>
        /// <returns>The canonical absolute path for the validated directory.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Thrown when any string parameter is null, empty, or whitespace.</exception>
        /// <exception cref="InvalidOperationException">Thrown when path resolution, directory creation, or capability validation fails.</exception>
        private static string ResolveAndValidateOperationalDirectory(
            IConfiguration configuration,
            string primarySetting,
            string logicalName,
            string probePrefix)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            ArgumentException.ThrowIfNullOrWhiteSpace(primarySetting);
            ArgumentException.ThrowIfNullOrWhiteSpace(logicalName);
            ArgumentException.ThrowIfNullOrWhiteSpace(probePrefix);

            string? configuredPath = configuration[primarySetting];

            if (string.IsNullOrWhiteSpace(configuredPath))
            {
                throw new InvalidOperationException(
                    $"{logicalName} directory configuration is missing. Set '{primarySetting}'.");
            }

            configuredPath = configuredPath.Trim();

            string resolvedPath;
            try
            {
                resolvedPath = Path.IsPathRooted(configuredPath)
                    ? Path.GetFullPath(configuredPath)
                    : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, configuredPath));
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Configured {logicalName} directory '{primarySetting}' value '{configuredPath}' cannot be resolved to an absolute path.", ex);
            }

            try
            {
                _ = Directory.CreateDirectory(resolvedPath);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Configured {logicalName} directory '{primarySetting}' value '{configuredPath}' could not be created at '{resolvedPath}'.", ex);
            }

            if (!Directory.Exists(resolvedPath))
            {
                throw new InvalidOperationException(
                    $"Configured {logicalName} directory '{primarySetting}' resolved to '{resolvedPath}', but the directory does not exist after creation attempt.");
            }

            FileAttributes attributes;
            try
            {
                attributes = File.GetAttributes(resolvedPath);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Configured {logicalName} directory '{primarySetting}' resolved to '{resolvedPath}', but attributes could not be read.", ex);
            }

            if ((attributes & FileAttributes.Directory) == 0)
            {
                throw new InvalidOperationException(
                    $"Configured {logicalName} directory '{primarySetting}' resolved to '{resolvedPath}', but this path is not a directory.");
            }

            ValidateCreateReadWriteReplaceDelete(resolvedPath, primarySetting, logicalName, probePrefix);
            return resolvedPath;
        }

        /// <summary>
        /// Verifies that a directory supports the file operations required by runtime ownership semantics.
        /// </summary>
        /// <param name="directoryPath">Absolute directory path under validation.</param>
        /// <param name="configuredProperty">Configuration key used for diagnostics.</param>
        /// <param name="logicalName">Logical directory name used in diagnostic messages.</param>
        /// <param name="probePrefix">Prefix for temporary validation probe files.</param>
        /// <exception cref="InvalidOperationException">Thrown when create/read/write/replace/delete probing fails for the target directory.</exception>
        private static void ValidateCreateReadWriteReplaceDelete(
            string directoryPath,
            string configuredProperty,
            string logicalName,
            string probePrefix)
        {
            string probeA = Path.Combine(directoryPath, $"{probePrefix}-{Guid.NewGuid():N}-a.tmp");
            string probeB = Path.Combine(directoryPath, $"{probePrefix}-{Guid.NewGuid():N}-b.tmp");

            try
            {
                using (FileStream create = new(
                    probeA,
                    FileMode.CreateNew,
                    FileAccess.ReadWrite,
                    FileShare.None))
                {
                    ReadOnlySpan<byte> payload = "directory-validation"u8;
                    create.Write(payload);
                    create.Flush();
                }

                _ = File.ReadAllText(probeA);
                File.WriteAllText(probeB, "replacement-content");

                File.Replace(probeB, probeA, destinationBackupFileName: null, ignoreMetadataErrors: true);

                _ = File.ReadAllText(probeA);
                File.Delete(probeA);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Configured {logicalName} directory '{configuredProperty}' at '{directoryPath}' failed create/read/write/replace/delete validation.", ex);
            }
            finally
            {
                TryDelete(probeA);
                TryDelete(probeB);
            }
        }

        /// <summary>
        /// Performs best-effort probe-file cleanup without surfacing cleanup failures.
        /// </summary>
        /// <param name="path">File path to delete when present.</param>
        /// <remarks>
        /// Cleanup exceptions are intentionally suppressed because probe failures are already reported through the
        /// primary validation exception path.
        /// </remarks>
        private static void TryDelete(string path)
        {
            try
            {
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
            catch
            {
                // Best-effort cleanup only.
            }
        }
    }
}
