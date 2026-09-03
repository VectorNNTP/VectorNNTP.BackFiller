// <copyright file="CertificateFileConventions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Defines the persisted BackFiller listener certificate file names and temp-path pattern.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Centralizes file-name and temporary-path conventions for persisted listener certificate artifacts.
    /// </summary>
    internal static class CertificateFileConventions
    {
        /// <summary>
        /// Canonical PFX file name used for the persisted listener certificate bundle.
        /// </summary>
        internal const string ListenerPfxFileName = "backfiller-listener.pfx";

        /// <summary>
        /// Canonical PEM file name used for the persisted listener certificate private key.
        /// </summary>
        internal const string CertificatePrivateKeyPemFileName = "backfiller-listener.key";

        /// <summary>
        /// Builds a same-directory temporary path for atomic replacement of a certificate artifact.
        /// </summary>
        /// <param name="targetPath">Final artifact path that will be replaced.</param>
        /// <returns>A unique temporary path in the same directory as <paramref name="targetPath"/>.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="targetPath"/> is blank.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="targetPath"/> has no directory component.</exception>
        internal static string BuildAtomicTempPath(string targetPath)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);

            string directory = Path.GetDirectoryName(targetPath)
                ?? throw new InvalidOperationException("Target path must include a directory.");

            string fileName = Path.GetFileName(targetPath);
            string tempToken = Guid.NewGuid().ToString("N");
            return Path.Combine(directory, $"{fileName}.{tempToken}.tmp");
        }
    }
}
