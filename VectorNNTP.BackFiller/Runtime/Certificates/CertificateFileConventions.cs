// <copyright file="CertificateFileConventions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Defines the persisted BackFiller listener certificate file names and temp-path pattern.

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Defines the file names used for the persisted BackFiller listener certificate bundle.
    /// </summary>
    internal static class CertificateFileConventions
    {
        internal const string ListenerPfxFileName = "backfiller-listener.pfx";
        internal const string CertificatePrivateKeyPemFileName = "backfiller-listener.key";

        /// <summary>
        /// Builds a deterministic temporary path used for atomic file replacement.
        /// </summary>
        /// <param name="targetPath">Final artifact path.</param>
        /// <returns>Temporary file path in the same directory.</returns>
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
