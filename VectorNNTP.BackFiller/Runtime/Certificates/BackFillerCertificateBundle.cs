// <copyright file="BackFillerCertificateBundle.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Represents a loaded listener certificate bundle and its source file.

using System.Security.Cryptography.X509Certificates;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Represents the loaded BackFiller listener certificate bundle and its source on disk.
    /// </summary>
    /// <remarks>
    /// The bundle owns both the active leaf certificate (including private key material) and all persisted intermediate
    /// certificates required for chain validation and TLS certificate-context publication.
    /// </remarks>
    internal sealed class BackFillerCertificateBundle : IDisposable
    {
        /// <summary>
        /// Initializes a certificate bundle with no persisted intermediates.
        /// </summary>
        /// <param name="certificate">Loaded listener leaf certificate with private key material.</param>
        /// <param name="sourcePath">PFX path from which the certificate bundle was loaded.</param>
        /// <param name="loadedAtUtc">UTC timestamp captured after the certificate bundle was loaded into memory.</param>
        public BackFillerCertificateBundle(
            X509Certificate2 certificate,
            string sourcePath,
            DateTimeOffset loadedAtUtc)
            : this(certificate, [], sourcePath, loadedAtUtc)
        {
        }

        /// <summary>
        /// Initializes a certificate bundle with explicit persisted intermediates.
        /// </summary>
        /// <param name="certificate">Loaded listener leaf certificate with private key material.</param>
        /// <param name="intermediateCertificates">Persisted intermediates required for chain evaluation and TLS context publication.</param>
        /// <param name="sourcePath">PFX path from which the certificate bundle was loaded.</param>
        /// <param name="loadedAtUtc">UTC timestamp captured after the certificate bundle was loaded into memory.</param>
        public BackFillerCertificateBundle(
            X509Certificate2 certificate,
            IReadOnlyList<X509Certificate2> intermediateCertificates,
            string sourcePath,
            DateTimeOffset loadedAtUtc)
        {
            ArgumentNullException.ThrowIfNull(certificate);
            ArgumentNullException.ThrowIfNull(intermediateCertificates);
            ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);

            Certificate = certificate;
            IntermediateCertificates = intermediateCertificates;
            SourcePath = sourcePath;
            LoadedAtUtc = loadedAtUtc;
        }

        /// <summary>
        /// Gets the loaded listener leaf certificate that includes private key material.
        /// </summary>
        public X509Certificate2 Certificate { get; }

        /// <summary>
        /// Gets the persisted intermediate certificates that complete the listener chain.
        /// </summary>
        public IReadOnlyList<X509Certificate2> IntermediateCertificates { get; }

        /// <summary>
        /// Gets the PFX path from which this bundle was loaded.
        /// </summary>
        public string SourcePath { get; }

        /// <summary>
        /// Gets the UTC timestamp captured after loading.
        /// </summary>
        public DateTimeOffset LoadedAtUtc { get; }

        /// <summary>
        /// Creates an independent owned clone of the bundle, including leaf private key and intermediate certificates.
        /// </summary>
        /// <returns>A cloned certificate bundle owned by the caller.</returns>
        public BackFillerCertificateBundle CloneOwned()
        {
            const string ClonePassword = "BackFiller-CertificateBundle-Clone";
            byte[] pfx = Certificate.Export(X509ContentType.Pkcs12, ClonePassword);
            X509Certificate2? clonedLeaf = null;
            List<X509Certificate2>? clonedIntermediates = null;

            try
            {
                clonedLeaf = new X509Certificate2(
                    pfx,
                    ClonePassword,
                    X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);

                clonedIntermediates = new List<X509Certificate2>(IntermediateCertificates.Count);
                for (int index = 0; index < IntermediateCertificates.Count; index++)
                {
                    clonedIntermediates.Add(new X509Certificate2(IntermediateCertificates[index].RawData));
                }

                BackFillerCertificateBundle clone = new(clonedLeaf, clonedIntermediates, SourcePath, LoadedAtUtc);
                clonedLeaf = null;
                clonedIntermediates = null;
                return clone;
            }
            finally
            {
                clonedLeaf?.Dispose();
                if (clonedIntermediates is not null)
                {
                    for (int index = 0; index < clonedIntermediates.Count; index++)
                    {
                        clonedIntermediates[index].Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Disposes owned leaf and intermediate certificates.
        /// </summary>
        public void Dispose()
        {
            Certificate.Dispose();
            for (int index = 0; index < IntermediateCertificates.Count; index++)
            {
                IntermediateCertificates[index].Dispose();
            }
        }
    }
}
