// <copyright file="BackFillerCertificateState.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Owns the currently active listener certificate reference published to runtime consumers.

using System.Net.Security;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Owns the currently active listener certificate bundle published to runtime consumers.
    /// </summary>
    /// <remarks>
    /// Replacing the active bundle disposes the previously published leaf and intermediate certificates. Callers that need
    /// reusable TLS material must request an owned runtime clone and dispose that clone after use.
    /// </remarks>
    internal sealed class BackFillerCertificateState : IDisposable
    {
        /// <summary>
        /// Owned runtime clone of the active certificate bundle with prebuilt TLS certificate context.
        /// </summary>
        internal sealed class RuntimeCertificateMaterial : IDisposable
        {
            /// <summary>
            /// Initializes owned runtime certificate material from a cloned bundle.
            /// </summary>
            /// <param name="bundle">Owned bundle clone.</param>
            public RuntimeCertificateMaterial(BackFillerCertificateBundle bundle)
            {
                ArgumentNullException.ThrowIfNull(bundle);
                Bundle = bundle;

                try
                {
                    if (bundle.IntermediateCertificates.Count > 0)
                    {
                        System.Security.Cryptography.X509Certificates.X509Certificate2Collection intermediateCollection = [];
                        for (int index = 0; index < bundle.IntermediateCertificates.Count; index++)
                        {
                            _ = intermediateCollection.Add(bundle.IntermediateCertificates[index]);
                        }

                        CertificateContext = SslStreamCertificateContext.Create(bundle.Certificate, intermediateCollection, offline: true);
                    }
                }
                catch
                {
                    Bundle.Dispose();
                    throw;
                }
            }

            /// <summary>
            /// Gets the owned certificate bundle clone.
            /// </summary>
            public BackFillerCertificateBundle Bundle { get; }

            /// <summary>
            /// Gets TLS certificate context carrying leaf and intermediates for server authentication.
            /// </summary>
            public SslStreamCertificateContext? CertificateContext { get; }

            /// <summary>
            /// Disposes the owned certificate bundle clone.
            /// </summary>
            public void Dispose()
            {
                Bundle.Dispose();
            }
        }

        /// <summary>
        /// Synchronizes publication, cloning, and disposal of the active certificate bundle.
        /// </summary>
        private readonly object _gate = new();

        /// <summary>
        /// Currently published listener certificate bundle, or <see langword="null"/> when no certificate is active.
        /// </summary>
        private BackFillerCertificateBundle? _current;

        /// <summary>
        /// Gets a value indicating whether a listener certificate is currently published.
        /// </summary>
        /// <value><see langword="true"/> when <see cref="Publish"/> has installed a bundle that has not yet been cleared.</value>
        internal bool HasCertificate
        {
            get
            {
                lock (_gate)
                {
                    return _current is not null;
                }
            }
        }

        /// <summary>
        /// Publishes a new active certificate bundle and disposes any previously active bundle.
        /// </summary>
        /// <param name="bundle">New listener certificate bundle whose ownership transfers into this state container.</param>
        public void Publish(BackFillerCertificateBundle bundle)
        {
            ArgumentNullException.ThrowIfNull(bundle);

            BackFillerCertificateBundle? previous;
            lock (_gate)
            {
                previous = _current;
                _current = bundle;
            }

            previous?.Dispose();
        }

        /// <summary>
        /// Creates an owned runtime clone containing leaf, intermediates, and TLS certificate context.
        /// </summary>
        /// <returns>Owned runtime certificate material, or <see langword="null"/> when no active certificate exists.</returns>
        internal RuntimeCertificateMaterial? GetCurrentRuntimeCertificateMaterialClone()
        {
            BackFillerCertificateBundle? clone;
            lock (_gate)
            {
                clone = _current?.CloneOwned();
            }

            return clone is null ? null : new RuntimeCertificateMaterial(clone);
        }

        /// <summary>
        /// Clears the active bundle and disposes it.
        /// </summary>
        public void Dispose()
        {
            BackFillerCertificateBundle? previous;
            lock (_gate)
            {
                previous = _current;
                _current = null;
            }

            previous?.Dispose();
        }
    }
}
