// <copyright file="BackFillerCertificateState.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Owns the currently active listener certificate reference published to runtime consumers.

using System.Security.Cryptography.X509Certificates;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Stores the currently active listener certificate bundle used by runtime consumers.
    /// </summary>
    /// <remarks>
    /// Replacing the active bundle disposes the previously published certificate. Callers that need a reusable copy
    /// must request a clone rather than holding on to the stored instance directly.
    /// </remarks>
    internal sealed class BackFillerCertificateState : IDisposable
    {
        /// <summary>
        /// Stores gate used by back filler certificate state.
        /// </summary>
        private readonly object _gate = new();
        /// <summary>
        /// Stores current used by back filler certificate state.
        /// </summary>
        private BackFillerCertificateBundle? _current;

        /// <summary>
        /// Gets a value indicating whether a certificate is currently available.
        /// </summary>
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
        /// <param name="bundle">New certificate bundle.</param>
        public void Publish(BackFillerCertificateBundle bundle)
        {
            ArgumentNullException.ThrowIfNull(bundle);

            BackFillerCertificateBundle? previous;
            lock (_gate)
            {
                previous = _current;
                _current = bundle;
            }

            previous?.Certificate.Dispose();
        }

        /// <summary>
        /// Gets a cloned copy of the currently active certificate, or <see langword="null"/> if unavailable.
        /// </summary>
        /// <returns>Cloned active certificate.</returns>
        internal X509Certificate2? GetCurrentCertificateClone()
        {
            lock (_gate)
            {
                if (_current is null)
                {
                    return null;
                }

                const string ClonePassword = "BackFiller-CertificateState-Clone";
                byte[] pfx = _current.Certificate.Export(X509ContentType.Pkcs12, ClonePassword);
                return new X509Certificate2(
                    pfx,
                    ClonePassword,
                    X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            BackFillerCertificateBundle? previous;
            lock (_gate)
            {
                previous = _current;
                _current = null;
            }

            previous?.Certificate.Dispose();
        }
    }
}
