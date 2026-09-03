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
    /// Owns the currently active listener certificate bundle published to runtime consumers.
    /// </summary>
    /// <remarks>
    /// Replacing the active bundle disposes the previously published certificate. Callers that need a reusable copy
    /// must request a clone rather than holding on to the stored instance directly.
    /// </remarks>
    internal sealed class BackFillerCertificateState : IDisposable
    {
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
        /// Publishes a new active certificate bundle and disposes the certificate from any previously active bundle.
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

            previous?.Certificate.Dispose();
        }

        /// <summary>
        /// Creates an independent clone of the currently active listener certificate.
        /// </summary>
        /// <returns>
        /// A new <see cref="X509Certificate2"/> instance that the caller owns and must dispose, or
        /// <see langword="null"/> when no active certificate is available.
        /// </returns>
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

        /// <summary>
        /// Clears the active bundle and disposes its certificate.
        /// </summary>
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
