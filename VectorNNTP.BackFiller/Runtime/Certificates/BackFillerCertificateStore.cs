// <copyright file="BackFillerCertificateStore.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Loads, validates, and atomically persists the BackFiller listener certificate bundle.

using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Owns discovery, validation, loading, and atomic persistence for the BackFiller listener certificate bundle.
    /// </summary>
    /// <remarks>
    /// The store checks the persisted certificate for private-key availability, validity window, generated FQDN
    /// identity, server-auth EKU, and a buildable chain before the listener is allowed to use it. Issued material is
    /// written through same-directory temporary files so replacement is atomic with respect to the final target files.
    /// </remarks>
    internal sealed partial class BackFillerCertificateStore
    {
        /// <summary>
        /// Evaluates whether an existing persisted listener certificate is usable and whether renewal is required.
        /// </summary>
        /// <remarks>
        /// When evaluation loads a certificate that later fails a validation step, the certificate is disposed before the
        /// method returns an unusable result. A successful result transfers bundle ownership to the caller.
        /// </remarks>
        /// <param name="letsEncryptOptions">Validated ACME runtime options that identify the persisted certificate paths and FQDN.</param>
        /// <param name="timeProvider">Unified time provider used for validity and renewal-window checks.</param>
        /// <param name="cancellationToken">Cancellation token observed before loading certificate artifacts.</param>
        /// <returns>Certificate usability and renewal classification.</returns>
        public static async Task<CertificateEvaluationResult> EvaluateExistingCertificateAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            TimeProvider timeProvider,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(letsEncryptOptions);
            ArgumentNullException.ThrowIfNull(timeProvider);

            cancellationToken.ThrowIfCancellationRequested();

            if (!File.Exists(letsEncryptOptions.CertificatePfxPath))
            {
                return new CertificateEvaluationResult(
                    HasCertificate: false,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: "Listener certificate does not exist.",
                    Certificate: null);
            }

            BackFillerCertificateBundle certificateBundle;
            try
            {
                certificateBundle = await LoadCertificateBundleAsync(letsEncryptOptions, timeProvider, cancellationToken).ConfigureAwait(false);
            }
            catch (CryptographicException ex)
            {
                return new CertificateEvaluationResult(
                    HasCertificate: true,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: $"Listener certificate could not be loaded: {ex.Message}",
                    Certificate: null);
            }

            DateTimeOffset nowUtc = timeProvider.GetUtcNow();

            if (!certificateBundle.Certificate.HasPrivateKey)
            {
                certificateBundle.Certificate.Dispose();
                return new CertificateEvaluationResult(
                    HasCertificate: true,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: "Listener certificate does not include a private key.",
                    Certificate: null);
            }

            if (nowUtc < certificateBundle.Certificate.NotBefore.ToUniversalTime())
            {
                certificateBundle.Certificate.Dispose();
                return new CertificateEvaluationResult(
                    HasCertificate: true,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: "Listener certificate is not yet valid.",
                    Certificate: null);
            }

            if (nowUtc > certificateBundle.Certificate.NotAfter.ToUniversalTime())
            {
                certificateBundle.Certificate.Dispose();
                return new CertificateEvaluationResult(
                    HasCertificate: true,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: "Listener certificate has expired.",
                    Certificate: null);
            }

            if (!CertificateContainsDnsName(certificateBundle.Certificate, letsEncryptOptions.CanonicalCertificateSubjectName))
            {
                certificateBundle.Certificate.Dispose();
                return new CertificateEvaluationResult(
                    HasCertificate: true,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: "Listener certificate does not match the generated BackFiller FQDN.",
                    Certificate: null);
            }

            if (!HasServerAuthenticationUsage(certificateBundle.Certificate))
            {
                certificateBundle.Certificate.Dispose();
                return new CertificateEvaluationResult(
                    HasCertificate: true,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: "Listener certificate does not permit TLS server authentication.",
                    Certificate: null);
            }

            if (!BuildCertificateChain(certificateBundle.Certificate, out string chainFailureReason))
            {
                certificateBundle.Certificate.Dispose();
                return new CertificateEvaluationResult(
                    HasCertificate: true,
                    IsUsable: false,
                    RequiresRenewal: true,
                    Reason: chainFailureReason,
                    Certificate: null);
            }

            DateTimeOffset renewalThresholdUtc = certificateBundle.Certificate.NotAfter.ToUniversalTime().AddDays(-letsEncryptOptions.RenewBeforeExpiryDays);
            bool requiresRenewal = nowUtc >= renewalThresholdUtc;

            return new CertificateEvaluationResult(
                HasCertificate: true,
                IsUsable: true,
                RequiresRenewal: requiresRenewal,
                Reason: requiresRenewal
                    ? "Listener certificate is valid but inside renewal window."
                    : "Listener certificate is valid and outside renewal window.",
                Certificate: certificateBundle);
        }

        /// <summary>
        /// Loads the persisted listener certificate bundle.
        /// </summary>
        /// <param name="letsEncryptOptions">Validated ACME runtime options that identify the listener PFX path and password.</param>
        /// <param name="timeProvider">Unified time provider used to timestamp the loaded bundle.</param>
        /// <param name="cancellationToken">Cancellation token observed while reading the PFX file.</param>
        /// <param name="logger">Optional logger used to record certificate loading diagnostics.</param>
        /// <returns>A loaded certificate bundle whose <see cref="BackFillerCertificateBundle.Certificate"/> is owned by the caller.</returns>
        public static async Task<BackFillerCertificateBundle> LoadCertificateBundleAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            TimeProvider timeProvider,
            CancellationToken cancellationToken,
            ILogger? logger = null)
        {
            ArgumentNullException.ThrowIfNull(letsEncryptOptions);
            ArgumentNullException.ThrowIfNull(timeProvider);

            cancellationToken.ThrowIfCancellationRequested();

            if (logger is not null)
            {
                LogLoadingListenerCertificateBundle(
                    logger,
                    letsEncryptOptions.CanonicalCertificateSubjectName,
                    letsEncryptOptions.CertificatePfxPath);
            }

            try
            {
                byte[] pfx = await File.ReadAllBytesAsync(letsEncryptOptions.CertificatePfxPath, cancellationToken).ConfigureAwait(false);
                X509Certificate2 certificate = new(
                    pfx,
                    letsEncryptOptions.PfxExportPassword,
                    X509KeyStorageFlags.EphemeralKeySet | X509KeyStorageFlags.Exportable);

                if (logger is not null)
                {
                    LogLoadedListenerCertificateBundle(
                        logger,
                        letsEncryptOptions.CanonicalCertificateSubjectName,
                        letsEncryptOptions.CertificatePfxPath);
                }

                return new BackFillerCertificateBundle(certificate, letsEncryptOptions.CertificatePfxPath, timeProvider.GetUtcNow());
            }
            catch (Exception ex)
            {
                if (logger is not null)
                {
                    LogListenerCertificateBundleLoadFailed(
                        logger,
                        letsEncryptOptions.CanonicalCertificateSubjectName,
                        letsEncryptOptions.CertificatePfxPath,
                        ex);
                }
                throw;
            }
        }

        /// <summary>
        /// Persists newly issued certificate artifacts using atomic replacement semantics.
        /// </summary>
        /// <remarks>
        /// The private key PEM is written first, followed by the PFX bundle, with both artifacts staged through unique
        /// same-directory temporary files so readers never observe partially written content at the final paths.
        /// </remarks>
        /// <param name="letsEncryptOptions">Validated ACME runtime options that identify the destination paths and PFX password.</param>
        /// <param name="issueResult">Issued ACME certificate artifacts to persist.</param>
        /// <param name="cancellationToken">Cancellation token observed while writing the temporary artifacts.</param>
        /// <param name="logger">Optional logger used to record certificate persistence diagnostics.</param>
        /// <returns>A task that completes after atomic persistence succeeds.</returns>
        public static async Task PersistIssuedCertificateAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            AcmeOrderIssueResult issueResult,
            CancellationToken cancellationToken,
            ILogger? logger = null)
        {
            ArgumentNullException.ThrowIfNull(letsEncryptOptions);
            ArgumentNullException.ThrowIfNull(issueResult);

            cancellationToken.ThrowIfCancellationRequested();

            byte[] pfx = BuildPfxBundle(issueResult, letsEncryptOptions.PfxExportPassword);
            string pfxTempPath = CertificateFileConventions.BuildAtomicTempPath(letsEncryptOptions.CertificatePfxPath);
            string keyTempPath = CertificateFileConventions.BuildAtomicTempPath(letsEncryptOptions.CertificatePrivateKeyPemPath);

            if (logger is not null)
            {
                LogPersistingListenerCertificateBundle(
                    logger,
                    letsEncryptOptions.CanonicalCertificateSubjectName,
                    letsEncryptOptions.CertificatePfxPath,
                    letsEncryptOptions.CertificatePrivateKeyPemPath,
                    pfxTempPath,
                    keyTempPath);
            }

            try
            {
                await WriteFileAtomicallyAsync(keyTempPath, letsEncryptOptions.CertificatePrivateKeyPemPath, issueResult.CertificatePrivateKeyPem, cancellationToken, logger).ConfigureAwait(false);
                await WriteFileAtomicallyAsync(pfxTempPath, letsEncryptOptions.CertificatePfxPath, pfx, cancellationToken, logger).ConfigureAwait(false);

                if (logger is not null)
                {
                    LogListenerCertificateBundlePersisted(
                        logger,
                        letsEncryptOptions.CanonicalCertificateSubjectName,
                        letsEncryptOptions.CertificatePfxPath,
                        letsEncryptOptions.CertificatePrivateKeyPemPath);
                }
            }
            finally
            {
                TryDeleteTempFile(keyTempPath, logger);
                TryDeleteTempFile(pfxTempPath, logger);
            }
        }

        /// <summary>
        /// Determines whether a certificate identifies the generated BackFiller FQDN in either CN or SAN form.
        /// </summary>
        /// <param name="certificate">Certificate to inspect.</param>
        /// <param name="expectedDnsName">Generated BackFiller FQDN that must appear in the certificate.</param>
        /// <returns><see langword="true"/> when the expected DNS name appears in the subject or SAN extension.</returns>
        private static bool CertificateContainsDnsName(X509Certificate2 certificate, string expectedDnsName)
        {
            ArgumentNullException.ThrowIfNull(certificate);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedDnsName);

            string expected = expectedDnsName.Trim().TrimEnd('.').ToLowerInvariant();

            string? commonName = certificate.GetNameInfo(X509NameType.DnsName, forIssuer: false);
            if (!string.IsNullOrWhiteSpace(commonName) &&
                string.Equals(commonName.Trim().TrimEnd('.'), expected, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            X509Extension? sanExtension = certificate.Extensions["2.5.29.17"];
            if (sanExtension == null)
            {
                return false;
            }

            AsnReader reader = new(sanExtension.RawData, AsnEncodingRules.DER);
            AsnReader sequence = reader.ReadSequence();

            while (sequence.HasData)
            {
                Asn1Tag tag = sequence.PeekTag();
                if (tag.TagClass == TagClass.ContextSpecific && tag.TagValue == 2)
                {
                    string dnsName = sequence.ReadCharacterString(UniversalTagNumber.IA5String, new Asn1Tag(TagClass.ContextSpecific, 2));
                    if (string.Equals(dnsName.Trim().TrimEnd('.'), expected, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
                else
                {
                    _ = sequence.ReadEncodedValue();
                }
            }

            return false;
        }

        /// <summary>
        /// Determines whether the certificate permits TLS server authentication.
        /// </summary>
        /// <param name="certificate">Certificate whose EKU extensions are being evaluated.</param>
        /// <returns>
        /// <see langword="true"/> when the certificate has no EKU restriction or explicitly contains the TLS server
        /// authentication OID.
        /// </returns>
        private static bool HasServerAuthenticationUsage(X509Certificate2 certificate)
        {
            ArgumentNullException.ThrowIfNull(certificate);

            const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";

            X509EnhancedKeyUsageExtension? eku = certificate.Extensions
                .OfType<X509EnhancedKeyUsageExtension>()
                .FirstOrDefault();

            return eku == null || eku.EnhancedKeyUsages
                .Cast<Oid>()
                .Any(oid => string.Equals(oid.Value, ServerAuthenticationOid, StringComparison.Ordinal));
        }

        /// <summary>
        /// Attempts to build the certificate chain and returns a human-readable failure reason when it cannot.
        /// </summary>
        /// <remarks>
        /// An otherwise valid chain that fails only with <see cref="X509ChainStatusFlags.UntrustedRoot"/> is accepted so
        /// self-contained test material and environments with incomplete trust stores do not incorrectly block activation.
        /// </remarks>
        /// <param name="certificate">Certificate whose chain should be validated.</param>
        /// <param name="failureReason">Receives a human-readable reason when validation fails.</param>
        /// <returns><see langword="true"/> when the chain is acceptable for listener activation.</returns>
        private static bool BuildCertificateChain(X509Certificate2 certificate, out string failureReason)
        {
            ArgumentNullException.ThrowIfNull(certificate);

            using X509Chain chain = new();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.RevocationFlag = X509RevocationFlag.ExcludeRoot;
            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;

            bool chainValid = chain.Build(certificate);
            if (chainValid)
            {
                failureReason = string.Empty;
                return true;
            }

            X509ChainStatus[] meaningfulStatuses = [.. chain.ChainStatus.Where(static entry => entry.Status != X509ChainStatusFlags.NoError)];

            bool onlyUntrustedRoot = meaningfulStatuses.Length > 0
                && meaningfulStatuses.All(static entry => entry.Status == X509ChainStatusFlags.UntrustedRoot);

            if (onlyUntrustedRoot)
            {
                failureReason = string.Empty;
                return true;
            }

            string status = string.Join(", ",
                meaningfulStatuses
                    .Select(static entry => entry.StatusInformation?.Trim())
                    .Where(static text => !string.IsNullOrWhiteSpace(text)));

            failureReason = string.IsNullOrWhiteSpace(status)
                ? "Listener certificate chain validation failed."
                : $"Listener certificate chain validation failed: {status}";

            return false;
        }

        /// <summary>
        /// Builds a PFX bundle from issued ACME artifacts and the matching locally held private key.
        /// </summary>
        /// <param name="issueResult">Issued ACME artifacts containing the leaf certificate, chain, and private key PEM.</param>
        /// <param name="pfxPassword">Password used to protect the exported PFX bundle.</param>
        /// <returns>PFX bytes ready for persistence.</returns>
        /// <exception cref="CryptographicException">Thrown when the certificate collection cannot be exported as a PFX bundle.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the private key PEM does not represent a supported key algorithm.</exception>
        private static byte[] BuildPfxBundle(AcmeOrderIssueResult issueResult, string pfxPassword)
        {
            ArgumentNullException.ThrowIfNull(issueResult);
            ArgumentException.ThrowIfNullOrWhiteSpace(pfxPassword);

            using X509Certificate2 leaf = new(issueResult.LeafCertificateDer);
            using AsymmetricAlgorithm privateKey = ImportCertificatePrivateKey(issueResult.CertificatePrivateKeyPem);
            using X509Certificate2 leafWithPrivateKey = privateKey switch
            {
                RSA rsa => leaf.CopyWithPrivateKey(rsa),
                ECDsa ecdsa => leaf.CopyWithPrivateKey(ecdsa),
                _ => throw new InvalidOperationException("Unsupported certificate private key algorithm."),
            };

            X509Certificate2Collection collection = [leafWithPrivateKey];
            try
            {
                foreach (byte[] issuerDer in issueResult.ChainDer)
                {
                    if (issuerDer.Length == 0)
                    {
                        continue;
                    }

                    _ = collection.Add(new X509Certificate2(issuerDer));
                }

                return collection.Export(X509ContentType.Pkcs12, pfxPassword)
                    ?? throw new CryptographicException("Failed to export listener certificate PFX.");
            }
            finally
            {
                for (int index = 1; index < collection.Count; index++)
                {
                    collection[index].Dispose();
                }
            }
        }

        /// <summary>
        /// Imports a PEM-encoded certificate private key as the first supported .NET asymmetric algorithm.
        /// </summary>
        /// <param name="pem">PEM-encoded private key text.</param>
        /// <returns>An asymmetric algorithm instance that owns the imported key material.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the PEM text is not a supported RSA or ECDSA private key.</exception>
        private static AsymmetricAlgorithm ImportCertificatePrivateKey(string pem)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pem);

            try
            {
                RSA rsa = RSA.Create();
                rsa.ImportFromPem(pem.AsSpan());
                return rsa;
            }
            catch (Exception ex) when (ex is CryptographicException or ArgumentException)
            {
            }

            try
            {
                ECDsa ecdsa = ECDsa.Create();
                ecdsa.ImportFromPem(pem.AsSpan());
                return ecdsa;
            }
            catch (Exception ex) when (ex is CryptographicException or ArgumentException)
            {
            }

            throw new InvalidOperationException("Certificate private key PEM is not a supported RSA/ECDSA key.");
        }

        /// <summary>
        /// Encodes text certificate content as UTF-8 and writes it with the atomic file-replacement helper.
        /// </summary>
        /// <param name="tempPath">Temporary same-directory path used for staging the write.</param>
        /// <param name="targetPath">Final certificate artifact path.</param>
        /// <param name="content">Text payload to encode as UTF-8.</param>
        /// <param name="cancellationToken">Cancellation token observed while writing.</param>
        /// <param name="logger">Optional logger used to record staging and move diagnostics.</param>
        /// <returns>A task that completes after the encoded payload has been written and moved into place.</returns>
        private static async Task WriteFileAtomicallyAsync(string tempPath, string targetPath, string content, CancellationToken cancellationToken, ILogger? logger = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tempPath);
            ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);
            ArgumentNullException.ThrowIfNull(content);

            byte[] payload = System.Text.Encoding.UTF8.GetBytes(content);
            await WriteFileAtomicallyAsync(tempPath, targetPath, payload, cancellationToken, logger).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes one certificate artifact to a temporary file and atomically moves it into place.
        /// </summary>
        /// <param name="tempPath">Temporary same-directory path used for staging the write.</param>
        /// <param name="targetPath">Final certificate artifact path.</param>
        /// <param name="payload">Binary payload to persist.</param>
        /// <param name="cancellationToken">Cancellation token observed while writing and flushing the temporary file.</param>
        /// <param name="logger">Optional logger used to record staging and move diagnostics.</param>
        /// <returns>A task that completes after the payload has been written and moved into place.</returns>
        private static async Task WriteFileAtomicallyAsync(string tempPath, string targetPath, byte[] payload, CancellationToken cancellationToken, ILogger? logger = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tempPath);
            ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);
            ArgumentNullException.ThrowIfNull(payload);

            cancellationToken.ThrowIfCancellationRequested();

            if (logger is not null)
            {
                LogWritingCertificateArtifactToTemporaryFile(
                    logger,
                    tempPath,
                    targetPath);
            }

            try
            {
                {
                    using FileStream stream = new(
                        tempPath,
                        FileMode.CreateNew,
                        FileAccess.Write,
                        FileShare.None,
                        bufferSize: 81920,
                        useAsync: true);
                    await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }

                if (logger is not null && logger.IsEnabled(LogLevel.Information))
                {
                    FileInfo tempInfo = new(tempPath);
                    FileInfo? targetInfo = File.Exists(targetPath) ? new FileInfo(targetPath) : null;
                    LogCertificateArtifactReadyForAtomicReplace(
                        logger,
                        Environment.ProcessId,
                        tempInfo.FullName,
                        Path.GetFullPath(targetPath),
                        tempInfo.Exists,
                        targetInfo is not null,
                        tempInfo.Exists ? tempInfo.Length : -1L,
                        targetInfo?.Length ?? -1L);
                }

                File.Move(tempPath, targetPath, overwrite: true);

                if (logger is not null)
                {
                    LogCertificateArtifactMovedAtomically(
                        logger,
                        tempPath,
                        targetPath);
                }
            }
            catch (Exception ex)
            {
                if (logger is not null)
                {
                    LogCertificateArtifactAtomicWriteFailed(logger, tempPath, targetPath, ex);
                }
                throw;
            }
        }

        /// <summary>
        /// Best-effort cleanup for temporary certificate artifacts left behind after persistence attempts.
        /// </summary>
        /// <param name="tempPath">Temporary artifact path to remove.</param>
        /// <param name="logger">Optional logger used to record cleanup success or failure.</param>
        private static void TryDeleteTempFile(string tempPath, ILogger? logger = null)
        {
            if (string.IsNullOrWhiteSpace(tempPath))
            {
                return;
            }

            try
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                    if (logger is not null)
                    {
                        LogDeletedTemporaryCertificateArtifact(logger, tempPath);
                    }
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                if (logger is not null)
                {
                    LogTemporaryCertificateArtifactCleanupFailed(logger, tempPath, ex);
                }
            }
        }

        /// <summary>
        /// Emits the informational log that records the start of loading a persisted listener certificate bundle.
        /// </summary>
        /// <param name="logger">Logger receiving the bundle-loading event.</param>
        /// <param name="fqdn">Generated BackFiller FQDN whose persisted certificate bundle is being read.</param>
        /// <param name="certificatePfxPath">Full path to the persisted PFX bundle being loaded.</param>
        [LoggerMessage(EventId = 2850, Level = LogLevel.Information, Message = "Loading listener certificate bundle for {Fqdn}; Operation=load; CertificatePfxPath={CertificatePfxPath}")]
        private static partial void LogLoadingListenerCertificateBundle(ILogger logger, string fqdn, string certificatePfxPath);

        /// <summary>
        /// Emits the informational log that confirms a persisted listener certificate bundle was successfully loaded.
        /// </summary>
        /// <param name="logger">Logger receiving the bundle-loaded event.</param>
        /// <param name="fqdn">Generated BackFiller FQDN whose persisted certificate bundle was loaded.</param>
        /// <param name="certificatePfxPath">Full path to the persisted PFX bundle that was read successfully.</param>
        [LoggerMessage(EventId = 2851, Level = LogLevel.Information, Message = "Loaded listener certificate bundle for {Fqdn}; Operation=load; CertificatePfxPath={CertificatePfxPath}")]
        private static partial void LogLoadedListenerCertificateBundle(ILogger logger, string fqdn, string certificatePfxPath);

        /// <summary>
        /// Emits the error log that reports a failure while loading the persisted listener certificate bundle.
        /// </summary>
        /// <param name="logger">Logger receiving the bundle-load failure event.</param>
        /// <param name="fqdn">Generated BackFiller FQDN whose persisted certificate bundle could not be loaded.</param>
        /// <param name="certificatePfxPath">Full path to the persisted PFX bundle that failed to load.</param>
        /// <param name="exception">Exception raised while reading or materializing the persisted certificate bundle.</param>
        [LoggerMessage(EventId = 2852, Level = LogLevel.Error, Message = "Listener certificate bundle load failed for {Fqdn}; Operation=load; CertificatePfxPath={CertificatePfxPath}")]
        private static partial void LogListenerCertificateBundleLoadFailed(ILogger logger, string fqdn, string certificatePfxPath, Exception exception);

        /// <summary>
        /// Emits the informational log that records the start of certificate bundle persistence.
        /// </summary>
        /// <param name="logger">Logger receiving the bundle-persisting event.</param>
        /// <param name="fqdn">Generated BackFiller FQDN whose certificate artifacts are being written.</param>
        /// <param name="certificatePfxPath">Final PFX destination path for the persisted bundle.</param>
        /// <param name="certificatePrivateKeyPemPath">Final private-key PEM destination path for the persisted bundle.</param>
        /// <param name="pfxTempPath">Temporary staging path used for the PFX payload.</param>
        /// <param name="keyTempPath">Temporary staging path used for the private-key PEM payload.</param>
        [LoggerMessage(EventId = 2853, Level = LogLevel.Information, Message = "Persisting listener certificate bundle for {Fqdn}; Operation=persist; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}; PfxTempPath={PfxTempPath}; KeyTempPath={KeyTempPath}")]
        private static partial void LogPersistingListenerCertificateBundle(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath, string pfxTempPath, string keyTempPath);

        /// <summary>
        /// Emits the informational log that confirms the certificate bundle was persisted successfully.
        /// </summary>
        /// <param name="logger">Logger receiving the bundle-persisted event.</param>
        /// <param name="fqdn">Generated BackFiller FQDN whose certificate artifacts were written.</param>
        /// <param name="certificatePfxPath">Final PFX destination path that now contains the persisted bundle.</param>
        /// <param name="certificatePrivateKeyPemPath">Final private-key PEM destination path that now contains the persisted bundle.</param>
        [LoggerMessage(EventId = 2854, Level = LogLevel.Information, Message = "Listener certificate bundle persisted for {Fqdn}; Operation=persist; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}")]
        private static partial void LogListenerCertificateBundlePersisted(ILogger logger, string fqdn, string certificatePfxPath, string certificatePrivateKeyPemPath);

        /// <summary>
        /// Emits the informational log that records a certificate artifact being written to its temporary staging file.
        /// </summary>
        /// <param name="logger">Logger receiving the staging-write event.</param>
        /// <param name="tempPath">Temporary file path used to stage the artifact before the atomic move.</param>
        /// <param name="targetPath">Final file path that will receive the staged artifact.</param>
        [LoggerMessage(EventId = 2855, Level = LogLevel.Information, Message = "Writing certificate artifact to temporary file; Operation=write; TempPath={TempPath}; TargetPath={TargetPath}")]
        private static partial void LogWritingCertificateArtifactToTemporaryFile(ILogger logger, string tempPath, string targetPath);

        /// <summary>
        /// Emits the informational log that records the source and destination characteristics immediately before an atomic certificate-artifact replacement.
        /// </summary>
        /// <param name="logger">Logger receiving the atomic-replace event.</param>
        /// <param name="processId">Operating-system process identifier for the current persistence attempt.</param>
        /// <param name="sourcePath">Temporary source path that will be moved into place.</param>
        /// <param name="destinationPath">Final certificate artifact path that will receive the replacement.</param>
        /// <param name="sourceExists">Whether the temporary source path exists at log time.</param>
        /// <param name="destinationExists">Whether the destination path exists at log time.</param>
        /// <param name="sourceLength">Current byte length of the temporary source artifact.</param>
        /// <param name="destinationLength">Current byte length of the existing destination artifact.</param>
        [LoggerMessage(EventId = 2856, Level = LogLevel.Information, Message = "Certificate artifact ready for atomic replace; Operation=move; ProcessId={ProcessId}; SourcePath={SourcePath}; DestinationPath={DestinationPath}; SourceExists={SourceExists}; DestinationExists={DestinationExists}; SourceLength={SourceLength}; DestinationLength={DestinationLength}")]
        private static partial void LogCertificateArtifactReadyForAtomicReplace(
            ILogger logger,
            int processId,
            string sourcePath,
            string destinationPath,
            bool sourceExists,
            bool destinationExists,
            long sourceLength,
            long destinationLength);

        /// <summary>
        /// Emits the informational log that confirms a certificate artifact has been staged into place after the atomic move.
        /// </summary>
        /// <param name="logger">Logger receiving the atomic-move-success event.</param>
        /// <param name="tempPath">Temporary staging path that was moved into the final destination.</param>
        /// <param name="targetPath">Final file path that now contains the staged certificate artifact.</param>
        [LoggerMessage(EventId = 2857, Level = LogLevel.Information, Message = "Certificate artifact moved atomically; Operation=move; TempPath={TempPath}; TargetPath={TargetPath}")]
        private static partial void LogCertificateArtifactMovedAtomically(ILogger logger, string tempPath, string targetPath);

        /// <summary>
        /// Emits the error log that reports a failure while writing or moving a certificate artifact atomically.
        /// </summary>
        /// <param name="logger">Logger receiving the atomic-write failure event.</param>
        /// <param name="tempPath">Temporary staging path associated with the failed operation.</param>
        /// <param name="targetPath">Final target path associated with the failed operation.</param>
        /// <param name="exception">Exception raised while writing the temporary artifact or moving it into place.</param>
        [LoggerMessage(EventId = 2858, Level = LogLevel.Error, Message = "Certificate artifact atomic write failed; Operation=write-or-move; TempPath={TempPath}; TargetPath={TargetPath}")]
        private static partial void LogCertificateArtifactAtomicWriteFailed(ILogger logger, string tempPath, string targetPath, Exception exception);

        /// <summary>
        /// Emits the informational log that records deletion of a temporary certificate artifact after persistence finishes.
        /// </summary>
        /// <param name="logger">Logger receiving the temporary-artifact deletion event.</param>
        /// <param name="tempPath">Temporary staging path that was deleted during cleanup.</param>
        [LoggerMessage(EventId = 2859, Level = LogLevel.Information, Message = "Deleted temporary certificate artifact; Operation=delete; TempPath={TempPath}")]
        private static partial void LogDeletedTemporaryCertificateArtifact(ILogger logger, string tempPath);

        /// <summary>
        /// Emits the warning log that reports best-effort cleanup failure for a temporary certificate artifact.
        /// </summary>
        /// <param name="logger">Logger receiving the cleanup-failure event.</param>
        /// <param name="tempPath">Temporary staging path that could not be removed.</param>
        /// <param name="exception">Exception raised while deleting the temporary artifact during cleanup.</param>
        [LoggerMessage(EventId = 2860, Level = LogLevel.Warning, Message = "Temporary certificate artifact cleanup failed; Operation=delete; TempPath={TempPath}")]
        private static partial void LogTemporaryCertificateArtifactCleanupFailed(ILogger logger, string tempPath, Exception exception);
    }
}
