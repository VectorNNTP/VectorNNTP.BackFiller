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
    internal sealed class BackFillerCertificateStore
    {
        /// <summary>
        /// Evaluates whether an existing persisted listener certificate is usable and whether renewal is required.
        /// </summary>
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="timeProvider">Unified time provider.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
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
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="timeProvider">Unified time provider.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Loaded listener certificate bundle.</returns>
        public static async Task<BackFillerCertificateBundle> LoadCertificateBundleAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            TimeProvider timeProvider,
            CancellationToken cancellationToken,
            ILogger? logger = null)
        {
            ArgumentNullException.ThrowIfNull(letsEncryptOptions);
            ArgumentNullException.ThrowIfNull(timeProvider);

            cancellationToken.ThrowIfCancellationRequested();

            logger?.LogInformation(
                "Loading listener certificate bundle for {Fqdn}; Operation=load; CertificatePfxPath={CertificatePfxPath}",
                letsEncryptOptions.CanonicalCertificateSubjectName,
                letsEncryptOptions.CertificatePfxPath);

            try
            {
                byte[] pfx = await File.ReadAllBytesAsync(letsEncryptOptions.CertificatePfxPath, cancellationToken).ConfigureAwait(false);
                X509Certificate2 certificate = new(
                    pfx,
                    letsEncryptOptions.PfxExportPassword,
                    X509KeyStorageFlags.EphemeralKeySet | X509KeyStorageFlags.Exportable);

                logger?.LogInformation(
                    "Loaded listener certificate bundle for {Fqdn}; Operation=load; CertificatePfxPath={CertificatePfxPath}",
                    letsEncryptOptions.CanonicalCertificateSubjectName,
                    letsEncryptOptions.CertificatePfxPath);

                return new BackFillerCertificateBundle(certificate, letsEncryptOptions.CertificatePfxPath, timeProvider.GetUtcNow());
            }
            catch (Exception ex)
            {
                logger?.LogError(
                    ex,
                    "Listener certificate bundle load failed for {Fqdn}; Operation=load; CertificatePfxPath={CertificatePfxPath}",
                    letsEncryptOptions.CanonicalCertificateSubjectName,
                    letsEncryptOptions.CertificatePfxPath);
                throw;
            }
        }

        /// <summary>
        /// Persists newly issued certificate artifacts using atomic replacement semantics.
        /// </summary>
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="issueResult">Issued ACME certificate artifacts.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
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

            logger?.LogInformation(
                "Persisting listener certificate bundle for {Fqdn}; Operation=persist; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}; PfxTempPath={PfxTempPath}; KeyTempPath={KeyTempPath}",
                letsEncryptOptions.CanonicalCertificateSubjectName,
                letsEncryptOptions.CertificatePfxPath,
                letsEncryptOptions.CertificatePrivateKeyPemPath,
                pfxTempPath,
                keyTempPath);

            try
            {
                await WriteFileAtomicallyAsync(keyTempPath, letsEncryptOptions.CertificatePrivateKeyPemPath, issueResult.CertificatePrivateKeyPem, cancellationToken, logger).ConfigureAwait(false);
                await WriteFileAtomicallyAsync(pfxTempPath, letsEncryptOptions.CertificatePfxPath, pfx, cancellationToken, logger).ConfigureAwait(false);

                logger?.LogInformation(
                    "Listener certificate bundle persisted for {Fqdn}; Operation=persist; CertificatePfxPath={CertificatePfxPath}; CertificatePrivateKeyPemPath={CertificatePrivateKeyPemPath}",
                    letsEncryptOptions.CanonicalCertificateSubjectName,
                    letsEncryptOptions.CertificatePfxPath,
                    letsEncryptOptions.CertificatePrivateKeyPemPath);
            }
            finally
            {
                TryDeleteTempFile(keyTempPath, logger);
                TryDeleteTempFile(pfxTempPath, logger);
            }
        }

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

        private static async Task WriteFileAtomicallyAsync(string tempPath, string targetPath, string content, CancellationToken cancellationToken, ILogger? logger = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tempPath);
            ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);
            ArgumentNullException.ThrowIfNull(content);

            byte[] payload = System.Text.Encoding.UTF8.GetBytes(content);
            await WriteFileAtomicallyAsync(tempPath, targetPath, payload, cancellationToken, logger).ConfigureAwait(false);
        }

        private static async Task WriteFileAtomicallyAsync(string tempPath, string targetPath, byte[] payload, CancellationToken cancellationToken, ILogger? logger = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tempPath);
            ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);
            ArgumentNullException.ThrowIfNull(payload);

            cancellationToken.ThrowIfCancellationRequested();

            logger?.LogInformation(
                "Writing certificate artifact to temporary file; Operation=write; TempPath={TempPath}; TargetPath={TargetPath}",
                tempPath,
                targetPath);

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

                FileInfo tempInfo = new(tempPath);
                FileInfo? targetInfo = File.Exists(targetPath) ? new FileInfo(targetPath) : null;
                logger?.LogInformation(
                    "Certificate artifact ready for atomic replace; Operation=move; ProcessId={ProcessId}; SourcePath={SourcePath}; DestinationPath={DestinationPath}; SourceExists={SourceExists}; DestinationExists={DestinationExists}; SourceLength={SourceLength}; DestinationLength={DestinationLength}",
                    Environment.ProcessId,
                    tempInfo.FullName,
                    Path.GetFullPath(targetPath),
                    tempInfo.Exists,
                    targetInfo is not null,
                    tempInfo.Exists ? tempInfo.Length : -1L,
                    targetInfo?.Length ?? -1L);

                File.Move(tempPath, targetPath, overwrite: true);

                logger?.LogInformation(
                    "Certificate artifact moved atomically; Operation=move; TempPath={TempPath}; TargetPath={TargetPath}",
                    tempPath,
                    targetPath);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Certificate artifact atomic write failed; Operation=write-or-move; TempPath={TempPath}; TargetPath={TargetPath}", tempPath, targetPath);
                throw;
            }
        }

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
                    logger?.LogInformation("Deleted temporary certificate artifact; Operation=delete; TempPath={TempPath}", tempPath);
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                logger?.LogWarning(ex, "Temporary certificate artifact cleanup failed; Operation=delete; TempPath={TempPath}", tempPath);
            }
        }
    }
}
