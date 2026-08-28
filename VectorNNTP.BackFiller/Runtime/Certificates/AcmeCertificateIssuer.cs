// <copyright file="AcmeCertificateIssuer.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Runtime.Certificates
// Implements the ACME DNS-01 issuance workflow for the BackFiller listener certificate.

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Certes;
using Certes.Acme;
using Certes.Acme.Resource;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Certificates
{
    /// <summary>
    /// Issues and renews the BackFiller listener certificate by driving the ACME DNS-01 workflow with Certes.
    /// </summary>
    /// <remarks>
    /// Certes owns the ACME protocol state machine: account lookup or creation, orders, authorizations, and challenge
    /// validation. Microsoft/.NET cryptography is used for the local key lifecycle because Certes returns and accepts
    /// PEM material, while the BackFiller listener ultimately needs a locally generated private key, a CSR, and a
    /// certificate chain that can be persisted as PFX/PEM artifacts for the inbound TLS listener.
    /// 
    /// The issuer never logs the ACME account PEM, the certificate private key, or the DNS-01 TXT value. Temporary
    /// challenge records are still cleaned up when the issuance workflow fails, and cancellation is allowed to abort
    /// the issuance without converting it into a successful result.
    /// </remarks>
    internal sealed partial class AcmeCertificateIssuer : IAcmeCertificateIssuer
    {
        /// <summary>
        /// Poll cadence used while waiting for ACME authorization, challenge, and order state transitions.
        /// </summary>
        private static readonly TimeSpan DefaultAcmePollInterval = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Clock source used for ACME deadlines and retry delay calculations.
        /// </summary>
        private readonly TimeProvider _timeProvider;

        /// <summary>
        /// Logger used for high-level ACME workflow milestones without secret material.
        /// </summary>
        private readonly ILogger<AcmeCertificateIssuer> _logger;

        /// <summary>
        /// Verifier that waits for the DNS-01 TXT challenge to appear at authoritative nameservers.
        /// </summary>
        private readonly IAuthoritativeDnsTxtPropagationVerifier _dnsPropagationVerifier;

        /// <summary>
        /// Factory that creates one Cloudflare TXT-record client per issuance attempt.
        /// </summary>
        private readonly Func<string, ICloudflareTxtRecordApi> _txtRecordClientFactory;

        /// <summary>
        /// Initializes one ACME certificate issuer.
        /// </summary>
        /// <param name="timeProvider">Unified time provider.</param>
        /// <param name="logger">Logger for ACME workflow diagnostics.</param>
        /// <param name="dnsPropagationVerifier">Authoritative TXT propagation verifier.</param>
        /// <param name="txtRecordClientFactory">Factory for Cloudflare TXT record clients.</param>
        public AcmeCertificateIssuer(
            TimeProvider timeProvider,
            ILogger<AcmeCertificateIssuer> logger,
            IAuthoritativeDnsTxtPropagationVerifier dnsPropagationVerifier,
            Func<string, ICloudflareTxtRecordApi>? txtRecordClientFactory = null)
        {
            ArgumentNullException.ThrowIfNull(timeProvider);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(dnsPropagationVerifier);

            _timeProvider = timeProvider;
            _logger = logger;
            _dnsPropagationVerifier = dnsPropagationVerifier;
            _txtRecordClientFactory = txtRecordClientFactory ?? (apiToken => new CloudflareTxtRecordApi(apiToken));
        }

        /// <summary>
        /// Issues one listener certificate by walking the full ACME DNS-01 lifecycle.
        /// </summary>
        /// <param name="letsEncryptOptions">Validated ACME runtime options that identify the generated FQDN and secret sources.</param>
        /// <param name="cancellationToken">Cancellation token that aborts the issuance workflow without converting it into success.</param>
        /// <returns>
        /// The leaf certificate DER, issuer chain DER, and the locally generated certificate private key PEM that must
        /// later be persisted with the listener certificate bundle.
        /// </returns>
        /// <exception cref="InvalidOperationException">Thrown when the ACME account, order, challenge, or certificate material is invalid.</exception>
        /// <exception cref="TimeoutException">Thrown when DNS propagation or ACME state transitions do not complete in time.</exception>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested before issuance completes.</exception>
        public async Task<AcmeOrderIssueResult> IssueCertificateAsync(
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(letsEncryptOptions);

            cancellationToken.ThrowIfCancellationRequested();

            string accountKeyPem = await ReadRequiredPemAsync(letsEncryptOptions.AcmeAccountKeyPemPath, cancellationToken).ConfigureAwait(false);
            IKey accountKey;
            try
            {
                accountKey = KeyFactory.FromPem(accountKeyPem);
            }
            catch (Exception ex) when (ex is CryptographicException or ArgumentException or InvalidOperationException)
            {
                throw new InvalidOperationException($"Configured ACME account key is invalid: {letsEncryptOptions.AcmeAccountKeyPemPath}", ex);
            }

            IKey certificateKey = await LoadOrCreateCertificateKeyAsync(letsEncryptOptions, cancellationToken).ConfigureAwait(false);
            string certificatePrivateKeyPem = certificateKey.ToPem();

            Uri directoryUri = letsEncryptOptions.UseStagingDirectory
                ? WellKnownServers.LetsEncryptStagingV2
                : WellKnownServers.LetsEncryptV2;

            AcmeContext acmeContext = new(directoryUri, accountKey);
            await EnsureAccountAsync(acmeContext, letsEncryptOptions.AcmeAccountEmail, cancellationToken).ConfigureAwait(false);

            IOrderContext orderContext = await acmeContext
                .NewOrder([letsEncryptOptions.CanonicalCertificateSubjectName])
                .ConfigureAwait(false);
            LogAcmeOrderCreated(_logger, letsEncryptOptions.CanonicalCertificateSubjectName);

            ICloudflareTxtRecordApi txtRecordClient = _txtRecordClientFactory(letsEncryptOptions.CloudFlareApiToken);
            await using (txtRecordClient.ConfigureAwait(false))
            {
                IReadOnlyList<IAuthorizationContext> authorizations = [.. await orderContext.Authorizations().ConfigureAwait(false)];
                for (int index = 0; index < authorizations.Count; index++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await CompleteAuthorizationAsync(
                        acmeContext,
                        authorizations[index],
                        txtRecordClient,
                        letsEncryptOptions,
                        cancellationToken).ConfigureAwait(false);
                }

                await WaitForOrderReadyAsync(orderContext, letsEncryptOptions, cancellationToken).ConfigureAwait(false);

                byte[] csr = BuildCertificateSigningRequest(certificateKey, letsEncryptOptions.CanonicalCertificateSubjectName);
                _ = await orderContext.Finalize(csr).ConfigureAwait(false);

                Order finalizedOrder = await WaitForOrderIssuedAsync(orderContext, letsEncryptOptions, cancellationToken).ConfigureAwait(false);
                if (finalizedOrder.Status != OrderStatus.Valid)
                {
                    throw new InvalidOperationException($"ACME order reached unexpected terminal state: {finalizedOrder.Status}");
                }

                CertificateChain chain = await orderContext.Download().ConfigureAwait(false);
                byte[] leaf = chain.Certificate.ToDer();
                IReadOnlyList<byte[]> issuers = [.. chain.Issuers.Select(static issuer => issuer.ToDer())];

                return new AcmeOrderIssueResult(
                    LeafCertificateDer: leaf,
                    ChainDer: issuers,
                    CertificatePrivateKeyPem: certificatePrivateKeyPem);
            }
        }

        /// <summary>
        /// Ensures that an ACME account exists before order creation begins.
        /// </summary>
        /// <remarks>
        /// The account key is loaded from disk before this method runs. If an account already exists for that key, it
        /// is reused; otherwise a new account is created with the configured contact address and terms-of-service
        /// agreement.
        /// </remarks>
        /// <param name="acmeContext">ACME context bound to the selected directory and account key.</param>
        /// <param name="email">Contact email used when a new account must be created.</param>
        /// <param name="cancellationToken">Cancellation token that aborts account creation.</param>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested before or during account creation.</exception>
        private static async Task EnsureAccountAsync(AcmeContext acmeContext, string email, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(acmeContext);
            ArgumentException.ThrowIfNullOrWhiteSpace(email);

            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                _ = await acmeContext.Account().ConfigureAwait(false);
                return;
            }
            catch
            {
                cancellationToken.ThrowIfCancellationRequested();
            }

            _ = await acmeContext.NewAccount(["mailto:" + email.Trim()], termsOfServiceAgreed: true).ConfigureAwait(false);
        }

        /// <summary>
        /// Completes one ACME authorization by creating, validating, and cleaning up the DNS-01 TXT challenge.
        /// </summary>
        /// <remarks>
        /// The TXT record is created before propagation polling begins and is always targeted for cleanup afterwards.
        /// If authorization fails, the original workflow exception remains primary; cleanup failures are only surfaced
        /// when there was no prior ACME/provisioning failure to preserve.
        /// </remarks>
        /// <param name="acmeContext">ACME context used to derive the DNS-01 challenge token.</param>
        /// <param name="authorizationContext">Authorization to complete.</param>
        /// <param name="txtRecordClient">Cloudflare TXT record client for challenge lifecycle management.</param>
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="cancellationToken">Cancellation token that aborts challenge processing.</param>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested before the authorization completes.</exception>
        /// <exception cref="Exception">Propagates the original ACME/provisioning failure, or a cleanup failure when no prior failure occurred.</exception>
        private async Task CompleteAuthorizationAsync(
            AcmeContext acmeContext,
            IAuthorizationContext authorizationContext,
            ICloudflareTxtRecordApi txtRecordClient,
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(acmeContext);
            ArgumentNullException.ThrowIfNull(authorizationContext);
            ArgumentNullException.ThrowIfNull(txtRecordClient);
            ArgumentNullException.ThrowIfNull(letsEncryptOptions);

            Authorization authorization = await authorizationContext.Resource().ConfigureAwait(false);
            if (authorization.Status == AuthorizationStatus.Valid)
            {
                return;
            }

            if (authorization.Status == AuthorizationStatus.Invalid)
            {
                throw new InvalidOperationException("ACME authorization is already invalid before DNS challenge handling.");
            }

            IChallengeContext dnsChallengeContext = await authorizationContext.Dns().ConfigureAwait(false);
            string txtHostName = $"_acme-challenge.{letsEncryptOptions.CanonicalCertificateSubjectName}";
            string txtValue = acmeContext.AccountKey.DnsTxt(dnsChallengeContext.Token);

            DnsChallengeRecordLease? lease = null;
            Exception? workflowException = null;
            Exception? cleanupException = null;

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                IReadOnlyList<CloudflareTxtRecordInfo> existingTxtRecords = await txtRecordClient
                    .GetTxtRecordsAsync(letsEncryptOptions.CloudFlareZoneId, txtHostName, cancellationToken)
                    .ConfigureAwait(false);

                string? existingOwnedRecordId = await ReconcileExistingChallengeRecordsAsync(
                    txtRecordClient,
                    letsEncryptOptions.CloudFlareZoneId,
                    txtHostName,
                    txtValue,
                    existingTxtRecords,
                    cancellationToken).ConfigureAwait(false);

                CloudflareTxtRecordInfo createdOrReusedRecord;
                bool recordAlreadyOwned = existingOwnedRecordId is not null;
                createdOrReusedRecord = recordAlreadyOwned
                    ? existingTxtRecords.First(record => string.Equals(record.Id, existingOwnedRecordId, StringComparison.Ordinal))
                    : await txtRecordClient
                        .AddTxtRecordAsync(letsEncryptOptions.CloudFlareZoneId, txtHostName, txtValue, cancellationToken)
                        .ConfigureAwait(false);

                lease = new DnsChallengeRecordLease(
                    ZoneId: letsEncryptOptions.CloudFlareZoneId,
                    RecordId: createdOrReusedRecord.Id,
                    RecordName: txtHostName,
                    RecordValue: txtValue,
                    IsOwnedByCurrentAttempt: !recordAlreadyOwned);

                if (!recordAlreadyOwned)
                {
                    LogDnsTxtRecordCreated(_logger, txtHostName, createdOrReusedRecord.Id);
                }

                await _dnsPropagationVerifier
                    .WaitForPropagationAsync(txtHostName, txtValue, letsEncryptOptions, cancellationToken)
                    .ConfigureAwait(false);

                cancellationToken.ThrowIfCancellationRequested();
                _ = await dnsChallengeContext.Validate().ConfigureAwait(false);

                await WaitForChallengeStatusAsync(dnsChallengeContext, letsEncryptOptions, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                workflowException = ex;
            }
            finally
            {
                if (lease is not null && lease.IsOwnedByCurrentAttempt)
                {
                    try
                    {
                        await txtRecordClient
                            .DeleteTxtRecordAsync(lease.ZoneId, lease.RecordId, CancellationToken.None)
                            .ConfigureAwait(false);

                        LogDnsTxtRecordRemoved(_logger, lease.RecordName, lease.RecordId);
                    }
                    catch (Exception cleanupEx)
                    {
                        cleanupException = cleanupEx;
                        LogDnsTxtRecordCleanupFailed(_logger, cleanupEx, lease.RecordName, lease.RecordId);
                    }
                }
            }

            if (workflowException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(workflowException).Throw();
            }

            if (cleanupException is not null)
            {
                throw new InvalidOperationException("ACME DNS-01 TXT record cleanup failed.", cleanupException);
            }

            Authorization postAuthorization = await authorizationContext.Resource().ConfigureAwait(false);
            if (postAuthorization.Status != AuthorizationStatus.Valid)
            {
                throw new InvalidOperationException($"ACME authorization did not reach valid state. Status={postAuthorization.Status}");
            }
        }

        /// <summary>
        /// Waits for the ACME challenge resource to transition to a terminal state.
        /// </summary>
        /// <remarks>
        /// The loop intentionally polls with a small fixed cadence so the issuer can react to cancellation without
        /// relying on an implementation-specific Certes callback model.
        /// </remarks>
        /// <param name="challengeContext">ACME challenge context.</param>
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <exception cref="InvalidOperationException">Thrown when the challenge becomes invalid before completion.</exception>
        /// <exception cref="TimeoutException">Thrown when the challenge does not become valid before the deadline.</exception>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested while polling.</exception>
        private async Task WaitForChallengeStatusAsync(
            IChallengeContext challengeContext,
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken)
        {
            DateTimeOffset deadline = _timeProvider.GetUtcNow().AddSeconds(letsEncryptOptions.DnsTxtPollTimeoutSeconds);
            while (_timeProvider.GetUtcNow() <= deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Challenge challenge = await challengeContext.Resource().ConfigureAwait(false);
                if (challenge.Status == ChallengeStatus.Valid)
                {
                    return;
                }

                if (challenge.Status == ChallengeStatus.Invalid)
                {
                    throw new InvalidOperationException("ACME DNS challenge validation returned invalid status.");
                }

                await Task.Delay(DefaultAcmePollInterval, cancellationToken).ConfigureAwait(false);
            }

            throw new TimeoutException("ACME DNS challenge validation timed out.");
        }

        /// <summary>
        /// Waits for the ACME order to become ready for finalization.
        /// </summary>
        /// <param name="orderContext">ACME order context.</param>
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <exception cref="InvalidOperationException">Thrown when the order becomes invalid before finalization.</exception>
        /// <exception cref="TimeoutException">Thrown when the order does not become ready before the deadline.</exception>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested while polling.</exception>
        private async Task WaitForOrderReadyAsync(
            IOrderContext orderContext,
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken)
        {
            DateTimeOffset deadline = _timeProvider.GetUtcNow().AddSeconds(letsEncryptOptions.DnsTxtPollTimeoutSeconds);
            while (_timeProvider.GetUtcNow() <= deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Order order = await orderContext.Resource().ConfigureAwait(false);
                if (order.Status == OrderStatus.Ready)
                {
                    return;
                }

                if (order.Status == OrderStatus.Invalid)
                {
                    throw new InvalidOperationException("ACME order became invalid before finalize.");
                }

                await Task.Delay(DefaultAcmePollInterval, cancellationToken).ConfigureAwait(false);
            }

            throw new TimeoutException("ACME order did not become ready before timeout.");
        }

        /// <summary>
        /// Waits for the finalized ACME order to return the issued certificate chain.
        /// </summary>
        /// <param name="orderContext">ACME order context.</param>
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The terminal valid order resource.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the order becomes invalid during finalization.</exception>
        /// <exception cref="TimeoutException">Thrown when the order does not become valid before the deadline.</exception>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested while polling.</exception>
        private async Task<Order> WaitForOrderIssuedAsync(
            IOrderContext orderContext,
            BackFillerLetsEncryptRuntimeOptions letsEncryptOptions,
            CancellationToken cancellationToken)
        {
            DateTimeOffset deadline = _timeProvider.GetUtcNow().AddSeconds(letsEncryptOptions.DnsTxtPollTimeoutSeconds);
            while (_timeProvider.GetUtcNow() <= deadline)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Order order = await orderContext.Resource().ConfigureAwait(false);
                if (order.Status == OrderStatus.Valid)
                {
                    return order;
                }

                if (order.Status == OrderStatus.Invalid)
                {
                    throw new InvalidOperationException("ACME order became invalid during finalization.");
                }

                await Task.Delay(DefaultAcmePollInterval, cancellationToken).ConfigureAwait(false);
            }

            throw new TimeoutException("ACME order finalization timed out.");
        }

        /// <summary>
        /// Builds the CSR used to request the listener certificate for the generated BackFiller FQDN.
        /// </summary>
        /// <remarks>
        /// Certes does not create the CSR; the PEM-encoded key returned earlier in the workflow is imported with
        /// Microsoft/.NET cryptography so the BackFiller listener can persist and later reload the resulting key pair.
        /// </remarks>
        /// <param name="certificateKey">Locally generated certificate private key.</param>
        /// <param name="fqdn">Generated BackFiller FQDN used for both the CN and SAN.</param>
        /// <returns>DER-encoded CSR bytes.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the private key algorithm is not RSA or ECDSA.</exception>
        private static byte[] BuildCertificateSigningRequest(IKey certificateKey, string fqdn)
        {
            ArgumentNullException.ThrowIfNull(certificateKey);
            ArgumentException.ThrowIfNullOrWhiteSpace(fqdn);

            using AsymmetricAlgorithm key = ImportCsrPrivateKey(certificateKey.ToPem());

            CertificateRequest request = key switch
            {
                RSA rsa => new CertificateRequest(
                    $"CN={fqdn}",
                    rsa,
                    HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1),
                ECDsa ecdsa => new CertificateRequest(
                    $"CN={fqdn}",
                    ecdsa,
                    HashAlgorithmName.SHA256),
                _ => throw new InvalidOperationException("Unsupported certificate private key algorithm."),
            };

            SubjectAlternativeNameBuilder sanBuilder = new();
            sanBuilder.AddDnsName(fqdn);
            request.CertificateExtensions.Add(sanBuilder.Build());

            X509KeyUsageFlags keyUsage = key is RSA
                ? X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment
                : X509KeyUsageFlags.DigitalSignature;
            request.CertificateExtensions.Add(new X509KeyUsageExtension(keyUsage, critical: true));

            OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, critical: true));

            return request.CreateSigningRequest();
        }

        /// <summary>
        /// Imports the PEM-encoded certificate private key using the first supported Microsoft/.NET key algorithm.
        /// </summary>
        /// <remarks>
        /// RSA is tried first because it is the default generated algorithm for listener keys. ECDSA is accepted so
        /// the store can load either key family if a different certificate source is supplied.
        /// </remarks>
        /// <param name="pem">PEM-encoded private key.</param>
        /// <returns>An asymmetric algorithm instance that owns the imported key material.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the PEM content is not a supported RSA/ECDSA private key.</exception>
        private static AsymmetricAlgorithm ImportCsrPrivateKey(string pem)
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
        /// Reads one required PEM file from disk and returns its text contents.
        /// </summary>
        /// <remarks>
        /// The caller is responsible for deciding whether the file represents the ACME account key or the listener
        /// certificate private key. The returned text is sensitive and is used only for cryptographic parsing.
        /// </remarks>
        /// <param name="path">Absolute path to the required PEM file.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The non-empty PEM contents.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the file is missing or empty.</exception>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested while reading.</exception>
        private static async Task<string> ReadRequiredPemAsync(string path, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);

            if (!File.Exists(path))
            {
                throw new InvalidOperationException($"Required PEM file was not found: {path}");
            }

            string pem = await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false);
            return string.IsNullOrWhiteSpace(pem) ? throw new InvalidOperationException($"Required PEM file is empty: {path}") : pem;
        }

        /// <summary>
        /// Loads the persisted certificate private key or generates one and atomically persists it for future reuse.
        /// </summary>
        /// <remarks>
        /// The generated private key is intentionally written to a same-directory temporary file first so the final
        /// key path is replaced atomically. This protects the listener certificate pipeline from partially written
        /// key material during startup or renewal.
        /// </remarks>
        /// <param name="letsEncryptOptions">Validated ACME runtime options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The loaded or newly generated ACME certificate private key.</returns>
        /// <exception cref="InvalidOperationException">Thrown when an existing key file is empty or malformed.</exception>
        /// <exception cref="OperationCanceledException">Thrown when cancellation is requested while reading or writing the key file.</exception>
        private static async Task<IKey> LoadOrCreateCertificateKeyAsync(BackFillerLetsEncryptRuntimeOptions letsEncryptOptions, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(letsEncryptOptions);

            string path = letsEncryptOptions.CertificatePrivateKeyPemPath;
            if (File.Exists(path))
            {
                string existingPem = await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false);
                if (string.IsNullOrWhiteSpace(existingPem))
                {
                    throw new InvalidOperationException($"Certificate private key file is empty: {path}");
                }

                try
                {
                    return KeyFactory.FromPem(existingPem);
                }
                catch (Exception ex) when (ex is CryptographicException or ArgumentException or InvalidOperationException)
                {
                    throw new InvalidOperationException($"Certificate private key file is malformed: {path}", ex);
                }
            }

            IKey generated = KeyFactory.NewKey(KeyAlgorithm.RS256);
            string generatedPem = generated.ToPem();
            string tempPath = CertificateFileConventions.BuildAtomicTempPath(path);

            try
            {
                await File.WriteAllTextAsync(tempPath, generatedPem, cancellationToken).ConfigureAwait(false);
                File.Move(tempPath, path, overwrite: false);
            }
            finally
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }

            return generated;
        }

        [LoggerMessage(EventId = 2800, Level = LogLevel.Information, Message = "ACME order created for generated BackFiller FQDN {Fqdn}")]
        private static partial void LogAcmeOrderCreated(ILogger logger, string fqdn);

        [LoggerMessage(EventId = 2801, Level = LogLevel.Information, Message = "ACME DNS-01 TXT record created for {RecordName}; RecordId={RecordId}")]
        private static partial void LogDnsTxtRecordCreated(ILogger logger, string recordName, string recordId);

        [LoggerMessage(EventId = 2802, Level = LogLevel.Information, Message = "ACME DNS-01 TXT record removed for {RecordName}; RecordId={RecordId}")]
        private static partial void LogDnsTxtRecordRemoved(ILogger logger, string recordName, string recordId);

        [LoggerMessage(EventId = 2803, Level = LogLevel.Warning, Message = "ACME DNS-01 TXT record cleanup failed for {RecordName}; RecordId={RecordId}")]
        private static partial void LogDnsTxtRecordCleanupFailed(ILogger logger, Exception exception, string recordName, string recordId);
    }
}
