using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Certificates;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

public sealed class BackFillerCertificateStoreTests
{
    [Fact]
    public async Task EvaluateExistingCertificateAsync_WhenCertificateMissing_ReturnsUnusableAndRequiresRenewal()
    {
        string tempDir = CreateUniqueTempDirectory();
        try
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
            CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

            Assert.False(result.HasCertificate);
            Assert.False(result.IsUsable);
            Assert.True(result.RequiresRenewal);
        }
        finally
        {
            DeleteDirectoryIfExists(tempDir);
        }
    }

    [Fact]
    public async Task EvaluateExistingCertificateAsync_WhenValidCertificateOutsideRenewalWindow_ReturnsUsableWithoutRenewal()
    {
        string tempDir = CreateUniqueTempDirectory();
        try
        {
            string fqdn = "bf-01.example.com";
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn, renewBeforeExpiryDays: 7);
            CreateAndWritePfx(options.CertificatePfxPath, options.PfxExportPassword, fqdn, notBeforeUtc: DateTimeOffset.UtcNow.AddDays(-2), notAfterUtc: DateTimeOffset.UtcNow.AddDays(30));

            CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

            Assert.True(result.HasCertificate);
            Assert.True(result.IsUsable);
            Assert.False(result.RequiresRenewal);
            result.Certificate?.Certificate.Dispose();
        }
        finally
        {
            DeleteDirectoryIfExists(tempDir);
        }
    }

    [Fact]
    public async Task EvaluateExistingCertificateAsync_WhenCertificateInsideRenewalWindow_ReturnsRequiresRenewal()
    {
        string tempDir = CreateUniqueTempDirectory();
        try
        {
            string fqdn = "bf-01.example.com";
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn, renewBeforeExpiryDays: 10);
            CreateAndWritePfx(options.CertificatePfxPath, options.PfxExportPassword, fqdn, notBeforeUtc: DateTimeOffset.UtcNow.AddDays(-2), notAfterUtc: DateTimeOffset.UtcNow.AddDays(5));

            CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

            Assert.True(result.HasCertificate);
            Assert.True(result.IsUsable);
            Assert.True(result.RequiresRenewal);
            result.Certificate?.Certificate.Dispose();
        }
        finally
        {
            DeleteDirectoryIfExists(tempDir);
        }
    }

    [Fact]
    public async Task EvaluateExistingCertificateAsync_WhenCertificateFqdnMismatch_ReturnsUnusable()
    {
        string tempDir = CreateUniqueTempDirectory();
        try
        {
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, "bf-01.example.com");
            CreateAndWritePfx(options.CertificatePfxPath, options.PfxExportPassword, "bf-99.example.com", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(20));

            CertificateEvaluationResult result = await BackFillerCertificateStore.EvaluateExistingCertificateAsync(options, TimeProvider.System, CancellationToken.None);

            Assert.True(result.HasCertificate);
            Assert.False(result.IsUsable);
            Assert.True(result.RequiresRenewal);
        }
        finally
        {
            DeleteDirectoryIfExists(tempDir);
        }
    }

    [Fact]
    public async Task PersistIssuedCertificateAsync_WritesLoadablePfxAndKey()
    {
        string tempDir = CreateUniqueTempDirectory();
        try
        {
            string fqdn = "bf-01.example.com";
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);
            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new(
                $"CN={fqdn}",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            SubjectAlternativeNameBuilder san = new();
            san.AddDnsName(fqdn);
            request.CertificateExtensions.Add(san.Build());

            using X509Certificate2 certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
            AcmeOrderIssueResult issueResult = new(
                LeafCertificateDer: certificate.Export(X509ContentType.Cert),
                ChainDer: [],
                CertificatePrivateKeyPem: rsa.ExportPkcs8PrivateKeyPem());

            await BackFillerCertificateStore.PersistIssuedCertificateAsync(options, issueResult, CancellationToken.None);

            Assert.True(File.Exists(options.CertificatePfxPath));
            Assert.True(File.Exists(options.CertificatePrivateKeyPemPath));

            BackFillerCertificateBundle bundle = await BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None);
            Assert.True(bundle.Certificate.HasPrivateKey);
            bundle.Certificate.Dispose();
        }
        finally
        {
            DeleteDirectoryIfExists(tempDir);
        }
    }

    [Fact]
    public async Task PersistIssuedCertificateAsync_WhenLeafIsEcdsa_WritesLoadablePfxAndKey()
    {
        string tempDir = CreateUniqueTempDirectory();
        try
        {
            string fqdn = "bf-01.example.com";
            BackFillerLetsEncryptRuntimeOptions options = CreateLetsEncryptOptions(tempDir, fqdn);
            using ECDsa ecdsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);
            CertificateRequest request = new(
                $"CN={fqdn}",
                ecdsa,
                HashAlgorithmName.SHA256);
            SubjectAlternativeNameBuilder san = new();
            san.AddDnsName(fqdn);
            request.CertificateExtensions.Add(san.Build());

            using X509Certificate2 certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
            AcmeOrderIssueResult issueResult = new(
                LeafCertificateDer: certificate.Export(X509ContentType.Cert),
                ChainDer: [],
                CertificatePrivateKeyPem: ecdsa.ExportPkcs8PrivateKeyPem());

            await BackFillerCertificateStore.PersistIssuedCertificateAsync(options, issueResult, CancellationToken.None);

            BackFillerCertificateBundle bundle = await BackFillerCertificateStore.LoadCertificateBundleAsync(options, TimeProvider.System, CancellationToken.None);
            Assert.True(bundle.Certificate.HasPrivateKey);
            Assert.Equal("1.2.840.10045.2.1", bundle.Certificate.PublicKey.Oid?.Value);
            bundle.Certificate.Dispose();
        }
        finally
        {
            DeleteDirectoryIfExists(tempDir);
        }
    }

    private static BackFillerLetsEncryptRuntimeOptions CreateLetsEncryptOptions(string tempDir, string fqdn, int renewBeforeExpiryDays = 7)
    {
        Directory.CreateDirectory(tempDir);
        return new BackFillerLetsEncryptRuntimeOptions(
            Enabled: true,
            CanonicalCertificateSubjectName: fqdn,
            AcmeAccountEmail: "security@example.com",
            AcmeAccountKeyPemPath: Path.Combine(tempDir, "account.key"),
            CertificatePfxPath: Path.Combine(tempDir, "backfiller-listener.pfx"),
            CertificatePrivateKeyPemPath: Path.Combine(tempDir, "certificate.key"),
            PfxExportPassword: "UnitTest-PfxPassword-123!",
            RenewBeforeExpiryDays: renewBeforeExpiryDays,
            RenewalCheckIntervalHours: 6,
            RenewalJitterRatio: 0.1,
            UseStagingDirectory: true,
            AcmeTransientRetryMaxAttempts: 5,
            DnsPropagationDelaySeconds: 0,
            DnsTxtPollIntervalSeconds: 1,
            DnsTxtPollTimeoutSeconds: 10,
            DnsAuthoritativeNsCacheMinutes: 1,
            DnsAuthoritativeQuorumRatio: 0.7,
            CloudFlareApiToken: "token",
            CloudFlareZoneId: "zone");
    }

    private static void CreateAndWritePfx(string pfxPath, string password, string fqdn, DateTimeOffset notBeforeUtc, DateTimeOffset notAfterUtc)
    {
        using RSA rsa = RSA.Create(2048);
        CertificateRequest request = new(
            $"CN={fqdn}",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);

        SubjectAlternativeNameBuilder sanBuilder = new();
        sanBuilder.AddDnsName(fqdn);
        request.CertificateExtensions.Add(sanBuilder.Build());
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        OidCollection enhancedKeyUsages = [new Oid("1.3.6.1.5.5.7.3.1")];
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsages, true));

        using X509Certificate2 certificate = request.CreateSelfSigned(notBeforeUtc, notAfterUtc);
        byte[] pfx = certificate.Export(X509ContentType.Pkcs12, password);
        File.WriteAllBytes(pfxPath, pfx);
    }

    private static string CreateUniqueTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"VectorNNTP-BackFiller-StoreTests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (!Directory.Exists(path))
        {
            return;
        }

        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch
        {
        }
    }
}
