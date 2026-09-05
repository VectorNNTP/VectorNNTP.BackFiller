// <copyright file="BackFillerOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: back filler options in the vector nntp.back filler configuration subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// BackFillerOptions.cs -- Strongly-typed configuration for BackFiller service settings.
//
// Supplies validation for BackFiller configuration section including network binding configuration.
//
// Validation rules for BindAddress:
//   - May be omitted; omitted means bind to all interfaces
//   - Each configured address must be syntactically valid IPv4 or IPv6
//   - Wildcard addresses (0.0.0.0 and ::) are permitted and mean all interfaces for that family
//   - Non-wildcard addresses must be assigned to a local network interface
//   - Duplicate addresses are not permitted
//   - Each address must be available for binding
//   - Must not conflict with existing listeners on the same address:port
//
// Validation rules for BindPort:
//   - Must be present and within valid range (1-65535)
//   - Should be non-privileged port (>=1024) for non-root execution
//
// The BackFiller service creates TCP listeners for incoming NNTP connections.
// IPv4 and IPv6 listeners are treated as independent endpoints.

using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Security;
using System.Security.Cryptography;

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// One validation diagnostic emitted while checking BackFiller listener bind-address configuration.
    /// </summary>
    /// <param name="Setting">Configuration key associated with the reported bind-address problem.</param>
    /// <param name="Message">Human-readable validation message describing the detected condition.</param>
    /// <param name="Severity">Severity that determines whether startup may continue.</param>
    internal record BindAddressValidationResult(
        string Setting,
        string Message,
        ValidationSeverity Severity);

    /// <summary>
    /// Configuration options for BackFiller service.
    /// </summary>
    /// <remarks>
    /// <para>BackFiller configuration includes network binding settings for accepting incoming TCP connections.</para>
    /// <para><b>BindAddress</b> defines the list of local IPv4 and IPv6 addresses where listeners are created.</para>
    /// <para><b>BindPort</b> defines the TCP port used by all listeners.</para>
    /// </remarks>
    internal sealed class BackFillerOptions
    {
        /// <summary>
        /// Gets or sets the list of IP addresses on which the service accepts incoming TCP connections.
        /// </summary>
        /// <remarks>
        /// <para>When omitted, BackFiller listens on all interfaces (IPv4 and IPv6 wildcard binding).</para>
        /// <para>Each configured address must be:</para>
        /// <list type="bullet">
        /// <item><description>Syntactically valid IPv4 or IPv6 address</description></item>
        /// <item><description>Assigned to a local network interface, unless it is a wildcard address (0.0.0.0 or ::)</description></item>
        /// <item><description>Available for binding by the application</description></item>
        /// <item><description>Compatible with the configured BindPort</description></item>
        /// </list>
        /// <para>IPv4 and IPv6 listeners are treated as independent endpoints.</para>
        /// <para>Example: ["198.18.0.66", "2c0f:f030:1280:101:198:18:0:66"]</para>
        /// </remarks>
        public string[]? BindAddress { get; set; }

        /// <summary>
        /// Gets or sets the TCP port on which the service accepts incoming connections.
        /// </summary>
        /// <remarks>
        /// <para>Must be a valid port number (1-65535).</para>
        /// <para>Ports 1-1023 are privileged and may require elevated permissions.</para>
        /// <para>The same port is used for all configured bind addresses.</para>
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:BindPort is required")]
        [Range(1, 65535, ErrorMessage = "BackFiller:BindPort must be between 1 and 65535")]
        public int? BindPort { get; set; }

        /// <summary>
        /// Gets or sets the BackFiller instance name used to construct the TLS/ACME hostname.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:Name is required")]
        [MinLength(1, ErrorMessage = "BackFiller:Name cannot be empty")]
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the BackFiller numeric instance identifier used to construct the TLS/ACME hostname.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:Id is required")]
        [Range(0, 99, ErrorMessage = "BackFiller:Id must be between 0 and 99")]
        public int? Id { get; set; }

        /// <summary>
        /// Gets or sets the DNS domain suffix used for BackFiller FQDN construction.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:DnsSuffix is required")]
        [MinLength(1, ErrorMessage = "BackFiller:DnsSuffix cannot be empty")]
        public string DnsSuffix { get; set; } = "usenet.ninja";

        /// <summary>
        /// Gets or sets the certificate directory used for ACME and TLS certificate artifacts.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:DirCerts is required")]
        [MinLength(1, ErrorMessage = "BackFiller:DirCerts cannot be empty")]
        public string? DirCerts { get; set; }

        /// <summary>
        /// Gets or sets Let's Encrypt configuration used for ACME account and certificate operations.
        /// </summary>
        /// <value>Validated ACME and certificate-management configuration for this BackFiller instance.</value>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt is required")]
        public LetsEncryptOptions LetsEncrypt { get; set; } = new();

        /// <summary>
        /// Gets or sets RabbitMQ configuration used for channel lifecycle and control-plane messaging safeguards.
        /// </summary>
        /// <value>Validated RabbitMQ runtime configuration consumed by messaging and health-management components.</value>
        [Required(ErrorMessage = "BackFiller:RabbitMQ is required")]
        public RabbitMqOptions RabbitMQ { get; set; } = new();

        /// <summary>
        /// Gets or sets TransitServer connection settings used for downstream NNTP article streaming.
        /// </summary>
        /// <value>Validated downstream TransitServer endpoint and transport configuration.</value>
        [Required(ErrorMessage = "BackFiller:TransitServer is required")]
        public TransitServerOptions TransitServer { get; set; } = new();

        /// <summary>
        /// Gets or sets graceful shutdown behavior used when stopping the BackFiller service.
        /// </summary>
        /// <value>Validated shutdown policy controlling grace-period timing and queued/active work handling.</value>
        [Required(ErrorMessage = "BackFiller:Shutdown is required")]
        public ShutdownOptions Shutdown { get; set; } = new();
    }

    /// <summary>
    /// Configuration options for BackFiller TLS/ACME and operational Cloudflare DNS workflows.
    /// </summary>
    /// <remarks>
    /// Cloudflare credentials remain mandatory for BackFiller DNS/FQDN operational workflows,
    /// even when <see cref="Enabled"/> is <see langword="false"/>.
    /// </remarks>
    internal sealed class LetsEncryptOptions
    {
        /// <summary>
        /// Gets or sets a value indicating whether TLS/ACME certificate issuance is enabled for BackFiller listener operations.
        /// </summary>
        /// <remarks>
        /// When disabled, ACME and certificate-renewal settings are not required, but Cloudflare DNS settings remain required.
        /// </remarks>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Gets or sets the ACME account contact email address.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:AcmeAccountEmail is required")]
        [MinLength(1, ErrorMessage = "BackFiller:LetsEncrypt:AcmeAccountEmail cannot be empty")]
        [EmailAddress(ErrorMessage = "BackFiller:LetsEncrypt:AcmeAccountEmail must be a valid email address")]
        public string AcmeAccountEmail { get; set; } = "security@usenet.ninja";

        /// <summary>
        /// Gets or sets the filename of the PEM-encoded ACME account private key.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:AcmeAccountKeyPem is required")]
        [MinLength(1, ErrorMessage = "BackFiller:LetsEncrypt:AcmeAccountKeyPem cannot be empty")]
        public string AcmeAccountKeyPem { get; set; } = "account.key";

        /// <summary>
        /// Gets or sets the maximum number of retry attempts for transient ACME operation failures.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts is required")]
        [Range(1, 10, ErrorMessage = "BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts must be between 1 and 10")]
        public int? AcmeTransientRetryMaxAttempts { get; set; } = 5;

        /// <summary>
        /// Gets or sets the TTL, in minutes, for reusing a successful clock-skew validation result.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes is required")]
        [Range(1, 60, ErrorMessage = "BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes must be between 1 and 60")]
        public int? ClockSkewCheckTtlMinutes { get; set; } = 5;

        /// <summary>
        /// Gets or sets the maximum permitted clock-skew, in minutes, for ACME operations.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:ClockSkewMaxMinutes is required")]
        [Range(1, 60, ErrorMessage = "BackFiller:LetsEncrypt:ClockSkewMaxMinutes must be between 1 and 60")]
        public int? ClockSkewMaxMinutes { get; set; } = 10;

        /// <summary>
        /// Gets or sets the authoritative DNS nameserver cache TTL, in minutes, for ACME DNS validation.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes is required")]
        [Range(1, 60, ErrorMessage = "BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes must be between 1 and 60")]
        public int? DnsAuthoritativeNsCacheMinutes { get; set; } = 5;

        /// <summary>
        /// Gets or sets the required authoritative DNS TXT quorum ratio for ACME DNS-01 propagation checks.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio is required")]
        [Range(typeof(double), "0.0000000001", "1", ErrorMessage = "BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio must be greater than 0 and less than or equal to 1")]
        public double? DnsAuthoritativeQuorumRatio { get; set; } = 0.7;

        /// <summary>
        /// Gets or sets the initial DNS propagation delay, in seconds, before authoritative DNS polling begins.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:DnsPropagationDelaySeconds is required")]
        [Range(0, 600, ErrorMessage = "BackFiller:LetsEncrypt:DnsPropagationDelaySeconds must be between 0 and 600")]
        public int? DnsPropagationDelaySeconds { get; set; } = 15;

        /// <summary>
        /// Gets or sets the interval, in seconds, between authoritative DNS TXT polling cycles.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds is required")]
        [Range(1, 60, ErrorMessage = "BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds must be between 1 and 60")]
        public int? DnsTxtPollIntervalSeconds { get; set; } = 3;

        /// <summary>
        /// Gets or sets the maximum timeout, in seconds, for authoritative DNS TXT propagation polling.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds is required")]
        [Range(1, 3600, ErrorMessage = "BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds must be between 1 and 3600")]
        public int? DnsTxtPollTimeoutSeconds { get; set; } = 600;

        /// <summary>
        /// Gets or sets the optional shared certificate SAN domain-name list.
        /// </summary>
        /// <remarks>
        /// For VectorNNTP.BackFiller, this setting is validated only for syntax and is ignored
        /// as a certificate-identity source. The generated BackFiller FQDN remains authoritative.
        /// </remarks>
        public string[]? DomainNames { get; set; }

        /// <summary>
        /// Gets or sets the password used to protect exported PFX/PKCS#12 certificate bundles.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:PfxExportPassword is required")]
        [MinLength(1, ErrorMessage = "BackFiller:LetsEncrypt:PfxExportPassword cannot be empty")]
        public string PfxExportPassword { get; set; } = "YOUR_PFX_PASSWORD";

        /// <summary>
        /// Gets or sets the interval, in hours, between certificate renewal state evaluations.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:RenewalCheckIntervalHours is required")]
        [Range(1, 168, ErrorMessage = "BackFiller:LetsEncrypt:RenewalCheckIntervalHours must be between 1 and 168")]
        public int? RenewalCheckIntervalHours { get; set; } = 6;

        /// <summary>
        /// Gets or sets the renewal scheduling jitter ratio applied to periodic certificate checks.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:RenewalJitterRatio is required")]
        [Range(typeof(double), "0", "0.9999999999", ErrorMessage = "BackFiller:LetsEncrypt:RenewalJitterRatio must be between 0 (inclusive) and 1 (exclusive)")]
        public double? RenewalJitterRatio { get; set; } = 0.1;

        /// <summary>
        /// Gets or sets the certificate renewal eligibility threshold, in days before expiration.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:RenewBeforeExpiryDays is required")]
        [Range(1, 60, ErrorMessage = "BackFiller:LetsEncrypt:RenewBeforeExpiryDays must be between 1 and 60")]
        public int? RenewBeforeExpiryDays { get; set; } = 7;

        /// <summary>
        /// Gets or sets a value indicating whether ACME operations should use the Let's Encrypt staging directory.
        /// </summary>
        public bool UseStagingDirectory { get; set; }

        /// <summary>
        /// Gets or sets the Cloudflare API token used for DNS management.
        /// </summary>
        /// <remarks>
        /// Required for BackFiller DNS/FQDN operational workflows regardless of <see cref="Enabled"/>.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:CloudFlareApiToken is required")]
        [MinLength(1, ErrorMessage = "BackFiller:LetsEncrypt:CloudFlareApiToken cannot be empty")]
        public string CloudFlareApiToken { get; set; } = "YOUR_CLOUDFLARE_API_TOKEN";

        /// <summary>
        /// Gets or sets the Cloudflare Zone ID used for DNS operations.
        /// </summary>
        /// <remarks>
        /// Required for BackFiller DNS/FQDN operational workflows regardless of <see cref="Enabled"/>.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:LetsEncrypt:CloudFlareZoneId is required")]
        [MinLength(1, ErrorMessage = "BackFiller:LetsEncrypt:CloudFlareZoneId cannot be empty")]
        public string CloudFlareZoneId { get; set; } = "5811a29d39a0732afb5f160c9b137c3d";
    }

    /// <summary>
    /// Configuration options for RabbitMQ channel leasing and operation timeout safeguards.
    /// </summary>
    internal sealed class RabbitMqOptions
    {
        /// <summary>
        /// Maximum RabbitMQ work-request envelope size, in bytes, admitted before the consumer copies the borrowed broker body.
        /// </summary>
        /// <remarks>
        /// This bounds the control-plane JSON request envelope only; it does not apply to article bodies or the transit queue budget.
        /// The default leaves headroom over the current canonical request shape while remaining in the KiB range to limit broker-controlled memory amplification.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes is required")]
        [Range(1, 4096, ErrorMessage = "BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes must be between 1 and 4096")]
        public int? WorkRequestMaxPayloadBytes { get; set; } = 1024;

        /// <summary>
        /// Gets or sets the maximum allowed duration, in seconds, for RabbitMQ operation-timeout coherence validation.
        /// </summary>
        /// <remarks>
        /// Current runtime does not enforce channel lease revocation; this value is validated and projected for lifecycle policy coherence.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds is required")]
        [Range(1, 3600, ErrorMessage = "BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds must be between 1 and 3600")]
        public int? ChannelLeaseTimeoutSeconds { get; set; } = 60;

        /// <summary>
        /// Gets or sets the RabbitMQ RPC operation timeout, in seconds, used for lease/operation coherence checks.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:RpcTimeoutSeconds is required")]
        [Range(1, 3600, ErrorMessage = "BackFiller:RabbitMQ:RpcTimeoutSeconds must be between 1 and 3600")]
        public int? RpcTimeoutSeconds { get; set; } = 30;

        /// <summary>
        /// Gets or sets the maximum duration, in seconds, that a broker-blocked RabbitMQ connection may remain blocked before recovery evaluation.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds is required")]
        [Range(5, 3600, ErrorMessage = "BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds must be between 5 and 3600")]
        public int? ConnectionBlockedTimeoutSeconds { get; set; } = 30;

        /// <summary>
        /// Gets or sets the collection of RabbitMQ broker host endpoints used for connection establishment.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:Hosts is required")]
        public string[]? Hosts { get; set; } = [];

        /// <summary>
        /// Gets or sets the RabbitMQ username used for credential-based authentication.
        /// </summary>
        public string? Username { get; set; }

        /// <summary>
        /// Gets or sets the RabbitMQ password used for credential-based authentication.
        /// </summary>
        public string? Password { get; set; }

        /// <summary>
        /// Gets or sets the RabbitMQ virtual host used for RabbitMQ namespace isolation.
        /// </summary>
        public string? VirtualHost { get; set; } = "/";

        /// <summary>
        /// Gets or sets a value indicating whether RabbitMQ connections use TLS/SSL encryption.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:EnableSsl is required")]
        public bool? EnableSsl { get; set; } = true;

        /// <summary>
        /// Gets or sets the RabbitMQ AMQP TCP port used when connecting to configured hosts.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:Port is required")]
        [Range(1, 65535, ErrorMessage = "BackFiller:RabbitMQ:Port must be between 1 and 65535")]
        public int? Port { get; set; } = 5672;

        /// <summary>
        /// Gets or sets the bounded in-memory delivery buffer capacity used by RabbitMQ consumer infrastructure.
        /// </summary>
        /// <remarks>
        /// Current runtime uses this value for delivery-channel buffering, not RabbitMQ channel-object pooling.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:ChannelPoolSize is required")]
        [Range(1, 8192, ErrorMessage = "BackFiller:RabbitMQ:ChannelPoolSize must be between 1 and 8192")]
        public int? ChannelPoolSize { get; set; } = 512;

        /// <summary>
        /// Gets or sets the configured minimum RabbitMQ connection count target for future pool-scaling policy.
        /// </summary>
        /// <remarks>
        /// Current runtime uses a single active RabbitMQ connection manager connection; this value is validated/projected but not enforced.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:MinConnections is required")]
        [Range(1, 512, ErrorMessage = "BackFiller:RabbitMQ:MinConnections must be between 1 and 512")]
        public int? MinConnections { get; set; } = 4;

        /// <summary>
        /// Gets or sets the configured maximum RabbitMQ connection count limit for future pool-scaling policy.
        /// </summary>
        /// <remarks>
        /// Current runtime uses a single active RabbitMQ connection manager connection; this value is validated/projected but not enforced.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:MaxConnections is required")]
        [Range(1, 512, ErrorMessage = "BackFiller:RabbitMQ:MaxConnections must be between 1 and 512")]
        public int? MaxConnections { get; set; } = 16;

        /// <summary>
        /// Gets or sets the maximum number of consecutive failed recovery attempts permitted for an individual RabbitMQ connection recovery context.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures is required")]
        [Range(1, 100, ErrorMessage = "BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures must be between 1 and 100")]
        public int? MaxConsecutiveRecoveryFailures { get; set; } = 5;

        /// <summary>
        /// Gets or sets the maximum pending channel-lease waiter target for future RabbitMQ channel-pool policy.
        /// </summary>
        /// <remarks>
        /// Current runtime creates owned channels directly without a shared lease-waiter queue; this value is validated/projected but not enforced.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:MaxPendingLeaseWaiters is required")]
        [Range(0, 65536, ErrorMessage = "BackFiller:RabbitMQ:MaxPendingLeaseWaiters must be between 0 and 65536")]
        public int? MaxPendingLeaseWaiters { get; set; } = 1024;

        /// <summary>
        /// Gets or sets the minimum idle duration, in seconds, for future RabbitMQ connection scale-down policy.
        /// </summary>
        /// <remarks>
        /// Current runtime does not implement multi-connection scale-down logic; this value is validated/projected for planned policy.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds is required")]
        [Range(30, 86400, ErrorMessage = "BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds must be between 30 and 86400")]
        public int? ConnectionScaleDownIdleSeconds { get; set; } = 300;

        /// <summary>
        /// Gets or sets the cooldown, in seconds, for future RabbitMQ connection scale-down policy.
        /// </summary>
        /// <remarks>
        /// Current runtime does not implement connection pool scale-down operations; this value is validated/projected for planned policy.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:ScaleDownCooldownSeconds is required")]
        [Range(0, 3600, ErrorMessage = "BackFiller:RabbitMQ:ScaleDownCooldownSeconds must be between 0 and 3600")]
        public int? ScaleDownCooldownSeconds { get; set; } = 30;

        /// <summary>
        /// Gets or sets the minimum base interval, in seconds, between automatic RabbitMQ network recovery attempts.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds is required")]
        [Range(1, 3600, ErrorMessage = "BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds must be between 1 and 3600")]
        public int? NetworkRecoveryIntervalSeconds { get; set; } = 5;

        /// <summary>
        /// Gets or sets the base delay, in milliseconds, used before pool-level RabbitMQ reconnect attempts.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:PoolReconnectBaseDelayMs is required")]
        [Range(50, 60000, ErrorMessage = "BackFiller:RabbitMQ:PoolReconnectBaseDelayMs must be between 50 and 60000")]
        public int? PoolReconnectBaseDelayMs { get; set; } = 250;

        /// <summary>
        /// Gets or sets the maximum delay, in milliseconds, allowed for pool-level RabbitMQ reconnect backoff.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:PoolReconnectMaxDelayMs is required")]
        [Range(50, 300000, ErrorMessage = "BackFiller:RabbitMQ:PoolReconnectMaxDelayMs must be between 50 and 300000")]
        public int? PoolReconnectMaxDelayMs { get; set; } = 30000;

        /// <summary>
        /// Gets or sets the minimum healthy RabbitMQ connection lifetime policy, in seconds, for future idle-retirement logic.
        /// </summary>
        /// <remarks>
        /// Current runtime does not enforce connection lifetime retirement; this value is validated/projected for planned policy.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds is required")]
        [Range(30, 86400, ErrorMessage = "BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds must be between 30 and 86400")]
        public int? MinimumConnectionLifetimeSeconds { get; set; } = 300;

        /// <summary>
        /// Gets or sets the maximum wait time, in seconds, for RabbitMQ publisher confirmations.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds is required")]
        [Range(1, 3600, ErrorMessage = "BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds must be between 1 and 3600")]
        public int? PublishConfirmTimeoutSeconds { get; set; } = 10;

        /// <summary>
        /// Gets or sets the maximum RabbitMQ shutdown-drain budget, in seconds, used for shutdown-policy validation and runtime projection.
        /// </summary>
        /// <remarks>
        /// Current RabbitMQ shutdown/disposal flow is cancellation-driven; this value is not used as an active internal timer in RabbitMQ components.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds is required")]
        [Range(1, 3600, ErrorMessage = "BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds must be between 1 and 3600")]
        public int? MaximumShutdownDrainTimeoutSeconds { get; set; } = 30;

        /// <summary>
        /// Gets or sets the degraded-capacity threshold policy for future RabbitMQ health evaluation logic.
        /// </summary>
        /// <remarks>
        /// Current runtime does not compute capacity-ratio degradation from this value; it remains a validated/projected policy input.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:DegradedThreshold is required")]
        [Range(0.000001d, 1d, ErrorMessage = "BackFiller:RabbitMQ:DegradedThreshold must be greater than 0 and less than or equal to 1")]
        public double? DegradedThreshold { get; set; } = 0.75;

        /// <summary>
        /// Gets or sets the consecutive-unhealthy threshold policy for future RabbitMQ health evaluation logic.
        /// </summary>
        /// <remarks>
        /// Current runtime does not maintain an unhealthy-evaluation counter from this value; it remains a validated/projected policy input.
        /// </remarks>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:UnhealthyThreshold is required")]
        [Range(1, 120, ErrorMessage = "BackFiller:RabbitMQ:UnhealthyThreshold must be between 1 and 120")]
        public int? UnhealthyThreshold { get; set; } = 5;

        /// <summary>
        /// Gets or sets the requested RabbitMQ heartbeat timeout, in seconds, for AMQP connection negotiation.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:RequestedHeartbeatSeconds is required")]
        [Range(0, 3600, ErrorMessage = "BackFiller:RabbitMQ:RequestedHeartbeatSeconds must be between 0 and 3600")]
        public int? RequestedHeartbeatSeconds { get; set; } = 60;

        /// <summary>
        /// Gets or sets the RabbitMQ socket operation timeout, in seconds, for low-level network I/O.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:SocketTimeoutSeconds is required")]
        [Range(5, 600, ErrorMessage = "BackFiller:RabbitMQ:SocketTimeoutSeconds must be between 5 and 600")]
        public int? SocketTimeoutSeconds { get; set; } = 30;

        /// <summary>
        /// Gets or sets the requested RabbitMQ channel limit per connection.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:RabbitMQ:RequestedChannelMax is required")]
        [Range(1, 65535, ErrorMessage = "BackFiller:RabbitMQ:RequestedChannelMax must be between 1 and 65535")]
        public int? RequestedChannelMax { get; set; } = 2047;

        /// <summary>
        /// Gets or sets the optional RabbitMQ consumer prefetch count used for Basic.Qos prefetch control.
        /// </summary>
        [Range(1, 65535, ErrorMessage = "BackFiller:RabbitMQ:ConsumerPrefetchCount must be between 1 and 65535")]
        public ushort? ConsumerPrefetchCount { get; set; }

        /// <summary>
        /// Gets or sets an optional AMQP CorrelationId used to gate temporary payload diagnostics.
        /// </summary>
        public string? DiagnosticPayloadCorrelationId { get; set; }
    }

    /// <summary>
    /// Configuration options for downstream TransitServer NNTP article streaming.
    /// </summary>
    internal sealed class TransitServerOptions
    {
        /// <summary>
        /// Gets or sets the hostname or IP address of the downstream NNTP TransitServer.
        /// </summary>
        [Required(ErrorMessage = "BackFiller:TransitServer:Host is required")]
        [MinLength(1, ErrorMessage = "BackFiller:TransitServer:Host cannot be empty")]
        public string Host { get; set; } = "localhost";

        /// <summary>
        /// Gets or sets the NNTP port used when connecting to the configured TransitServer host.
        /// </summary>
        [Range(1, 65535, ErrorMessage = "BackFiller:TransitServer:Port must be between 1 and 65535")]
        public int Port { get; set; } = 119;

        /// <summary>
        /// Gets or sets a value indicating whether TLS is used for TransitServer connections.
        /// </summary>
        public bool UseSsl { get; set; }
    }

    /// <summary>
    /// Graceful shutdown policy configuration.
    /// </summary>
    /// <remarks>
    /// <para>Configured via appsettings.json under the <c>BackFiller:Shutdown</c> section.</para>
    /// <para><see cref="GracePeriodSeconds"/> is the complete application shutdown budget and is used for both
    /// worker drain/cancellation behavior and Generic Host <see cref="HostOptions.ShutdownTimeout"/>.</para>
    /// <para>Shutdown sequence:</para>
    /// <list type="number">
    /// <item><description>Stop admitting new work (close listening sockets).</description></item>
    /// <item><description>Signal shutdown to running workers (if <see cref="FinishActiveArticles"/> is true, allow active work to complete).</description></item>
    /// <item><description>If <see cref="DrainQueuedWork"/> is true, continue processing already-admitted queue items only.</description></item>
    /// <item><description>Wait up to <see cref="GracePeriodSeconds"/> for graceful shutdown to complete.</description></item>
    /// <item><description>If <see cref="GracePeriodSeconds"/> expires, cancel remaining queued/active work and force shutdown.</description></item>
    /// <item><description>Close provider connections, flush telemetry, and exit cleanly.</description></item>
    /// </list>
    /// </remarks>
    internal sealed class ShutdownOptions : IValidatableObject
    {
        /// <summary>
        /// Lower bound, in seconds, accepted for the configured graceful-shutdown budget.
        /// </summary>
        internal const int MinimumGracePeriodSeconds = 5;
        /// <summary>
        /// Upper bound, in seconds, accepted for the configured graceful-shutdown budget.
        /// </summary>
        internal const int MaximumGracePeriodSeconds = 600;

        /// <summary>
        /// Gets or sets the grace period, in seconds, to allow for graceful shutdown.
        /// </summary>
        public int GracePeriodSeconds { get; set; } = 30;

        /// <summary>
        /// Gets or sets whether to continue processing already-admitted queued work during shutdown.
        /// </summary>
        public bool DrainQueuedWork { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to allow active work to finish during shutdown.
        /// </summary>
        public bool FinishActiveArticles { get; set; } = true;

        /// <summary>
        /// Validates cross-property shutdown constraints.
        /// </summary>
        /// <param name="validationContext">Validation context for this options instance.</param>
        /// <returns>Validation errors when constraints are violated.</returns>
        public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
        {
            ArgumentNullException.ThrowIfNull(validationContext);

            if (GracePeriodSeconds is < MinimumGracePeriodSeconds or > MaximumGracePeriodSeconds)
            {
                yield return new ValidationResult(
                    $"GracePeriodSeconds must be between {MinimumGracePeriodSeconds} and {MaximumGracePeriodSeconds} seconds.",
                    [nameof(GracePeriodSeconds)]);
            }

            if (DrainQueuedWork && !FinishActiveArticles)
            {
                yield return new ValidationResult(
                    "DrainQueuedWork requires FinishActiveArticles to be true.",
                    [nameof(DrainQueuedWork), nameof(FinishActiveArticles)]);
            }
        }
    }

    /// <summary>
    /// One validation diagnostic emitted while checking TransitServer endpoint configuration.
    /// </summary>
    /// <param name="Setting">Configuration key associated with the reported TransitServer problem.</param>
    /// <param name="Message">Human-readable validation message describing the detected condition.</param>
    /// <param name="Severity">Severity that determines whether startup may continue.</param>
    internal record TransitServerValidationResult(
        string Setting,
        string Message,
        ValidationSeverity Severity);

    /// <summary>
    /// Validates TransitServer connection configuration inputs.
    /// </summary>
    internal static class TransitServerValidator
    {
        /// <summary>
        /// Validates TransitServer host configuration constraints.
        /// </summary>
        /// <param name="transitServer">TransitServer options snapshot.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for TransitServer settings.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<TransitServerValidationResult> Validate(
            TransitServerOptions? transitServer,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            List<TransitServerValidationResult> diagnostics = [];

            if (transitServer == null)
            {
                diagnostics.Add(new TransitServerValidationResult(
                    $"{settingPrefix}:TransitServer",
                    "TransitServer section is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string hostSetting = $"{settingPrefix}:TransitServer:Host";
            if (string.IsNullOrWhiteSpace(transitServer.Host))
            {
                diagnostics.Add(new TransitServerValidationResult(
                    hostSetting,
                    "Host must not be empty or whitespace",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string trimmedHost = transitServer.Host.Trim();

            if (trimmedHost.Contains("://", StringComparison.Ordinal))
            {
                diagnostics.Add(new TransitServerValidationResult(
                    hostSetting,
                    "Host must not include a URI scheme",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (trimmedHost.Contains('@'))
            {
                diagnostics.Add(new TransitServerValidationResult(
                    hostSetting,
                    "Host must not include credentials",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (trimmedHost.Contains('/'))
            {
                diagnostics.Add(new TransitServerValidationResult(
                    hostSetting,
                    "Host must not include path syntax",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (trimmedHost.Contains('?'))
            {
                diagnostics.Add(new TransitServerValidationResult(
                    hostSetting,
                    "Host must not include query parameters",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            bool isIpAddress = IPAddress.TryParse(trimmedHost, out _);
            if (!isIpAddress)
            {
                if (trimmedHost.Contains(':', StringComparison.Ordinal))
                {
                    diagnostics.Add(new TransitServerValidationResult(
                        hostSetting,
                        "Host must not include a port value",
                        ValidationSeverity.Error));
                    return diagnostics;
                }

                if (Uri.CheckHostName(trimmedHost) != UriHostNameType.Dns)
                {
                    diagnostics.Add(new TransitServerValidationResult(
                        hostSetting,
                        "Host must be a valid hostname or IP address",
                        ValidationSeverity.Error));
                }
            }

            string portSetting = $"{settingPrefix}:TransitServer:Port";
            if (transitServer.Port <= 0)
            {
                diagnostics.Add(new TransitServerValidationResult(
                    portSetting,
                    "Port must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (transitServer.Port > 65535)
            {
                diagnostics.Add(new TransitServerValidationResult(
                    portSetting,
                    "Port must be between 1 and 65535",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (transitServer.UseSsl && transitServer.Port == 119)
            {
                diagnostics.Add(new TransitServerValidationResult(
                    portSetting,
                    "Port 119 is conventionally non-TLS while UseSsl is true",
                    ValidationSeverity.Warning));
            }
            else if (!transitServer.UseSsl && transitServer.Port == 563)
            {
                diagnostics.Add(new TransitServerValidationResult(
                    portSetting,
                    "Port 563 is conventionally TLS while UseSsl is false",
                    ValidationSeverity.Warning));
            }

            return diagnostics;
        }
    }

    /// <summary>
    /// One validation diagnostic emitted while checking RabbitMQ connectivity and policy settings.
    /// </summary>
    /// <param name="Setting">Configuration key associated with the reported RabbitMQ problem.</param>
    /// <param name="Message">Human-readable validation message describing the detected condition.</param>
    /// <param name="Severity">Severity that determines whether startup may continue.</param>
    internal record RabbitMqValidationResult(
        string Setting,
        string Message,
        ValidationSeverity Severity);

    /// <summary>
    /// Validates RabbitMQ lease-timeout configuration inputs.
    /// </summary>
    internal static class RabbitMqValidator
    {
        /// <summary>
        /// Validates channel lease timeout and timeout coherence constraints.
        /// </summary>
        /// <param name="rabbitMq">RabbitMQ options snapshot.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for RabbitMQ lease settings.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<RabbitMqValidationResult> Validate(
            RabbitMqOptions? rabbitMq,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            List<RabbitMqValidationResult> diagnostics = [];

            if (rabbitMq == null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    $"{settingPrefix}:RabbitMQ",
                    "RabbitMQ section is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string leaseSetting = $"{settingPrefix}:RabbitMQ:ChannelLeaseTimeoutSeconds";
            if (rabbitMq.ChannelLeaseTimeoutSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    leaseSetting,
                    "ChannelLeaseTimeoutSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ChannelLeaseTimeoutSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    leaseSetting,
                    "ChannelLeaseTimeoutSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ChannelLeaseTimeoutSeconds > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    leaseSetting,
                    "ChannelLeaseTimeoutSeconds must be between 1 and 3600",
                    ValidationSeverity.Error));
            }

            string rpcTimeoutSetting = $"{settingPrefix}:RabbitMQ:RpcTimeoutSeconds";
            if (rabbitMq.RpcTimeoutSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    rpcTimeoutSetting,
                    "RpcTimeoutSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.RpcTimeoutSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    rpcTimeoutSetting,
                    "RpcTimeoutSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.RpcTimeoutSeconds > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    rpcTimeoutSetting,
                    "RpcTimeoutSeconds must be between 1 and 3600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.ChannelLeaseTimeoutSeconds < rabbitMq.RpcTimeoutSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    leaseSetting,
                    "ChannelLeaseTimeoutSeconds must be greater than or equal to RpcTimeoutSeconds",
                    ValidationSeverity.Error));
            }

            string blockedSetting = $"{settingPrefix}:RabbitMQ:ConnectionBlockedTimeoutSeconds";
            if (rabbitMq.ConnectionBlockedTimeoutSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    blockedSetting,
                    "ConnectionBlockedTimeoutSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ConnectionBlockedTimeoutSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    blockedSetting,
                    "ConnectionBlockedTimeoutSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ConnectionBlockedTimeoutSeconds is < 5 or > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    blockedSetting,
                    "ConnectionBlockedTimeoutSeconds must be between 5 and 3600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.ConnectionBlockedTimeoutSeconds < rabbitMq.RpcTimeoutSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    blockedSetting,
                    "ConnectionBlockedTimeoutSeconds must be greater than or equal to RpcTimeoutSeconds",
                    ValidationSeverity.Error));
            }

            string hostsSetting = $"{settingPrefix}:RabbitMQ:Hosts";
            if (rabbitMq.Hosts is null || rabbitMq.Hosts.Length == 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    hostsSetting,
                    "Hosts must contain at least one entry",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            HashSet<string> normalizedHosts = new(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < rabbitMq.Hosts.Length; i++)
            {
                string? host = rabbitMq.Hosts[i];
                string itemSetting = $"{hostsSetting}:{i}";

                if (string.IsNullOrWhiteSpace(host))
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        itemSetting,
                        "Host entry cannot be empty",
                        ValidationSeverity.Error));
                    continue;
                }

                string trimmedHost = host.Trim();

                if (trimmedHost.Contains("://", StringComparison.Ordinal))
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        itemSetting,
                        "Host entry must not include a URI scheme",
                        ValidationSeverity.Error));
                    continue;
                }

                if (trimmedHost.Contains('@'))
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        itemSetting,
                        "Host entry must not include credentials",
                        ValidationSeverity.Error));
                    continue;
                }

                if (trimmedHost.Contains('/'))
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        itemSetting,
                        "Host entry must not include path or virtual host syntax",
                        ValidationSeverity.Error));
                    continue;
                }

                if (trimmedHost.Contains('?'))
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        itemSetting,
                        "Host entry must not include query parameters",
                        ValidationSeverity.Error));
                    continue;
                }

                bool isIpAddress = IPAddress.TryParse(trimmedHost, out _);
                if (!isIpAddress && Uri.CheckHostName(trimmedHost) != UriHostNameType.Dns)
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        itemSetting,
                        "Host entry must be a valid hostname or IP address",
                        ValidationSeverity.Error));
                    continue;
                }

                if (!normalizedHosts.Add(trimmedHost))
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        itemSetting,
                        "Duplicate host entries are not allowed",
                        ValidationSeverity.Error));
                }
            }

            string usernameSetting = $"{settingPrefix}:RabbitMQ:Username";
            string passwordSetting = $"{settingPrefix}:RabbitMQ:Password";
            bool hasUsername = !string.IsNullOrWhiteSpace(rabbitMq.Username);
            bool hasPassword = !string.IsNullOrWhiteSpace(rabbitMq.Password);

            if (rabbitMq.Username is not null && string.IsNullOrWhiteSpace(rabbitMq.Username))
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    usernameSetting,
                    "Username must not be empty or whitespace when configured",
                    ValidationSeverity.Error));
            }

            if (string.Equals(rabbitMq.Username, "guest", StringComparison.OrdinalIgnoreCase))
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    usernameSetting,
                    "Username 'guest' is intended for development; use a dedicated service account for production",
                    ValidationSeverity.Warning));
            }

            if (hasUsername && string.IsNullOrWhiteSpace(rabbitMq.Password))
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    passwordSetting,
                    "Password is required when Username is configured",
                    ValidationSeverity.Error));
            }

            if (!hasUsername && rabbitMq.Password != null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    usernameSetting,
                    "Username is required when Password is configured",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.Password is not null && string.IsNullOrWhiteSpace(rabbitMq.Password))
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    passwordSetting,
                    "Password must not be empty or whitespace when password authentication is configured",
                    ValidationSeverity.Error));
            }

            string virtualHostSetting = $"{settingPrefix}:RabbitMQ:VirtualHost";
            if (rabbitMq.VirtualHost is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    virtualHostSetting,
                    "VirtualHost is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (string.IsNullOrWhiteSpace(rabbitMq.VirtualHost))
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    virtualHostSetting,
                    "VirtualHost must not be empty or whitespace",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.VirtualHost.Contains('\0'))
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    virtualHostSetting,
                    "VirtualHost contains invalid null character",
                    ValidationSeverity.Error));
            }

            string enableSslSetting = $"{settingPrefix}:RabbitMQ:EnableSsl";
            if (rabbitMq.EnableSsl is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    enableSslSetting,
                    "EnableSsl is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (hasPassword && rabbitMq.EnableSsl == false)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    enableSslSetting,
                    "TLS is disabled while password authentication is configured; enable TLS for production deployments",
                    ValidationSeverity.Warning));
            }

            string portSetting = $"{settingPrefix}:RabbitMQ:Port";
            if (rabbitMq.Port is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    portSetting,
                    "Port is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.Port <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    portSetting,
                    "Port must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.Port > 65535)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    portSetting,
                    "Port must be between 1 and 65535",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.EnableSsl == true && rabbitMq.Port == 5672)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    portSetting,
                    "Port 5672 is conventionally non-TLS while EnableSsl is true",
                    ValidationSeverity.Warning));
            }
            else if (rabbitMq.EnableSsl == false && rabbitMq.Port == 5671)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    portSetting,
                    "Port 5671 is conventionally TLS while EnableSsl is false",
                    ValidationSeverity.Warning));
            }

            string scaleDownSetting = $"{settingPrefix}:RabbitMQ:ConnectionScaleDownIdleSeconds";
            if (rabbitMq.ConnectionScaleDownIdleSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownSetting,
                    "ConnectionScaleDownIdleSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ConnectionScaleDownIdleSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownSetting,
                    "ConnectionScaleDownIdleSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ConnectionScaleDownIdleSeconds is < 30 or > 86400)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownSetting,
                    "ConnectionScaleDownIdleSeconds must be between 30 and 86400",
                    ValidationSeverity.Error));
            }

            string scaleDownCooldownSetting = $"{settingPrefix}:RabbitMQ:ScaleDownCooldownSeconds";
            if (rabbitMq.ScaleDownCooldownSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownCooldownSetting,
                    "ScaleDownCooldownSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ScaleDownCooldownSeconds < 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownCooldownSetting,
                    "ScaleDownCooldownSeconds must be greater than or equal to zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ScaleDownCooldownSeconds > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownCooldownSetting,
                    "ScaleDownCooldownSeconds must be between 0 and 3600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.ScaleDownCooldownSeconds is > 0 and < 5)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownCooldownSetting,
                    "ScaleDownCooldownSeconds is very low and may cause connection churn",
                    ValidationSeverity.Warning));
            }

            if (rabbitMq.ScaleDownCooldownSeconds > rabbitMq.ConnectionScaleDownIdleSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    scaleDownCooldownSetting,
                    "ScaleDownCooldownSeconds exceeds ConnectionScaleDownIdleSeconds and may delay normal scale-down responsiveness",
                    ValidationSeverity.Warning));
            }

            string minimumLifetimeSetting = $"{settingPrefix}:RabbitMQ:MinimumConnectionLifetimeSeconds";
            if (rabbitMq.MinimumConnectionLifetimeSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minimumLifetimeSetting,
                    "MinimumConnectionLifetimeSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MinimumConnectionLifetimeSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minimumLifetimeSetting,
                    "MinimumConnectionLifetimeSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MinimumConnectionLifetimeSeconds is < 30 or > 86400)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minimumLifetimeSetting,
                    "MinimumConnectionLifetimeSeconds must be between 30 and 86400",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.ConnectionScaleDownIdleSeconds is > 0 && rabbitMq.MinimumConnectionLifetimeSeconds > rabbitMq.ConnectionScaleDownIdleSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minimumLifetimeSetting,
                    "MinimumConnectionLifetimeSeconds exceeds ConnectionScaleDownIdleSeconds and may prevent expected idle scale-down behavior",
                    ValidationSeverity.Warning));
            }

            string minConnectionsSetting = $"{settingPrefix}:RabbitMQ:MinConnections";
            if (rabbitMq.MinConnections is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minConnectionsSetting,
                    "MinConnections is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MinConnections <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minConnectionsSetting,
                    "MinConnections must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MinConnections > 512)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minConnectionsSetting,
                    "MinConnections must be between 1 and 512",
                    ValidationSeverity.Error));
            }

            string maxConnectionsSetting = $"{settingPrefix}:RabbitMQ:MaxConnections";
            if (rabbitMq.MaxConnections is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    maxConnectionsSetting,
                    "MaxConnections is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaxConnections <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    maxConnectionsSetting,
                    "MaxConnections must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaxConnections > 512)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    maxConnectionsSetting,
                    "MaxConnections must be between 1 and 512",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.MinConnections is > 0 && rabbitMq.MaxConnections is > 0 && rabbitMq.MinConnections > rabbitMq.MaxConnections)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    minConnectionsSetting,
                    "MinConnections must be less than or equal to MaxConnections",
                    ValidationSeverity.Error));
            }

            string networkRecoverySetting = $"{settingPrefix}:RabbitMQ:NetworkRecoveryIntervalSeconds";
            if (rabbitMq.NetworkRecoveryIntervalSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    networkRecoverySetting,
                    "NetworkRecoveryIntervalSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.NetworkRecoveryIntervalSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    networkRecoverySetting,
                    "NetworkRecoveryIntervalSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.NetworkRecoveryIntervalSeconds > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    networkRecoverySetting,
                    "NetworkRecoveryIntervalSeconds must be between 1 and 3600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.ConnectionBlockedTimeoutSeconds is > 0 && rabbitMq.NetworkRecoveryIntervalSeconds > rabbitMq.ConnectionBlockedTimeoutSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    networkRecoverySetting,
                    "NetworkRecoveryIntervalSeconds exceeds ConnectionBlockedTimeoutSeconds and may delay recovery while blocked connections are considered timed out",
                    ValidationSeverity.Warning));
            }

            string poolReconnectBaseDelaySetting = $"{settingPrefix}:RabbitMQ:PoolReconnectBaseDelayMs";
            if (rabbitMq.PoolReconnectBaseDelayMs is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolReconnectBaseDelaySetting,
                    "PoolReconnectBaseDelayMs is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.PoolReconnectBaseDelayMs <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolReconnectBaseDelaySetting,
                    "PoolReconnectBaseDelayMs must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.PoolReconnectBaseDelayMs is < 50 or > 60000)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolReconnectBaseDelaySetting,
                    "PoolReconnectBaseDelayMs must be between 50 and 60000",
                    ValidationSeverity.Error));
            }

            string poolReconnectMaxDelaySetting = $"{settingPrefix}:RabbitMQ:PoolReconnectMaxDelayMs";
            if (rabbitMq.PoolReconnectMaxDelayMs is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolReconnectMaxDelaySetting,
                    "PoolReconnectMaxDelayMs is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.PoolReconnectMaxDelayMs <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolReconnectMaxDelaySetting,
                    "PoolReconnectMaxDelayMs must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.PoolReconnectMaxDelayMs is < 50 or > 300000)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolReconnectMaxDelaySetting,
                    "PoolReconnectMaxDelayMs must be between 50 and 300000",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.PoolReconnectBaseDelayMs is > 0 && rabbitMq.PoolReconnectMaxDelayMs is > 0 && rabbitMq.PoolReconnectMaxDelayMs < rabbitMq.PoolReconnectBaseDelayMs)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolReconnectMaxDelaySetting,
                    "PoolReconnectMaxDelayMs must be greater than or equal to PoolReconnectBaseDelayMs",
                    ValidationSeverity.Error));
            }

            string recoveryFailuresSetting = $"{settingPrefix}:RabbitMQ:MaxConsecutiveRecoveryFailures";
            if (rabbitMq.MaxConsecutiveRecoveryFailures is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    recoveryFailuresSetting,
                    "MaxConsecutiveRecoveryFailures is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaxConsecutiveRecoveryFailures <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    recoveryFailuresSetting,
                    "MaxConsecutiveRecoveryFailures must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaxConsecutiveRecoveryFailures > 100)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    recoveryFailuresSetting,
                    "MaxConsecutiveRecoveryFailures must be between 1 and 100",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.NetworkRecoveryIntervalSeconds is > 0 && rabbitMq.PoolReconnectBaseDelayMs is > 0)
            {
                int networkRecoveryIntervalMs;
                try
                {
                    networkRecoveryIntervalMs = checked(rabbitMq.NetworkRecoveryIntervalSeconds.Value * 1000);
                }
                catch (OverflowException)
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        networkRecoverySetting,
                        "NetworkRecoveryIntervalSeconds produces an invalid recovery interval",
                        ValidationSeverity.Error));
                    return diagnostics;
                }

                if (rabbitMq.PoolReconnectBaseDelayMs < 50)
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        poolReconnectBaseDelaySetting,
                        "PoolReconnectBaseDelayMs must not be less than 50 milliseconds",
                        ValidationSeverity.Error));
                }

                if (rabbitMq.PoolReconnectBaseDelayMs < Math.Min(250, networkRecoveryIntervalMs / 20))
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        poolReconnectBaseDelaySetting,
                        "PoolReconnectBaseDelayMs is very low relative to NetworkRecoveryIntervalSeconds and may cause aggressive reconnect behavior",
                        ValidationSeverity.Warning));
                }
            }

            string waitersSetting = $"{settingPrefix}:RabbitMQ:MaxPendingLeaseWaiters";
            if (rabbitMq.MaxPendingLeaseWaiters is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    waitersSetting,
                    "MaxPendingLeaseWaiters is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaxPendingLeaseWaiters < 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    waitersSetting,
                    "MaxPendingLeaseWaiters must be greater than or equal to zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaxPendingLeaseWaiters > 65536)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    waitersSetting,
                    "MaxPendingLeaseWaiters must be between 0 and 65536",
                    ValidationSeverity.Error));
            }

            string publishConfirmTimeoutSetting = $"{settingPrefix}:RabbitMQ:PublishConfirmTimeoutSeconds";
            if (rabbitMq.PublishConfirmTimeoutSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    publishConfirmTimeoutSetting,
                    "PublishConfirmTimeoutSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.PublishConfirmTimeoutSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    publishConfirmTimeoutSetting,
                    "PublishConfirmTimeoutSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.PublishConfirmTimeoutSeconds > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    publishConfirmTimeoutSetting,
                    "PublishConfirmTimeoutSeconds must be between 1 and 3600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.RpcTimeoutSeconds is > 0 && rabbitMq.PublishConfirmTimeoutSeconds > rabbitMq.RpcTimeoutSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    publishConfirmTimeoutSetting,
                    "PublishConfirmTimeoutSeconds exceeds RpcTimeoutSeconds and may outlive command-level RPC expectations",
                    ValidationSeverity.Warning));
            }

            string drainSetting = $"{settingPrefix}:RabbitMQ:MaximumShutdownDrainTimeoutSeconds";
            if (rabbitMq.MaximumShutdownDrainTimeoutSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    drainSetting,
                    "MaximumShutdownDrainTimeoutSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaximumShutdownDrainTimeoutSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    drainSetting,
                    "MaximumShutdownDrainTimeoutSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.MaximumShutdownDrainTimeoutSeconds > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    drainSetting,
                    "MaximumShutdownDrainTimeoutSeconds must be between 1 and 3600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.PublishConfirmTimeoutSeconds is > 0 && rabbitMq.MaximumShutdownDrainTimeoutSeconds is > 0 && rabbitMq.PublishConfirmTimeoutSeconds > rabbitMq.MaximumShutdownDrainTimeoutSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    publishConfirmTimeoutSetting,
                    "PublishConfirmTimeoutSeconds exceeds MaximumShutdownDrainTimeoutSeconds and may prevent bounded shutdown drain",
                    ValidationSeverity.Warning));
            }

            string requestedHeartbeatSetting = $"{settingPrefix}:RabbitMQ:RequestedHeartbeatSeconds";
            if (rabbitMq.RequestedHeartbeatSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    requestedHeartbeatSetting,
                    "RequestedHeartbeatSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.RequestedHeartbeatSeconds < 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    requestedHeartbeatSetting,
                    "RequestedHeartbeatSeconds must be greater than or equal to zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.RequestedHeartbeatSeconds > 3600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    requestedHeartbeatSetting,
                    "RequestedHeartbeatSeconds must be between 0 and 3600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.RequestedHeartbeatSeconds == 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    requestedHeartbeatSetting,
                    "RequestedHeartbeatSeconds is 0; heartbeats are disabled",
                    ValidationSeverity.Warning));
            }

            string socketTimeoutSetting = $"{settingPrefix}:RabbitMQ:SocketTimeoutSeconds";
            if (rabbitMq.SocketTimeoutSeconds is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    socketTimeoutSetting,
                    "SocketTimeoutSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.SocketTimeoutSeconds <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    socketTimeoutSetting,
                    "SocketTimeoutSeconds must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.SocketTimeoutSeconds is < 5 or > 600)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    socketTimeoutSetting,
                    "SocketTimeoutSeconds must be between 5 and 600",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.SocketTimeoutSeconds < 5)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    socketTimeoutSetting,
                    "SocketTimeoutSeconds is very low and may cause false-positive network failures",
                    ValidationSeverity.Warning));
            }

            if (rabbitMq.RequestedHeartbeatSeconds is > 0 && rabbitMq.SocketTimeoutSeconds > rabbitMq.RequestedHeartbeatSeconds)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    socketTimeoutSetting,
                    "SocketTimeoutSeconds exceeds RequestedHeartbeatSeconds and may delay low-level network failure detection relative to heartbeat policy",
                    ValidationSeverity.Warning));
            }

            string degradedSetting = $"{settingPrefix}:RabbitMQ:DegradedThreshold";
            if (rabbitMq.DegradedThreshold is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    degradedSetting,
                    "DegradedThreshold is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.DegradedThreshold is <= 0d or > 1d)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    degradedSetting,
                    "DegradedThreshold must be greater than 0 and less than or equal to 1",
                    ValidationSeverity.Error));
            }

            string unhealthySetting = $"{settingPrefix}:RabbitMQ:UnhealthyThreshold";
            if (rabbitMq.UnhealthyThreshold is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    unhealthySetting,
                    "UnhealthyThreshold is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.UnhealthyThreshold <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    unhealthySetting,
                    "UnhealthyThreshold must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.UnhealthyThreshold > 120)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    unhealthySetting,
                    "UnhealthyThreshold must be between 1 and 120",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.UnhealthyThreshold > 60)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    unhealthySetting,
                    "UnhealthyThreshold is high and may delay unhealthy-state detection",
                    ValidationSeverity.Warning));
            }

            string requestedChannelMaxSetting = $"{settingPrefix}:RabbitMQ:RequestedChannelMax";
            if (rabbitMq.RequestedChannelMax is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    requestedChannelMaxSetting,
                    "RequestedChannelMax is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.RequestedChannelMax <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    requestedChannelMaxSetting,
                    "RequestedChannelMax must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.RequestedChannelMax > 65535)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    requestedChannelMaxSetting,
                    "RequestedChannelMax must be between 1 and 65535",
                    ValidationSeverity.Error));
            }

            string poolSetting = $"{settingPrefix}:RabbitMQ:ChannelPoolSize";
            if (rabbitMq.ChannelPoolSize is null)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolSetting,
                    "ChannelPoolSize is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ChannelPoolSize <= 0)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolSetting,
                    "ChannelPoolSize must be greater than zero",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (rabbitMq.ChannelPoolSize > 8192)
            {
                diagnostics.Add(new RabbitMqValidationResult(
                    poolSetting,
                    "ChannelPoolSize must be between 1 and 8192",
                    ValidationSeverity.Error));
            }

            if (rabbitMq.MaxConnections is > 0 && rabbitMq.RequestedChannelMax is > 0)
            {
                int effectiveChannelLimit;
                try
                {
                    effectiveChannelLimit = checked(rabbitMq.MaxConnections.Value * rabbitMq.RequestedChannelMax.Value);
                }
                catch (OverflowException)
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        poolSetting,
                        "MaxConnections and RequestedChannelMax produce an invalid effective channel limit",
                        ValidationSeverity.Error));
                    return diagnostics;
                }

                if (rabbitMq.ChannelPoolSize > effectiveChannelLimit)
                {
                    diagnostics.Add(new RabbitMqValidationResult(
                        poolSetting,
                        $"ChannelPoolSize must be less than or equal to effective channel limit ({effectiveChannelLimit}) derived from MaxConnections * RequestedChannelMax",
                        ValidationSeverity.Error));
                }
            }

            return diagnostics;
        }
    }

    /// <summary>
    /// One validation diagnostic emitted while checking BackFiller identity and generated DNS hostname inputs.
    /// </summary>
    /// <param name="Setting">Configuration key associated with the reported identity problem.</param>
    /// <param name="Message">Human-readable validation message describing the detected condition.</param>
    /// <param name="Severity">Severity that determines whether startup may continue.</param>
    internal record BackFillerIdentityValidationResult(
        string Setting,
        string Message,
        ValidationSeverity Severity);

    /// <summary>
    /// Validates BackFiller hostname identity inputs (Name, Id, DnsSuffix).
    /// </summary>
    internal static class BackFillerIdentityValidator
    {
        /// <summary>
        /// Validates Name/Id/DnsSuffix and checks generated hostname validity.
        /// </summary>
        /// <param name="name">BackFiller instance name.</param>
        /// <param name="id">BackFiller numeric instance identifier.</param>
        /// <param name="dnsSuffix">DNS domain suffix.</param>
        /// <param name="settingPrefix">Configuration setting prefix for diagnostics.</param>
        /// <returns>Validation diagnostics.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<BackFillerIdentityValidationResult> Validate(
            string? name,
            int? id,
            string? dnsSuffix,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            List<BackFillerIdentityValidationResult> diagnostics = [];

            if (string.IsNullOrWhiteSpace(name))
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:Name",
                    "Name is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (id == null)
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:Id",
                    "Id is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (id is < 0 or > 99)
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:Id",
                    "Id must be between 0 and 99",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (string.IsNullOrWhiteSpace(dnsSuffix))
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:DnsSuffix",
                    "DnsSuffix is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string normalizedName = name.Trim().ToLowerInvariant();
            string normalizedSuffix = CanonicalizeDnsSuffix(dnsSuffix);

            if (!IsValidDnsLabel(normalizedName))
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:Name",
                    "Name must be a valid DNS label (letters, digits, hyphens; no leading/trailing hyphen; max 63 chars)",
                    ValidationSeverity.Error));
            }

            if (!IsValidDnsSuffix(normalizedSuffix, out string? suffixError))
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:DnsSuffix",
                    suffixError ?? "DnsSuffix is invalid",
                    ValidationSeverity.Error));
            }

            if (diagnostics.Count > 0)
            {
                return diagnostics;
            }

            string hostLabel = $"{normalizedName}{FormatBackFillerId(id.Value)}";

            if (!IsValidDnsLabel(hostLabel))
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:Name",
                    "Name + Id produces an invalid host label (must be <=63 chars and DNS-label compliant)",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string fqdn = $"{hostLabel}.{normalizedSuffix}";

            if (fqdn.Length > 253)
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:DnsSuffix",
                    "Generated FQDN exceeds the DNS maximum length of 253 characters",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (Uri.CheckHostName(fqdn) != UriHostNameType.Dns)
            {
                diagnostics.Add(new BackFillerIdentityValidationResult(
                    $"{settingPrefix}:DnsSuffix",
                    "Generated FQDN is not a valid DNS hostname",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Formats a BackFiller numeric instance identifier as a two-digit zero-padded value.
        /// </summary>
        /// <param name="id">BackFiller instance identifier.</param>
        /// <returns>Identifier in canonical two-digit format (00-99).</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="id"/> is outside the supported range 0-99.</exception>
        internal static string FormatBackFillerId(int id)
        {
            return id is < 0 or > 99
                ? throw new ArgumentOutOfRangeException(nameof(id), id, "BackFiller Id must be between 0 and 99.")
                : id.ToString("D2", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Builds the canonical BackFiller FQDN from validated identity parts.
        /// </summary>
        /// <param name="name">BackFiller instance name.</param>
        /// <param name="id">BackFiller numeric instance identifier.</param>
        /// <param name="dnsSuffix">DNS domain suffix.</param>
        /// <returns>Canonical generated BackFiller FQDN.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> or <paramref name="dnsSuffix"/> is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="id"/> is outside the supported range 0-99.</exception>
        internal static string BuildBackFillerFqdn(string name, int id, string dnsSuffix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            ArgumentException.ThrowIfNullOrWhiteSpace(dnsSuffix);

            string normalizedName = name.Trim().ToLowerInvariant();
            string normalizedSuffix = CanonicalizeDnsSuffix(dnsSuffix);
            string hostLabel = $"{normalizedName}{FormatBackFillerId(id)}";
            return $"{hostLabel}.{normalizedSuffix}";
        }

        /// <summary>
        /// Canonicalizes a DNS suffix for identity and dependency validation workflows.
        /// </summary>
        /// <param name="dnsSuffix">DNS suffix text to normalize.</param>
        /// <returns>Canonical DNS suffix form (trimmed, trailing-dot removed, lowercase).</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="dnsSuffix"/> is null, empty, or whitespace.</exception>
        internal static string CanonicalizeDnsSuffix(string dnsSuffix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(dnsSuffix);
            return dnsSuffix.Trim().TrimEnd('.').ToLowerInvariant();
        }

        /// <summary>
        /// Validates DNS suffix syntax for ACME/TLS hostname construction.
        /// </summary>
        /// <param name="dnsSuffix">DNS suffix text to validate.</param>
        /// <param name="error">Validation error message when invalid.</param>
        /// <returns><see langword="true"/> when valid; otherwise <see langword="false"/>.</returns>
        private static bool IsValidDnsSuffix(string dnsSuffix, out string? error)
        {
            if (dnsSuffix.Any(char.IsWhiteSpace))
            {
                error = "DnsSuffix must not contain whitespace";
                return false;
            }

            if (dnsSuffix.Contains("://", StringComparison.Ordinal))
            {
                error = "DnsSuffix must not contain a URL scheme such as https://";
                return false;
            }

            if (dnsSuffix.Contains(':', StringComparison.Ordinal))
            {
                error = "DnsSuffix must not contain a port number";
                return false;
            }

            if (dnsSuffix.Contains('/', StringComparison.Ordinal) || dnsSuffix.Contains('\\', StringComparison.Ordinal))
            {
                error = "DnsSuffix must not contain a path";
                return false;
            }

            string[] labels = dnsSuffix.Split('.', StringSplitOptions.None);
            if (labels.Length == 0 || labels.Any(static x => string.IsNullOrWhiteSpace(x)))
            {
                error = "DnsSuffix must be a valid DNS domain suffix";
                return false;
            }

            foreach (string label in labels)
            {
                if (!IsValidDnsLabel(label))
                {
                    error = "DnsSuffix must contain only valid DNS labels";
                    return false;
                }
            }

            error = null;
            return true;
        }

        /// <summary>
        /// Validates a single DNS label.
        /// </summary>
        /// <param name="label">Label to validate.</param>
        /// <returns><see langword="true"/> when valid; otherwise <see langword="false"/>.</returns>
        private static bool IsValidDnsLabel(string label)
        {
            if (string.IsNullOrWhiteSpace(label) || label.Length > 63)
            {
                return false;
            }

            if (label[0] == '-' || label[^1] == '-')
            {
                return false;
            }

            foreach (char c in label)
            {
                bool isValid = c is (>= 'a' and <= 'z') or
                               (>= 'A' and <= 'Z') or
                               (>= '0' and <= '9') or
                               '-';
                if (!isValid)
                {
                    return false;
                }
            }

            return true;
        }
    }

    /// <summary>
    /// One validation diagnostic emitted while checking BackFiller ACME and Cloudflare DNS settings.
    /// </summary>
    /// <param name="Setting">Configuration key associated with the reported ACME or Cloudflare problem.</param>
    /// <param name="Message">Human-readable validation message describing the detected condition.</param>
    /// <param name="Severity">Severity that determines whether startup may continue.</param>
    internal record LetsEncryptValidationResult(
        string Setting,
        string Message,
        ValidationSeverity Severity);

    /// <summary>
    /// Validates Let's Encrypt ACME configuration inputs.
    /// </summary>
    internal static class LetsEncryptValidator
    {
        /// <summary>
        /// Shared data-annotations email validator reused by ACME account-email validation.
        /// </summary>
        private static readonly EmailAddressAttribute EmailValidator = new();

        /// <summary>
        /// Validates ACME account email configuration.
        /// </summary>
        /// <param name="acmeAccountEmail">Configured ACME account email.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for ACME account email.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateAcmeAccountEmail(string? acmeAccountEmail, string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:AcmeAccountEmail";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (string.IsNullOrWhiteSpace(acmeAccountEmail))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "AcmeAccountEmail is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (acmeAccountEmail.Any(char.IsWhiteSpace))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "AcmeAccountEmail must not contain whitespace or control characters",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (acmeAccountEmail.Any(char.IsControl))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "AcmeAccountEmail must not contain whitespace or control characters",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (!EmailValidator.IsValid(acmeAccountEmail))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "AcmeAccountEmail must be a valid email address",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured maximum number of transient ACME retry attempts.
        /// </summary>
        /// <param name="acmeTransientRetryMaxAttempts">Configured transient retry max-attempts value.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for transient retry max-attempts configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateAcmeTransientRetryMaxAttempts(
            int? acmeTransientRetryMaxAttempts,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:AcmeTransientRetryMaxAttempts";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (acmeTransientRetryMaxAttempts is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "AcmeTransientRetryMaxAttempts is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (acmeTransientRetryMaxAttempts is < 1 or > 10)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "AcmeTransientRetryMaxAttempts must be between 1 and 10",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured TTL for reusing successful clock-skew validation results.
        /// </summary>
        /// <param name="clockSkewCheckTtlMinutes">Configured clock-skew check TTL in minutes.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for clock-skew check TTL configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateClockSkewCheckTtlMinutes(
            int? clockSkewCheckTtlMinutes,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:ClockSkewCheckTtlMinutes";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (clockSkewCheckTtlMinutes is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "ClockSkewCheckTtlMinutes is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (clockSkewCheckTtlMinutes is < 1 or > 60)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "ClockSkewCheckTtlMinutes must be between 1 and 60",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured maximum permitted clock-skew, in minutes.
        /// </summary>
        /// <param name="clockSkewMaxMinutes">Configured maximum permitted clock-skew in minutes.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for clock-skew maximum configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateClockSkewMaxMinutes(
            int? clockSkewMaxMinutes,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:ClockSkewMaxMinutes";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (clockSkewMaxMinutes is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "ClockSkewMaxMinutes is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (clockSkewMaxMinutes is < 1 or > 60)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "ClockSkewMaxMinutes must be between 1 and 60",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured authoritative DNS nameserver cache TTL, in minutes.
        /// </summary>
        /// <param name="dnsAuthoritativeNsCacheMinutes">Configured authoritative DNS nameserver cache TTL in minutes.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for authoritative nameserver cache TTL configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateDnsAuthoritativeNsCacheMinutes(
            int? dnsAuthoritativeNsCacheMinutes,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:DnsAuthoritativeNsCacheMinutes";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (dnsAuthoritativeNsCacheMinutes is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsAuthoritativeNsCacheMinutes is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (dnsAuthoritativeNsCacheMinutes is < 1 or > 60)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsAuthoritativeNsCacheMinutes must be between 1 and 60",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured authoritative DNS quorum ratio for ACME DNS-01 propagation checks.
        /// </summary>
        /// <param name="dnsAuthoritativeQuorumRatio">Configured authoritative DNS quorum ratio.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for authoritative DNS quorum-ratio configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateDnsAuthoritativeQuorumRatio(
            double? dnsAuthoritativeQuorumRatio,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:DnsAuthoritativeQuorumRatio";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (dnsAuthoritativeQuorumRatio is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsAuthoritativeQuorumRatio is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (double.IsNaN(dnsAuthoritativeQuorumRatio.Value) ||
                double.IsInfinity(dnsAuthoritativeQuorumRatio.Value) ||
                dnsAuthoritativeQuorumRatio <= 0 ||
                dnsAuthoritativeQuorumRatio > 1)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsAuthoritativeQuorumRatio must be greater than 0 and less than or equal to 1",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured initial DNS propagation delay before authoritative polling starts.
        /// </summary>
        /// <param name="dnsPropagationDelaySeconds">Configured DNS propagation delay in seconds.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for DNS propagation delay configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateDnsPropagationDelaySeconds(
            int? dnsPropagationDelaySeconds,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:DnsPropagationDelaySeconds";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (dnsPropagationDelaySeconds is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsPropagationDelaySeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (dnsPropagationDelaySeconds is < 0 or > 600)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsPropagationDelaySeconds must be between 0 and 600",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured authoritative DNS TXT polling interval.
        /// </summary>
        /// <param name="dnsTxtPollIntervalSeconds">Configured DNS TXT polling interval in seconds.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for DNS TXT polling interval configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateDnsTxtPollIntervalSeconds(
            int? dnsTxtPollIntervalSeconds,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:DnsTxtPollIntervalSeconds";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (dnsTxtPollIntervalSeconds is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsTxtPollIntervalSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (dnsTxtPollIntervalSeconds is < 1 or > 60)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsTxtPollIntervalSeconds must be between 1 and 60",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured DNS TXT polling timeout.
        /// </summary>
        /// <param name="dnsTxtPollTimeoutSeconds">Configured DNS TXT polling timeout in seconds.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for DNS TXT polling timeout configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateDnsTxtPollTimeoutSeconds(
            int? dnsTxtPollTimeoutSeconds,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:DnsTxtPollTimeoutSeconds";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (dnsTxtPollTimeoutSeconds is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsTxtPollTimeoutSeconds is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (dnsTxtPollTimeoutSeconds is < 1 or > 3600)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "DnsTxtPollTimeoutSeconds must be between 1 and 3600",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates coherence between DNS TXT polling interval and timeout values.
        /// </summary>
        /// <param name="dnsTxtPollIntervalSeconds">Configured DNS TXT polling interval in seconds.</param>
        /// <param name="dnsTxtPollTimeoutSeconds">Configured DNS TXT polling timeout in seconds.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for interval/timeout coherence.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateDnsTxtPollingCoherence(
            int? dnsTxtPollIntervalSeconds,
            int? dnsTxtPollTimeoutSeconds,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            List<LetsEncryptValidationResult> diagnostics = [];
            if (dnsTxtPollIntervalSeconds is null || dnsTxtPollTimeoutSeconds is null)
            {
                return diagnostics;
            }

            if (dnsTxtPollIntervalSeconds >= dnsTxtPollTimeoutSeconds)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    $"{settingPrefix}:DnsTxtPollIntervalSeconds",
                    "DnsTxtPollIntervalSeconds must be less than DnsTxtPollTimeoutSeconds",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the optional shared certificate SAN domain-name list.
        /// </summary>
        /// <param name="domainNames">Configured shared domain-name entries.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for shared domain-name configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateDomainNames(
            string[]? domainNames,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            List<LetsEncryptValidationResult> diagnostics = [];
            if (domainNames is null)
            {
                return diagnostics;
            }

            if (domainNames.Length == 0)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    $"{settingPrefix}:DomainNames",
                    "DomainNames must contain at least one DNS name when provided",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            for (int i = 0; i < domainNames.Length; i++)
            {
                string setting = $"{settingPrefix}:DomainNames:{i}";
                string? current = domainNames[i];

                if (string.IsNullOrWhiteSpace(current))
                {
                    diagnostics.Add(new LetsEncryptValidationResult(
                        setting,
                        "DomainNames entries must not be empty",
                        ValidationSeverity.Error));
                    continue;
                }

                if (current.Any(char.IsWhiteSpace) || current.Any(char.IsControl))
                {
                    diagnostics.Add(new LetsEncryptValidationResult(
                        setting,
                        "DomainNames entries must not contain whitespace or control characters",
                        ValidationSeverity.Error));
                    continue;
                }

                string normalized = current.Trim().TrimEnd('.').ToLowerInvariant();
                if (normalized.Length == 0)
                {
                    diagnostics.Add(new LetsEncryptValidationResult(
                        setting,
                        "DomainNames entries must be valid DNS names",
                        ValidationSeverity.Error));
                    continue;
                }

                if (normalized.StartsWith("*.", StringComparison.Ordinal))
                {
                    string wildcardSuffix = normalized[2..];
                    if (wildcardSuffix.Length == 0 ||
                        wildcardSuffix.StartsWith('.') ||
                        Uri.CheckHostName(wildcardSuffix) != UriHostNameType.Dns ||
                        wildcardSuffix.IndexOf('.', StringComparison.Ordinal) < 0)
                    {
                        diagnostics.Add(new LetsEncryptValidationResult(
                            setting,
                            "DomainNames wildcard entries must be valid DNS names in the form *.example.com",
                            ValidationSeverity.Error));
                    }

                    continue;
                }

                if (Uri.CheckHostName(normalized) != UriHostNameType.Dns)
                {
                    diagnostics.Add(new LetsEncryptValidationResult(
                        setting,
                        "DomainNames entries must be valid DNS names",
                        ValidationSeverity.Error));
                }
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured PFX export password used to protect PKCS#12 bundles.
        /// </summary>
        /// <param name="pfxExportPassword">Configured PFX export password.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for PFX export password configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidatePfxExportPassword(
            string? pfxExportPassword,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:PfxExportPassword";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (string.IsNullOrWhiteSpace(pfxExportPassword))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "PfxExportPassword is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (pfxExportPassword.Any(char.IsWhiteSpace) || pfxExportPassword.Any(char.IsControl))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "PfxExportPassword must not contain whitespace or control characters",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (string.Equals(pfxExportPassword, "YOUR_PFX_PASSWORD", StringComparison.Ordinal))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "PfxExportPassword must be changed from the default template placeholder",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (pfxExportPassword.Length < 12)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "PfxExportPassword must be at least 12 characters",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured certificate renewal-check interval.
        /// </summary>
        /// <param name="renewalCheckIntervalHours">Configured renewal-check interval in hours.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for renewal-check interval configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateRenewalCheckIntervalHours(
            int? renewalCheckIntervalHours,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:RenewalCheckIntervalHours";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (renewalCheckIntervalHours is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "RenewalCheckIntervalHours is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (renewalCheckIntervalHours is < 1 or > 168)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "RenewalCheckIntervalHours must be between 1 and 168",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured renewal-check scheduling jitter ratio.
        /// </summary>
        /// <param name="renewalJitterRatio">Configured renewal-check jitter ratio.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for renewal-check jitter ratio configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateRenewalJitterRatio(
            double? renewalJitterRatio,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:RenewalJitterRatio";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (renewalJitterRatio is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "RenewalJitterRatio is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (double.IsNaN(renewalJitterRatio.Value) ||
                double.IsInfinity(renewalJitterRatio.Value) ||
                renewalJitterRatio < 0 ||
                renewalJitterRatio >= 1)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "RenewalJitterRatio must be between 0 (inclusive) and 1 (exclusive)",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured certificate renewal eligibility threshold.
        /// </summary>
        /// <param name="renewBeforeExpiryDays">Configured renewal eligibility threshold in days.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for renewal eligibility threshold configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateRenewBeforeExpiryDays(
            int? renewBeforeExpiryDays,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:RenewBeforeExpiryDays";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (renewBeforeExpiryDays is null)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "RenewBeforeExpiryDays is required",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (renewBeforeExpiryDays is < 1 or > 60)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "RenewBeforeExpiryDays must be between 1 and 60",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured Cloudflare API token used for DNS-management operations.
        /// </summary>
        /// <param name="cloudFlareApiToken">Configured Cloudflare API token value.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for Cloudflare API token configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateCloudFlareApiToken(
            string? cloudFlareApiToken,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:CloudFlareApiToken";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (string.IsNullOrWhiteSpace(cloudFlareApiToken))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "CloudFlareApiToken is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (cloudFlareApiToken.Any(char.IsWhiteSpace) || cloudFlareApiToken.Any(char.IsControl))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "CloudFlareApiToken must not contain whitespace or control characters",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (string.Equals(cloudFlareApiToken, "YOUR_CLOUDFLARE_API_TOKEN", StringComparison.Ordinal))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "CloudFlareApiToken must be changed from the default template placeholder",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the configured Cloudflare zone identifier used for DNS-management operations.
        /// </summary>
        /// <param name="cloudFlareZoneId">Configured Cloudflare Zone ID.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for Cloudflare zone-id configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateCloudFlareZoneId(
            string? cloudFlareZoneId,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string setting = $"{settingPrefix}:CloudFlareZoneId";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (string.IsNullOrWhiteSpace(cloudFlareZoneId))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "CloudFlareZoneId is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string normalized = cloudFlareZoneId.Trim();
            if (normalized.Length != 32 || !normalized.All(static c => char.IsAsciiHexDigit(c)) ||
                !normalized.Equals(normalized.ToLower(CultureInfo.InvariantCulture), StringComparison.Ordinal))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    setting,
                    "CloudFlareZoneId must be a 32-character lowercase hexadecimal identifier",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Validates the ACME account private key file configuration and loadability.
        /// </summary>
        /// <param name="acmeAccountKeyPem">Configured PEM filename.</param>
        /// <param name="dirCerts">Configured certificate directory.</param>
        /// <param name="settingPrefix">Configuration prefix used for diagnostics.</param>
        /// <returns>Validation diagnostics for ACME account key configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<LetsEncryptValidationResult> ValidateAcmeAccountKeyPem(
            string? acmeAccountKeyPem,
            string? dirCerts,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            string keySetting = $"{settingPrefix}:AcmeAccountKeyPem";
            List<LetsEncryptValidationResult> diagnostics = [];

            if (string.IsNullOrWhiteSpace(acmeAccountKeyPem))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem is required and cannot be empty",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (string.IsNullOrWhiteSpace(dirCerts))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    "BackFiller:DirCerts",
                    "DirCerts is required to resolve AcmeAccountKeyPem",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string normalizedFileName = acmeAccountKeyPem.Trim();

            if (Path.IsPathRooted(normalizedFileName))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem must be a filename relative to BackFiller:DirCerts, not an absolute path",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (normalizedFileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem contains invalid filename characters",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (normalizedFileName.Contains(Path.DirectorySeparatorChar, StringComparison.Ordinal) ||
                normalizedFileName.Contains(Path.AltDirectorySeparatorChar, StringComparison.Ordinal))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem must contain only a filename and must not include path separators",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string resolvedCertDirectory = Path.IsPathRooted(dirCerts)
                ? Path.GetFullPath(dirCerts)
                : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, dirCerts.Trim()));

            string resolvedKeyPath = Path.GetFullPath(Path.Combine(resolvedCertDirectory, normalizedFileName));

            if (!IsPathWithinDirectory(resolvedKeyPath, resolvedCertDirectory))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem resolves outside BackFiller:DirCerts (path traversal is not allowed)",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (!File.Exists(resolvedKeyPath))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem file does not exist",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            string pemContent;
            try
            {
                using FileStream stream = File.Open(resolvedKeyPath, FileMode.Open, FileAccess.Read, FileShare.Read);
                using StreamReader reader = new(stream);
                pemContent = reader.ReadToEnd();
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or SecurityException)
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem file cannot be opened for reading",
                    ValidationSeverity.Error));
                return diagnostics;
            }

            if (!CanLoadSupportedPrivateKey(pemContent))
            {
                diagnostics.Add(new LetsEncryptValidationResult(
                    keySetting,
                    "AcmeAccountKeyPem must contain a valid PEM-encoded supported private key",
                    ValidationSeverity.Error));
            }

            return diagnostics;
        }

        /// <summary>
        /// Determines whether a resolved path is contained within a resolved base directory.
        /// </summary>
        /// <param name="resolvedPath">Resolved absolute path to test.</param>
        /// <param name="resolvedBaseDirectory">Resolved absolute base directory path.</param>
        /// <returns><see langword="true"/> when <paramref name="resolvedPath"/> is within <paramref name="resolvedBaseDirectory"/>.</returns>
        private static bool IsPathWithinDirectory(string resolvedPath, string resolvedBaseDirectory)
        {
            string normalizedBase = resolvedBaseDirectory.EndsWith(Path.DirectorySeparatorChar)
                ? resolvedBaseDirectory
                : resolvedBaseDirectory + Path.DirectorySeparatorChar;

            StringComparison comparison = OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;

            return resolvedPath.StartsWith(normalizedBase, comparison);
        }

        /// <summary>
        /// Determines whether PEM text can be loaded as a supported private key type.
        /// </summary>
        /// <param name="pemContent">PEM file text content.</param>
        /// <returns><see langword="true"/> when a supported private key can be loaded.</returns>
        private static bool CanLoadSupportedPrivateKey(string pemContent)
        {
            return !string.IsNullOrWhiteSpace(pemContent) && (pemContent.Contains("-----BEGIN EC PRIVATE KEY-----", StringComparison.Ordinal)
                ? TryLoadEcPrivateKey(pemContent)
                : pemContent.Contains("-----BEGIN RSA PRIVATE KEY-----", StringComparison.Ordinal)
                ? TryLoadRsaPrivateKey(pemContent)
                : pemContent.Contains("-----BEGIN PRIVATE KEY-----", StringComparison.Ordinal) && (TryLoadEcPrivateKey(pemContent) || TryLoadRsaPrivateKey(pemContent)));
        }

        /// <summary>
        /// Attempts to load an RSA private key from PEM content.
        /// </summary>
        /// <param name="pemContent">PEM text content.</param>
        /// <returns><see langword="true"/> when RSA private key import succeeds.</returns>
        private static bool TryLoadRsaPrivateKey(string pemContent)
        {
            try
            {
                using RSA rsa = RSA.Create();
                rsa.ImportFromPem(pemContent);
                _ = rsa.ExportParameters(includePrivateParameters: true);
                return true;
            }
            catch (Exception ex) when (ex is CryptographicException or ArgumentException)
            {
                return false;
            }
        }

        /// <summary>
        /// Attempts to load an ECDSA private key from PEM content.
        /// </summary>
        /// <param name="pemContent">PEM text content.</param>
        /// <returns><see langword="true"/> when ECDSA private key import succeeds.</returns>
        private static bool TryLoadEcPrivateKey(string pemContent)
        {
            try
            {
                using ECDsa ecdsa = ECDsa.Create();
                ecdsa.ImportFromPem(pemContent);
                _ = ecdsa.ExportParameters(includePrivateParameters: true);
                return true;
            }
            catch (Exception ex) when (ex is CryptographicException or ArgumentException or PlatformNotSupportedException)
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Custom validator for bind address configuration.
    /// </summary>
    /// <remarks>
    /// <para>Validates bind address syntax, local interface assignment for non-wildcard addresses, and binding availability.</para>
    /// <para>When no bind addresses are configured, runtime listener creation must explicitly map to wildcard endpoints (IPv4 Any and IPv6 Any).</para>
    /// <para>This validator is preflight-only and does not itself create or reserve runtime listeners.</para>
    /// <para>Distinguishes between different failure modes:</para>
    /// <list type="bullet">
    /// <item><description>Invalid IP address syntax</description></item>
    /// <item><description>Valid address not assigned locally</description></item>
    /// <item><description>Address assigned but unavailable for binding</description></item>
    /// <item><description>Address/port combination already in use</description></item>
    /// <item><description>Operating system or permission failure</description></item>
    /// </list>
    /// </remarks>
    internal static class BindAddressValidator
    {
        /// <summary>
        /// Validates bind address configuration for correctness and availability.
        /// </summary>
        /// <param name="bindAddresses">The array of IP addresses to validate.</param>
        /// <param name="bindPort">The TCP port to validate with each address.</param>
        /// <param name="settingPrefix">The configuration setting prefix (for diagnostic messages).</param>
        /// <returns>List of validation diagnostics (errors and warnings). Empty if valid with no warnings.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="settingPrefix"/> is null, empty, or whitespace.</exception>
        public static List<BindAddressValidationResult> Validate(
            string[]? bindAddresses,
            int? bindPort,
            string settingPrefix)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(settingPrefix);

            List<BindAddressValidationResult> diagnostics = [];

            // Omitted/empty BindAddress means wildcard listeners across all interfaces.
            if (bindAddresses == null || bindAddresses.Length == 0)
            {
                return diagnostics;
            }

            if (bindPort == null)
            {
                return diagnostics;
            }

            if (bindPort is < 1 or > 65535)
            {
                diagnostics.Add(new BindAddressValidationResult(
                    $"{settingPrefix}:BindPort",
                    "BindPort must be between 1 and 65535",
                    ValidationSeverity.Error));

                return diagnostics;
            }

            int port = bindPort.Value;

            // Warn about privileged ports
            if (port < 1024)
            {
                diagnostics.Add(new BindAddressValidationResult(
                    $"{settingPrefix}:BindPort",
                    $"Port {port} is privileged (<1024) and may require elevated permissions",
                    ValidationSeverity.Warning));
            }

            // Get all local IP addresses assigned to network interfaces.
            bool canEnumerateLocalAddresses = TryGetLocalIPAddresses(
                out HashSet<IPAddress> localAddresses,
                out Exception? localAddressEnumerationError);

            if (!canEnumerateLocalAddresses && localAddressEnumerationError != null)
            {
                diagnostics.Add(new BindAddressValidationResult(
                    $"{settingPrefix}:BindAddress",
                    $"Unable to enumerate local network interfaces: {localAddressEnumerationError.Message}",
                    ValidationSeverity.Error));
            }

            HashSet<IPAddress> configuredAddresses = [];

            // Validate each configured bind address
            for (int i = 0; i < bindAddresses.Length; i++)
            {
                string addressString = bindAddresses[i];
                string addressSetting = $"{settingPrefix}:BindAddress[{i}]";

                if (string.IsNullOrWhiteSpace(addressString))
                {
                    diagnostics.Add(new BindAddressValidationResult(
                        addressSetting,
                        "BindAddress entries cannot be empty",
                        ValidationSeverity.Error));
                    continue;
                }

                // Wildcard tokens are semantic markers and must not be parsed as literal IP addresses.
                if (BindAddressDnsAddressDeriver.IsWildcardBindAddressToken(addressString))
                {
                    continue;
                }

                // Validation 1: Syntax - must be a valid IP address
                if (!IPAddress.TryParse(addressString, out IPAddress? address))
                {
                    diagnostics.Add(new BindAddressValidationResult(
                        addressSetting,
                        $"Invalid IP address syntax: '{addressString}'",
                        ValidationSeverity.Error));
                    continue; // Cannot continue validation for this address
                }

                // Validation 2: Duplicate address detection using parsed/normalized IPAddress values.
                if (!configuredAddresses.Add(address))
                {
                    diagnostics.Add(new BindAddressValidationResult(
                        addressSetting,
                        $"Duplicate bind address '{addressString}' is configured",
                        ValidationSeverity.Error));
                    continue;
                }

                // Validation 3: Local assignment - non-wildcard addresses must be assigned to a local interface.
                // Skip this check when local interface enumeration failed to avoid misleading diagnostics.
                if (canEnumerateLocalAddresses && !IsWildcardAddress(address) && !localAddresses.Contains(address))
                {
                    diagnostics.Add(new BindAddressValidationResult(
                        addressSetting,
                        $"Address '{addressString}' is not assigned to any local network interface",
                        ValidationSeverity.Error));
                    continue;
                }

                // Validation 4: Binding availability - attempt to bind to address:port
                BindAddressValidationResult? bindingResult = TestBinding(address, port, addressSetting);
                if (bindingResult != null)
                {
                    diagnostics.Add(bindingResult);
                }
            }

            return diagnostics;
        }

        /// <summary>
        /// Attempts to enumerate all IP addresses assigned to local network interfaces.
        /// </summary>
        /// <param name="addresses">Set of local IP addresses when enumeration succeeds.</param>
        /// <param name="error">Enumeration error when enumeration fails.</param>
        /// <returns><see langword="true"/> when enumeration succeeds; otherwise <see langword="false"/>.</returns>
        private static bool TryGetLocalIPAddresses(out HashSet<IPAddress> addresses, out Exception? error)
        {
            addresses = [];
            error = null;

            try
            {
                foreach (NetworkInterface nic in NetworkInterface.GetAllNetworkInterfaces())
                {
                    // Skip interfaces that are down
                    if (nic.OperationalStatus != OperationalStatus.Up)
                    {
                        continue;
                    }

                    IPInterfaceProperties ipProps = nic.GetIPProperties();
                    foreach (UnicastIPAddressInformation addr in ipProps.UnicastAddresses)
                    {
                        _ = addresses.Add(addr.Address);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                error = ex;
                return false;
            }
        }

        /// <summary>
        /// Determines whether the address is a wildcard listener address.
        /// </summary>
        /// <param name="address">Address to evaluate.</param>
        /// <returns><see langword="true"/> for 0.0.0.0 or ::; otherwise <see langword="false"/>.</returns>
        private static bool IsWildcardAddress(IPAddress address)
        {
            ArgumentNullException.ThrowIfNull(address);
            return IPAddress.Any.Equals(address) || IPAddress.IPv6Any.Equals(address);
        }

        /// <summary>
        /// Performs a preflight bind test for an address:port combination.
        /// </summary>
        /// <remarks>
        /// This check is diagnostic only and is not an authoritative reservation of the endpoint.
        /// A separate process may bind the endpoint after validation and before listener startup.
        /// Listener initialization must still handle bind failures as the final source of truth.
        /// </remarks>
        /// <param name="address">The IP address to test.</param>
        /// <param name="port">The TCP port to test.</param>
        /// <param name="settingName">The configuration setting name (for diagnostic messages).</param>
        /// <returns>Validation result if binding fails; null if binding succeeds at validation time.</returns>
        private static BindAddressValidationResult? TestBinding(IPAddress address, int port, string settingName)
        {
            Socket? testSocket = null;
            try
            {
                AddressFamily family = address.AddressFamily;
                testSocket = new Socket(family, SocketType.Stream, ProtocolType.Tcp);

                // Attempt to bind - this will fail if:
                // - Address is not local
                // - Port is already in use
                // - Insufficient permissions
                testSocket.Bind(new IPEndPoint(address, port));

                // Success - binding is available
                return null;
            }
            catch (SocketException ex)
            {
                // Classify socket exception into specific error categories
                return ex.SocketErrorCode switch
                {
                    SocketError.AddressAlreadyInUse =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Address '{address}' port {port} is already in use by another process",
                            ValidationSeverity.Error),

                    SocketError.AddressNotAvailable =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Address '{address}' is not available for binding (not assigned to a local interface)",
                            ValidationSeverity.Error),

                    SocketError.AccessDenied =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Access denied when binding to '{address}' port {port} (privileged port or insufficient permissions)",
                            ValidationSeverity.Error),

                    SocketError.InvalidArgument =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Bind arguments are invalid for '{address}' port {port}. Verify address family and port configuration. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.AddressFamilyNotSupported =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Address family for '{address}' is not supported by this host for binding. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.SocketNotSupported =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Socket type/protocol requested for '{address}' port {port} is not supported by this host. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.OperationNotSupported =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Bind operation is not supported for '{address}' port {port}' on this host. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.NetworkDown =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Network stack is down while binding '{address}' port {port}. Verify interface and network subsystem health. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.NetworkUnreachable =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Network is unreachable for bind address '{address}' port {port}. Verify local interface and routing state. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.HostDown =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Host networking stack reported down while binding '{address}' port {port}. Verify local host network health. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.HostUnreachable =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Host was reported unreachable while binding '{address}' port {port}. Verify local host networking and address configuration. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.ConnectionRefused =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Socket layer reported connection refusal while validating bind for '{address}' port {port}. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.TimedOut =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Bind validation timed out for '{address}' port {port}. Verify host networking responsiveness. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.NoBufferSpaceAvailable =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Insufficient system socket/buffer resources to bind '{address}' port {port}. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.TooManyOpenSockets =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Socket limit reached while binding '{address}' port {port}. Reduce open sockets or increase system limits. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.ProcessLimit =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Process resource limit prevented binding '{address}' port {port}. Review per-process socket/file-descriptor limits. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.ProtocolNotSupported =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Requested protocol is not supported while binding '{address}' port {port}. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.ProtocolFamilyNotSupported =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Requested protocol family is not supported while binding '{address}' port {port}. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.HostNotFound =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Host resolution failed during bind validation for '{address}' port {port}. Verify host/network name-resolution state. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.TryAgain =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Temporary name-resolution failure occurred during bind validation for '{address}' port {port}. Retry after DNS/network stabilization. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.NoRecovery =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Non-recoverable name-resolution error occurred during bind validation for '{address}' port {port}. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.NoData =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Name-resolution completed without usable data during bind validation for '{address}' port {port}. {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    SocketError.Success or
                    SocketError.OperationAborted or
                    SocketError.IOPending or
                    SocketError.Interrupted or
                    SocketError.Fault or
                    SocketError.WouldBlock or
                    SocketError.InProgress or
                    SocketError.AlreadyInProgress or
                    SocketError.NotSocket or
                    SocketError.DestinationAddressRequired or
                    SocketError.MessageSize or
                    SocketError.ProtocolType or
                    SocketError.ProtocolOption or
                    SocketError.NetworkReset or
                    SocketError.ConnectionAborted or
                    SocketError.ConnectionReset or
                    SocketError.IsConnected or
                    SocketError.NotConnected or
                    SocketError.Shutdown or
                    SocketError.SystemNotReady or
                    SocketError.VersionNotSupported or
                    SocketError.NotInitialized or
                    SocketError.Disconnecting or
                    SocketError.TypeNotFound or
                    SocketError.SocketError =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Cannot bind to '{address}' port {port}: {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error),

                    _ =>
                        new BindAddressValidationResult(
                            settingName,
                            $"Cannot bind to '{address}' port {port}: {ex.Message} (SocketError: {ex.SocketErrorCode})",
                            ValidationSeverity.Error)
                };
            }
            catch (Exception ex)
            {
                // Unexpected exception during binding test
                return new BindAddressValidationResult(
                    settingName,
                    $"Unexpected error testing binding to '{address}' port {port}: {ex.Message}",
                    ValidationSeverity.Error);
            }
            finally
            {
                testSocket?.Dispose();
            }
        }
    }
}
