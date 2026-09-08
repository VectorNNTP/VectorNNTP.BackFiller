// <copyright file="BackFillerRuntimeOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller.Configuration
// Immutable runtime configuration snapshot used by startup validation and hosted services.

using System.Net;

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Immutable runtime configuration snapshot produced after successful startup validation.
    /// </summary>
    /// <remarks>
    /// This snapshot is created once during startup projection and then consumed by hosted runtime components as the
    /// authoritative operational configuration contract.
    /// </remarks>
    /// <param name="CanonicalBackFillerFqdn">Canonical BackFiller FQDN derived from validated identity inputs.</param>
    /// <param name="BackFillerId">Authoritative BackFiller server identifier derived from validated configuration.</param>
    /// <param name="CanonicalDnsSuffix">Canonical DNS suffix derived from validated configuration.</param>
    /// <param name="ValidatedLogDirectory">Validated absolute log directory path.</param>
    /// <param name="ValidatedCertificateDirectory">Validated absolute certificate directory path when listener certificate runtime projection is included; otherwise <see langword="null"/> for non-listener scoped snapshots.</param>
    /// <param name="RabbitMqHosts">Canonical RabbitMQ host list used by runtime services.</param>
    /// <param name="RabbitMqPort">Validated RabbitMQ port used by runtime services.</param>
    /// <param name="RabbitMqEnableSsl">Validated RabbitMQ TLS mode used by runtime services.</param>
    /// <param name="TransitServerHost">Canonical TransitServer host used by runtime services.</param>
    /// <param name="TransitServerPort">Validated TransitServer port used by runtime services.</param>
    /// <param name="TransitServerUseSsl">Validated TransitServer TLS mode used by runtime services.</param>
    /// <param name="BindPort">Validated inbound listener TCP port.</param>
    /// <param name="ConfiguredBindAddressTokens">Configured inbound bind-address tokens preserved from BackFiller configuration.</param>
    /// <param name="ShutdownGracePeriodSeconds">Validated graceful-shutdown grace period in seconds used by runtime services.</param>
    /// <param name="ShutdownDrainQueuedWork">Validated graceful-shutdown queued-work drain flag used by runtime services.</param>
    /// <param name="ShutdownFinishActiveArticles">Validated graceful-shutdown active-work completion flag used by runtime services.</param>
    /// <param name="RabbitMqMaximumShutdownDrainTimeoutSeconds">Validated RabbitMQ shutdown-drain timeout in seconds used by runtime services.</param>
    /// <param name="WriteBatchCoalesceMicroseconds">Configured writer coalescing window in microseconds for transit write batching experiments.</param>
    /// <param name="TransitQueueMaxItemCount">Global transit queue maximum admitted queued work-item count.</param>
    /// <param name="TransitRetryMaxAttempts">Global transit per-item maximum transmission attempts.</param>
    /// <param name="TransitReconnectInitializationTimeout">Maximum reconnect initialization time when admitted work is outstanding.</param>
    /// <param name="TransitShutdownDrainGracePeriod">Initial transit shutdown drain grace period.</param>
    /// <param name="TransitShutdownDrainInactivityWatchdog">Transit shutdown inactivity watchdog duration.</param>
    /// <param name="TransitShutdownAbsoluteMaximum">Absolute transit shutdown duration ceiling.</param>
    /// <param name="CanonicalBindAddresses">Canonical, deduplicated bind-address set validated at startup.</param>
    /// <param name="ArticleRetention">Validated immutable article-retention runtime options projected from BackFiller:ArticleRetention.</param>
    /// <param name="Listener">Validated immutable Listener runtime options projected from BackFiller:Listener.</param>
    /// <param name="LetsEncrypt">Validated immutable Let's Encrypt/ACME runtime options.</param>
    /// <param name="RabbitMq">Validated immutable RabbitMQ runtime options projected from BackFiller:RabbitMQ.</param>
    internal sealed record BackFillerRuntimeOptions(
        string CanonicalBackFillerFqdn,
        int BackFillerId,
        string CanonicalDnsSuffix,
        string ValidatedLogDirectory,
        string? ValidatedCertificateDirectory,
        IReadOnlyList<string> RabbitMqHosts,
        int RabbitMqPort,
        bool RabbitMqEnableSsl,
        string TransitServerHost,
        int TransitServerPort,
        bool TransitServerUseSsl,
        int BindPort = 119,
        IReadOnlyList<string>? ConfiguredBindAddressTokens = null,
        int ShutdownGracePeriodSeconds = 30,
        bool ShutdownDrainQueuedWork = true,
        bool ShutdownFinishActiveArticles = true,
        int RabbitMqMaximumShutdownDrainTimeoutSeconds = 30,
        int WriteBatchCoalesceMicroseconds = 250,
        int TransitQueueMaxItemCount = 2048,
        int TransitRetryMaxAttempts = 3,
        TimeSpan? TransitReconnectInitializationTimeout = null,
        TimeSpan? TransitShutdownDrainGracePeriod = null,
        TimeSpan? TransitShutdownDrainInactivityWatchdog = null,
        TimeSpan? TransitShutdownAbsoluteMaximum = null,
        IReadOnlyList<IPAddress>? CanonicalBindAddresses = null,
        ArticleRetentionRuntimeOptions? ArticleRetention = null,
        ListenerRuntimeOptions? Listener = null,
        BackFillerLetsEncryptRuntimeOptions? LetsEncrypt = null,
        RabbitMqRuntimeOptions? RabbitMq = null)
    {
        /// <summary>
        /// Gets the effective reconnect initialization timeout.
        /// </summary>
        /// <value>Configured reconnect initialization timeout, or a default of 2 seconds when not specified.</value>
        internal TimeSpan EffectiveTransitReconnectInitializationTimeout => TransitReconnectInitializationTimeout ?? TimeSpan.FromSeconds(2);

        /// <summary>
        /// Gets the effective transit shutdown drain grace period.
        /// </summary>
        /// <value>Configured drain grace period, or a default of 5 minutes when not specified.</value>
        internal TimeSpan EffectiveTransitShutdownDrainGracePeriod => TransitShutdownDrainGracePeriod ?? TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets the effective transit shutdown inactivity watchdog duration.
        /// </summary>
        /// <value>Configured inactivity watchdog duration, or a default of 30 seconds when not specified.</value>
        internal TimeSpan EffectiveTransitShutdownDrainInactivityWatchdog => TransitShutdownDrainInactivityWatchdog ?? TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets the effective absolute transit shutdown maximum duration.
        /// </summary>
        /// <value>Configured absolute shutdown ceiling, or a default of 30 minutes when not specified.</value>
        internal TimeSpan EffectiveTransitShutdownAbsoluteMaximum => TransitShutdownAbsoluteMaximum ?? TimeSpan.FromMinutes(30);

        /// <summary>
        /// Gets the canonical bind-address set, or an empty set when no explicit bind addresses are configured.
        /// </summary>
        /// <value>Validated canonical bind addresses used by listener and DNS projection logic.</value>
        internal IReadOnlyList<IPAddress> EffectiveCanonicalBindAddresses => CanonicalBindAddresses ?? [];

        /// <summary>
        /// Gets configured bind-address tokens, or an empty set when omitted.
        /// </summary>
        /// <value>Original configured bind-address tokens preserved for runtime consumers that need token-level semantics.</value>
        internal IReadOnlyList<string> EffectiveConfiguredBindAddressTokens => ConfiguredBindAddressTokens ?? [];

        /// <summary>
        /// Gets effective article-retention runtime options for shared in-memory article ownership.
        /// </summary>
        /// <value>
        /// Configured retention runtime options when present; otherwise defaults to a 4 GiB payload budget,
        /// 60-second insertion TTL, and one-second sweep cadence.
        /// </value>
        internal ArticleRetentionRuntimeOptions EffectiveArticleRetention => ArticleRetention
            ?? new ArticleRetentionRuntimeOptions(
                MaximumRetainedPayloadBytes: 4L * 1024 * 1024 * 1024,
                RetentionTtl: TimeSpan.FromSeconds(60),
                SweepInterval: TimeSpan.FromSeconds(1));

        /// <summary>
        /// Gets effective Listener runtime options for parser bounds, receipt-ack deadline, Found payload pressure, and active connection cap.
        /// </summary>
        /// <value>
        /// Configured Listener runtime options when present; otherwise defaults to a 256 KiB parser accumulation limit,
        /// 30-second receipt-ack timeout, 64 MiB queued/in-flight Found payload budget per connection, and 1024 active connections.
        /// </value>
        internal ListenerRuntimeOptions EffectiveListener => Listener
            ?? new ListenerRuntimeOptions(
                ParserAccumulationMaxBytes: 262144,
                AwaitingReceiptAckTimeout: TimeSpan.FromSeconds(30),
                MaxQueuedFoundPayloadBytes: 67108864,
                MaxActiveConnections: 1024);

        /// <summary>
        /// Gets validated ACME runtime options required by the mandatory TLS listener certificate workflow.
        /// </summary>
        /// <value>Validated ACME runtime options required by certificate-management flows.</value>
        /// <exception cref="InvalidOperationException">Thrown when ACME runtime options are not available.</exception>
        internal BackFillerLetsEncryptRuntimeOptions EffectiveLetsEncrypt => LetsEncrypt
            ?? throw new InvalidOperationException("BackFiller runtime options do not include Let's Encrypt settings.");
    }
}
