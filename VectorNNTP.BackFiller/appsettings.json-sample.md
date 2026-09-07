# VectorNNTP.BackFiller Configuration Contract (appsettings.json-sample)

This document describes the authoritative externally configurable contract represented by `appsettings.json-sample`.

## Scope and conventions

- Source of truth is code binding + validation + runtime projection:
  - `Configuration/BackFillerOptions.cs`
  - `Configuration/ConnectionStringsOptions.cs`
  - `Startup/Configuration/ConfigurationValidator.cs`
  - `Startup/Configuration/RuntimeSnapshotFactory.cs`
  - direct reads in `Startup/Logging/SerilogConfigurator.cs` and `Configuration/OperationalDirectoryValidator.cs`
- `appsettings.json-sample` is valid JSON (no inline comments) to remain loadable by standard configuration providers.
- Defaults below are code-derived defaults, not environment-tuned recommendations.
- Secrets are placeholders only. Replace before production.

## Configuration matrix

| Configuration Path | Type | Default | Required | Validation | Consumer | Documented |
|---|---|---|---|---|---|---|
| BackFiller:BindAddress | string[]? | null (omitted => wildcard bind derivation) | No | Each token must be valid IP/wildcard/local bindable; no duplicates; wildcard semantics validated | BindAddressValidator -> RuntimeSnapshotFactory -> Listener endpoint derivation | Yes |
| BackFiller:BindPort | int? | none | Yes | 1..65535 | Listener socket binding, identity dump, runtime snapshot | Yes |
| BackFiller:Name | string? | none | Yes | non-empty; DNS label compatible through identity validator | Canonical FQDN generation; certificate subject; parser canonicalization | Yes |
| BackFiller:Id | int? | none | Yes | 0..99 | Canonical FQDN generation | Yes |
| BackFiller:DnsSuffix | string | usenet.ninja | Yes | non-empty; canonical suffix + generated FQDN must be valid DNS and <=253 chars | Canonical FQDN generation | Yes |
| BackFiller:DirLogs | string? | none | Yes | non-empty; resolvable path; creatable dir; write/read/replace/delete probe must pass | OperationalDirectoryValidator + Serilog file sink target directory | Yes |
| BackFiller:DirCerts | string? | none | Yes | non-empty; resolvable path; creatable dir; capability probe; needed for account-key resolution | OperationalDirectoryValidator + certificate/key path projection | Yes |
| BackFiller:Listener:ParserAccumulationMaxBytes | int | 262144 | Yes | 32768..int.MaxValue | Listener protocol buffering hard cap | Yes |
| BackFiller:Listener:AwaitingReceiptAckTimeoutSeconds | int | 30 | Yes | 1..300 | Receipt-ack wait timeout per connection | Yes |
| BackFiller:Listener:MaxQueuedFoundPayloadBytes | int | 67108864 | Yes | 1..int.MaxValue | Per-connection queued/in-flight Found payload budget | Yes |
| BackFiller:Listener:MaxActiveConnections | int | 1024 | Yes | 1..int.MaxValue | Listener admission/backpressure cap | Yes |
| BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes | int | 4 | Yes | >=1; additionally <= 80% of physical memory (in GiB); conversion to bytes must fit long | Retention memory capacity (converted to bytes at startup) | Yes |
| BackFiller:ArticleRetention:RetentionTtlSeconds | int | 60 | Yes | 1..60 | Absolute retained payload TTL | Yes |
| BackFiller:ArticleRetention:SweepIntervalSeconds | int | 1 | Yes | 1..60 | TTL sweep cadence | Yes |
| BackFiller:LetsEncrypt:Enabled | bool | true | Yes (section-level) | boolean | Enables ACME issuance workflow; when false, listener TLS workflow disabled, but Cloudflare values still required | Yes |
| BackFiller:LetsEncrypt:AcmeAccountEmail | string | security@usenet.ninja | Conditionally required (Enabled=true) | non-empty, no whitespace/control, valid email | ACME account registration/runtime options | Yes |
| BackFiller:LetsEncrypt:AcmeAccountKeyPem | string | account.key | Conditionally required (Enabled=true) | non-empty filename, not rooted path, valid chars, resolvable under DirCerts, PEM loadability checks | ACME account key path projection | Yes |
| BackFiller:LetsEncrypt:AcmeTransientRetryMaxAttempts | int? | 5 | Conditionally required (Enabled=true) | 1..10 | ACME transient retry policy | Yes |
| BackFiller:LetsEncrypt:ClockSkewCheckTtlMinutes | int? | 5 | Conditionally required (Enabled=true) | 1..60 | Clock skew validation cache TTL | Yes |
| BackFiller:LetsEncrypt:ClockSkewMaxMinutes | int? | 10 | Conditionally required (Enabled=true) | 1..60 | Allowed UTC skew for ACME safety checks | Yes |
| BackFiller:LetsEncrypt:DnsAuthoritativeNsCacheMinutes | int? | 5 | Conditionally required (Enabled=true) | 1..60 | Authoritative NS cache TTL during DNS-01 | Yes |
| BackFiller:LetsEncrypt:DnsAuthoritativeQuorumRatio | double? | 0.7 | Conditionally required (Enabled=true) | >0 and <=1; rejects NaN/Infinity | Authoritative DNS TXT quorum success threshold | Yes |
| BackFiller:LetsEncrypt:DnsPropagationDelaySeconds | int? | 15 | Conditionally required (Enabled=true) | 0..600 | Initial wait before DNS TXT polling starts | Yes |
| BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds | int? | 3 | Conditionally required (Enabled=true) | 1..60 and must be < DnsTxtPollTimeoutSeconds | DNS TXT polling cadence | Yes |
| BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds | int? | 600 | Conditionally required (Enabled=true) | 1..3600 and must be > DnsTxtPollIntervalSeconds | Total DNS TXT polling timeout | Yes |
| BackFiller:LetsEncrypt:DomainNames | string[]? | null | No | if provided: non-empty array, valid DNS/wildcard entries | Compatibility-only input; runtime identity uses generated canonical FQDN instead | Yes |
| BackFiller:LetsEncrypt:PfxExportPassword | string | YOUR_PFX_PASSWORD (template) | Conditionally required (Enabled=true) | non-empty; no whitespace/control; must not equal template placeholder; min length 12 | PFX/PKCS#12 protection password | Yes |
| BackFiller:LetsEncrypt:RenewalCheckIntervalHours | int? | 6 | Conditionally required (Enabled=true) | 1..168 | Renewal scheduler cadence | Yes |
| BackFiller:LetsEncrypt:RenewalJitterRatio | double? | 0.1 | Conditionally required (Enabled=true) | >=0 and <1; rejects NaN/Infinity | Renewal jitter spread policy | Yes |
| BackFiller:LetsEncrypt:RenewBeforeExpiryDays | int? | 7 | Conditionally required (Enabled=true) | 1..60 | Renewal eligibility threshold before expiry | Yes |
| BackFiller:LetsEncrypt:UseStagingDirectory | bool | false | No | boolean | ACME environment selection (warning if true) | Yes |
| BackFiller:LetsEncrypt:CloudFlareApiToken | string | YOUR_CLOUDFLARE_API_TOKEN (template) | Yes (even when Enabled=false) | non-empty; no whitespace/control; must not equal template placeholder | Cloudflare DNS operations | Yes |
| BackFiller:LetsEncrypt:CloudFlareZoneId | string | 5811a29d39a0732afb5f160c9b137c3d (code default) | Yes (even when Enabled=false) | non-empty; must be 32-char lowercase hex | Cloudflare DNS zone targeting | Yes |
| BackFiller:RabbitMQ:WorkRequestMaxPayloadBytes | int? | 1024 | Yes | 1..4096 | Max control-plane request envelope bytes before copy/reject behavior | Yes |
| BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds | int? | 60 | Yes | 1..3600 and >= RpcTimeoutSeconds | RabbitMQ operation-timeout coherence policy | Yes |
| BackFiller:RabbitMQ:RpcTimeoutSeconds | int? | 30 | Yes | 1..3600 and <= ChannelLeaseTimeoutSeconds and <= ConnectionBlockedTimeoutSeconds | AMQP continuation/handshake timeout | Yes |
| BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds | int? | 30 | Yes | 5..3600 and >= RpcTimeoutSeconds | Timeout budget for blocked connection handling | Yes |
| BackFiller:RabbitMQ:Hosts | string[]? | [] | Yes | at least one host; each host valid DNS/IP, no scheme/path/query/credentials, no duplicates | Connection host list | Yes |
| BackFiller:RabbitMQ:Username | string? | null | Conditionally required | if configured cannot be whitespace; required when Password set; warning if guest | Broker authentication | Yes |
| BackFiller:RabbitMQ:Password | string? | null | Conditionally required | required when Username set; must not be whitespace when configured | Broker authentication secret | Yes |
| BackFiller:RabbitMQ:VirtualHost | string? | / | No | non-empty when provided, no whitespace/control | AMQP namespace selection | Yes |
| BackFiller:RabbitMQ:EnableSsl | bool? | true | Yes | boolean | Enables TLS to RabbitMQ | Yes |
| BackFiller:RabbitMQ:Port | int? | 5672 | Yes | 1..65535 | Broker TCP port | Yes |
| BackFiller:RabbitMQ:ChannelPoolSize | int? | 512 | Yes | 1..8192 and <= MaxConnections*RequestedChannelMax (checked overflow) | Delivery buffer/channel-capacity policy | Yes |
| BackFiller:RabbitMQ:MinConnections | int? | 4 | Yes | 1..512 and <= MaxConnections | Policy projection for minimum connection target | Yes |
| BackFiller:RabbitMQ:MaxConnections | int? | 16 | Yes | 1..512 and >= MinConnections | Policy projection for maximum connection target | Yes |
| BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures | int? | 5 | Yes | 1..100 | Max consecutive recovery failure budget | Yes |
| BackFiller:RabbitMQ:MaxPendingLeaseWaiters | int? | 1024 | Yes | 0..65536 | Policy projection for lease waiter pressure bound | Yes |
| BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds | int? | 300 | Yes | 30..86400 | Policy projection for idle scale-down eligibility | Yes |
| BackFiller:RabbitMQ:ScaleDownCooldownSeconds | int? | 30 | Yes | 0..3600; warning when exceeds idle threshold | Policy projection for scale-down cooldown | Yes |
| BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds | int? | 5 | Yes | 1..3600; warning if > blocked timeout | Recovery retry interval | Yes |
| BackFiller:RabbitMQ:PoolReconnectBaseDelayMs | int? | 250 | Yes | 50..60000 and <= max delay | Reconnect backoff base | Yes |
| BackFiller:RabbitMQ:PoolReconnectMaxDelayMs | int? | 30000 | Yes | 50..300000 and >= base delay | Reconnect backoff cap | Yes |
| BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds | int? | 300 | Yes | 30..86400; warning if > idle scale-down threshold | Policy projection for connection retirement | Yes |
| BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds | int? | 10 | Yes | 1..3600 | Publish confirm timeout | Yes |
| BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds | int? | 30 | Yes | 1..3600 and must be <= BackFiller:Shutdown:GracePeriodSeconds | RabbitMQ shutdown drain budget projection | Yes |
| BackFiller:RabbitMQ:DegradedThreshold | double? | 0.75 | Yes | >0 and <=1 | Policy threshold for degraded capacity state | Yes |
| BackFiller:RabbitMQ:UnhealthyThreshold | int? | 5 | Yes | 1..120; warning when >60 | Consecutive unhealthy threshold policy | Yes |
| BackFiller:RabbitMQ:RequestedHeartbeatSeconds | int? | 60 | Yes | 0..3600; warning when 0; coherence warning with socket timeout | AMQP heartbeat negotiation | Yes |
| BackFiller:RabbitMQ:SocketTimeoutSeconds | int? | 30 | Yes | 5..600; warning when <10; warning when > heartbeat | Socket I/O timeout | Yes |
| BackFiller:RabbitMQ:RequestedChannelMax | int? | 2047 | Yes | 1..65535 | AMQP requested channel limit | Yes |
| BackFiller:RabbitMQ:ConsumerPrefetchCount | ushort? | null | No | if set: 1..65535 | Optional Basic.Qos prefetch | Yes |
| BackFiller:RabbitMQ:DiagnosticPayloadCorrelationId | string? | null | No | no structural validator | Optional temporary payload diagnostic gate | Yes |
| BackFiller:TransitServer:Host | string | localhost | Yes | non-empty; no scheme/credentials/path/query/embedded port; valid DNS/IP | Outbound transit endpoint host | Yes |
| BackFiller:TransitServer:Port | int | 119 | Yes | 1..65535; warning on 119+TLS and 563+non-TLS combinations | Outbound transit endpoint port | Yes |
| BackFiller:TransitServer:UseSsl | bool | false | Yes | boolean | TLS for outbound transit | Yes |
| BackFiller:Shutdown:GracePeriodSeconds | int | 30 | Yes | 5..600 | Global graceful shutdown budget; also host shutdown timeout | Yes |
| BackFiller:Shutdown:DrainQueuedWork | bool | true | Yes | cross-rule: cannot be true when FinishActiveArticles=false | Shutdown drain behavior for admitted queued work | Yes |
| BackFiller:Shutdown:FinishActiveArticles | bool | true | Yes | cross-rule with DrainQueuedWork | Shutdown policy for in-flight active work | Yes |
| ConnectionStrings:GrabberDB | string? | none | Yes | non-empty; valid connection string; server/database/user id required; warnings for suboptimal pool/timeouts | Control-plane DB connectivity | Yes |
| Serilog:MinimumLevel:Default | string? | fallback Information if parse fails | No | parsed to Serilog/Microsoft log levels | Startup logging minimum level | Yes |

## Effective default derivation notes

- Defaults came from property initializers on options classes plus runtime snapshot fallback expressions where nullable values are projected.
- For non-nullable properties with initializers, the initializer is the effective config default.
- For nullable + `[Required]` properties, the default exists in code but missing/null can still fail validation depending on binder behavior and custom validators.
- `BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes` is operator-facing GiB and converted at startup to runtime bytes (`* 1024^3`) with checked overflow.
- `Serilog:MinimumLevel:Default` is direct-read; invalid values parse to Information.

## Important cross-setting rules

- `BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds >= BackFiller:RabbitMQ:RpcTimeoutSeconds`
- `BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds >= BackFiller:RabbitMQ:RpcTimeoutSeconds`
- `BackFiller:RabbitMQ:MinConnections <= BackFiller:RabbitMQ:MaxConnections`
- `BackFiller:RabbitMQ:PoolReconnectBaseDelayMs <= BackFiller:RabbitMQ:PoolReconnectMaxDelayMs`
- `BackFiller:RabbitMQ:ChannelPoolSize <= MaxConnections * RequestedChannelMax` (checked overflow)
- `BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds <= BackFiller:Shutdown:GracePeriodSeconds`
- `BackFiller:Shutdown:DrainQueuedWork` requires `BackFiller:Shutdown:FinishActiveArticles=true`
- `BackFiller:LetsEncrypt:DnsTxtPollIntervalSeconds < BackFiller:LetsEncrypt:DnsTxtPollTimeoutSeconds`
- `BackFiller:ArticleRetention:MaximumRetainedPayloadGigabytes` must also be <= 80% of physical memory in GiB on startup host.
  - Linux authoritative source: `/proc/meminfo` `MemTotal` (kB converted to bytes).
  - Windows authoritative source: `GlobalMemoryStatusEx().ullTotalPhys`.
  - `/sys`, cgroup/container memory limits, available/free memory, process memory, and GC-reported memory are not substituted because they represent different resource semantics than physical system memory.
  - If physical system memory cannot be determined from the authoritative source, startup configuration validation fails so retention capacity is never accepted without the 80% safety boundary.
  - On Linux, missing/unreadable/malformed `/proc/meminfo` `MemTotal` indicates an unsupported environment for this retention-validation policy.
- `BackFiller:LetsEncrypt:CloudFlareApiToken` and `BackFiller:LetsEncrypt:CloudFlareZoneId` are required even if `BackFiller:LetsEncrypt:Enabled=false`.

## Exclusions (intentionally not in sample)

- `Logging` section: currently not consumed by production startup/runtime configuration path.
- `OpenTelemetry:PrometheusEndpoint`: currently not consumed by production startup/runtime configuration path.
- Most `Serilog` subtree (sinks/templates/write targets): production logger pipeline is hard-coded in `SerilogConfigurator`; only `Serilog:MinimumLevel:Default` is read.
- Runtime-only projected values (for example transit queue capacities/coalesce/shutdown windows and derived canonical paths/FQDN/byte conversions) are not direct external settings.

## Orphaned/obsolete/stale findings

- Stale-in-practice appsettings keys were observed in current `appsettings.json`:
  - `Logging:*`
  - `OpenTelemetry:PrometheusEndpoint`
  - most `Serilog` properties beyond `Serilog:MinimumLevel:Default`
- Compatibility-shaped setting:
  - `BackFiller:LetsEncrypt:DomainNames` remains validated but runtime certificate identity uses generated canonical BackFiller FQDN instead.

## Missing configuration opportunities (meaningful only)

Potential operator knobs currently hard-coded in runtime snapshot and not externally configurable:

- Transit write coalescing microseconds (`WriteBatchCoalesceMicroseconds` = 250)
- Transit queue maximum item count (`TransitQueueMaxItemCount` = 2048)
- Transit retry max attempts (`TransitRetryMaxAttempts` = 3)
- Transit shutdown timings (`TransitShutdownDrainGracePeriod` = 5 min, inactivity watchdog 30 sec, absolute max 30 min)

These are reported only as opportunities; no behavior/schema changes were introduced.
