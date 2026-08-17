# VectorNNTP.BackFiller Production Audit Status

## Audit Mode
- Mode: Batched production audit
- Scope: Currently implemented production subsystems only
- Exclusions: Future/not-implemented runtime architecture (work distribution runtime, provider pools/reconciliation, grabber workers, article retrieval/processing/recovery)

## Status Legend
- CLOSED
- DEFECT FOUND / FIXED
- PARTIALLY AUDITED
- NOT AUDITED
- FUTURE / NOT IMPLEMENTED

## Current Ledger (Batch Initialization)
| Subsystem | Production Paths | Status | Notes |
|---|---|---|---|
| Connection-string validation/parity | `Configuration/ConnectionStringsOptions.cs`, `Configuration/MySqlConnectionStringUtilities.cs`, `Startup/Configuration/ConfigurationFingerprintService.cs` | DEFECT FOUND / FIXED | Pool-size invalid-value diagnostic classification fixed; regression tests present. |
| Operational directory validation | `Configuration/OperationalDirectoryValidator.cs`, `Startup/Configuration/RuntimeSnapshotFactory.cs` | CLOSED | Audited clean in prior run evidence. |
| RabbitMQ configuration validation | `Configuration/BackFillerOptions.cs` (`RabbitMqValidator`), `Startup/Configuration/ConfigurationValidator.cs` | CLOSED | DEFECT FOUND / FIXED in this batch: whitespace-only password acceptance; fixed with credential-whitespace checks and regression tests; validated (focused + broader + full suite + build + validate-config). |
| Startup validation orchestration | `Startup/Validation/StartupValidationPipeline.cs` | CLOSED | Audited in batch: cancellation pre-check, config/dependency gating, runtime snapshot build path; no concrete defect found in this pass. |
| Dependency probe runner | `Startup/Validation/DependencyProbeRunner.cs` | CLOSED | Audited in batch: concurrent probe fan-out/aggregation and result merge semantics; no concrete defect found in this pass. |
| RabbitMQ dependency probe | `Startup/Validation/RabbitMqDependencyProbe.cs` | CLOSED | Audited in batch: host normalization, timeout/cancellation, sanitized socket/auth classification; no concrete defect found in this pass. |
| Database dependency probe | `Startup/Validation/DatabaseDependencyProbe.cs` | DEFECT FOUND / FIXED | Defect: unsanitized exception text leaked in generic failure path (`Failed to connect: {ex.Message}`); fixed to sanitized message with debug-only exception logging; regression added. |
| Cloudflare dependency probe | `Startup/Validation/CloudflareDependencyProbe.cs` | CLOSED | Audited in batch: token/zone guards, timeout/cancellation behavior, and sanitized failure classification; no concrete defect found in this pass. |
| Transit server dependency probe | `Startup/Validation/TransitServerDependencyProbe.cs` | CLOSED | Audited in batch: STARTTLS/TLS negotiation path, stream-mode checks, cancellation and sanitized failure mapping; no concrete defect found in this pass. |
| Runtime snapshot creation | `Startup/Configuration/RuntimeSnapshotFactory.cs` | CLOSED | Audited in batch: canonicalization, directory validation integration, snapshot construction and error surfacing; no concrete defect found in this pass. |
| Exit code policy | `Startup/ExitCodePolicy.cs` | CLOSED | Audited in batch: exit-code mapping and signal/unknown translation semantics; no concrete defect found in this pass. |
| Shutdown policy provider | `Startup/Shutdown/ShutdownPolicyProvider.cs` | CLOSED | Audited in batch: defensive bind/validation/clone semantics; currently isolated from runtime call graph, no concrete runtime correctness defect found in this pass. |

## Active Batch (10 Subsystems)
1. RabbitMQ configuration validation (`Configuration/BackFillerOptions.cs`, `Startup/Configuration/ConfigurationValidator.cs`) — CLOSED this session after defect fix; include as skipped/closed in batch accounting.
2. Startup validation orchestration (`Startup/Validation/StartupValidationPipeline.cs`) — CLOSED in this batch.
3. Dependency probe runner (`Startup/Validation/DependencyProbeRunner.cs`) — CLOSED in this batch.
4. RabbitMQ dependency probe (`Startup/Validation/RabbitMqDependencyProbe.cs`) — CLOSED in this batch.
5. Database dependency probe (`Startup/Validation/DatabaseDependencyProbe.cs`) — DEFECT FOUND / FIXED in this batch.
6. Cloudflare dependency probe (`Startup/Validation/CloudflareDependencyProbe.cs`) — CLOSED in this batch.
7. Transit server dependency probe (`Startup/Validation/TransitServerDependencyProbe.cs`) — CLOSED in this batch.
8. Runtime snapshot creation (`Startup/Configuration/RuntimeSnapshotFactory.cs`) — CLOSED in this batch.
9. Exit code policy (`Startup/ExitCodePolicy.cs`) — CLOSED in this batch.
10. Shutdown policy provider (`Startup/Shutdown/ShutdownPolicyProvider.cs`) — CLOSED in this batch.

## Closure / Skip Criteria
- Mark as SKIPPED (already CLOSED) only when current production behavior and relevant tests indicate no unresolved boundary risk from this batch scope.
- Mark as CLOSED after deep audit if no concrete defect is found.
- Mark as DEFECT FOUND / FIXED when first concrete subsystem defect is fixed with deterministic regression and validation.

## Batch 2 Target Subsystems (Initialized)
| Subsystem | Production Paths | Initial Status | Batch 2 Notes |
|---|---|---|---|
| Program top-level startup orchestration | `Program.cs` | CLOSED | Batch 2 audited: startup-phase orchestration, cancellation/event unhooking, lifecycle transitions, exit-code paths, and exception/finally behavior reviewed; no concrete defect established in this pass. |
| Startup Commands pipeline | `Startup/Commands/OperationalCommandParser.cs`, `Startup/Commands/OperationalCommandDispatcher.cs`, `Startup/Commands/OperationalCommandExecutor.cs`, `Startup/Commands/*CommandHandler.cs` | CLOSED | Batch 2 audited: strict parse/dispatch contract, configuration-gated command handling, and safe output semantics reviewed; no concrete defect established in this pass. |
| HostComposer | `Startup/Hosting/HostComposer.cs` | CLOSED | Batch 2 audited: validated runtime snapshot wiring, DI ownership, shutdown timeout mapping, and hosted-service registrations; no concrete defect established in this pass. |
| HostLifetimeCoordinator | `Startup/Hosting/HostLifetimeCoordinator.cs` | CLOSED | Batch 2 audited: startup/run/shutdown sequencing, readiness gating, lifecycle transitions, and graceful-shutdown signaling; no concrete defect established in this pass. |
| SystemdNotifier | `Startup/Hosting/SystemdNotifier.cs` | CLOSED | Batch 2 audited: status normalization, throttle gating, library availability caching, and exception-safe no-op behavior off-systemd; no concrete defect established in this pass. |
| SerilogConfigurator | `Startup/Logging/SerilogConfigurator.cs` | CLOSED | Batch 2 audited: bootstrap-to-production swap, min-level parsing, sink setup/failure handling, and provider registration reviewed; no concrete defect established in this pass. |
| ProcessBootstrapper | `Startup/ProcessBootstrapper.cs` | CLOSED | Batch 2 audited: culture/bootstrap init, threadpool diagnostics, global exception handler registration and terminal flush behavior reviewed; no concrete defect established in this pass. |
| ControlPlaneService | `ControlPlane/ControlPlaneService.cs` | CLOSED | Batch 2 audited: startup barrier, periodic refresh cadence, cancellation handling, and refresh-failure containment reviewed; no concrete defect established in this pass. |
| TransitConnection negotiation / COMPRESS fallback | `Runtime/Transit/TransitConnection.cs`, `Runtime/Transit/TransitProtocolParser.cs` | DEFECT FOUND / FIXED | Concrete issue classified as deterministic test defect (not production): interoperability fallback test emitted zlib-compressed 203 line before reading compressed `MODE STREAM`, causing race-dependent socket-abort/IO failure instead of intended unsupported-compression fallback signal. Fixed test server ordering/handshake emulation; production semantics unchanged and strict. |
| MySqlNntpAccountSnapshotProvider refresh/publication boundary | `Runtime/Accounts/MySqlNntpAccountSnapshotProvider.cs` | CLOSED | Batch 2 audited: startup provisioning guard, snapshot atomic publication, overlap-suppression, cancellation and refresh failure semantics reviewed; no concrete defect established in this pass. |

## Batch Notes
- This ledger is initialized from current repository/test state and prior in-session verified evidence.
- Each subsystem in the active batch will be updated with: closure/skip reason, defect details (if any), fix files, regression tests, and validation outcomes.

## Batch 3 Target Subsystems (Initialized)
| Subsystem | Production Paths | Initial Status | Batch 3 Notes |
|---|---|---|---|
| TransitPublisher runtime ownership boundary | `Runtime/Transit/TransitPublisher.cs` | CLOSED | Batch 3 audited: submission admission/cancellation ordering, bounded-channel backpressure, per-slot reconnect gate ownership, stale-connection replacement, in-flight completion semantics, queued/submitted/accepted/rejected/ambiguous metrics accounting, and disposal/task ownership reviewed; no concrete defect established in this pass. |
| TransitPublisherStartupInitializer ownership/lifecycle boundary | `Runtime/Transit/TransitPublisherStartupInitializer.cs` | CLOSED | Batch 3 audited: startup blocking initialization ownership, cancellation/failure propagation from publisher initialize path, and shutdown disposal handoff semantics reviewed; no concrete defect established in this pass. |
| ShutdownCoordinator integration boundary | `Runtime/Shutdown/ShutdownCoordinator.cs` | CLOSED | Batch 3 audited: graceful/forced state transitions, timer escalation races, cancellation publication ordering, disposal races, and host-stop integration path via HostLifetimeCoordinator reviewed; no concrete defect established in this pass. |
| ConfigurationFingerprintService parity/canonicalization boundary | `Startup/Configuration/ConfigurationFingerprintService.cs` | CLOSED | Batch 3 audited: deterministic fingerprint construction, connection-string sanitization fallback semantics, sensitive-key exclusion, and canonical ordering/parity behavior reviewed; no concrete defect established in this pass. |
| ServiceLifecycle integration boundary | `Runtime/Lifecycle/ServiceLifecycle.cs` | CLOSED | Batch 3 audited: transition validity matrix, observer isolation/reentrancy guard, concurrent transition blocking during notifications, and monotonic duration tracking reviewed; no concrete defect established in this pass. |
| MySqlConnectionStringUtilities canonicalization/diagnostics boundary | `Configuration/MySqlConnectionStringUtilities.cs` | CLOSED | Batch 3 audited: dual-path alias ambiguity handling, malformed-but-parseable rejection behavior, typed pool-size parsing semantics, and canonical extraction consistency reviewed; no concrete defect established in this pass. |
| ValidationLogging sanitization/severity boundary | `Startup/Validation/ValidationLogging.cs` | CLOSED | Batch 3 audited: error/warning severity mapping, structured field logging paths, and reliance on upstream sanitized validation messages across startup and command flows reviewed; no concrete defect established in this pass. |

## Batch 4 Target Subsystems (Initialized)
| Subsystem | Production Paths | Initial Status | Batch 4 Notes |
|---|---|---|---|
| TransitConnection deep ownership/race boundary | `Runtime/Transit/TransitConnection.cs` | CLOSED | Batch 4 audited: initialization/negotiation (CAPABILITIES, STARTTLS, COMPRESS, MODE STREAM), response-loop ownership, write-gate concurrency, in-flight Message-ID tracking, cancellation timing paths, malformed/late response handling, outstanding ambiguous completion, and disposal interaction reviewed; no concrete defect established in this pass. |
| NntpAccountSnapshotStartupInitializer boundary | `Runtime/Accounts/NntpAccountSnapshotStartupInitializer.cs` | CLOSED | Batch 4 audited: startup ordering enforces provisioning -> initial snapshot load -> publication before startup completion; provisioning/load failures and cancellation propagate hard startup failure; stop path is intentional no-op with snapshot ownership retained by provider for process lifetime; no concrete defect established in this pass. |
| StartupValidationPipeline boundary | `Startup/Validation/StartupValidationPipeline.cs` | CLOSED | Batch 4 audited: cancellation pre-check contract, configuration-before-dependency ordering, runtime snapshot build gating, and startup/validate path parity reviewed; no concrete defect established in this pass. |
| DependencyProbeRunner boundary | `Startup/Validation/DependencyProbeRunner.cs` | CLOSED | Batch 4 audited: deterministic probe fan-out (database/cloudflare/transit), Task.WhenAll fan-in aggregation, cancellation propagation, and merged-result semantics reviewed; no concrete defect established in this pass. |
| TransitServerDependencyProbe boundary | `Startup/Validation/TransitServerDependencyProbe.cs` | CLOSED | Batch 4 audited: greeting/CAPABILITIES/STARTTLS/TLS auth/capability refresh/STREAMING+MODE STREAM/QUIT negotiation path, malformed status handling, cancellation, strict TLS validation, and sanitized failure classification reviewed; no concrete defect established in this pass. |
| RuntimeSnapshotFactory boundary | `Startup/Configuration/RuntimeSnapshotFactory.cs` | CLOSED | Batch 4 audited: validated input canonicalization into immutable runtime options, directory validation integration, defaults/normalization and startup handoff behavior reviewed; no concrete defect established in this pass. |
| ControlPlaneService boundary | `ControlPlane/ControlPlaneService.cs` | CLOSED | Batch 4 audited: hosted-service start/execute/stop lifecycle, refresh cadence and overlap behavior, cancellation handling, and refresh-failure containment with snapshot provider interaction reviewed; no concrete defect established in this pass. |
| HostLifetimeCoordinator boundary | `Startup/Hosting/HostLifetimeCoordinator.cs` | CLOSED | Batch 4 audited: host start/wait/shutdown sequencing, readiness publication gating, application-start/stopping callbacks, lifecycle transition race handling, shutdown-coordinator signaling, and systemd interaction reviewed; no concrete defect established in this pass. |

## Batch 5 Target Subsystems (Initialized)
| Subsystem | Production Paths | Initial Status | Batch 5 Notes |
|---|---|---|---|
| ConfigurationValidator cross-boundary parity | `Startup/Configuration/ConfigurationValidator.cs` | DEFECT FOUND / FIXED | Batch 5 audited: command/startup/runtime parity reviewed across StartupValidationPipeline and RuntimeSnapshotFactory. Defect fixed: null `BackFiller:Shutdown` path could throw during validation (`ValidateAnnotatedObject(backFiller.Shutdown, ...)`) instead of deterministic config diagnostics. Fixed with null-guarded shutdown object validation; regression added. |
| BackFillerOptions deep validation semantics | `Configuration/BackFillerOptions.cs` | CLOSED | Batch 5 audited deep validator branches (DataAnnotations + BindAddress/Identity/RabbitMQ/Transit/LetsEncrypt/Shutdown cross-field invariants, defaults, whitespace/range semantics, and runtime snapshot parity). No additional concrete production defect established in this pass beyond subsystem #1 guard fix path. |
| ConnectionStringsOptions + MySql utilities parity | `Configuration/ConnectionStringsOptions.cs`, `Configuration/MySqlConnectionStringUtilities.cs` | CLOSED | Batch 5 audited jointly with runtime/fingerprint/provisioning call paths (`ConfigurationValidator`, `ConfigurationFingerprintService`, `DatabaseDependencyProbe`, `MySqlNntpAccountSnapshotProvider`). Canonical alias interpretation, ambiguity rejection, pool-size parity, and server-vs-database builder behavior are consistent with current implemented startup/runtime model; no concrete defect established in this pass. |
| ValidateStartupCommandHandler startup parity | `Startup/Commands/ValidateStartupCommandHandler.cs` | CLOSED | Batch 5 audited against Program startup and StartupValidationPipeline: same dependency timeout (`5s`), same configuration/dependency ordering and exit-code semantics (`0/2/3/1` mapped through command path), with expected command-only differences (sync wait + console output path). No concrete defect established in this pass. |
| ValidateConfigCommandHandler snapshot/diagnostics parity | `Startup/Commands/ValidateConfigCommandHandler.cs` | CLOSED | Batch 5 audited against startup configuration/snapshot path: uses same connection-string + BackFiller validators and same runtime snapshot factory gating before success, preserving parity with startup config acceptance. Exit-code and diagnostics behavior is deterministic for command semantics; no concrete defect established in this pass. |
| DatabaseDependencyProbe exception/cancellation taxonomy | `Startup/Validation/DatabaseDependencyProbe.cs` | CLOSED | Batch 5 audited deeper exception taxonomy, cancellation propagation, timeout-linked token ownership, command/connection disposal, and sanitized diagnostics. Generic and MySqlException paths preserve secret-safe output and startup hard-failure semantics without introducing independent policy timeouts beyond caller-supplied timeout budget. No concrete defect established in this pass. |
| CloudflareDependencyProbe API failure classification | `Startup/Validation/CloudflareDependencyProbe.cs` | CLOSED | Batch 5 audited Cloudflare API failure mapping, malformed/unsuccessful responses, cancellation/timeout behavior, and exception sanitization. Probe remains fail-closed with deterministic sanitized categories (auth/access/not-found/api-failed), no token leakage, and no retry/timeout policy divergence beyond caller timeout budget. No concrete defect established in this pass. |
| TransitPublisher + TransitConnection high-concurrency integration | `Runtime/Transit/TransitPublisher.cs`, `Runtime/Transit/TransitConnection.cs` | DEFECT FOUND / FIXED | Batch 5 high-concurrency audit found admission-gating defect: `TransitPublisher.PublishAsync` rejected submissions based on publisher-level `_state` (slot-0 driven), causing false `Unavailable` outcomes in multi-slot scenarios where non-primary slots remained viable. Fixed by gating on disposal/worker availability instead of slot-0 state; added deterministic regression (`PublishAsync_WhenPrimarySlotFaultedButSecondarySlotHealthy_StillAdmitsAndPublishes`). Focused transit suites validated. |
| ProcessBootstrapper global fault-handler semantics | `Startup/ProcessBootstrapper.cs` | CLOSED | Batch 5 audited process-wide bootstrap behavior: culture/bootstrap logger setup, global unhandled + unobserved task handler registration semantics, fail-safe flush/exception swallowing boundaries, and Program.Main lifecycle integration ordering. Current behavior preserves terminal-failure visibility without swallowing main-path exceptions; no concrete defect established in this pass. |
| SystemdNotifier lifecycle/notification idempotency | `Startup/Hosting/SystemdNotifier.cs` | CLOSED | Batch 5 audited READY/STOPPING/STATUS paths, status normalization/truncation, throttle and library-availability state handling, repeated invocation idempotency, and HostLifetimeCoordinator call-site interaction. Notification failures remain debug-level/no-op for app health semantics as intended; no concrete defect established in this pass. |

## Batch 6 Target Subsystems (Initialized)
| Subsystem | Production Paths | Initial Status | Batch 6 Notes |
|---|---|---|---|
| MySqlNntpAccountSnapshotProvider lifecycle boundary | `Runtime/Accounts/MySqlNntpAccountSnapshotProvider.cs` | CLOSED | Batch 6 audited: startup provisioning contract (CREATE DATABASE/TABLE IF NOT EXISTS), authoritative schema/table name (`nntpbackfilleraccounts`), strict row mapping (GUID/keepalive/usessl), refresh overlap gate, cancellation-before-publication check, atomic snapshot swap, and stale-snapshot preservation on refresh failure reviewed across startup initializer + control-plane refresh consumers; no concrete defect established in this pass. |
| TransitPublisherStartupInitializer ownership boundary | `Runtime/Transit/TransitPublisherStartupInitializer.cs` | CLOSED | Batch 6 audited with TransitPublisher + host composition call paths: startup blocks on `InitializeAsync` and propagates cancellation/failure, shutdown disposes publisher through single ownership path, and disposal-startup races remain shutdown-safe because publisher guards `_disposeRequested` and tears down partially initialized connections before publish worker activation. Repeated stop/dispose path remains idempotent at current implementation boundary; no concrete defect established in this pass. |
| ShutdownCoordinator integration/race boundary | `Runtime/Shutdown/ShutdownCoordinator.cs` | CLOSED | Batch 6 re-audited with HostLifetimeCoordinator integration and concurrency suites: graceful/forced transition monotonicity, reason first-writer precedence, timer-driven escalation, cancel-outside-lock callback safety, disposal races, and repeated signal idempotency remain consistent with current runtime lifecycle behavior. Cancellation-source ownership and cleanup paths are explicit and race-tolerant; no concrete defect established in this pass. |
| ControlPlaneService refresh/failure boundary | `ControlPlane/ControlPlaneService.cs` | CLOSED | Batch 6 audited current implemented control-plane scope: startup barrier semantics, heartbeat-driven refresh cadence (60s via 30s ticks), cancellation/shutdown exit behavior, prolonged refresh-failure containment with warning telemetry, and snapshot-provider interaction preserving stale snapshots on failures. Refresh work is serialized (no overlap) and naturally backpressured when refresh exceeds cadence. No concrete defect established in this pass. |
| SerilogConfigurator bootstrap/runtime transition boundary | `Startup/Logging/SerilogConfigurator.cs` | CLOSED | Batch 6 audited bootstrap-to-runtime logger swap with ProcessBootstrapper/Program integration: validated minimum-level parsing parity, sink construction failure propagation, swap ordering, bootstrap disposal fallback, and shutdown flush ownership (`Log.CloseAndFlushAsync` in Program finally). Current path avoids configuration-secret logging at this boundary and preserves terminal failure visibility on sink setup errors; no concrete defect established in this pass. |
| HostComposer DI ownership boundary | `Startup/Hosting/HostComposer.cs` | CLOSED | Batch 6 audited DI composition graph and ownership boundaries: singleton registrations (`BackFillerRuntimeOptions`, `TimeProvider`, `ServiceLifecycle`, `ShutdownCoordinator`, `MySqlNntpAccountSnapshotProvider`, `TransitPublisher`) plus hosted service registrations (`NntpAccountSnapshotStartupInitializer`, `TransitPublisherStartupInitializer`, `ControlPlaneService`) are singular and intentional. Runtime snapshot handoff and host-timeout mapping are consistent with startup validation model; no duplicate/circular lifetime defect established in this pass. |
| DumpConfigCommandHandler sanitization boundary | `Startup/Commands/DumpConfigCommandHandler.cs` | CLOSED | Batch 6 audited as security boundary across command parser/dispatcher/executor flow: output scope is constrained to `BackFiller*` and `ConnectionStrings*`, cleartext is strict allowlist-only, all other non-empty values render `[REDACTED]`, nulls are excluded, and key ordering is deterministic. Exit semantics are deterministic (`0` success, `1` when configuration missing). No concrete plaintext-secret exposure defect established in this pass. |
| ValidationLogging severity/sanitization boundary | `Startup/Validation/ValidationLogging.cs` | CLOSED | Batch 6 audited all implemented callers (`Program` startup validation path). Severity mapping is deterministic (errors→Error, warnings→Warning), formatting is stable, and this component forwards pre-sanitized validation/dependency diagnostics without injecting raw exception text at this boundary. `--validate-config`/`--validate-startup` command paths intentionally use direct console emission and do not invoke ValidationLogging. No concrete defect established in this pass. |

## Batch 6 Validation Summary
- Focused subsystem validations: static/call-graph + targeted boundary tests reviewed during each subsystem audit; no new production defects confirmed in Batch 6.
- Full test project: `VectorNNTP.BackFiller.Tests` -> 914/914 passed.
- Full solution build: successful.
- Practical startup validation: `dotnet run --project VectorNNTP.BackFiller/VectorNNTP.BackFiller.csproj -- --validate-config` passed (exit code 0).
- Practical graceful shutdown validation: practical host run reached steady state and performed graceful shutdown on stop signal with final app exit `ExitCode=0` in logs.

## Batch 7 Target Subsystems (Initialized)
| Subsystem | Production Paths | Initial Status | Batch 7 Notes |
|---|---|---|---|
| TransitConnection deep protocol/ownership integration | `Runtime/Transit/TransitConnection.cs` | DEFECT FOUND / FIXED | Batch 7 cross-boundary audit found disposal-ownership defect: `DisposeAsync` could complete without canceling/joining response loop or resolving pending submissions when server kept socket open but withheld TAKETHIS response, leaving admitted publish tasks unresolved under shutdown/dispose races. Fixed by canceling and awaiting response loop during dispose, resolving remaining pending submissions as `Ambiguous`, and disposing loop CTS deterministically. Regression added: `DisposeAsync_WhenSubmissionAwaitsTakethisResponse_CompletesSubmissionAsAmbiguous`; focused transit suites validated. |
| TransitProtocolParser framing/parse contract boundary | `Runtime/Transit/TransitProtocolParser.cs` | CLOSED | Batch 7 audited parser independently and at TransitConnection integration boundary: newline framing over PipeReader, CRLF trimming, EOF/oversized-line hard failures, strict status-line/code separator parsing, capability multiline terminator enforcement, and STREAM/STREAMING alias semantics are interpreted consistently by connection negotiation/response loop paths. Malformed TAKETHIS response lines correctly fault connection and complete outstanding submissions as ambiguous through consumer behavior. No concrete defect established in this pass. |
| TransitPublishContracts producer/consumer status semantics | `Runtime/Transit/TransitPublishContracts.cs` | CLOSED | Batch 7 audited all status producers/consumers across TransitConnection + TransitPublisher: `Accepted/Rejected/Ambiguous` carry terminal attempt outcomes, `Unavailable` remains pre-attempt/retry-admission signal, `Canceled` represents caller/shutdown cancellation, and `Failed` is confined to duplicate in-flight Message-ID admission on a single connection. Metrics semantics remain consistent (`Submitted` increments on queue admission, outcome counters increment only on terminal connection outcomes). `Queued` is currently unused by runtime producers/consumers but does not create a behavioral mismatch in current implementation. No concrete defect established in this pass. |
| RuntimeSnapshotFactory parity boundary | `Startup/Configuration/RuntimeSnapshotFactory.cs` | CLOSED | Batch 7 parity audit across Program startup, StartupValidationPipeline, ConfigurationValidator, and ValidateConfig command confirms single canonical runtime snapshot build path with consistent defaults/normalization for current implemented fields. Structural validation gates snapshot creation first, then runtime consumers receive immutable validated values (dirs/FQDN/transit/rabbit/shutdown). No validation/runtime divergence or startup/command mismatch established in this pass. |
| ConfigurationFingerprintService parity/canonicalization boundary | `Startup/Configuration/ConfigurationFingerprintService.cs` | DEFECT FOUND / FIXED | Batch 7 parity audit found runtime/fingerprint canonicalization divergence: `BackFiller:DnsSuffix` participates in runtime canonicalization (`CanonicalizeDnsSuffix`) but fingerprinting previously hashed raw values, so semantically equivalent effective configs (e.g., case/trailing-dot/outer-whitespace variants) produced different `ConfigurationId` values. Fixed by canonicalizing non-secret `BackFiller:DnsSuffix` before hashing; regression added: `CalculateConfigurationFingerprint_DnsSuffixCanonicalizationParity_ProducesSameFingerprint`; focused fingerprint suite validated. |
| ValidateConfigCommandHandler startup parity boundary | `Startup/Commands/ValidateConfigCommandHandler.cs` | CLOSED | Batch 7 audited command/startup parity: command path validates connection strings and BackFiller options, then invokes `RuntimeSnapshotFactory.BuildRuntimeOptionsSnapshot` using the same canonical snapshot gate as startup. Errors/warnings and exit-code mapping remain consistent with current implemented startup semantics; no concrete defect established in this pass. |
| ValidateStartupCommandHandler startup parity boundary | `Startup/Commands/ValidateStartupCommandHandler.cs` | CLOSED | Batch 7 audited startup-readiness command parity against Program startup: command executes canonical pipeline (`StartupValidationPipeline.ValidateConfigurationAndDependenciesAsync`) and applies deterministic configuration/dependency exit-code mapping consistent with production startup outcomes. No concrete defect established in this pass. |
| StartupValidationPipeline aggregation/parity boundary | `Startup/Validation/StartupValidationPipeline.cs` | CLOSED | Batch 7 audited cancellation pre-check, configuration-first gating, runtime snapshot construction, dependency-probe gating, and tuple result semantics across Program and operational commands. Current implementation preserves expected config/dependency/runtime parity and cancellation behavior; no concrete defect established in this pass. |
| DependencyProbeRunner ownership/fan-in boundary | `Startup/Validation/DependencyProbeRunner.cs` | CLOSED | Batch 7 audited concurrent probe fan-out (`Database`, `Cloudflare`, `TransitServer`) and deterministic fan-in/merge ordering for failures/warnings/errors. Ownership and aggregation semantics remain consistent with caller expectations in startup pipeline and validate-startup command paths; no concrete defect established in this pass. |
| Program top-level orchestration integration boundary | `Program.cs` | CLOSED | Batch 7 audited top-level integration across command dispatch, startup validation, runtime snapshot gating, host composition, lifecycle transitions, cancellation handling, and final exit-code/logging behavior. With the Batch 7 subsystem fixes applied, no additional concrete cross-boundary defect was established in this pass. |

## Batch 8 — Full Implemented-Surface Inventory and Closure Audit

### Batch 8 Inventory Reconciliation Notes
- Inventory baseline: all `*.cs` production source files in `VectorNNTP.BackFiller/` (excluding `bin/obj`), reconciled against prior batch ledger entries.
- Newly ledgered production areas (previously not explicitly represented as subsystems): `BuildInfo.cs`, `Startup/BuildInfoService.cs`, `Startup/Validation/ValidationResults.cs`, `Runtime/Accounts/NntpAccountSnapshot.cs`, `Startup/Commands/DiagnosticsCommandHandler.cs`, `Startup/Commands/HelpCommandHandler.cs`, `Startup/Commands/VersionCommandHandler.cs`, `Startup/Commands/OperationalCommand.cs`.
- Dead/unused implementation evidence in current architecture (pre-cleanup):
  - `Startup/Shutdown/ShutdownPolicyProvider.cs` type had no production call site.
  - `DiagnosticsCommandHandler.GetShutdownPolicySummary(...)` had no call site.
  - `BuildInfoService.RegisterBuildInfo(...)`, `GetCompactVersionString()`, and `GetBuildInfoJson()` had no call sites.

## Final Audit Closure Batch — Dead/Unused Production Code Disposition

### Reachability Analysis (Repository-Wide)
| Item | Reachability Class | Evidence Summary | Disposition |
|---|---|---|---|
| `Startup/Shutdown/ShutdownPolicyProvider.cs` | E. DEFINITELY DEAD | No production references, no test references, no DI registration, no reflection/config activation paths found; runtime shutdown policy uses validated runtime snapshot path (`Program` -> `StartupValidationPipeline` -> `RuntimeSnapshotFactory` -> `HostComposer` / `HostLifetimeCoordinator`). | Removed file. |
| `DiagnosticsCommandHandler.GetShutdownPolicySummary(...)` | E. DEFINITELY DEAD | Method has no call sites in production/tests and no reflective invocation paths. | Removed method. |
| `BuildInfoService.RegisterBuildInfo(...)` | E. DEFINITELY DEAD | No call sites in production/tests; no DI activation path reaches it. | Removed method. |
| `BuildInfoService.GetCompactVersionString()` | E. DEFINITELY DEAD | No call sites in production/tests and no reflective invocation paths. | Removed method. |
| `BuildInfoService.GetBuildInfoJson()` | E. DEFINITELY DEAD | No call sites in production/tests and no reflective invocation paths. | Removed method. |
| `GlobalUsings.cs` | D. SUPPORT/BUILD INFRASTRUCTURE | Provides global namespace import used across compilation units (`global using VectorNNTP.Backfiller.Startup.Validation;`). | Retained. |
| `Properties/AssemblyInfo.cs` | D. SUPPORT/BUILD INFRASTRUCTURE | Provides `InternalsVisibleTo("VectorNNTP.BackFiller.Tests")` for test access to internals. | Retained. |

### Dead Code Cleanup Applied
- Removed file: `VectorNNTP.BackFiller/Startup/Shutdown/ShutdownPolicyProvider.cs`
- Removed method: `DiagnosticsCommandHandler.GetShutdownPolicySummary(...)`
- Removed methods: `BuildInfoService.RegisterBuildInfo(...)`, `BuildInfoService.GetCompactVersionString()`, `BuildInfoService.GetBuildInfoJson()`

### Test Impact Analysis
- No tests depended on removed dead implementation.
- Existing command/startup/info/account/fingerprint test paths remained valid after cleanup.

### Batch 8 Target Subsystems
| Subsystem | Production Paths | Classification | Batch 8 Status | Batch 8 Notes |
|---|---|---|---|---|
| Build metadata resolution + startup logging boundary | `BuildInfo.cs`, `Startup/BuildInfoService.cs` | CLOSED — CROSS-BOUNDARY AUDITED | CLOSED | Audited startup initialization (`Program` -> `BuildInfoService.InitializeBuildInfo`), runtime fingerprint logging call site (`HostComposer`), metadata extraction/fallback paths (commit/dirty/version), and exception behavior. No concrete production defect established in this pass. |
| Informational operational commands boundary | `Startup/Commands/OperationalCommand.cs`, `Startup/Commands/HelpCommandHandler.cs`, `Startup/Commands/VersionCommandHandler.cs`, `Startup/Commands/DiagnosticsCommandHandler.cs` | CLOSED — CROSS-BOUNDARY AUDITED | CLOSED | Audited parser/dispatcher/executor integration and informational command exit semantics (`0`), including configuration-independent command paths. No concrete production defect established in this pass. |
| Validation result contract boundary | `Startup/Validation/ValidationResults.cs` | CLOSED — CROSS-BOUNDARY AUDITED | CLOSED | Audited `ConfigurationValidationResult` / `DependencyValidationResult` construction and `IsValid` semantics across pipeline, probes, command handlers, and `Program`. No concrete contract mismatch established in this pass. |
| Account snapshot immutable contract boundary | `Runtime/Accounts/NntpAccountSnapshot.cs` | CLOSED — CROSS-BOUNDARY AUDITED | CLOSED | Audited immutable account/snapshot-state contracts and publication/consumption boundaries in `MySqlNntpAccountSnapshotProvider` and related tests. No concrete production defect established in this pass. |
| Shutdown policy provider implementation reachability | `Startup/Shutdown/ShutdownPolicyProvider.cs` | DEAD / UNUSED IMPLEMENTATION | CLOSED (classified) | Type is currently unreferenced by production call graph (`Program` -> `StartupValidationPipeline` -> `RuntimeSnapshotFactory` -> `HostComposer`/`HostLifetimeCoordinator`); runtime shutdown policy currently flows via validated runtime snapshot options, not this provider. |

## Complete Implemented-Surface Inventory (Batch 8)

### CLOSED — DEEPLY AUDITED
- `Program.cs`
- `Startup/Hosting/HostLifetimeCoordinator.cs`
- `Runtime/Shutdown/ShutdownCoordinator.cs`
- `Runtime/Transit/TransitPublisher.cs`
- `Runtime/Transit/TransitConnection.cs`
- `Runtime/Transit/TransitProtocolParser.cs`
- `Startup/Configuration/ConfigurationValidator.cs`
- `Configuration/BackFillerOptions.cs`
- `Configuration/ConnectionStringsOptions.cs`
- `Configuration/MySqlConnectionStringUtilities.cs`
- `Startup/Configuration/RuntimeSnapshotFactory.cs`
- `Startup/Validation/StartupValidationPipeline.cs`
- `Startup/Validation/DependencyProbeRunner.cs`
- `Startup/Validation/DatabaseDependencyProbe.cs`
- `Startup/Validation/CloudflareDependencyProbe.cs`
- `Startup/Validation/RabbitMqDependencyProbe.cs`
- `Startup/Validation/TransitServerDependencyProbe.cs`
- `Runtime/Accounts/MySqlNntpAccountSnapshotProvider.cs`
- `Runtime/Accounts/NntpAccountSnapshotStartupInitializer.cs`
- `Runtime/Transit/TransitPublisherStartupInitializer.cs`
- `ControlPlane/ControlPlaneService.cs`
- `Startup/Hosting/HostComposer.cs`
- `Startup/Hosting/SystemdNotifier.cs`
- `Startup/Logging/SerilogConfigurator.cs`
- `Startup/ProcessBootstrapper.cs`
- `Startup/Commands/ValidateConfigCommandHandler.cs`
- `Startup/Commands/ValidateStartupCommandHandler.cs`
- `Startup/Commands/DumpConfigCommandHandler.cs`
- `Startup/Configuration/ConfigurationFingerprintService.cs`
- `Configuration/OperationalDirectoryValidator.cs`
- `Startup/ExitCodePolicy.cs`

### CLOSED — CROSS-BOUNDARY AUDITED
- `BuildInfo.cs`
- `Startup/BuildInfoService.cs`
- `Startup/Validation/ValidationResults.cs`
- `Runtime/Accounts/NntpAccountSnapshot.cs`
- `Runtime/Transit/TransitPublishContracts.cs`
- `Startup/Commands/OperationalCommandParser.cs`
- `Startup/Commands/OperationalCommandDispatcher.cs`
- `Startup/Commands/OperationalCommandExecutor.cs`
- `Startup/Commands/OperationalCommand.cs`
- `Startup/Commands/DiagnosticsCommandHandler.cs`
- `Startup/Commands/HelpCommandHandler.cs`
- `Startup/Commands/VersionCommandHandler.cs`
- `Configuration/BackFillerRuntimeOptions.cs`

### PARTIALLY AUDITED
- None.

### NOT AUDITED
- None.

### FUTURE / NOT IMPLEMENTED
- RabbitMQ work distribution runtime
- Source-provider runtime pools / dynamic provider reconciliation
- Grabber/backfiller workers
- Article searching/downloading/validation/header-injection/recovery/replay
- CHECK and future provider health/recovery architecture

### SUPPORT / BUILD INFRASTRUCTURE
- `GlobalUsings.cs` (global using for shared startup validation namespace)
- `Properties/AssemblyInfo.cs` (`InternalsVisibleTo("VectorNNTP.BackFiller.Tests")` for test access)

## Final Closure Validation Summary
- Focused/broader validations:
  - `ProgramCommandLineTests`: 26/26 passed.
  - `NntpAccountSnapshotStateTests`: 1/1 passed.
  - `ConfigurationFingerprintTests`: 104/104 passed.
- Full test project: `VectorNNTP.BackFiller.Tests` -> 916/916 passed.
- Full solution build: successful.
- Practical startup validation: `dotnet run --project VectorNNTP.BackFiller/VectorNNTP.BackFiller.csproj -- --validate-config` passed (exit code 0).
- Practical normal startup: host reached steady state (`Application started` milestone observed).
- Practical graceful shutdown: shutdown signal processed and final app exit logged as `ExitCode=0`.

## Overall Production Audit Status
COMPLETE

No substantial implemented production subsystem remains unaudited, and no known concrete production defect remains.
