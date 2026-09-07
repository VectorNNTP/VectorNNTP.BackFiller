# Forensic Engineering Review — VectorNNTP.BackFiller

**Review date:** 2026-09-07  
**Repository:** VectorNNTP/VectorNNTP.BackFiller  
**Reviewed source commit:** `b0f00e67a6b27590ee278c56d46eafc8608658f0`  
**Review checkout:** `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller`  
**Deliverable:** Report only; this document does not implement any proposed remediation.

The original investigation was read-only. The user subsequently authorized committing this report and opening a documentation-only PR.
Source, tests, configuration, workflows, dependencies, and existing documentation were not changed to implement findings.
The report records the completed investigation, not a new investigation or an abridged executive-only replacement.

## Reading guide and evidence standard

All source locations refer to the reviewed commit, not to future fixes. Absolute paths identify the review checkout; symbols and line ranges
identify the relevant code within those files. Findings are independently actionable even where they share a source file.

- **CONFIRMED:** The stated defect follows from inspected implementation and a reachable scenario. This does not mean it was executed.
- **STRONGLY_SUPPORTED:** Implementation supports the risk, but a platform, dependency, deployment, or timing condition remains unverified.
- **ARCHITECTURAL_CONCERN:** An established design/contract issue whose intended policy or production impact needs clarification.
- **OPTIMIZATION_OPPORTUNITY / NEEDS_MEASUREMENT:** A candidate improvement, not a demonstrated speedup or defect.
- **OBSERVATION:** Relevant evidence without a demonstrated defect.

Severity concerns consequence, not certainty. Confidence/type concerns evidence, not impact. No numerical confidence percentages are used.
“Measurement required: NO” means measurement is not needed to establish the source-level issue; regression validation is still required.
“YES” identifies findings needing runtime/platform verification or cost measurement before quantifying impact.

No builds, tests, benchmarks, exploits, live dependency probes, or advisory-database queries were performed as part of the investigation.
Proposed validation strategies below are not completed test results. No runtime performance numbers are claimed.
Documentation-delivery checks on this report are separate from validation of the reviewed application.

### Contents

1. [Executive Summary](#1-executive-summary)
2. [Overall Architecture Assessment](#2-overall-architecture-assessment)
3. [Finding Summary Table](#3-finding-summary-table)
4. [Critical/High Findings](#4-criticalhigh-findings)
5. [Medium Findings](#5-medium-findings)
6. [Low/Informational Findings](#6-lowinformational-findings)
7. [Performance & Allocation Assessment](#7-performance--allocation-assessment)
8. [Memory & GC Assessment](#8-memory--gc-assessment)
9. [Concurrency & Lifecycle Assessment](#9-concurrency--lifecycle-assessment)
10. [Networking/Protocol Assessment](#10-networkingprotocol-assessment)
11. [RabbitMQ Assessment](#11-rabbitmq-assessment)
12. [Retention Assessment](#12-retention-assessment)
13. [Transit Assessment](#13-transit-assessment)
14. [Security Assessment](#14-security-assessment)
15. [Error/Failure Assessment](#15-errorfailure-assessment)
16. [Observability Assessment](#16-observability-assessment)
17. [Maintainability/Architecture Assessment](#17-maintainabilityarchitecture-assessment)
18. [Testing/Benchmark Assessment](#18-testingbenchmark-assessment)
19. [CI/Dependency Assessment](#19-cidependency-assessment)
20. [Prioritized Remediation Roadmap](#20-prioritized-remediation-roadmap)
21. [Measurement/Profiling Plan](#21-measurementprofiling-plan)
22. [Repository Review Coverage](#22-repository-review-coverage)
23. [Review Limitations and Unknowns](#23-review-limitations-and-unknowns)
24. [Final Engineering Assessment](#24-final-engineering-assessment)

## 1. Executive Summary

The application contains substantial engineering investment: explicit lifecycle states, configuration snapshots, generation-aware RabbitMQ
channels, bounded delivery admission, retained-payload leases, protocol validation, structured logging, and specialized tests/benchmarks.
These are useful foundations, not superficial abstractions.

The serious problems arise where those mechanisms compose:

- Consumer-generation cancellation can terminate the only article-processing loop and stop the host.
- RabbitMQ recovery can strand consumers; cancellation during batch retirement can leave never-completing retirement tasks.
- Transit negotiation failures can permanently remove workers while admission stays open.
- Transit state, ownership counters, and completion publication are not atomic.
- A Transit response watchdog does not interrupt a blocked socket write.
- A listener ACK timer can act on a later request reusing the same numeric identifier, threatening lease lifetime.
- Production listener shutdown cancels established transfers instead of invoking the session's graceful-drain mechanism.
- Omitted bind configuration means wildcard listening but empty DNS desired state, permitting deletion of service address records.
- Connection-count limits do not bound stalled-client occupancy duration.

Additional confirmed defects concern NNTP lease accounting, keepalive retirement, idle-token accumulation, RabbitMQ routing/disposition,
parser-stage compatibility, resource limits, certificate DNS handling, configuration, deployment, and validation tooling.

The report contains **10 HIGH, 34 MEDIUM, 5 LOW, and 1 INFORMATIONAL findings**. Several entries group tightly related manifestations,
not independent claims of equal certainty. No CRITICAL finding was justified. HIGH security availability impact is conditional on exposure.
The retention-generation issue was explicitly downgraded to an architectural concern after contract review.

**Production assessment:** The implementation should not yet be treated as proven for unattended, very-high-throughput operation.
Correctness, bounded failure recovery, and measurement validity should precede concurrency expansion. This is not a claim that every deployment
currently experiences every finding, nor a substitute for reproducing and fixing the individual scenarios.

## 2. Overall Architecture Assessment

### 2.1 Startup and readiness

The reviewed path is bootstrap/command dispatch → configuration binding and validation → immutable runtime projection and directory validation
→ baseline dependency checks → DNS reconciliation → certificate availability → host/DI startup → service readiness.

Runtime projection includes operational-directory creation/probes. DNS and ACME introduce externally visible work before hosted execution;
the validity and consistency of their inputs therefore matter before those operations run. H09 and M18 concern this composed boundary.
Readiness is represented by the lifecycle object and systemd notification after host startup; it should not be inferred from DI registration order.

Current source implements mandatory TLS/ACME management. An old `LetsEncrypt.Enabled` input is obsolete, not a supported optional-issuance mode.
Historical wording and stale test names do not override that implementation contract.

### 2.2 Work path and settlement

1. MySQL supplies account snapshots; the control plane constructs/manages provider NNTP sessions.
2. Application-managed RabbitMQ connectivity creates consumer sessions and topology.
3. Consumer callbacks copy admitted borrowed bodies into owned delivery objects.
4. A bounded channel feeds **one production article-processing loop**.
5. The loop acquires an NNTP session, retrieves the article, parses/validates it, and materializes canonical bytes.
6. Retention accepts ownership; Transit admits the Message-ID identity.
7. The service publishes a response and settles the source delivery.
8. Transit later leases retained payloads for external NNTP delivery.
9. The TLS listener serves retained data and observes receipt acknowledgments.
10. Consumer completion, TTL, pressure, and lease release govern logical removal and physical disposal.

Source success means **retention plus Transit admission**, not remote Transit acceptance. That distinction is an actual contract, not itself a defect.
Likewise, retaining an article does not guarantee it survives TTL/pressure indefinitely.

### 2.3 Concurrency and ownership domains

| Domain | Actual boundary | Important qualification |
|---|---|---|
| RabbitMQ connectivity | One application-managed connection slot | Reserved pool settings do not create a pool |
| Consumer delivery | Multiple sessions plus bounded application delivery queue | Broker/client prefetch is a separate boundary |
| Article processing | One serial hosted loop | Provider connection count is not article concurrency |
| NNTP acquisition | Per-account sessions/lease ownership | Keepalive and control-plane refresh run independently |
| Retention | Central payload owner with read leases | Logical removal differs from physical disposal |
| Transit | Identity queue, connection workers, pipeline depth | Staging and leases add resource lifetimes outside queue count |
| Listener | Per-session request/byte limits plus global connection count | Count limits do not bound connection tenure |
| CPU | Parsing/materialization within current processing path | No independent production CPU worker pool was established |

The main architectural weakness is that ownership is represented by different IDs, flags, counters, and task-completion sources at each boundary.
Several transitions update only a subset of those representations.

## 3. Finding Summary Table

Types in this table are shortened: C = CONFIRMED, S = STRONGLY_SUPPORTED, A = ARCHITECTURAL_CONCERN, O = OBSERVATION.
Detailed entries contain qualifications, tests, consequences, and proposed validation.

| ID | Severity | Type | Title |
|---|---|---|---|
| H01 | HIGH | C/S | Delivery cancellation escapes the only processor; fatal stop can look successful |
| H02 | HIGH | C/S | Consumer recovery can remain permanently Retiring |
| H03 | HIGH | C | Interrupted retirement batch abandons unstarted operations |
| H04 | HIGH | C | Transit negotiation failure permanently removes a worker |
| H05 | HIGH | C | Transit state, counters, and completion publication diverge |
| H06 | HIGH | S | Response watchdog does not interrupt blocked Transit writes |
| H07 | HIGH | C | Stale listener timer terminalizes a replacement request |
| H08 | HIGH | C | Production graceful listener shutdown aborts transfers |
| H09 | HIGH | C | Omitted bind address can delete published DNS addresses |
| H10 | HIGH | S | Stalled clients can occupy all listener capacity |
| M01 | MEDIUM | C | Idle retirement decrements unrelated active-lease accounting |
| M02 | MEDIUM | C | Successful keepalive loses deferred retirement |
| M03 | MEDIUM | C | Idle keepalive tokens accumulate without a bound |
| M04 | MEDIUM | C | Account managers escape initialization/retirement cleanup |
| M05 | MEDIUM | C | Shutdown signaling bypasses RabbitMQ disposal |
| M06 | MEDIUM | S | Shutdown initiation depends on logging/notification success |
| M07 | MEDIUM | C | Transit response timing inherits idle time and initialization policy |
| M08 | MEDIUM | A | Completion crosses retention admission generations |
| M09 | MEDIUM | A | Serial article processing and first-account selection constrain capacity |
| M10 | MEDIUM | C | Nonreplyable invalid requests can requeue forever |
| M11 | MEDIUM | C | Unroutable responses can cause source settlement |
| M12 | MEDIUM | C | Actual failure responses violate the schema |
| M13 | MEDIUM | C | Acquisition and yEnc validation disagree about unstuffing |
| M14 | MEDIUM | C | Folded semantic headers are rejected |
| M15 | MEDIUM | C | Parser-accepted mixed separators fail materialization |
| M16 | MEDIUM | C | Acquisition/parser limits do not form one resource boundary |
| M17 | MEDIUM | C | Explicit zero prefetch bypasses the declared range |
| M18 | MEDIUM | C/S | Database validation, provisioning, and runtime values disagree |
| M19 | MEDIUM | A | Reserved pool settings impose fictitious configuration constraints |
| M20 | MEDIUM | C | DNS timeout/failure bypasses fallback and quorum |
| M21 | MEDIUM | C | TXT creation omits recovery ownership metadata |
| M22 | MEDIUM | S | Certificate intermediates are not preserved through reload/cloning |
| M23 | MEDIUM | C | Supplied systemd template mismatches publishing |
| M24 | MEDIUM | C | Transit snapshots and percentile fields misrepresent execution |
| M25 | MEDIUM | S | Cache access relies on external network trust |
| B01 | MEDIUM | C | Throughput metrics mix incompatible work/time windows |
| B02 | MEDIUM | C | Benchmark cancellation can leak byte-budget reservations |
| B03 | MEDIUM | S | Harness cancellation/shutdown leaves owned work unjoined |
| B04 | MEDIUM | C | Isolated gate can report PASS after abnormal completion |
| B05 | MEDIUM | C | Benchmark entrypoints violate option contracts |
| B06 | MEDIUM | C | Diagnostic telemetry does not observe its named events |
| B07 | MEDIUM | C | Valid-yEnc parser benchmark fixture is lossy |
| B08 | MEDIUM | C | Build reproducibility depends on ambient SDKs |
| B09 | MEDIUM | C | Success-path validation tests contain invalid ACME fixtures |
| L01 | LOW | C | Production bypasses obsolete-key rejection |
| L02 | LOW | C | Valid MySQL separator syntax becomes an alias conflict |
| L03 | LOW | C | Certificate-directory normalization is inconsistent |
| L04 | LOW | C | Opt-in corpus corruption helper overruns destination capacity |
| L05 | LOW | C | Some evaluated certificate bundles are not disposed |
| L06 | INFORMATIONAL | O | Private-key-formatted artifact needs classification |

## 4. Critical/High Findings

### H01 — Delivery-local cancellation can terminate the only processing loop

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED application propagation; STRONGLY_SUPPORTED host/supervisor consequence.
- **Category:** Cancellation, settlement, service availability.
- **Location:** `RabbitMqArticleProcessingService` processing loop, lines 77–134:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/RabbitMqArticleProcessingService.cs`.
  `SettleAsync`, lines 822–866:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/RabbitMq/RabbitMqBackboneConsumerSession.cs`.
  Host outcome, lines 299–340:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Hosting/HostLifetimeCoordinator.cs`.
- **Problem:** A delivery-generation failure is allowed to terminate the sole hosted processing task.
- **Evidence:** The loop has a `finally` but no per-delivery exception boundary. Successful consumer recreation cancels its own CTS, independently
  of host cancellation. The sink subsequently settles using the canceled operation token. Settlement sets `_settled` before the cancelable gate
  wait; that wait is outside the catch that resets the flag.
- **Execution path:** Consumer recreation → old session cancellation → acquisition/processing cancellation result → result sink → NACK → gate wait.
- **Failure scenario:** T1 processes a delivery; T2 replaces its consumer and cancels the session token; T1 reaches settlement after its early
  cancellation check; gate waiting throws; the exception escapes the only processor.
- **Concrete consequence:** Ordinary broker recovery can become application-wide stop. Default Generic Host background-service failure handling
  stops the host; the normal lifetime path need not rethrow that failure or establish a nonzero exit. `Restart=on-failure` may therefore not restart it.
- **Why existing implementation does not prevent it:** Generation checks prevent stale ACKs, not hosted-task failure. The real sink cancellation
  test expects abandonment but does not verify the hosted loop processes another delivery afterward.
- **Recommended remediation:** Isolate generation-local cancellation and settlement failures while retaining exactly-one ownership. Distinguish
  requested shutdown from fatal background-service termination and establish corresponding process exit semantics.
- **Potential behavioural impact:** Broker replacement should cease stopping the process; genuinely fatal service failures may now exit nonzero.
- **Validation strategy:** Start the actual hosted processor, replace a consumer during processing/settlement, and prove a later delivery completes.
  Separately inject a fatal background failure and verify process exit and supervisor restart behavior.
- **Measurement required:** NO.

### H02 — Consumer recovery can remain permanently Retiring

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED conditional state-machine defect; STRONGLY_SUPPORTED real closed-channel trigger, not executed.
- **Category:** RabbitMQ recovery, rollback, lifecycle.
- **Location:** `StopCoreAsync` 367–399, recreation 634–640, startup 224–226 and 286–352:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/RabbitMq/RabbitMqBackboneConsumerSession.cs`.
- **Problem:** Recovery performs fallible broker cancellation before the local cleanup needed to make the object retryable.
- **Evidence:** Running changes to Retiring before `BasicCancelAsync`. Recovery passes `expectedShutdown:false`, so cancel failure is rethrown before
  cleanup. Start/replacement paths do not repair Retiring. Reconciliation reuses the same object.
- **Execution path:** Consumer shutdown/unregistration or connection replacement → `RecreateConsumerCoreAsync` → `StopCoreAsync` → broker cancel.
- **Failure scenario:** The channel is unusable, cancellation throws, and the consumer remains Retiring; subsequent refresh cannot restart it.
- **Concrete consequence:** Consumer capacity can disappear until process restart.
- **Why existing implementation does not prevent it:** Best-effort shutdown cancellation handling is a different branch. Recovery fakes permit
  cancellation even on closed/disposed channels. A related gap places `BasicQosAsync` after resource assignment but before startup rollback.
- **Recommended remediation:** Separate best-effort broker operations from unconditional local cleanup; expand startup rollback to cover QoS.
  Leave failed starts/recreations in a retryable state without losing admitted-delivery ownership.
- **Potential behavioural impact:** Recovery can retry rather than strand sessions; cleanup may now run after previously escaping failures.
- **Validation strategy:** Inject cancel and QoS failures, inspect owned resources/state, then reconcile successfully on a new channel generation.
- **Measurement required:** NO.

### H03 — Interrupted retirement batches abandon unstarted operations

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED.
- **Category:** Ownership, cancellation, shutdown liveness.
- **Location:** `RetireCapacityAsync` 276–306, reconciliation 518–542, shutdown 637–654:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/RabbitMq/RabbitMqConsumerSessionFactory.cs`.
- **Problem:** Retirement ownership is reserved for an entire batch before operations execute sequentially.
- **Evidence:** All selected sessions leave the active map and receive completion sources. An exception from an early operation skips later operations.
  Each operation's finally completes only its own source. Shutdown awaits previously reserved retirement tasks without cancellation.
- **Execution path:** Account removal/capacity reduction or reconciliation → batch reservation → sequential retirement → shutdown join.
- **Failure scenario:** T1 reserves A/B/C; A blocks draining; T2 cancels; A exits; B/C never start; shutdown later awaits B/C forever.
- **Concrete consequence:** Cleanup ownership is lost and shutdown can hang.
- **Why existing implementation does not prevent it:** Shutdown catches failures for newly executed operations, not unstarted old reservations.
  Tests named “without deadlock” manually reconcile and stop without starting the hosted loop that performs the production shutdown join.
- **Recommended remediation:** Every reserved operation must execute or be explicitly canceled/terminalized after any earlier failure.
- **Potential behavioural impact:** Cancellation no longer abandons later retirements; terminal failure reporting must remain distinguishable from success.
- **Validation strategy:** Start the real hosted consumer service, reserve at least three retirements, cancel the first drain, then assert every
  reservation and shutdown task completes and all removed sessions are disposed.
- **Measurement required:** NO.

### H04 — Transit negotiation failure can permanently terminate a worker

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED.
- **Category:** Worker supervision, connection recovery, admission liveness.
- **Location:** Initialization/cleanup 601–720:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitConnection.cs`.
  Worker 1022–1024, 1159–1270; initialization 1583–1609; classifier 1733–1750:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitPublisher.cs`.
- **Problem:** Failure classification depends on a mutable handshake state that cleanup has already reset.
- **Evidence:** `InitializeAsync` cleans up to Disconnected before rethrow. The classifier requires AwaitingGreeting, CapabilitiesNegotiation,
  or StartingStreaming. The worker's assignment has not completed, so its outer connection-dependent catch also misses the exception.
- **Execution path:** Work demand → create/initialize connection → unacceptable negotiation response → cleanup → failed classification → worker exit.
- **Failure scenario:** A provider passes startup validation but later returns an unacceptable greeting, lacks STREAMING, or rejects MODE STREAM.
- **Concrete consequence:** A worker exits permanently. With one configured slot, admission may stay open with no worker draining work.
- **Why existing implementation does not prevent it:** Startup checks cannot guarantee later connections. Worker-exit handling finalizes disposal,
  not unexpected capacity loss. Negotiation tests inspect the connection exception without verifying publisher recovery.
- **Recommended remediation:** Preserve typed failure provenance independently of current state; supervise worker loss or freeze admission explicitly.
- **Potential behavioural impact:** Negotiation errors become recoverable or explicit service unavailability rather than silent capacity disappearance.
- **Validation strategy:** Reject each handshake stage after successful startup probing and verify bounded recovery or terminal completion of all work.
- **Measurement required:** NO.

### H05 — Transit terminal state, counters, and completion publication can diverge

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED.
- **Category:** Concurrency, resource accounting, exactly-once completion.
- **Location:** Admission/claim 140–210, terminal accounting 355–448:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/GlobalTransitWorkQueue.cs`.
  Registration 706–729, forced terminalization 1472–1528:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitPublisher.cs`.
- **Problem:** Item state does not atomically represent acquired queue/in-flight ownership.
- **Evidence:** Items register before capacity ownership. Claim CAS precedes queued decrement/in-flight increment. Forced terminalization uses
  prior state to decrement ownership and can throw after terminal CAS but before completing the task.
- **Execution path:** Admission/claim overlaps `ForceTerminalizeRemainingWorkAsync`, which is reachable during runtime initialization failure.
- **Failure scenario:** T1 marks Claimed and pauses; T2 terminalizes, attempts an in-flight decrement before T1 incremented it, and throws.
  A registered capacity waiter labeled Queued exposes a corresponding pre-admission mismatch.
- **Concrete consequence:** Accounting underflow, corrupted capacity, and terminal items whose completion tasks never finish.
- **Why existing implementation does not prevent it:** `_claimGate` excludes claimers, not forced terminalization. Normal shutdown's processing
  barrier narrows one overlap but does not remove runtime forced completion.
- **Recommended remediation:** Represent pre-admission state explicitly and make ownership/state transfers indivisible. Do not let invariant
  reporting interrupt mandatory completion publication.
- **Potential behavioural impact:** Correct counter values, admission capacity, and cancellation outcomes; retain terminal-status semantics.
- **Validation strategy:** Deterministically pause registration, reservation, and claim transfer; force completion; reconcile all counters and tasks.
- **Measurement required:** NO.

### H06 — Response watchdog does not interrupt a blocked Transit write

- **Severity:** HIGH.
- **Confidence/type:** STRONGLY_SUPPORTED.
- **Category:** Networking, backpressure, bounded lease lifetime.
- **Location:** Batch write/flush 776–830; watchdog/fault signaling 1255–1322:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitConnection.cs`.
- **Problem:** The response watchdog signals a fault in a different cancellation domain from the blocked writer.
- **Evidence:** `FlushAsync` uses the worker token. Fault signaling cancels the response-loop CTS and completes response signaling but does not
  cancel that write or close the transport.
- **Execution path:** Stage leased batch → socket flush → peer stops reading → watchdog fault → worker remains inside flush.
- **Failure scenario:** A sufficiently large batch fills transport buffers against a nonreading peer; no independent host cancellation occurs.
- **Concrete consequence:** The slot and batch leases remain occupied until shutdown or another transport failure, despite a reported timeout.
- **Why existing implementation does not prevent it:** Withheld-response tests consume payload first; they exercise response wait, not blocked write.
  Shutdown cancellation can release the write, so permanent survival beyond process termination is not claimed.
- **Recommended remediation:** Make connection-fault signaling terminate both read and write ownership; preserve uncertainty for partially sent data.
- **Potential behavioural impact:** Faults recover promptly, but classification must not turn partially transmitted articles into unsafe automatic retries.
- **Validation strategy:** Use a nonreading peer, exceed socket-buffer capacity, and verify bounded worker recovery, completion, and lease release.
- **Measurement required:** YES, for actual blocked-write behavior and recovery latency.

### H07 — Stale listener ACK timer can terminalize a replacement request

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED.
- **Category:** Request identity, concurrent cleanup, pooled-memory lifetime.
- **Location:** `ApplyCompletion`, timeout registration, and terminalization 673–819:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Listener/ListenerProtocolSession.cs`.
- **Problem:** A timeout is associated with a reusable numeric ID rather than the original request instance.
- **Evidence:** AwaitingAck observation and timer registration are separate; timeout terminalization looks up the current request by ID.
- **Execution path:** Found write completion → AwaitingAck observation → receipt ACK/removal → ID reuse → old timer callback.
- **Failure scenario:** T1 observes A awaiting ACK and pauses; T2 ACKs/removes A and admits B under the same ID; T1 installs A's timer;
  A's timeout terminalizes B while B is writing.
- **Concrete consequence:** B's lease can be released while its writer references the payload. If retention logically removed it, the pooled
  buffer may be returned during writing. Missing context can also bypass reservation cleanup.
- **Why existing implementation does not prevent it:** Numeric uniqueness applies only while the old context exists. Some timeout tests reach
  EOF cleanup without establishing actual timer-expiry behavior.
- **Recommended remediation:** Bind registration/cancellation/terminalization to request-instance identity and observe timer-task lifetime.
- **Potential behavioural impact:** Old timers become harmless after ID reuse; wire-visible identifiers need not change.
- **Validation strategy:** Pause A's timer registration, ACK A, reuse its ID for a blocked B write, expire A's timer, and assert B retains ownership.
- **Measurement required:** NO.

### H08 — Production graceful listener shutdown aborts active transfers

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED.
- **Category:** Graceful shutdown, protocol completion, lease ownership.
- **Location:** Cancellation and cleanup 87–125; established session wiring 208–224:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Listener/BackFillerListenerSocketService.cs`.
- **Problem:** Stopping admission and aborting established I/O use the same graceful-shutdown-linked token.
- **Evidence:** The linked token reaches established sessions. Finally closes active clients before joining them. The session's
  `BeginGracefulShutdown` mechanism is not invoked by production socket wiring.
- **Execution path:** Host graceful signal → accept/session token cancellation → client closure → session join.
- **Failure scenario:** T1 sends a Found payload or awaits receipt; T2 begins graceful shutdown; T1 is canceled/closed immediately.
- **Concrete consequence:** Truncated transfers and abandoned ACKs despite configured active-work finishing behavior.
- **Why existing implementation does not prevent it:** A blocked-task shutdown test does not exercise a real TLS transfer or receipt phase.
- **Recommended remediation:** Separate admission cancellation, established-session drain, and forced deadline transport cancellation.
- **Potential behavioural impact:** Shutdown can use the grace period to complete existing transfers; no new work should enter during drain.
- **Validation strategy:** Stop the actual TLS listener during transfer and ACK wait; verify completion within grace and forced closure after expiry.
- **Measurement required:** NO.

### H09 — Omitted bind address can delete service DNS records

- **Severity:** HIGH.
- **Confidence/type:** CONFIRMED.
- **Category:** Configuration consistency, destructive startup side effects.
- **Location:** Null/empty derivation 30–37:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Configuration/BindAddressDnsAddressDeriver.cs`.
  Desired state/deletion 79, 124–135, 348–367:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Validation/CloudflareDnsSynchronizationProbe.cs`.
- **Problem:** Omitted binding means no DNS addresses but wildcard listener endpoints.
- **Evidence:** The deriver returns an empty set; listener construction maps omitted binding to IPv4/IPv6 wildcard sockets. Exact-set DNS
  synchronization deletes existing records absent from the desired set.
- **Execution path:** Omitted bind configuration → runtime snapshot → empty desired addresses → DNS reconciliation → listeners start.
- **Failure scenario:** Existing A/AAAA records are present when this otherwise accepted configuration starts.
- **Concrete consequence:** A listening service becomes undiscoverable through its published name.
- **Why existing implementation does not prevent it:** Explicit wildcard tests and exact-set DNS tests do not cover omitted-input composition.
- **Recommended remediation:** Normalize omitted/empty/wildcard semantics consistently and guard unintended empty destructive reconciliation.
- **Potential behavioural impact:** Startup may reject unusable derivation rather than delete records; valid wildcard startup publishes eligible addresses.
- **Validation strategy:** Compose snapshot and DNS synchronization for omitted, empty, explicit wildcard, and no-eligible-interface cases.
- **Measurement required:** NO.

### H10 — Stalled clients can occupy all listener capacity

- **Severity:** HIGH when untrusted clients can reach the listener.
- **Confidence/type:** STRONGLY_SUPPORTED; static security-specialist review, no exploit executed.
- **Category:** Availability/security, connection admission, slow clients.
- **Location:** Slot/TLS lifetime 167–248:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Listener/BackFillerListenerSocketService.cs`.
  Read/write cancellation 325–335, 658–706:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Listener/ListenerProtocolSession.cs`.
- **Problem:** Connection limits bound count but not how long inactive clients retain slots.
- **Evidence:** Slots are consumed before TLS. Authentication and session I/O receive shutdown-linked cancellation without application-level
  handshake, idle, read, or write deadlines. Receipt timeout begins only after a completed Found transfer.
- **Execution path:** TCP accept → reserve slot → incomplete TLS or idle/stalled session → slot retained until peer closure or shutdown.
- **Failure scenario:** Reachable peers hold enough connections without useful progress to consume the configured cap.
- **Concrete consequence:** Legitimate clients are denied admission; slow transfers can also prolong retention leases.
- **Why existing implementation does not prevent it:** Cap tests release peers explicitly. ACK timers do not protect handshake/idle phases.
  External firewall/load-balancer policies may mitigate exposure but were unavailable.
- **Recommended remediation:** Define handshake, idle, and stalled-transfer deadlines and retain deployment-level admission controls.
- **Potential behavioural impact:** Legitimately slow clients may need configured allowances; enforce progress rather than arbitrary payload-size penalties.
- **Validation strategy:** Stalled TLS, idle authenticated sessions, nonreading clients, eviction, and subsequent legitimate capacity recovery.
- **Measurement required:** YES, for network deadline behavior and legitimate-client impact.

## 5. Medium Findings

### M01 — Idle retirement decrements unrelated active-lease accounting

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED accounting defect; premature-disposal consequence is conditional.
- **Category:** NNTP lease ownership, concurrency.
- **Location:** `ReleaseAsync` 545–559; retirement 935–989:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Grabber/NntpArticleExecutionSessionManager.cs`.
- **Problem:** Idle retirement uses the release path for a lease it never acquired.
- **Evidence:** Retirement sets Busy and invokes release; release decrements the aggregate active count whenever positive.
- **Execution path:** Independent control-plane endpoint update → idle-slot retirement → ordinary lease release.
- **Failure scenario:** T1 holds an article lease on A; T2 retires idle B; B's release decrements A's aggregate ownership count.
- **Concrete consequence:** Accounting reports zero while work remains; overlapping disposal can close an active session prematurely.
- **Why existing implementation does not prevent it:** Serial article processing still permits independent idle-slot refresh. Normal account
  removal drains RabbitMQ first, narrowing premature disposal; separate idle/active retirement tests miss their composition.
- **Recommended remediation:** Separate real acquisition ownership from maintenance/retirement Busy state.
- **Potential behavioural impact:** Drain waits reflect actual leases rather than unrelated maintenance events.
- **Validation strategy:** Hold A, retire B, assert active count remains one, and overlap disposal before A releases.
- **Measurement required:** NO.

### M02 — Successful keepalive loses deferred retirement

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED with nonzero keepalive.
- **Category:** NNTP capacity, lifecycle reconciliation.
- **Location:** Maintenance 750–820; retirement 953–966:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Grabber/NntpArticleExecutionSessionManager.cs`.
- **Problem:** Successful maintenance relinquishes Busy ownership without processing a retirement requested during the probe.
- **Evidence:** DATE success clears Busy and requeues; acquisition skips RetireRequested; subsequent maintenance/retirement skips it while
  reconciliation still counts its non-null session.
- **Execution path:** DATE probe → concurrent control-plane reconciliation → probe success → unusable but counted slot.
- **Failure scenario:** T1 starts DATE; T2 marks retirement; T1 succeeds and requeues instead of retiring.
- **Concrete consequence:** Usable provider capacity can remain missing until disposal.
- **Why existing implementation does not prevent it:** Pending-acquirer suppression only prevents starting probes, not refresh during a running probe.
  Zero keepalive avoids the scenario. Tests do not overlap successful DATE and retirement.
- **Recommended remediation:** Complete deferred retirement when maintenance releases ownership.
- **Potential behavioural impact:** Endpoint changes retire promptly rather than preserve unusable sessions.
- **Validation strategy:** Pause DATE, request retirement, release success, and verify closure plus replacement capacity.
- **Measurement required:** NO.

### M03 — Idle keepalive tokens accumulate without a bound

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED growth mechanism; magnitude unmeasured.
- **Category:** Memory lifetime, queue scheduling.
- **Location:** Unbounded channel 160–165, maintenance 750–752, requeue 805–817:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Grabber/NntpArticleExecutionSessionManager.cs`.
- **Problem:** Maintenance changes scheduling flags without consuming the old queued token.
- **Evidence:** It clears Enqueued and later writes a new token after success; the previous token remains in the unbounded idle channel.
- **Execution path:** Repeated successful idle DATE cycles with no acquisition consuming tokens.
- **Failure/performance scenario:** Sustained idle operation across many sessions accumulates roughly one extra token per affected successful cycle.
- **Concrete consequence:** Growing queue metadata and later stale-token dequeue work.
- **Why existing implementation does not prevent it:** Busy checks prevent simultaneous duplicate leases, not stale scheduling representations.
- **Recommended remediation:** Maintain exactly one schedulable idle representation per slot.
- **Potential behavioural impact:** Idle scheduling remains bounded without changing connection counts.
- **Validation strategy:** Run controlled repeated probes without acquisition; queue size must remain proportional to slots, then acquire all usable slots.
- **Measurement required:** YES, for retained memory and dequeue-cost magnitude.

### M04 — Account managers escape initialization and retirement cleanup

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Resource ownership, partial startup, shutdown.
- **Location:** Startup 209–266, removal 331–361, add/cleanup 439–486:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/ControlPlane/ControlPlaneService.cs`.
- **Problem:** The ownership map omits objects whose cleanup still needs to run.
- **Evidence:** Removal deletes managers before awaited RabbitMQ retirement; cancellation skips Dispose. Partial startup can initialize A, fail on B,
  and never start the execution loop whose finally cleans A. Manager maintenance CTS is independently owned.
- **Execution path:** Account removal → canceled consumer drain, or multi-account startup → later initialization failure.
- **Failure scenario:** T1 removes A; T2 cancels drain; A is absent from final cleanup. Alternatively B fails after A has started maintenance.
- **Concrete consequence:** Sessions/maintenance escape deterministic cleanup. Ordinary host-stop leakage lasts until process exit, not beyond it.
- **Why existing implementation does not prevent it:** Final cleanup iterates surviving map entries; the single-account cancellation test misses earlier successes.
- **Recommended remediation:** Track initializing and retiring ownership until disposal completes; roll back all partial startup successes.
- **Potential behavioural impact:** Cancellation waits for or explicitly accounts for cleanup rather than losing managers.
- **Validation strategy:** Cancel multi-account startup and account removal during drain; assert every created manager stops and disposes.
- **Measurement required:** NO.

### M05 — Shutdown signaling bypasses RabbitMQ disposal

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Connection cleanup, disposal state.
- **Location:** `DisposeAsync` 240–275, shutdown callback 544–555:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/RabbitMq/RabbitMqConnectionManager.cs`.
- **Problem:** Shutdown-requested and disposal-completed are represented by the same early-return flag.
- **Evidence:** Shutdown sets `_disposeRequested`; later Dispose sees it and returns before connection closure, task joining, and registration disposal.
- **Execution path:** Shutdown notification → startup initializer stop/DI disposal → skipped cleanup.
- **Failure scenario:** Normal shutdown notification arrives before disposal, which is the intended lifetime ordering.
- **Concrete consequence:** Owned connection/recovery resources are not deterministically closed/joined.
- **Why existing implementation does not prevent it:** Recovery-loop finally only releases its gate; it does not independently perform disposal.
- **Recommended remediation:** Separate shutdown requested from disposal started/completed and return one shared actual disposal task.
- **Potential behavioural impact:** Normal stop performs the cleanup it previously skipped; repeated callers join the same cleanup.
- **Validation strategy:** Signal shutdown first, dispose concurrently/repeatedly, and assert connection close, task completion, and one-time resource disposal.
- **Measurement required:** NO.

### M06 — Shutdown initiation depends on logging and lifecycle notifications succeeding

- **Severity:** MEDIUM.
- **Confidence/type:** STRONGLY_SUPPORTED bounded-shutdown risk.
- **Category:** Logging backpressure, lifecycle ordering.
- **Location:** Stopping callback 494–542:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Hosting/HostLifetimeCoordinator.cs`.
  Blocking async sinks 69–80:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Logging/SerilogConfigurator.cs`.
  Notification/transition gating 359–369, 400–457:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Lifecycle/ServiceLifecycle.cs`.
- **Problem:** Mandatory cancellation/deadline establishment occurs after fallible or blocking diagnostics/notifications.
- **Evidence:** The stopping callback logs before signaling graceful shutdown; both async sinks block when full. Lifecycle transition contention
  can reject Draining while Ready observers are being notified, before the coordinator signal is reached.
- **Execution path:** Host stopping callback → logging/lifecycle transition → graceful signal and forced timer.
- **Failure scenario:** T1 stalls sink delivery and fills its buffer; T2 logs shutdown and blocks before starting its deadline.
  Separately, Ready notification can overlap Stop and reject the transition.
- **Concrete consequence:** The mechanism intended to bound shutdown may never start. Release-path logging can similarly delay ownership decrement.
- **Why existing implementation does not prevent it:** Healthy sink/count tests do not exercise blocked sinks; later lifecycle rechecks do not
  necessarily repair a skipped coordinator signal.
- **Recommended remediation:** Make cancellation, deadline, and ownership actions independent of diagnostic success; preserve intentional sink policy explicitly.
- **Potential behavioural impact:** Shutdown remains bounded even if logging must be delayed/lost or an observer fails.
- **Validation strategy:** Saturate a deliberately blocked sink and deterministically overlap Ready observers with Stop.
- **Measurement required:** NO.

### M07 — Transit response timing inherits idle time and initialization policy

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Timeout epochs, error classification.
- **Location:** Progress timestamp/pending/watchdog 627, 762–774, 1255–1274:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitConnection.cs`.
  Timeout construction 1562–1573, 1646–1650:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitPublisher.cs`.
- **Problem:** Idle time is charged to new pending work; initialization timing is stored as the steady-state connection timeout.
- **Evidence:** Zero-pending watchdog checks skip without resetting progress. New pending registration does not start a new epoch.
  Demand-created connections receive the short initialization policy in an immutable response timeout field.
- **Execution path:** Healthy connection idles → new batch registers → watchdog checks old timestamp; or initialization → Ready using the same timeout.
- **Failure scenario:** Idle exceeds timeout, then a healthy new request loses the race to the next watchdog check. A response valid under the intended
  normal timeout can also exceed the retained short initialization timeout.
- **Concrete consequence:** False Ambiguous outcomes and unnecessary reconnects.
- **Why existing implementation does not prevent it:** A no-outstanding test does not exercise a connected idle-to-active transition.
- **Recommended remediation:** Separate initialization/response deadlines and reset the active epoch on zero-to-nonzero pending ownership.
- **Potential behavioural impact:** Healthy slow responses and first work after idle are no longer prematurely faulted.
- **Validation strategy:** Connected idle reuse plus replies between initialization and normal response thresholds.
- **Measurement required:** NO.

### M08 — Retention completion crosses admission generations

- **Severity:** MEDIUM.
- **Confidence/type:** ARCHITECTURAL_CONCERN; cross-generation effect established, intended-contract violation unresolved.
- **Category:** Identity, retention completion contract.
- **Location:** Entry creation 170, completion 376–410, removal 454–489:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Retention/ArticleRetentionAuthority.cs`.
  Completion contract 58–65, 307–315:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Retention/ArticleRetentionContracts.cs`.
- **Problem:** Late work for a removed retained instance can complete the current instance with the same Message-ID.
- **Evidence:** Completion looks up the current entry by identity; new entries reset completion flags. Listener completion carries Message-ID, not generation.
- **Execution path:** Old lease → TTL/pressure removal → same-ID re-admission → delayed old ACK/Transit completion → current entry completion.
- **Failure scenario:** A transfers but delays ACK; A expires; B re-admits the identity and gets a fresh response; B Transit completes;
  A ACK marks B listener-complete; B's requester then receives NotFound before B's TTL without intervening pressure.
- **Concrete consequence:** A new successful response may not establish a new independent retrieval opportunity.
- **Why existing implementation does not prevent it:** Leases protect physical bytes, not logical generation. However, the documented API explicitly
  completes by Message-ID, and composed duplicate-response tests do not require separate completion obligations.
- **Recommended remediation:** Decide whether completion is identity-wide or admission-specific before implementing generation-bound completion.
- **Potential behavioural impact:** Adding generations would change intentional identity-level policy if that is the actual desired contract.
- **Validation strategy:** Two correlation IDs, expiry/re-admission, delayed old ACK, and retrieval under the new response; assert the agreed policy.
- **Measurement required:** NO.

### M09 — Serial article processing and first-account selection constrain capacity

- **Severity:** MEDIUM.
- **Confidence/type:** ARCHITECTURAL_CONCERN; source-level serialization confirmed, cost NEEDS_MEASUREMENT.
- **Category:** Throughput, fairness, resource amplification.
- **Location:** Processing loop 81–112:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/RabbitMqArticleProcessingService.cs`.
  Account selection 169–199:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/ControlPlane/ControlPlaneService.cs`.
- **Problem:** Multiple consumers/provider sessions feed a single serial article path, and selection awaits the first sorted account.
- **Evidence:** One hosted processor sequentially awaits acquisition, result handling, publication, and settlement. Selection does not first
  choose among immediately available accounts.
- **Execution path:** Shared delivery queue → one article → all downstream awaits → next article.
- **Performance scenario:** One slow provider, release/reconnect, queue admission, or publish confirmation delays unrelated backbones.
- **Concrete consequence:** Connection counts do not yield equivalent article throughput or isolation. First-account selection can delay using later capacity.
- **Why existing implementation does not prevent it:** Broker prefetch and provider session counts are different concurrency domains.
  The current serial loop narrows claims of simultaneous first-account contention; no routine many-caller workload is assumed.
- **Recommended remediation:** Measure the composed path, then choose bounded worker concurrency and fair acquisition with explicit memory budgets.
- **Potential behavioural impact:** Parallelization changes memory demand, completion ordering, and race exposure; apply after ownership fixes.
- **Validation strategy:** Multi-backbone workload with one stalled provider/publisher; measure unrelated latency and actual acquisition concurrency.
- **Measurement required:** YES.

### M10 — Nonreplyable invalid requests can requeue forever

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** RabbitMQ poison-message handling.
- **Location:** Metadata parsing 158–165:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/RabbitMqArticleWorkRequestParser.cs`.
  Result disposition 207–243:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/RabbitMqArticleResultSink.cs`.
- **Problem:** Irrecoverable missing reply metadata is handled like transient response-publication failure.
- **Evidence:** InvalidRequest requires a response; publisher cannot reply without destination/correlation information; sink requeues on publication failure.
- **Execution path:** Missing ReplyTo/CorrelationId → InvalidRequest → required reply fails → NACK requeue.
- **Failure scenario:** The same unchanged malformed delivery is repeatedly redelivered.
- **Concrete consequence:** Poison traffic consumes processing/broker capacity and competes with valid work.
- **Why existing implementation does not prevent it:** Redelivery cannot reconstruct missing metadata; bounded buffering does not bound retry count.
- **Recommended remediation:** Terminally classify/dead-letter nonreplyable invalid input separately from transient destination failure.
- **Potential behavioural impact:** Such malformed messages stop being retried; preserve diagnostics/dead-letter policy for operators.
- **Validation strategy:** Missing metadata must reach a bounded terminal outcome while subsequent valid deliveries continue.
- **Measurement required:** NO.

### M11 — Unroutable responses can still cause source settlement

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED broker-semantics mismatch.
- **Category:** Response delivery, RabbitMQ confirmation.
- **Location:** Response publishing 130–156:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/RabbitMqArticleResponsePublisher.cs`.
- **Problem:** Publisher confirmation is treated as sufficient despite disabled mandatory routing detection.
- **Evidence:** `mandatory:false` permits confirmed publication that routes nowhere; the sink subsequently settles the original request.
- **Execution path:** Reply queue disappears → response publish/confirm → source ACK or terminal disposition.
- **Failure scenario:** An ephemeral reply destination is removed before publication reaches the broker.
- **Concrete consequence:** The requester receives no response while source work is no longer available for normal retry.
- **Why existing implementation does not prevent it:** Confirms establish broker acceptance, not successful routing. Publisher tests ignore mandatory behavior.
- **Recommended remediation:** Observe mandatory returns or define equivalent destination-loss detection with a bounded absent-destination policy.
- **Potential behavioural impact:** Unroutable replies become visible and may change source disposition; avoid introducing endless requeue.
- **Validation strategy:** Delete reply destination just before publish and verify the intended response/source outcome.
- **Measurement required:** NO.

### M12 — Actual failure responses violate the published schema

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Wire protocol, schema composition.
- **Location:** Failure construction 287–292, empty-text path 430:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/RabbitMqArticleWorkRequestParser.cs`.
  Response factory 63–79:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/ArticleWorkResponseFactory.cs`.
- **Problem:** Generated invalid-request responses do not always satisfy required identity/error fields.
- **Evidence:** Failure construction can supply empty MessageId despite schema minimum length; an empty-text path can omit required failure error details.
- **Execution path:** Malformed input → parse failure → disposition/factory → serializer → schema-validating client.
- **Failure scenario:** Client receives one of these composed responses rather than the handcrafted examples used in schema tests.
- **Concrete consequence:** Consumers reject the service's own failure response.
- **Why existing implementation does not prevent it:** Schema tests validate handcrafted payloads rather than generated failure paths.
- **Recommended remediation:** Define version-compatible unavailable-identity/error representation and align parser, factory, serializer, and schema.
- **Potential behavioural impact:** Wire failure shape may require a compatibility decision; do not invent a valid-looking identity.
- **Validation strategy:** Validate actual parser-to-serializer failure results against the schema for every malformed-input category.
- **Measurement required:** NO.

### M13 — Acquisition and yEnc validation disagree about dot-unstuffing

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED internal integration mismatch; strict external interoperability extent unverified.
- **Category:** Byte preservation, transport/encoding boundary.
- **Location:** Acquisition unstuffing 599–604:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Acquisition/NntpArticleAcquisitionSession.cs`.
  Validator unstuffing 390–393:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/YEnc/YEncArticleValidator.cs`.
- **Problem:** Logical article bytes can undergo a second transport unstuffing operation.
- **Evidence:** Acquisition maps wire leading three dots to two; unchanged logical memory reaches validation, which maps leading two dots to one.
  Standalone validator tests accept a representation whose decoded bytes differ through acquisition.
- **Execution path:** NNTP read → parser bridge → yEnc validation → CRC/size failure → invalid-article disposition.
- **Failure scenario:** Dot-prefixed encoded content reaches the composed path; extra removal changes decoded length/CRC.
- **Concrete consequence:** Internal standalone/full-pipeline acceptance differs and can lead to terminal rejection.
- **Why existing implementation does not prevent it:** Synthetic encoders escape every dot, excluding this integration case.
  Normative leading-dot permission was not independently verified; this is not a claim all conforming encoders are affected.
- **Recommended remediation:** Specify whether validator input is wire-stuffed or logical bytes and unstuff transport exactly once.
- **Potential behavioural impact:** Standalone callers may need a clearly separated input contract; preserve article body bytes.
- **Validation strategy:** One fixture through standalone and actual acquisition paths must produce identical expected decoded bytes and CRC.
- **Measurement required:** NO.

### M14 — Folded Date and From values are rejected

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Header parsing, semantic validation.
- **Location:** Continuations 343–354, From checks 654–660:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Parsing/NntpArticleParser.cs`.
  Printable-input validation 147–150:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/DateParser/NewsDateParser.cs`.
- **Problem:** Raw folded values are passed to semantic validators without unfolding.
- **Evidence:** Continuation slices preserve CRLF; Date/From consumers reject those controls before interpreting whitespace.
- **Execution path:** Header continuation accepted → raw value decoded/validated → semantic failure.
- **Failure scenario:** A folded Date with no alternative date candidate, or a folded From, otherwise satisfies the intended header grammar.
- **Concrete consequence:** An article is rejected despite the parser's apparent continuation support.
- **Why existing implementation does not prevent it:** Continuation coverage uses Subject rather than these semantic consumers.
- **Recommended remediation:** Unfold bounded semantic scratch values while preserving raw retained bytes.
- **Potential behavioural impact:** Additional valid folded headers are accepted; canonical byte preservation remains unchanged.
- **Validation strategy:** Folded/unfolded Date/From parity, alternate-date candidates, limits, and raw-byte equality.
- **Measurement required:** NO.

### M15 — Parser-accepted mixed separators fail canonical materialization

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Parser/materializer contract.
- **Location:** Path insertion and boundary detection 57–59, 196–216:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Processing/NntpArticleCanonicalMaterializer.cs`.
- **Problem:** Materialization recognizes fewer separator combinations than parsing.
- **Evidence:** Parsing can accept a last header ending LF followed by a CRLF blank line; materialization searches only uniform CRLFCRLF/LFLF/CRCR.
- **Execution path:** Parser success → missing Path → canonical Path insertion → boundary not found → exception.
- **Failure scenario:** Mixed separator input is accepted and requires insertion; an existing Path avoids this particular insertion path.
- **Concrete consequence:** Successfully parsed articles cannot complete canonicalization; downstream handling must treat an unexpected stage failure.
- **Why existing implementation does not prevent it:** Materializer separator tests cover only uniform CRLF, LF, and CR.
- **Recommended remediation:** Carry the parser's validated boundary or give both stages one accepted boundary language.
- **Potential behavioural impact:** Previously accepted-but-unmaterializable inputs either complete correctly or are rejected consistently earlier.
- **Validation strategy:** Mixed separators with missing/existing Path, byte preservation, and composed result disposition.
- **Measurement required:** NO.

### M16 — Acquisition/parser limits do not form a consistent resource boundary

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED; cost magnitude unmeasured.
- **Category:** Memory, CPU, parsing limits.
- **Location:** Acquisition maximum 118–124:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Acquisition/NntpArticleAcquisitionContracts.cs`.
  Parser maximum 290–297:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Parsing/NntpArticleParserContracts.cs`.
  Limit enforcement 288–403, 870–889:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Articles/Parsing/NntpArticleParser.cs`.
- **Problem:** Larger upstream bounds permit work that downstream defaults necessarily reject; local limits also have boundary gaps.
- **Evidence:** Acquisition allows 256 MiB, parser 64 MiB. yEnc scan bounds line start but line-end search can traverse beyond the scan budget.
  Folded values can exceed the aggregate intended value limit; header count has a one-entry discrepancy.
- **Execution path:** Default acquisition → whole buffer → default parsing; or parser line/continuation scanning.
- **Failure/performance scenario:** A 128 MiB article is downloaded before rejection; a long newline-free body exceeds intended detection work;
  several individually bounded continuation lines exceed the aggregate value budget.
- **Concrete consequence:** Avoidable network/memory/CPU work and misleading advertised limits.
- **Why existing implementation does not prevent it:** Larger section/article limits still bound the work, but do not enforce the smaller intended limits.
- **Recommended remediation:** Share effective bounds and enforce cumulative values/exact boundaries; preserve stream synchronization on early rejection.
- **Potential behavioural impact:** Oversized articles reject earlier and malformed boundary cases may change classification.
- **Validation strategy:** Limit−1/equal/+1, newline-free body, folded aggregates, exact header counts, and reusable-connection behavior after rejection.
- **Measurement required:** YES for cost; not needed to establish the limit mismatches.

### M17 — Explicit zero prefetch bypasses the declared range

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Configuration, RabbitMQ backpressure.
- **Location:** Prefetch property 593–597:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Configuration/BackFillerOptions.cs`.
  Nested validation 271–276:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Configuration/ConfigurationValidator.cs`.
- **Problem:** Explicit zero is accepted despite the declared range of 1–65535.
- **Evidence:** Nested validation does not apply the property annotation; zero is projected and reaches `BasicQosAsync`.
- **Execution path:** Configuration binding → incomplete validation → runtime snapshot → broker QoS.
- **Failure scenario:** An operator explicitly configures zero expecting validation rather than AMQP unlimited prefetch.
- **Concrete consequence:** The explicit value removes a finite broker/client prefetch boundary.
- **Why existing implementation does not prevent it:** Bounded application delivery buffering does not bound all client/broker prefetch. Null omission
  intentionally leaves the broker policy unchanged and is not itself a defect.
- **Recommended remediation:** Enforce explicit-value range while preserving omission semantics.
- **Potential behavioural impact:** Previously accepted explicit zero fails startup with the declared validation error.
- **Validation strategy:** Actual startup pipeline with null, zero, one, and maximum values, then inspect emitted QoS.
- **Measurement required:** NO.

### M18 — Database validation, provisioning, and runtime configuration disagree

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED provisioning contradiction; STRONGLY_SUPPORTED startup reload race.
- **Category:** Startup ordering, immutable configuration.
- **Location:** Database probe 47–79:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Validation/DatabaseDependencyProbe.cs`.
  Provider construction 116, provisioning 479–510:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Accounts/MySqlNntpAccountSnapshotProvider.cs`.
- **Problem:** Validation requires a database that later provisioning intends to create; the provider also rereads mutable configuration.
- **Evidence:** Probe connects to the configured database. Missing-database failure exits before hosted CREATE DATABASE logic.
  Later provider construction reads raw reloadable configuration rather than a frozen validated connection value.
- **Execution path:** Probe database A → DNS/certificate startup work → provider construction → optional schema/database provisioning.
- **Failure scenario:** Fresh database does not exist, or configuration reload changes A to B between validation and provider construction.
- **Concrete consequence:** Advertised provisioning is unreachable for a fresh database; runtime may use a database not validated.
- **Why existing implementation does not prevent it:** Provider-only provisioning tests bypass startup; the immutable snapshot omits this connection value.
- **Recommended remediation:** Separate server reachability, authorized provisioning, and database access; freeze database settings once.
- **Potential behavioural impact:** Fresh installation can provision after validation; mid-startup reload no longer changes the target.
- **Validation strategy:** Missing database with/without provisioning permission and configuration change between probe/provider construction.
- **Measurement required:** NO.

### M19 — Reserved RabbitMQ pool settings impose fictitious runtime constraints

- **Severity:** MEDIUM.
- **Confidence/type:** ARCHITECTURAL_CONCERN; concrete configuration inconsistency confirmed.
- **Category:** Configuration semantics, resource model.
- **Location:** Capacity relationship 1819–1841:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Configuration/BackFillerOptions.cs`.
  Delivery-buffer projection 35:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/RabbitMq/RabbitMqConsumerInfrastructureOptions.cs`.
- **Problem:** Reserved pool controls constrain an active delivery-buffer setting as if it represented broker channels.
- **Evidence:** Runtime maintains one connection slot; MinConnections/MaxConnections/lease waiter controls have no corresponding runtime pool.
  ChannelPoolSize maps to delivery capacity, yet validation compares it to MaxConnections × RequestedChannelMax.
- **Execution path:** Configuration cross-check → unchanged single-connection runtime topology.
- **Failure scenario:** MaxConnections=1, channel max=128, delivery capacity=512 is rejected; raising otherwise inert MaxConnections to 4 admits
  the same active delivery-buffer configuration without creating more connections.
- **Concrete consequence:** Artificial deployment restrictions and misleading resource expectations.
- **Why existing implementation does not prevent it:** Source comments disclose some reserved controls, but the validator and runtime-option language retain pool semantics.
- **Recommended remediation:** Distinguish active/reserved inputs and validate delivery capacity independently. Do not implement pooling merely to satisfy names.
- **Potential behavioural impact:** Some previously rejected configurations become valid; no pooling implementation is implied.
- **Validation strategy:** Contract tests showing reserved-setting variation does not change topology; independently validate active delivery limits.
- **Measurement required:** NO.

### M20 — DNS timeout and partial failure bypass fallback and quorum

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Certificate availability, cancellation classification.
- **Location:** Quorum 98–111, resolver fallback 227–240, UDP timeout 320–332:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Certificates/AuthoritativeDnsTxtPropagationVerifier.cs`.
- **Problem:** Internal query timeout is treated as caller cancellation, and one failed authority can defeat an otherwise satisfied quorum.
- **Evidence:** Receive timeout uses a linked CTS; fallback catches SocketException/TimeoutException, not the internally generated cancellation.
  Authoritative polling awaits every query without isolating transport failure before checking success count.
- **Execution path:** Resolver discovery → authoritative address list → sequential TXT queries → quorum check.
- **Failure scenario:** First recursive resolver silently drops a query while another is healthy; or three of four authorities succeed for a 0.7
  quorum but the fourth fails. IPv6 authority reachability can differ on IPv4-only hosts.
- **Concrete consequence:** Issuance/renewal fails despite available resolver/quorum capacity; internal cancellation also bypasses provisioning fallback.
- **Why existing implementation does not prevent it:** Quorum is evaluated only after all queries succeed as operations; caller/internal cancellation are not separated.
- **Recommended remediation:** Translate internal timeout, preserve actual caller cancellation, and treat per-authority transport failures as failed observations.
- **Potential behavioural impact:** Partial DNS failure becomes tolerable within the configured quorum/timeout policy.
- **Validation strategy:** Silent-first/healthy-second resolver; failure before/after quorum; mixed address families; real caller cancellation.
- **Measurement required:** NO.

### M21 — Created TXT records lack the metadata required for stale-record recovery

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** External artifact ownership, restart recovery.
- **Location:** Creation 82–89:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Certificates/CloudflareTxtRecordApi.cs`.
  Ownership predicate 90–117:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Certificates/AcmeCertificateIssuer.Dns01Helpers.cs`.
- **Problem:** Production creation omits comments/tags that recovery requires to recognize its own records.
- **Evidence:** NewDnsRecord sends name/type/content/proxy/TTL only; recovery requires ownership markers. The recovery fake supplies those markers.
- **Execution path:** Create challenge → process interruption/cleanup failure → new order → stale reconciliation.
- **Failure scenario:** Process exits after publication; the next order needs a different TXT value.
- **Concrete consequence:** Own abandoned records are treated as unrelated and can accumulate.
- **Why existing implementation does not prevent it:** Same-attempt cleanup retains the record ID; it cannot repair interrupted attempts after restart.
- **Recommended remediation:** Persist explicit controlled ownership metadata and preserve unrelated records.
- **Potential behavioural impact:** Future owned stale records become removable; do not infer ownership of arbitrary preexisting records.
- **Validation strategy:** Exercise production request mapping, interrupt after creation, restart, and verify owned cleanup without unrelated deletion.
- **Measurement required:** NO.

### M22 — Certificate intermediates are not preserved through reload and cloning

- **Severity:** MEDIUM.
- **Confidence/type:** STRONGLY_SUPPORTED, platform verification required.
- **Category:** TLS availability, certificate ownership.
- **Location:** Load 181–195, chain build 352–361:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Certificates/BackFillerCertificateStore.cs`.
  Clone 80–85:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Certificates/BackFillerCertificateState.cs`.
- **Problem:** The persisted issuer chain is not explicitly retained/supplied during reload validation and listener certificate cloning.
- **Evidence:** Persistence includes intermediates; loading retains one X509Certificate2; chain building has no ExtraStore; cloning exports that certificate.
- **Execution path:** Complete PFX persistence → reload into bundle → chain evaluation → clone → TLS handshake.
- **Failure scenario:** Clean Linux intermediate cache, necessary intermediate only in PFX, AIA unavailable.
- **Concrete consequence:** Reload may reject a complete persisted bundle or listener clients may not receive required intermediates.
- **Why existing implementation does not prevent it:** Existing fixtures are self-signed with empty chains; implicit platform caching is not a reliable explicit ownership contract.
- **Recommended remediation:** Retain imported chain certificates, supply evaluation/TLS certificate context, and dispose replacement ownership correctly.
- **Potential behavioural impact:** Reduces unnecessary reissuance and makes chain serving independent of incidental machine cache.
- **Validation strategy:** Root/intermediate/leaf fixture, clean-cache Linux, unavailable AIA, persisted reload, and client-visible chain verification.
- **Measurement required:** YES, for platform behavior; no throughput claim.

### M23 — Supplied systemd template does not match normal publishing

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED template/configuration mismatch; actual deployed units unknown.
- **Category:** Linux packaging, operations.
- **Location:** ExecStart 36, directives 56–94:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/vectornntp-backfiller.service`.
  Publishing 145–148:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.csproj`.
- **Problem:** The supplied service command and several directive interpretations do not match intended deployment.
- **Evidence:** ExecStart references differently cased `VectorNNTP.Backfiller.dll`; normal self-contained single-file output is an executable.
  Inline `#` suffixes are not systemd inline comments. Purported automatic-restart/backoff settings do not establish the documented behavior.
- **Execution path:** Normal Linux publish → install supplied unit → start/systemd parse.
- **Failure scenario:** Operator uses the template with ordinary output rather than correcting executable naming and directives.
- **Concrete consequence:** Startup failure or settings not enforced as described.
- **Why existing implementation does not prevent it:** Windows publishing does not smoke-test Linux unit behavior; template customizability does not make defaults correct.
- **Recommended remediation:** Align executable, unit syntax, restart semantics, and writable directories with real publishing.
- **Potential behavioural impact:** Correct unit behavior may expose required writable-path allowances; assess the effective current configuration first.
- **Validation strategy:** Clean Linux publish, systemd unit verification, start/stop, runtime directories, fatal-exit restart.
- **Measurement required:** NO.

### M24 — Transit snapshots and percentile fields misrepresent execution

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Observability, diagnostic consistency.
- **Location:** Snapshot 381–435 and retirement 1181–1224:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitPublisher.cs`.
  Flushed marking 791–797, diagnostics 1135–1141:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Transit/TransitConnection.cs`.
- **Problem:** Published metrics do not consistently describe completed transitions or actual distributions.
- **Evidence:** Lifetime totals are read before per-slot version capture; retirement between them can omit retired totals.
  P50/P95/P99 batch fields contain the mean with an empty histogram. Flushed is set before awaiting actual flush.
- **Execution path:** Concurrent snapshot/connection retirement; batch staging/flush; diagnostics serialization.
- **Failure scenario:** T1 reads lifetime totals; T2 retires a connection; T1 sees final stable slots but misses transferred bytes.
  Nonuniform batch sizes or blocked flush expose other semantic mismatches.
- **Concrete consequence:** Counters can appear to regress, tails are fictitious, and transmission-stage diagnosis is misleading.
- **Why existing implementation does not prevent it:** Slot version checks do not encompass the whole aggregate read; mean is not a percentile.
- **Recommended remediation:** Capture coherent epochs, collect or honestly omit distributions, and distinguish staged from completed flush.
- **Potential behavioural impact:** Diagnostic meanings/historical comparisons change; production wire behavior need not change.
- **Validation strategy:** Paused retirement snapshot, known nonuniform distribution, and blocked flush stage assertions.
- **Measurement required:** NO.

### M25 — Cache retrieval relies on external network trust

- **Severity:** MEDIUM, conditional on confidentiality expectations and reachability.
- **Confidence/type:** STRONGLY_SUPPORTED exposure risk; no unconditional authorization-contract violation established.
- **Category:** Authentication/authorization boundary.
- **Location:** TLS settings 200–224:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Listener/BackFillerListenerSocketService.cs`.
  Digest lookup 36–58:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Listener/ListenerProtocolRetentionRequestHandler.cs`.
- **Problem:** Transport encryption is not client authorization for retained content.
- **Evidence:** Client certificates are not required and lookup uses the digest derived from a retained Message-ID without an additional authorization check.
- **Execution path:** Reach listener → TLS → compute/provide identity digest → retained payload retrieval.
- **Failure scenario:** A network-reachable party knows the Message-ID while its bytes are retained.
- **Concrete consequence:** That party can retrieve the retained article; it cannot use this path to trigger arbitrary external acquisition.
- **Why existing implementation does not prevent it:** Knowledge of identity is not a separate credential. Private-network/allowlist intent was unavailable.
- **Recommended remediation:** Explicitly document/enforce trusted-network access or introduce the required client authorization policy.
- **Potential behavioural impact:** Additional authorization may change existing clients; network-only mitigation may preserve wire compatibility.
- **Validation strategy:** Deployment boundary audit and allowed/denied client retrieval tests.
- **Measurement required:** NO.

### B01 — Throughput metrics mix incompatible work and time windows

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED measurement-semantic defects.
- **Category:** Benchmark validity.
- **Location:** Window/drain coordination 90–99:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementRunCoordinator.cs`.
  Result formulas 113, 156–164:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Metrics/BenchmarkResultFactory.cs`.
  Warmup/deadline 553–625:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/TransitDiagnosticSuiteRunner.cs`.
- **Problem:** Numerators and denominators do not consistently describe the same work cohort and interval.
- **Evidence:** Drain completions contribute to accepted totals divided by pre-drain time; fixed-count windows can end at enqueue completion.
  Diagnostic deadlines/counters start before warmup while elapsed measurement starts afterward. Some accepted-Gbps paths use generated bytes.
- **Execution path:** Warmup → admission window → drain → result aggregation.
- **Performance scenario:** Late ACKs, long warmup, or all-rejected traffic produce misleading accepted-rate output.
- **Concrete consequence:** Apparent throughput cannot reliably size production or compare implementations.
- **Why existing implementation does not prevent it:** A documented/frozen formula can still have a misleading name; contract tests can freeze the same mismatch.
- **Recommended remediation:** Define matched admission/completion cohorts and windows; separate offered, transmitted, accepted, and drained work.
- **Potential behavioural impact:** Version metric semantics and rebaseline instead of silently comparing old/new figures.
- **Validation strategy:** Independent event ledger with delayed ACKs, all rejections, known warmup, and fixed-count admission followed by drain.
- **Measurement required:** YES.

### B02 — Benchmark cancellation can leak byte-budget reservations

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Harness concurrency, accounting.
- **Location:** Reservation/release 92–130, 231–265:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Execution/ByteBudget.cs`.
  Duplicate implementation 140–187, 285–320:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/TransitBenchmarkCore.cs`.
- **Problem:** Capacity reservation and successful delivery to a waiter are not one ownership transition.
- **Evidence:** Release charges/removes a waiter under lock and completes its task outside lock; canceled completion can win and failed TrySetResult is ignored.
- **Execution path:** Waiting budget acquisition → release selects waiter → cancellation → attempted successful completion.
- **Failure scenario:** T1 reserves for a waiter; T2 cancels its completion before T1 publishes success; capacity remains charged with no owner.
- **Concrete consequence:** Harness capacity leaks and later producers can stall.
- **Why existing implementation does not prevent it:** Removing the waiter does not establish that its caller received the reservation; both copies share the race.
- **Recommended remediation:** Atomically transfer reservation ownership or refund when completion publication loses.
- **Potential behavioural impact:** Canceled benchmark operations stop consuming budget permanently.
- **Validation strategy:** Deterministically race cancellation between reservation and completion in both implementations; reconcile full capacity afterward.
- **Measurement required:** NO.

### B03 — Harness cancellation/shutdown does not reliably join owned work

- **Severity:** MEDIUM.
- **Confidence/type:** STRONGLY_SUPPORTED; bounded normal publisher behavior narrows backlog scenarios.
- **Category:** Benchmark lifecycle, fake-server backpressure.
- **Location:** Coordinator 37–99:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementRunCoordinator.cs`.
  Server shutdown/queue/flush 190–217, 287–292, 744–802:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Execution/BenchmarkDevNullTransitServer.cs`.
- **Problem:** Cancellation bypasses unconditional producer/telemetry joining; fake-server response flush can outlive shutdown.
- **Evidence:** Coordinator lacks an unconditional full drain/join finally. Fake-server response queue is unbounded and flush is tokenless.
- **Execution path:** Cancellation during measurement, or nonreading peer during fake-server response output → disposal.
- **Failure scenario:** Owned producers/telemetry remain active after coordinator exits; fake-server flush blocks against saturated peer buffers.
- **Concrete consequence:** Incomplete artifacts, escaping tasks, or hanging harness shutdown.
- **Why existing implementation does not prevent it:** Outer publisher disposal and budget-finally are partial defenses, not joins for every task.
  Prepared workload disposal is empty, so this is not asserted as pooled use-after-return. Normal publisher flow control constrains some backlog.
- **Recommended remediation:** Unconditional ownership joins plus bounded/cancelable fake-server teardown.
- **Potential behavioural impact:** Cancellation can await controlled cleanup and classify unfinished runs explicitly.
- **Validation strategy:** Cancel each phase and saturate a nonreading peer; assert all tasks exit and artifacts declare incomplete runs.
- **Measurement required:** YES for blocked transport behavior.

### B04 — Isolated regression gate can report PASS after abnormal completion

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED static classification defect.
- **Category:** Test assurance, diagnostic classification.
- **Location:** Classification 163–205 and completion flags 595–612:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/tools/testing/IsolatedRegressionGate/Program.cs`.
  Exit/result ordering 169–179, 221–235:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/tools/testing/RunIsolatedRegressionGate.ps1`.
- **Problem:** Gate result classification does not consistently include platform completion state.
- **Evidence:** C# gate records aborted/canceled flags but can report success from a passed result. PowerShell exits on nonzero test status before
  consuming result evidence, misclassifying regressions as infrastructure/unexecuted failures.
- **Execution path:** Test passes → platform aborts during teardown → completion callback → gate classification.
- **Failure scenario:** A passed result exists, but the run as a whole did not complete normally.
- **Concrete consequence:** False PASS in C#; misleading failure accounting in PowerShell, whose overall exit still fails.
- **Why existing implementation does not prevent it:** The C# gate's own watchdog timeout already fails correctly; this is a different completion path.
- **Recommended remediation:** Make results and completion status jointly authoritative; preserve test-failure evidence before infrastructure classification.
- **Potential behavioural impact:** Previously accepted abnormal runs fail; diagnostics distinguish failed, aborted, canceled, skipped, and unexecuted.
- **Validation strategy:** Synthetic completion callbacks and result files for pass-then-abort, cancellation, ordinary failure, skip, and clean pass.
- **Measurement required:** NO.

### B05 — Benchmark entrypoints violate their option contracts

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Benchmark CLI, reproducibility.
- **Location:** Modes/options 35, 74–86, 115–159:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/TransitServerStressRunner.cs`.
  Fixed configuration 280–291:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Configuration/TransitBenchmarkConfig.cs`.
- **Problem:** Some modes ignore requested duration or cannot supply required runtime identity expectations.
- **Evidence:** Fake-server mode uses a fixed ten-second duration instead of a requested duration; sweep/forensic construction omits mandatory identity
  values and does not pass the CLI expectations through to the guard.
- **Execution path:** Documented CLI mode → new mode-specific options/configuration → identity guard/measurement.
- **Failure scenario:** A requested long run measures a shorter fixed interval, or a mode fails identity validation before doing work.
- **Concrete consequence:** Unreproducible or unreachable documented benchmark execution paths.
- **Why existing implementation does not prevent it:** Ordinary mode tests do not establish all entrypoint option propagation.
- **Recommended remediation:** Use one validated option set across modes and explicitly test each documented entrypoint.
- **Potential behavioural impact:** Duration and identity validation actually follow operator input.
- **Validation strategy:** CLI-level mode tests with distinctive duration/identity values and observed execution configuration.
- **Measurement required:** NO.

### B06 — Diagnostic telemetry does not faithfully observe execution

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED instrumentation defects; magnitude unmeasured.
- **Category:** Queue, latency, CPU, and logging measurement.
- **Location:** Queue publication 75–96:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Execution/BoundedArticleQueue.cs`.
  CPU attribution 125–205:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Metrics/RuntimeMetricsCollector.cs`.
  Epoch/first-event tracking 628–654, 939–977:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/Metrics/MeasurementMetricsCollector.cs`.
  Logging count 165–174, 276–290:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/AsyncSinkStressRunner.cs`.
- **Problem:** Metrics observe different events or time populations from their names.
- **Evidence:** Queue item visibility precedes occupancy increment; some timestamps follow publication. CPU diffs changing lifetime process sets
  and selects NNTPD by name, not endpoint ownership. First-after-measurement can use first-ever category timestamps. Logging Written counts a sibling
  synchronous sink, not completion of the asynchronous Console/File sinks.
- **Execution path:** Concurrent enqueue/dequeue, warmup-to-measurement transition, process churn, or async log output.
- **Failure scenario:** Reader wins before count increment; process exits/appears; category first occurs in warmup; actual sink stalls while sibling count advances.
- **Concrete consequence:** Negative queue telemetry, invalid latency/CPU attribution, and unsupported log-delivery/loss claims.
- **Why existing implementation does not prevent it:** Synthetic telemetry contracts do not force publication races or distinguish observation points.
- **Recommended remediation:** Define observation points and epochs; report unavailable attribution rather than substitute unrelated counters.
- **Potential behavioural impact:** Historical metrics may change; benchmark workload behavior need not.
- **Validation strategy:** Deterministic queue race, known timestamps across warmup, controlled process churn, and intentionally stalled async sinks.
- **Measurement required:** YES.

### B07 — Valid-yEnc parser benchmark fixture is corrupted by ASCII encoding

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED.
- **Category:** Benchmark workload validity.
- **Location:** Fixture generation/encoding 123–141, 210–231, 299, 417–461:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Benchmarks/NntpArticleParserBenchmarks.cs`.
- **Problem:** A success-path benchmark can measure validation rejection instead.
- **Evidence:** Encoded binary characters are converted through ASCII, replacing non-ASCII bytes, while the original CRC remains.
- **Execution path:** Generate yEnc → stringify/ASCII encode → parser benchmark.
- **Performance scenario:** The fixture label indicates valid yEnc, but bytes no longer match encoded length/CRC intent.
- **Concrete consequence:** Timings do not represent the claimed production success workload.
- **Why existing implementation does not prevent it:** Setup does not assert the intended parse/validation result.
- **Recommended remediation:** Preserve encoded bytes and validate success, decoded length, and CRC before measurement.
- **Potential behavioural impact:** Benchmark results must be rebaselined; no production parser change is implied.
- **Validation strategy:** Setup assertions and independently known encoded fixture, then rerun matching identity/configuration.
- **Measurement required:** YES for subsequent performance conclusions.

### B08 — Build reproducibility depends on ambient SDKs

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED declared-toolchain gap; current hosted CI failure not asserted.
- **Category:** Build/CI reproducibility.
- **Location:** Language version 40–42:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.csproj`.
  SDK setup 34, 268, 309:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/.github/workflows/build.yml`.
  SDK setup 31:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/.github/workflows/code-coverage.yml`.
- **Problem:** Declared SDK setup does not establish the compiler/solution capabilities the repository uses.
- **Evidence:** C# 13 and the solution format require capabilities not supplied by SDK 8 alone; no global SDK pin establishes selection.
- **Execution path:** Clean environment follows documented/workflow SDK setup → restore/build solution.
- **Failure scenario:** Only the declared SDK is installed, unlike a hosted image with additional ambient newer SDKs.
- **Concrete consequence:** Reproducibility differs by runner image rather than explicit repository toolchain.
- **Why existing implementation does not prevent it:** `net8.0` target does not imply SDK 8 supports newer compiler/solution features.
- **Recommended remediation:** Pin a supported SDK and align workflows while retaining intended .NET 8 runtime targeting.
- **Potential behavioural impact:** Explicit compiler selection may surface diagnostics previously dependent on ambient versions.
- **Validation strategy:** Clean environment containing only the pinned SDK; build production/tests/benchmarks under required platforms.
- **Measurement required:** NO.

### B09 — Success-path validation tests contain invalid mandatory-ACME fixtures

- **Severity:** MEDIUM.
- **Confidence/type:** CONFIRMED static contradiction, not executed test results.
- **Category:** Test reachability, configuration migration.
- **Location:** `BuildConfiguration` 3017–3069:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Tests/Startup/Validation/ProgramValidationSemanticsTests.cs`.
  Fixtures 52–97:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Tests/Startup/Validation/TransitServerValidatorIsolationTests.cs`.
  Fixtures 237–309:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Tests/Startup/Validation/ArticleRetentionPhysicalMemoryPolicyTests.cs`.
- **Problem:** Tests intended to reach successful validation/dependency phases provide configuration rejected earlier.
- **Evidence:** Mandatory ACME values remain at rejected defaults, including the PFX export password placeholder, without a valid fixture override.
- **Execution path:** Intended success fixture → current mandatory ACME validation → configuration error before target dependency behavior.
- **Failure scenario:** Tests assert valid configuration or later dependency outcomes despite the earlier invalid input.
- **Concrete consequence:** Test names/assertions do not establish protection of their intended scenarios.
- **Why existing implementation does not prevent it:** Placeholder-rejection tests confirm the current validation rule; stale disabled-mode assumptions do not bypass it.
- **Recommended remediation:** Supply deterministic valid ACME fixtures and keep isolated configuration testing away from real external issuance.
- **Potential behavioural impact:** Test-only correction; do not weaken mandatory TLS/ACME validation.
- **Validation strategy:** Execute affected existing suites and demonstrate success paths reach the intended probe boundary.
- **Measurement required:** NO.

## 6. Low/Informational Findings

### L01 — Production bypasses obsolete-key rejection

- **Severity:** LOW.
- **Confidence/type:** CONFIRMED.
- **Category:** Configuration migration.
- **Location:** IConfiguration overload 183–194:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Configuration/ConfigurationValidator.cs`.
  Production binding 88–96:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Startup/Validation/StartupValidationPipeline.cs`.
- **Problem:** Obsolete `LetsEncrypt.Enabled` rejection is not reached by actual binding flow.
- **Evidence:** Production and validate-config bind an object before invoking the object overload, losing the obsolete key.
- **Execution path:** Legacy configuration → binding ignores removed property → object validation → mandatory issuance behavior.
- **Failure scenario:** An older configuration includes Enabled=false expecting the explicit migration error.
- **Concrete consequence:** The intended explanatory rejection is bypassed; mandatory DNS/certificate work may proceed.
- **Why existing implementation does not prevent it:** Direct IConfiguration-overload tests do not exercise production callers.
- **Recommended remediation:** Check obsolete inputs before binding; do not restore optional TLS.
- **Potential behavioural impact:** Legacy configurations fail clearly rather than silently ignoring the key.
- **Validation strategy:** Actual startup and operational validation commands with the obsolete key.
- **Measurement required:** NO.

### L02 — Valid MySQL separators become false alias conflicts

- **Severity:** LOW.
- **Confidence/type:** CONFIRMED.
- **Category:** Configuration parser parity.
- **Location:** Raw parsing 525–529, extraction failure 664–671:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Configuration/MySqlConnectionStringUtilities.cs`.
- **Problem:** Effective provider parsing and startup field extraction accept different separator syntax.
- **Evidence:** Provider-valid leading/consecutive separators pass DbConnectionStringBuilder, but raw parsing rejects them and extraction fails.
- **Execution path:** Provider-valid connection string → raw field extraction → failed extraction translated into alias-conflict validation.
- **Failure scenario:** A single-server connection string contains a leading separator, with no actual conflicting aliases.
- **Concrete consequence:** Startup is blocked by a misleading conflict error.
- **Why existing implementation does not prevent it:** Tests recognizing valid separators call effective parsing, not the field extractors used by startup.
- **Recommended remediation:** Accept provider-valid empty separators while retaining real duplicate/alias conflict detection.
- **Potential behavioural impact:** Additional provider-valid forms become accepted without weakening conflict rules.
- **Validation strategy:** Extractor/validator parity for leading, repeated, trailing, whitespace-separated delimiters and actual conflicts.
- **Measurement required:** NO.

### L03 — Certificate-directory whitespace normalization is inconsistent

- **Severity:** LOW.
- **Confidence/type:** CONFIRMED.
- **Category:** Configuration normalization.
- **Location:** Key path validation 2906–2908:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Configuration/BackFillerOptions.cs`.
  Operational normalization 99–106:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Configuration/OperationalDirectoryValidator.cs`.
- **Problem:** Absolute and relative certificate-directory input are not normalized consistently before dependent checks.
- **Evidence:** One path trims relative but not absolute input; operational validation trims both.
- **Execution path:** Padded absolute certificate path → account-key validation versus directory validation.
- **Failure scenario:** A valid key exists at the trimmed absolute directory.
- **Concrete consequence:** Configuration rejects a location another validation path treats as valid.
- **Why existing implementation does not prevent it:** Existing whitespace tests focus on log-directory behavior.
- **Recommended remediation:** Canonicalize once before all directory/key consumers.
- **Potential behavioural impact:** Padded valid paths become consistently accepted or consistently rejected by one documented rule.
- **Validation strategy:** Absolute/relative paths with surrounding whitespace and existing/missing key cases.
- **Measurement required:** NO.

### L04 — Opt-in corpus corruption helper overruns its destination

- **Severity:** LOW.
- **Confidence/type:** CONFIRMED.
- **Category:** Test-data tooling.
- **Location:** `CorruptDeterministically` 243–259:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller.Tests/Runtime/Articles/Acquisition/NntpArticleAcquisitionCorpusGenerationTests.cs`.
- **Problem:** Destination size cannot contain the suffix the helper copies.
- **Evidence:** Allocation is source.Length + 16; appended suffix is 36 bytes.
- **Execution path:** Enabled corpus acquisition succeeds → corruption helper → BlockCopy.
- **Failure scenario:** Required nonempty sample set reaches its first corrupt sample generation.
- **Concrete consequence:** Opt-in generation necessarily throws after successful downloads.
- **Why existing implementation does not prevent it:** Ordinary CI bypasses the opt-in generator.
- **Recommended remediation:** Allocate from actual suffix length and verify the produced sample is invalid for the intended reason.
- **Potential behavioural impact:** Test tooling only; no application behavior change.
- **Validation strategy:** Offline helper test independent of credentials/network acquisition.
- **Measurement required:** NO.

### L05 — Some evaluated certificate bundles are not disposed

- **Severity:** LOW.
- **Confidence/type:** CONFIRMED ownership omission.
- **Category:** Native-resource lifetime.
- **Location:** Evaluation/replacement/fallback 117–152, 172–198:
  `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Runtime/Certificates/BackFillerCertificateProvisioningService.cs`.
- **Problem:** A bundle transferred from evaluation has no explicit disposal when not reused or published.
- **Evidence:** Reuse/fallback transfers it, but successful replacement and some cancellation paths leave the evaluated certificate undisposed.
- **Execution path:** Evaluate current certificate → issue replacement or cancel → unused evaluated bundle.
- **Failure scenario:** Renewal succeeds or aborts after evaluation.
- **Concrete consequence:** Nondeterministic cleanup of certificate/native resources; no large sustained leak magnitude is established.
- **Why existing implementation does not prevent it:** Ownership transfer on other branches does not cover unused bundles.
- **Recommended remediation:** Dispose every untransferred bundle explicitly.
- **Potential behavioural impact:** No intended certificate-selection change; preserve state ownership on fallback/reuse.
- **Validation strategy:** Ownership assertions across replacement, reuse, failure fallback, and cancellation.
- **Measurement required:** NO.

### L06 — Private-key-formatted repository artifact needs classification

- **Severity:** INFORMATIONAL.
- **Confidence/type:** OBSERVATION; live credential status unknown.
- **Category:** Sensitive-artifact hygiene.
- **Location:** `/home/runner/work/VectorNNTP.BackFiller/VectorNNTP.BackFiller/VectorNNTP.BackFiller/Docs/account.key`.
- **Problem:** Security review identified private-key PEM formatting, but provenance/use was not established.
- **Evidence:** Only artifact type is recorded; no key material is reproduced in this report.
- **Execution path:** Unknown; a production or fixture consumer was not established.
- **Failure scenario:** If live or formerly trusted account material, repository exposure matters; inert fixture material has a different interpretation.
- **Concrete consequence:** Conditional credential exposure, not a confirmed live compromise.
- **Why existing implementation does not prevent it:** The available evidence does not classify the artifact as inert or establish rotation history.
- **Recommended remediation:** Authorized owners should classify it; rotate/revoke if live, otherwise document fixture status without publishing more material.
- **Potential behavioural impact:** Rotation could affect external account access and must follow owner-controlled procedures.
- **Validation strategy:** Authorized credential inventory/use/history assessment; do not test it against external services without authorization.
- **Measurement required:** NO.

## 7. Performance & Allocation Assessment

### 7.1 End-to-end throughput ceiling

The production ARTICLE path is serial (M09). Its source-level upper-bound model is:

> sustainable article rate ≤ 1 / mean sequential per-article service time

That service time includes acquisition, relevant release/reconnect work, parsing/materialization, retention/Transit admission,
response publication, and source settlement. Network throughput alone cannot remove those waits.
This is a model, not a measured rate. One stalled provider/publisher introduces head-of-line blocking across unrelated backbones.

### 7.2 Significant byte and object paths

| Stage | Observed allocation/copy behavior | Frequency/lifetime | Assessment |
|---|---|---|---|
| Broker callback | Owned body copy after size/admission checks | Per admitted request; until delivery processing completes | Required borrowed-body lifetime boundary |
| Acquisition | Pooled buffer starts small and grows geometrically | Per article; old/new arrays coexist during growth | Large transient memory possible |
| Parser | Slices/views largely alias acquisition memory | Until canonicalization/processing releases input | Useful byte-preserving design |
| Canonicalizer | Second pooled whole-article buffer | Original and canonical buffers coexist during materialization | Approximately another article-sized live buffer |
| Retention | Central ownership of canonical buffer | TTL/completion/pressure plus active leases | Payload cap is meaningful but not RSS cap |
| Transit queue | Work identity/metadata, not independent payload copies | Queued and active lifetime | Good separation of queue metadata and body ownership |
| Transit staging | Framed/dot-stuffed bytes and batch lists/leases | Through batch flush/completion or failure cleanup | Slow tail retains batch resources |
| yEnc | Linear decoding/CRC; bounded stack batching | Per encoded article | No full decoded heap-body copy identified |
| Message-ID hashing | Encoding, digest, hex-related objects; repeated identity work | Per relevant lookup/admission/response path | Opportunity only after profiling |
| Async logging | Event/enrichment/sink buffers | Depends on rate and sink drain | Blocking-full policy affects latency and liveness |

Pool ownership is preferable to allocating large fresh arrays per operation, but does not remove copy bandwidth or retained pool memory.
No claim is made that every allocation is avoidable or a defect.

### 7.3 CPU complexity and scheduling

- Parsing, canonicalization, CRC, and dot transformation are primarily linear in the processed byte count.
- yEnc CRC validation uses small stack batches rather than materializing the full decoded file.
- Transit dot-stuffing performs sizing and transformation passes. Approximate staged frame bytes are payload length plus leading-dot additions,
  Message-ID framing, and possibly a trailing CRLF; payload shape changes CPU/copy cost.
- A worker's batch completion waits make slowest-response latency relevant to throughput and lease lifetime.
- Five-millisecond capacity polling implies approximately **200 waiter checks per waiter-second**, derived, not observed.
- Normal Transit metadata scales approximately with queued items plus connection count × depth plus pre-admission waiters.
  Queue capacity alone does not bound all registered waiter metadata; upstream production serialization narrows current amplification.
- M03 adds stale-token scanning under sustained idle operation; that is a demonstrated growth mechanism, not speculative micro-optimization.

### 7.4 Optimization opportunities, not additional confirmed defects

Repeated hashing/encoding, allocation of per-batch collections, cancellation registrations, task-completion sources, buffer resizing,
and lock contention warrant measurement under realistic traffic. Avoid optimizing them before correcting ownership and measurement errors.
Certificate export/import cloning per connection is also a connection-churn CPU/allocation hypothesis, not an established bottleneck.
No percentage improvement, CPU budget, or line-rate claim is justified from this static review.

## 8. Memory & GC Assessment

### 8.1 Ownership and coexistence

Normal article ownership is:

**acquisition buffer → parser views → canonical buffer → retention → Transit/listener read leases → physical disposal/pool return.**

During geometric growth, old/new acquisition arrays coexist. During materialization, acquired and canonical arrays coexist.
Transit staging adds another representation. Multiple listener requests can share one retained payload through leases rather than copying it.
Tasks, queued writes, and batch lists extend reference lifetimes; timeout/connection failure matters as much as initial allocation.

### 8.2 Important disproved concern

Retention's payload accounting **includes logically removed entries while active leases retain physical ownership**.
TTL/pressure removal therefore does not automatically free logical byte capacity while leaving uncounted leased payload behind.
It would be incorrect to report that leases bypass the configured retention payload bound.

The bound does not include pool bucket rounding, arrays cached after return, acquisition/materialization transients, Transit staging,
metadata, TLS/native buffers, socket queues, or RabbitMQ client storage. These explain why RSS can exceed reported retained payload without a retention leak.

### 8.3 Derived listener capacity model

Default configuration permits approximately:

- 1,024 connections × 64 outstanding requests = **65,536 request contexts**.
- 1,024 × 8 processing permits = **8,192 aggregate per-session permits**.
- 1,024 × 64 MiB queued/in-flight Found reservation = **64 GiB aggregate reservation allowance**.

The final number is **not necessarily 64 GiB of distinct allocated payload**: multiple reservations may reference the same retained bytes.
It demonstrates that per-session limits are not a whole-process budget.
ACK-wait leases can outlive Found-byte reservations; H10 can prolong those lifetimes and H07 can incorrectly release them.

### 8.4 GC implications and unknowns

Large article arrays readily exceed typical LOH thresholds. Long leases, pool caches, and staging may retain objects into older generations.
Possible outcomes include higher live-set scanning, delayed memory return, and allocation stalls under pressure; none was measured here.
Pinning, LOH fragmentation, Gen2 collection frequency, and RSS stabilization require runtime traces.
Do not claim a stable pool-backed allocation rate proves bounded resident memory.

Memory validation should reconcile acquired array capacity, logical payload length, retained bytes, active leases, staged bytes,
application queues, broker-client queues, managed heap, and RSS separately.

## 9. Concurrency & Lifecycle Assessment

### 9.1 Existing protections

Admission gates, generation-aware settlement, retention leases, explicit lifecycle states, and the processing-drain barrier are meaningful defenses.
The barrier narrows normal shutdown overlap between processing and Transit; it does not protect every runtime fault-forced terminalization path.

### 9.2 Repeated failure pattern

The recurring pattern is:

> Publish a state claiming ownership changed, then separately perform the counter/resource/completion operation that makes it true.

Examples are Transit Claimed before in-flight increment, batch retirement reservations before execution, `_disposeRequested` before disposal,
and numeric listener IDs reused before a delayed timer is attached. NNTP Busy conflates active article, maintenance, and retirement ownership.

H01–H08 and M01–M06 include production interleavings and missing defenses. These should be reproduced with controlled scheduling rather than sleeps.

### 9.3 Findings not promoted

- A concurrent internal `ListenerProtocolSession.DisposeAsync` versus `RunAsync` race was considered. Current socket wiring awaits Run and did not
  establish that disposal overlap; it is not reported as a production defect.
- A shutdown-coordinator state-publication versus disposal race was considered. A surviving production consumer failure was not sufficiently
  established after callback/disposal ordering was examined.
- Account-manager orphaning was narrowed to its actual ownership gap; normal host termination closes process resources.
- Active-lease accounting is definitely wrong, but normal account-removal draining narrows premature-session-close claims.

These qualifications avoid treating every internal API interleaving as a reachable production failure.

## 10. Networking/Protocol Assessment

### 10.1 Sound mechanisms

- Acquisition explicitly implements line framing, disconnect handling, and transport dot-unstuffing.
- Listener parser accumulation, request count, descriptor queues, and connection count have bounds.
- Request/result identity checks prevent wrong-Message-ID publication.
- Certificate cloning occurs under the state lock, and each connection owns its clone; replacement-vs-handshake disposal was not established as a defect.
- Transit distinguishes definitive acceptance/rejection from uncertain transmission instead of indiscriminately retrying.

### 10.2 Important gaps

Response timeout is not a write timeout (H06); connection count is not connection tenure (H10); graceful cancellation is not a drain protocol (H08).
The acquisition/yEnc and parser/materializer boundaries disagree on representations (M13/M15).
Folded semantic headers and limit enforcement need composition testing, not only parser-isolated tests.
Certificate-chain serving after reload needs clean-machine TLS verification (M22).

Partial read/write, slow peer, half-close, disconnect, malformed input, and oversized input should be tested through actual transports,
including task/lease cleanup, not merely exception type assertions.

## 11. RabbitMQ Assessment

### 11.1 Verified strengths

Borrowed delivery memory is copied before lifetime escape; oversized requests can be rejected before copying.
Settlement checks original channel generation. Application delivery buffering is bounded.
Response publication precedes source settlement, and the code attempts to distinguish retryable publication failure.

### 11.2 Semantic and lifecycle gaps

H01–H03 concern generation cancellation, recovery state, and retirement liveness.
M05 concerns deterministic connection cleanup.
M10 distinguishes permanently nonreplyable requests from transient publication failure.
M11 separates broker acceptance from routing.
M12 requires the service's actual responses to satisfy its own schema.
M17 distinguishes application buffering from broker/client prefetch.

Automatic client recovery is disabled in favor of application-managed recovery; therefore stranded application recovery is especially significant.
Channel count, connection count, consumer count, prefetch, application queue capacity, and article concurrency must be reported separately.

“Exactly once” here should mean explicit local ownership/settlement accounting, not a claim of distributed exactly-once delivery.
Broker disconnect around publication/ACK necessarily creates retry/uncertainty decisions; preserve those semantics during remediation.

## 12. Retention Assessment

Retention separates Message-ID/digest indexes, logical readability, physical ownership, read leases, TTL/pressure, and two completion channels.
The physical accounting and lease model are valuable, and digest collisions are checked against identity.
MD5 lookup use alone did not establish a collision-driven wrong-article bug.

M08 is the central contract question: does a new successful response create a new retrieval obligation, or does identity-wide completion survive
re-admission? New entries reset flags, but the explicit completion API is Message-ID-based. Implementation structure alone does not settle policy.

Additional qualified observations:

- Logical removal can leave bytes unavailable for new readers but still owned by old readers; this is intentional lease protection.
- Insertion-age ordering and UTC clock behavior deserve skew/reordering tests before making strict expiry-latency claims.
- Capacity failure while logically removed leases remain is not automatically a leak; report the responsible outstanding readers.
- Pressure eviction should not be redesigned solely to satisfy benchmark throughput; preserve the actual retention policy.

## 13. Transit Assessment

Transit's identity-only queue is a useful memory architecture. It depends on sound transitions through admission, claim, lease acquisition,
staging, flushing, response, uncertain failure, reconnection, and disposal.

H04–H06 show that worker supervision, accounting/completion, and transport-fault termination are not yet sufficient.
M07 shows timeout policy is not aligned with work lifetime. M24 shows diagnostics cannot currently be taken at face value.

Additional observations requiring policy or reproduction before promotion:

- Duplicate Message-IDs within one unsent batch can make registration throw and affect ambiguity classification of other registered items.
  Existing tests permit duplicate ambiguity; intended duplicate semantics need agreement before declaring a contract violation.
- Deferred disposal tracking retains entries until shutdown, scaling with retired connections.
- QUIT/pipe completion before transport closure deserves a blocked-write disposal test.
- Latent retry paths were not promoted without an established production scheduling path.

Any remediation must preserve 431/439 handling, definitive versus ambiguous outcomes, and the prohibition on blind retries after uncertain transmission.

## 14. Security Assessment

A dedicated security specialist reviewed exploitability concerns before parent synthesis.

### Established boundaries

- **Availability:** H10 is conditional on untrusted reachability; application deadlines do not protect listener slots.
- **Confidentiality:** M25 identifies network-trust-only retained-article access, not arbitrary acquisition or a proven violation of a documented private-network policy.
- **Artifact hygiene:** L06 records private-key formatting without claiming current validity, ownership, or compromise.

No additional confirmed vulnerability was returned by the focused workflow/dependency security review.
That result does not certify security:

- Advisory databases and transitive dependency source were not checked.
- Actual firewall, service identity, broker permissions, certificate store, and deployment configuration were unavailable.
- No exploit or credential-use test was executed.
- No payload, credential, or private-key contents are reproduced here.

Avoid framing reliability bugs as confirmed remote vulnerabilities without the missing exposure/trigger evidence.

## 15. Error/Failure Assessment

| Failure mode | Traced behavior / concern | Relevant findings |
|---|---|---|
| Consumer generation replaced | Session cancellation can escape sole processor | H01 |
| Consumer channel unusable | Recovery broker cancellation can strand Retiring | H02 |
| Retirement interrupted | Unstarted reserved operations never complete | H03 |
| Transit negotiation rejected | Cleanup erases classification state; worker exits | H04 |
| Runtime forced completion | Item state can outrun acquired accounting/completion | H05 |
| Transit peer stops reading | Response fault does not interrupt writer | H06 |
| Listener ID reused | Old timer can act on new request | H07 |
| Graceful shutdown during transfer | Shared cancellation aborts established I/O | H08 |
| Omitted binding | DNS desired state differs from listener semantics | H09 |
| Slow/idle listener peer | No per-client deadline | H10 |
| Account refresh during maintenance | Lease/retirement scheduling inconsistencies | M01–M04 |
| RabbitMQ shutdown signal | Actual disposal can be skipped | M05 |
| Logging sink stalls | Shutdown initiation may block before deadline | M06 |
| Missing reply metadata/destination | Poison retries or silently unroutable response | M10–M11 |
| Malformed request | Generated failure may violate schema | M12 |
| Parser-stage incompatibility | Rejection or exception after earlier stage success | M13–M16 |
| DNS query fails | Timeout mistaken for cancellation; quorum bypassed | M20 |
| ACME interruption | Stale record ownership not recoverable | M21 |
| Certificate reload on clean machine | Intermediate chain not explicitly preserved | M22 |

The required common taxonomy is caller cancellation, generation retirement, timeout, invalid input, transient transport failure,
definitive remote rejection, uncertain transmission, and fatal service failure. Broad catches and shared flags should not collapse those categories.

## 16. Observability Assessment

Structured logging, identity fields, and lifecycle events are existing strengths.
The need is not generic “more logging,” but truthful ownership and milestone observations:

- Live Transit worker count versus admission-open status, to expose H04.
- Requested shutdown versus fatal hosted-service failure and process exit reason, to expose H01.
- Queued/in-flight/pre-admission/terminal work reconciliation, to expose H05.
- Reserved-but-unstarted retirement age/count, to expose H03.
- Active article leases separately from maintenance Busy slots, to expose M01/M02.
- Idle scheduling token count versus usable sessions, to expose M03.
- Reply routing failure separately from publisher confirmation, to expose M11.
- Retained readable bytes versus logically removed leased bytes and actual array capacity, to explain memory pressure.
- Actual percentiles, coherent connection totals, and completed flush milestones, rather than M24/B06 substitutions.

Mandatory cleanup and shutdown signaling must not depend on successful logging.
Async-sink sibling counters do not prove file/console delivery or losslessness.
Preserve established event IDs/structured fields when fixing behavior unless a specific semantic correction requires change.

## 17. Maintainability/Architecture Assessment

Class size is not the basis for these concerns. The concrete engineering consequence is multi-responsibility ownership that makes local fixes
affect unrelated lifecycle states:

| Component | Coupled responsibilities | Demonstrated consequence |
|---|---|---|
| TransitPublisher | Admission, counters, worker supervision, retries, retirement, disposal, diagnostics | H04/H05 and M24 cross these boundaries |
| NNTP session manager | Article leases, idle queue, DATE maintenance, refresh, retirement, reconnect, drain | M01–M03 confuse Busy/scheduling/lease ownership |
| RabbitMQ consumer infrastructure | Broker lifecycle, generation settlement, admission, retirement reservations, hosted stop | H01–H03 cannot be proven by isolated channel tests |
| Startup configuration | Binding, annotations, custom parsing, obsolete-key checks, reserved settings, runtime rereads | M17–M19 and L01–L03 diverge by caller |
| Benchmark infrastructure | Duplicated budgets/queues, mode-specific setup, telemetry, timing, cleanup | Same bugs and semantics drift across variants |

Prefer explicit ownership/state-transfer boundaries and one authoritative configuration/measurement contract.
Do not add production interfaces solely to mock existing behavior or split classes merely to reduce line count.
Documentation that repeats names without describing these invariants is not a substitute for a coherent contract.

## 18. Testing/Benchmark Assessment

### 18.1 Important test counterevidence and blind spots

- RabbitMQ “without deadlock” tests manually reconcile and stop without starting the hosted loop; the actual retirement join is untested there.
- Closed/disposed channel fakes allow BasicCancel success, obscuring recovery failure.
- Listener timeout tests can terminate through EOF before the timer behavior they appear to name.
- Idle and active retirement tests do not combine their aggregate accounting.
- Keepalive success and retirement are not deterministically overlapped.
- Multi-slot concurrent initialization cancellation has an explicitly skipped scenario; a single-slot cancellation test is narrower.
- Timing-based “concurrent initialization” coverage does not introduce the assumed delay; quick sequential behavior can satisfy it.
- Schema contracts use handcrafted responses, missing production malformed-input composition.
- Canonicalization fixtures cover uniform boundaries, not parser-accepted mixed boundaries.
- yEnc test encoders escape every dot, excluding the disputed representation boundary.
- Certificate tests use self-signed/empty-chain material rather than persisted intermediates on clean Linux.
- ACME recovery fakes add ownership metadata absent from production requests.
- Some supposed-success startup fixtures are invalid under current mandatory ACME rules.

These are statements about what tests prove, not claims that the suites were executed and failed.

### 18.2 Benchmark validity

B01–B09 address work/time cohorts, budget ownership, cleanup, gate truthfulness, CLI propagation, telemetry, fixture validity, SDK reproducibility,
and invalid fixtures. Those are prerequisites for using results to size production or judge optimization.

Other observations not promoted to independent defects:

- Per-article synchronous console output exists in a measurement path and should be accounted for in workload cost.
- All-run latency collections grow with completions.
- Prepared identity metadata can be large and is outside payload queue budget.
- A finite prepared identity population can end a requested long run; artifacts should disclose actual offered count/duration.
- Some “peak” values are sampled post-drain rather than proven peaks.
- Acquisition benchmarks performing multiple operations per invocation require correct interpretation of per-operation normalization.
- In-process fake-server CPU is not independent production server CPU.

Historical campaign results and the performance checkpoint are evidence of past recorded work, not reproduced measurements of this commit.

## 19. CI/Dependency Assessment

### Positive controls

Production enables .NET analyzers and warning-as-error policy; style/code-quality categories are promoted.
Build, coverage, dependency review, and CodeQL workflows exist.
Benchmark contracts and isolated lifecycle tooling are represented in repository validation.

### Meaningful gaps

- B08: Explicit SDK setup does not establish the required compiler/solution toolchain.
- M23: Windows publishing does not validate Linux executable/unit behavior.
- B04: An assurance gate can misclassify abnormal completion.
- B09 and section 18: Tests can exist without exercising their intended production boundary.
- Coverage workflow scope does not establish per-PR protection of all runtime/benchmark invariants; no percentage substitutes for correct scenarios.
- Declared package versions were inspected statically, but no current advisory or transitive implementation audit was performed.

No current CI pass/fail status is asserted. There was no user-reported current workflow failure to diagnose in the original investigation.
No workflow, dependency, warning policy, test, or baseline was changed by this report.

## 20. Prioritized Remediation Roadmap

Priority reflects production impact, not cosmetic value. Proposed work should be split into independently validated changes.

### P0 — Immediate

1. Isolate delivery-generation cancellation and establish fatal process exit semantics (H01).
2. Make RabbitMQ consumer rollback and every reserved retirement complete ownership (H02/H03).
3. Make Transit ownership/state/completion transitions consistent and supervise worker loss (H04/H05).
4. Make connection faults terminate blocked writes without erasing uncertainty (H06).
5. Bind listener timers to request instances and separate drain from forced closure (H07/H08).
6. Prevent destructive empty DNS reconciliation from omitted binding (H09).
7. Restrict untrusted listener exposure and establish stalled-client deadlines (H10).

### P1 — Important production reliability

- Correct NNTP lease/maintenance/idle scheduling and account cleanup (M01–M04).
- Correct RabbitMQ disposal, nonreplyable-message policy, and response routing semantics (M05/M10/M11).
- Make mandatory shutdown signaling independent of diagnostics (M06).
- Correct Transit active timeout epochs (M07).
- Resolve retention identity/admission completion policy before changing it (M08).
- Align actual response schema, parser representations, and effective bounds (M12–M17).
- Freeze database inputs and make intended provisioning reachable (M18).
- Correct DNS fallback/quorum, TXT ownership, and certificate chain preservation (M20–M22).
- Validate real Linux packaging/unit behavior and the network trust boundary (M23/M25).

### P2 — Worthwhile assurance and throughput work

- Repair test fixtures, hosted lifecycle coverage, and abnormal-completion classification.
- Correct benchmark work/time windows, telemetry, fixture validity, option propagation, budgets, and cleanup (B01–B09).
- Correct Transit diagnostic semantics before tuning from them (M24).
- Pin build SDK and add relevant clean Linux/composed-path validation.
- Remove fictitious active/reserved setting relationships (M19).
- Measure global serialization, then introduce justified bounded concurrency/fair selection (M09).
- Address low-impact migration/path/parser parity and certificate disposal (L01–L05).
- Classify the sensitive artifact through authorized ownership procedures (L06; urgency increases if live).

### P3 — Optional after measurement

- Reduce repeated hashing/encoding and avoidable per-batch allocations where profiles show material cost.
- Replace polling admission if waiter overhead is significant.
- Consolidate duplicated benchmark scheduling/budget implementations.
- Improve non-obvious ownership documentation at repaired boundaries rather than adding boilerplate.

No implementation duration or performance-improvement estimate is justified or supplied.

## 21. Measurement/Profiling Plan

### 21.1 Establish valid accounting first

Define monotonic admission, write-start, write-complete, definitive-response, source-settlement, listener-send, and receipt-ACK timestamps.
Separate offered, admitted, transmitted, accepted, rejected, ambiguous, unavailable, canceled, and completed cohorts.
Report drain separately or include its time consistently with its completions. Validate metrics with a deterministic event ledger before profiling.

### 21.2 Composed workloads

Use real RabbitMQ and controlled NNTP/TLS peers around the actual hosted composition, not only Transit microbenchmarks.
Vary article-size distributions, near-limit payloads, yEnc/non-yEnc, dot-heavy content, folded headers, invalid input, and cache reuse.
Keep build configuration, runtime identity, x64 platform, topology, warmup, and measurement windows constant.
Preserve repository clean/build/runtime-identity/watchdog requirements for later executed campaigns.

### 21.3 Required observations

| Dimension | Observe separately |
|---|---|
| Throughput | Requests/s, articles/s, acquired bytes/s, transmitted bytes/s, accepted bytes/s |
| Latency | Queue, acquisition, parsing, materialization, publish-confirm, Transit response, listener transfer, receipt; p50/p95/p99 |
| CPU | Worker CPU versus fake/external server CPU; CPU per useful byte/article |
| Allocation/GC | Allocation rate, object types, Gen0/1/2, LOH, pauses, live set, pool capacity |
| Memory | Logical retained bytes, physically leased bytes, staged bytes, managed heap, native/TLS, RSS |
| Queues | Broker unacked/client buffers, delivery queue, pre-admission waiters, Transit states, idle tokens, listener descriptors |
| Connections | Configured, live, usable, initializing, retiring, faulted, and worker count |
| Outcomes | Source ACK/NACK/drop, response route/confirm, definitive/ambiguous Transit, listener receipt |

### 21.4 Failure and sustained-operation matrix

- Idle keepalive without acquisition, then burst acquisition.
- Account endpoint/capacity churn during active article and DATE operations.
- Consumer generation replacement during every processing/settlement phase.
- Full admission queues and forced Transit completion.
- Negotiation rejection after successful startup validation.
- Nonreading Transit/listener peers, slow TLS, idle sessions, delayed receipt ACKs, and numeric request-ID reuse.
- Missing reply destinations and irrecoverable request metadata.
- Stalled logging sinks before and during shutdown.
- DNS resolver loss, authoritative partial reachability, ACME interruption, clean-cache certificate reload.
- Shutdown at peak retained/staged bytes, including forced deadline.

Use controlled interleavings before soak tests. After each run, reconcile every admitted operation and every owned resource.
Only then optimize one measured bottleneck at a time. No profiling activity in this section has been executed for this report.

## 22. Repository Review Coverage

### 22.1 Inventory

The initial working-tree inventory contained **395 paths**, **287 C# files**, and **5 project files**.
This is an inspection inventory, not a claim of a git-verified tracked-file count.

| Area | C# inventory |
|---|---:|
| Production | 150 |
| Main tests | 72 |
| Benchmarks | 48 |
| Benchmark tests | 16 |
| Isolated C# gate | 1 |
| Total | 287 |

The solution, project references, build props, analyzer/style configuration, sample configuration, schemas, workflows, scripts,
documentation, fixture inventory, and repository configuration were included in the coverage process.

### 22.2 Inspection depth ledger

“Full” below means reviewers reported reading implementation bodies rather than only matching search results.
Overlap between areas means counts must not be summed as distinct files.
The review does not claim a per-line coverage proof for every artifact.

| Area | Inspection completed | Qualification |
|---|---|---|
| Program/startup commands/hosting/logging/accounts | Parent inspected execution paths, callers, configurations, and associated test evidence | A consolidated per-line ledger for every auxiliary test was not retained |
| Configuration/startup configuration/validation | 26 production files reviewed; follow-up completed 17 relevant test files including helpers | Some initial scans were expanded to full reads; other runtime tests belong to other areas |
| RabbitMQ/acquisition/grabber/processing | All 35 behavior-bearing source files reviewed | Full follow-up on 14 related test/helper files; adjacent areas separately assigned |
| Parsing/date/Message-ID/yEnc | All 18 production files reviewed; seven related test files fully read | Large corpus byte-level review limited as below |
| Retention/Transit | 12 production files and 11 adjacent tests/helpers fully read, plus result sink/options/composed integration paths | Internal latent paths not promoted without production reachability |
| Listener/lifecycle/shutdown/control plane | 13 production files and eight adjacent test files reviewed, plus runtime options | Some adjacent source was sampled and covered by other reviewers |
| Certificates | Every Runtime/Certificates source file fully read in follow-up | All three runtime certificate test files, TLS fixture, dependency-probe tests, and socket tests read |
| Benchmarks/benchmark tests | All 48 benchmark and 16 benchmark-test C# files read | Historical binaries/artifacts not re-executed or independently verified |
| Build/tooling | Five projects, three props, solution, relevant editorconfigs, all testing tools, build/coverage workflows | No builds/tests executed |
| Security/workflows/dependencies | Specialist reviewed security boundaries and focused four-workflow/dependency/policy scope | Specialist did not provide a complete per-line coverage ledger; advisory DB not queried |
| Documentation/repository configuration | Root guidance, samples, protocol/schema material, twelve MySQL/config docs, benchmark docs, changelog, performance checkpoint | Historical claims were not accepted as current measurements |

The full RabbitMQ-related test follow-up included consumer infrastructure, acquisition, workflow/session manager, processing, response factory,
publisher/sink, canonicalizer, wire protocol, and composed integration tests. The configuration follow-up included binding, MySQL extraction,
operational directories, retention policy, identity, Cloudflare, ACME, program validation, and Transit validation tests.

### 22.3 Explicit exclusions and partial inspection

- Thirteen corpus fixtures were inventoried; eight smaller fixtures were fully viewed, five larger fixtures only metadata/targeted-scanned.
- Placeholder/artifact directories were inventoried rather than treated as executable evidence.
- Generated/source-generated outputs were not produced or inspected; declarations and generator usage were reviewed in source.
- Agent-instruction directory contents were excluded.
- External dependency source and transitive implementation behavior were not exhaustively reviewed.
- Actual deployment configuration, firewalls, broker/server settings, certificate stores, and private corpus material were unavailable.
- Historical performance binary artifacts and current CI runtime results/logs were not validated.
- Credential validity/use was not tested.

Accordingly, this is broad whole-application forensic coverage with explicit residual gaps, not a false certification that every byte and dependency
was exhaustively audited. The original exhaustive objective exceeds what static repository inspection alone can establish.

## 23. Review Limitations and Unknowns

### 23.1 Static versus observed behavior

CONFIRMED findings are source-proven conditional paths, not executed reproductions.
No allocation, throughput, CPU, latency, memory-growth, GC, broker, TLS, systemd, or network measurements were invented.
Derived counts/formulas are labeled and must not be mistaken for observed resident memory or performance.

Unverified conditions include actual RabbitMQ.Client behavior on specific dead-channel operations, Linux certificate import/chain behavior,
listener network exposure, fatal-exit supervisor behavior, and strict external yEnc leading-dot interoperability.
Retention per-admission completion policy remains unresolved.

### 23.2 Adversarial changes to conclusions

The final pass actively sought defensive code, tests, ordering, and contract counterevidence:

- Retention leased bytes remain counted: an alleged cap bypass was rejected.
- Retention cross-generation completion was downgraded because the API explicitly completes by identity.
- Internal concurrent listener disposal was not treated as a demonstrated production call path.
- Shutdown-coordinator publication/disposal concern was not promoted without a surviving production consequence.
- Orphan manager consequences were narrowed to deterministic-cleanup loss, ordinarily until process exit.
- NNTP lease accounting is wrong, but normal consumer draining narrows premature-disposal claims.
- First-account monopolization was downgraded in light of the globally serial production processor.
- Exact `=ybegin ` detection means malformed-stem detection was not promoted as a parser contract defect.
- Double unstuffing remains an internal integration mismatch, not an unqualified claim about every conforming yEnc encoder.
- Fake-server backlog/cleanup concerns were separated from public production security and normal bounded publisher flow.
- Gate watchdog timeout already fails correctly; only the different abnormal platform-completion path remains.
- No current dependency CVE, live secret, deployed unit failure, or CI failure was asserted without the required evidence.

### 23.3 Remaining verification requirement

Every remediation should begin with the specified regression scenario and preserve original ownership, ACK/NACK, retry uncertainty,
byte-preservation, TLS, and shutdown contracts. Broad refactoring or performance expansion is not a substitute for proving each invariant.
The existence of this report does not certify that the application passes tests or meets a production SLO.

## 24. Final Engineering Assessment

The codebase has strong local concepts but insufficiently reliable composition under cancellation, queue pressure, connection failure, and shutdown.
The central engineering priorities are:

1. Every admitted operation reaches a terminal result or explicit abandonment exactly once.
2. Every created resource remains owned/tracked until disposal completes.
3. Every failed worker either recovers or closes admission visibly.
4. Every timeout terminates the operation it claims to bound.
5. Graceful shutdown closes admission without prematurely destroying active work.
6. Configuration and diagnostics describe actual runtime behavior.
7. Tests and benchmarks observe the production boundaries and outcomes they claim to validate.

Increasing concurrency before repairing these invariants is likely to amplify resource and failure exposure.
After correctness and evidence quality improve, the identity-only Transit queue, retention lease model, and existing protocol infrastructure
provide a useful basis for measured throughput work.

**Final disposition:** Prioritize the P0/P1 ownership and recovery findings, reproduce them deterministically, and repair benchmark assurance before
using current performance claims for capacity planning. No application fixes are included in this report.
