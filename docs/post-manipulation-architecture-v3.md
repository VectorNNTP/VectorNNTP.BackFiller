# VectorNNTP.BackFiller — Post-Manipulation Architecture Redesign (v3)

## Status
Proposed architecture baseline (implementation-agnostic, invariant-driven)

## Scope
Post-manipulation only:
- canonical article handoff
- lifecycle coordination
- retention ownership
- listener serving
- transit scheduling/delivery facet
- settlement semantics
- restart/recovery correctness

---

## 1) Executive Decision

The system adopts a **Listener-first, Transit-parallel** architecture with a single lifecycle authority.

### Priority hierarchy
1. **Listener (latency-critical)**
2. **Transit (best-effort background)**

### Ingestion success contract
An article is successfully ingested when:
1) canonical payload is retained, and  
2) ALC commits `LISTENER_VISIBLE`.

Transit outcome is orthogonal and must not gate ingestion success.

---

## 2) Architectural Invariants (Highest Authority)

These invariants are non-negotiable.

1. ALC is the sole authority for **primary lifecycle state**.
2. Retention is the sole authority for **canonical payload bytes**.
3. Listener and Transit are **sibling consumers/facets**.
4. Transit can never gate, delay, regress, or invalidate `LISTENER_VISIBLE`.
5. Transit state can never determine upstream ingestion success.
6. Listener cannot wait for Transit or consume Transit-controlled resources.
7. Transit cannot consume Listener-critical resources.
8. Consumers report events; consumers do not mutate lifecycle state.
9. A stale/late event can never mutate a newer lifecycle version.
10. Event ordering is never required for correctness.
11. A lease grants temporary operation authority, not lifecycle ownership.
12. `LISTENER_VISIBLE` means eligible to serve, not that serving has occurred.
13. The only intentional shared dependency between Listener and Transit is Retention.
14. No synchronous call on Listener critical path may transit into Transit-owned code/state/synchronization.
15. Queue never owns canonical payload bytes.
16. ALC records authorization/state; it is not a scheduler.
17. Existing implementation structure is not an architectural constraint; rewrite/refactor is permitted to satisfy invariants.

---

## 3) Primary Lifecycle vs Transit Facet (Separate State Domains)

### 3.1 Primary lifecycle (authoritative ingestion/availability domain)
`NEW -> RETAINING -> RETAINED -> LISTENER_VISIBLE -> EXPIRED|EVICTED|FAILED_ADMISSION`

Transit does **not** advance this lifecycle.

### 3.2 Transit facet (parallel domain)
`NOT_REQUIRED | NOT_ENQUEUED | DEFERRED -> ENQUEUED -> INFLIGHT -> DELIVERED | RETRY | EXHAUSTED`

Transit facet state is separate from primary lifecycle.

### Prohibited cross-domain mutations
Invalid by definition:
- `TRANSIT_EXHAUSTED -> primary FAILED`
- `TRANSIT_INFLIGHT -> primary RETAINED`
- `TRANSIT_DELIVERED -> primary COMPLETED` (no such primary meaning)

---

## 4) Formal Definition of `LISTENER_VISIBLE`

`LISTENER_VISIBLE` means:

> ALC has committed that the article is eligible for Listener serving, subject to Retention confirming payload availability at read time.

It does **not** imply:
- a Listener request occurred,
- a Listener serve succeeded,
- a Listener connection exists,
- a Listener ACK occurred,
- Transit is queued/succeeded.

---

## 5) Identity Contract

### 5.1 ArticleKey immutability
- Created exactly once post-manipulation.
- Never replaced downstream.
- Never mutated for article lifetime.

### 5.2 Identity fields
- `ArticleKey` (primary identity)
- `MessageIdMd5Ascii32` (lookup representation only, not lifecycle identity)
- optional canonical metadata (`CanonicalHash`, `CanonicalLengthBytes`)

### 5.3 Distinct identity concepts (must never be conflated)
- `ArticleKey`
- `PrimaryLifecycleVersion`
- `TransitFacetVersion`
- `LeaseId`
- `EventId`
- `TransitAttemptId`

### 5.4 Collision/conflict behavior
On identity conflict/collision:
- reject transition,
- emit conflict event,
- never overwrite authoritative record unsafely.

---

## 6) Version Semantics

### 6.1 Dual-version model
Per `ArticleKey`:
1. `PrimaryLifecycleVersion` (primary transitions only)
2. `TransitFacetVersion` (transit facet transitions only)

Transit retries must not churn primary lifecycle version.

### 6.2 Meaning of versions
Versions are **concurrency/idempotency tokens only**.  
They are **not** business/lifecycle stage counters.

No correctness rule may infer lifecycle meaning from numeric distance (e.g., “v17 is 1 stage beyond v16”).  
Authoritative progress is explicit state, not version magnitude.

### 6.3 Mutation rule
Accepted mutation requires version match (optimistic CAS/serialized equivalent).  
Stale version mutation is rejected without side effects.

---

## 7) Event Model and Idempotency Contract

Every externally reported event includes at minimum:
- `ArticleKey`
- `EventId`
- `EventType`
- relevant observed version (`PrimaryLifecycleVersion` and/or `TransitFacetVersion`)
- optional `LeaseId`
- optional `TransitAttemptId`
- timestamp

### Guarantees
1. Duplicate events are expected and safe.
2. Out-of-order events are expected and safe.
3. Unknown/stale events are rejectable without side effects.
4. Event ordering is not required for correctness.
5. Event handling must be safely repeatable or safely rejectable.

---

## 8) Lease Semantics (Formal)

### 8.1 Lease types
- `ReadLease` (Listener)
- `SendLease` (Transit)

### 8.2 Lease rules
- explicit acquire
- optional renewal policy (implementation-defined)
- bounded TTL
- explicit release
- duplicate/stale release is safe no-op/reject
- lease invalid when bound version no longer matches
- lease never transfers lifecycle ownership

### 8.3 Maximum absolute lease lifetime (mandatory)
Renewal must not permit unbounded lifetime.  
Each lease type has a configured **maximum absolute lifetime**.  
At max lifetime:
- operation must complete, or
- reacquire under current state/version policy.

Applies to both Listener and Transit leases.

### 8.4 Crash/restart lease behavior
- leases do not survive as ownership rights
- restart invalidates stale leases deterministically (epoch/version policy)
- stale in-flight operations fail/retry safely

### 8.5 Lease expiry during active operation
If lease expires mid-operation, outcome must be deterministic and reported; unauthorized lifecycle mutation remains prohibited.

---

## 9) Durability and Recovery Model (Correctness Requirements)

Storage mechanism is implementation-defined; correctness is mandatory.

### 9.1 Authoritative state requirement
ALC state must be either:
- durable, or
- deterministically reconstructable from durable data with sufficient metadata to preserve invariants.

### 9.2 Must survive restart (or be reconstructable)
- primary lifecycle state
- transit facet intent/state sufficient for re-scheduling required transit
- version counters
- idempotency/dedupe identifiers within required window
- settlement correlation needed for safe re-emit/reject logic

### 9.3 Restart rules
- stale leases invalidated deterministically
- no post-restart regression below authoritative truth
- committed `LISTENER_VISIBLE` remains valid if payload retained
- transit recovery resumes asynchronously; listener path not blocked

### 9.4 Critical crash windows
Must remain correct for crashes:
- after retention commit, before listener-visible commit
- after listener-visible commit, before settlement emit
- after transit enqueue intent, before enqueue side effect
- after transit success, before outcome report

---

## 10) Synchronous vs Asynchronous Boundary

### 10.1 Synchronous / Listener-critical
Allowed:
- ALC serve lookup
- retention availability check
- ReadLease acquire
- payload read/stream

Forbidden:
- transit queue interaction
- transit state/health lookup
- transit locks/synchronization
- transit scheduler/retry logic
- waiting on transit async outcomes

### 10.2 Asynchronous / background
- transit enqueue/defer/retry
- transit outcomes/terminalization
- non-critical telemetry
- reconciliation loops

Retention eviction notifications may be async and must be safely serialized by ALC.

---

## 11) Dependency Direction Contract

Allowed:
- `Listener -> ALC` (serve authorization/snapshot)
- `Listener -> Retention` (payload read via lease)
- `Transit -> ALC` (authorization/outcome report)
- `Transit -> Retention` (payload read via send lease)
- `ResultSink -> ALC` (authoritative success boundary/idempotency checks)
- `ResultSink -> broker transport` (emit path)

Forbidden:
- `Listener -> Transit`
- `Transit -> Listener`
- `ResultSink` deciding success independently of ALC truth

---

## 12) Failure Ownership Matrix

| Failure Type | Deciding Authority |
|---|---|
| Retention admission failure | ALC + Retention |
| Primary lifecycle transition validity | ALC |
| Listener visibility decision | ALC |
| Listener socket/session/network failure | Listener subsystem |
| Transit queue full/defer decision | Transit scheduler/facet policy |
| Transit connection/send failure | Transit subsystem |
| Transit retry decision | Transit scheduler/facet policy (committed through ALC facet updates) |
| Transit completion validity | ALC |
| Payload expiry/eviction/corruption/invalid handle | Retention (reported to ALC) |
| Upstream settlement emit/retry | Result Sink using ALC success boundary |

---

## 13) Retention as Legitimate Shared Dependency

Retention is intentionally shared by Listener and Transit.

### Shared dependency semantics
If retention makes payload unavailable:
- Listener becomes unavailable for that article.
- Transit may resolve terminally as payload-unavailable.

### Expiry vs Eviction
- `EXPIRED`: TTL-based unavailability.
- `EVICTED`: pressure/emergency-based unavailability.

Serve behavior may be identical (not available), but telemetry/outcomes must distinguish cause.

---

## 14) Upstream Settlement Boundary (Crash-Safe)

Settlement success boundary:
`Canonicalized -> Retained -> LISTENER_VISIBLE -> Upstream Success`

### Required properties
- settlement emission idempotent
- settlement decision derived from ALC authoritative truth
- crash after `LISTENER_VISIBLE` allows safe recovery/re-emit or deduped completion
- success cannot be emitted if `LISTENER_VISIBLE` absent

Transit health/outcome does not participate in ingestion success.

---

## 15) Transit Enqueue Semantics and Recovery

Transit enqueue failure/defer must not affect listener visibility.

### Required behavior
- required-but-not-enqueued transit intent must be authoritative/recoverable
- deterministic mechanism must always exist to re-offer required pending transit work after restart/scheduler failure
- `NOT_ENQUEUED` is recoverable pending, never terminal graveyard

### Transit policy states
- `NOT_REQUIRED`: policy says no transit needed
- `NOT_ENQUEUED`: required but not yet scheduled
- `DEFERRED`: temporarily unschedulable, retry later
- `ENQUEUED`, `INFLIGHT`, `DELIVERED`, `RETRY`, `EXHAUSTED`

`NOT_REQUIRED` and `NOT_ENQUEUED` must remain distinct.

### Scheduler boundary
ALC may record eligibility/deferred intent.  
Transit scheduler determines when execution occurs.  
ALC must not become scheduler/timer engine.

---

## 16) Resource Isolation Architecture (Hard Requirement)

Isolation applies to execution resources, not only locks.

Separate budgets/pools for:
- listener connections
- listener sessions
- listener buffers/memory reservations
- transit queue capacity
- transit workers
- transit connections
- transit retry capacity

Transit may not consume capacity reserved for Listener-critical path.

---

## 17) Forbidden Architectures

Prohibited patterns:
- Listener waits for Transit.
- Listener checks Transit health before serving.
- Transit owns/mutates primary lifecycle state.
- Transit-delivered required for ingestion success.
- Queue owning payload bytes.
- Consumers directly mutating lifecycle truth.
- Global lock coupling Listener and Transit paths.
- Shared unbounded queue across critical/background workloads.
- ALC performing network IO.
- ALC executing retry timers/scheduler loops.

---

## 18) Minimal Primary State Principle

Do not add primary lifecycle states unless correctness requires it.

Preferred primary states:
`NEW, RETAINING, RETAINED, LISTENER_VISIBLE, EXPIRED, EVICTED, FAILED_ADMISSION`

Operational complexity belongs in facets (Transit), not primary lifecycle.

---

## 19) Rollout Strategy

### Phase 0 — Ownership contract ratification
Lock invariants, ownership, mutation authority.

### Phase 0.5 — Architectural proof
Prove:
- isolation guarantees
- stale-event rejection
- version semantics
- lease semantics (including max absolute lifetime)
- recovery guarantees

### Phase 1 — Canonical boundary
Single ArticleKey creation and downstream identity discipline.

### Phase 2 — Retention authority hardening
Retention as sole byte authority.

### Phase 3 — Listener visibility path
`retained -> LISTENER_VISIBLE -> serve` validated with Transit disabled.

### Phase 4 — Transit parallel facet
Enable transit as independent consumer with recoverable pending intent.

### Phase 5 — Settlement migration
Move success emission to `LISTENER_VISIBLE` boundary.

### Phase 6 — Legacy path removal
Remove old multi-writer lifecycle/terminalization paths.

---

## 20) Mandatory Test Matrix (Invariant-Focused)

### 20.1 Isolation tests
- Transit catastrophe isolation (must pass)
- Listener catastrophe isolation (must pass)
- Transit-off listener correctness (must pass)

### 20.2 Event/version/lease tests
- duplicate events
- out-of-order events
- stale version events
- stale lease events
- duplicate release
- completion after expiry/eviction
- lease max-lifetime enforcement

### 20.3 Crash/restart tests
- crash after retention commit
- crash after listener-visible commit
- crash during transit enqueue
- crash during transit send
- crash after transit success before report
- ALC restart
- Transit restart
- Listener restart
- full process restart

### 20.4 Resource tests
- transit worker exhaustion
- transit queue exhaustion
- listener connection exhaustion
- shared retention exhaustion

All tests validate invariants, not implementation internals.

---

## 21) Definition of Done

- [ ] ALC sole primary lifecycle authority
- [ ] Retention sole payload authority
- [ ] ArticleKey generated once and immutable
- [ ] Primary/Transit version semantics implemented as concurrency tokens only
- [ ] Event/lease identities and stale handling proven
- [ ] Max absolute lease lifetime enforced
- [ ] Listener independent of Transit (no sync dependency)
- [ ] Transit independent of Listener (except shared retention dependency)
- [ ] ResultSink success boundary driven by ALC truth
- [ ] Resource isolation proven under load
- [ ] Restart/recovery guarantees proven
- [ ] Settlement boundary crash-safe/idempotent
- [ ] Transit queue bounded and non-owning
- [ ] `NOT_ENQUEUED` recovery mechanism proven deterministic
- [ ] Expiry vs eviction semantics observable
- [ ] Duplicate/out-of-order event safety proven
- [ ] Transit-off listener test passes
- [ ] Transit-catastrophe listener test passes
- [ ] Listener-catastrophe transit test passes
- [ ] No legacy multi-writer lifecycle mutation paths remain

---

## 22) Final Decision Statement

This v3 architecture formalizes:

- **Primary contract:** retained + `LISTENER_VISIBLE` availability
- **Transit contract:** independent best-effort propagation facet
- **ALC contract:** lifecycle authority and authorization, not scheduler/executor
- **Retention contract:** sole canonical payload authority

This is the baseline for implementation and architectural compliance going forward.