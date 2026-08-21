# Project Context

## Current Checkpoint
- Commit: `b8d557f`
- Label: `known-good archaeological checkpoint`
- Purpose: stable recovery baseline before next major optimization phase.

## Repository State
- Expected state at this checkpoint: clean working tree before new optimization work.
- Governance scope for this document: current-state checkpoint, not a full diary.

## Architecture
- Core production pipeline areas under active archaeology/performance focus:
  - `VectorNNTP.BackFiller/Runtime/Transit/TransitConnection.cs`
  - `VectorNNTP.BackFiller/Runtime/Transit/TransitPublisher.cs`
- Benchmark harness is separate and intentional architecture:
  - `VectorNNTP.BackFiller.Benchmarks/*`
- Keep production behavior, benchmark harness behavior, and server-side test harness behavior explicitly separated in analysis.

## Known-Good Behaviour
- Archaeology-validated checkpoint behavior captured at `b8d557f`.
- Production behavior should be interpreted from source + commit history + tests, not from conversational memory.

## Recovered Behaviour
- Recovery stream completed for production areas (PC-01..PC-08), including:
  - response correlation hardening
  - lifecycle/retry cleanup
  - STARTTLS negotiation
  - failed initialization cleanup
  - preemption terminalization

## Test Contract Repairs
- Test-only contract repair stream completed: T-01, T-02, T-03, T-04.
- These repairs are validated and should not be casually reclassified as production regressions.

## Benchmark Topology
- Topology was explicitly re-validated from canonical command -> config -> runtime connect path:
  - `transit-validate` currently resolves `BackFiller:TransitServer` and connects to local `198.18.0.66:119` (`incoming.usenet.ninja`) when using canonical mode.
  - That endpoint was observed as owned by local `Vector.NNTP.NNTPD` process, not a test fixture server.
- Dedicated benchmark isolation mode now exists for BackFiller ceiling measurement without NNTPD spool/server work:
  - `transit-benchmark-fakeserver` mode
  - Benchmark producers -> BackFiller transit pipeline -> benchmark-only `/dev/null` NNTP sink -> `239` correlation response
- Keep canonical real-endpoint runs and fake-server-isolated runs explicitly separated in reports/artifacts.

## Benchmark Configuration
- Canonical baseline stress profile (as previously used):
  - duration-driven measurement
  - queue/article/concurrency settings defined via benchmark CLI + appsettings loading path
  - identity-guarded benchmark execution policy (clean/build/verify identity before run)
- Reference command source:
  - `docs/benchmarks/phase1-baseline/canonical-benchmark-commands.md`

## Performance Baselines
- Baseline set at checkpoint reported sustained accepted throughput approximately in the `0.29–0.74 Gbps` range for the analyzed canonical stress repeats.
- This baseline is the recorded checkpoint-era reference and must be tied to exact artifacts/configuration when reused.

## Performance Experiments
- Completed (forensics/baseline):
  - identity/provenance capture
  - end-to-end pipeline validation run
  - repeated stress runs for throughput and stability
  - hang-forensics lifecycle boundary analysis
- Directional follow-up observation (requires exact artifact/config confirmation before becoming canonical):
  - substantially higher throughput observed after reducing benchmark console trace noise in exploratory work.

## Current Bottlenecks
- In baseline forensic runs, dominant pressure indicators included:
  - high ambiguous completions under stress
  - high producer blocked percentage
  - elevated publish/socket/response latency tails
  - post-run shutdown lifecycle complexity

## Current Hypotheses
- Intermittent benchmark non-exit appears at post-reporting shutdown lifecycle boundary rather than measurement/drain/report generation itself.
- High-frequency synchronous console trace output is likely a measurement perturbation/noise source, but causality for shutdown hang must be proven with controlled evidence.

## Completed Work
- Archaeology recovery checkpoint established (`b8d557f`).
- PC-01..PC-08 and T-01..T-04 streams validated.
- Baseline measurement + hang forensics completed without production/test behavior changes during forensic phases.

## Pending Work
- Confirm canonical high-throughput direction with exact reproducible artifact/config pair before promoting it to canonical baseline.
- Continue planned optimization work one meaningful change at a time with before/after metrics.
- Investigate pipe-level byte-oriented DotStuffing/DotUnstuffing path to reduce string materialization, intermediate allocations, encoding/decoding, copies, and GC pressure.
- Evaluate similar byte-oriented principles in FakeServer independently of production Transit conclusions.

## Do Not Touch / Protected Areas
- Do not casually rewrite validated archaeology recoveries in `TransitConnection` / `TransitPublisher`.
- Do not conflate benchmark harness modifications with production pipeline optimizations.
- Do not treat test-only repairs (T-01..T-04) as implicit production defects.
- Do not change benchmark topology attribution (FakeServer vs TransitServer).
- Future architecture considerations listed below are intentional considerations, not immediate implementation tasks.

## Important Historical Evidence
- Known-good checkpoint commit:
  - `b8d557f` (`known-good archaeological checkpoint`)
- Baseline benchmark command and archaeology references:
  - `docs/benchmarks/phase1-baseline/canonical-benchmark-commands.md`
  - `docs/benchmarks/phase1-baseline/pre-refactor-source-baseline.md`
  - `docs/benchmarks/phase1-baseline/artifact-parity-spec.md`
- Forensics/baseline artifacts (examples from checkpoint-era runs):
  - `.vs/baseline-transit-validate-run.txt`
  - `.vs/baseline-transit-stress-run1-canonical.txt`
  - `.vs/baseline-transit-stress-run2-canonical.txt`
  - `.vs/baseline-transit-stress-run3-canonical-retry.txt`
  - `VectorNNTP.BackFiller.Benchmarks/bin/Debug/net8.0/win-x64/transit-benchmark-result-*.json`

## Known Limitations
- This file is a state checkpoint; it is not the source of truth.
- Throughput claims are valid only with matching configuration, runtime identity, and artifact evidence.
- Conversational summaries can omit details; always reconcile with code/history/artifacts.

## Next Intended Experiment
- Begin next major optimization phase focused on byte-oriented DotStuffing/DotUnstuffing directly in the pipe path, then measure impact with unchanged benchmark configuration and full before/after artifact capture.
- Include explicit guardrails to avoid benchmark-only “speedups” being misreported as production pipeline gains.

---

## Evidence Hierarchy (Authoritative Order)
1. Current source code
2. Git history / commits
3. Tests and test results
4. Benchmark artifacts
5. Project documentation
6. Persistent Copilot context
7. Conversational memory

If this context conflicts with higher-tier evidence, stop and report the conflict. Do not silently rewrite history.

---

## Protected Future Considerations (Not Implementation Instructions)
The following are intentional future considerations and are not to be implemented as part of governance/checkpoint tasks:
- BackFiller may eventually expose a TCP/TLS listening service for real-time article retrieval by NNRPD.
- Retrieved article bodies may need shared in-memory access across TransitServer delivery and listening-socket retrieval.
- A dedicated in-memory article-storage/lifetime component may eventually be preferable to embedding that responsibility in `TransitConnection`.
- RabbitMQ provides transactional article requests.
- Real NNTP provider connections are intended to remain heavily utilized and pre-established.
- The eventual BackFiller architecture is latency-sensitive and part of a real-time NNTP request path.
