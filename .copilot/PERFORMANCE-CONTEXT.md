# Project Context

## Current Checkpoint
- Commit: `f521647`
- Label: `benchmark dev-null infrastructure milestone`
- Purpose: clean starting point for BackFiller-only ceiling baseline campaign against benchmark fake-server sink.

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
- Historical canonical stress baseline (real-endpoint topology era) reported sustained accepted throughput approximately in the `0.29–0.74 Gbps` range and must not be treated as BackFiller-only ceiling.
- Clean fake-server control baseline campaign (2026-08-21, short low-load profile) against `BENCHMARK FAKE SERVER / DEV NULL` / `BenchmarkDevNullTransitServer/v1`:
  - Sustained accepted throughput (Gbps): `0.3836`, `0.3909`, `0.3924` (mean `0.3890`, CV `1.20%`)
  - Generated throughput (Gbps): `0.7673`, `0.7818`, `0.7821` (mean `0.7771`, CV `1.09%`)
  - Accepted/rejected/ambiguous per run: `600/0/600`, `600/0/600`, `602/0/598`
  - Reconciliation: `FakeServerAccepted = BackFillerAccepted + 5` (fixed smoke-phase offset).
- Sustained fake-server saturation baseline campaign (2026-08-22, duration-driven workload, 120s measurement + 15s warmup, three identical runs):
  - Sustained accepted throughput (Gbps): `0.1441`, `0.1444`, `0.1428` (mean `0.1438`, CV `0.59%`)
  - Generated throughput (Gbps): `2.0895`, `2.0273`, `1.9899` (mean `2.0356`, CV `2.47%`)
  - Accepted articles/sec (derived): `68.73`, `68.89`, `68.13` (mean `68.58`, CV `0.59%`)
  - Accepted/rejected/ambiguous per run: `8247/0/111329`, `8267/0/107759`, `8175/0/105712`
  - CPU avg sampled mean: `4.63%` (peak host CPU mean `15.87%`)
  - Working set mean: `844.02 MB`; managed heap mean: `542.89 MB`; allocated MB mean: `54988.03`
  - Queue peak depth mean: `3581`; queue peak bytes mean: `938,737,664`
  - Reconciliation: `FakeServerAccepted = BackFillerAccepted` (delta `0` in each run).

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
- Benchmark infrastructure milestone `f521647` introduced `BenchmarkDevNullTransitServer/v1` and endpoint identity reporting.
- Clean fake-server control baseline campaign completed with PID-scoped topology isolation verification and variance summary artifacts.
- Sustained saturation campaign completed by updating benchmark workload behavior to duration-driven fake-server execution with continuous message-ID supply in benchmark harness only; three identical 120s runs captured and analyzed.

## Pending Work
- No optimization actions are active in this campaign context.
- Any future optimization phase must begin from the sustained fake-server saturation baseline evidence set and retain endpoint-identity and topology isolation proof in artifacts.
- Significant non-active future engineering work discovered during performance campaigns should be captured in root `TODO.md` (backlog) rather than overloaded into this performance-state document.

## Do Not Touch / Protected Areas
- Do not casually rewrite validated archaeology recoveries in `TransitConnection` / `TransitPublisher`.
- Do not conflate benchmark harness modifications with production pipeline optimizations.
- Do not treat test-only repairs (T-01..T-04) as implicit production defects.
- Do not change benchmark topology attribution (FakeServer vs TransitServer).
- Future architecture considerations listed below are intentional considerations, not immediate implementation tasks.

## Important Historical Evidence
- Known-good archaeology checkpoint commit:
  - `b8d557f` (`known-good archaeological checkpoint`)
- Benchmark fake-server infrastructure milestone:
  - `f521647` (`Add benchmark dev-null server for isolated performance testing`)
- Baseline benchmark command and archaeology references:
  - `docs/benchmarks/phase1-baseline/canonical-benchmark-commands.md`
  - `docs/benchmarks/phase1-baseline/pre-refactor-source-baseline.md`
  - `docs/benchmarks/phase1-baseline/artifact-parity-spec.md`
- Clean fake-server control baseline campaign evidence:
  - `.vs/baseline-fakeserver-campaign-20260821/campaign-summary.json`
  - `.vs/baseline-fakeserver-campaign-20260821/baseline-metrics.json`
  - `.vs/baseline-fakeserver-campaign-20260821/acceptance-reconciliation.json`
  - `.vs/baseline-fakeserver-campaign-20260821/variance-summary.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260821-235606.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260821-235611.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260821-235616.json`
- Sustained fake-server saturation campaign evidence:
  - `.vs/baseline-fakeserver-saturation-campaign-20260822/campaign-summary.json`
  - `.vs/baseline-fakeserver-saturation-campaign-20260822/saturation-metrics.json`
  - `.vs/baseline-fakeserver-saturation-campaign-20260822/acceptance-reconciliation.json`
  - `.vs/baseline-fakeserver-saturation-campaign-20260822/variance-summary.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260822-000722.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260822-001052.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260822-001421.json`

## Known Limitations
- This file is a state checkpoint; it is not the source of truth.
- Throughput claims are valid only with matching configuration, runtime identity, and artifact evidence.
- Conversational summaries can omit details; always reconcile with code/history/artifacts.

## Next Intended Experiment
- Campaign is currently in measurement-only stop state after sustained fake-server saturation baseline establishment.
- Do not begin optimization until explicitly authorized in a future prompt.

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
