# Changelog

This changelog records significant engineering events, archaeology recoveries, architectural decisions, test-contract repairs, benchmark changes, and measured performance outcomes.

## 2026-08-21

### Commit: b8d557f9696c2d9d919241852e12b43541e2a1de (b8d557f)
### Category: Recovery / Archaeology Checkpoint

#### Summary
Established known-good archaeological checkpoint `b8d557f` after validated recovery and contract-alignment work.

#### Why
Create a stable baseline before the next performance optimization phase and prevent repeated rediscovery/reintroduction of already-investigated behavior.

#### Files / Components
- Production areas: `TransitConnection`, `TransitPublisher`
- Recovery tracks: PC-01..PC-08 (response correlation hardening, lifecycle/retry cleanup, STARTTLS negotiation, failed initialization cleanup, preemption terminalization)
- Test-contract repairs: T-01..T-04

#### Test / Validation
- Recovery and contract validations completed prior to checkpoint.
- Benchmark identity-guarded build and run path validated for `VectorNNTP.BackFiller.Benchmarks` (Debug/net8.0/win-x64).

#### Performance Impact
No optimization claim at checkpoint. Checkpoint serves as baseline control point.

#### Notes
Do not casually treat T-01..T-04 repairs as production regressions requiring re-fix.

---

## 2026-08-21

### Commit: b8d557f9696c2d9d919241852e12b43541e2a1de (baseline measured against checkpoint state)
### Category: Benchmark Baseline / Forensics

#### Summary
Performed canonical benchmark baseline and hang forensics on recovered implementation without production/test changes.

#### Why
Answer whether current implementation sustains >1 Gbps and determine where intermittent benchmark hangs occur.

#### Files / Components
- Benchmark harness/orchestration: `VectorNNTP.BackFiller.Benchmarks/*`
- Transcripts/artifacts: `.vs/baseline-transit-*.txt`, `transit-benchmark-result-*.json/.csv`

#### Test / Validation
- End-to-end transit validation mode completed with accepted work and no rejected/ambiguous outcomes.
- Canonical stress runs produced result artifacts and final reports; intermittent non-exit behavior observed after artifact/report phase.

#### Performance Impact
- Canonical sustained accepted throughput observed in the ~0.29–0.74 Gbps range across stress repeats for the analyzed baseline set.
- This baseline should be treated as reproducible evidence for current harness/configuration state at measurement time.

#### Notes
- Forensic boundary identified after measurement/drain/report/artifact stages, in post-reporting publisher/connection disposal/process shutdown lifecycle.
- High-volume synchronous console trace output was identified as substantial noise/overhead risk, but not proven as direct hang cause in the forensic set.
- Separate later exploratory direction indicated materially higher throughput after trace-noise reduction; record as directional and require exact artifact/config re-verification before promoting as canonical baseline.

---

## 2026-08-21

### Category: Benchmark Infrastructure

#### Summary
Added a benchmark-only `/dev/null` transit sink mode (`transit-benchmark-fakeserver`) to isolate BackFiller pipeline ceiling measurement from real NNTPD spool/server processing.

#### Why
Current canonical benchmark topology was proven to terminate at live local NNTPD (`198.18.0.66:119`), which introduces server-side work outside BackFiller throughput isolation goals.

#### Files / Components
- Benchmark mode routing: `VectorNNTP.BackFiller.Benchmarks/Program.cs`
- Benchmark runner integration: `VectorNNTP.BackFiller.Benchmarks/TransitServerStressRunner.cs`
- Benchmark fake sink implementation: `VectorNNTP.BackFiller.Benchmarks/Execution/BenchmarkDevNullTransitServer.cs`
- Benchmark endpoint identity/config metadata: `VectorNNTP.BackFiller.Benchmarks/Configuration/TransitBenchmarkConfig.cs`
- Benchmark reporting/artifacts identity surfacing: `VectorNNTP.BackFiller.Benchmarks/Execution/TransitBenchmarkOrchestrator.cs`, `VectorNNTP.BackFiller.Benchmarks/Reporting/BenchmarkConsoleReporter.cs`, `VectorNNTP.BackFiller.Benchmarks/Artifacts/BenchmarkResultArtifact.cs`
- Benchmark tests: `VectorNNTP.BackFiller.Tests/Benchmarks/BenchmarkDevNullTransitServerTests.cs`

#### Test / Validation
- Added benchmark fake-server protocol/correlation tests and endpoint metadata contract assertions.
- Validation run required to prove fake sink receives benchmark connections while real NNTPD remains at zero benchmark connections.

#### Performance Impact
No production-path optimization claim. This change is benchmark infrastructure only.

#### Notes
- Existing canonical `transit-validate` and `transit-stress` behavior remains unchanged.
- Benchmark artifacts now explicitly include endpoint identity (`EndpointType`, `EndpointIdentity`, `EndpointHost`, `EndpointPort`, `EndpointUseSsl`) to prevent topology ambiguity.
