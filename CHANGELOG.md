# Changelog

This changelog records significant engineering events, archaeology recoveries, architectural decisions, test-contract repairs, benchmark changes, and measured performance outcomes.

## 2026-08-22

### Category: Benchmark Instrumentation / Forensics

#### Summary
Added opt-in dispatch-consumer queue-read forensics to the benchmark harness and documented the resulting evidence in
`docs/benchmarks/queue-consumer-callstack-forensics.md`.

#### Why
An unexplained ~150-200 ms delay obtaining an article from the queue had to be attributed to a measured cause rather
than a hypothesis, without changing architecture, `Channel<T>` usage, consumer count, batching, pipeline depth, socket
behaviour, or ThreadPool settings.

#### Files / Components
- Benchmark: `Diagnostics/QueueConsumerForensics.cs`, `Diagnostics/QueueConsumerProbe.cs`,
  `Diagnostics/QueueConsumerForensicsReport.cs`, `Diagnostics/QueueConsumerForensicsWriter.cs`
- Benchmark wiring: `Execution/BoundedArticleQueue.cs`, `Execution/MeasurementExecutionEngine.cs`,
  `Execution/MeasurementRunCoordinator.cs`, `Configuration/TransitBenchmarkConfig.cs`, `TransitBenchmarkCliOptions.cs`
  (new `--queue-consumer-forensics <true|false>` flag, inert by default)
- Tests: `VectorNNTP.BackFiller.Tests/Benchmarks/QueueConsumerForensicsTests.cs`
- Documentation: `docs/benchmarks/queue-consumer-callstack-forensics.md`

#### Test / Validation
- 9 focused unit tests pass.
- Identity-guarded fake-server benchmark run exported `queue-consumer-callstacks.json` / `.txt`.

#### Performance Impact
No optimization claim. Instrumentation is opt-in and disabled by default. Measured findings: consumers are
asynchronously parked in `await WaitToReadAsync(...)`; there is no synchronous blocking wait, lock, or semaphore between
`WaitToReadAsync` and `TryRead`; `TryRead` is sub-microsecond (interval E p50 0.6 us); the wait resides in interval C
(wake/continuation scheduling) and interval A (queue genuinely empty). No failed `TryRead` occurred while
`CurrentQueuedCount > 0` and unchanged.

#### Notes
`CurrentQueuedCount` is an independent counter, not the Channel's readable item count; it was observed transiently
negative.

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

---

## 2026-08-21

### Commit: f52164704e118fa71557af88593ee89f3a4c1396 (f521647)
### Category: Benchmark Baseline / Measurement Campaign

#### Summary
Established a clean measurement-only BackFiller ceiling baseline campaign using `transit-benchmark-fakeserver` against `BenchmarkDevNullTransitServer/v1`.

#### Why
Previous high-throughput observations against local real `Vector.NNTP.NNTPD` include server-side processing and are not valid as a pure BackFiller pipeline ceiling baseline.

#### Files / Components
- Benchmark mode: `transit-benchmark-fakeserver`
- Benchmark endpoint identity: `BENCHMARK FAKE SERVER / DEV NULL`, `BenchmarkDevNullTransitServer/v1`
- Campaign evidence directory: `.vs/baseline-fakeserver-campaign-20260821/`
- Run artifacts:
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260821-235606.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260821-235611.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260821-235616.json`

#### Test / Validation
- Verified clean working tree and identity-guarded benchmark/prod binaries at campaign start.
- Ran three identical baseline executions; all terminated normally (`exit code 0`).
- PID-scoped socket checks proved benchmark process listened on fake-server port and had zero established connections to real NNTPD endpoint `198.18.0.66:119`.
- Endpoint identity in logs/artifacts matched expected fake-server values for every run.

#### Performance Impact
No production-path change and no optimization claim. This entry records baseline measurement only.

#### Baseline Results (3-run identical campaign)
- Sustained accepted throughput (Gbps): `0.3836`, `0.3909`, `0.3924` (mean `0.3890`, CV `1.20%`)
- Generated throughput (Gbps): `0.7673`, `0.7818`, `0.7821` (mean `0.7771`, CV `1.09%`)
- Accepted articles/sec: `60.0`, `60.0`, `60.2` (mean `60.07`, CV `0.19%`)
- Accepted/rejected/ambiguous:
  - Run1: `600/0/600`
  - Run2: `600/0/600`
  - Run3: `602/0/598`
- FakeServer reconciliation: `FakeServerAccepted = BackFillerAccepted + 5` on all runs (fixed smoke-phase offset).

#### Notes
- Instantaneous peak Gbps was recorded as a derived estimate from artifact latency telemetry (`ArticleTargetBytes` and `P50SocketWriteUs`) due no explicit interval throughput field in current artifact schema.
- Campaign stopped after baseline establishment; no optimization work was started.

---

## 2026-08-22

### Commit: post-f521647 sustained benchmark campaign (workspace measurement run)
### Category: Benchmark Baseline / Sustained Saturation Campaign

#### Summary
Established a sustained fake-server saturation baseline by driving the current unoptimized BackFiller with duration-based continuous workload instead of a small finite article budget.

#### Why
The previous three-run fake-server baseline was a low-load control point and not a BackFiller ceiling. This campaign was executed to answer how hard the current unoptimized BackFiller can be driven when real NNTPD bottlenecks are removed.

#### Files / Components
- Benchmark command mode: `transit-benchmark-fakeserver`
- Benchmark workload/config changes:
  - `VectorNNTP.BackFiller.Benchmarks/TransitServerStressRunner.cs`
  - `VectorNNTP.BackFiller.Benchmarks/Workload/PreparedBenchmarkWorkload.cs`
- Campaign evidence directory: `.vs/baseline-fakeserver-saturation-campaign-20260822/`
- Run artifacts:
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260822-000722.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260822-001052.json`
  - `VectorNNTP.BackFiller.Benchmarks/bin/x64/Debug/net8.0/win-x64/transit-benchmark-result-20260822-001421.json`

#### Test / Validation
- Three identical sustained runs (`120s` measurement, `15s` warmup) terminated normally (`exit code 0`).
- Endpoint identity in all runs: `BENCHMARK FAKE SERVER / DEV NULL` / `BenchmarkDevNullTransitServer/v1`.
- PID-scoped evidence confirmed benchmark listener ownership and zero benchmark-process established connections to real NNTPD endpoint `198.18.0.66:119`.
- Reconciliation check: BackFiller accepted counts matched FakeServer accepted counts exactly in all runs.

#### Performance Impact
No production optimization claim. This entry records benchmark workload configuration and saturation baseline measurement only.

#### Sustained Saturation Results (3-run identical campaign)
- Benchmark duration: `120s` measurement per run (`15s` warmup)
- Sustained accepted throughput (Gbps): `0.1441`, `0.1444`, `0.1428` (mean `0.1438`, CV `0.59%`)
- Generated throughput (Gbps): `2.0895`, `2.0273`, `1.9899` (mean `2.0356`, CV `2.47%`)
- Accepted articles/sec (derived): `68.73`, `68.89`, `68.13` (mean `68.58`, CV `0.59%`)
- Accepted/rejected/ambiguous:
  - Run1: `8247/0/111329`
  - Run2: `8267/0/107759`
  - Run3: `8175/0/105712`
- CPU avg sampled: `4.58%`, `4.70%`, `4.61%` (mean `4.63%`)
- Host CPU peak sampled: `15.18%`, `16.57%`, `15.85%` (mean `15.87%`)
- Working set MB: `866.60`, `796.34`, `869.11` (mean `844.02`)
- Managed heap MB: `473.48`, `582.67`, `572.52` (mean `542.89`)
- Allocated MB total: `54038.03`, `56169.91`, `54756.13` (mean `54988.03`)
- GC activity (Gen0/Gen1/Gen2):
  - Run1: `330/321/314`
  - Run2: `951/941/303`
  - Run3: `1413/1404/295`
- Queue peak depth: `3582`, `3581`, `3580` (near 1024 MiB queue pressure)
- Queue peak bytes: `938999808`, `938737664`, `938475520`
- Publish latency p50/p95/p99 us:
  - Run1: `312009.9/552131.1/7240090.9`
  - Run2: `322317.2/2169381.3/6028539.3`
  - Run3: `330853.8/2304924.9/6034065.9`
- FakeServer accepted articles: `8247`, `8267`, `8175`
- FakeServer consumed payload bytes: `3166151076`, `3200491678`, `3135742604`
- Instantaneous peak Gbps estimate (derived from `ArticleTargetBytes` / `P50SocketWriteUs`): mean `0.5339` (min `0.4691`, max `0.5802`)

#### Notes
- `AcceptedArticlesPerSecond` field in current artifact schema remained `0.0`; campaign report uses derived accepted rate (`AcceptedArticles / MeasurementSeconds`) for throughput-rate interpretation.
- Sustained queue pressure indicates producer supply was no longer the limiting factor; campaign successfully avoided finite tiny-workload early completion.
- Campaign stopped after establishing the new saturation baseline; no optimization work started.
