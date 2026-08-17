# Phase 2 benchmark correctness baseline

## Baseline context

- Branch: `master`
- Baseline commit at start of capture: `1b30aa2caea2242263b5b424c72bb0a4cf6ba1d2`
- Phase 1 completion checkpoint document: `docs/benchmarks/phase1-structural-extraction-complete.md`
- Purpose: establish a warning and correctness baseline before any Phase 2 hardening work.

## Repository and solution state

- Current branch: `master`
- Current SHA (baseline start): `1b30aa2caea2242263b5b424c72bb0a4cf6ba1d2`
- Git status at baseline start: clean working tree
- Solution: `VectorNNTP.BackFiller.slnx`
- Projects in solution:
  - `VectorNNTP.BackFiller/VectorNNTP.BackFiller.csproj`
  - `VectorNNTP.BackFiller.Benchmarks/VectorNNTP.BackFiller.Benchmarks.csproj`
  - `VectorNNTP.BackFiller.Tests/VectorNNTP.BackFiller.Tests.csproj`
- Target frameworks:
  - `VectorNNTP.BackFiller`: `net8.0`
  - `VectorNNTP.BackFiller.Benchmarks`: `net8.0`
  - `VectorNNTP.BackFiller.Tests`: `net8.0`

## Benchmark project warning inventory (`VectorNNTP.BackFiller.Benchmarks`)

Benchmark-project warnings captured from benchmark build output:

1. `CS8604`
   - File: `VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementExecutionEngine.cs`
   - Line: `31`, Column: `80`
   - Description: Possible null reference argument for parameter `MessageId` in `QueuedArticle.QueuedArticle(string MessageId, int PayloadLength)`.
   - Classification: **B) pre-existing benchmark warning**

2. `CS8604`
   - File: `VectorNNTP.BackFiller.Benchmarks/Execution/TransitBenchmarkOrchestrator.cs`
   - Line: `152`, Column: `50`
   - Description: Possible null reference argument for parameter `messageId` in `TransitPublisher.PublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)`.
   - Classification: **B) pre-existing benchmark warning**

Benchmark warning classification summary:

- A) introduced by Phase 1 extraction: **0**
- B) pre-existing benchmark warning: **2**
- C) unrelated/generated/tooling warning: **0** (within benchmark project warning set)

## Full solution warning inventory baseline

From `dotnet build VectorNNTP.BackFiller.slnx`:

- Total warnings: **179**

Warning count by project:

- `VectorNNTP.BackFiller`: **173**
- `VectorNNTP.BackFiller.Benchmarks`: **2**
- `VectorNNTP.BackFiller.Tests`: **4**

Warning count by warning ID:

- `CA1848`: 77
- `CA1873`: 63
- `IDE0005`: 12
- `CS1573`: 10
- `CS8600`: 4
- `CA1822`: 2
- `CS8604`: 2
- `xUnit1031`: 2
- `CA1068`: 1
- `CA1513`: 1
- `CS8602`: 1
- `IDE0010`: 1
- `IDE0040`: 1
- `xUnit1026`: 1
- `xUnit2031`: 1

## Contract validation baseline

Validation commands executed:

- `dotnet build VectorNNTP.BackFiller.slnx`
- `dotnet test VectorNNTP.BackFiller.Tests --filter "FullyQualifiedName~CreateBenchmarkResultContractTests|FullyQualifiedName~MeasurementMetricsClassificationContractTests|FullyQualifiedName~BenchmarkArtifactContractTests|FullyQualifiedName~BenchmarkConsoleReporterContractTests"`

Results:

- Build result: **PASS**
- Focused contract tests: **PASS**
  - Total: **10**
  - Failed: **0**
  - Skipped: **0**

## Benchmark architecture snapshot

Current ownership after Phase 1:

- `TransitServerStressRunner`
  - benchmark mode entrypoints
  - runtime identity/build version ownership
  - orchestration delegate wiring
  - thin forwarding to extracted components

- `TransitBenchmarkOrchestrator`
  - benchmark lifecycle orchestration
  - publisher lifecycle
  - phase ordering
  - smoke/warmup orchestration
  - final reporting flow

- `MeasurementRunCoordinator`
  - measurement lifecycle
  - queue/metrics/runtime setup
  - worker startup
  - measurement window delay
  - drain handoff and result callback wiring

- `MeasurementExecutionEngine`
  - producer execution
  - dispatcher execution
  - telemetry execution
  - drain/shutdown execution

- `BenchmarkResultFactory`
  - benchmark result assembly
  - throughput calculations
  - conversion logic
  - runtime/forensic mapping and fallback reads

## Baseline statement

Phase 2 begins from a structurally stable benchmark implementation. No correctness or performance changes have been applied.
