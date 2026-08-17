# VectorNNTP.BackFiller.Benchmarks Pre-Refactor Source Baseline

## Purpose
This document freezes the benchmark source and execution baseline before Phase 1 structural extraction. The extraction phase must preserve behavior exactly.

## Source Identity
- Repository: `C:\Users\chrisk\source\repos\VectorNNTP.BackFiller`
- Branch: `master`
- Commit: `fa316ff754c975017b624bc4dddfdfb4a1974eca`
- Working tree at capture: clean
- Solution: `VectorNNTP.BackFiller.slnx`

## Build / Runtime Identity
- Target framework: `net8.0`
- Benchmark assembly path: `C:\Users\chrisk\source\repos\VectorNNTP.BackFiller\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll`
- Build configuration: `Debug`
- Runtime identifier: `win-x64`
- Benchmark assembly version: `1.1.229.8569`
- Benchmark file version: `1.1.229.8569`
- .NET SDK: `10.0.400`
- .NET host runtime: `10.0.11`
- .NET runtime used by project target family: `Microsoft.NETCore.App 8.0.30`

## Environment Identity
- OS: `Microsoft Windows 11 Enterprise`
- OS version/build: `10.0.26200` / `26200`
- CPU: `12th Gen Intel(R) Core(TM) i9-12900KF`
- Physical cores: `16`
- Logical processors: `24`
- Physical memory bytes: `34186448896`

## Expected Benchmark Output Sections (Transit Stress Path)
Expected console section ordering:
1. `=== Transit Publisher Production-Path Benchmark ===`
2. `=== Phase 1: Initialization ===`
3. `=== Phase 2: TLS / TransitPublisher startup ===`
4. `=== Phase 3: Smoke test (REAL publisher, realistic ~1MiB articles) ===`
5. `=== Phase 3.5: Workload preparation ===`
6. `=== Phase 4: Warmup ===`
7. `=== Phase 5: EXACT measurement window ===`
8. `=== Phase 6: Drain ===`
9. `=== Phase 7: Connection topology diagnostics ===`
10. `=== Phase 8: Final results ===`

## Artifact Location Contract
Structured artifact location is derived from benchmark base directory (`AppContext.BaseDirectory`) with file names:
- `transit-benchmark-result-<yyyyMMdd-HHmmss>.json`
- `transit-benchmark-result-<yyyyMMdd-HHmmss>.csv`

## Variance Policy (Phase 1)
Structural extraction parity checks must treat these as naturally variable:
- Timestamps
- Runtime-generated IDs
- CPU usage and GC sample noise
- Wall-clock duration jitter

Any changes to schema, section names/order, formulas, status classification, or timing semantics are not allowed in Phase 1.
