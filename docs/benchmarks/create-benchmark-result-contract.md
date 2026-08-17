# CreateBenchmarkResult contract boundary

`TransitServerStressRunner.CreateBenchmarkResult(...)` is the benchmark truth boundary for transit benchmark outputs.

It assembles `MeasurementSnapshot`, `RuntimeSnapshot`, `ForensicSnapshot`, workload preparation metadata, and drain lifecycle values into the canonical `BenchmarkResult` contract. That result then flows into:

- console benchmark reporting
- JSON artifact output
- CSV artifact output

## Why this boundary is protected before extraction

This method includes benchmark semantics that are easy to regress during structural movement:

- throughput formulas (`GeneratedGbps`, `AdmittedGbps`, `AcceptedGbps`)
- producer backpressure formulas (`ProducerActivePercent`, `ProducerBlockedPercent`)
- conversion formulas (stopwatch ticks to milliseconds, bytes to MB, bytes to Gbps)
- classification mapping (accepted / rejected / ambiguous groupings)
- runtime fallback rules (snapshot-preferred, process/GC fallback when unavailable)
- GC count semantics (current `GC.CollectionCount(...)` values at result creation time)
- forensic latency passthrough behavior (no reinterpretation)
- artifact schema/order and console section ordering

## Extraction rule

Any extraction of `CreateBenchmarkResult` must maintain contract parity with these tests.

The tests intentionally freeze current formulas and status mappings. Cleanup and semantic changes are deferred to later, explicit benchmark behavior work.
