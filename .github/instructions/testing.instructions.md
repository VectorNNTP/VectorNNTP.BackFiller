---
applyTo: "**/*Tests.cs"
---
# Testing and validation

- Preserve the existing xUnit suites, benchmark contract tests, coverage, and regression baselines. Never remove, skip, weaken, suppress, or bypass a test to obtain a passing build.
- Test observable behavior and contracts: configuration diagnostics, protocol parsing, provider failures, cancellation, ownership, exactly-once settlement, backpressure, resource disposal, readiness, and layered shutdown.
- Make tests deterministic. Use explicit barriers, channels, tasks, and cancellation sources instead of arbitrary delays or polling.
- For concurrency/lifecycle tests, isolate one test or method per process where required, clean stale test processes, use the repository watchdog, and capture forensics before terminating a hung testhost.
- Run restore, clean/build, relevant tests, and a warning-free rebuild for changed scope. Classify failures before changing code: production regression, exposed production bug, intentional contract change, stale contract, environment/tooling, infrastructure, or unknown.
