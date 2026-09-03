---
applyTo: "**/*.{cs,csproj,json,md}"
---
# Performance engineering

BackFiller is intended for very-high-throughput NNTP/network service operation. Treat hot paths, network I/O, queueing, parsing, and logging as performance-sensitive while preserving correctness.

- Avoid unnecessary allocations, copies, boxing, closures, LINQ, task creation, synchronization, lock contention, and retained large buffers. Make buffer and pooled-resource ownership explicit.
- Prefer asynchronous, cancellable socket/stream I/O and bounded queues/backpressure. Consider throughput, mean and tail latency, CPU, memory, allocations/GC, connection utilization, and contention.
- Keep high-frequency logs cheap and structured; use metrics for telemetry that would otherwise create log volume.
- Establish a clean baseline before changing performance code. Evaluate one meaningful change at a time with stable configuration and credible measurements; never accept speculative micro-optimizations.
- Keep benchmark, fake-server, and production topology attribution separate. Follow the identity-guarded clean/build/run policy and repository watchdog requirements for benchmark or blocking tests.
