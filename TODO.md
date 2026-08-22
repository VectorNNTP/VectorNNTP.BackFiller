# TODO Backlog

This file is the persistent engineering backlog for significant future work.

## Governance
- Do not use this as a dumping ground for trivial tasks.
- Entries must represent meaningful future engineering work.
- Each entry must explain what is wanted and why it matters.
- Preserve architectural context when rationale could be forgotten later.
- Do not silently delete TODO items because they are inconvenient.
- Mark completed, deferred, or cancelled items explicitly instead of erasing history.
- When significant new work is discovered during archaeology, profiling, benchmarking, debugging, or optimization, add it here instead of relying on conversational memory.
- Do not implement TODO items merely because they were added.
- TODO items are backlog commitments, not authorization to begin work.
- Before starting a substantial TODO item, define scope and relationship to current governance/checkpoints.

## Separation of Responsibilities
- `CHANGELOG.md`: historical record of completed/significant events.
- `TODO.md`: outstanding future work.
- `.copilot/PERFORMANCE-CONTEXT.md`: current performance state, evidence, hypotheses, checkpoints, and active experimental context.

## Status Conventions
Use these explicit markers and keep history:
- `[OPEN]`
- `[IN-PROGRESS]`
- `[DEFERRED]`
- `[CANCELLED]`
- `[DONE]`

## Backlog Items

### 1) [OPEN] Separate FakeServer into a dedicated solution project

**What**
Move the benchmark FakeServer implementation out of `VectorNNTP.BackFiller.Benchmarks` and establish a reusable, independently maintained FakeServer/test-server project in the solution.

Current implementation location:
- `VectorNNTP.BackFiller.Benchmarks/Execution/BenchmarkDevNullTransitServer.cs`

Desired architectural direction:
- Dedicated FakeServer project
  - reusable protocol/server infrastructure
  - NNTPD / Transit testing
  - NNRPD / Reader testing
  - BackFiller benchmark infrastructure
  - protocol/framing tests
  - failure/disconnect testing
  - load/stress testing
  - future server-side performance experiments

**Why**
The current FakeServer has already proven valuable for isolating BackFiller performance from Vector.NNTP.NNTPD performance. As NNTPD/Transit and NNRPD/Reader testing expands, a reusable FakeServer project enables a controlled shared test environment across protocol implementations without coupling server infrastructure to a single benchmark project.

**Scope Notes**
- Backlog item only. Do not implement this refactor without explicit scoped authorization.
- Before execution, define migration scope, project boundaries, ownership, and compatibility with active governance/performance checkpoints.

### 2) [OPEN] Reduce dispatch-consumer wake-to-run latency (interval C)

**What**
Queue-read forensics (`docs/benchmarks/queue-consumer-callstack-forensics.md`) established that dispatch consumers are
purely async-parked in `await BoundedArticleQueue.WaitToReadAsync(...)` and that the observed wait time resides in
interval C (item becomes eligible -> the parked consumer's continuation actually runs) plus interval A (queue genuinely
empty). `TryRead` itself is sub-microsecond and no lock, semaphore, or synchronous blocking wait exists between
`WaitToReadAsync` and `TryRead`.

**Why**
Any future latency work on the dispatch path must target continuation scheduling / producer supply, not the queue read
itself. Recording it here prevents rediscovering the same evidence.

**Scope Notes**
- Backlog item only. Forensics work was instrumentation-only and changed no architecture, batching, consumer count, or
  ThreadPool settings; the same constraints apply until an optimization phase is explicitly authorized.

### 3) [OPEN] Decide whether `BoundedArticleQueue` depth accounting should be exact

**What**
`BoundedArticleQueue.CurrentQueuedCount` is an independent counter updated *after* the corresponding channel operation
in both directions, so it can transiently under-count, over-count, and even read negative. Forensic runs observed
negative depths at WAIT_START and classified 92 failed `TryRead`s as "undeterminable" for that reason.

**Why**
Reported queue depth is used for reasoning about backlog; it is currently an approximation and must not be treated as
"items `ChannelReader.TryRead` can return".

**Scope Notes**
- Backlog item only. Changing the accounting is a behaviour change and requires explicit authorization.
