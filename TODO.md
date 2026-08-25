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

### 1) [OPEN] Extract temporary BackFiller-local validation implementations into a shared validation library

**What**
Extract the temporary NNTP validation implementations currently duplicated/adapted inside BackFiller into a dedicated shared validation package once architecture boundaries are stable.

Current temporary local implementation locations:
- `VectorNNTP.BackFiller/Runtime/Articles/Validation/NntpMessageIdValidation.cs`
- `VectorNNTP.BackFiller/Runtime/Articles/Validation/NntpMessageIdValidationSimd.cs`
- `VectorNNTP.BackFiller/Runtime/Articles/Validation/NntpMessageIdCharClasses.cs`

Reference implementation provenance:
- `C:\Users\chrisk\source\repos\Vector.NNTP\Vector.NNTP.Utilities\Validation`

Desired future direction:
- one authoritative shared validation library
- no duplicated Message-ID grammar/SIMD/bitmap logic across repos
- explicit compatibility and allocation-behavior tests during extraction

**Why**
BackFiller currently uses controlled temporary duplication by decision to avoid coupling to an immature shared boundary. This preserves delivery velocity now, but long-term maintainability requires one authoritative shared validation package to prevent behavioral drift and duplicated optimization work.

**Scope Notes**
- Backlog item only. Do not perform extraction in the current acquisition task.
- Extraction must preserve NNTP/INN Message-ID grammar behavior and hot-path allocation characteristics.

### 2) [OPEN] Separate FakeServer into a dedicated solution project

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
