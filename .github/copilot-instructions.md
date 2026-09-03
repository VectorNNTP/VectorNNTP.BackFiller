# VectorNNTP.BackFiller Copilot Instructions

## Project

VectorNNTP.BackFiller is a .NET 8, C# 13 worker service that retrieves unavailable Usenet articles from external NNTP providers, validates them, and returns recovered work through the VectorNNTP pipeline. It is high-throughput, highly concurrent networking infrastructure; correctness, bounded resources, and graceful shutdown are essential.

The solution is `VectorNNTP.BackFiller.slnx` and contains:

- `VectorNNTP.BackFiller/`: production worker, startup/configuration validation, certificates, RabbitMQ, NNTP transit/listener, article parsing and processing.
- `VectorNNTP.BackFiller.Tests/`: xUnit unit, integration-style, lifecycle, protocol, validation, and regression tests.
- `VectorNNTP.BackFiller.Benchmarks/`: BenchmarkDotNet and controlled transit performance harnesses.
- `VectorNNTP.BackFiller.Benchmarks.Tests/`: benchmark contract and infrastructure tests.
- `tools/testing/`: isolated regression and watchdog tooling.
- `.github/workflows/`: build, coverage, dependency, and CodeQL validation.
- `.copilot/PERFORMANCE-CONTEXT.md`: existing performance checkpoint; preserve it and treat source, history, tests, and artifacts as stronger evidence.

## Runtime and architecture

- Target `net8.0`, x64 only, nullable enabled, implicit usings enabled, and C# 13. Release publishing is self-contained/single-file with ReadyToRun, tiered PGO, and server GC.
- Startup must be ordered: bind configuration, validate it, create one immutable canonical runtime snapshot, validate directories, validate dependencies, then construct/start DI and hosted services. Do not perform expensive, irreversible, externally visible work before validation.
- Do not infer readiness from service-registration order. Enforce readiness through explicit dependencies/orchestration; `ServiceLifecycle.Ready` and systemd `READY=1` represent the same operational milestone.
- Every admitted unit of work must be settled exactly once on success, failure, cancellation, timeout, and shutdown. Preserve ownership, ACK/NACK, retry, backpressure, and drain invariants.
- Treat external NNTP, RabbitMQ, DNS, Cloudflare, ACME, and database input as untrusted or failure-prone. Preserve cancellation and disposal ownership across asynchronous boundaries.
- Cloudflare validation is required independently of the optional Let's Encrypt issuance flow. ACME-only settings are ignored when issuance is disabled.
- Listener wildcard semantics are explicit: empty address maps to independent `0.0.0.0` and `::` endpoints; `*`, `Any`, `0.0.0.0`, and `::` are wildcard tokens, not published DNS addresses. Configure IPv6-only behavior explicitly and derive wildcard DNS addresses from eligible local interfaces.
- Use UTC and culture-invariant machine-facing formatting. Article logs include structured `MessageId`; protocol TX/RX debug logs exclude payloads and credentials; article outcomes include `MessageId`, `Outcome`, and monotonic elapsed `Duration`.

## Build and test

From the repository root:

```text
dotnet restore VectorNNTP.BackFiller.slnx
dotnet build VectorNNTP.BackFiller.slnx --configuration Release -p:Platform=x64
dotnet test VectorNNTP.BackFiller.Tests/VectorNNTP.BackFiller.Tests.csproj --configuration Release -p:Platform=x64
dotnet test VectorNNTP.BackFiller.Benchmarks.Tests/VectorNNTP.BackFiller.Benchmarks.Tests.csproj --configuration Release -p:Platform=x64
```

Use the repository's isolated regression/watchdog tooling for potentially blocking lifecycle or concurrency tests. Do not use `--no-build` for benchmark validation: clean, build, verify runtime identity, then run matching Debug/x64/net8.0/win-x64 settings. Preserve CI diagnostics and existing baselines.

Builds enable .NET analyzers, code style, and generated documentation. Completion requires zero errors and warnings attributable to the change; fix source/design issues rather than suppressing diagnostics. Do not weaken, skip, or remove tests.

## Engineering conventions

- Follow `CONTRIBUTING.md` and `.editorconfig`: explicit access modifiers, `_camelCase` private fields, PascalCase public members/constants, camelCase locals/parameters, x64 platform, and a 160-character practical line limit.
- Prefer simple designs and existing abstractions. Do not add DI seams solely to mock straightforward startup logic.
- Prefer async I/O, `ConfigureAwait(false)` where appropriate, deterministic coordination over sleeps, and explicit cancellation. Avoid blocking waits, fire-and-forget work with unobserved failures, unnecessary LINQ/allocations/copies/boxing, and contention in hot paths.
- Performance changes require a baseline, one measurable change at a time, stable configuration, and evidence covering throughput, latency/tails, CPU, allocations/GC, memory, queues, and connections. Do not optimize speculative theory.
- Use structured source-generated logging where the existing project pattern applies; guard expensive disabled-level work and never log secrets.
- Document every documentable C# symbol, including meaningful private/internal helpers and tests. Preserve valid XML documentation verbatim during additive documentation passes. Every `.cs` file needs the repository header and required attribution: `Copyright © Chris Knipe cknipe@opticnetworks.net`.
- Keep behavior/API changes narrowly scoped, add regression coverage for behavior changes, and update related documentation/history when an architectural decision or measured result changes.
