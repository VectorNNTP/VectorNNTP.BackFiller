---
mode: agent
description: Review VectorNNTP.BackFiller changes for correctness, architecture, concurrency, and operational safety.
---
# VectorNNTP.BackFiller code review

Read the complete diff and surrounding callers, interfaces, tests, configuration, lifecycle, and ownership paths before reporting findings. Review correctness first, then exactly-once work settlement, protocol behavior, cancellation, shutdown, resource disposal, error handling, security, analyzers/documentation, and unintended API or behavior changes.

Treat RabbitMQ, NNTP, DNS, database, and certificate inputs as failure-prone or untrusted. Check async code for races, lost exceptions, blocking waits, premature disposal, retry/accounting errors, and readiness assumptions. Check logging for structured correlation, UTC/culture invariance, payload/credential leaks, and hot-path cost.

Report only specific, actionable defects or material risks with severity, location, impact, and correction. Do not propose unrelated refactoring or speculative optimization; require tests for behavioral changes and preserve existing baselines.
