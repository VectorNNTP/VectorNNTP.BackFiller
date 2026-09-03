---
mode: agent
description: Review VectorNNTP.BackFiller tests for meaningful, deterministic regression protection.
---
# VectorNNTP.BackFiller test review

Review tests with their production contracts, callers, lifecycle, and failure paths. Check correctness, meaningful observable assertions, completeness, determinism, isolation, concurrency safety, cancellation/disposal coverage, exactly-once settlement, protocol/configuration regression protection, and preservation of benchmark and CI baselines.

Prefer explicit synchronization over sleeps, and classify failures before recommending changes. Never recommend removing, skipping, weakening, suppressing, or bypassing a test merely to obtain a passing build. Test changes must protect the actual failure mode rather than only implementation details.
