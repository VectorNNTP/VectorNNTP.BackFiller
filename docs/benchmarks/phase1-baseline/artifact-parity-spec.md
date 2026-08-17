# Artifact and Output Parity Specification (Phase 1)

## Purpose
Define how pre/post extraction benchmark parity is evaluated without allowing behavior drift.

## Structural Parity (must remain identical)
- Artifact schema shape
- Field names
- Field ordering (where applicable)
- CSV header names/order
- Console report section names/order
- Metric labels and units

## Semantic Parity (must remain identical)
- Counter definitions and aggregation behavior
- Throughput formulas and unit math
- Latency formulas and stage relationships
- Status classification grouping
- Timing model meaning for each reported stage

## Naturally Variable (allowed)
- Wall-clock timestamps
- Runtime-generated identifiers
- Minor runtime-duration jitter
- CPU/GC/heap sampled values

## Manual Review Required
Manual review is mandatory if any of the following differ:
- Added/removed/renamed fields
- Added/removed/reordered report sections
- Any formula output that implies equation changes
- Status counts that imply classification change
- Timing outputs that imply stage redefinition

## Phase 1 Enforcement
Any structural or semantic parity break blocks progression to the next extraction commit.
Potential improvements discovered during extraction must be logged separately and excluded from Phase 1 commits.
