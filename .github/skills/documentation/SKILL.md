---
name: documentation
description: Add and improve accurate XML documentation in the VectorNNTP.BackFiller C# codebase without changing behavior.
---
# Documentation skill

## Inspect first

Read the entire file, its partial siblings, interfaces/base types, callers, consumers, tests, and nearby protocol/configuration documentation. Identify lifecycle, ownership, cancellation, threading, error, and performance assumptions before writing prose.

## Find meaningful symbols

Cover documentable types and members at every accessibility level: constructors, methods, properties, fields, constants, events, operators, indexers, nested types, enum members, local helpers with non-obvious contracts, and test fixtures/helpers. Prioritize symbols whose behavior or invariant is not obvious from its name.

## Write useful XML

Use concise `<summary>` text and accurate `<param>`, `<returns>`, and `<exception>` elements. Add `<remarks>` for lifecycle, ownership/disposal, concurrency/thread safety, cancellation, side effects, protocol/configuration constraints, and only verified allocation or performance properties. Use `<see cref>` and `<c>` references where they clarify the contract. Describe what callers can rely on, not implementation trivia.

## Preserve behavior and existing quality

Keep valid existing XML unchanged and make additive-only edits. Do not rename symbols, reorder code, alter control flow, change public API design, modify tests, or introduce abstractions. Never use warning suppression, `NoWarn`, exclusions, or placeholder prose to conceal missing documentation.

## Validate

Review generated XML for correct cref/param names and escaped markup. Inspect the diff for accidental behavior changes, then run the existing relevant build with analyzers and focused tests followed by the normal warning-free rebuild. Treat new diagnostics as defects to fix at their source; distinguish unrelated pre-existing diagnostics explicitly.
