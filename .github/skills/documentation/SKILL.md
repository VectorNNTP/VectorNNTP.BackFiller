---
name: documentation
description: Add and improve accurate XML documentation in the VectorNNTP.BackFiller C# codebase without changing behavior.
---
# Documentation skill

## Inspect before writing

Read the complete target file, partial siblings, interfaces/base types, callers, consumers, tests, protocol/configuration documentation, and relevant history where needed. Search actual repository usage. Establish lifecycle, ownership, cancellation, threading, state, error, logging, I/O, buffering, allocation, and performance assumptions before writing prose. Do not infer semantics from names.

## Identify meaningful symbols

Review types, constructors, methods, properties, fields, constants, events, operators, indexers, nested types, enum members, test fixtures, and helpers at every accessibility level. Document symbols whose contract, invariant, state transition, mapping, resource lifetime, concurrency rule, validation rule, failure behavior, framework integration, or verified performance characteristic would help an engineer. Do not manufacture comments for obvious private implementation details.

## Write accurate engineering-contract XML

Use concise `<summary>` text; accurate `<param>`, `<returns>`, `<value>`, and `<typeparam>` elements; and `<remarks>` for substantive context. Use `<exception>` only when the method establishes a meaningful thrown-exception contract. Distinguish thrown exceptions from exception arguments, caught/logged exceptions, and failures represented in results. Use `<see cref>`, `<seealso>`, and `<c>` when references genuinely clarify the contract.

For generated logging or other source-generated/framework declarations, document the semantic behavior established by the declaration, including applicable severity and structured properties. Do not document speculative generated internals or imply a log is always emitted when filtering can suppress it. Document concurrency, cancellation, disposal, side effects, protocol/configuration constraints, and performance only when supported by implementation or evidence.

## Preserve scope and existing quality

Preserve accurate existing documentation verbatim. Improve vague, incomplete, misleading, or incorrect prose rather than deleting it. Documentation passes are additive-only, change only the selected target file, and must not alter behavior, API design, signatures, source generation, logging, configuration, analyzers, tests, or related files. Never add boilerplate, warning suppressions, `NoWarn`, exclusions, or placeholder text.

## Validate and report

Check escaped XML, valid tags, unique tags, real cref/seealso/parameter/type references, property `<value>`, method-only `<returns>`, and grammatically complete prose. Inspect the final diff to prove no behavior or scope drift. Build only the target production project; do not run tests unless explicitly requested. Report reviewed/new/improved members, tag counts, intentionally unchanged documentation, pre-existing concerns, build result, no-test status, single-file scope, and no behavior change.
