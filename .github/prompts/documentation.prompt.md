---
mode: agent
description: Perform the canonical, additive, single-file XML documentation pass for VectorNNTP.BackFiller.
---
# Canonical VectorNNTP.BackFiller documentation-only pass

Documentation-only pass for:

`<TARGET_FILE>`

IMPORTANT: Execute this entire task continuously from start to finish. Do not stop between individual edits/files. The ONLY file you may modify is `<TARGET_FILE>`. Stop only for a genuine blocking issue, compilation failure, or another condition that requires my input.

## Objective

Document `<TARGET_FILE>` to the engineering-contract standard established by completed VectorNNTP.BackFiller documentation passes. This is not a tag-counting exercise. Documentation must help an engineer understand the code's responsibility, observable behavior, relationships, contracts, constraints, side effects, failure behavior, concurrency, lifecycle, and material performance implications.

Base every claim on the actual implementation and verified repository usage.

## Read the entire file first

Before editing:

1. Read the entire target file.
2. Understand every type, partial type, member, existing XML comment, attribute, generator declaration, and helper.
3. Search relevant repository call sites and inspect related types and implementations.
4. Verify actual inputs, outputs, state changes, exceptions, cancellation, concurrency, resource ownership, I/O, buffering, logging, allocation, and performance behavior.
5. Understand framework-, compiler-, dependency-injection-, and source-generator-consumed contracts.
6. Do not infer semantics from names alone.
7. Do not modify any other file.

## Documentation standard

For each type or partial type, write a precise `<summary>` and use `<remarks>` for useful architectural context, invariants, constraints, lifecycle, concurrency, or other substantive information. Avoid generic wording such as “Provides functionality for” or “Handles” unless expanded into an actual contract.

For every public, protected, internal, or otherwise meaningful method:

- Explain what it actually does, meaningful parameters, return values, side effects, state changes, failure behavior, asynchronous/cancellation/thread-safety behavior, and material performance behavior.
- Add `<param>` and `<returns>` only when applicable.
- Add private documentation only when it captures a non-obvious architectural decision, algorithm, invariant, state transition, error rule, validation rule, resource/lifetime rule, representation mapping, or framework contract.
- Do not invent behavior or mechanically repeat documentation that is already sufficient.

For properties, use `<value>` rather than `<returns>` and document useful meaning, state, lifecycle, or mutability. Document constants and fields when their meaning is part of an important contract. Document enum purpose and non-obvious meaningful members without inventing numeric stability guarantees.

Use `<exception>` only for an established exception contract. Distinguish exceptions thrown by a method from exception values passed in, logged, inspected, caught, or represented in a result.

For generated or framework-integrated declarations, document the semantic contract exposed to callers. For generated logging, document declared severity and structured properties only when they are observable and established by the declaration; do not imply emission when filtering may suppress it.

Use valid XML documentation: `<summary>`, `<param>`, `<returns>`, `<value>`, `<exception>`, `<remarks>`, `<typeparam>`, and `<seealso>` only with real applicable targets. Avoid duplicate tags, malformed XML, invalid property returns, broken cref references, incomplete prose, and boilerplate such as “Gets or sets” or “Performs the operation” unless expanded into useful semantics.

## Preserve good documentation

Preserve accurate, useful existing XML verbatim. Improve documentation only when vague, incomplete, misleading, or incorrect; do not rewrite good documentation for style. Do not perform unrelated cleanup elsewhere.

## Documentation-only restriction

Do not refactor, rename, change signatures/accessibility/types/constants/event IDs/log levels/templates/structured fields/attributes/source generation/exception handling/filtering/logging calls/validation/algorithms/configuration/analyzers/tests, or modify related source files. If an issue is discovered, leave it untouched and report it separately.

## Validation

After editing:

1. Verify that only `<TARGET_FILE>` was modified by this pass.
2. Build only the target production project containing the file; the build must succeed.
3. Do not run tests unless explicitly requested.
4. Inspect the final diff for behavior, formatting, scope, and documentation accuracy.
5. Confirm XML validity; correct use of property `<value>`, method `<returns>`, real generic/parameter references, and meaningful exception contracts.
6. Confirm no useful documentation was removed and no source-generation declaration, event ID, level, template, or structured field changed.

## Final report

Report:

- Total members reviewed.
- Members newly documented.
- Members whose documentation was meaningfully improved.
- Changes by tag: `<summary>`, `<param>`, `<returns>`, `<exception>`, `<remarks>`, `<value>`, and `<seealso>`.
- Documentation intentionally left unchanged and why.
- Pre-existing logging/design/code-quality/performance concerns intentionally left untouched.
- Build result.
- Confirmation that tests were not run.
- Confirmation that only `<TARGET_FILE>` was modified.
- Confirmation that no production behavior changed.

## Reusability

This prompt is intended for repeated use against individual C# files. Replace `<TARGET_FILE>` with the selected file, adapt repository/call-site inspection to that file, and retain the complete documentation-only restriction, single-file scope, production-project build, no-tests-by-default rule, and final reporting requirements. Do not weaken the workflow because a target appears simple.

Follow `.github/instructions/documentation.instructions.md` for permanent standards and `.github/skills/documentation/SKILL.md` for detailed methodology.
