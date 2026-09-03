---
applyTo: "**/*.{cs,md}"
---
# Documentation standards

- Every `.cs` file starts with a specific header containing purpose/responsibility and `Copyright © Chris Knipe cknipe@opticnetworks.net`.
- Documentation is an engineering contract, not tag counting. Base claims on implementation and verified repository usage, not names or assumptions.
- Provide meaningful XML documentation for documentable types and members at every accessibility level, including tests and private helpers when they communicate a non-obvious contract. Explain purpose, invariants, parameters, returns, exceptions, lifecycle, ownership, threading, cancellation, performance, logging, and side effects where applicable.
- Use `<summary>` for primary descriptions, `<param>` for actual parameters, `<returns>` only for returned values, `<value>` for properties, `<exception>` only for established thrown-exception contracts, and `<remarks>`, `<typeparam>`, and `<seealso>` only when substantively applicable.
- Preserve existing accurate XML verbatim. Improve vague or incorrect documentation without deleting it, and make documentation passes additive-only and limited to the target file.
- Treat source-generated and framework-integrated declarations according to their actual semantic contract. Distinguish thrown exceptions from exception parameters, logged exceptions, and represented failures; do not imply logs are emitted when filtering can suppress them.
- Ensure XML is well-formed, tags are non-duplicated, references and parameter names exist, property documentation does not use `<returns>`, and prose does not invent behavior or external guarantees.
- Avoid boilerplate and meaningless private documentation. Do not alter behavior, API design, namespaces, tests, source-generation declarations, logging, configuration, analyzers, or production code merely to improve documentation.
- Never hide documentation diagnostics with suppressions or exclusions. Resolve underlying issues and validate the target production project build; documentation passes do not run tests unless explicitly requested.
- Follow established terminology and `CONTRIBUTING.md` formatting. The reusable workflow is `.github/prompts/documentation.prompt.md`; detailed methodology is `.github/skills/documentation/SKILL.md`.
