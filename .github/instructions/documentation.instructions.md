---
applyTo: "**/*.{cs,md}"
---
# Documentation standards

- Every `.cs` file starts with a specific header containing purpose/responsibility and `Copyright © Chris Knipe cknipe@opticnetworks.net`.
- Provide meaningful XML documentation for every documentable C# type and member, regardless of accessibility, including tests and private helpers where they communicate a contract. Explain purpose, invariants, parameters, returns, exceptions, lifecycle, threading, performance, and side effects when applicable.
- Preserve existing high-quality XML documentation verbatim in documentation passes; make additive-only changes and do not alter behavior, API design, namespaces, or tests merely to silence warnings.
- Never hide documentation diagnostics with suppressions or exclusions. Resolve the underlying documentation or design issue and validate with the normal build/analyzers.
- Follow established terminology and `CONTRIBUTING.md` formatting. Documentation must be precise enough to guide maintenance, not generic boilerplate.
