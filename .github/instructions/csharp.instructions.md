---
applyTo: "**/*.cs"
---
# C# project conventions

- Target net8.0 with C# 13, nullable enabled, implicit usings enabled, and x64 platform settings. Use explicit access modifiers and follow the naming/style rules in `CONTRIBUTING.md` and `.editorconfig`.
- Preserve the repository's file-specific header format and required attribution. Add meaningful XML documentation for every documentable symbol, including internal/private helpers and test symbols; document contracts, ownership, failure, threading, and side effects rather than adding filler.
- Keep startup bind/validate/normalize logic authoritative and use the immutable validated runtime snapshot thereafter. Do not rebind raw configuration in later services.
- Prefer async I/O and explicit cancellation/disposal ownership. Never introduce blocking waits, unobserved fire-and-forget tasks, arbitrary sleeps, or timing hacks in concurrency code.
- Preserve exactly-once work settlement, bounded admission/backpressure, ACK/NACK semantics, retry behavior, and shutdown drain guarantees. Treat provider and protocol data as untrusted.
- Use existing abstractions and patterns; avoid new DI indirection without a concrete need. Use source-generated structured logging where established, with masked credentials and no article payload logging.
- Builds must remain analyzer/style/documentation-warning free. Fix diagnostics in source; do not add suppressions, `NoWarn`, exclusions, or equivalent hiding.
