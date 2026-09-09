# ACME Account Key Test Fixture Provenance

This directory contains deterministic ACME account-key fixture material used only by startup/configuration tests.

- Scope: `VectorNNTP.BackFiller.Tests` only
- Usage: copied into isolated or shared test cert directories by test helpers
- Credential status: non-production test fixture key material

## Included fixture files

- `account-private-key.pem`

The fixture file is static test data and is not generated at runtime.
