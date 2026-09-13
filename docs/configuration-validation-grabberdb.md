# Configuration Validation - ConnectionStrings:GrabberDB

## Overview

The `VectorNNTP.BackFiller` application implements comprehensive validation for the `ConnectionStrings:GrabberDB` configuration setting according to specification **3.15.1**.

## Purpose

`ConnectionStrings:GrabberDB` defines the MySQL connection string for the VectorNNTP control-plane database, used for:
- Application configuration retrieval
- Operational state management
- Provider connection lifecycle decisions

**CRITICAL:** This database is **NOT** part of the high-throughput article retrieval data path. Database queries must never be required for individual article requests.

## Validation Phases

### Phase 1: Configuration Validation (Startup)

The application performs the following validation checks at startup **before** attempting to connect:

#### 1.1 Presence and Non-Empty
- **Rule:** Connection string must be present and non-empty
- **Error:** `"ConnectionStrings:GrabberDB is required"`
- **Test Coverage:** `Validate_NullConnectionString_ReturnsRequiredError`, `Validate_EmptyConnectionString_ReturnsRequiredError`, `Validate_WhitespaceConnectionString_ReturnsRequiredError`

#### 1.2 Syntax Validation
- **Rule:** Connection string must have valid key=value syntax
- **Error:** `"Invalid connection string syntax: {exception message}"`
- **Test Coverage:** `Validate_MalformedConnectionString_ReturnsSyntaxError`, `Validate_InvalidKeyValuePairs_ReturnsSyntaxError`
- **Implementation:** Uses `DbConnectionStringBuilder` to parse and validate syntax

#### 1.3 Server/Host Validation
- **Rule:** Must specify server/host using one of: `Server`, `Host`, `Data Source`, `DataSource`
- **Error:** `"Connection string must specify a server/host (Server, Host, or Data Source)"`
- **Test Coverage:** `Validate_MissingServer_ReturnsServerRequiredError`, `Validate_EmptyServer_ReturnsServerError`, `Validate_ValidServerKeyVariations_AcceptsServerValue`
- **Note:** `DbConnectionStringBuilder` removes keys with empty values, so `"Server="` is treated as missing

#### 1.4 Database Name Validation
- **Rule:** Must specify database name using one of: `Database`, `Initial Catalog`, `InitialCatalog`
- **Error:** `"Connection string must specify a database name (Database or Initial Catalog)"`
- **Test Coverage:** `Validate_MissingDatabase_ReturnsDatabaseRequiredError`, `Validate_EmptyDatabase_ReturnsDatabaseError`, `Validate_ValidDatabaseKeyVariations_AcceptsDatabaseValue`

#### 1.5 Connection Pooling Validation
- **Rule:** Pool size must be appropriate for control-plane database usage
- **Recommendations:**
  - `Min Pool Size`: 0 or 1 (single connection or very small pool)
  - `Max Pool Size`: ≤10 (NOT a high-throughput database)
- **Severity:** **Warning** (not error - application can start with suboptimal pool settings)
- **Warnings:**
  - `"Min Pool Size={value} is excessive for control-plane database. Recommended: 0 or 1 (GrabberDB has low utilization and should use a single connection or very small pool)"`
  - `"Max Pool Size={value} is excessive for control-plane database. Recommended: ≤10 (GrabberDB is NOT part of the article retrieval data path and does not require large pools)"`
- **Test Coverage:** `Validate_ExcessiveMinPoolSize_ReturnsPoolingWarning`, `Validate_ExcessiveMaxPoolSize_ReturnsPoolingWarning`, `Validate_AppropriatePoolSize_AcceptsConfiguration`
- **Note:** See [Validation Errors vs. Warnings](./validation-errors-vs-warnings.md) for why pool settings are warnings, not errors

### Phase 2: Startup Dependency Probe (Server-Level MySQL Validation)

After configuration validation and runtime snapshot creation pass, startup runs a **server-level MySQL dependency probe** against the frozen GrabberDB projection. This probe intentionally validates the MySQL endpoint without selecting the target runtime database.

#### 2.1 Server-Level Probe Contract

**What It Validates:**
- ✅ Network reachability (can we reach the MySQL server on host:port?)
- ✅ Authentication handshake (are credentials accepted?)
- ✅ TLS/SSL negotiation (if configured)
- ✅ Protocol compatibility (server/auth plugin compatibility)

**What It Does NOT Validate:**
- ❌ Target database existence/accessibility
- ❌ Required table existence
- ❌ Target-database query permissions via `SELECT 1`

A missing target database is **not** a dependency-probe failure under this contract.

**Implementation:**
- **Provider:** `MySqlConnector`
- **Method:** `DatabaseDependencyProbe.ValidateDatabaseConnectivityAsync(...)`
- **Behavior:** Builds a server-level connection from the frozen runtime projection, clears the database selection, and validates `OpenAsync(...)` only.

**Failure Classification:**
- Distinguishes timeouts, authentication failures, TLS/protocol problems, and network reachability failures.
- Uses sanitized startup dependency diagnostics without exposing credentials or full connection strings.

### Phase 3: Authoritative Startup Provisioning Boundary

Target-database and schema readiness are enforced later by the authoritative startup provisioning boundary:

- Hosted initializer: `NntpAccountSnapshotStartupInitializer.StartAsync(...)`
- Provisioning owner: `MySqlNntpAccountSnapshotProvider.EnsureStartupDependenciesAsync(...)`
- Production store operation sequence:
  1. Ensure database exists (`CREATE DATABASE IF NOT EXISTS ...`)
  2. Open selected database
  3. Ensure required table exists (`CREATE TABLE IF NOT EXISTS nntpbackfilleraccounts ...`)

This boundary is where missing-database and missing-table scenarios are resolved (or fail deterministically when privileges are insufficient).

**Control-Plane Constraint:** Validation/provisioning run during startup only and remain outside the article retrieval data path.

**Documentation:** See [MySQL Runtime Validation](./mysql-runtime-validation.md) for startup dependency/provisioning sequencing and diagnostics semantics.

## Error Handling Philosophy

### Configuration Errors
- **Behavior:** ALWAYS block startup (exit code 2)
- **Collection:** All errors are collected before reporting (not fail-fast)
- **Logging:** Uses `LogConfigurationValidationErrors(...)` to report all issues

### Dependency Errors
- **Behavior:** Block startup if database is a mandatory dependency
- **Collection:** All failures/warnings/errors are collected
- **Logging:** Uses `LogDependencyValidationErrors(...)` to report connectivity issues

## Implementation Architecture

### Files
- **`VectorNNTP.BackFiller/Configuration/ConnectionStringsOptions.cs`**
  - `ConnectionStringsOptions` class with `[Required]` attribute on `GrabberDB` property
  - `ConnectionStringValidator` static class with `Validate(...)` method
  - Connection pooling validation
  - **No provider inference:** Provider validation happens at runtime via actual connectivity testing

- **`VectorNNTP.BackFiller/Startup/Validation/StartupValidationPipeline.cs`**
  - Startup validation orchestration and runtime snapshot construction
- **`VectorNNTP.BackFiller/Startup/Validation/DependencyProbeRunner.cs`**
  - Dependency probe coordination using frozen runtime options
- **`VectorNNTP.BackFiller/Startup/Validation/DatabaseDependencyProbe.cs`**
  - Server-level MySQL dependency probe (`OpenAsync` on server-level target)
- **`VectorNNTP.BackFiller/Runtime/Accounts/NntpAccountSnapshotStartupInitializer.cs`**
  - Authoritative hosted startup provisioning boundary invocation
- **`VectorNNTP.BackFiller/Runtime/Accounts/MySqlNntpAccountSnapshotProvider.cs`**
  - Database/table provisioning and initial account snapshot load boundary

### Test Coverage
- **`VectorNNTP.BackFiller.Tests/ConnectionStringValidationTests.cs`**
  - 36 comprehensive tests covering all validation rules
  - Basic validation (null, empty, whitespace)
  - Syntax validation (malformed strings)
  - Required components (server, database, authentication)
  - Connection pooling
  - Multiple errors scenarios
  - Well-formed connection strings

### Test Results
```
Test summary: total: 185, failed: 0, succeeded: 185, skipped: 0
```
- 36 ConnectionStringValidation tests
- 149 existing tests (ServiceLifecycle, ConfigurationFingerprint, etc.)

## Usage Example

### Valid MySQL Connection String (Current Configuration)
```json
{
  "ConnectionStrings": {
	"GrabberDB": "Server=198.18.0.3;User ID=nntparticles;Password=<redacted>;Database=nntp;Minimum Pool Size=1;Maximum Pool Size=5;Connection Idle Timeout=10"
  }
}
```

### Validation Flow
1. **Startup:** `Program.cs` calls startup validation pipeline to validate config and build frozen runtime options.
2. **Configuration Phase:** Validates syntax, server, database, authentication, pooling.
3. **Dependency Probe Phase:** Performs server-level MySQL reachability/auth/TLS validation against frozen runtime projection (no target database selection).
4. **Host Startup Provisioning Phase:** `NntpAccountSnapshotStartupInitializer` invokes provider provisioning to create/verify target database and required accounts table.
5. **Initial Snapshot Phase:** Provider loads and publishes initial account snapshot.
6. **Failure Handling:** Configuration errors fail configuration validation; dependency probe failures fail dependency validation; provisioning failures fail hosted startup deterministically.

## Operational Semantics

### Database Usage Patterns
- **Infrequent:** Periodic queries for provider connection lifecycle decisions
- **Asynchronous:** All database operations use async/await
- **Isolated:** Database operations performed by dedicated control-plane components
- **Not in Article Path:** Database queries never required for individual article requests

### Resilience
- **Temporary Outages:** Should NOT interrupt active article retrieval
- **Cached Configuration:** Grabber continues with cached provider configuration during reconnection
- **Startup:** Database is **mandatory** dependency - unreachable database blocks startup

### Connection Strategy
- **Single Connection or Small Pool:** NOT a high-performance connection pool
- **Min Pool Size:** 0 or 1
- **Max Pool Size:** ≤10
- **Justification:** Control-plane usage pattern, not article-level concurrency control

## Future Enhancements

### Configuration Hot-Reload
The current implementation validates at startup. Future enhancements could:
- Monitor configuration changes
- Re-validate on reload
- Gracefully handle database connection string updates without restart

### Health Checks
The connectivity test could be integrated with:
- ASP.NET Core Health Checks
- Periodic background connectivity verification
- Readiness/liveness probes for Kubernetes

## Related Specifications

- **3.15.1 ConnectionStrings:GrabberDB** (implemented in this document)
- Configuration fingerprinting (already implemented)
- Service lifecycle state machine (already implemented)
