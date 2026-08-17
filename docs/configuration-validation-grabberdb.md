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

### Phase 2: Runtime MySQL Connectivity Validation (Startup)

After configuration validation passes, the application performs a **MySQL connectivity test** using actual `MySqlConnection.OpenAsync()` and query execution. This validates what static validation cannot detect.

#### 2.1 MySQL Runtime Validation

**What It Validates:**
- ✅ Network reachability (can we reach the MySQL server on host:port?)
- ✅ Authentication (are credentials valid? is the auth plugin supported?)
- ✅ Database accessibility (does the database exist? is it accessible?)
- ✅ Permissions (can we execute basic queries like `SELECT 1`?)
- ✅ TLS/SSL negotiation (if `SslMode` is configured)
- ✅ Protocol compatibility (server version, authentication plugins)

**What Static Validation CANNOT Catch:**
- ❌ Invalid credentials (wrong username/password or expired token)
- ❌ MySQL server unreachable (network down, firewall, wrong host/port)
- ❌ Unsupported authentication plugin (e.g., `caching_sha2_password` issues)
- ❌ Database doesn't exist or is inaccessible
- ❌ Permission denied (user lacks necessary privileges)
- ❌ TLS/SSL configuration failures
- ❌ Incompatible MySQL server version

**Implementation:**
- **Provider:** This application uses **MySQL exclusively** via `MySqlConnector`
- **Method:** `ValidateDatabaseConnectivityAsync()` in `Program.Validation.cs`
- **Steps:**
  1. Create `MySqlConnector.MySqlConnection` with configured connection string
  2. Call `OpenAsync()` with timeout to validate network, auth, TLS
  3. Execute `SELECT 1` to verify database access and permissions
  4. Log success with actual server and database name from connection properties

- **Timeout:** Configurable (default: 10 seconds via `dependencyTimeout`)
- **Success:** Logs `"GrabberDB connectivity validated successfully (Server: {Server}, Database: {Database})"`
- **Failures:**
  - **Timeout:** `"Connection timeout after {timeout}s"` (network/firewall/wrong host)
  - **MySqlException:** `"MySQL connection failed: {message} (Error #{errorNumber})"` with specific error codes:
    - `1045` - Access denied (wrong username/password)
    - `1049` - Unknown database (database doesn't exist)
    - `1130` - Host not allowed (IP restriction)
    - `2002` - Can't connect to MySQL server (Unix socket)
    - `2003` - Can't connect to MySQL server (TCP - host/port unreachable)
    - `2013` - Lost connection during query
    - `2026` - SSL connection error (TLS handshake failure)
    - `2061` - Authentication plugin error (unsupported auth method)
  - **Other Exception:** `"Failed to connect: {message}"` (DNS failure, socket errors, etc.)

- **Cancellation:** Propagates shutdown cancellation token

**Critical Principle:** Provider validation happens at **runtime** via actual connectivity testing with MySqlConnector, not via unreliable connection string heuristics. If the connection string is for a non-MySQL database, the connectivity test will fail with a clear error.

**Control-Plane Constraint:** This validation runs **ONCE** during startup, **NOT** in the article retrieval data path. Temporary database outages after startup MUST NOT interrupt active article operations.

**Documentation:** See [MySQL Runtime Validation](./mysql-runtime-validation.md) for complete details, error codes, and design rationale.

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

- **`VectorNNTP.BackFiller/Program.Validation.cs`**
  - `ConfigurationValidationResult` class
  - `DependencyValidationResult` class
  - `ValidateConfigurationAndDependenciesAsync(...)` orchestrator
  - `ValidateConnectionStrings(...)` configuration validator
  - `ValidateDatabaseConnectivityAsync(...)` MySQL connectivity tester
  - `ValidateAnnotatedObject<TOptions>(...)` DataAnnotations helper
  - Logging helpers

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
	"GrabberDB": "Server=198.18.0.3;User ID=nntparticles;Password=1e916heXBfHu673mtsrK8ZAVFnvhc4qC;Database=nntp;Minimum Pool Size=1;Maximum Pool Size=5;Connection Idle Timeout=10"
  }
}
```

### Validation Flow
1. **Startup:** `Program.cs` calls `ValidateConfigurationAndDependenciesAsync(...)`
2. **Configuration Phase:** Validates syntax, server, database, authentication, pooling
3. **Dependency Phase:** Attempts MySQL connection with 5-second timeout
4. **Success:** Logs `"GrabberDB connectivity validated successfully (Server: ..., Database: ...)"`
5. **Failure:** Blocks startup, logs errors, exits with code 2

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
