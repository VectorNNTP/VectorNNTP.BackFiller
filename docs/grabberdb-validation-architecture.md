# GrabberDB Validation: Complete Architecture

## Overview

The `ConnectionStrings:GrabberDB` validation implements a **two-phase validation strategy** that combines static syntax checking with runtime MySQL connectivity testing.

## Validation Pipeline

```
┌──────────────────────────────────────────────────────────┐
│ Application Startup                                      │
└──────────────────────┬───────────────────────────────────┘
					   │
		 ┌─────────────▼──────────────┐
		 │  Phase 1: Static           │
		 │  Configuration Validation  │
		 ├────────────────────────────┤
		 │ • Syntax validation        │
		 │ • Required fields          │
		 │ • Pool size warnings       │
		 │                            │
		 │ Location:                  │
		 │ ConnectionStringValidator  │
		 │   .Validate()              │
		 └─────────────┬──────────────┘
					   │
				   [errors?]
				  ┌────┴────┐
				  │   YES   │──────→ ❌ Block startup (exit code 2)
				  └─────────┘
				  │   NO    │
				  └────┬────┘
					   │
		 ┌─────────────▼──────────────┐
		 │  Phase 2: Runtime MySQL    │
		 │  Connectivity Validation   │
		 ├────────────────────────────┤
		 │ • MySqlConnection.Open()   │
		 │ • Authentication check     │
		 │ • Database access test     │
		 │ • SELECT 1 execution       │
		 │                            │
		 │ Location:                  │
		 │ ValidateDatabase           │
		 │   ConnectivityAsync()      │
		 └─────────────┬──────────────┘
					   │
				  [failures?]
				  ┌────┴────┐
				  │   YES   │──────→ ❌ Block startup (dependency unavailable)
				  └─────────┘
				  │   NO    │
				  └────┬────┘
					   │
		 ┌─────────────▼──────────────┐
		 │  ✅ Application Ready       │
		 │  GrabberDB validated and   │
		 │  available for control-    │
		 │  plane operations          │
		 └────────────────────────────┘
```

## Phase 1: Static Configuration Validation

### What It Validates

| Check | Requirement | Severity | Example Error
|-------|-------------|----------|---------------
| **Presence** | Connection string exists and is non-empty | Error | `"Connection string is required and cannot be empty"`
| **Syntax** | Valid `key=value` pairs | Error | `"Invalid connection string syntax: {details}"`
| **Server** | Contains `Server`, `Host`, `Data Source`, or aliases | Error | `"Connection string must specify a server/host"`
| **Database** | Contains `Database` or `Initial Catalog` | Error | `"Connection string must specify a database name"`
| **User ID** | Contains `User ID` or username aliases | Error | `"Connection string must specify a MySQL user ID"`
| **Min Pool Size** | Recommended ≤1 for control-plane | **Warning** | `"Min Pool Size={value} is excessive..."`
| **Max Pool Size** | Recommended ≤10 for control-plane | **Warning** | `"Max Pool Size={value} is excessive..."`

### Key Features

1. **MySQL-specific alias parsing** via `MySqlConnectionStringUtilities`
   - Server: `Server`, `Host`, `Data Source`, `DataSource`, `Address`, `Addr`, `Network Address`
   - Database: `Database`, `Initial Catalog`, `InitialCatalog`
   - Username: `User ID`, `UserID`, `Username`, `Uid`, `User name`, `User`
   - All lookups are **case-insensitive** (matches MySqlConnector behavior)

2. **Warnings vs. Errors**
   - Errors → invalid configuration → **blocks startup**
   - Warnings → suboptimal configuration → **logs but continues**
   - Pool size recommendations are warnings (any valid pool size works, but large pools are inappropriate for control-plane)

3. **Shared canonical parser**
   - Both validation and fingerprinting use `MySqlConnectionStringUtilities`
   - Prevents drift between validation and configuration hashing
   - Single source of truth for MySQL connection string interpretation

### Files

- `VectorNNTP.BackFiller/Configuration/ConnectionStringsOptions.cs`
  - `ValidationSeverity` enum
  - `ConnectionStringValidationResult` record
  - `ConnectionStringValidator.Validate()` method
  - `ValidateConnectionPooling()` helper

- `VectorNNTP.BackFiller/Configuration/MySqlConnectionStringUtilities.cs`
  - Canonical MySQL alias parsing
  - Case-insensitive lookup helpers

## Phase 2: Runtime MySQL Connectivity Validation

### What It Validates

**Things static validation CANNOT detect:**

| Issue | How Runtime Validation Detects It | MySQL Error
|-------|-----------------------------------|-------------
| Invalid credentials | `MySqlException` during `OpenAsync()` | Error 1045
| Wrong password/token | `MySqlException` with auth failure | Error 1045
| Database doesn't exist | `MySqlException` after connection | Error 1049
| MySQL server unreachable | Connection timeout or network error | Error 2003
| Firewall blocking port | Connection timeout | Timeout
| Unsupported auth plugin | `MySqlException` during auth handshake | Error 2061
| Permission denied | `MySqlException` on query execution | Error 1142
| TLS/SSL failure | Connection error during handshake | Error 2026
| Wrong port | Connection refused or timeout | Error 2003
| Incompatible server | Protocol mismatch during handshake | `MySqlException`
| Host IP restriction | MySQL denies connection | Error 1130

### Implementation

```csharp
// Create MySqlConnection
await using var connection = new MySqlConnector.MySqlConnection(connectionString);

// Attempt to open connection (validates network, auth, TLS, protocol)
await connection.OpenAsync(cts.Token).ConfigureAwait(false);

// Execute test query (validates database access and permissions)
await using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT 1";
await cmd.ExecuteScalarAsync(cts.Token).ConfigureAwait(false);

// Log success with actual connection properties
Log.Information("GrabberDB connectivity validated successfully " +
	"(Server: {Server}, Database: {Database})",
	connection.DataSource,
	connection.Database);
```

### Error Handling

1. **Timeout** → `OperationCanceledException`
   - Network issues, slow server, firewall, wrong host/port

2. **MySqlException** → MySQL-specific errors
   - Includes error number for diagnosis (e.g., 1045, 1049, 2003, 2061)
   - See [MySQL Error Reference](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html)

3. **Other Exception** → General connectivity failures
   - DNS resolution, socket errors, etc.

### Timeout Configuration

- Default: **10 seconds**
- Configured at startup via `dependencyTimeout` parameter
- Prevents indefinite hangs on network/database issues

### Control-Plane Constraint

**CRITICAL:** This validation runs **ONCE** during startup.

- ✅ Validates connectivity before application becomes Ready
- ✅ Prevents startup with inaccessible database
- ❌ NOT executed in the article retrieval data path
- ❌ Temporary outages after startup MUST NOT interrupt article operations

### Files

- `VectorNNTP.BackFiller/Program.Validation.cs`
  - `ValidateDatabaseConnectivityAsync()` method
  - Comprehensive error handling for MySQL exceptions
  - Detailed logging of connection validation steps

## Test Coverage

### Static Validation Tests

**File:** `VectorNNTP.BackFiller.Tests/ConnectionStringValidationTests.cs`

Coverage:
- ✅ Null/empty/whitespace connection strings
- ✅ Malformed syntax (invalid key=value pairs)
- ✅ Missing server/database/username
- ✅ Server/database/username alias variations (case-insensitive)
- ✅ Pool size warnings (Min > 1, Max > 10)
- ✅ Valid MySQL connection strings with various configurations

**Result:** 220 tests passing

### MySQL Utility Tests

**File:** `VectorNNTP.BackFiller.Tests/MySqlConnectionStringUtilitiesTests.cs`

Coverage:
- ✅ All server aliases (`Server`, `Host`, `Data Source`, etc.)
- ✅ All database aliases (`Database`, `Initial Catalog`)
- ✅ All username aliases (`User ID`, `Username`, `Uid`, etc.)
- ✅ Password handling
- ✅ Pool size extraction (`Min Pool Size`, `Max Pool Size`)
- ✅ Case-insensitive lookup behavior
- ✅ Missing/malformed value handling

### Runtime Connectivity Tests

**Status:** Integration-level (requires MySQL instance)

Recommended test scenarios:
1. ✅ Valid connection → Success
2. ❌ Wrong password → MySqlException 1045
3. ❌ Database doesn't exist → MySqlException 1049
4. ❌ Server unreachable → Timeout
5. ❌ Wrong port → Connection refused
6. ⏱️ Slow server → Timeout after configured duration
7. 🔒 TLS required but not configured → SSL error

**Note:** Current tests focus on static validation. Runtime connectivity requires live MySQL infrastructure for comprehensive testing.

## Documentation

### Complete Documentation Set

1. **[MySQL Runtime Validation](./mysql-runtime-validation.md)**
   - What runtime validation catches
   - MySQL error codes reference
   - Implementation details
   - Security considerations

2. **[Validation Errors vs. Warnings](./validation-errors-vs-warnings.md)**
   - Why pool settings are warnings, not errors
   - Severity classification rules
   - API design rationale

3. **[MySQL Connection String Utilities](./mysql-connection-string-utilities.md)**
   - Canonical MySQL parsing layer
   - Alias coverage
   - Case-insensitive lookup semantics

4. **[Configuration Validation: GrabberDB](./configuration-validation-grabberdb.md)**
   - Complete validation rules
   - Test coverage summary
   - Error handling philosophy

5. **[Fix: MySQL Authentication Model](./fix-mysql-authentication-model.md)**
   - Simplified auth model (User ID + optional password)
   - Removed Integrated Security handling

6. **[Fix: Provider Inference Removal](./fix-provider-inference-removal.md)**
   - Why heuristic provider detection was removed
   - MySQL-only runtime validation approach

## Design Principles

### 1. Fail Fast
Detect configuration/connectivity issues **at startup**, not during operation.

### 2. Two-Phase Validation
- **Static:** Syntax and required fields (cheap, no I/O)
- **Runtime:** Actual MySQL connectivity (expensive, validates real-world issues)

### 3. Clear Diagnostics
- Include error codes (MySqlException.Number)
- Log connection details (Server, Database)
- Distinguish timeouts from auth failures from network issues

### 4. Warnings for Recommendations
- Pool size is a recommendation for this workload, not a hard requirement
- Any valid pool size works with MySQL, but large pools are wasteful for control-plane

### 5. Control-Plane Constraint
- Validation happens **once** at startup
- Database is NOT in the article retrieval data path
- Temporary outages after startup should be tolerated (cached config, reconnection)

### 6. Canonical MySQL Interpretation
- Single source of truth: `MySqlConnectionStringUtilities`
- Used by validation, fingerprinting, and any other MySQL parsing
- Prevents inconsistencies and drift

### 7. Security-Aware
- Passwords can be provided via `ProvidePasswordCallback` (not just in connection string)
- Logs never include passwords/tokens
- Configuration fingerprinting redacts credentials

## Benefits

1. **Early Failure Detection**
   - Invalid config or unreachable database blocks startup immediately
   - Operators know about issues before production traffic arrives

2. **Clear Root Cause**
   - Static validation: "You forgot to specify the database name"
   - Runtime validation: "MySQL Error 1045: Access denied for user 'app'@'10.0.0.5'"

3. **Operational Flexibility**
   - Warnings for suboptimal settings don't prevent startup
   - Operators can fix recommendations at their convenience

4. **MySQL-Specific Validation**
   - Leverages MySqlConnector error codes and messages
   - No guessing about provider types from connection strings

5. **Consistent Parsing**
   - Validation and fingerprinting use the same MySQL interpretation
   - Cannot diverge or produce conflicting results

6. **Test Coverage**
   - Comprehensive static validation tests (220 passing)
   - Clear separation between unit-testable and integration-level validation

## Future Enhancements

### Potential Additions

1. **Connection Pooling Runtime Validation**
   - Verify pool behavior during startup (e.g., can we create N connections?)
   - Detect pool exhaustion scenarios early

2. **Schema Validation**
   - Check for required tables/procedures at startup
   - Verify schema version compatibility

3. **Permission Validation**
   - Test specific required permissions (SELECT, UPDATE on specific tables)
   - Fail fast if schema changes broke permission model

4. **Periodic Health Checks**
   - Re-validate connectivity after startup (not in data path)
   - Alert if control-plane database becomes unreachable

5. **Integration Test Suite**
   - Docker-based MySQL instance for CI/CD
   - Automated testing of all MySQL error scenarios

6. **Configuration Warnings Dashboard**
   - Collect and display all config warnings at startup
   - Help operators prioritize optimization work

## Related Systems

### Configuration Fingerprinting

`Program.ConfigurationFingerprint.cs` uses the same `MySqlConnectionStringUtilities` to:
- Canonicalize MySQL properties for consistent hashing
- Redact credentials before fingerprint calculation
- Generate stable configuration identifiers for deployment comparison

**Benefit:** Validation and fingerprinting cannot interpret MySQL differently.

### Service Lifecycle

`ServiceLifecycle.cs` tracks application readiness:
- `Initializing` → Configuration validation runs
- `Ready` → All validation passed (config + dependencies)
- `Faulted` → Validation failed (startup blocked)

Validation failures prevent transition to `Ready` state.

## Summary

The GrabberDB validation architecture provides:

✅ **Comprehensive validation** (syntax + runtime connectivity)  
✅ **Clear diagnostics** (errors vs. warnings, MySQL error codes)  
✅ **MySQL-specific testing** (OpenAsync + SELECT 1)  
✅ **Fail-fast startup behavior** (block startup on invalid config/unreachable DB)  
✅ **Consistent parsing** (shared utilities prevent drift)  
✅ **Security-aware** (supports programmatic password/token delivery)  
✅ **Control-plane optimized** (validates once at startup, not per-request)  
✅ **Well-tested** (220 passing tests for static validation)  
✅ **Well-documented** (comprehensive docs for operators and developers)  

This design ensures the application never starts with invalid or unreachable database configuration, while providing clear guidance for operators when configuration is suboptimal.
