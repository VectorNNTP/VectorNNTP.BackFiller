# MySQL Runtime Connectivity Validation

## Overview

Beyond static connection string validation, startup performs two distinct runtime stages:

1. **Server-level dependency probe** (before host composition) using frozen GrabberDB runtime projection.
2. **Authoritative database/table provisioning** (during hosted startup) before initial account snapshot load.

This separation ensures server reachability/auth/TLS issues fail dependency validation while missing database/table scenarios are handled at the provisioning boundary.

## Validation Pipeline

```
Application Startup
	↓
┌─────────────────────────────────────────┐
│ Phase 1: Syntactic Validation           │
│ (ConnectionStringValidator.Validate)    │
├─────────────────────────────────────────┤
│ • Connection string syntax              │
│ • Required fields present               │
│   - Server/Host                         │
│   - Database                            │
│   - User ID                             │
│ • Pool size recommendations (warnings)  │
└─────────────────────────────────────────┘
	↓ (if valid)
┌─────────────────────────────────────────┐
│ Phase 2: Server-Level Dependency Probe  │
│ (DatabaseDependencyProbe)               │
├─────────────────────────────────────────┤
│ • Build server-level MySqlConnection    │
│   from frozen runtime projection        │
│ • OpenAsync() with timeout              │
│ • Validate network/auth/TLS/protocol    │
│ • No target database selection/query    │
└─────────────────────────────────────────┘
	↓ (if valid)
┌─────────────────────────────────────────┐
│ Phase 3: Hosted Startup Provisioning    │
│ (NntpAccountSnapshotStartupInitializer) │
├─────────────────────────────────────────┤
│ • Ensure database exists                │
│ • Ensure required table exists          │
│ • Load initial account snapshot         │
└─────────────────────────────────────────┘
```

## What Runtime Validation Catches

### ✅ Issues Detected by Startup Server-Level Probe

| Issue | Detection Method | Error Type
|-------|------------------|------------
| **Invalid credentials** | `MySqlException` during `OpenAsync()` | `MySqlException` with error number
| **MySQL server unreachable** | Connection timeout or network error | `OperationCanceledException` or `MySqlException`
| **Unsupported authentication plugin** | `MySqlException` during auth handshake | `MySqlException` (plugin error)
| **TLS/SSL failure** | Connection error during handshake | `MySqlException` or connection exception
| **Incompatible MySQL server** | Protocol mismatch during handshake | `MySqlException`
| **Wrong port** | Connection timeout or refused | Timeout or connection exception
| **Expired/invalid auth token** | `MySqlException` during authentication | `MySqlException` (auth failed)
| **Network connectivity issues** | Connection timeout | `OperationCanceledException`
| **Firewall blocking connection** | Connection timeout or refused | Connection exception

### ✅ Issues Detected by Hosted Provisioning Boundary

| Issue | Detection Stage | Classification
|-------|-----------------|---------------
| **Database missing** | `create-database` | Provisioning stage failure (with MySQL error number)
| **Database inaccessible** | `select-database` | Provisioning stage failure (with MySQL error number)
| **Table missing or creation denied** | `create-table` | Provisioning stage failure (with MySQL error number)

### ❌ Issues NOT Detected by Static Validation Alone

Static validation **cannot** detect:
- Whether credentials are correct
- Whether the server is online
- Network reachability
- Authentication plugin compatibility
- TLS configuration issues
- Whether the target database/table can be created or accessed at runtime

Static validation only verifies **syntax** and **required field presence**.

## Implementation Details

### Location
- `VectorNNTP.BackFiller/Startup/Validation/DatabaseDependencyProbe.cs`
- `VectorNNTP.BackFiller/Runtime/Accounts/NntpAccountSnapshotStartupInitializer.cs`
- `VectorNNTP.BackFiller/Runtime/Accounts/MySqlNntpAccountSnapshotProvider.cs`

### Code Flow

1. Startup validation builds a frozen `BackFillerRuntimeOptions` snapshot including `GrabberDb` projection.
2. `DatabaseDependencyProbe.ValidateDatabaseConnectivityAsync(...)` creates a server-level MySQL connection (database cleared) and runs `OpenAsync(...)` with timeout.
3. Dependency probe reports sanitized connectivity/auth/TLS/protocol diagnostics.
4. Hosted initializer invokes provider provisioning boundary:
   - `CREATE DATABASE IF NOT EXISTS ...`
   - database selection/open
   - `CREATE TABLE IF NOT EXISTS nntpbackfilleraccounts ...`
5. Initial account snapshot load executes after provisioning succeeds.

### Error Handling

- **Dependency probe phase:** classifies server-level MySQL connectivity failures with sanitized startup dependency diagnostics.
- **Provisioning phase:** wraps stage-specific MySQL failures deterministically as:
  - `server-connect`
  - `create-database`
  - `select-database`
  - `create-table`

Each provisioning failure preserves MySQL error number and stage classification without logging secrets.

## Common MySQL Error Numbers (Stage-Dependent)

| Error # | Typical Meaning | Where It Can Surface
|---------|-----------------|----------------------
| 1045 | Access denied | server-level probe or `server-connect`
| 1049 | Unknown database | `select-database`
| 1130 | Host not allowed | server-level probe or `server-connect`
| 1142 | Command denied (table/DDL permission) | `create-table`
| 2002 | Can't connect (Unix socket) | server-level probe or `server-connect`
| 2003 | Can't connect (TCP) | server-level probe or `server-connect`
| 2013 | Lost connection | probe/provisioning stages
| 2026 | SSL connection error | server-level probe or `server-connect`
| 2061 | Authentication plugin error | server-level probe or `server-connect`

## Configuration

### Timeout

Default: **10 seconds** (specified at startup)

```csharp
var (configResult, dependencyResult) = await ValidateConfigurationAndDependenciesAsync(
	configuration,
	dependencyTimeout: TimeSpan.FromSeconds(10),
	cancellationToken);
```

### When It Runs

- **Dependency probe phase:** once during startup validation, after static validation and runtime snapshot creation.
- **Provisioning phase:** during hosted startup initializer, before initial account snapshot publication.
- **Before** the application transitions to `Ready` state.
- **Not** in the article retrieval data path (control-plane only)

### Startup Behavior

| Validation Result | Behavior
|-------------------|----------
| Syntactic validation fails | ❌ Startup blocked immediately (configuration error)
| Runtime connectivity fails | ❌ Startup blocked (dependency unavailable)
| Both pass | ✅ Application transitions to `Ready` state

## Design Principles

### 1. **Fail Fast**
Detect configuration/connectivity issues at startup, not during operation.

### 2. **Clear Diagnostics**
Include MySQL error numbers and connection details in failure messages.

### 3. **Timeout Protection**
Network/database issues cannot hang startup indefinitely.

### 4. **Cancellation Support**
Application shutdown signals (`CancellationToken`) are propagated correctly.

### 5. **Control-Plane Only**
Database validation happens **once at startup**, not per-request.

### 6. **Real Connectivity**
Use actual `MySqlConnection.OpenAsync()` + query execution, not heuristics.

## Security Considerations

### Password Handling

Passwords can be provided:
1. **In connection string** (less secure, visible in config)
2. **Via `ProvidePasswordCallback`** (preferred for tokens/secrets)

Example with callback:
```csharp
var builder = new MySqlConnectionStringBuilder(connectionString);
var connection = new MySqlConnection(builder.ConnectionString);

// Provide password/token programmatically
connection.ProvidePasswordCallback = (MySqlProvidePasswordContext context) =>
{
	return ValueTask.FromResult(GetPasswordFromSecretStore());
};

await connection.OpenAsync(ct);
```

### Logging

- ✅ **Logged**: Server name, database name, timeout, success/failure
- ❌ **NOT logged**: Passwords, tokens, full connection strings

## Testing Strategy

### Unit Testing Limitations

Runtime MySQL validation is **integration-level** and requires:
- A real MySQL server instance
- Valid credentials
- Network connectivity

Unit tests cannot fully validate this behavior without infrastructure.

### Integration Testing

Recommended test scenarios:
1. ✅ **Valid connection** → Success
2. ❌ **Wrong password** → `MySqlException` 1045
3. ❌ **Database doesn't exist** → `MySqlException` 1049
4. ❌ **Server unreachable** → Timeout or connection error
5. ❌ **Wrong port** → Connection refused or timeout
6. ⏱️ **Slow server** → Timeout after configured duration
7. 🔒 **TLS required but not configured** → SSL error

### Current Test Coverage

- ✅ Static validation: comprehensive (220 tests passing)
- ⚠️ Runtime validation: integration-dependent (requires MySQL instance)

## Related Documentation

- [Connection String Validation: Errors vs. Warnings](./validation-errors-vs-warnings.md)
- [MySQL Connection String Utilities](./mysql-connection-string-utilities.md)
- [Configuration Validation: GrabberDB](./configuration-validation-grabberdb.md)

## References

- [MySqlConnector Error Codes](https://mysqlconnector.net/troubleshooting/connection-issues/)
- [MySQL Server Error Reference](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html)
- MySqlException.Number property for error classification
