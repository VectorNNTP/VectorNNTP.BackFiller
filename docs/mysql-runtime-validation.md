# MySQL Runtime Connectivity Validation

## Overview

Beyond static connection string validation, the application performs **runtime MySQL connectivity validation** during startup using `MySqlConnector` to verify:

1. The MySQL server is reachable
2. Credentials are valid
3. Authentication succeeds
4. The specified database is accessible
5. Basic query execution works

This catches failures that static validation cannot detect.

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
│ Phase 2: Runtime MySQL Validation       │
│ (ValidateDatabaseConnectivityAsync)     │
├─────────────────────────────────────────┤
│ • Create MySqlConnection                │
│ • OpenAsync() with timeout              │
│ • Execute test query: SELECT 1          │
│ • Verify Server & Database properties   │
└─────────────────────────────────────────┘
	↓ (if valid)
┌─────────────────────────────────────────┐
│ Application Ready                        │
│ (GrabberDB available for control-plane) │
└─────────────────────────────────────────┘
```

## What Runtime Validation Catches

### ✅ Issues Detected by Runtime Validation

| Issue | Detection Method | Error Type
|-------|------------------|------------
| **Invalid credentials** | `MySqlException` during `OpenAsync()` | `MySqlException` with error number
| **MySQL server unreachable** | Connection timeout or network error | `OperationCanceledException` or `MySqlException`
| **Unsupported authentication plugin** | `MySqlException` during auth handshake | `MySqlException` (plugin error)
| **Database doesn't exist** | `MySqlException` after connection | `MySqlException` (unknown database)
| **Permission denied** | `MySqlException` on query execution | `MySqlException` (access denied)
| **TLS/SSL failure** | Connection error during handshake | `MySqlException` or connection exception
| **Incompatible MySQL server** | Protocol mismatch during handshake | `MySqlException`
| **Wrong port** | Connection timeout or refused | Timeout or connection exception
| **Expired/invalid auth token** | `MySqlException` during authentication | `MySqlException` (auth failed)
| **Network connectivity issues** | Connection timeout | `OperationCanceledException`
| **Firewall blocking connection** | Connection timeout or refused | Connection exception

### ❌ Issues NOT Detected by Static Validation

Static validation **cannot** detect:
- Whether credentials are correct
- Whether the server is online
- Whether the database exists
- Whether permissions are sufficient
- Network reachability
- Authentication plugin compatibility
- TLS configuration issues

Static validation only verifies **syntax** and **required field presence**.

## Implementation Details

### Location
`VectorNNTP.BackFiller/Program.Validation.cs` → `ValidateDatabaseConnectivityAsync()`

### Code Flow

```csharp
private static async Task<DependencyValidationResult> ValidateDatabaseConnectivityAsync(
	IConfiguration configuration,
	TimeSpan timeout,
	CancellationToken cancellationToken)
{
	string? connectionString = configuration.GetConnectionString("GrabberDB");

	// Create MySqlConnection (MySqlConnector provider)
	await using var connection = new MySqlConnector.MySqlConnection(connectionString);

	// Attempt to open connection with timeout
	// This validates:
	// - Network reachability
	// - Authentication (username/password or token)
	// - TLS negotiation
	// - Protocol compatibility
	await connection.OpenAsync(cts.Token).ConfigureAwait(false);

	// Execute test query to verify database accessibility and permissions
	// This validates:
	// - Database exists
	// - User has basic SELECT permissions
	await using var cmd = connection.CreateCommand();
	cmd.CommandText = "SELECT 1";
	await cmd.ExecuteScalarAsync(cts.Token).ConfigureAwait(false);

	// Log success with actual Server and Database properties
	Log.Information("GrabberDB connectivity validated successfully " +
		"(Server: {Server}, Database: {Database})",
		connection.DataSource,
		connection.Database);
}
```

### Error Handling

```csharp
catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
{
	throw; // Propagate application shutdown signal
}
catch (OperationCanceledException)
{
	// Connection timeout (network unreachable, slow server, wrong host, etc.)
	failures.Add(("GrabberDB", $"Connection timeout after {timeout.TotalSeconds:F1}s"));
}
catch (MySqlConnector.MySqlException ex)
{
	// MySQL-specific errors with error numbers:
	// - 1045: Access denied (bad credentials)
	// - 1049: Unknown database
	// - 2002: Can't connect to MySQL server (network)
	// - 2003: Can't connect (port/firewall)
	// - 2013: Lost connection during query
	// - 2026: TLS/SSL error
	// - 2027: Malformed packet
	// - 2061: Authentication plugin error
	failures.Add(("GrabberDB", $"MySQL connection failed: {ex.Message} (Error #{ex.Number})"));
}
catch (Exception ex)
{
	// Other errors (DNS resolution, socket errors, etc.)
	failures.Add(("GrabberDB", $"Failed to connect: {ex.Message}"));
}
```

## Common MySQL Error Numbers

| Error # | Meaning | Cause
|---------|---------|-------
| 1045 | Access denied | Wrong username/password
| 1049 | Unknown database | Database doesn't exist or typo
| 1130 | Host not allowed | MySQL server denies connection from this IP
| 2002 | Can't connect (Unix socket) | Server down or wrong socket path
| 2003 | Can't connect (TCP) | Server down, wrong host/port, or firewall
| 2013 | Lost connection | Network interruption or server kill
| 2026 | SSL connection error | TLS handshake failure
| 2061 | Authentication plugin error | Unsupported auth method (e.g., `caching_sha2_password`)

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

- **Once** during application startup
- **Before** the application transitions to `Ready` state
- **After** static connection string validation passes
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
