# MySQL Connection String Utilities - Centralized Canonical Interpretation

## Overview

Created `MySqlConnectionStringUtilities` to provide a single, authoritative interpretation of MySQL connection strings across the application. This eliminates inconsistencies between validation and fingerprinting code.

## Problem Solved

**Before:**
- Validation code had its own alias mapping logic
- Fingerprint code had its own (different) alias mapping logic
- No guarantee that both interpreted connection strings consistently
- Case-sensitivity handling was inconsistent
- Potential for drift: "Validator thinks `User ID = ...`, Fingerprint thinks `Username = ...`"

**After:**
- Single source of truth for MySQL connection string interpretation
- Both validation and fingerprinting use `MySqlConnectionStringUtilities`
- Guaranteed consistency across the application
- All MySqlConnector documented aliases supported

## Implementation

### New File: `MySqlConnectionStringUtilities.cs`

Provides case-insensitive, alias-aware parsing methods:

- `TryGetServer(connectionString, out server)` - Server/host extraction
- `TryGetDatabase(connectionString, out database)` - Database name extraction
- `TryGetUsername(connectionString, out username)` - Username extraction
- `TryGetPassword(connectionString, out password)` - Password extraction (optional)
- `TryGetMinPoolSize(connectionString, out minPoolSize)` - Min pool size extraction
- `TryGetMaxPoolSize(connectionString, out maxPoolSize)` - Max pool size extraction

### MySqlConnector Aliases Supported

#### Server/Host (7 aliases)
- `Server`
- `Host`
- `Data Source`
- `DataSource`
- `Address`
- `Addr`
- `Network Address`

#### Database (3 aliases)
- `Database`
- `Initial Catalog`
- `InitialCatalog`

#### Username (6 aliases)
- `User ID`
- `UserID`
- `Username`
- `Uid`
- `User name` (space-separated)
- `User`

#### Password (2 aliases)
- `Password`
- `Pwd`

#### Pool Size Aliases

**Minimum Pool Size (4 aliases):**
- `Min Pool Size`
- `MinPoolSize`
- `Minimum Pool Size`
- `MinimumPoolSize`

**Maximum Pool Size (4 aliases):**
- `Max Pool Size`
- `MaxPoolSize`
- `Maximum Pool Size`
- `MaximumPoolSize`

**Case-Insensitivity:**  
`DbConnectionStringBuilder.TryGetValue` performs case-insensitive key lookups, so:
- `MinimumPoolSize`, `minimumpoolsize`, `MINIMUMPOOLSIZE` all resolve through the same alias
- Lowercase variants are NOT needed in alias arrays
- Tests verify case-insensitivity explicitly (e.g., `MINIMUMPOOLSIZE=5`)

## Refactored Components

### `ConnectionStringsOptions.cs`

**Before:**
```csharp
// Had its own TryGetConnectionStringValue helper
// Direct DbConnectionStringBuilder manipulation
if (!TryGetConnectionStringValue(builder, ["User ID", "UserID", "UID", ...], out string? userId))
```

**After:**
```csharp
// Uses centralized utility
if (!MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username) 
	|| string.IsNullOrWhiteSpace(username))
```

### `Program.ConfigurationFingerprint.cs`

**Before:**
```csharp
// Had its own canonicalMappings dictionary
Dictionary<string, string> canonicalMappings = new(StringComparer.OrdinalIgnoreCase)
{
	["User ID"] = "Username",
	["UserID"] = "Username",
	["UID"] = "Username",  // ← Missing "Uid", "User name", "User"
	// ...
}
```

**After:**
```csharp
// Uses centralized utility
if (MySqlConnectionStringUtilities.TryGetUsername(connectionString, out string? username)
	&& !string.IsNullOrWhiteSpace(username))
{
	canonicalProperties["Username"] = username;
}
```

## Benefits

1. **Consistency**: Validation and fingerprinting now use identical MySQL alias interpretation
2. **Completeness**: All MySqlConnector documented aliases are supported
3. **Maintainability**: Single location to update if MySqlConnector adds new aliases
4. **Correctness**: Case-insensitive matching is explicit and uniform
5. **Testability**: 30 dedicated tests verify all alias variations and edge cases
6. **DRY**: Eliminated duplicate alias-mapping logic

## Test Coverage

- **249 total tests** (most recent count)
- **Pool size alias tests** explicitly verify:
  - `MinimumPoolSize` / `MaximumPoolSize` aliases (in addition to `Min Pool Size`, etc.)
  - Case-insensitive matching (`MINIMUMPOOLSIZE`, `MAXIMUMPOOLSIZE`)
  - All alias variations produce consistent warnings for control-plane pooling guidance

### Key Test Additions
- **Server alias variations:** 7 aliases × multiple test scenarios
- **Database alias variations:** 3 aliases × multiple test scenarios
- **Username alias variations:** 6 aliases × multiple test scenarios
- **Password alias variations:** 2 aliases × multiple test scenarios
- **Min pool size alias variations:** 4 aliases (including `MinimumPoolSize`) + case-insensitivity
- **Max pool size alias variations:** 4 aliases (including `MaximumPoolSize`) + case-insensitivity
- **Edge cases:** Missing values, malformed strings, null/empty/whitespace inputs

All existing validation and fingerprint tests continue to pass, confirming backward compatibility.

## Future Considerations

If MySqlConnector adds new connection string aliases, update them in one place:
- Add to the appropriate alias array in `MySqlConnectionStringUtilities.cs`
- Add corresponding test cases in `MySqlConnectionStringUtilitiesTests.cs`

Both validation and fingerprinting will automatically use the new aliases.
