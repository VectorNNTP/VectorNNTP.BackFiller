# Ambiguous Alias Detection - Critical Security Fix

## Issue

`Server=db01;Host=db02` was silently choosing one value instead of rejecting the ambiguous configuration.

This is **CRITICAL** for:
1. **Security**: Which server is actually being used?
2. **Configuration fingerprinting**: Hash must be stable and deterministic
3. **Operational safety**: Ambiguous configs hide errors

## Root Cause

`MySqlConnectionStringUtilities.TryGetConnectionStringValue` simply returned the **first match**:

```csharp
// ❌ BEFORE (dangerous)
foreach (string key in possibleKeys)
{
	if (builder.TryGetValue(key, out object? objValue))
	{
		value = objValue?.ToString();
		return true;  // ← Returns FIRST match, ignores conflicts
	}
}
```

For `Server=db01;Host=db02`, it would return `"db01"` (or`"db02"` depending on iteration order), **silently ignoring the conflict**.

## Solution

### 1. **Ambiguity Detection in Utilities**

Updated `TryGetConnectionStringValue` to detect conflicts:

```csharp
// ✅ AFTER (safe)
string? foundValue = null;
bool foundAny = false;

foreach (string key in possibleKeys)
{
	if (builder.TryGetValue(key, out object? objValue))
	{
		string? currentValue = objValue?.ToString();

		if (!foundAny)
		{
			foundValue = currentValue;
			foundAny = true;
		}
		else
		{
			// Check for conflict
			if (!string.Equals(foundValue, currentValue, StringComparison.Ordinal))
			{
				// AMBIGUOUS: different values for same property
				value = null;
				return false;  // ← Reject conflicting aliases
			}
			// Same value = redundant but acceptable
		}
	}
}
```

### 2. **Explicit Validation Rules**

Added **Rule 2a** in `ConnectionStringValidator.Validate`:

```csharp
// Check if alias exists BUT Try Get fails => CONFLICT
bool serverAliasExists = builder.ContainsKey("Server") || builder.ContainsKey("Host") || ...;

if (serverAliasExists && !MySqlConnectionStringUtilities.TryGetServer(connectionString, out _))
{
	diagnostics.Add(new ConnectionStringValidationResult(
		settingName,
		"Connection string contains conflicting server/host aliases with different values...",
		ValidationSeverity.Error));
	return diagnostics;  // ← Cannot continue with ambiguous config
}
```

Checks performed for:
- Server/Host
- Database
- Username
- Password
- Min Pool Size
- Max Pool Size

### 3. **Behavior**

| Case | Example | Result |
|------|---------|--------|
| **Conflicting** | `Server=db01;Host=db02` | ❌ **ERROR** (ambiguous) |
| **Redundant but identical** | `Server=db01;Host=db01` | ✅ **ACCEPTED** (consistent) |
| **Single alias** | `Server=db01` | ✅ **ACCEPTED** (normal) |

## Test Coverage

### MySqlConnectionStringUtilitiesTests (12 new tests)
- `TryGetServer_WhenConflictingServerAliases_ReturnsFalse`
- `TryGetServer_WhenRedundantButIdenticalServerAliases_ReturnsTrue`
- `TryGetDatabase_WhenConflictingDatabaseAliases_ReturnsFalse`
- `TryGetDatabase_WhenRedundantButIdenticalDatabaseAliases_ReturnsTrue`
- `TryGetUsername_WhenConflictingUsernameAliases_ReturnsFalse`
- `TryGetUsername_WhenRedundantButIdenticalUsernameAliases_ReturnsTrue`
- `TryGetPassword_WhenConflictingPasswordAliases_ReturnsFalse`
- `TryGetPassword_WhenRedundantButIdenticalPasswordAliases_ReturnsTrue`
- `TryGetMinPoolSize_WhenConflictingMinPoolSizeAliases_ReturnsFalse`
- `TryGetMaxPoolSize_WhenConflictingMaxPoolSizeAliases_ReturnsFalse`
- `TryGet_WhenMultipleConflictingAliases_ReturnsFalse` (2 variations)

### ConnectionStringValidationTests (9 new tests)
- `Validate_AmbiguousConflictingAliases_ReturnsError` (6 variations)
  - Server conflict
  - Database conflict
  - Username conflict
  - Password conflict
  - Min pool size conflict
  - Max pool size conflict
- `Validate_RedundantButIdenticalAliases_AcceptsConfiguration` (3 variations)
  - Server redundant
  - Database redundant
  - Username redundant

**Total: 270/270 tests passing** ✅

## Security Impact

### Before
```csharp
// Production config
"Server=prod-db01;Port=3306;Database=GrabberDB;User ID=app"

// Attacker modifies config (typo-squatting or injection)
"Server=prod-db01;Host=attacker-db.evil.com;Port=3306;Database=GrabberDB;User ID=app"

// Result: Silently connects to attacker-db.evil.com (or prod-db01, unpredictable)
```

### After
```csharp
// Same malicious config
"Server=prod-db01;Host=attacker-db.evil.com;..."

// Result: Application FAILS TO START with clear error:
// "Connection string contains conflicting server/host aliases with different values..."
```

## Configuration Fingerprint Impact

### Before
```csharp
Config A: "Server=db01;Database=GrabberDB;User ID=admin"
Config B: "Host=db01;Database=GrabberDB;User ID=admin"

// Same effective config, DIFFERENT fingerprints (Server vs Host)
```

### After
Both produce **identical canonical fingerprints** because `MySqlConnectionStringUtilities` enforces canonical interpretation, AND ambiguous configs are rejected early.

## Why Redundant But Identical Is Allowed

```csharp
// This is accepted:
"Server=db01;Host=db01;Database=GrabberDB;User ID=admin"
```

Rationale:
1. **No ambiguity**: All aliases point to the same value
2. **Defensive programming**: Some tools/libraries might redundantly specify both
3. **Migration safety**: During config refactoring, redundant entries are safer than silent conflicts

## Critical Takeaway

**Ambiguous configuration is a security vulnerability.**

`Server=db01;Host=db02` must NEVER silently pick one—it must fail loudly and explicitly.

This change protects:
- Configuration integrity
- Security (prevents config injection attacks)
- Configuration fingerprint stability
- Operational predictability
