# Pool Size Alias Cleanup - Rely on Case-Insensitive Lookup

## Issue

The initial implementation added redundant lowercase aliases to pool size arrays:

```csharp
// ❌ BEFORE (redundant)
private static readonly string[] MinPoolSizeAliases =
[
	"Min Pool Size",
	"MinPoolSize",
	"Minimum Pool Size",
	"MinimumPoolSize",
	"minpoolsize"        // ← Redundant
];
```

## Root Cause

`DbConnectionStringBuilder.TryGetValue` already performs **case-insensitive key lookups**, so:
- `MinimumPoolSize`
- `minimumpoolsize`
- `MINIMUMPOOLSIZE`

All resolve through the **same alias entry** (`"MinimumPoolSize"`).

## Fix

### 1. Removed Redundant Lowercase Aliases

**Before:**
```csharp
"MinimumPoolSize",
"minpoolsize"          // ← Unnecessary
```

**After:**
```csharp
"MinimumPoolSize"      // Case-insensitive lookup handles all variations
```

### 2. Updated Documentation

Added explicit note in `MySqlConnectionStringUtilities.cs`:

```csharp
// Note: DbConnectionStringBuilder performs case-insensitive key lookups, so
//       "MinimumPoolSize", "minimumpoolsize", and "MINIMUMPOOLSIZE" all resolve
//       through the same alias. Lowercase variants are not needed in alias arrays.
```

### 3. Enhanced Comments in Implementation

```csharp
// Try each possible key with case-insensitive comparison.
// DbConnectionStringBuilder.TryGetValue performs case-insensitive key lookups,
// so "MinimumPoolSize", "minimumpoolsize", and "MINIMUMPOOLSIZE" all resolve
// through the same alias entry.
foreach (string key in possibleKeys)
{
	if (builder.TryGetValue(key, out object? objValue))
	{
		// ...
	}
}
```

### 4. Updated Tests to Verify Case-Insensitivity

**Before:**
```csharp
[InlineData("Server=localhost;...;minpoolsize=5")]
// Testing lowercase alias explicitly
```

**After:**
```csharp
[InlineData("Server=localhost;...;MinimumPoolSize=5")]
[InlineData("Server=localhost;...;MINIMUMPOOLSIZE=5")] // Case-insensitive
// Testing that case-insensitivity works, not redundant aliases
```

## Final Alias Arrays

```csharp
// Minimum pool size - 4 distinct aliases (case-insensitive)
private static readonly string[] MinPoolSizeAliases =
[
	"Min Pool Size",
	"MinPoolSize",
	"Minimum Pool Size",
	"MinimumPoolSize"
];

// Maximum pool size - 4 distinct aliases (case-insensitive)
private static readonly string[] MaxPoolSizeAliases =
[
	"Max Pool Size",
	"MaxPoolSize",
	"Maximum Pool Size",
	"MaximumPoolSize"
];
```

## Benefits

1. **Cleaner code:** No redundant entries in alias arrays
2. **Explicit testing:** Tests now explicitly verify case-insensitivity instead of implicitly relying on duplicate aliases
3. **Documentation:** Clear explanation of why lowercase variants aren't needed
4. **Maintainability:** Future maintainers understand the case-insensitive behavior

## Test Results

- **All 249 tests pass** ✅
- Case-insensitivity explicitly verified with `MINIMUMPOOLSIZE` and `MAXIMUMPOOLSIZE` test cases
- No behavior change—just cleaner implementation

## Lesson Learned

When using `DbConnectionStringBuilder`:
- Trust the framework's case-insensitive behavior
- Test case-insensitivity explicitly (with `UPPERCASE` test data)
- Don't clutter alias arrays with case variations
- Document the case-insensitive contract clearly
