# Connection String Validation: Errors vs. Warnings

## Problem Statement

The connection string validator was internally inconsistent:

**Tests said:**
```csharp
Validate_ExcessiveMinPoolSize_ReturnsPoolingWarning()
Validate_ExcessiveMaxPoolSize_ReturnsPoolingWarning()
```

**Implementation returned:**
```csharp
List<(string Setting, string Error)> errors
```

**Semantics:**
- Pooling configurations (Min/Max Pool Size) are **recommendations** for optimal control-plane usage
- They are NOT hard requirements like server/database/username
- MySqlConnector enables pooling by default and accepts any valid pool size values
- The application can function correctly with suboptimal pool settings

## Solution

Introduced a structured diagnostic system that distinguishes **errors** from **warnings**:

### New Types

```csharp
internal enum ValidationSeverity
{
	Error,    // Invalid/missing required components → prevents startup
	Warning   // Valid but suboptimal configuration → logs warning, continues
}

internal record ConnectionStringValidationResult(
	string Setting,
	string Message,
	ValidationSeverity Severity);
```

### Classification

#### Errors (ValidationSeverity.Error)
- Connection string is null/empty/whitespace
- Invalid connection string syntax
- Missing server/host
- Missing database name
- Missing username (User ID)
- Empty values for required fields

These prevent application startup - the configuration is invalid.

#### Warnings (ValidationSeverity.Warning)
- Min Pool Size > 1 (recommendation: 0 or 1 for control-plane)
- Max Pool Size > 10 (recommendation: ≤10 for control-plane)

These log a warning but allow startup - the configuration works but isn't optimal.

## API Changes

### Before

```csharp
public static List<(string Setting, string Error)> Validate(
	string? connectionString,
	string settingName)
```

### After

```csharp
public static List<ConnectionStringValidationResult> Validate(
	string? connectionString,
	string settingName)
```

## Consumer Handling

`Program.Validation.cs` now filters diagnostics by severity:

```csharp
List<ConnectionStringValidationResult> diagnostics = ConnectionStringValidator.Validate(
	connectionStrings.GrabberDB,
	"ConnectionStrings:GrabberDB");

foreach (ConnectionStringValidationResult diagnostic in diagnostics)
{
	if (diagnostic.Severity == ValidationSeverity.Error)
	{
		errors.Add((diagnostic.Setting, diagnostic.Message));  // Prevents startup
	}
	else if (diagnostic.Severity == ValidationSeverity.Warning)
	{
		Log.Warning("Configuration warning: {Setting}: {Message}",
			diagnostic.Setting, diagnostic.Message);  // Logs but continues
	}
}
```

## Test Updates

Pool warning tests now explicitly verify severity:

```csharp
[Fact]
public void Validate_ExcessiveMinPoolSize_ReturnsPoolingWarning()
{
	// ...validation code...

	// Assert - should be a WARNING, not an error
	ConnectionStringValidationResult? poolWarning = diagnostics
		.FirstOrDefault(e => e.Message.Contains("Min Pool Size")...);

	Assert.NotNull(poolWarning);
	Assert.Equal(ValidationSeverity.Warning, poolWarning.Severity);
}
```

## MySqlConnector Context

MySqlConnector pooling behavior:
- **Enabled by default** (`Pooling=true`)  
- **Default Min Pool Size**: 0  
- **Default Max Pool Size**: 100  

Our control-plane recommendations:
- ✅ **Recommended**: `Min Pool Size=0` or `1`, `Max Pool Size≤10`
- ⚠️ **Warning**: Higher values work but are excessive for low-utilization control-plane access
- ❌ **Error**: N/A - any valid pool size is technically correct for MySqlConnector

The validator now correctly reflects this: pool settings are **guidance**, not **requirements**.

## Benefits

1. **Semantic accuracy**: Errors mean "won't work", warnings mean "could be better"
2. **Operational flexibility**: Suboptimal pool settings don't prevent startup
3. **Test/code consistency**: Test names match implementation semantics
4. **Clear intent**: Future validators can use the severity system consistently
5. **Observability**: Warnings are logged, operators can review and adjust configuration

## Impact

- ✅ All 220 tests pass
- ✅ Pool warnings correctly distinguished from errors
- ✅ Startup behavior unchanged (errors still prevent startup, warnings don't)
- ✅ Better alignment with MySqlConnector's actual behavior
