param(
	[string]$RepoRoot = '.',
	[string]$ProjectPath = 'VectorNNTP.BackFiller.Tests/VectorNNTP.BackFiller.Tests.csproj',
	[string]$WatchdogPath = 'tools/testing/TransitPublisherSuiteWatchdog.v2.ps1',
	[string[]]$RequestedClasses = @(
		'VectorNNTP.Backfiller.Tests.TransitConnectionTests',
		'VectorNNTP.Backfiller.Tests.TransitPublisherTests',
		'VectorNNTP.Backfiller.Tests.TransitConnectionDisposalDiagnosticsTests'
	),
	[string[]]$RequestedTests = @(),
	[int]$InactivitySeconds = 45,
	[switch]$AllowSkipped
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$resolvedRepoRoot = (Resolve-Path -Path $RepoRoot).Path
Set-Location -Path $resolvedRepoRoot

$resolvedWatchdogPath = Join-Path $resolvedRepoRoot $WatchdogPath
if (-not (Test-Path -Path $resolvedWatchdogPath)) {
	throw "Watchdog script not found: $resolvedWatchdogPath"
}

$artifactsDir = Join-Path $resolvedRepoRoot 'artifacts'
New-Item -ItemType Directory -Force -Path $artifactsDir | Out-Null

$summaryJsonPath = Join-Path $artifactsDir 'isolated-regression-gate-summary.json'
$summaryMarkdownPath = Join-Path $artifactsDir 'isolated-regression-gate-summary.md'
$gitStatusBeforePath = Join-Path $artifactsDir 'isolated-regression-gate-git-status-before.txt'
$gitStatusAfterPath = Join-Path $artifactsDir 'isolated-regression-gate-git-status-after.txt'
$listTestsRawPath = Join-Path $artifactsDir 'isolated-regression-gate-list-tests-raw.txt'

$gitBefore = (& git -C $resolvedRepoRoot status --short) -join [Environment]::NewLine
Set-Content -Path $gitStatusBeforePath -Value $gitBefore

function New-DiscoveryCase {
	param(
		[string]$FullyQualifiedName
	)

	$trimmed = $FullyQualifiedName.Trim()
	$match = [regex]::Match($trimmed, '^(?<class>.+)\.(?<method>[^.\(]+)(?<suffix>\(.*\))?$')
	if (-not $match.Success) {
		return [pscustomobject]@{
			FullyQualifiedName = $trimmed
			ClassName = $null
			MethodName = $null
			IsTheoryCase = $false
		}
	}

	$suffix = $match.Groups['suffix'].Value
	return [pscustomobject]@{
		FullyQualifiedName = $trimmed
		ClassName = $match.Groups['class'].Value
		MethodName = $match.Groups['method'].Value
		IsTheoryCase = -not [string]::IsNullOrWhiteSpace($suffix)
	}
}

function Parse-ListTestsInventory {
	param([string[]]$Lines)

	$inventory = [System.Collections.Generic.List[object]]::new()
	foreach ($line in $Lines) {
		$trimmed = $line.Trim()
		if ([string]::IsNullOrWhiteSpace($trimmed)) {
			continue
		}

		if ($trimmed -match '^VectorNNTP\.Backfiller\.Tests\.') {
			$inventory.Add((New-DiscoveryCase -FullyQualifiedName $trimmed))
		}
	}

	return $inventory
}

function Resolve-ArtifactPath {
	param(
		[string]$PathValue,
		[string]$RepoRootPath
	)

	if ([string]::IsNullOrWhiteSpace($PathValue)) {
		return $null
	}

	if ([System.IO.Path]::IsPathRooted($PathValue)) {
		return $PathValue
	}

	$relativePath = $PathValue -replace '^\.\\', ''
	return Join-Path $RepoRootPath $relativePath
}

function Parse-StdoutEvidence {
	param([string]$StdoutPath)

	$evidence = [ordered]@{
		TestRunSuccessful = $false
		NoMatch = $false
		TotalTests = $null
		PassedTests = $null
		FailedTests = $null
		SkippedTests = $null
	}

	if (-not (Test-Path -Path $StdoutPath)) {
		return [pscustomobject]$evidence
	}

	$lines = Get-Content -Path $StdoutPath -ErrorAction SilentlyContinue
	foreach ($line in $lines) {
		if ($line -match '^Test Run Successful\.$') {
			$evidence['TestRunSuccessful'] = $true
		}

		if ($line -match 'No test matches the given testcase filter') {
			$evidence['NoMatch'] = $true
		}

		if ($line -match '^Total tests:\s*(\d+)') {
			$evidence['TotalTests'] = [int]$Matches[1]
		}

		if ($line -match '^\s+Passed:\s*(\d+)') {
			$evidence['PassedTests'] = [int]$Matches[1]
		}

		if ($line -match '^\s+Failed:\s*(\d+)') {
			$evidence['FailedTests'] = [int]$Matches[1]
		}

		if ($line -match '^\s+Skipped:\s*(\d+)') {
			$evidence['SkippedTests'] = [int]$Matches[1]
		}
	}

	return [pscustomobject]$evidence
}

function Classify-IsolatedResult {
	param(
		$FinalRun,
		$StdoutEvidence,
		[bool]$AllowSkippedCases
	)

	$exitCode = [int]$FinalRun.exitCode
	$hangDetected = [bool]$FinalRun.hangDetected
	$watchdogIntervention = $hangDetected

	if ($hangDetected) {
		return [pscustomobject]@{
			Classification = 'HANG'
			Reason = 'watchdog hangDetected=true'
			Executed = $true
			Passed = $false
			Failed = $false
			Skipped = $false
			Hang = $true
			WatchdogIntervention = $watchdogIntervention
		}
	}

	if ($exitCode -ne 0) {
		return [pscustomobject]@{
			Classification = 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR'
			Reason = "watchdog exitCode=$exitCode"
			Executed = $false
			Passed = $false
			Failed = $false
			Skipped = $false
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	if ($StdoutEvidence.NoMatch) {
		return [pscustomobject]@{
			Classification = 'DISCOVERY/SCOPE MISMATCH'
			Reason = 'watchdog reported no test matches filter'
			Executed = $false
			Passed = $false
			Failed = $false
			Skipped = $false
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	if ($null -eq $StdoutEvidence.TotalTests -or $StdoutEvidence.TotalTests -le 0) {
		return [pscustomobject]@{
			Classification = 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR'
			Reason = 'totalTests missing or zero'
			Executed = $false
			Passed = $false
			Failed = $false
			Skipped = $false
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	if ($null -eq $StdoutEvidence.PassedTests) {
		return [pscustomobject]@{
			Classification = 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR'
			Reason = 'passedTests missing'
			Executed = $true
			Passed = $false
			Failed = $false
			Skipped = $false
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	$failedValue = $StdoutEvidence.FailedTests
	$skippedValue = $StdoutEvidence.SkippedTests
	$effectiveFailed = if ($null -eq $failedValue) { 0 } else { [int]$failedValue }
	$effectiveSkipped = if ($null -eq $skippedValue) { 0 } else { [int]$skippedValue }

	if ($effectiveFailed -gt 0) {
		return [pscustomobject]@{
			Classification = 'TEST FAILURE'
			Reason = "failedTests=$effectiveFailed"
			Executed = $true
			Passed = $false
			Failed = $true
			Skipped = $false
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	if ($effectiveSkipped -gt 0 -and -not $AllowSkippedCases) {
		return [pscustomobject]@{
			Classification = 'SKIPPED'
			Reason = "skippedTests=$effectiveSkipped (not allowed)"
			Executed = $true
			Passed = $false
			Failed = $false
			Skipped = $true
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	if (-not $StdoutEvidence.TestRunSuccessful) {
		return [pscustomobject]@{
			Classification = 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR'
			Reason = 'missing explicit Test Run Successful signal'
			Executed = $true
			Passed = $false
			Failed = $false
			Skipped = $false
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	if ([int]$StdoutEvidence.PassedTests -ne [int]$StdoutEvidence.TotalTests) {
		return [pscustomobject]@{
			Classification = 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR'
			Reason = "passedTests=$($StdoutEvidence.PassedTests) does not equal totalTests=$($StdoutEvidence.TotalTests)"
			Executed = $true
			Passed = $false
			Failed = $false
			Skipped = $false
			Hang = $false
			WatchdogIntervention = $watchdogIntervention
		}
	}

	return [pscustomobject]@{
		Classification = 'PASS'
		Reason = 'all success conditions satisfied'
		Executed = $true
		Passed = $true
		Failed = $false
		Skipped = $false
		Hang = $false
		WatchdogIntervention = $watchdogIntervention
	}
}

$listTestArgs = @('test', $ProjectPath, '--list-tests', '--nologo', '--verbosity', 'minimal')
$listOutput = & dotnet @listTestArgs 2>&1
$listExit = $LASTEXITCODE
Set-Content -Path $listTestsRawPath -Value ($listOutput -join [Environment]::NewLine)

if ($listExit -ne 0) {
	throw "dotnet test --list-tests failed with exit code $listExit"
}

$inventory = Parse-ListTestsInventory -Lines $listOutput
if ($inventory.Count -eq 0) {
	throw 'No test inventory entries were discovered from dotnet test --list-tests output.'
}

$discoveryMismatches = [System.Collections.Generic.List[string]]::new()
$inventoryByClass = @{}
foreach ($entry in $inventory) {
	if ([string]::IsNullOrWhiteSpace($entry.ClassName)) {
		continue
	}

	if (-not $inventoryByClass.ContainsKey($entry.ClassName)) {
		$inventoryByClass[$entry.ClassName] = [System.Collections.Generic.List[object]]::new()
	}

	$inventoryByClass[$entry.ClassName].Add($entry)
}

$selectedCases = [System.Collections.Generic.List[object]]::new()

if ($RequestedClasses.Count -gt 0) {
	foreach ($requestedClass in $RequestedClasses) {
		if (-not $inventoryByClass.ContainsKey($requestedClass)) {
			$discoveryMismatches.Add("Requested class not found in authoritative inventory: $requestedClass")
			continue
		}

		foreach ($case in $inventoryByClass[$requestedClass]) {
			$selectedCases.Add($case)
		}
	}
}

if ($RequestedTests.Count -gt 0) {
	foreach ($requestedTest in $RequestedTests) {
		$match = $inventory | Where-Object { $_.FullyQualifiedName -eq $requestedTest } | Select-Object -First 1
		if ($null -eq $match) {
			$discoveryMismatches.Add("Requested test case not found in authoritative inventory: $requestedTest")
			continue
		}

		if (-not ($selectedCases | Where-Object { $_.FullyQualifiedName -eq $requestedTest })) {
			$selectedCases.Add($match)
		}
	}
}

if ($selectedCases.Count -eq 0) {
	$discoveryMismatches.Add('No executable cases selected for gate execution.')
}

$perClassDiscovery = [System.Collections.Generic.List[object]]::new()
$selectedClassNames = if ($RequestedClasses.Count -gt 0) { $RequestedClasses } else { ($selectedCases | Select-Object -ExpandProperty ClassName -Unique) }
foreach ($className in $selectedClassNames) {
	$classCases = @($selectedCases | Where-Object { $_.ClassName -eq $className })
	$methodCount = (@($classCases | Select-Object -ExpandProperty MethodName -Unique)).Count
	$theoryCaseCount = (@($classCases | Where-Object IsTheoryCase)).Count

	$perClassDiscovery.Add([pscustomobject]@{
		Class = $className
		ClassFound = $inventoryByClass.ContainsKey($className)
		DiscoveredMethods = $methodCount
		DiscoveredExecutableCases = $classCases.Count
		TheoryExecutableCases = $theoryCaseCount
	})
}

$runResults = [System.Collections.Generic.List[object]]::new()
$hardStop = $false
$hardStopReason = $null

if ($discoveryMismatches.Count -gt 0) {
	$hardStop = $true
	$hardStopReason = 'DISCOVERY/SCOPE MISMATCH'
}

if (-not $hardStop) {
	foreach ($testCase in $selectedCases) {
		$filter = "FullyQualifiedName=$($testCase.FullyQualifiedName)"

		try {
			$watchdogSummaryPath = & $resolvedWatchdogPath -RepoRoot $resolvedRepoRoot -ProjectPath $ProjectPath -RequestedFilter $filter -InactivitySeconds $InactivitySeconds
		}
		catch {
			$hardStop = $true
			$hardStopReason = "watchdog process failure: $($_.Exception.Message)"
			$runResults.Add([pscustomobject]@{
				Class = $testCase.ClassName
				Method = $testCase.MethodName
				FullyQualifiedName = $testCase.FullyQualifiedName
				DiscoverySource = 'dotnet test --list-tests'
				Filter = $filter
				WatchdogSummaryPath = $null
				StdoutPath = $null
				StderrPath = $null
				Executed = $false
				Passed = $false
				Failed = $false
				Skipped = $false
				Hang = $false
				WatchdogIntervention = $false
				DurationSeconds = $null
				ExitCode = $null
				Classification = 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR'
				Reason = $hardStopReason
				TotalTests = $null
				PassedTests = $null
				FailedTests = $null
				SkippedTests = $null
			})
			break
		}

		if (-not (Test-Path -Path $watchdogSummaryPath)) {
			$hardStop = $true
			$hardStopReason = "missing watchdog summary artifact: $watchdogSummaryPath"
			break
		}

		$watchdogSummary = Get-Content -Path $watchdogSummaryPath -Raw | ConvertFrom-Json
		if ($null -eq $watchdogSummary.finalRun) {
			$hardStop = $true
			$hardStopReason = "malformed watchdog summary (missing finalRun): $watchdogSummaryPath"
			break
		}

		$finalRun = $watchdogSummary.finalRun
		$stdoutAbsolutePath = Resolve-ArtifactPath -PathValue $finalRun.stdoutPath -RepoRootPath $resolvedRepoRoot
		$stderrAbsolutePath = Resolve-ArtifactPath -PathValue $finalRun.stderrPath -RepoRootPath $resolvedRepoRoot
		$stdoutEvidence = Parse-StdoutEvidence -StdoutPath $stdoutAbsolutePath
		$classification = Classify-IsolatedResult -FinalRun $finalRun -StdoutEvidence $stdoutEvidence -AllowSkippedCases:$AllowSkipped

		$runResults.Add([pscustomobject]@{
			Class = $testCase.ClassName
			Method = $testCase.MethodName
			FullyQualifiedName = $testCase.FullyQualifiedName
			DiscoverySource = 'dotnet test --list-tests'
			Filter = $filter
			WatchdogSummaryPath = $watchdogSummaryPath
			StdoutPath = $finalRun.stdoutPath
			StderrPath = $finalRun.stderrPath
			Executed = $classification.Executed
			Passed = $classification.Passed
			Failed = $classification.Failed
			Skipped = $classification.Skipped
			Hang = $classification.Hang
			WatchdogIntervention = $classification.WatchdogIntervention
			DurationSeconds = $finalRun.durationSeconds
			ExitCode = $finalRun.exitCode
			Classification = $classification.Classification
			Reason = $classification.Reason
			TotalTests = $stdoutEvidence.TotalTests
			PassedTests = $stdoutEvidence.PassedTests
			FailedTests = $stdoutEvidence.FailedTests
			SkippedTests = $stdoutEvidence.SkippedTests
		})

		if ($classification.Classification -ne 'PASS') {
			$hardStop = $true
			$hardStopReason = "hard stop condition: $($classification.Classification)"
			break
		}
	}
}

$perClassReconciliation = [System.Collections.Generic.List[object]]::new()
foreach ($classItem in $perClassDiscovery) {
	$classRuns = @($runResults | Where-Object { $_.Class -eq $classItem.Class })
	$executed = (@($classRuns | Where-Object Executed).Count)
	$passed = (@($classRuns | Where-Object Passed).Count)
	$failed = (@($classRuns | Where-Object Failed).Count)
	$skipped = (@($classRuns | Where-Object Skipped).Count)
	$hangs = (@($classRuns | Where-Object Hang).Count)
	$interventions = (@($classRuns | Where-Object WatchdogIntervention).Count)
	$infraErrors = (@($classRuns | Where-Object { $_.Classification -eq 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR' }).Count)
	$reconciled = ($classItem.DiscoveredExecutableCases -eq $executed -and $executed -eq ($passed + $failed + $skipped))

	$perClassReconciliation.Add([pscustomobject]@{
		Class = $classItem.Class
		ClassFound = $classItem.ClassFound
		DiscoveredMethods = $classItem.DiscoveredMethods
		DiscoveredExecutableCases = $classItem.DiscoveredExecutableCases
		ExecutedCases = $executed
		PassedCases = $passed
		FailedCases = $failed
		SkippedCases = $skipped
		Hangs = $hangs
		WatchdogInterventions = $interventions
		InfrastructureErrors = $infraErrors
		Reconciled = $reconciled
	})
}

$totalDiscovered = ($perClassDiscovery | Measure-Object -Property DiscoveredExecutableCases -Sum).Sum
$totalExecuted = (@($runResults | Where-Object Executed).Count)
$totalPassed = (@($runResults | Where-Object Passed).Count)
$totalFailed = (@($runResults | Where-Object Failed).Count)
$totalSkipped = (@($runResults | Where-Object Skipped).Count)
$totalHangs = (@($runResults | Where-Object Hang).Count)
$totalInterventions = (@($runResults | Where-Object WatchdogIntervention).Count)
$totalInfraErrors = (@($runResults | Where-Object { $_.Classification -eq 'INFRASTRUCTURE/RESULT-RECONCILIATION ERROR' }).Count)
$overallReconciled = ($totalDiscovered -eq $totalExecuted -and $totalExecuted -eq ($totalPassed + $totalFailed + $totalSkipped))
$hasDiscoveryMismatch = ($discoveryMismatches.Count -gt 0)

$green = ($overallReconciled -and $totalFailed -eq 0 -and $totalSkipped -eq 0 -and $totalHangs -eq 0 -and $totalInterventions -eq 0 -and $totalInfraErrors -eq 0 -and -not $hasDiscoveryMismatch -and -not $hardStop)

$summary = [ordered]@{
	generatedAtUtc = [DateTimeOffset]::UtcNow.ToString('o')
	repositoryRoot = $resolvedRepoRoot
	projectPath = $ProjectPath
	watchdogPath = $WatchdogPath
	inactivitySeconds = $InactivitySeconds
	requestedClasses = $RequestedClasses
	requestedTests = $RequestedTests
	discoverySource = 'dotnet test --list-tests'
	listTestsRawPath = $listTestsRawPath
	gitStatusBeforePath = $gitStatusBeforePath
	gitStatusAfterPath = $gitStatusAfterPath
	discoveryMismatches = $discoveryMismatches
	perClassDiscovery = $perClassDiscovery
	runs = $runResults
	perClassReconciliation = $perClassReconciliation
	overall = [ordered]@{
		discoveredExecutableCases = $totalDiscovered
		executedCases = $totalExecuted
		passedCases = $totalPassed
		failedCases = $totalFailed
		skippedCases = $totalSkipped
		hangs = $totalHangs
		watchdogInterventions = $totalInterventions
		infrastructureErrors = $totalInfraErrors
		discoveryMismatch = $hasDiscoveryMismatch
		reconciled = $overallReconciled
		hardStop = $hardStop
		hardStopReason = $hardStopReason
		green = $green
	}
}

$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryJsonPath

$markdown = [System.Text.StringBuilder]::new()
$null = $markdown.AppendLine('# Isolated Regression Gate Summary')
$null = $markdown.AppendLine()
$null = $markdown.AppendLine("- GeneratedAtUtc: $($summary.generatedAtUtc)")
$null = $markdown.AppendLine("- DiscoverySource: $($summary.discoverySource)")
$null = $markdown.AppendLine("- InactivitySeconds: $($summary.inactivitySeconds)")
$null = $markdown.AppendLine("- Green: $($summary.overall.green)")
$null = $markdown.AppendLine("- HardStop: $($summary.overall.hardStop)")
$null = $markdown.AppendLine("- HardStopReason: $($summary.overall.hardStopReason)")
$null = $markdown.AppendLine()
$null = $markdown.AppendLine('## Overall Reconciliation')
$null = $markdown.AppendLine("- discoveredExecutableCases: $($summary.overall.discoveredExecutableCases)")
$null = $markdown.AppendLine("- executedCases: $($summary.overall.executedCases)")
$null = $markdown.AppendLine("- passedCases: $($summary.overall.passedCases)")
$null = $markdown.AppendLine("- failedCases: $($summary.overall.failedCases)")
$null = $markdown.AppendLine("- skippedCases: $($summary.overall.skippedCases)")
$null = $markdown.AppendLine("- hangs: $($summary.overall.hangs)")
$null = $markdown.AppendLine("- watchdogInterventions: $($summary.overall.watchdogInterventions)")
$null = $markdown.AppendLine("- infrastructureErrors: $($summary.overall.infrastructureErrors)")
$null = $markdown.AppendLine("- discoveryMismatch: $($summary.overall.discoveryMismatch)")
$null = $markdown.AppendLine("- reconciled: $($summary.overall.reconciled)")
$null = $markdown.AppendLine()

if ($summary.discoveryMismatches.Count -gt 0) {
	$null = $markdown.AppendLine('## Discovery Mismatches')
	foreach ($mismatch in $summary.discoveryMismatches) {
		$null = $markdown.AppendLine("- $mismatch")
	}
	$null = $markdown.AppendLine()
}

$null = $markdown.AppendLine('## Per-Class Reconciliation')
foreach ($row in $summary.perClassReconciliation) {
	$null = $markdown.AppendLine("- $($row.Class): discovered=$($row.DiscoveredExecutableCases), executed=$($row.ExecutedCases), passed=$($row.PassedCases), failed=$($row.FailedCases), skipped=$($row.SkippedCases), hangs=$($row.Hangs), interventions=$($row.WatchdogInterventions), infraErrors=$($row.InfrastructureErrors), reconciled=$($row.Reconciled)")
}
$null = $markdown.AppendLine()

$null = $markdown.AppendLine('## Run Results')
foreach ($run in $summary.runs) {
	$null = $markdown.AppendLine("- $($run.FullyQualifiedName): classification=$($run.Classification), executed=$($run.Executed), passed=$($run.Passed), failed=$($run.Failed), skipped=$($run.Skipped), hang=$($run.Hang), intervention=$($run.WatchdogIntervention), durationSeconds=$($run.DurationSeconds), exitCode=$($run.ExitCode), summary=$($run.WatchdogSummaryPath)")
}
$null = $markdown.AppendLine()
$null = $markdown.AppendLine("- GitStatusBefore: $gitStatusBeforePath")
$null = $markdown.AppendLine("- GitStatusAfter: $gitStatusAfterPath")

Set-Content -Path $summaryMarkdownPath -Value $markdown.ToString()

$gitAfter = (& git -C $resolvedRepoRoot status --short) -join [Environment]::NewLine
Set-Content -Path $gitStatusAfterPath -Value $gitAfter

Write-Output $summaryJsonPath
if (-not $green) {
	exit 2
}
