param(
	[Parameter(Mandatory = $true)]
	[string]$RepoRoot,
	[Parameter(Mandatory = $true)]
	[string]$ProjectPath,
	[Parameter(Mandatory = $true)]
	[string]$RequestedFilter,
	[int]$InactivitySeconds = 45
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'
Set-Location -Path $RepoRoot

$artifactsDir = Join-Path $RepoRoot 'artifacts'
New-Item -ItemType Directory -Force -Path $artifactsDir | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'

function Get-Descendants {
	param([int]$RootPid)
	$all = Get-CimInstance Win32_Process
	$queue = [System.Collections.Generic.Queue[int]]::new()
	$seen = [System.Collections.Generic.HashSet[int]]::new()
	$queue.Enqueue($RootPid)

	while ($queue.Count -gt 0) {
		$pidValue = $queue.Dequeue()
		if ($seen.Add($pidValue)) {
			$children = $all | Where-Object { $_.ParentProcessId -eq $pidValue }
			foreach ($child in $children) {
				$queue.Enqueue([int]$child.ProcessId)
			}
		}
	}

	return $all | Where-Object { $seen.Contains([int]$_.ProcessId) }
}

function Get-LiveTestHost {
	param([int]$RootPid)
	$descendants = Get-Descendants -RootPid $RootPid
	return $descendants | Where-Object { $_.Name -match '^testhost(\.net8\.0)?\.exe$' } | Select-Object -First 1
}

function Stop-ProcessTree {
	param([int]$RootPid)
	$desc = Get-Descendants -RootPid $RootPid | Sort-Object ProcessId -Descending
	foreach ($proc in $desc) {
		try { Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue } catch { }
	}
}

function Invoke-WatchedRun {
	param(
		[string]$Filter,
		[string]$Label,
		[string]$MonitorPath,
		[int]$IdleSeconds
	)

	$stdoutPath = Join-Path $artifactsDir ("TransitPublisher-suite-$stamp.$Label.stdout.log")
	$stderrPath = Join-Path $artifactsDir ("TransitPublisher-suite-$stamp.$Label.stderr.log")

	$psi = [System.Diagnostics.ProcessStartInfo]::new('dotnet')
	$psi.WorkingDirectory = $RepoRoot
	$psi.UseShellExecute = $false
	$psi.RedirectStandardOutput = $true
	$psi.RedirectStandardError = $true
	$psi.CreateNoWindow = $true
	$null = $psi.ArgumentList.Add('test')
	$null = $psi.ArgumentList.Add($ProjectPath)
	$null = $psi.ArgumentList.Add('--filter')
	$null = $psi.ArgumentList.Add($Filter)
	$null = $psi.ArgumentList.Add('--logger')
	$null = $psi.ArgumentList.Add('console;verbosity=detailed')

	$stdoutWriter = [System.IO.StreamWriter]::new($stdoutPath, $false, [System.Text.UTF8Encoding]::new($false))
	$stderrWriter = [System.IO.StreamWriter]::new($stderrPath, $false, [System.Text.UTF8Encoding]::new($false))
	$outputHandler = [System.Diagnostics.DataReceivedEventHandler]{
		param($sender, $eventArgs)
		if ($null -ne $eventArgs.Data) {
			$stdoutWriter.WriteLine($eventArgs.Data)
			$stdoutWriter.Flush()
		}
	}
	$errorHandler = [System.Diagnostics.DataReceivedEventHandler]{
		param($sender, $eventArgs)
		if ($null -ne $eventArgs.Data) {
			$stderrWriter.WriteLine($eventArgs.Data)
			$stderrWriter.Flush()
		}
	}

	$runner = [System.Diagnostics.Process]::new()
	$runner.StartInfo = $psi
	$runner.add_OutputDataReceived($outputHandler)
	$runner.add_ErrorDataReceived($errorHandler)
	$null = $runner.Start()
	$runner.BeginOutputReadLine()
	$runner.BeginErrorReadLine()

	$runStart = [DateTimeOffset]::UtcNow
	$lastProgressUtc = [DateTimeOffset]::UtcNow
	$lastOutputLength = 0L
	$seenCompleted = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
	$lastCompleted = $null
	$hangDetected = $false
	$hangInfo = @{}

	while (-not $runner.HasExited) {
		Start-Sleep -Seconds 2

		$testHostProc = Get-LiveTestHost -RootPid $runner.Id
		$testHostPid = if ($null -ne $testHostProc) { [int]$testHostProc.ProcessId } else { $null }

		if (Test-Path $stdoutPath) {
			$len = (Get-Item $stdoutPath).Length
			if ($len -ne $lastOutputLength) {
				$lastOutputLength = $len
				$lastProgressUtc = [DateTimeOffset]::UtcNow
			}

			$tail = Get-Content $stdoutPath -Tail 400 -ErrorAction SilentlyContinue
			foreach ($line in $tail) {
				if ($line -match '\[xUnit\.net.*\]\s+\s*(VectorNNTP\.Backfiller\.Tests\.TransitPublisherTests\.[^\s]+)\s+\[(PASS|FAIL|SKIP)\]') {
					$tn = $Matches[1]
					if ($seenCompleted.Add($tn)) {
						$lastCompleted = $tn
						$lastProgressUtc = [DateTimeOffset]::UtcNow
					}
				}
			}
		}

		$idle = ([DateTimeOffset]::UtcNow - $lastProgressUtc).TotalSeconds
		Add-Content -Path $MonitorPath -Value "$(Get-Date -Format o) label=$Label runnerPid=$($runner.Id) hostPid=$testHostPid completed=$($seenCompleted.Count) lastCompleted=$lastCompleted idleSec=$([int]$idle)"

		if ($idle -ge $IdleSeconds) {
			$hangDetected = $true
			$hangStamp = Get-Date -Format 'yyyyMMdd-HHmm'
			$hangInfo['detectedUtc'] = [DateTimeOffset]::UtcNow.ToString('o')
			$hangInfo['runnerPid'] = $runner.Id
			$hangInfo['lastCompletedTest'] = $lastCompleted
			$hangInfo['completedCount'] = $seenCompleted.Count

			if ($null -ne $testHostProc) {
				$hostLive = Get-Process -Id $testHostProc.ProcessId -ErrorAction SilentlyContinue
				$hangInfo['testhost'] = [ordered]@{
					ProcessId = $testHostProc.ProcessId
					ParentProcessId = $testHostProc.ParentProcessId
					Name = $testHostProc.Name
					CommandLine = $testHostProc.CommandLine
					StartTime = if ($hostLive) { $hostLive.StartTime.ToString('o') } else { $null }
					CpuSeconds = if ($hostLive) { $hostLive.CPU } else { $null }
					WorkingSetMB = if ($hostLive) { [math]::Round($hostLive.WorkingSet64 / 1MB, 2) } else { $null }
				}

				$stackPath = Join-Path $artifactsDir ("TransitPublisher-suite-hang-$hangStamp-pid$($testHostProc.ProcessId)-stack.txt")
				$heapPath = Join-Path $artifactsDir ("TransitPublisher-suite-hang-$hangStamp-pid$($testHostProc.ProcessId)-heap.dmp")
				$fullPath = Join-Path $artifactsDir ("TransitPublisher-suite-hang-$hangStamp-pid$($testHostProc.ProcessId)-full.dmp")
				$analysisPath = Join-Path $artifactsDir ("TransitPublisher-suite-hang-$hangStamp-pid$($testHostProc.ProcessId)-analysis.txt")

				& dotnet-stack report --process-id $testHostProc.ProcessId *> $stackPath
				if (-not (Test-Path $stackPath) -or (Get-Item $stackPath).Length -eq 0) {
					& dotnet-stack report -p $testHostProc.ProcessId *> $stackPath
				}

				& dotnet-dump collect -p $testHostProc.ProcessId -o $heapPath --type Heap | Out-Null
				& dotnet-dump collect -p $testHostProc.ProcessId -o $fullPath --type Full | Out-Null

				if (Test-Path $heapPath) {
					& dotnet-dump analyze $heapPath -c "dumpasync -completed 0" -c "threads" -c "clrstack -all" -c "exit" *> $analysisPath
				}

				$activeTest = $null
				if (Test-Path $analysisPath) {
					$analysisLines = Get-Content $analysisPath -ErrorAction SilentlyContinue
					foreach ($ln in $analysisLines) {
						if ($ln -match 'TransitPublisherTests\.<([^>]+)>d__') {
							$activeTest = "VectorNNTP.BackFiller.Tests.TransitPublisherTests.$($Matches[1])"
							break
						}
					}
				}

				$hangInfo['stackPath'] = $stackPath
				$hangInfo['heapDumpPath'] = $heapPath
				$hangInfo['fullDumpPath'] = $fullPath
				$hangInfo['analysisPath'] = $analysisPath
				$hangInfo['activeTestFromDump'] = $activeTest
			}

			Stop-ProcessTree -RootPid $runner.Id
			break
		}
	}

	$runner.WaitForExit()
	$runner.CancelOutputRead()
	$runner.CancelErrorRead()
	$runner.remove_OutputDataReceived($outputHandler)
	$runner.remove_ErrorDataReceived($errorHandler)
	$stdoutWriter.Dispose()
	$stderrWriter.Dispose()

	$endUtc = [DateTimeOffset]::UtcNow
	$exitCode = if ($runner.HasExited) { $runner.ExitCode } else { -1 }

	$commandString = ('dotnet test {0} --filter "{1}" --logger "console;verbosity=detailed"' -f $ProjectPath, $Filter)
	$result = [ordered]@{
		label = $Label
		filter = $Filter
		command = $commandString
		runnerPid = $runner.Id
		startUtc = $runStart.ToString('o')
		endUtc = $endUtc.ToString('o')
		durationSeconds = [math]::Round(($endUtc - $runStart).TotalSeconds, 2)
		exitCode = $exitCode
		stdoutPath = $stdoutPath
		stderrPath = $stderrPath
		hangDetected = $hangDetected
		hang = $hangInfo
	}

	if (Test-Path $stdoutPath) {
		$allOut = Get-Content $stdoutPath -ErrorAction SilentlyContinue
		$summaryLine = $allOut | Where-Object { $_ -match '^Test summary:' } | Select-Object -Last 1
		if ($summaryLine -and $summaryLine -match 'total:\s*(\d+),\s*failed:\s*(\d+),\s*succeeded:\s*(\d+),\s*skipped:\s*(\d+),\s*duration:\s*([^\r\n]+)') {
			$result['total'] = [int]$Matches[1]
			$result['failed'] = [int]$Matches[2]
			$result['succeeded'] = [int]$Matches[3]
			$result['skipped'] = [int]$Matches[4]
			$result['testDuration'] = $Matches[5]
		}

		$noMatch = $allOut | Where-Object { $_ -match 'No test matches the given testcase filter' } | Select-Object -Last 1
		if ($noMatch) { $result['filterNoMatch'] = $noMatch }
	}

	return $result
}

$monitorPath = Join-Path $artifactsDir ("TransitPublisher-suite-$stamp.monitor.log")
$summaryPath = Join-Path $artifactsDir ("TransitPublisher-suite-$stamp.summary.json")
$textPath = Join-Path $artifactsDir ("TransitPublisher-suite-$stamp.summary.txt")

function Get-RunFieldValue {
	param(
		$Run,
		[string]$Name,
		$Fallback = 'N/A'
	)

	if ($Run -is [System.Collections.IDictionary]) {
		if ($Run.Contains($Name)) {
			return $Run[$Name]
		}

		return $Fallback
	}

	$property = $Run.PSObject.Properties[$Name]
	if ($null -ne $property) {
		return $property.Value
	}

	return $Fallback
}

$primary = Invoke-WatchedRun -Filter $RequestedFilter -Label 'primary' -MonitorPath $monitorPath -IdleSeconds $InactivitySeconds
$finalRun = $primary

if (($primary.Contains('filterNoMatch')) -and ($RequestedFilter -match '^TypeName=(.+)$')) {
	$typeName = $Matches[1]
	$fallbackFilter = "FullyQualifiedName~$typeName."
	$fallback = Invoke-WatchedRun -Filter $fallbackFilter -Label 'fallback' -MonitorPath $monitorPath -IdleSeconds $InactivitySeconds
	$finalRun = $fallback
	$finalRun['fallbackFromRequestedTypeName'] = $RequestedFilter
}

$summary = [ordered]@{
	watchdogStartedUtc = [DateTimeOffset]::UtcNow.ToString('o')
	requestedFilter = $RequestedFilter
	primaryRun = $primary
	finalRun = $finalRun
	monitorPath = $monitorPath
}

$summary | ConvertTo-Json -Depth 10 | Set-Content $summaryPath
$durationValue = Get-RunFieldValue -Run $finalRun -Name 'testDuration' -Fallback $null
if ($null -eq $durationValue) {
	$durationSeconds = Get-RunFieldValue -Run $finalRun -Name 'durationSeconds' -Fallback $null
	if ($null -ne $durationSeconds) {
		$durationValue = "$durationSeconds s"
	}
	else {
		$durationValue = 'N/A'
	}
}

@(
	"SummaryPath: $summaryPath",
	"RequestedFilter: $RequestedFilter",
	"FinalFilter: $(Get-RunFieldValue -Run $finalRun -Name 'filter')",
	"ExitCode: $(Get-RunFieldValue -Run $finalRun -Name 'exitCode')",
	"Total: $(Get-RunFieldValue -Run $finalRun -Name 'total')",
	"Passed: $(Get-RunFieldValue -Run $finalRun -Name 'succeeded')",
	"Failed: $(Get-RunFieldValue -Run $finalRun -Name 'failed')",
	"Skipped: $(Get-RunFieldValue -Run $finalRun -Name 'skipped')",
	"Duration: $durationValue",
	"HangDetected: $(Get-RunFieldValue -Run $finalRun -Name 'hangDetected')",
	"Stdout: $(Get-RunFieldValue -Run $finalRun -Name 'stdoutPath')",
	"Stderr: $(Get-RunFieldValue -Run $finalRun -Name 'stderrPath')",
	"Monitor: $monitorPath"
) | Set-Content $textPath

Write-Output $summaryPath
