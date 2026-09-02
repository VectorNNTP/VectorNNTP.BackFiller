// <copyright file="Program.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// IsolatedRegressionGate/Program: runs one selected test in an isolated vstest process and emits reconciled JSON and Markdown evidence.

using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.TestPlatform.VsTestConsole.TranslationLayer;
using Microsoft.TestPlatform.VsTestConsole.TranslationLayer.Interfaces;
using Microsoft.VisualStudio.TestPlatform.ObjectModel;
using Microsoft.VisualStudio.TestPlatform.ObjectModel.Client;
using Microsoft.VisualStudio.TestPlatform.ObjectModel.Logging;

namespace IsolatedRegressionGate;

/// <summary>
/// Coordinates isolated test discovery, selection, execution, timeout escalation, and evidence publication.
/// </summary>
internal static class Program
{
    /// <summary>
    /// Gets or sets the utility Version.
    /// </summary>
    private const string UtilityVersion = "0.1.0";

    /// <summary>
    /// Runs the gate workflow and returns the documented gate exit code.
    /// <param name="args">Command-line options controlling the repository, test scope, and timeout.</param>
    /// <returns>The <see cref="GateExitCode"/> value describing the final classification.</returns>
    /// <remarks>
    /// Discovery and execution are reconciled through the test platform's <see cref="TestCase"/> and
    /// <see cref="TestResult"/> objects so that a passing process exit cannot mask a scope or infrastructure failure.
    /// </remarks>
    /// </summary>
    private static async Task<int> Main(string[] args)
    {
        GateOptions options = GateOptions.Parse(args);
        DateTimeOffset startedUtc = DateTimeOffset.UtcNow;

        string repoRoot = Path.GetFullPath(options.RepoRoot);
        string artifactsDir = Path.Combine(repoRoot, "artifacts");
        Directory.CreateDirectory(artifactsDir);

        string summaryJsonPath = Path.Combine(artifactsDir, "isolated-regression-gate-csharp-summary.json");
        string summaryMarkdownPath = Path.Combine(artifactsDir, "isolated-regression-gate-csharp-summary.md");

        GateSummary summary = new()
        {
            UtilityVersion = UtilityVersion,
            Runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
            StartedUtc = startedUtc,
            RequestedClass = options.RequestedClass,
            RequestedTest = options.RequestedTest,
            TimeoutSeconds = options.TimeoutSeconds,
            ExitCode = (int)GateExitCode.InfrastructureError,
            FinalClassification = "INFRASTRUCTURE/RESULT-RECONCILIATION ERROR",
            TestSdkVersion = "18.8.1",
            PlatformObjectModelVersion = typeof(TestCase).Assembly.GetName().Version?.ToString(),
        };

        IVsTestConsoleWrapper? wrapper = null;

        try
        {
            string testAssemblyPath = ResolveTestAssemblyPath(repoRoot, options);
            summary.TestAssemblyPath = testAssemblyPath;

            string vstestConsolePath = ResolveVsTestConsolePath();
            summary.VsTestConsolePath = vstestConsolePath;

            wrapper = new VsTestConsoleWrapper(vstestConsolePath);
            wrapper.StartSession();
            wrapper.InitializeExtensions(new[] { testAssemblyPath });

            var discoveryHandler = new DiscoveryEventsCollector();
            var discoveryOptions = new TestPlatformOptions { CollectMetrics = true };
            string runSettings = "<RunSettings></RunSettings>";
            summary.RunSettings = runSettings;

            wrapper.DiscoverTests(new[] { testAssemblyPath }, runSettings, discoveryOptions, discoveryHandler);
            DiscoverySnapshot discoverySnapshot = await discoveryHandler.Completion.Task.ConfigureAwait(false);

            summary.Discovery = new DiscoverySummary
            {
                TotalDiscovered = discoverySnapshot.Cases.Count,
                IsAborted = discoverySnapshot.IsAborted,
                IsFullyDiscovered = discoverySnapshot.IsFullyDiscovered,
                Cases = discoverySnapshot.Cases.Select(TestCaseIdentity.FromTestCase).ToList(),
            };

            if (discoverySnapshot.IsAborted)
            {
                summary.FinalClassification = "INFRASTRUCTURE/RESULT-RECONCILIATION ERROR";
                summary.InfrastructureError = true;
                summary.ReconciliationState = "Discovery aborted";
                summary.ExitCode = (int)GateExitCode.InfrastructureError;
                return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
            }

            List<TestCase> selected = SelectCases(discoverySnapshot.Cases, options, summary);
            summary.SelectedCount = selected.Count;

            if (selected.Count == 0)
            {
                summary.FinalClassification = "DISCOVERY/SCOPE MISMATCH";
                summary.ReconciliationState = "No matching discovered TestCase";
                summary.InfrastructureError = false;
                summary.ExitCode = (int)GateExitCode.DiscoveryMismatch;
                return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
            }

            if (selected.Count > 1)
            {
                summary.FinalClassification = "INFRASTRUCTURE/RESULT-RECONCILIATION ERROR";
                summary.ReconciliationState = "Multiple matching TestCase candidates";
                summary.InfrastructureError = true;
                summary.ExitCode = (int)GateExitCode.InfrastructureError;
                return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
            }

            TestCase selectedCase = selected[0];
            summary.SelectedTestCase = TestCaseIdentity.FromTestCase(selectedCase);
            summary.Execution = new ExecutionSummary
            {
                ExecutedViaTestCaseObjects = true,
                ExecutedCount = 1,
                UsedFilterString = null,
            };

            var runHandler = new RunEventsCollector();
            var runOptions = new TestPlatformOptions { CollectMetrics = true };
            DateTimeOffset runStartedUtc = DateTimeOffset.UtcNow;
            summary.Execution.RunStartedUtc = runStartedUtc;

            wrapper.RunTests(new[] { selectedCase }, runSettings, runOptions, runHandler);

            Task completedTask = await Task.WhenAny(runHandler.Completion.Task, Task.Delay(TimeSpan.FromSeconds(options.TimeoutSeconds))).ConfigureAwait(false);
            if (completedTask != runHandler.Completion.Task)
            {
                summary.Timeout = true;
                summary.HangDetected = true;
                summary.Execution.ElapsedSeconds = Math.Round((DateTimeOffset.UtcNow - runStartedUtc).TotalSeconds, 2);
                summary.Execution.CancelRequested = true;
                wrapper.CancelTestRun();

                Task cancelWait = await Task.WhenAny(runHandler.Completion.Task, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
                if (cancelWait != runHandler.Completion.Task)
                {
                    summary.Execution.AbortRequested = true;
                    wrapper.AbortTestRun();
                }

                summary.FinalClassification = "HANG/TIMEOUT";
                summary.InfrastructureError = true;
                summary.ReconciliationState = "Run did not complete within timeout";
                summary.ExitCode = (int)GateExitCode.Timeout;
                return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
            }

            RunSnapshot runSnapshot = await runHandler.Completion.Task.ConfigureAwait(false);
            summary.Execution.ElapsedSeconds = Math.Round((DateTimeOffset.UtcNow - runStartedUtc).TotalSeconds, 2);
            summary.Execution.Complete = runSnapshot.Completed;
            summary.Execution.IsCanceled = runSnapshot.IsCanceled;
            summary.Execution.IsAborted = runSnapshot.IsAborted;

            List<TestResult> matchingResults = runSnapshot.Results.Where(r =>
                    string.Equals(r.TestCase?.Id.ToString(), selectedCase.Id.ToString(), StringComparison.OrdinalIgnoreCase)
                    || string.Equals(r.TestCase?.DisplayName, selectedCase.DisplayName, StringComparison.Ordinal)
                    || string.Equals(r.TestCase?.FullyQualifiedName, selectedCase.FullyQualifiedName, StringComparison.Ordinal))
                .ToList();

            summary.Execution.ReceivedResultCount = matchingResults.Count;

            if (!runSnapshot.Completed)
            {
                summary.FinalClassification = "INFRASTRUCTURE/RESULT-RECONCILIATION ERROR";
                summary.InfrastructureError = true;
                summary.ReconciliationState = "Run did not report completion";
                summary.ExitCode = (int)GateExitCode.InfrastructureError;
                return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
            }

            if (matchingResults.Count != 1)
            {
                summary.FinalClassification = "INFRASTRUCTURE/RESULT-RECONCILIATION ERROR";
                summary.InfrastructureError = true;
                summary.ReconciliationState = $"Expected 1 structured TestResult for selected TestCase, received {matchingResults.Count}";
                summary.ExitCode = (int)GateExitCode.InfrastructureError;
                return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
            }

            TestResult result = matchingResults[0];
            summary.StructuredTestResult = StructuredResult.FromTestResult(result);

            if (result.Outcome == TestOutcome.Passed)
            {
                summary.FinalClassification = "PASS";
                summary.InfrastructureError = false;
                summary.Timeout = false;
                summary.HangDetected = false;
                summary.ReconciliationState = "Selected one case, executed one case, received one passing TestResult";
                summary.ExitCode = (int)GateExitCode.Pass;
            }
            else if (result.Outcome == TestOutcome.Failed)
            {
                summary.FinalClassification = "TEST FAILURE";
                summary.InfrastructureError = false;
                summary.ReconciliationState = "Structured TestResult outcome is Failed";
                summary.ExitCode = (int)GateExitCode.TestFailure;
            }
            else
            {
                summary.FinalClassification = "TEST FAILURE";
                summary.InfrastructureError = false;
                summary.ReconciliationState = $"Structured TestResult outcome is {result.Outcome}";
                summary.ExitCode = (int)GateExitCode.TestFailure;
            }

            return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
        }
        catch (SelectionException sx)
        {
            summary.InfrastructureError = sx.Infrastructure;
            summary.FinalClassification = sx.Infrastructure
                ? "INFRASTRUCTURE/RESULT-RECONCILIATION ERROR"
                : "DISCOVERY/SCOPE MISMATCH";
            summary.ReconciliationState = sx.Message;
            summary.ExitCode = sx.Infrastructure ? (int)GateExitCode.InfrastructureError : (int)GateExitCode.DiscoveryMismatch;
            return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            summary.InfrastructureError = true;
            summary.FinalClassification = "INFRASTRUCTURE/RESULT-RECONCILIATION ERROR";
            summary.ReconciliationState = ex.Message;
            summary.ExceptionType = ex.GetType().FullName;
            summary.ExceptionStack = ex.ToString();
            summary.ExitCode = (int)GateExitCode.InfrastructureError;
            return await WriteAndReturn(summary, summaryJsonPath, summaryMarkdownPath).ConfigureAwait(false);
        }
        finally
        {
            if (wrapper is not null)
            {
                try
                {
                    wrapper.EndSession();
                }
                catch
                {
                }

                (wrapper as IDisposable)?.Dispose();
            }
        }
    }

    /// <summary>
    /// Writes the machine-readable and human-readable gate summaries and returns the selected exit code.
    /// <param name="summary">Summary to serialize.</param>
    /// <param name="summaryJsonPath">Destination for the indented JSON summary.</param>
    /// <param name="summaryMarkdownPath">Destination for the Markdown summary.</param>
    /// <returns>The exit code stored in <paramref name="summary"/>.</returns>
    /// </summary>
    private static async Task<int> WriteAndReturn(GateSummary summary, string summaryJsonPath, string summaryMarkdownPath)
    {
        summary.CompletedUtc = DateTimeOffset.UtcNow;

        JsonSerializerOptions jsonOptions = new()
        {
            WriteIndented = true,
        };

        await File.WriteAllTextAsync(summaryJsonPath, JsonSerializer.Serialize(summary, jsonOptions)).ConfigureAwait(false);
        await File.WriteAllTextAsync(summaryMarkdownPath, BuildMarkdown(summary)).ConfigureAwait(false);
        return summary.ExitCode;
    }

    /// <summary>
    /// Locates the newest installed .NET SDK test-console assembly.
    /// <returns>The full path to <c>vstest.console.dll</c>.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no SDK or test-console assembly is installed.</exception>
    /// </summary>
    private static string ResolveVsTestConsolePath()
    {
        string dotnetRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "dotnet", "sdk");
        if (!Directory.Exists(dotnetRoot))
        {
            throw new InvalidOperationException($"dotnet sdk root not found: {dotnetRoot}");
        }

        string? candidate = Directory.GetDirectories(dotnetRoot)
            .Select(path => new { Path = path, Name = Path.GetFileName(path) })
            .OrderByDescending(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .Select(x => Path.Combine(x.Path, "vstest.console.dll"))
            .FirstOrDefault(File.Exists);

        if (candidate is null)
        {
            throw new InvalidOperationException("Unable to locate vstest.console.dll from installed dotnet SDK directories.");
        }

        return candidate;
    }

    /// <summary>
    /// Resolves the test assembly explicitly requested by the caller or selects the newest built target assembly.
    /// <param name="repoRoot">Absolute repository root used to resolve relative paths.</param>
    /// <param name="options">Parsed gate options.</param>
    /// <returns>The full path to the test assembly to discover and execute.</returns>
    /// <exception cref="FileNotFoundException">Thrown when a requested project, assembly, or build output is missing.</exception>
    /// </summary>
    private static string ResolveTestAssemblyPath(string repoRoot, GateOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.TestAssemblyPath))
        {
            string explicitPath = Path.GetFullPath(Path.Combine(repoRoot, options.TestAssemblyPath));
            if (!File.Exists(explicitPath))
            {
                throw new FileNotFoundException($"Requested test assembly path not found: {explicitPath}");
            }

            return explicitPath;
        }

        string projectPath = Path.GetFullPath(Path.Combine(repoRoot, options.ProjectPath));
        if (!File.Exists(projectPath))
        {
            throw new FileNotFoundException($"Test project path not found: {projectPath}");
        }

        string projectDir = Path.GetDirectoryName(projectPath)!;
        string projectName = Path.GetFileNameWithoutExtension(projectPath);

        string[] candidates = Directory.GetFiles(projectDir, projectName + ".dll", SearchOption.AllDirectories)
            .Where(p => p.Contains(Path.Combine("bin", "Debug", "net8.0"), StringComparison.OrdinalIgnoreCase)
                     || p.Contains(Path.Combine("bin", "Release", "net8.0"), StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(File.GetLastWriteTimeUtc)
            .ToArray();

        if (candidates.Length == 0)
        {
            throw new FileNotFoundException("No built test assembly found under project bin output. Build test project first.");
        }

        return candidates[0];
    }

    /// <summary>
    /// Filters discovered cases by the requested class or fully qualified test name and records candidates.
    /// <param name="discovered">Cases returned by test discovery.</param>
    /// <param name="options">Selection criteria supplied on the command line.</param>
    /// <param name="summary">Summary receiving candidate diagnostics.</param>
    /// <returns>All cases matching the requested scope.</returns>
    /// </summary>
    private static List<TestCase> SelectCases(IReadOnlyList<TestCase> discovered, GateOptions options, GateSummary summary)
    {
        IEnumerable<TestCase> query = discovered;

        if (!string.IsNullOrWhiteSpace(options.RequestedClass))
        {
            query = query.Where(tc => tc.FullyQualifiedName.StartsWith(options.RequestedClass + ".", StringComparison.Ordinal));
        }

        if (!string.IsNullOrWhiteSpace(options.RequestedTest))
        {
            query = query.Where(tc =>
                string.Equals(tc.FullyQualifiedName, options.RequestedTest, StringComparison.Ordinal)
                || string.Equals(tc.DisplayName, options.RequestedTest, StringComparison.Ordinal));
        }

        List<TestCase> selected = query.ToList();
        summary.SelectionDiagnostics = new SelectionDiagnostics
        {
            RequestedClass = options.RequestedClass,
            RequestedTest = options.RequestedTest,
            CandidateCount = selected.Count,
            Candidates = selected.Select(TestCaseIdentity.FromTestCase).ToList(),
        };

        return selected;
    }

    /// <summary>
    /// Builds the operator-facing Markdown representation of a gate summary.
    /// <param name="summary">Summary whose discovery, execution, and classification data is rendered.</param>
    /// <returns>Markdown text suitable for writing as an artifact.</returns>
    /// </summary>
    private static string BuildMarkdown(GateSummary summary)
    {
        StringBuilder sb = new();
        sb.AppendLine("# Isolated Regression Gate C# Summary");
        sb.AppendLine();
        sb.AppendLine($"- UtilityVersion: {summary.UtilityVersion}");
        sb.AppendLine($"- Runtime: {summary.Runtime}");
        sb.AppendLine($"- TestSdkVersion: {summary.TestSdkVersion}");
        sb.AppendLine($"- PlatformObjectModelVersion: {summary.PlatformObjectModelVersion}");
        sb.AppendLine($"- VsTestConsolePath: {summary.VsTestConsolePath}");
        sb.AppendLine($"- RequestedClass: {summary.RequestedClass}");
        sb.AppendLine($"- RequestedTest: {summary.RequestedTest}");
        sb.AppendLine($"- TestAssemblyPath: {summary.TestAssemblyPath}");
        sb.AppendLine($"- TimeoutSeconds: {summary.TimeoutSeconds}");
        sb.AppendLine();

        sb.AppendLine("## Exit Codes");
        sb.AppendLine("- 0 = PASS");
        sb.AppendLine("- 1 = TEST FAILURE");
        sb.AppendLine("- 2 = INFRASTRUCTURE/RESULT-RECONCILIATION ERROR");
        sb.AppendLine("- 3 = DISCOVERY/SCOPE MISMATCH");
        sb.AppendLine("- 4 = HANG/TIMEOUT");
        sb.AppendLine();

        sb.AppendLine("## Discovery");
        sb.AppendLine($"- TotalDiscovered: {summary.Discovery?.TotalDiscovered}");
        sb.AppendLine($"- IsAborted: {summary.Discovery?.IsAborted}");
        sb.AppendLine($"- IsFullyDiscovered: {summary.Discovery?.IsFullyDiscovered}");
        sb.AppendLine();

        sb.AppendLine("## Selection");
        sb.AppendLine($"- CandidateCount: {summary.SelectionDiagnostics?.CandidateCount}");
        if (summary.SelectedTestCase is not null)
        {
            sb.AppendLine($"- Selected.Id: {summary.SelectedTestCase.Id}");
            sb.AppendLine($"- Selected.FullyQualifiedName: {summary.SelectedTestCase.FullyQualifiedName}");
            sb.AppendLine($"- Selected.DisplayName: {summary.SelectedTestCase.DisplayName}");
            sb.AppendLine($"- Selected.Source: {summary.SelectedTestCase.Source}");
        }

        sb.AppendLine();
        sb.AppendLine("## Execution");
        sb.AppendLine($"- ExecutedViaTestCaseObjects: {summary.Execution?.ExecutedViaTestCaseObjects}");
        sb.AppendLine($"- UsedFilterString: {summary.Execution?.UsedFilterString ?? "<none>"}");
        sb.AppendLine($"- ExecutedCount: {summary.Execution?.ExecutedCount}");
        sb.AppendLine($"- ReceivedResultCount: {summary.Execution?.ReceivedResultCount}");
        sb.AppendLine($"- Complete: {summary.Execution?.Complete}");
        sb.AppendLine($"- IsCanceled: {summary.Execution?.IsCanceled}");
        sb.AppendLine($"- IsAborted: {summary.Execution?.IsAborted}");
        sb.AppendLine($"- CancelRequested: {summary.Execution?.CancelRequested}");
        sb.AppendLine($"- AbortRequested: {summary.Execution?.AbortRequested}");
        sb.AppendLine($"- ElapsedSeconds: {summary.Execution?.ElapsedSeconds}");
        sb.AppendLine();

        sb.AppendLine("## Structured TestResult");
        if (summary.StructuredTestResult is null)
        {
            sb.AppendLine("- <none>");
        }
        else
        {
            sb.AppendLine($"- Outcome: {summary.StructuredTestResult.Outcome}");
            sb.AppendLine($"- Duration: {summary.StructuredTestResult.Duration}");
            sb.AppendLine($"- ErrorMessage: {summary.StructuredTestResult.ErrorMessage}");
            sb.AppendLine($"- ErrorStackTrace: {summary.StructuredTestResult.ErrorStackTrace}");
        }

        sb.AppendLine();
        sb.AppendLine("## Final");
        sb.AppendLine($"- Timeout: {summary.Timeout}");
        sb.AppendLine($"- HangDetected: {summary.HangDetected}");
        sb.AppendLine($"- InfrastructureError: {summary.InfrastructureError}");
        sb.AppendLine($"- ReconciliationState: {summary.ReconciliationState}");
        sb.AppendLine($"- FinalClassification: {summary.FinalClassification}");
        sb.AppendLine($"- ExitCode: {summary.ExitCode}");

        return sb.ToString();
    }
}

/// <summary>
/// Collects asynchronous test-discovery callbacks and publishes one immutable discovery snapshot.
/// </summary>
internal sealed class DiscoveryEventsCollector : ITestDiscoveryEventsHandler2
{
    /// <summary>
    /// Gets or sets the _cases.
    /// </summary>
    private readonly List<TestCase> _cases = [];
    /// <summary>
    /// Runs the _sync benchmark scenario.
    /// </summary>
    private readonly object _sync = new();

    /// <summary>
    /// Runs the completion benchmark scenario.
    /// </summary>
    public TaskCompletionSource<DiscoverySnapshot> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Adds a discovery batch to the thread-safe case collection.
    /// <param name="discoveredTestCases">Cases reported by the test platform; <see langword="null"/> is ignored.</param>
    /// </summary>
    public void HandleDiscoveredTests(IEnumerable<TestCase>? discoveredTestCases)
    {
        if (discoveredTestCases is null)
        {
            return;
        }

        lock (_sync)
        {
            _cases.AddRange(discoveredTestCases);
        }
    }

    /// <summary>
    /// Completes discovery and publishes the final snapshot, including the last batch.
    /// <param name="discoveryCompleteEventArgs">Completion state reported by the test platform.</param>
    /// <param name="lastChunk">Final cases not previously delivered through <see cref="HandleDiscoveredTests"/>.</param>
    /// </summary>
    public void HandleDiscoveryComplete(DiscoveryCompleteEventArgs discoveryCompleteEventArgs, IEnumerable<TestCase>? lastChunk)
    {
        lock (_sync)
        {
            if (lastChunk is not null)
            {
                _cases.AddRange(lastChunk);
            }

            Completion.TrySetResult(new DiscoverySnapshot(
                _cases.ToList(),
                discoveryCompleteEventArgs.IsAborted,
                !discoveryCompleteEventArgs.IsAborted));
        }
    }

    /// <summary>
    /// Receives a raw platform message that is intentionally not retained.
    /// <param name="rawMessage">Raw message supplied by the test platform.</param>
    /// </summary>
    public void HandleRawMessage(string rawMessage)
    {
        _ = rawMessage;
    }

    /// <summary>
    /// Receives a platform log message that is intentionally not retained.
    /// <param name="level">Severity assigned by the test platform.</param>
    /// <param name="message">Message text, if present.</param>
    /// </summary>
    public void HandleLogMessage(TestMessageLevel level, string? message)
    {
        _ = level;
        _ = message;
    }
}

/// <summary>
/// Collects asynchronous test-run callbacks and publishes one immutable execution snapshot.
/// </summary>
internal sealed class RunEventsCollector : ITestRunEventsHandler
{
    /// <summary>
    /// Gets or sets the _results.
    /// </summary>
    private readonly List<TestResult> _results = [];
    /// <summary>
    /// Runs the _sync benchmark scenario.
    /// </summary>
    private readonly object _sync = new();

    /// <summary>
    /// Runs the completion benchmark scenario.
    /// </summary>
    public TaskCompletionSource<RunSnapshot> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Adds newly reported test results to the thread-safe result collection.
    /// <param name="testRunChangedArgs">Incremental result update; updates without results are ignored.</param>
    /// </summary>
    public void HandleTestRunStatsChange(TestRunChangedEventArgs? testRunChangedArgs)
    {
        if (testRunChangedArgs?.NewTestResults is null)
        {
            return;
        }

        lock (_sync)
        {
            _results.AddRange(testRunChangedArgs.NewTestResults);
        }
    }

    /// <summary>
    /// Completes execution and publishes the final result snapshot.
    /// <param name="testRunCompleteArgs">Terminal run state reported by the test platform.</param>
    /// <param name="lastChunkArgs">Final result batch, if one was not reported earlier.</param>
    /// <param name="runContextAttachments">Attachments produced by the run; not consumed by this gate.</param>
    /// <param name="executorUris">Executor identifiers; not consumed by this gate.</param>
    /// </summary>
    public void HandleTestRunComplete(
        TestRunCompleteEventArgs testRunCompleteArgs,
        TestRunChangedEventArgs? lastChunkArgs,
        ICollection<AttachmentSet>? runContextAttachments,
        ICollection<string>? executorUris)
    {
        lock (_sync)
        {
            if (lastChunkArgs?.NewTestResults is not null)
            {
                _results.AddRange(lastChunkArgs.NewTestResults);
            }

            Completion.TrySetResult(new RunSnapshot(
                _results.ToList(),
                Completed: true,
                IsCanceled: testRunCompleteArgs.IsCanceled,
                IsAborted: testRunCompleteArgs.IsAborted));
        }
    }

    /// <summary>
    /// Declines debugger-attached process launch because the gate does not support interactive debugging.
    /// <param name="testProcessStartInfo">Process description supplied by the test platform.</param>
    /// <returns><c>-1</c>, indicating that no process was launched.</returns>
    /// </summary>
    public int LaunchProcessWithDebuggerAttached(TestProcessStartInfo testProcessStartInfo)
    {
        _ = testProcessStartInfo;
        return -1;
    }

    /// <summary>
    /// Receives a raw platform message that is intentionally not retained.
    /// <param name="rawMessage">Raw message supplied by the test platform.</param>
    /// </summary>
    public void HandleRawMessage(string rawMessage)
    {
        _ = rawMessage;
    }

    /// <summary>
    /// Receives a platform log message that is intentionally not retained.
    /// <param name="level">Severity assigned by the test platform.</param>
    /// <param name="message">Message text, if present.</param>
    /// </summary>
    public void HandleLogMessage(TestMessageLevel level, string? message)
    {
        _ = level;
        _ = message;
    }
}

/// <summary>
/// Captures the cases and completion state reported by test discovery.
/// </summary>
/// <param name="Cases">All cases received from discovery, including the final chunk.</param>
/// <param name="IsAborted">Indicates that the test platform aborted discovery.</param>
/// <param name="IsFullyDiscovered">Indicates that discovery reached normal completion.</param>
internal sealed record DiscoverySnapshot(IReadOnlyList<TestCase> Cases, bool IsAborted, bool IsFullyDiscovered);
/// <summary>
/// Captures test results and terminal state reported by test execution.
/// </summary>
/// <param name="Results">Results received for the executed test run.</param>
/// <param name="Completed">Indicates that the platform reported run completion.</param>
/// <param name="IsCanceled">Indicates that cancellation was requested or observed.</param>
/// <param name="IsAborted">Indicates that the platform aborted execution.</param>
internal sealed record RunSnapshot(IReadOnlyList<TestResult> Results, bool Completed, bool IsCanceled, bool IsAborted);

/// <summary>
/// Defines process exit codes used to distinguish test outcomes from gate infrastructure failures.
/// </summary>
internal enum GateExitCode
{
    /// <summary>Execution completed and the selected test passed.</summary>
    Pass = 0,
    /// <summary>The selected test completed with a failing or non-passing outcome.</summary>
    TestFailure = 1,
    /// <summary>The gate could not reconcile discovery or execution results.</summary>
    InfrastructureError = 2,
    /// <summary>Discovery produced no unique case matching the requested scope.</summary>
    DiscoveryMismatch = 3,
    /// <summary>The test did not complete before cancellation and abort escalation.</summary>
    Timeout = 4,
}

/// <summary>
/// Reports a selection failure and whether it represents an infrastructure error.
/// </summary>
internal sealed class SelectionException(string message, bool infrastructure) : Exception(message)
{
    /// <summary>
    /// Gets or sets the infrastructure.
    /// </summary>
    public bool Infrastructure { get; } = infrastructure;
}

/// <summary>
/// Stores validated command-line options for one isolated regression-gate invocation.
/// </summary>
internal sealed class GateOptions
{
    /// <summary>
    /// Gets the repository root used to resolve relative paths.
    /// </summary>
    public string RepoRoot { get; private set; } = ".";
    /// <summary>
    /// Gets the test project path used when an assembly path is not supplied.
    /// </summary>
    public string ProjectPath { get; private set; } = "VectorNNTP.BackFiller.Tests/VectorNNTP.BackFiller.Tests.csproj";
    /// <summary>
    /// Gets the optional test assembly path, relative to <see cref="RepoRoot"/>.
    /// </summary>
    public string? TestAssemblyPath { get; private set; }
    /// <summary>
    /// Gets the optional fully qualified test-class prefix to select.
    /// </summary>
    public string? RequestedClass { get; private set; }
    /// <summary>
    /// Gets the optional fully qualified test or display name to select.
    /// </summary>
    public string? RequestedTest { get; private set; }
    /// <summary>
    /// Gets the execution timeout in seconds before cancellation escalation.
    /// </summary>
    public int TimeoutSeconds { get; private set; } = 45;

    /// <summary>
    /// Parses command-line options and enforces the requirement for a requested class or test.
    /// <param name="args">Raw command-line arguments.</param>
    /// <returns>Options controlling the gate invocation.</returns>
    /// <exception cref="ArgumentException">Thrown for unknown options, missing values, or an empty selection.</exception>
    /// </summary>
    public static GateOptions Parse(string[] args)
    {
        GateOptions options = new();

        for (int i = 0; i < args.Length; i++)
        {
            string token = args[i];
            string Next() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"Missing value for {token}");

            switch (token)
            {
                case "--repo-root":
                    options.RepoRoot = Next();
                    break;
                case "--project-path":
                    options.ProjectPath = Next();
                    break;
                case "--test-assembly-path":
                    options.TestAssemblyPath = Next();
                    break;
                case "--requested-class":
                    options.RequestedClass = Next();
                    break;
                case "--requested-test":
                    options.RequestedTest = Next();
                    break;
                case "--timeout-seconds":
                    options.TimeoutSeconds = int.Parse(Next());
                    break;
                default:
                    throw new ArgumentException($"Unknown argument: {token}");
            }
        }

        if (string.IsNullOrWhiteSpace(options.RequestedClass) && string.IsNullOrWhiteSpace(options.RequestedTest))
        {
            throw new ArgumentException("Either --requested-class or --requested-test must be provided.");
        }

        return options;
    }
}

/// <summary>
/// Accumulates discovery, execution, reconciliation, and artifact metadata for one gate invocation.
/// </summary>
internal sealed class GateSummary
{
    /// <summary>
    /// Gets or sets the utility Version.
    /// </summary>
    public string UtilityVersion { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the runtime.
    /// </summary>
    public string Runtime { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the test SdkVersion.
    /// </summary>
    public string TestSdkVersion { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the platform ObjectModelVersion.
    /// </summary>
    public string? PlatformObjectModelVersion { get; set; }
    /// <summary>
    /// Gets or sets the started Utc.
    /// </summary>
    public DateTimeOffset StartedUtc { get; set; }
    /// <summary>
    /// Gets or sets the completed Utc.
    /// </summary>
    public DateTimeOffset CompletedUtc { get; set; }
    /// <summary>
    /// Gets or sets the vs TestConsolePath.
    /// </summary>
    public string? VsTestConsolePath { get; set; }
    /// <summary>
    /// Gets or sets the run Settings.
    /// </summary>
    public string? RunSettings { get; set; }
    /// <summary>
    /// Gets or sets the test AssemblyPath.
    /// </summary>
    public string? TestAssemblyPath { get; set; }
    /// <summary>
    /// Gets or sets the requested Class.
    /// </summary>
    public string? RequestedClass { get; set; }
    /// <summary>
    /// Gets or sets the requested Test.
    /// </summary>
    public string? RequestedTest { get; set; }
    /// <summary>
    /// Gets or sets the timeout Seconds.
    /// </summary>
    public int TimeoutSeconds { get; set; }
    /// <summary>
    /// Gets or sets the discovery.
    /// </summary>
    public DiscoverySummary? Discovery { get; set; }
    /// <summary>
    /// Gets or sets the selection Diagnostics.
    /// </summary>
    public SelectionDiagnostics? SelectionDiagnostics { get; set; }
    /// <summary>
    /// Gets or sets the selected Count.
    /// </summary>
    public int SelectedCount { get; set; }
    /// <summary>
    /// Gets or sets the selected TestCase.
    /// </summary>
    public TestCaseIdentity? SelectedTestCase { get; set; }
    /// <summary>
    /// Gets or sets the execution.
    /// </summary>
    public ExecutionSummary? Execution { get; set; }
    /// <summary>
    /// Gets or sets the structured TestResult.
    /// </summary>
    public StructuredResult? StructuredTestResult { get; set; }
    /// <summary>
    /// Gets or sets the timeout.
    /// </summary>
    public bool Timeout { get; set; }
    /// <summary>
    /// Gets or sets the hang Detected.
    /// </summary>
    public bool HangDetected { get; set; }
    /// <summary>
    /// Gets or sets the infrastructure Error.
    /// </summary>
    public bool InfrastructureError { get; set; }
    /// <summary>
    /// Gets or sets the reconciliation State.
    /// </summary>
    public string? ReconciliationState { get; set; }
    /// <summary>
    /// Gets or sets the exception Type.
    /// </summary>
    public string? ExceptionType { get; set; }
    /// <summary>
    /// Gets or sets the exception Stack.
    /// </summary>
    public string? ExceptionStack { get; set; }
    /// <summary>
    /// Gets or sets the final Classification.
    /// </summary>
    public string FinalClassification { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the exit Code.
    /// </summary>
    public int ExitCode { get; set; }
}

/// <summary>
/// Represents the discovery Summary class used by the benchmark or regression gate.
/// </summary>
internal sealed class DiscoverySummary
{
    /// <summary>
    /// Gets or sets the total Discovered.
    /// </summary>
    public int TotalDiscovered { get; set; }
    /// <summary>
    /// Gets or sets the is Aborted.
    /// </summary>
    public bool IsAborted { get; set; }
    /// <summary>
    /// Gets or sets the is FullyDiscovered.
    /// </summary>
    public bool IsFullyDiscovered { get; set; }
    /// <summary>
    /// Gets or sets the cases.
    /// </summary>
    public List<TestCaseIdentity> Cases { get; set; } = [];
}

/// <summary>
/// Represents the selection Diagnostics class used by the benchmark or regression gate.
/// </summary>
internal sealed class SelectionDiagnostics
{
    /// <summary>
    /// Gets or sets the requested Class.
    /// </summary>
    public string? RequestedClass { get; set; }
    /// <summary>
    /// Gets or sets the requested Test.
    /// </summary>
    public string? RequestedTest { get; set; }
    /// <summary>
    /// Gets or sets the candidate Count.
    /// </summary>
    public int CandidateCount { get; set; }
    /// <summary>
    /// Gets or sets the candidates.
    /// </summary>
    public List<TestCaseIdentity> Candidates { get; set; } = [];
}

/// <summary>
/// Represents the execution Summary class used by the benchmark or regression gate.
/// </summary>
internal sealed class ExecutionSummary
{
    /// <summary>
    /// Gets or sets the executed ViaTestCaseObjects.
    /// </summary>
    public bool ExecutedViaTestCaseObjects { get; set; }
    /// <summary>
    /// Gets or sets the used FilterString.
    /// </summary>
    public string? UsedFilterString { get; set; }
    /// <summary>
    /// Gets or sets the executed Count.
    /// </summary>
    public int ExecutedCount { get; set; }
    /// <summary>
    /// Gets or sets the received ResultCount.
    /// </summary>
    public int ReceivedResultCount { get; set; }
    /// <summary>
    /// Gets or sets the run StartedUtc.
    /// </summary>
    public DateTimeOffset RunStartedUtc { get; set; }
    /// <summary>
    /// Gets or sets the elapsed Seconds.
    /// </summary>
    public double ElapsedSeconds { get; set; }
    /// <summary>
    /// Gets or sets the complete.
    /// </summary>
    public bool Complete { get; set; }
    /// <summary>
    /// Gets or sets the is Canceled.
    /// </summary>
    public bool IsCanceled { get; set; }
    /// <summary>
    /// Gets or sets the is Aborted.
    /// </summary>
    public bool IsAborted { get; set; }
    /// <summary>
    /// Gets or sets the cancel Requested.
    /// </summary>
    public bool CancelRequested { get; set; }
    /// <summary>
    /// Gets or sets the abort Requested.
    /// </summary>
    public bool AbortRequested { get; set; }
}

/// <summary>
/// Represents the test CaseIdentity class used by the benchmark or regression gate.
/// </summary>
internal sealed class TestCaseIdentity
{
    /// <summary>
    /// Gets or sets the id.
    /// </summary>
    public string? Id { get; set; }
    /// <summary>
    /// Gets or sets the fully QualifiedName.
    /// </summary>
    public string FullyQualifiedName { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the display Name.
    /// </summary>
    public string? DisplayName { get; set; }
    /// <summary>
    /// Gets or sets the source.
    /// </summary>
    public string? Source { get; set; }

    /// <summary>
    /// Creates a value from TestCase.

    /// </summary>
    public static TestCaseIdentity FromTestCase(TestCase testCase) => new()
    {
        Id = testCase.Id.ToString(),
        FullyQualifiedName = testCase.FullyQualifiedName,
        DisplayName = testCase.DisplayName,
        Source = testCase.Source,
    };
}

/// <summary>
/// Represents the structured Result class used by the benchmark or regression gate.
/// </summary>
internal sealed class StructuredResult
{
    /// <summary>
    /// Gets or sets the outcome.
    /// </summary>
    public string? Outcome { get; set; }
    /// <summary>
    /// Gets or sets the duration.
    /// </summary>
    public string? Duration { get; set; }
    /// <summary>
    /// Gets or sets the error Message.
    /// </summary>
    public string? ErrorMessage { get; set; }
    /// <summary>
    /// Gets or sets the error StackTrace.
    /// </summary>
    public string? ErrorStackTrace { get; set; }
    /// <summary>
    /// Gets or sets the display Name.
    /// </summary>
    public string? DisplayName { get; set; }

    /// <summary>
    /// Creates a value from TestResult.

    /// </summary>
    public static StructuredResult FromTestResult(TestResult result) => new()
    {
        Outcome = result.Outcome.ToString(),
        Duration = result.Duration.ToString(),
        ErrorMessage = result.ErrorMessage,
        ErrorStackTrace = result.ErrorStackTrace,
        DisplayName = result.DisplayName,
    };
}
