// <copyright file="Program.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// IsolatedRegressionGate/Program: runs selected tests in isolated vstest processes and emits machine-readable forensic summaries.

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
/// Defines the program class for benchmark or isolated-regression execution.
/// </summary>
internal static class Program
{
    /// <summary>
    /// Gets or sets the utility Version value.
    /// </summary>
    private const string UtilityVersion = "0.1.0";

    /// <summary>
    /// Performs the main operation.
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
    /// Performs the write AndReturn operation.
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
    /// Performs the resolve VsTestConsolePath operation.
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
    /// Performs the resolve TestAssemblyPath operation.
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
    /// Performs the select Cases operation.
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
    /// Performs the build Markdown operation.
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
/// Defines the discovery EventsCollector class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class DiscoveryEventsCollector : ITestDiscoveryEventsHandler2
{
    /// <summary>
    /// Gets or sets the _cases value.
    /// </summary>
    private readonly List<TestCase> _cases = [];
    /// <summary>
    /// Performs the _sync operation.
    /// </summary>
    private readonly object _sync = new();

    /// <summary>
    /// Performs the completion operation.
    /// </summary>
    public TaskCompletionSource<DiscoverySnapshot> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Performs the handle DiscoveredTests operation.
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
    /// Performs the handle DiscoveryComplete operation.
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
    /// Performs the handle RawMessage operation.
    /// </summary>
    public void HandleRawMessage(string rawMessage)
    {
        _ = rawMessage;
    }

    /// <summary>
    /// Performs the handle LogMessage operation.
    /// </summary>
    public void HandleLogMessage(TestMessageLevel level, string? message)
    {
        _ = level;
        _ = message;
    }
}

/// <summary>
/// Defines the run EventsCollector class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class RunEventsCollector : ITestRunEventsHandler
{
    /// <summary>
    /// Gets or sets the _results value.
    /// </summary>
    private readonly List<TestResult> _results = [];
    /// <summary>
    /// Performs the _sync operation.
    /// </summary>
    private readonly object _sync = new();

    /// <summary>
    /// Performs the completion operation.
    /// </summary>
    public TaskCompletionSource<RunSnapshot> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Performs the handle TestRunStatsChange operation.
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
    /// Performs the handle TestRunComplete operation.
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
    /// Performs the launch ProcessWithDebuggerAttached operation.
    /// </summary>
    public int LaunchProcessWithDebuggerAttached(TestProcessStartInfo testProcessStartInfo)
    {
        _ = testProcessStartInfo;
        return -1;
    }

    /// <summary>
    /// Performs the handle RawMessage operation.
    /// </summary>
    public void HandleRawMessage(string rawMessage)
    {
        _ = rawMessage;
    }

    /// <summary>
    /// Performs the handle LogMessage operation.
    /// </summary>
    public void HandleLogMessage(TestMessageLevel level, string? message)
    {
        _ = level;
        _ = message;
    }
}

/// <summary>
/// Defines the discovery Snapshot record for benchmark or isolated-regression execution.
/// </summary>
internal sealed record DiscoverySnapshot(IReadOnlyList<TestCase> Cases, bool IsAborted, bool IsFullyDiscovered);
/// <summary>
/// Defines the run Snapshot record for benchmark or isolated-regression execution.
/// </summary>
internal sealed record RunSnapshot(IReadOnlyList<TestResult> Results, bool Completed, bool IsCanceled, bool IsAborted);

/// <summary>
/// Defines the gate ExitCode enum for benchmark or isolated-regression execution.
/// </summary>
internal enum GateExitCode
{
    Pass = 0,
    TestFailure = 1,
    InfrastructureError = 2,
    DiscoveryMismatch = 3,
    Timeout = 4,
}

/// <summary>
/// Defines the selection Exception class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class SelectionException(string message, bool infrastructure) : Exception(message)
{
    /// <summary>
    /// Gets or sets the infrastructure value.
    /// </summary>
    public bool Infrastructure { get; } = infrastructure;
}

/// <summary>
/// Defines the gate Options class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class GateOptions
{
    /// <summary>
    /// Gets or sets the repo Root value.
    /// </summary>
    public string RepoRoot { get; private set; } = ".";
    /// <summary>
    /// Gets or sets the project Path value.
    /// </summary>
    public string ProjectPath { get; private set; } = "VectorNNTP.BackFiller.Tests/VectorNNTP.BackFiller.Tests.csproj";
    /// <summary>
    /// Gets or sets the test AssemblyPath value.
    /// </summary>
    public string? TestAssemblyPath { get; private set; }
    /// <summary>
    /// Gets or sets the requested Class value.
    /// </summary>
    public string? RequestedClass { get; private set; }
    /// <summary>
    /// Gets or sets the requested Test value.
    /// </summary>
    public string? RequestedTest { get; private set; }
    /// <summary>
    /// Gets or sets the timeout Seconds value.
    /// </summary>
    public int TimeoutSeconds { get; private set; } = 45;

    /// <summary>
    /// Performs the parse operation.
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
/// Defines the gate Summary class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class GateSummary
{
    /// <summary>
    /// Gets or sets the utility Version value.
    /// </summary>
    public string UtilityVersion { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the runtime value.
    /// </summary>
    public string Runtime { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the test SdkVersion value.
    /// </summary>
    public string TestSdkVersion { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the platform ObjectModelVersion value.
    /// </summary>
    public string? PlatformObjectModelVersion { get; set; }
    /// <summary>
    /// Gets or sets the started Utc value.
    /// </summary>
    public DateTimeOffset StartedUtc { get; set; }
    /// <summary>
    /// Gets or sets the completed Utc value.
    /// </summary>
    public DateTimeOffset CompletedUtc { get; set; }
    /// <summary>
    /// Gets or sets the vs TestConsolePath value.
    /// </summary>
    public string? VsTestConsolePath { get; set; }
    /// <summary>
    /// Gets or sets the run Settings value.
    /// </summary>
    public string? RunSettings { get; set; }
    /// <summary>
    /// Gets or sets the test AssemblyPath value.
    /// </summary>
    public string? TestAssemblyPath { get; set; }
    /// <summary>
    /// Gets or sets the requested Class value.
    /// </summary>
    public string? RequestedClass { get; set; }
    /// <summary>
    /// Gets or sets the requested Test value.
    /// </summary>
    public string? RequestedTest { get; set; }
    /// <summary>
    /// Gets or sets the timeout Seconds value.
    /// </summary>
    public int TimeoutSeconds { get; set; }
    /// <summary>
    /// Gets or sets the discovery value.
    /// </summary>
    public DiscoverySummary? Discovery { get; set; }
    /// <summary>
    /// Gets or sets the selection Diagnostics value.
    /// </summary>
    public SelectionDiagnostics? SelectionDiagnostics { get; set; }
    /// <summary>
    /// Gets or sets the selected Count value.
    /// </summary>
    public int SelectedCount { get; set; }
    /// <summary>
    /// Gets or sets the selected TestCase value.
    /// </summary>
    public TestCaseIdentity? SelectedTestCase { get; set; }
    /// <summary>
    /// Gets or sets the execution value.
    /// </summary>
    public ExecutionSummary? Execution { get; set; }
    /// <summary>
    /// Gets or sets the structured TestResult value.
    /// </summary>
    public StructuredResult? StructuredTestResult { get; set; }
    /// <summary>
    /// Gets or sets the timeout value.
    /// </summary>
    public bool Timeout { get; set; }
    /// <summary>
    /// Gets or sets the hang Detected value.
    /// </summary>
    public bool HangDetected { get; set; }
    /// <summary>
    /// Gets or sets the infrastructure Error value.
    /// </summary>
    public bool InfrastructureError { get; set; }
    /// <summary>
    /// Gets or sets the reconciliation State value.
    /// </summary>
    public string? ReconciliationState { get; set; }
    /// <summary>
    /// Gets or sets the exception Type value.
    /// </summary>
    public string? ExceptionType { get; set; }
    /// <summary>
    /// Gets or sets the exception Stack value.
    /// </summary>
    public string? ExceptionStack { get; set; }
    /// <summary>
    /// Gets or sets the final Classification value.
    /// </summary>
    public string FinalClassification { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the exit Code value.
    /// </summary>
    public int ExitCode { get; set; }
}

/// <summary>
/// Defines the discovery Summary class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class DiscoverySummary
{
    /// <summary>
    /// Gets or sets the total Discovered value.
    /// </summary>
    public int TotalDiscovered { get; set; }
    /// <summary>
    /// Gets or sets the is Aborted value.
    /// </summary>
    public bool IsAborted { get; set; }
    /// <summary>
    /// Gets or sets the is FullyDiscovered value.
    /// </summary>
    public bool IsFullyDiscovered { get; set; }
    /// <summary>
    /// Gets or sets the cases value.
    /// </summary>
    public List<TestCaseIdentity> Cases { get; set; } = [];
}

/// <summary>
/// Defines the selection Diagnostics class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class SelectionDiagnostics
{
    /// <summary>
    /// Gets or sets the requested Class value.
    /// </summary>
    public string? RequestedClass { get; set; }
    /// <summary>
    /// Gets or sets the requested Test value.
    /// </summary>
    public string? RequestedTest { get; set; }
    /// <summary>
    /// Gets or sets the candidate Count value.
    /// </summary>
    public int CandidateCount { get; set; }
    /// <summary>
    /// Gets or sets the candidates value.
    /// </summary>
    public List<TestCaseIdentity> Candidates { get; set; } = [];
}

/// <summary>
/// Defines the execution Summary class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class ExecutionSummary
{
    /// <summary>
    /// Gets or sets the executed ViaTestCaseObjects value.
    /// </summary>
    public bool ExecutedViaTestCaseObjects { get; set; }
    /// <summary>
    /// Gets or sets the used FilterString value.
    /// </summary>
    public string? UsedFilterString { get; set; }
    /// <summary>
    /// Gets or sets the executed Count value.
    /// </summary>
    public int ExecutedCount { get; set; }
    /// <summary>
    /// Gets or sets the received ResultCount value.
    /// </summary>
    public int ReceivedResultCount { get; set; }
    /// <summary>
    /// Gets or sets the run StartedUtc value.
    /// </summary>
    public DateTimeOffset RunStartedUtc { get; set; }
    /// <summary>
    /// Gets or sets the elapsed Seconds value.
    /// </summary>
    public double ElapsedSeconds { get; set; }
    /// <summary>
    /// Gets or sets the complete value.
    /// </summary>
    public bool Complete { get; set; }
    /// <summary>
    /// Gets or sets the is Canceled value.
    /// </summary>
    public bool IsCanceled { get; set; }
    /// <summary>
    /// Gets or sets the is Aborted value.
    /// </summary>
    public bool IsAborted { get; set; }
    /// <summary>
    /// Gets or sets the cancel Requested value.
    /// </summary>
    public bool CancelRequested { get; set; }
    /// <summary>
    /// Gets or sets the abort Requested value.
    /// </summary>
    public bool AbortRequested { get; set; }
}

/// <summary>
/// Defines the test CaseIdentity class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class TestCaseIdentity
{
    /// <summary>
    /// Gets or sets the id value.
    /// </summary>
    public string? Id { get; set; }
    /// <summary>
    /// Gets or sets the fully QualifiedName value.
    /// </summary>
    public string FullyQualifiedName { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the display Name value.
    /// </summary>
    public string? DisplayName { get; set; }
    /// <summary>
    /// Gets or sets the source value.
    /// </summary>
    public string? Source { get; set; }

    /// <summary>
    /// Performs the from TestCase operation.
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
/// Defines the structured Result class for benchmark or isolated-regression execution.
/// </summary>
internal sealed class StructuredResult
{
    /// <summary>
    /// Gets or sets the outcome value.
    /// </summary>
    public string? Outcome { get; set; }
    /// <summary>
    /// Gets or sets the duration value.
    /// </summary>
    public string? Duration { get; set; }
    /// <summary>
    /// Gets or sets the error Message value.
    /// </summary>
    public string? ErrorMessage { get; set; }
    /// <summary>
    /// Gets or sets the error StackTrace value.
    /// </summary>
    public string? ErrorStackTrace { get; set; }
    /// <summary>
    /// Gets or sets the display Name value.
    /// </summary>
    public string? DisplayName { get; set; }

    /// <summary>
    /// Performs the from TestResult operation.
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
