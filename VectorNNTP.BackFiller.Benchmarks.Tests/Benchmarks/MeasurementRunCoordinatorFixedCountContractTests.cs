// <copyright file="MeasurementRunCoordinatorFixedCountContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for measurement run coordinator fixed count contract, covering benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the measurement run coordinator fixed count contract test suite.

using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates fixed-count mode decisions in the measurement coordinator.
    /// </summary>
    public sealed class MeasurementRunCoordinatorFixedCountContractTests
    {
        /// <summary>
        /// Ensures fixed-count mode closes measurement by admitted-cohort target rather than producer completion.
        /// </summary>
        [Fact]
        public void RunAsync_Source_UsesAdmittedCountBoundaryWhenMeasurementArticleCountIsConfigured()
        {
            string source = ReadCoordinatorSource();

            Assert.Contains("if (config.MeasurementArticleCount is null)", source, StringComparison.Ordinal);
            Assert.Contains("await Task.Delay(config.MeasurementDuration, cancellationToken)", source, StringComparison.Ordinal);
            Assert.Contains("await metrics.WaitForAdmittedCountAsync(targetAdmitted, cancellationToken)", source, StringComparison.Ordinal);
        }

        /// <summary>
        /// Ensures warmup is skipped in fixed-count mode to keep measurement article accounting exact.
        /// </summary>
        [Fact]
        public void RunCoreAsync_Source_SkipsWarmupWhenMeasurementArticleCountIsConfigured()
        {
            string source = ReadOrchestratorSource();

            Assert.Contains("if (config.MeasurementArticleCount is null)", source, StringComparison.Ordinal);
            Assert.Contains("RunWarmupAsync", source, StringComparison.Ordinal);
            Assert.Contains("Warmup skipped for fixed article-count mode", source, StringComparison.Ordinal);
        }

        /// <summary>
        /// Ensures queue admission is closed after producer shutdown is requested and producer tasks are awaited.
        /// </summary>
        [Fact]
        public void DrainAndShutdownAsync_Source_StopsAdmissionAfterProducerCompletionWait()
        {
            string source = ReadDrainSource();

            int producerCancelIndex = source.IndexOf("producerStopCts.Cancel();", StringComparison.Ordinal);
            int awaitProducersIndex = source.IndexOf("await Task.WhenAll(producerTasks)", StringComparison.Ordinal);
            int stopAdmissionIndex = source.IndexOf("queue.StopAdmission();", StringComparison.Ordinal);

            Assert.True(producerCancelIndex >= 0);
            Assert.True(awaitProducersIndex > producerCancelIndex);
            Assert.True(stopAdmissionIndex > awaitProducersIndex);
        }

        /// <summary>
        /// Verifies fixed-count measurement waits on admitted cohort completion and not on producer/task completion shape.
        /// </summary>
        [Fact]
        public async Task WaitForAdmittedCountAsync_CompletesExactlyAtConfiguredTargetAsync()
        {
            MeasurementMetrics metrics = new(articleBytes: 1024);
            long startTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementStart(startTick);

            const int target = 4;
            Task waitTask = metrics.WaitForAdmittedCountAsync(target, CancellationToken.None);

            metrics.OnAdmitted(1024, startTick + 1);
            metrics.OnAdmitted(1024, startTick + 2);
            metrics.OnAdmitted(1024, startTick + 3);

            Assert.False(waitTask.IsCompleted);

            metrics.OnAdmitted(1024, startTick + 4);

            await waitTask;
            Assert.Equal(target, metrics.GetAdmittedCount());
        }

        /// <summary>
        /// Verifies deterministic boundary split for terminal events using explicit monotonic tick facts.
        /// </summary>
        [Fact]
        public void TerminalClassification_BoundaryOrdering_IsDeterministic()
        {
            MeasurementMetrics metrics = new(articleBytes: 1024);
            long startTick = Stopwatch.GetTimestamp();
            metrics.MarkMeasurementStart(startTick);

            long boundaryTick = startTick + 1000;

            metrics.OnAdmitted(1024, startTick + 10);
            metrics.OnPublishResult(
                new TransitPublishResult("<before@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", startTick + 20, startTick + 21, startTick + 22, startTick + 23, startTick + 24, startTick + 25, startTick + 26, startTick + 27),
                bytes: 1024,
                dequeuedTick: startTick + 9,
                publishStartTick: startTick + 20,
                publishEndTick: startTick + 27,
                pendingAtSubmit: 1,
                pendingAtComplete: 0,
                completionTickOverride: boundaryTick - 1);

            metrics.OnAdmitted(1024, startTick + 30);
            metrics.OnPublishResult(
                new TransitPublishResult("<equal@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", startTick + 40, startTick + 41, startTick + 42, startTick + 43, startTick + 44, startTick + 45, startTick + 46, startTick + 47),
                bytes: 1024,
                dequeuedTick: startTick + 29,
                publishStartTick: startTick + 40,
                publishEndTick: startTick + 47,
                pendingAtSubmit: 1,
                pendingAtComplete: 0,
                completionTickOverride: boundaryTick);

            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            metrics.OnAdmitted(1024, startTick + 50);
            metrics.OnPublishResult(
                new TransitPublishResult("<after@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", startTick + 60, startTick + 61, startTick + 62, startTick + 63, startTick + 64, startTick + 65, startTick + 66, startTick + 67),
                bytes: 1024,
                dequeuedTick: startTick + 49,
                publishStartTick: startTick + 60,
                publishEndTick: startTick + 67,
                pendingAtSubmit: 1,
                pendingAtComplete: 0,
                completionTickOverride: boundaryTick + 1);

            MeasurementSnapshot snapshot = metrics.Snapshot();
            Assert.Equal(3, snapshot.AcceptedCount);
            Assert.Equal(2, snapshot.AcceptedWithinWindowCount);
            Assert.Equal(1, snapshot.AcceptedPostMeasurementCount);
        }

        /// <summary>
        /// Reads the current coordinator source from repository to assert fixed-count control-flow contract.
        /// </summary>
        /// <returns>The value returned by the read coordinator source helper.</returns>
        /// <summary>
        /// Confirms the read coordinator source behavior.
        /// </summary>
        /// <returns>The value returned by the read coordinator source helper.</returns>
        private static string ReadCoordinatorSource()
        {
            return ReadBenchmarkSource("Execution", "MeasurementRunCoordinator.cs");
        }

        /// <summary>
        /// Reads the current orchestrator source from repository to assert fixed-count warmup contract.
        /// </summary>
        /// <returns>The value returned by the read orchestrator source helper.</returns>
        /// <summary>
        /// Confirms the read orchestrator source behavior.
        /// </summary>
        /// <returns>The value returned by the read orchestrator source helper.</returns>
        private static string ReadOrchestratorSource()
        {
            return ReadBenchmarkSource("Execution", "TransitBenchmarkOrchestrator.cs");
        }

        /// <summary>
        /// Reads the current drain implementation source from repository to assert queue/drain ordering contract.
        /// </summary>
        /// <returns>The value returned by the read drain source helper.</returns>
        /// <summary>
        /// Confirms the read drain source behavior.
        /// </summary>
        /// <returns>The value returned by the read drain source helper.</returns>
        private static string ReadDrainSource()
        {
            return ReadBenchmarkSource("Execution", "MeasurementExecutionEngine.Drain.cs");
        }

        /// <summary>
        /// Confirms the read benchmark source behavior.
        /// </summary>
        /// <returns>The value returned by the read benchmark source helper.</returns>
        /// <summary>
        /// Confirms the read benchmark source behavior.
        /// </summary>
        /// <param name="pathSegments">The path segments used by this test scenario.</param>
        /// <returns>The value returned by the read benchmark source helper.</returns>
        private static string ReadBenchmarkSource(params string[] pathSegments)
        {
            string repoRoot = ResolveRepositoryRoot();
            string[] allSegments = [repoRoot, "VectorNNTP.BackFiller.Benchmarks", .. pathSegments];
            string path = Path.Combine(allSegments);
            return File.ReadAllText(path);
        }

        /// <summary>
        /// Confirms the resolve repository root behavior.
        /// </summary>
        /// <returns>The value returned by the resolve repository root helper.</returns>
        /// <summary>
        /// Confirms the resolve repository root behavior.
        /// </summary>
        /// <returns>The value returned by the resolve repository root helper.</returns>
        private static string ResolveRepositoryRoot()
        {
            foreach (string startPath in EnumerateRootCandidates())
            {
                for (DirectoryInfo? current = new(startPath); current is not null; current = current.Parent)
                {
                    string solutionPath = Path.Combine(current.FullName, "VectorNNTP.BackFiller.slnx");
                    string benchmarksProjectPath = Path.Combine(current.FullName, "VectorNNTP.BackFiller.Benchmarks", "Execution", "MeasurementRunCoordinator.cs");
                    if (File.Exists(solutionPath) && File.Exists(benchmarksProjectPath))
                    {
                        return current.FullName;
                    }
                }
            }

            throw new DirectoryNotFoundException("Unable to locate repository root for benchmark source-contract tests.");
        }

        /// <summary>
        /// Confirms the enumerate root candidates behavior.
        /// </summary>
        /// <returns>The value returned by the enumerate root candidates helper.</returns>
        /// <summary>
        /// Confirms the enumerate root candidates behavior.
        /// </summary>
        /// <returns>The value returned by the enumerate root candidates helper.</returns>
        private static IEnumerable<string> EnumerateRootCandidates()
        {
            yield return AppContext.BaseDirectory;
            yield return Directory.GetCurrentDirectory();
        }
    }
}
