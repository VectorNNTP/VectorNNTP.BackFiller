// <copyright file="MeasurementRunCoordinatorFixedCountContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Contract and behavior tests for the measurement run coordinator fixed count contract benchmark component.

using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates fixed-count mode decisions in the measurement coordinator.
    /// </summary>
    public sealed class MeasurementRunCoordinatorFixedCountContractTests
    {
        /// <summary>
        /// Ensures fixed-count mode uses producer completion rather than measurement duration delay.
        /// </summary>
        [Fact]
        public void RunAsync_Source_UsesProducerCompletionPathWhenMeasurementArticleCountIsConfigured()
        {
            string source = ReadCoordinatorSource();

            Assert.Contains("if (config.MeasurementArticleCount is null)", source, StringComparison.Ordinal);
            Assert.Contains("await Task.Delay(config.MeasurementDuration, cancellationToken)", source, StringComparison.Ordinal);
            Assert.Contains("await Task.WhenAll(producerTasks)", source, StringComparison.Ordinal);
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
        /// Ensures queue admission is completed only after producer tasks are awaited during drain.
        /// </summary>
        [Fact]
        public void DrainAndShutdownAsync_Source_CompletesQueueAfterProducerCompletion()
        {
            string source = ReadDrainSource();

            int awaitProducersIndex = source.IndexOf("await Task.WhenAll(producerTasks)", StringComparison.Ordinal);
            int stopAdmissionIndex = source.IndexOf("queue.StopAdmission();", StringComparison.Ordinal);

            Assert.True(awaitProducersIndex >= 0);
            Assert.True(stopAdmissionIndex > awaitProducersIndex);
        }

        /// <summary>
        /// Reads the current coordinator source from repository to assert fixed-count control-flow contract.
        /// </summary>
        private static string ReadCoordinatorSource()
        {
            return ReadBenchmarkSource("Execution", "MeasurementRunCoordinator.cs");
        }

        /// <summary>
        /// Reads the current orchestrator source from repository to assert fixed-count warmup contract.
        /// </summary>
        private static string ReadOrchestratorSource()
        {
            return ReadBenchmarkSource("Execution", "TransitBenchmarkOrchestrator.cs");
        }

        /// <summary>
        /// Reads the current drain implementation source from repository to assert queue/drain ordering contract.
        /// </summary>
        private static string ReadDrainSource()
        {
            return ReadBenchmarkSource("Execution", "MeasurementExecutionEngine.Drain.cs");
        }

        /// <summary>
        /// Verifies the ReadBenchmarkSource scenario and expected contract.
        /// </summary>
        private static string ReadBenchmarkSource(params string[] pathSegments)
        {
            string repoRoot = ResolveRepositoryRoot();
            string[] allSegments = [repoRoot, "VectorNNTP.BackFiller.Benchmarks", .. pathSegments];
            string path = Path.Combine(allSegments);
            return File.ReadAllText(path);
        }

        /// <summary>
        /// Verifies the ResolveRepositoryRoot scenario and expected contract.
        /// </summary>
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
        /// Verifies the EnumerateRootCandidates scenario and expected contract.
        /// </summary>
        private static IEnumerable<string> EnumerateRootCandidates()
        {
            yield return AppContext.BaseDirectory;
            yield return Directory.GetCurrentDirectory();
        }
    }
}
