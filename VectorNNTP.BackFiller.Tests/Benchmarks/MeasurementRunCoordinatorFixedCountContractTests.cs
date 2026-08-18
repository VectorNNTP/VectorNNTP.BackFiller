using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

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
        string repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
        string path = Path.Combine(repoRoot, "VectorNNTP.BackFiller.Benchmarks", "Execution", "MeasurementRunCoordinator.cs");

        return File.ReadAllText(path);
    }

    /// <summary>
    /// Reads the current orchestrator source from repository to assert fixed-count warmup contract.
    /// </summary>
    private static string ReadOrchestratorSource()
    {
        string repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
        string path = Path.Combine(repoRoot, "VectorNNTP.BackFiller.Benchmarks", "Execution", "TransitBenchmarkOrchestrator.cs");

        return File.ReadAllText(path);
    }

    /// <summary>
    /// Reads the current drain implementation source from repository to assert queue/drain ordering contract.
    /// </summary>
    private static string ReadDrainSource()
    {
        string repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
        string path = Path.Combine(repoRoot, "VectorNNTP.BackFiller.Benchmarks", "Execution", "MeasurementExecutionEngine.Drain.cs");

        return File.ReadAllText(path);
    }
}
