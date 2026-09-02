// <copyright file="BenchmarkConsoleReporterContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for benchmark console reporter contract, covering benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the benchmark console reporter contract test suite.

using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
        /// Confirms the benchmark console reporter contract tests behavior.
    /// </summary>
    public sealed class BenchmarkConsoleReporterContractTests
    {
        /// <summary>
        /// Confirms the print final report contains expected sections in stable order behavior.
        /// </summary>
        [Fact]
        public void PrintFinalReport_ContainsExpectedSectionsInStableOrder()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 20);
            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                BenchmarkContractTestHelper.CreateMeasurementSnapshot(),
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(128L * 1024 * 1024, 64L * 1024 * 1024, 32L * 1024 * 1024),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 7, 1, 0, 0, 20, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(1),
                outstandingAtMeasurementEnd: 2,
                drainedAfterMeasurement: 2,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: true);

            StringWriter writer = new();
            TextWriter original = Console.Out;

            try
            {
                Console.SetOut(writer);
                BenchmarkConsoleReporter.PrintFinalReport(result, config);
            }
            finally
            {
                Console.SetOut(original);
            }

            string output = writer.ToString();

            Assert.Contains("Benchmark Build Version:", output, StringComparison.Ordinal);
            Assert.Contains("Endpoint type:", output, StringComparison.Ordinal);
            Assert.Contains("Endpoint identity:", output, StringComparison.Ordinal);
            Assert.Contains("Endpoint host:", output, StringComparison.Ordinal);
            Assert.Contains("Endpoint port:", output, StringComparison.Ordinal);
            Assert.Contains("Preparation summary:", output, StringComparison.Ordinal);
            Assert.Contains("Generated articles:", output, StringComparison.Ordinal);
            Assert.Contains("Admitted articles:", output, StringComparison.Ordinal);
            Assert.Contains("Accepted articles:", output, StringComparison.Ordinal);
            Assert.Contains("Queue target depth (articles):", output, StringComparison.Ordinal);
            Assert.Contains("Queue depth samples:", output, StringComparison.Ordinal);
            Assert.Contains("CPU % (avg sampled):", output, StringComparison.Ordinal);
            Assert.Contains("Forensic timing and time-series:", output, StringComparison.Ordinal);

            int benchmarkIndex = output.IndexOf("Benchmark Build Version:", StringComparison.Ordinal);
            int endpointTypeIndex = output.IndexOf("Endpoint type:", StringComparison.Ordinal);
            int prepIndex = output.IndexOf("Preparation summary:", StringComparison.Ordinal);
            int generatedIndex = output.IndexOf("Generated articles:", StringComparison.Ordinal);
            int queueIndex = output.IndexOf("Queue target depth (articles):", StringComparison.Ordinal);
            int cpuIndex = output.IndexOf("CPU % (avg sampled):", StringComparison.Ordinal);
            int forensicIndex = output.IndexOf("Forensic timing and time-series:", StringComparison.Ordinal);

            Assert.True(benchmarkIndex >= 0);
            Assert.True(endpointTypeIndex > benchmarkIndex);
            Assert.True(prepIndex > endpointTypeIndex);
            Assert.True(generatedIndex > prepIndex);
            Assert.True(queueIndex > generatedIndex);
            Assert.True(cpuIndex > queueIndex);
            Assert.True(forensicIndex > cpuIndex);
        }

        /// <summary>
        /// Ensures queue telemetry output marks the explicit no-sample condition.
        /// </summary>
        [Fact]
        public void PrintFinalReport_WhenQueueDepthHasNoSamples_PrintsExplicitNoSampleMarkers()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 20);
            MeasurementSnapshot snapshot = BenchmarkContractTestHelper.CreateMeasurementSnapshot(queueDepthSampleCount: 0);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                snapshot,
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(128L * 1024 * 1024, 64L * 1024 * 1024, 32L * 1024 * 1024),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 7, 1, 0, 0, 20, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(1),
                outstandingAtMeasurementEnd: 2,
                drainedAfterMeasurement: 2,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: true);

            StringWriter writer = new();
            TextWriter original = Console.Out;

            try
            {
                Console.SetOut(writer);
                BenchmarkConsoleReporter.PrintFinalReport(result, config);
            }
            finally
            {
                Console.SetOut(original);
            }

            string output = writer.ToString();

            Assert.Contains("Queue depth samples: 0", output, StringComparison.Ordinal);
            Assert.Contains("Queue minimum depth: (no samples)", output, StringComparison.Ordinal);
            Assert.Contains("Queue average depth: (no samples)", output, StringComparison.Ordinal);
            Assert.Contains("Queue average bytes: (no samples)", output, StringComparison.Ordinal);
        }
    }
}
