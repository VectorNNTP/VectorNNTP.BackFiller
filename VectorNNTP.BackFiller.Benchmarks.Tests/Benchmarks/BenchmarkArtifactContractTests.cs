// <copyright file="BenchmarkArtifactContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for benchmark artifact contract, covering benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the benchmark artifact contract test suite.

using System.Text;
using System.Text.Json;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Confirms the benchmark artifact contract tests behavior.
    /// </summary>
    public sealed class BenchmarkArtifactContractTests
    {
        /// <summary>
        /// Confirms the benchmark result artifact from maps core benchmark and config values without reinterpretation behavior.
        /// </summary>
        [Fact]
        public void BenchmarkResultArtifact_From_MapsCoreBenchmarkAndConfigValuesWithoutReinterpretation()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(
                measurementSeconds: 30,
                maxResidentBytes: 128L * 1024 * 1024,
                articleTargetBytes: 1024 * 1024,
                maxQueuedArticles: 512);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                BenchmarkContractTestHelper.CreateMeasurementSnapshot(),
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(256L * 1024 * 1024, 128L * 1024 * 1024, 64L * 1024 * 1024),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 4, 1, 2, 3, 4, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 4, 1, 2, 3, 34, TimeSpan.Zero),
                drainDuration: TimeSpan.FromMilliseconds(789),
                outstandingAtMeasurementEnd: 4,
                drainedAfterMeasurement: 4,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: true);

            BenchmarkResultArtifact artifact = BenchmarkResultArtifact.From(result, config, processorCount: 8);

            Assert.Equal(result.BenchmarkBuildVersion, artifact.BenchmarkBuildVersion);
            Assert.Equal(result.GeneratedArticles, artifact.GeneratedArticles);
            Assert.Equal(result.GeneratedGbps, artifact.GeneratedGbps);
            Assert.Equal(result.AdmittedArticles, artifact.AdmittedArticles);
            Assert.Equal(result.AdmittedGbps, artifact.AdmittedGbps);
            Assert.Equal(result.AcceptedArticles, artifact.AcceptedArticles);
            Assert.Equal(result.AcceptedGbps, artifact.AcceptedGbps);
            Assert.Equal(result.RejectedArticles, artifact.RejectedArticles);
            Assert.Equal(result.AmbiguousArticles, artifact.AmbiguousArticles);
            Assert.Equal(result.PeakQueueDepth, artifact.PeakQueueDepth);
            Assert.Equal(result.PeakQueuedBytes, artifact.PeakQueueBytes);
            Assert.Equal(result.PeakInFlight, artifact.PeakDispatcherInFlight);
            Assert.Equal(result.PeakActualPending, artifact.PeakActualPending);
            Assert.Equal(result.ProducerBlockedPercent, artifact.ProducerBlockedPercent);
            Assert.Equal(result.ProducerQueueWaitMilliseconds, artifact.ProducerQueueWaitMs);
            Assert.Equal(result.WorkingSetMb, artifact.WorkingSetMb);
            Assert.Equal(result.GcHeapMb, artifact.GcHeapMb);
            Assert.Equal(result.AllocatedMb, artifact.AllocatedMb);
            Assert.Equal(result.Gen0Collections, artifact.Gen0);
            Assert.Equal(result.Gen1Collections, artifact.Gen1);
            Assert.Equal(result.Gen2Collections, artifact.Gen2);
            Assert.Equal(result.PendingDepthLatencyBuckets, artifact.PendingDepthLatencyBuckets);
            Assert.Equal(result.ObservabilityNotes, artifact.ObservabilityNotes);
            Assert.Equal(result.FixedCountBoundaryTelemetry, artifact.FixedCountBoundaryTelemetry);
            Assert.Equal(result.SubmissionPumpFault, artifact.SubmissionPumpFault);
            Assert.Equal(result.P1GreetingProvenance, artifact.P1GreetingProvenance);

            Assert.Equal(config.EndpointType, artifact.EndpointType);
            Assert.Equal(config.EndpointIdentity, artifact.EndpointIdentity);
            Assert.Equal(config.EndpointHost, artifact.EndpointHost);
            Assert.Equal(config.EndpointPort, artifact.EndpointPort);
            Assert.Equal(config.EndpointUseSsl, artifact.EndpointUseSsl);

            Assert.Equal(config.Mode.ToString(), artifact.Mode);
            Assert.Equal(config.WarmupDuration.TotalSeconds, artifact.WarmupSeconds);
            Assert.Equal(config.MeasurementDuration.TotalSeconds, artifact.MeasurementSeconds);
            Assert.Equal(config.ConnectionPoolSize, artifact.ConnectionPoolSize);
            Assert.Equal(config.PerConnectionPipelineDepth, artifact.PipelineDepth);
            Assert.Equal(config.DispatchWorkerCount, artifact.DispatchWorkers);
            Assert.Equal(config.GeneratorWorkerCount, artifact.GeneratorWorkers);
            Assert.Equal(config.MaxQueuedArticles, artifact.QueueMaxArticles);
            Assert.Equal(config.MaxResidentBytes, artifact.QueueMaxBytes);
            Assert.Equal(config.ArticleTargetBytes, artifact.ArticleTargetBytes);
            Assert.Equal(config.ProducerQueueTargetArticles, artifact.ProducerQueueTargetArticles);

            Assert.Equal(8 * (result.AverageCpuPercent / 100d), artifact.EquivalentBusyCores);
        }
        /// <summary>
        /// Confirms the json artifact writer serialize contains expected contract shape and property names behavior.
        /// </summary>
        [Fact]
        public void JsonArtifactWriter_Serialize_ContainsExpectedContractShapeAndPropertyNames()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig();
            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                BenchmarkContractTestHelper.CreateMeasurementSnapshot(),
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 5, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 5, 1, 0, 0, 10, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(1),
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: 0,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: true);

            BenchmarkResultArtifact artifact = BenchmarkResultArtifact.From(result, config, processorCount: Environment.ProcessorCount);
            string json = JsonArtifactWriter.Serialize(artifact);

            using JsonDocument document = JsonDocument.Parse(json);
            JsonElement root = document.RootElement;

            Assert.Equal(JsonValueKind.Object, root.ValueKind);
            Assert.True(root.TryGetProperty("BenchmarkBuildVersion", out _));
            Assert.True(root.TryGetProperty("RuntimeAssemblyVersion", out _));
            Assert.True(root.TryGetProperty("RuntimeAssemblyPath", out _));
            Assert.True(root.TryGetProperty("EndpointType", out _));
            Assert.True(root.TryGetProperty("EndpointIdentity", out _));
            Assert.True(root.TryGetProperty("EndpointHost", out _));
            Assert.True(root.TryGetProperty("EndpointPort", out _));
            Assert.True(root.TryGetProperty("EndpointUseSsl", out _));
            Assert.True(root.TryGetProperty("WorkloadPreGenerationMs", out _));
            Assert.True(root.TryGetProperty("PayloadPreparationMs", out _));
            Assert.True(root.TryGetProperty("GeneratedArticles", out _));
            Assert.True(root.TryGetProperty("GeneratedGbps", out _));
            Assert.True(root.TryGetProperty("AdmittedArticles", out _));
            Assert.True(root.TryGetProperty("AcceptedArticles", out _));
            Assert.True(root.TryGetProperty("RejectedArticles", out _));
            Assert.True(root.TryGetProperty("AmbiguousArticles", out _));
            Assert.True(root.TryGetProperty("ProducerBlockedPercent", out _));
            Assert.True(root.TryGetProperty("WorkingSetMb", out _));
            Assert.True(root.TryGetProperty("GcHeapMb", out _));
            Assert.True(root.TryGetProperty("AllocatedMb", out _));
            Assert.True(root.TryGetProperty("Gen0", out _));
            Assert.True(root.TryGetProperty("Gen1", out _));
            Assert.True(root.TryGetProperty("Gen2", out _));
            Assert.True(root.TryGetProperty("AverageDispatchQueueWaitUs", out _));
            Assert.True(root.TryGetProperty("P50DispatchQueueWaitUs", out _));
            Assert.True(root.TryGetProperty("P95DispatchQueueWaitUs", out _));
            Assert.True(root.TryGetProperty("P99DispatchQueueWaitUs", out _));
            Assert.True(root.TryGetProperty("MaxDispatchQueueWaitUs", out _));
            Assert.True(root.TryGetProperty("PendingDepthLatencyBuckets", out _));
            Assert.True(root.TryGetProperty("EffectiveQueueArticleCapacityFromBytes", out _));
            Assert.True(root.TryGetProperty("ObservabilityNotes", out _));
            Assert.True(root.TryGetProperty("FixedCountBoundaryTelemetry", out _));
        }
        /// <summary>
        /// Confirms the benchmark result artifact to csv preserves header order escaping and newline contract behavior.
        /// </summary>
        [Fact]
        public void BenchmarkResultArtifact_ToCsv_PreservesHeaderOrderEscapingAndNewlineContract()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig();
            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                BenchmarkContractTestHelper.CreateMeasurementSnapshot(),
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 6, 1, 0, 0, 10, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(1),
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: 0,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: true);

            BenchmarkResultArtifact artifact = BenchmarkResultArtifact.From(result, config, processorCount: 4) with
            {
                PendingDepthLatencyBuckets = "Depth 1-4: \"quoted\",value",
                ObservabilityNotes = "LineA\nLineB",
                RuntimeAssemblyPath = "C:\\temp\\runner.exe"
            };

            string csv = artifact.ToCsv();

            Assert.EndsWith(Environment.NewLine, csv, StringComparison.Ordinal);

            string[] records = SplitCsvRecords(csv);
            Assert.Equal(2, records.Length);

            string header = records[0];
            string row = records[1];

            Assert.Contains("benchmark_build_version,runtime_assembly_version,runtime_assembly_path", header, StringComparison.Ordinal);
            Assert.Contains(",generated_articles,generated_gbps,admitted_articles,admitted_gbps,accepted_articles,accepted_gbps,rejected_articles,ambiguous_articles,", header, StringComparison.Ordinal);
            Assert.Contains(",dispatch_wait_us_avg,dispatch_wait_us_p50,dispatch_wait_us_p95,dispatch_wait_us_p99,dispatch_wait_us_max,", header, StringComparison.Ordinal);
            Assert.Contains(",effective_queue_article_capacity_from_bytes,pending_depth_latency_buckets,observability_notes", header, StringComparison.Ordinal);

            Assert.Contains("\"Depth 1-4: \"\"quoted\"\",value\"", row, StringComparison.Ordinal);
            Assert.Contains("\"LineA\nLineB\"", row, StringComparison.Ordinal);
        }

        /// <summary>
        /// Splits a CSV payload into records while preserving embedded quotes and escaped CSV content.
        /// </summary>
        /// <param name="csv">
        /// The serialized CSV content to parse into individual records for contract validation.
        /// </param>
        /// <returns>
        /// An array of CSV records with quoted fields preserved as written by the artifact serializer.
        /// </returns>
        private static string[] SplitCsvRecords(string csv)
        {
            List<string> records = [];
            StringBuilder current = new();
            bool inQuotes = false;

            for (int i = 0; i < csv.Length; i++)
            {
                char ch = csv[i];

                if (ch == '"')
                {
                    if (inQuotes && i + 1 < csv.Length && csv[i + 1] == '"')
                    {
                        current.Append('"');
                        current.Append('"');
                        i++;
                        continue;
                    }

                    inQuotes = !inQuotes;
                    current.Append(ch);
                    continue;
                }

                if (!inQuotes && ch == '\r' && i + 1 < csv.Length && csv[i + 1] == '\n')
                {
                    if (current.Length > 0)
                    {
                        records.Add(current.ToString());
                        current.Clear();
                    }

                    i++;
                    continue;
                }

                if (!inQuotes && ch == '\n')
                {
                    if (current.Length > 0)
                    {
                        records.Add(current.ToString());
                        current.Clear();
                    }

                    continue;
                }

                current.Append(ch);
            }

            if (current.Length > 0)
            {
                records.Add(current.ToString());
            }

            return [.. records];
        }
    }
}
