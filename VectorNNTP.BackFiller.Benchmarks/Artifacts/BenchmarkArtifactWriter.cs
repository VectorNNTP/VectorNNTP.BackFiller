// <copyright file="BenchmarkArtifactWriter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Artifacts/BenchmarkArtifactWriter: writes durable JSON and CSV representations of benchmark measurements.

using System.Globalization;

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Represents the benchmark ArtifactWriter class used by the benchmark or regression gate.
    /// </summary>
    internal static class BenchmarkArtifactWriter
    {
        /// <summary>
        /// Builds the structured benchmark result and writes its JSON and CSV artifacts using one
        /// timestamped base name so the two representations describe the same measurement.
        /// </summary>
        internal static void WriteStructuredResultArtifacts<TArtifact>(
            BenchmarkResult result,
            TransitBenchmarkConfig config,
            int processorCount,
            Func<BenchmarkResult, TransitBenchmarkConfig, int, TArtifact> artifactFactory,
            Func<TArtifact, string> csvFactory)
        {
            try
            {
                TArtifact artifact = artifactFactory(result, config, processorCount);
                string json = JsonArtifactWriter.Serialize(artifact);
                string csv = csvFactory(artifact);

                string stamp = DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
                string baseDir = AppContext.BaseDirectory;
                string jsonPath = JsonArtifactWriter.GetArtifactPath(baseDir, stamp);
                string csvPath = CsvArtifactWriter.GetArtifactPath(baseDir, stamp);

                JsonArtifactWriter.WriteToPath(jsonPath, json);
                CsvArtifactWriter.WriteToPath(csvPath, csv);

                Console.WriteLine();
                Console.WriteLine("Structured benchmark artifacts written:");
                Console.WriteLine($"JSON: {jsonPath}");
                Console.WriteLine($"CSV:  {csvPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WARNING: Failed to write structured benchmark artifacts: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }
}
