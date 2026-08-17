using System.Globalization;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class BenchmarkArtifactWriter
{
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
