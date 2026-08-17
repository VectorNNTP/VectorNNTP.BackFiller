namespace VectorNNTP.BackFiller.Benchmarks;

internal static class CsvArtifactWriter
{
    internal static string GetArtifactPath(string baseDirectory, string stamp)
    {
        return Path.Combine(baseDirectory, $"transit-benchmark-result-{stamp}.csv");
    }

    internal static void WriteToPath(string path, string csv)
    {
        File.WriteAllText(path, csv);
    }
}
