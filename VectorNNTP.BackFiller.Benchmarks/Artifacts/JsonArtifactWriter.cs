using System.Text.Json;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class JsonArtifactWriter
{
    internal static string Serialize<TArtifact>(TArtifact artifact)
    {
        return JsonSerializer.Serialize(artifact, new JsonSerializerOptions { WriteIndented = true });
    }

    internal static string GetArtifactPath(string baseDirectory, string stamp)
    {
        return Path.Combine(baseDirectory, $"transit-benchmark-result-{stamp}.json");
    }

    internal static void WriteToPath(string path, string json)
    {
        File.WriteAllText(path, json);
    }
}
