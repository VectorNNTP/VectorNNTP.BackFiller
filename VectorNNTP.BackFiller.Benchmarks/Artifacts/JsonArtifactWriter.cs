// <copyright file="JsonArtifactWriter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Artifacts/JsonArtifactWriter: serializes benchmark result records as indented JSON artifacts.

using System.Text.Json;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the json ArtifactWriter class used by the benchmark or regression gate.
/// </summary>
internal static class JsonArtifactWriter
{
    /// <summary>
    /// Serializes an artifact with indented JSON for human inspection and durable benchmark output.
    /// </summary>
    internal static string Serialize<TArtifact>(TArtifact artifact)
    {
        return JsonSerializer.Serialize(artifact, new JsonSerializerOptions { WriteIndented = true });
    }

    /// <summary>
    /// Gets ArtifactPath.

    /// </summary>
    internal static string GetArtifactPath(string baseDirectory, string stamp)
    {
        return Path.Combine(baseDirectory, $"transit-benchmark-result-{stamp}.json");
    }

    /// <summary>
    /// Writes ToPath.

    /// </summary>
    internal static void WriteToPath(string path, string json)
    {
        File.WriteAllText(path, json);
    }
}
