// <copyright file="JsonArtifactWriter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Artifacts/JsonArtifactWriter: writes durable JSON and CSV representations of benchmark measurements.

using System.Text.Json;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the json ArtifactWriter class for benchmark or isolated-regression execution.
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
    /// Performs the get ArtifactPath operation.
    /// </summary>
    internal static string GetArtifactPath(string baseDirectory, string stamp)
    {
        return Path.Combine(baseDirectory, $"transit-benchmark-result-{stamp}.json");
    }

    /// <summary>
    /// Performs the write ToPath operation.
    /// </summary>
    internal static void WriteToPath(string path, string json)
    {
        File.WriteAllText(path, json);
    }
}
