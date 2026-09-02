// <copyright file="CsvArtifactWriter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Artifacts/CsvArtifactWriter: writes durable JSON and CSV representations of benchmark measurements.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the csv ArtifactWriter class used by this benchmark or regression-gate component.
/// </summary>
internal static class CsvArtifactWriter
{
    /// <summary>
    /// Executes the get ArtifactPath operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static string GetArtifactPath(string baseDirectory, string stamp)
    {
        return Path.Combine(baseDirectory, $"transit-benchmark-result-{stamp}.csv");
    }

    /// <summary>
    /// Executes the write ToPath operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static void WriteToPath(string path, string csv)
    {
        File.WriteAllText(path, csv);
    }
}
