// <copyright file="CsvArtifactWriter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Artifacts/CsvArtifactWriter: writes durable JSON and CSV representations of benchmark measurements.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the csv ArtifactWriter class for benchmark or isolated-regression execution.
/// </summary>
internal static class CsvArtifactWriter
{
    /// <summary>
    /// Performs the get ArtifactPath operation.
    /// </summary>
    internal static string GetArtifactPath(string baseDirectory, string stamp)
    {
        return Path.Combine(baseDirectory, $"transit-benchmark-result-{stamp}.csv");
    }

    /// <summary>
    /// Performs the write ToPath operation.
    /// </summary>
    internal static void WriteToPath(string path, string csv)
    {
        File.WriteAllText(path, csv);
    }
}
