// <copyright file="CsvArtifactWriter.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Artifacts/CsvArtifactWriter: serializes benchmark result rows as deterministic CSV artifacts.

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Represents the csv ArtifactWriter class used by the benchmark or regression gate.
    /// </summary>
    internal static class CsvArtifactWriter
    {
        /// <summary>
        /// Gets ArtifactPath.
        /// </summary>
        internal static string GetArtifactPath(string baseDirectory, string stamp)
        {
            return Path.Combine(baseDirectory, $"transit-benchmark-result-{stamp}.csv");
        }

        /// <summary>
        /// Writes ToPath.
        /// </summary>
        internal static void WriteToPath(string path, string csv)
        {
            File.WriteAllText(path, csv);
        }
    }
}
