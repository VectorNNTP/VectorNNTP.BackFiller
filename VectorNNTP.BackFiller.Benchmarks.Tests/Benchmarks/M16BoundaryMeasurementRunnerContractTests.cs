// <copyright file="M16BoundaryMeasurementRunnerContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for m16 boundary measurement runner contracts, covering truthful metric labeling and expected versus observed materialization semantics.

using VectorNNTP.BackFiller.Benchmarks;
using VectorNNTP.Backfiller.Runtime.Articles;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Verifies M16 boundary measurement output remains truthful and explicitly non-comparative.
    /// </summary>
    [Collection(ConsoleCaptureSerializationCollection.Name)]
    public sealed class M16BoundaryMeasurementRunnerContractTests
    {
        /// <summary>
        /// Verifies the M16 measurement report distinguishes expected acquisition materialization from observed late materialization.
        /// </summary>
        [Fact]
        public async Task RunAsync_WhenExecuted_ReportsTruthfulMaterializationMetricLabels()
        {
            StringWriter writer = new();
            TextWriter originalOut = Console.Out;
            Console.SetOut(writer);

            try
            {
                await M16BoundaryMeasurementRunner.RunAsync().ConfigureAwait(false);
            }
            finally
            {
                Console.SetOut(originalOut);
            }

            string output = writer.ToString();
            Assert.Contains("MeasurementNote: Scenarios are intentionally non-comparative", output, StringComparison.Ordinal);
            Assert.Contains("AvgServerBytesWritten:", output, StringComparison.Ordinal);
            Assert.Contains("AvgExpectedAcquisitionMaterializationBytes:", output, StringComparison.Ordinal);
            Assert.Contains("AvgObservedMaterializedPayloadBytes:", output, StringComparison.Ordinal);
            Assert.Contains("AvgExpectedMaterializationThresholdBytes:", output, StringComparison.Ordinal);
            Assert.Contains($"AvgExpectedAcquisitionMaterializationBytes: {ArticleResourceLimits.MaxArticleBytes}", output, StringComparison.Ordinal);
            Assert.Contains("Scenario: EarlyRejectThroughAcquisitionPath", output, StringComparison.Ordinal);
            Assert.Contains("Scenario: LateRejectAfterClientMaterialization", output, StringComparison.Ordinal);
        }
    }
}
