// <copyright file="TransitBenchmarkConfigArticleCountValidationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for transit benchmark config article count validation, covering configuration and validation contracts; NNTP article and transport behavior; benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the transit benchmark config article count validation test suite.

using Microsoft.Extensions.Configuration;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates fixed-count configuration semantics without requiring live appsettings loading.
    /// </summary>
    public sealed class TransitBenchmarkConfigArticleCountValidationTests
    {
        /// <summary>
        /// Verifies configuration rejects mutually exclusive article-count and duration-seconds options.
        /// </summary>
        [Fact]
        public void Load_WhenArticleCountAndDurationSecondsAreBothSpecified_ThrowsInvalidOperationException()
        {
            TransitBenchmarkCliOptions options = new(
                DurationSeconds: 10,
                WarmupSeconds: 5,
                ConnectionPoolSize: 1,
                PipelineDepth: 1,
                DispatchWorkers: 1,
                QueueMegabytes: 64,
                QueueArticles: 64,
                ArticleKilobytes: 1024,
                GeneratorWorkers: 1,
                WriteBatchCoalesceMicroseconds: 250,
                ArticleCount: 200);

            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["BackFiller:TransitServer:Host"] = "incoming.usenet.ninja",
                    ["BackFiller:TransitServer:Port"] = "563",
                    ["BackFiller:TransitServer:UseSsl"] = "true",
                })
                .Build();

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
                TransitBenchmarkConfig.LoadFromConfiguration(
                    TimeSpan.FromSeconds(10),
                    BenchmarkMode.Validation,
                    options,
                    configuration,
                    appSettingsPath: "in-memory:test"));

            Assert.Contains("mutually exclusive", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
