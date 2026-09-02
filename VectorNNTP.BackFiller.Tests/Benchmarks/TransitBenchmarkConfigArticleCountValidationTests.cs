// <copyright file="TransitBenchmarkConfigArticleCountValidationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Contract and behavior tests for the transit benchmark config article count validation benchmark component.

using System.Reflection;
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

            MethodInfo loadMethod = typeof(TransitBenchmarkConfig)
                .GetMethod("Load", BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException("TransitBenchmarkConfig.Load was not found.");

            TargetInvocationException ex = Assert.Throws<TargetInvocationException>(() =>
                loadMethod.Invoke(
                    null,
                    [
                        TimeSpan.FromSeconds(10),
                        BenchmarkMode.Validation,
                        options,
                        Type.Missing,
                        Type.Missing,
                        Type.Missing,
                        Type.Missing,
                        Type.Missing,
                    ]));

            _ = Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Contains("mutually exclusive", ex.InnerException!.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
