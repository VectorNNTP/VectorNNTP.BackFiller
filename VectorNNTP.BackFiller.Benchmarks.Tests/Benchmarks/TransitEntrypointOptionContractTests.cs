// <copyright file="TransitEntrypointOptionContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for transit benchmark entrypoint option contracts, covering duration propagation and runtime identity expectations.
// Primary responsibility: documents executable behavioral contracts for benchmark mode option forwarding.

using Microsoft.Extensions.Configuration;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates benchmark entrypoint behavioral contracts for mode planning, duration planning, and forensic identity compatibility.
    /// </summary>
    public sealed class TransitEntrypointOptionContractTests
    {
        /// <summary>
        /// Verifies that transit-validate planning preserves a caller-supplied duration override.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitValidateDurationProvided_UsesProvidedDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-validate", "--duration-seconds", "37"]);

            Assert.Equal(TransitEntrypointKind.TransitValidate, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(37), plan.EffectiveDuration);
        }

        /// <summary>
        /// Verifies that transit-benchmark-fakeserver planning preserves a caller-supplied duration override.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitFakeServerDurationProvided_UsesProvidedDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-benchmark-fakeserver", "--duration-seconds", "37"]);

            Assert.Equal(TransitEntrypointKind.TransitBenchmarkFakeServer, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(37), plan.EffectiveDuration);
        }

        /// <summary>
        /// Verifies that transit-single-trace planning preserves a caller-supplied duration override.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitSingleTraceDurationProvided_UsesProvidedDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-single-trace", "--duration-seconds", "37"]);

            Assert.Equal(TransitEntrypointKind.TransitSingleTrace, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(37), plan.EffectiveDuration);
        }

        /// <summary>
        /// Verifies that validation planning preserves existing default duration behavior when no duration override is supplied.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitValidateDurationOmitted_UsesDefaultValidationDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-validate"]);

            Assert.Equal(TransitEntrypointKind.TransitValidate, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(10), plan.EffectiveDuration);
        }

        /// <summary>
        /// Verifies documented mode tokens map to the intended production entrypoint kinds.
        /// </summary>
        [Theory]
        [InlineData("transit-validate", nameof(TransitEntrypointKind.TransitValidate))]
        [InlineData("transit-benchmark-fakeserver", nameof(TransitEntrypointKind.TransitBenchmarkFakeServer))]
        [InlineData("transit-single-trace", nameof(TransitEntrypointKind.TransitSingleTrace))]
        [InlineData("transit-generator-worker-sweep", nameof(TransitEntrypointKind.TransitGeneratorWorkerSweep))]
        [InlineData("transit-forensic-32worker", nameof(TransitEntrypointKind.TransitForensic32Worker))]
        public void BuildEntrypointPlan_WhenModeProvided_MapsToExpectedEntrypointKind(string mode, string expectedKindName)
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan([mode]);

            Assert.Equal(expectedKindName, plan.Kind.ToString());
        }

        /// <summary>
        /// Verifies that the transit benchmark configuration boundary consumes a supplied measurement duration exactly.
        /// </summary>
        [Theory]
        [InlineData(37)]
        [InlineData(10)]
        public void LoadFromConfiguration_WhenDurationProvided_UsesExactMeasurementDuration(int durationSeconds)
        {
            TransitBenchmarkCliOptions options = new(
                DurationSeconds: null,
                WarmupSeconds: 10,
                ConnectionPoolSize: 4,
                PipelineDepth: 8,
                DispatchWorkers: 32,
                QueueMegabytes: 256,
                QueueArticles: 256,
                ArticleKilobytes: 1024,
                GeneratorWorkers: 1,
                WriteBatchCoalesceMicroseconds: 250,
                ExpectedAssemblyPath: RuntimeIdentity.RuntimeAssemblyPath,
                ExpectedAssemblyVersion: RuntimeIdentity.RuntimeAssemblyVersion,
                ExpectedFileVersion: RuntimeIdentity.AssemblyFileVersion,
                ExpectedConfiguration: RuntimeIdentity.Configuration,
                ExpectedPlatform: RuntimeIdentity.Platform,
                ExpectedTargetFramework: RuntimeIdentity.TargetFramework,
                ExpectedRuntimeIdentifier: RuntimeIdentity.RuntimeIdentifier,
                ExpectedArchitecture: RuntimeIdentity.Architecture,
                ExpectedProductionAssemblyPath: RuntimeIdentity.ProductionDependencyPath,
                ExpectedProductionAssemblyVersion: RuntimeIdentity.ProductionDependencyAssemblyVersion,
                ExpectedProductionFileVersion: RuntimeIdentity.ProductionDependencyFileVersion);

            TransitBenchmarkConfig config = TransitBenchmarkConfig.LoadFromConfiguration(
                TimeSpan.FromSeconds(durationSeconds),
                BenchmarkMode.Validation,
                options,
                BuildTransitConfiguration(),
                appSettingsPath: "in-memory:test");

            Assert.Equal(TimeSpan.FromSeconds(durationSeconds), config.MeasurementDuration);
        }

        /// <summary>
        /// Verifies forensic mode options remain compatible with the actual runtime identity guard contract using deterministic in-memory configuration.
        /// </summary>
        [Fact]
        public void CreateForensicModeOptions_WhenLoadedFromInMemoryConfig_PassesRuntimeIdentityGuard()
        {
            TransitBenchmarkCliOptions options = TransitServerStressRunner.CreateForensicModeOptions(generatorWorkers: 32);

            TransitBenchmarkConfig config = TransitBenchmarkConfig.LoadFromConfiguration(
                TimeSpan.FromSeconds(30),
                BenchmarkMode.Forensic,
                options,
                BuildTransitConfiguration(),
                appSettingsPath: "in-memory:test");

            RuntimeIdentityGuard.EnsureMatches(config.ExpectedRuntimeIdentity, RuntimeIdentity);

            Assert.Equal(BenchmarkMode.Forensic, config.Mode);
            Assert.Equal(32, config.GeneratorWorkerCount);
        }

        /// <summary>
        /// Builds deterministic transit endpoint configuration for configuration-boundary tests.
        /// </summary>
        /// <returns>An in-memory configuration root containing required transit endpoint values.</returns>
        private static IConfiguration BuildTransitConfiguration()
        {
            return new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["BackFiller:TransitServer:Host"] = "incoming.usenet.ninja",
                    ["BackFiller:TransitServer:Port"] = "563",
                    ["BackFiller:TransitServer:UseSsl"] = "true",
                })
                .Build();
        }

        /// <summary>
        /// Captures runtime identity from the same authoritative source used by production benchmark execution.
        /// </summary>
        private static RuntimeExecutionIdentity RuntimeIdentity { get; } = RuntimeExecutionIdentityCapture.Capture(typeof(TransitServerStressRunner).Assembly);
    }
}
