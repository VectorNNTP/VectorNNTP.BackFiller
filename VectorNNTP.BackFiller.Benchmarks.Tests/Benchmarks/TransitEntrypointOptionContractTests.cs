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
    /// Validates benchmark entrypoint behavioral contracts for mode routing, duration planning, and forensic identity compatibility.
    /// </summary>
    public sealed class TransitEntrypointOptionContractTests
    {
        /// <summary>
        /// Proves transit-validate planning preserves a caller-supplied duration override.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitValidateDurationProvided_UsesProvidedDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-validate", "--duration-seconds", "37"]);

            Assert.Equal(TransitEntrypointKind.TransitValidate, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(37), plan.EffectiveDuration);
        }

        /// <summary>
        /// Proves transit-benchmark-fakeserver planning preserves a caller-supplied duration override.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitFakeServerDurationProvided_UsesProvidedDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-benchmark-fakeserver", "--duration-seconds", "37"]);

            Assert.Equal(TransitEntrypointKind.TransitBenchmarkFakeServer, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(37), plan.EffectiveDuration);
        }

        /// <summary>
        /// Proves transit-single-trace planning preserves a caller-supplied duration override.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitSingleTraceDurationProvided_UsesProvidedDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-single-trace", "--duration-seconds", "37"]);

            Assert.Equal(TransitEntrypointKind.TransitSingleTrace, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(37), plan.EffectiveDuration);
        }

        /// <summary>
        /// Proves validation-mode planning retains the existing default duration when no duration override is supplied.
        /// </summary>
        [Fact]
        public void BuildEntrypointPlan_WhenTransitValidateDurationOmitted_UsesDefaultValidationDuration()
        {
            TransitEntrypointPlan plan = Program.BuildEntrypointPlan(["transit-validate"]);

            Assert.Equal(TransitEntrypointKind.TransitValidate, plan.Kind);
            Assert.Equal(TimeSpan.FromSeconds(10), plan.EffectiveDuration);
        }

        /// <summary>
        /// Proves documented mode tokens map to intended production entrypoint kinds.
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
        /// Proves forensic mode options remain compatible with the actual runtime identity guard contract using deterministic in-memory configuration.
        /// </summary>
        [Fact]
        public void CreateForensicModeOptions_WhenLoadedFromInMemoryConfig_PassesRuntimeIdentityGuard()
        {
            RuntimeExecutionIdentity runtimeIdentity = RuntimeExecutionIdentityCapture.Capture(typeof(TransitServerStressRunner).Assembly);
            TransitBenchmarkCliOptions options = TransitServerStressRunner.CreateForensicModeOptions(generatorWorkers: 32);

            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["BackFiller:TransitServer:Host"] = "incoming.usenet.ninja",
                    ["BackFiller:TransitServer:Port"] = "563",
                    ["BackFiller:TransitServer:UseSsl"] = "true",
                })
                .Build();

            TransitBenchmarkConfig config = TransitBenchmarkConfig.LoadFromConfiguration(
                TimeSpan.FromSeconds(30),
                BenchmarkMode.Forensic,
                options,
                configuration,
                appSettingsPath: "in-memory:test");

            RuntimeIdentityGuard.EnsureMatches(config.ExpectedRuntimeIdentity, runtimeIdentity);

            Assert.Equal(BenchmarkMode.Forensic, config.Mode);
            Assert.Equal(32, config.GeneratorWorkerCount);
        }
    }
}
