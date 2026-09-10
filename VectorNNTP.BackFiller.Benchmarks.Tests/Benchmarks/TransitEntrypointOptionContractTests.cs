// <copyright file="TransitEntrypointOptionContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for transit benchmark entrypoint option contracts, covering duration propagation and runtime identity expectations.
// Primary responsibility: documents executable behavioral contracts for benchmark mode option forwarding.

using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Defines a non-parallel collection for tests that temporarily install process-wide benchmark execution hooks.
    /// </summary>
    [CollectionDefinition("TransitEntrypointOptionContractSerial", DisableParallelization = true)]
    public sealed class TransitEntrypointOptionContractSerialCollection
    {
    }

    /// <summary>
    /// Validates benchmark entrypoint behavioral contracts for duration propagation and runtime identity guard compatibility.
    /// </summary>
    [Collection("TransitEntrypointOptionContractSerial")]
    public sealed class TransitEntrypointOptionContractTests
    {
        /// <summary>
        /// Proves Program dispatch propagates caller duration for transit-validate to effective guarded config boundary.
        /// </summary>
        [Fact]
        public async Task ProgramMain_WhenTransitValidateDurationProvided_PropagatesDurationToGuardedConfig()
        {
            const int expectedSeconds = 37;
            TransitBenchmarkConfig? capturedConfig = null;

            TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = (config, _, _) =>
            {
                capturedConfig = config;
                return true;
            };

            try
            {
                await Program.Main([
                    "transit-validate",
                    "--duration-seconds", expectedSeconds.ToString(),
                    "--expected-assembly-path", RuntimeIdentity.RuntimeAssemblyPath,
                    "--expected-assembly-version", RuntimeIdentity.RuntimeAssemblyVersion,
                    "--expected-file-version", RuntimeIdentity.AssemblyFileVersion!,
                    "--expected-target-framework", RuntimeIdentity.TargetFramework!,
                    "--expected-architecture", RuntimeIdentity.Architecture,
                    "--expected-production-assembly-path", RuntimeIdentity.ProductionDependencyPath!,
                    "--expected-production-assembly-version", RuntimeIdentity.ProductionDependencyAssemblyVersion!,
                    "--expected-production-file-version", RuntimeIdentity.ProductionDependencyFileVersion!,
                ]);
            }
            finally
            {
                TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = null;
            }

            Assert.NotNull(capturedConfig);
            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), capturedConfig.Value.MeasurementDuration);
            Assert.Equal(BenchmarkMode.Validation, capturedConfig.Value.Mode);
        }

        /// <summary>
        /// Proves Program dispatch propagates caller duration for transit-benchmark-fakeserver to effective guarded config boundary.
        /// </summary>
        [Fact]
        public async Task ProgramMain_WhenFakeServerDurationProvided_PropagatesDurationToGuardedConfig()
        {
            const int expectedSeconds = 37;
            TransitBenchmarkConfig? capturedConfig = null;

            TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = (config, _, _) =>
            {
                capturedConfig = config;
                return true;
            };

            try
            {
                await Program.Main([
                    "transit-benchmark-fakeserver",
                    "--duration-seconds", expectedSeconds.ToString(),
                    "--expected-assembly-path", RuntimeIdentity.RuntimeAssemblyPath,
                    "--expected-assembly-version", RuntimeIdentity.RuntimeAssemblyVersion,
                    "--expected-file-version", RuntimeIdentity.AssemblyFileVersion!,
                    "--expected-target-framework", RuntimeIdentity.TargetFramework!,
                    "--expected-architecture", RuntimeIdentity.Architecture,
                    "--expected-production-assembly-path", RuntimeIdentity.ProductionDependencyPath!,
                    "--expected-production-assembly-version", RuntimeIdentity.ProductionDependencyAssemblyVersion!,
                    "--expected-production-file-version", RuntimeIdentity.ProductionDependencyFileVersion!,
                ]);
            }
            finally
            {
                TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = null;
            }

            Assert.NotNull(capturedConfig);
            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), capturedConfig.Value.MeasurementDuration);
            Assert.Equal(BenchmarkMode.Validation, capturedConfig.Value.Mode);
            Assert.Equal(BenchmarkDevNullTransitServer.EndpointTypeLabel, capturedConfig.Value.EndpointType);
        }

        /// <summary>
        /// Proves Program dispatch propagates caller duration for transit-single-trace to effective guarded config boundary.
        /// </summary>
        [Fact]
        public async Task ProgramMain_WhenSingleTraceDurationProvided_PropagatesDurationToGuardedConfig()
        {
            const int expectedSeconds = 37;
            TransitBenchmarkConfig? capturedConfig = null;

            TransitSingleTraceRunner.GuardedExecutionShortCircuitHook = (config, _) =>
            {
                capturedConfig = config;
                return true;
            };

            try
            {
                await Program.Main([
                    "transit-single-trace",
                    "--duration-seconds", expectedSeconds.ToString(),
                    "--expected-assembly-path", RuntimeIdentity.RuntimeAssemblyPath,
                    "--expected-assembly-version", RuntimeIdentity.RuntimeAssemblyVersion,
                    "--expected-file-version", RuntimeIdentity.AssemblyFileVersion!,
                    "--expected-target-framework", RuntimeIdentity.TargetFramework!,
                    "--expected-architecture", RuntimeIdentity.Architecture,
                    "--expected-production-assembly-path", RuntimeIdentity.ProductionDependencyPath!,
                    "--expected-production-assembly-version", RuntimeIdentity.ProductionDependencyAssemblyVersion!,
                    "--expected-production-file-version", RuntimeIdentity.ProductionDependencyFileVersion!,
                ]);
            }
            finally
            {
                TransitSingleTraceRunner.GuardedExecutionShortCircuitHook = null;
            }

            Assert.NotNull(capturedConfig);
            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), capturedConfig.Value.MeasurementDuration);
            Assert.Equal(BenchmarkMode.Validation, capturedConfig.Value.Mode);
        }

        /// <summary>
        /// Proves forensic-mode options are compatible with actual RuntimeIdentityGuard contract.
        /// </summary>
        [Fact]
        public void CreateForensicModeOptions_WhenInvoked_ProducesGuardCompatibleIdentityExpectations()
        {
            TransitBenchmarkCliOptions options = TransitServerStressRunner.CreateForensicModeOptions(generatorWorkers: 32);
            TransitBenchmarkConfig config = TransitBenchmarkConfig.Load(TimeSpan.FromSeconds(30), BenchmarkMode.Forensic, options);

            RuntimeIdentityGuard.EnsureMatches(config.ExpectedRuntimeIdentity, RuntimeIdentity);

            Assert.Equal(BenchmarkMode.Forensic, config.Mode);
            Assert.Equal(32, config.GeneratorWorkerCount);
        }

        /// <summary>
        /// Proves sweep mode routes through guarded core path using actual mode implementation.
        /// </summary>
        [Fact]
        public async Task SweepMode_WhenExecuted_RoutesIntoGuardedCorePath()
        {
            int invocations = 0;

            TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = (_, _, _) =>
            {
                invocations++;
                return true;
            };

            try
            {
                await TransitServerStressRunner.RunGeneratorWorkerSweepAsync();
            }
            finally
            {
                TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = null;
            }

            Assert.Equal(6, invocations);
        }

        /// <summary>
        /// Proves forensic-32worker mode routes through guarded core path using actual mode implementation.
        /// </summary>
        [Fact]
        public async Task Forensic32Mode_WhenExecuted_RoutesIntoGuardedCorePath()
        {
            int invocations = 0;

            TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = (_, _, _) =>
            {
                invocations++;
                return true;
            };

            try
            {
                await TransitServerStressRunner.RunForensic32WorkerAsync();
            }
            finally
            {
                TransitBenchmarkOrchestrator.GuardedExecutionShortCircuitHook = null;
            }

            Assert.Equal(1, invocations);
        }

        /// <summary>
        /// Captures runtime identity from the same authoritative source used by production benchmark code.
        /// </summary>
        private static RuntimeExecutionIdentity RuntimeIdentity { get; } = RuntimeExecutionIdentityCapture.Capture(typeof(TransitServerStressRunner).Assembly);
    }
}
