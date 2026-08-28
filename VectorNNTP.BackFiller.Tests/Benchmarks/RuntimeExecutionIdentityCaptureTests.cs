// <copyright file="RuntimeExecutionIdentityCaptureTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using System.Reflection;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates runtime identity capture contracts for benchmark and production assembly separation.
    /// </summary>
    public sealed class RuntimeExecutionIdentityCaptureTests
    {
        /// <summary>
        /// Verifies captured runtime assembly path points to the executing benchmark assembly identity source.
        /// </summary>
        [Fact]
        public void Capture_RuntimeAssemblyPath_UsesBenchmarkAssemblyIdentity()
        {
            RuntimeExecutionIdentity identity = RuntimeExecutionIdentityCapture.Capture(
                fallbackAssembly: typeof(TransitServerStressRunner).Assembly,
                entryAssembly: typeof(TransitServerStressRunner).Assembly);

            Assert.EndsWith("VectorNNTP.BackFiller.Benchmarks.dll", identity.RuntimeAssemblyPath, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Verifies captured runtime assembly path is not reported as the production dependency assembly.
        /// </summary>
        [Fact]
        public void Capture_RuntimeAssemblyPath_IsNotProductionDependencyAssemblyPath()
        {
            RuntimeExecutionIdentity identity = RuntimeExecutionIdentityCapture.Capture(
                fallbackAssembly: typeof(TransitServerStressRunner).Assembly,
                entryAssembly: typeof(TransitServerStressRunner).Assembly);

            Assert.DoesNotContain("VectorNNTP.BackFiller.dll", identity.RuntimeAssemblyPath, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Verifies production dependency identity is captured from the loaded production assembly.
        /// </summary>
        [Fact]
        public void Capture_ProductionDependencyIdentity_MatchesTransitPublisherAssembly()
        {
            RuntimeExecutionIdentity identity = RuntimeExecutionIdentityCapture.Capture(
                fallbackAssembly: typeof(TransitServerStressRunner).Assembly,
                entryAssembly: typeof(TransitServerStressRunner).Assembly);
            Assembly productionAssembly = typeof(TransitPublisher).Assembly;

            string expectedProductionPath = Path.GetFullPath(productionAssembly.Location);
            string expectedProductionAssemblyVersion = productionAssembly.GetName().Version?.ToString() ?? "(unknown)";
            string expectedProductionFileVersion = productionAssembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version ?? "(unknown)";

            Assert.Equal(expectedProductionPath, identity.ProductionDependencyPath, ignoreCase: true);
            Assert.Equal(expectedProductionAssemblyVersion, identity.ProductionDependencyAssemblyVersion);
            Assert.Equal(expectedProductionFileVersion, identity.ProductionDependencyFileVersion);
        }
    }
}
